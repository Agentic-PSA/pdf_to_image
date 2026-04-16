"""
PDF-to-Images Service
Dedicated microservice for converting PDF documents to JPEG page images.

Three conversion strategies:
  1. /pdf-to-images        — Batch: returns all pages as base64 JSON array
  2. /pdf-to-images-stream — Stream: NDJSON, one JSON line per page (low memory client-side)
  3. /pdf-to-images-chunked — Chunked background job for large PDFs (bounded RAM ~130 MB)

Auth is handled externally (nginx/ingress bearer token).
"""

import asyncio
import base64
import functools
import io
import json
import logging
import os
import shutil
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import BackgroundTasks, FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, model_validator
from starlette.responses import StreamingResponse

# ── Logging ────────────────────────────────────────────────────

_log_fmt = "%(asctime)s %(levelname)s %(message)s"
_log_datefmt = "%Y-%m-%d %H:%M:%S"

logging.basicConfig(level=logging.INFO, format=_log_fmt, datefmt=_log_datefmt)
logger = logging.getLogger("pdf-to-images")

for _uv_name in ("uvicorn", "uvicorn.access", "uvicorn.error"):
    _uv_logger = logging.getLogger(_uv_name)
    _uv_logger.handlers.clear()
    _handler = logging.StreamHandler()
    _handler.setFormatter(logging.Formatter(_log_fmt, datefmt=_log_datefmt))
    _uv_logger.addHandler(_handler)
    _uv_logger.propagate = False

# ── Configuration ──────────────────────────────────────────────

MAX_FILE_SIZE_MB = int(os.getenv("MAX_FILE_SIZE_MB", "500"))
DEFAULT_DPI = int(os.getenv("DEFAULT_DPI", "200"))
OUTPUT_ROOT = os.getenv("OUTPUT_ROOT", "/data/output")

# Chunked processing settings
# Number of pages rasterised at once — keeps peak RAM ~130 MB
PDF_CHUNK_SIZE = int(os.getenv("PDF_CHUNK_SIZE", "20"))

# In-memory tracking for chunked background jobs
chunked_jobs: Dict[str, Dict[str, Any]] = {}

# ── App ────────────────────────────────────────────────────────

app = FastAPI(
    title="PDF-to-Images Service",
    version="1.0.0",
    description="Dedicated microservice for PDF-to-JPEG conversion with 3 strategies: batch, stream, chunked.",
)

# ── Models ─────────────────────────────────────────────────────


class PdfToImagesRequest(BaseModel):
    """Request body for batch and stream endpoints (base64 input)."""

    pdf_base64: str
    dpi: int = 200
    quality: int = 92
    pages: Optional[str] = None  # "all", "1-5", "3"


class ChunkedJobRequest(BaseModel):
    """Request body for submitting a chunked PDF-to-images background job."""

    # Source — exactly one required
    file_download_url: Optional[str] = None
    file_base64: Optional[str] = None
    file_name: str = "document.pdf"

    # Processing settings
    dpi: int = 200
    quality: int = 85
    chunk_size: Optional[int] = None  # override PDF_CHUNK_SIZE

    # Optional callback URL — POST with result summary when done
    callback_url: Optional[str] = None

    @model_validator(mode="after")
    def _exactly_one_source(self):
        has_url = bool(self.file_download_url)
        has_b64 = bool(self.file_base64)
        if has_url and has_b64:
            raise ValueError(
                "Provide either file_download_url or file_base64, not both"
            )
        if not has_url and not has_b64:
            raise ValueError(
                "Provide exactly one of file_download_url or file_base64"
            )
        return self


# ── Helpers ────────────────────────────────────────────────────


def parse_pages(pages_str: Optional[str]) -> Optional[tuple]:
    """Parse page range: None/'all' → None, '1-5' → (1, 5), '3' → (3, 3)."""
    if not pages_str or pages_str.strip().lower() == "all":
        return None
    parts = pages_str.strip().split("-")
    if len(parts) == 1:
        p = int(parts[0])
        return (p, p)
    return (int(parts[0]), int(parts[1]))


def _validate_pdf_bytes(pdf_bytes: bytes):
    """Common validation for decoded PDF bytes."""
    if len(pdf_bytes) > MAX_FILE_SIZE_MB * 1024 * 1024:
        raise HTTPException(
            status_code=413, detail=f"PDF too large. Max: {MAX_FILE_SIZE_MB} MB"
        )


# ── Health ─────────────────────────────────────────────────────


@app.get("/health")
async def health():
    """Health check — verifies poppler availability and reports active jobs."""
    poppler_ok = shutil.which("pdftoppm") is not None
    output_ok = Path(OUTPUT_ROOT).exists()

    active_jobs = sum(
        1 for j in chunked_jobs.values() if j["status"] == "processing"
    )

    return {
        "status": "ok" if poppler_ok else "degraded",
        "poppler_available": poppler_ok,
        "output_dir_accessible": output_ok,
        "active_chunked_jobs": active_jobs,
        "total_chunked_jobs": len(chunked_jobs),
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }


# ══════════════════════════════════════════════════════════════
# 1. BATCH — /pdf-to-images
# ══════════════════════════════════════════════════════════════


@app.post("/pdf-to-images")
async def pdf_to_images(req: PdfToImagesRequest):
    """
    Convert a PDF (base64) to an array of JPEG page images (base64).

    All pages are rasterised in one shot and returned as a single JSON
    response.  Best for small/medium PDFs (up to ~50 pages).
    """
    try:
        from pdf2image import convert_from_bytes
    except ImportError:
        raise HTTPException(status_code=500, detail="pdf2image not installed")

    try:
        pdf_bytes = base64.b64decode(req.pdf_base64)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid base64 PDF data")

    _validate_pdf_bytes(pdf_bytes)

    pages = parse_pages(req.pages)
    kwargs: Dict[str, Any] = {"dpi": req.dpi, "fmt": "jpeg"}
    if pages:
        kwargs["first_page"] = pages[0]
        kwargs["last_page"] = pages[1]

    try:
        images = convert_from_bytes(pdf_bytes, **kwargs)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"PDF conversion failed: {e}")

    result_pages = []
    start_page = pages[0] if pages else 1
    for i, img in enumerate(images):
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=req.quality)
        img_b64 = base64.b64encode(buf.getvalue()).decode("utf-8")
        buf.close()
        result_pages.append({"page": start_page + i, "image_base64": img_b64})

    return {
        "pages": result_pages,
        "total_pages": len(result_pages),
        "dpi": req.dpi,
    }


# ══════════════════════════════════════════════════════════════
# 2. STREAM — /pdf-to-images-stream
# ══════════════════════════════════════════════════════════════


@app.post("/pdf-to-images-stream")
async def pdf_to_images_stream(req: PdfToImagesRequest):
    """
    Convert PDF pages to JPEG images and stream them as NDJSON
    (one JSON line per page).  Avoids buffering all pages in the
    response body at once — the client can process each page as
    it arrives.
    """
    try:
        from pdf2image import convert_from_bytes
    except ImportError:
        raise HTTPException(status_code=500, detail="pdf2image not installed")

    try:
        pdf_bytes = base64.b64decode(req.pdf_base64)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid base64 PDF data")

    _validate_pdf_bytes(pdf_bytes)

    async def generate():
        pages = parse_pages(req.pages)
        kwargs: Dict[str, Any] = {"dpi": req.dpi, "fmt": "jpeg"}
        if pages:
            kwargs["first_page"] = pages[0]
            kwargs["last_page"] = pages[1]

        try:
            images = convert_from_bytes(pdf_bytes, **kwargs)
        except Exception as e:
            yield json.dumps({"error": str(e)}) + "\n"
            return

        start_page = pages[0] if pages else 1
        for i, img in enumerate(images):
            buf = io.BytesIO()
            img.save(buf, format="JPEG", quality=req.quality)
            img_b64 = base64.b64encode(buf.getvalue()).decode("utf-8")
            buf.close()
            yield json.dumps(
                {"page": start_page + i, "image_base64": img_b64}
            ) + "\n"

    return StreamingResponse(generate(), media_type="application/x-ndjson")


# ══════════════════════════════════════════════════════════════
# 3. CHUNKED — /pdf-to-images-chunked  (background job)
# ══════════════════════════════════════════════════════════════


async def chunked_worker(job_id: str, job: ChunkedJobRequest):
    """
    Background task: rasterise a large PDF page-by-page without loading
    the whole document into RAM at once.

    Memory strategy
    ───────────────
    1. Write the PDF to a temp file on disk (stream download or base64 decode).
    2. Use pdfinfo_from_path to learn the page count (no rasterisation).
    3. Rasterise only PDF_CHUNK_SIZE pages at a time via convert_from_path,
       discard the PIL Images before the next chunk.

    Peak RAM per chunk:  PDF_CHUNK_SIZE × DPI² × channels ≈ 20 × 6.5 MB ≈ 130 MB
    vs. naive approach:  2000 pages × 6.5 MB ≈ 13 GB

    4. Both pdfinfo_from_path and convert_from_path run in a thread-pool
       executor so the synchronous poppler calls never block the event loop.
    5. Each page image is saved to disk as a JPEG file — no large list
       accumulation in RAM.
    """
    chunked_jobs[job_id] = {
        "status": "processing",
        "pages_done": 0,
        "total_pages": 0,
        "started_at": datetime.utcnow().isoformat(),
        "completed_at": None,
        "error": None,
        "output_dir": None,
    }

    tmp_path: Optional[str] = None
    chunk_size = job.chunk_size or PDF_CHUNK_SIZE

    try:
        from pdf2image import convert_from_path, pdfinfo_from_path
        import httpx

        loop = asyncio.get_running_loop()

        # ── Step 1: Write PDF to disk ──────────────────────────────────
        tmp_path = f"/tmp/chunked_{job_id}.pdf"

        if job.file_download_url:
            logger.info(f"[chunked] Job {job_id}: downloading {job.file_download_url}")
            async with httpx.AsyncClient(timeout=300) as client:
                async with client.stream(
                    "GET",
                    job.file_download_url,
                ) as resp:
                    resp.raise_for_status()
                    with open(tmp_path, "wb") as fh:
                        async for chunk in resp.aiter_bytes(chunk_size=1024 * 1024):
                            fh.write(chunk)
        elif job.file_base64:
            logger.info(f"[chunked] Job {job_id}: decoding base64")
            pdf_bytes = base64.b64decode(job.file_base64)
            with open(tmp_path, "wb") as fh:
                fh.write(pdf_bytes)
            del pdf_bytes
        else:
            raise ValueError(
                "No file source (file_download_url or file_base64 required)"
            )

        file_size_mb = os.path.getsize(tmp_path) / 1024 / 1024
        logger.info(f"[chunked] Job {job_id}: PDF on disk {file_size_mb:.1f} MB")

        # ── Step 2: Page count ─────────────────────────────────────────
        info = await loop.run_in_executor(
            None, functools.partial(pdfinfo_from_path, tmp_path)
        )
        total_pages = info["Pages"]
        chunked_jobs[job_id]["total_pages"] = total_pages
        logger.info(f"[chunked] Job {job_id}: {total_pages} pages")

        # ── Step 3: Prepare output directory ───────────────────────────
        output_dir = Path(OUTPUT_ROOT) / job_id
        output_dir.mkdir(parents=True, exist_ok=True)
        chunked_jobs[job_id]["output_dir"] = str(output_dir)

        pages_done = 0

        # ── Step 4: Rasterise in chunks ────────────────────────────────
        for chunk_start_0 in range(0, total_pages, chunk_size):
            chunk_end_0 = min(chunk_start_0 + chunk_size, total_pages)
            first_page = chunk_start_0 + 1
            last_page = chunk_end_0

            logger.info(
                f"[chunked] Job {job_id}: rasterising pages "
                f"{first_page}–{last_page} of {total_pages} (DPI={job.dpi})"
            )

            chunk_images = await loop.run_in_executor(
                None,
                functools.partial(
                    convert_from_path,
                    tmp_path,
                    dpi=job.dpi,
                    fmt="jpeg",
                    first_page=first_page,
                    last_page=last_page,
                ),
            )

            for i, img in enumerate(chunk_images):
                page_num = chunk_start_0 + i + 1
                page_filename = f"page_{page_num:05d}.jpg"
                img.save(
                    str(output_dir / page_filename),
                    format="JPEG",
                    quality=job.quality,
                )
                pages_done += 1

            chunked_jobs[job_id]["pages_done"] = pages_done
            logger.info(
                f"[chunked] Job {job_id}: progress {pages_done}/{total_pages}"
            )

            # Free PIL images before next chunk
            del chunk_images

        # ── Step 5: Write manifest ─────────────────────────────────────
        manifest = {
            "job_id": job_id,
            "file_name": job.file_name,
            "total_pages": total_pages,
            "dpi": job.dpi,
            "quality": job.quality,
            "pages": [
                {
                    "page": p,
                    "filename": f"page_{p:05d}.jpg",
                    "path": f"/output/{job_id}/page_{p:05d}.jpg",
                }
                for p in range(1, total_pages + 1)
            ],
            "completed_at": datetime.utcnow().isoformat() + "Z",
        }
        (output_dir / "manifest.json").write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2)
        )

        chunked_jobs[job_id]["status"] = "completed"
        chunked_jobs[job_id]["pages_done"] = total_pages
        chunked_jobs[job_id]["completed_at"] = datetime.utcnow().isoformat()
        logger.info(f"[chunked] Job {job_id}: completed ({total_pages} pages)")

        # ── Step 6: Optional callback ──────────────────────────────────
        if job.callback_url:
            try:
                async with httpx.AsyncClient(timeout=30) as client:
                    resp = await client.post(
                        job.callback_url,
                        json={
                            "job_id": job_id,
                            "status": "completed",
                            "total_pages": total_pages,
                            "manifest_url": f"/output/{job_id}/manifest.json",
                        },
                    )
                    logger.info(
                        f"[chunked] Job {job_id}: callback {resp.status_code}"
                    )
            except Exception as cb_err:
                logger.warning(
                    f"[chunked] Job {job_id}: callback failed: {cb_err}"
                )

    except Exception as e:
        error_msg = f"{type(e).__name__}: {e}"
        logger.error(f"[chunked] Job {job_id} failed: {error_msg}")
        chunked_jobs[job_id]["status"] = "failed"
        chunked_jobs[job_id]["error"] = error_msg
    finally:
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.unlink(tmp_path)
            except Exception:
                pass


@app.post("/pdf-to-images-chunked")
async def submit_chunked_job(
    req: ChunkedJobRequest,
    background_tasks: BackgroundTasks,
):
    """
    Submit a large PDF for background chunked conversion.

    The worker streams the PDF to disk, rasterises pages in configurable
    chunks (default 20 pages) to keep RAM bounded at ~130 MB, saves each
    page as a JPEG file, and writes a manifest.json when done.

    Returns 202 Accepted with a job_id to poll status.
    """
    job_id = uuid.uuid4().hex[:16]

    background_tasks.add_task(chunked_worker, job_id, req)

    return JSONResponse(
        status_code=202,
        content={
            "job_id": job_id,
            "message": f"Chunked conversion accepted for {req.file_name}",
            "status_url": f"/chunked-job/{job_id}",
        },
    )


@app.get("/chunked-job/{job_id}")
async def get_chunked_job_status(job_id: str):
    """Poll the status of a chunked conversion job."""
    if job_id not in chunked_jobs:
        raise HTTPException(status_code=404, detail="Job not found")

    return chunked_jobs[job_id]


@app.get("/chunked-jobs")
async def list_chunked_jobs():
    """List all chunked conversion jobs."""
    return {
        "jobs": chunked_jobs,
        "total": len(chunked_jobs),
        "active": sum(
            1 for j in chunked_jobs.values() if j["status"] == "processing"
        ),
    }


# ── Serve output files (chunked job results) ──────────────────


@app.get("/output/{job_id}/{filename}")
async def serve_output_file(job_id: str, filename: str):
    """
    Download a page image or manifest.json produced by a chunked job.
    """
    file_path = Path(OUTPUT_ROOT) / job_id / filename

    if not file_path.exists() or not file_path.is_file():
        raise HTTPException(status_code=404, detail="File not found")

    # Path traversal guard
    if not str(file_path.resolve()).startswith(str(Path(OUTPUT_ROOT).resolve())):
        raise HTTPException(status_code=403, detail="Access denied")

    media_type = "image/jpeg" if filename.endswith(".jpg") else "application/json"
    return FileResponse(str(file_path), media_type=media_type)


@app.delete("/output/{job_id}")
async def delete_output(job_id: str):
    """Delete all output files for a completed chunked job."""
    output_dir = Path(OUTPUT_ROOT) / job_id
    if not output_dir.exists():
        raise HTTPException(status_code=404, detail="Output not found")

    if not str(output_dir.resolve()).startswith(str(Path(OUTPUT_ROOT).resolve())):
        raise HTTPException(status_code=403, detail="Access denied")

    shutil.rmtree(output_dir)
    chunked_jobs.pop(job_id, None)

    return {"deleted": True, "job_id": job_id}