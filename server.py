from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse
import fitz  # PyMuPDF
import base64, io, json

app = FastAPI()


@app.post("/pdf-to-images")
async def pdf_to_images(req: Request):
    body = await req.json()
    pdf_b64 = body["pdf_base64"]
    dpi = body.get("dpi", 200)
    quality = body.get("quality", 85)
    pdf_bytes = base64.b64decode(pdf_b64)
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    pages = []
    for i, page in enumerate(doc):
        mat = fitz.Matrix(dpi / 72, dpi / 72)
        pix = page.get_pixmap(matrix=mat)
        img_bytes = pix.tobytes("jpeg", quality)
        pages.append({
            "page": i + 1,
            "image_base64": base64.b64encode(img_bytes).decode()
        })
    return JSONResponse({"pages": pages, "total_pages": len(pages)})


@app.post("/pdf-to-images-stream")
async def pdf_to_images_stream(req: Request):
    body = await req.json()
    pdf_b64 = body["pdf_base64"]
    dpi = body.get("dpi", 200)
    quality = body.get("quality", 85)
    pdf_bytes = base64.b64decode(pdf_b64)

    def generate():
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        total_pages = len(doc)

        for i, page in enumerate(doc):
            mat = fitz.Matrix(dpi / 72, dpi / 72)
            pix = page.get_pixmap(matrix=mat)
            img_bytes = pix.tobytes("jpeg", quality)
            record = {
                "page": i + 1,
                "total_pages": total_pages,
                "image_base64": base64.b64encode(img_bytes).decode(),
            }
            yield json.dumps(record, separators=(",", ":")) + "\n"

        doc.close()

    return StreamingResponse(generate(), media_type="application/x-ndjson")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8002)