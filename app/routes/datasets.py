from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from fastapi.templating import Jinja2Templates
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))

router = APIRouter()

def get_catalog(request: Request):
    return request.app.state.catalog

@router.get("/")
async def homepage(request: Request):
    catalog = get_catalog(request)
    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={"datasets": catalog}
    )

@router.get("/datasets")
async def list_datasets(request: Request):
    catalog = get_catalog(request)
    return JSONResponse(content=catalog)

@router.get("/datasets/{dataset_id}")
async def dataset_detail(dataset_id: str, request: Request):
    catalog = get_catalog(request)
    dataset = next((d for d in catalog if d["id"] == dataset_id), None)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    return templates.TemplateResponse(
        request=request,
        name="dataset.html",
        context={"dataset": dataset}
    )

@router.get("/datasets/{dataset_id}/download")
async def download_dataset(dataset_id: str, request: Request):
    catalog = get_catalog(request)
    dataset = next((d for d in catalog if d["id"] == dataset_id), None)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")

    file_path = dataset["file_path"]
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found")

    mime_types = {
        "CSV": "text/csv",
        "PARQUET": "application/octet-stream",
        "JSONL": "application/jsonlines"
    }
    media_type = mime_types.get(dataset["file_type"], "application/octet-stream")

    return FileResponse(
        path=file_path,
        media_type=media_type,
        filename=os.path.basename(file_path)
    )