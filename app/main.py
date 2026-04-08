from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from contextlib import asynccontextmanager
import os

from app.catalog import build_catalog
from app.routes.datasets import router as datasets_router

# --- Catalog loaded once at startup ---
catalog = []

@asynccontextmanager
async def lifespan(app: FastAPI):
    global catalog
    catalog = build_catalog()
    app.state.catalog = catalog
    print(f"✅ Catalog loaded — {len(catalog)} datasets found.")
    yield

# --- App ---
app = FastAPI(
    title="African Open Datasets",
    description="Structured African data, free to download.",
    version="1.0.0",
    lifespan=lifespan,
)

# --- Static files & templates ---
BASE_DIR = os.path.dirname(__file__)
app.mount("/static", StaticFiles(directory=os.path.join(BASE_DIR, "static")), name="static")
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))

# --- Routes ---
app.include_router(datasets_router)

# --- Make catalog + templates available to routes ---
app.state.templates = templates