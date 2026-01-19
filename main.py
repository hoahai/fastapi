from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse

from apps.spendsphere.api.main import app as spendsphere_app
from apps.shiftzy.api.main import app as shiftzy_app


app = FastAPI()

# Mount app-specific APIs under distinct prefixes.
app.mount("/api/spendsphere", spendsphere_app)
app.mount("/api/shiftzy", shiftzy_app)


@app.get("/")
def root():
    html_path = Path(__file__).resolve().parent / "static" / "index.html"
    return FileResponse(html_path)


@app.get("/ping")
def ping():
    return {"status": "ok"}
