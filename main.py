from fastapi import FastAPI

from apps.spendsphere.api.main import app as spendsphere_app
from apps.shiftzy.api.main import app as shiftzy_app


app = FastAPI()

# Mount app-specific APIs under distinct prefixes.
app.mount("/spendsphere", spendsphere_app)
app.mount("/shiftzy", shiftzy_app)


@app.get("/")
def root():
    return {
        "status": "ok",
        "apps": {
            "spendsphere": "/spendsphere",
            "shiftzy": "/shiftzy",
        },
    }
