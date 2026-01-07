# main.py
from dotenv import load_dotenv

load_dotenv("secrets/.env")
from fastapi import FastAPI, HTTPException
from functions.db_queries import (
    get_masterbudgets,
    get_allocations,
    get_rollbreakdowns,
)
from functions.ggSheet import get_rollovers
from functions.utils import get_current_period
from functions.spendsphere import run_google_ads_budget_pipeline

app = FastAPI()


# -------------------------------
# ROOT
# -------------------------------
@app.get("/")
def root():
    return {"status": "Hello World!"}


# -------------------------------
# UTILS
# -------------------------------
@app.get("/api/utils/current-period")
def getCurrentPeriod():
    return get_current_period()


# -------------------------------
# BUDGETS
# -------------------------------
@app.get("/api/budgets/{account_code}")
def getBudgets(account_code: str):
    # 1️⃣ Validate input
    if not account_code or not account_code.strip():
        raise HTTPException(status_code=400, detail="account_code is required")

    data = get_masterbudgets(account_code)

    # 2️⃣ Handle not found
    if not data:
        raise HTTPException(
            status_code=404,
            detail=f"No budgets found for account_code '{account_code}'",
        )

    return data


# -------------------------------
# ALLOCATIONS
# -------------------------------
@app.get("/api/allocations/{account_code}")
def getAllocations(account_code: str):
    # 1️⃣ Validate input
    if not account_code or not account_code.strip():
        raise HTTPException(status_code=400, detail="account_code is required")

    data = get_allocations(account_code)

    # 2️⃣ Handle not found
    if not data:
        raise HTTPException(
            status_code=404,
            detail=f"No allocations found for account_code '{account_code}'",
        )

    return data


# -------------------------------
# ROLLOVERS
# -------------------------------
@app.get("/api/rollovers/{account_code}")
def getRollovers(account_code):
    # 1️⃣ Validate input
    if not account_code or not account_code.strip():
        raise HTTPException(status_code=400, detail="account_code is required")

    data = get_rollovers(account_code)

    # 2️⃣ Handle not found
    if not data:
        raise HTTPException(
            status_code=404,
            detail=f"No rollovers found for account_code '{account_code}'",
        )

    return data


@app.get("/api/rollovers/breakdown/{account_code}")
def getRolloversBreakDown(account_code: str):
    # 1️⃣ Validate input
    if not account_code or not account_code.strip():
        raise HTTPException(status_code=400, detail="account_code is required")

    data = get_rollbreakdowns(account_code)

    # 2️⃣ Handle not found
    if not data:
        raise HTTPException(
            status_code=404,
            detail=f"No rollovers breakdown found for account_code '{account_code}'",
        )

    return data


# -------------------------------
# SPENDSPHERE UPDATE
# -------------------------------
@app.get("/api/update/all")
def update_google_ads():
    return run_google_ads_budget_pipeline(account_codes=["TAAA"], dry_run=False)


# This is important for Vercel
if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
