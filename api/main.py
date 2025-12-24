# main.py
from dotenv import load_dotenv
load_dotenv("secrets/.env")
from fastapi import FastAPI, HTTPException
from functions import repository
from functions.util import get_current_period

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
        raise HTTPException(
            status_code=400,
            detail="account_code is required"
        )

    data = repository.get_budgets_by_account(account_code)

    # 2️⃣ Handle not found
    if not data:
        raise HTTPException(
            status_code=404,
            detail=f"No budgets found for account_code '{account_code}'"
        )

    return data


# -------------------------------
# ALLOCATIONS
# -------------------------------
@app.get("/api/allocations/{account_code}")
def getAllocations(account_code: str):
    # 1️⃣ Validate input
    if not account_code or not account_code.strip():
        raise HTTPException(
            status_code=400,
            detail="account_code is required"
        )

    data = repository.get_allocations_by_account(account_code)

    # 2️⃣ Handle not found
    if not data:
        raise HTTPException(
            status_code=404,
            detail=f"No allocations found for account_code '{account_code}'"
        )

    return data


# -------------------------------
# ROLLOVERS
# -------------------------------
@app.get("/api/rollovers/{account_code}")
def getRollovers(account_code):
    # 1️⃣ Validate input
    if not account_code or not account_code.strip():
        raise HTTPException(
            status_code=400,
            detail="account_code is required"
        )

    data = repository.getRollovers(account_code)

    # 2️⃣ Handle not found
    if not data:
        raise HTTPException(
            status_code=404,
            detail=f"No rollovers found for account_code '{account_code}'"
        )

    return data

@app.get("/api/rollovers/breakdown/{account_code}")
def getRolloversBreakDown(account_code: str):
    # 1️⃣ Validate input
    if not account_code or not account_code.strip():
        raise HTTPException(
            status_code=400,
            detail="account_code is required"
        )

    data = repository.get_rollbreakdowns_by_account(account_code)

    # 2️⃣ Handle not found
    if not data:
        raise HTTPException(
            status_code=404,
            detail=f"No rollovers breakdown found for account_code '{account_code}'"
        )

    return data




# This is important for Vercel
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)