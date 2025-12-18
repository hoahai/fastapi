# functions/constants.py

# ---- General Congifs ----
TIMEZONE = "America/Chicago"


# ---- MySQL Budgets ----
SERVICE_BUDGETS = [
    "e39a81c6-6ff7-436e-9bce-5d4e25cef244",
    "42801189-8217-4ce1-9623-30c02d9d1518",
    "c6ac34bc-0fc0-46a6-9723-e83780ebb938",
    "c2586fb3-caae-4842-9139-764c1064338c",
    "87949499-baf1-472a-baed-1c6a1ee12b0a",
    "6830db64-0c38-440e-9a8f-b8b8ab1d00af",
]


# ---- Google Ads parallel execution config ----
GGAD_MAX_WORKERS = 3

GGAD_MAX_RETRIES = 3
GGAD_INITIAL_BACKOFF = 1.0     # seconds
GGAD_MAX_BACKOFF = 10.0        # seconds

GGAD_ACCOUNT_TIMEOUT = 30.0    # seconds
GGAD_JITTER_MIN = 0.0
GGAD_JITTER_MAX = 3.0