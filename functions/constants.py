# functions/constants.py

# =========================
# GENERAL CONFIGS
# =========================
TIMEZONE = "America/Chicago"


# =========================
# MYSQL BUDGETS
# =========================
SERVICE_BUDGETS = [
    "e39a81c6-6ff7-436e-9bce-5d4e25cef244",
    "42801189-8217-4ce1-9623-30c02d9d1518",
    "c6ac34bc-0fc0-46a6-9723-e83780ebb938",
    "c2586fb3-caae-4842-9139-764c1064338c",
    "87949499-baf1-472a-baed-1c6a1ee12b0a",
    "6830db64-0c38-440e-9a8f-b8b8ab1d00af",
]
SERVICE_MAPPING = {
    "e39a81c6-6ff7-436e-9bce-5d4e25cef244": {
        "serviceName": "Digital Display Ads",
        "adTypeCode": "DIS"
    },
    "42801189-8217-4ce1-9623-30c02d9d1518": {
        "serviceName": "Digital Pre-Roll",
        "adTypeCode": "VID"
    },
    "c6ac34bc-0fc0-46a6-9723-e83780ebb938": {
        "serviceName": "Search Engine Marketing",
        "adTypeCode": "SEM"
    },
    "c2586fb3-caae-4842-9139-764c1064338c": {
        "serviceName": "Inventory Search Engine Marketing",
        "adTypeCode": "PM"
    },
    "87949499-baf1-472a-baed-1c6a1ee12b0a": {
        "serviceName": "Vehicle Listing Ads",
        "adTypeCode": "PM"
    },
    "6830db64-0c38-440e-9a8f-b8b8ab1d00af": {
        "serviceName": "Site Retargeting",
        "adTypeCode": "DIS"
    }
}

ADTYPES = {
    "SEM": {
        "order": 1,
        "adTypeQuery": "SEARCH",
        "fullName": "Search Ad",
        "shortName": "Search",
    },
    "DIS": {
        "order": 2,
        "adTypeQuery": "DISPLAY",
        "fullName": "Display Ad",
        "shortName": "Display",
    },
    "VID": {
        "order": 3,
        "adTypeQuery": "VIDEO",
        "fullName": "Video Ad",
        "shortName": "Video",
    },
    "PM": {
        "order": 4,
        "adTypeQuery": "PERFORMANCE_MAX",
        "fullName": "Performance Max",
        "shortName": "Performance Max",
    },
    "DM": {
        "order": 5,
        "adTypeQuery": "DEMAND_GEN",
        "fullName": "Demand Gen",
        "shortName": "Demand Gen",
    },
}


# =========================
# PARALLEL EXECUTION CONFIG
# =========================

PARALLEL_MAX_WORKERS = 8

PARALLEL_MAX_RETRIES = 3

PARALLEL_INITIAL_BACKOFF = 1.0      # seconds
PARALLEL_MAX_BACKOFF = 10.0         # seconds

PARALLEL_TASK_TIMEOUT = 60          # seconds per task

PARALLEL_JITTER_MIN = 0.1            # seconds
PARALLEL_JITTER_MAX = 0.5 

PARALLEL_RATE_LIMIT = 5        # requests
PARALLEL_RATE_INTERVAL = 1.0   # per second


# =====================
# LOGGING CONFIG
# =====================

LOGGING_ENABLED = True        # Set False to completely disable logging
LOG_LEVEL = "INFO"           # DEBUG | INFO | WARNING | ERROR | CRITICAL