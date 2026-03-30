# shared/constants.py
from pathlib import Path

# =========================
# GENERAL CONFIGS
# =========================
TIMEZONE = "America/Chicago"

# =====================
# GOOGLE ADS SAFETY LIMITS
# =====================

# Max number of updates allowed in a single request
GGADS_MAX_UPDATES_PER_REQUEST = 100

# Max number of campaigns that can be paused at once
GGADS_MAX_PAUSED_CAMPAIGNS = 50

# Budget safety rules
GGADS_MIN_BUDGET = 0.01  # must be > 0
GGADS_MAX_BUDGET_MULTIPLIER = 10  # prevent 10× / 100× spikes
GGADS_MIN_BUDGET_DELTA = 0.50  # skip small changes unless forcing 0.00/0.01
BUDGET_LESS_THAN_SPEND_TOLERANCE = 0.50  # allow small spend-over-allocation drift
GGADS_MUTATION_MAX_ATTEMPTS = 3  # initial attempt + retries for transient errors
GGADS_MUTATION_RETRY_INITIAL_BACKOFF_SECONDS = 1.0
GGADS_MUTATION_RETRY_MAX_BACKOFF_SECONDS = 8.0

# Allowed campaign statuses
GGADS_ALLOWED_CAMPAIGN_STATUSES = {"ENABLED", "PAUSED"}


# =========================
# PARALLEL EXECUTION CONFIG
# =========================

PARALLEL_MAX_WORKERS = 8

PARALLEL_MAX_RETRIES = 3

PARALLEL_INITIAL_BACKOFF = 1.0  # seconds
PARALLEL_MAX_BACKOFF = 10.0  # seconds

PARALLEL_TASK_TIMEOUT = 60  # seconds per task

PARALLEL_JITTER_MIN = 0.1  # seconds
PARALLEL_JITTER_MAX = 0.5

PARALLEL_RATE_LIMIT = 5  # requests
PARALLEL_RATE_INTERVAL = 1.0  # per second


# =====================
# LOGGING CONFIG
# =====================

# Global switch
LOGGING_ENABLED = True

# Logging level
# DEBUG | INFO | WARNING | ERROR | CRITICAL
LOG_LEVEL = "INFO"

# Axiom logging level
AXIOM_LOG_LEVEL = "INFO"

# Directory for all logs (anchored to repo/fastapi)
LOG_DIR = str(Path(__file__).resolve().parents[1] / "logs")

# Per-run file rotation (within a single run)
LOG_MAX_BYTES = 1 * 1024 * 1024  # 10 MB per file
LOG_BACKUP_COUNT = 5  # Rotated files per run

# Retention policy
LOG_RETENTION_DAYS = 1  # Delete logs older than N days
