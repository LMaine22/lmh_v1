import os

MASSIVE_API_BASE = os.getenv("MASSIVE_API_BASE", "https://api.massive.com/v3")
MASSIVE_API_KEY = os.getenv("MASSIVE_API_KEY", "")

DEFAULT_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "30"))
MAX_RETRIES = int(os.getenv("HTTP_MAX_RETRIES", "3"))
CONCURRENCY = int(os.getenv("HTTP_CONCURRENCY", "8"))
PAGE_LIMIT = int(os.getenv("HTTP_PAGE_LIMIT", "1000"))
