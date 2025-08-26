import os

# Ensure REDIS_URL is present so modules fail fast in a controlled way during tests
os.environ.setdefault("REDIS_URL", "redis://localhost:6379/0")
