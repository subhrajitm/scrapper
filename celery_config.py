"""
Celery configuration and task definitions.
Uses Redis as message broker and result backend.
"""

import os
from celery import Celery

# Redis configuration
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

# Create Celery app
celery_app = Celery(
    "legalscrape",
    broker=REDIS_URL,
    backend=REDIS_URL,
    include=["celery_tasks"],
)

# Celery configuration
celery_app.conf.update(
    # Task settings
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,

    # Task execution
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    task_time_limit=3600,  # 1 hour max per task
    task_soft_time_limit=3300,  # Soft limit at 55 minutes

    # Worker settings
    worker_prefetch_multiplier=1,  # One task at a time per worker
    worker_concurrency=2,  # 2 concurrent workers

    # Result settings
    result_expires=86400,  # Results expire after 24 hours

    # Retry settings
    task_default_retry_delay=60,  # Retry after 1 minute
    task_max_retries=3,

    # Beat schedule for periodic tasks
    beat_schedule={
        "clear-expired-cache": {
            "task": "celery_tasks.clear_expired_cache_task",
            "schedule": 3600.0,  # Every hour
        },
    },
)


def is_celery_available() -> bool:
    """Check if Celery/Redis is available."""
    try:
        from redis import Redis
        r = Redis.from_url(REDIS_URL, socket_connect_timeout=2)
        r.ping()
        return True
    except Exception:
        return False
