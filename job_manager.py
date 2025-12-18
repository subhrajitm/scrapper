"""
Job Manager - Orchestrates scraping jobs with database persistence and optional Celery.

This module provides a unified interface for starting and managing scrape jobs.
It uses SQLite for persistence and optionally dispatches to Celery for background processing.
Falls back to in-memory threading if Redis/Celery is not available.
"""

import logging
import uuid
from typing import List, Dict, Any, Optional

from database import (
    create_job,
    get_job,
    update_job,
    get_job_items,
    save_scraped_item,
    get_cached_search,
    save_search_cache,
    Job,
)

logger = logging.getLogger(__name__)

# Check if Celery is available
_celery_available: Optional[bool] = None


def is_celery_available() -> bool:
    """Check if Celery/Redis is available and working."""
    global _celery_available
    if _celery_available is not None:
        return _celery_available

    try:
        from celery_config import is_celery_available as check_celery
        _celery_available = check_celery()
    except Exception:
        _celery_available = False

    return _celery_available


def start_job(urls: List[str]) -> str:
    """
    Start a new scraping job.
    
    If Celery is available, dispatches to background worker.
    Otherwise, uses the existing in-memory threading approach.
    
    Returns the job_id.
    """
    job_id = uuid.uuid4().hex

    # Create job in database
    create_job(job_id, urls)

    if is_celery_available():
        # Dispatch to Celery
        logger.info(f"Starting job {job_id} via Celery")
        try:
            from celery_tasks import scrape_websites_task
            scrape_websites_task.delay(job_id, urls)
        except Exception as e:
            logger.error(f"Failed to dispatch to Celery: {e}")
            # Fall back to in-memory
            _start_job_inmemory(job_id, urls)
    else:
        # Use existing in-memory approach
        logger.info(f"Starting job {job_id} in-memory (Celery not available)")
        _start_job_inmemory(job_id, urls)

    return job_id


def _start_job_inmemory(job_id: str, urls: List[str]) -> None:
    """Start job using the existing in-memory scrapy_scraper module."""
    from scrapy_scraper import start_scrape_job as legacy_start
    # The legacy function generates its own job_id, so we need to sync
    # For now, we'll start with legacy and sync progress to DB
    import threading

    def run_and_sync():
        from scrapy_scraper import (
            _ensure_job_structures,
            _ensure_reactor_running,
            _get_or_create_runner,
            _build_scrapy_settings,
            update_progress,
            scraped_items_by_job,
            items_lock,
        )
        from spiders.website_spider import WebsiteSpider
        from twisted.internet import reactor, defer

        _ensure_job_structures(job_id)
        update_progress(job_id=job_id, status="running", total=len(urls), urls=urls)

        # Update database
        update_job(job_id, status="running", message="Starting scrape...")

        _ensure_reactor_running()
        runner = _get_or_create_runner()

        @defer.inlineCallbacks
        def crawl_all():
            url_status = {u: "pending" for u in urls}
            completed = 0

            for url in urls:
                url_status[url] = "scraping"
                update_progress(job_id=job_id, url_status=url_status, message=f"Scraping {url}...")
                update_job(job_id, url_status=url_status, message=f"Scraping {url}...")

                try:
                    yield runner.crawl(
                        WebsiteSpider,
                        start_urls=[url],
                        job_id=job_id,
                    )
                    url_status[url] = "completed"
                    completed += 1
                except Exception as e:
                    logger.error(f"Error scraping {url}: {e}")
                    url_status[url] = "error"

                update_progress(job_id=job_id, completed=completed, url_status=url_status)
                update_job(job_id, completed=completed, url_status=url_status)

            update_progress(job_id=job_id, status="completed", message="Completed!")
            update_job(job_id, status="completed", message="Completed!")

            # Sync items to database
            with items_lock:
                items = scraped_items_by_job.get(job_id, [])
            for item in items:
                save_scraped_item(job_id, item)

        reactor.callFromThread(lambda: defer.ensureDeferred(crawl_all()))

    thread = threading.Thread(target=run_and_sync, daemon=True)
    thread.start()


def stop_job(job_id: str) -> bool:
    """Stop a running job."""
    # Update database
    update_job(job_id, status="cancelled", message="Job cancelled by user")

    # Also stop via legacy if running in-memory
    try:
        from scrapy_scraper import stop_scrape_job
        stop_scrape_job(job_id)
    except Exception:
        pass

    # If using Celery, we could revoke the task here
    # (requires storing task_id in the database)

    return True


def get_job_progress(job_id: str) -> Dict[str, Any]:
    """Get job progress from database or in-memory."""
    # Try database first
    job = get_job(job_id)
    if job:
        return job.to_dict()

    # Fall back to in-memory
    try:
        from scrapy_scraper import get_scraping_progress
        return get_scraping_progress(job_id)
    except Exception:
        return {
            "id": job_id,
            "status": "unknown",
            "message": "Job not found",
        }


def get_job_results(job_id: str) -> List[Dict[str, Any]]:
    """Get scraped items for a job."""
    # Try database first
    items = get_job_items(job_id)
    if items:
        return items

    # Fall back to in-memory
    try:
        from scrapy_scraper import get_scraped_items
        return get_scraped_items(job_id)
    except Exception:
        return []


def get_cached_results(
    practice_area: str,
    location: str,
    country: str,
    page: int,
) -> Optional[Dict[str, Any]]:
    """Get cached search results."""
    return get_cached_search(practice_area, location, country, page)


def cache_results(
    practice_area: str,
    location: str,
    country: str,
    page: int,
    results: List[str],
    total_results: int,
    ttl_hours: int = 24,
) -> None:
    """Cache search results."""
    save_search_cache(practice_area, location, country, page, results, total_results, ttl_hours)
