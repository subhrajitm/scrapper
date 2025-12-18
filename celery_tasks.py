"""
Celery tasks for background scraping jobs.
"""

import logging
from typing import List, Dict, Any
from datetime import datetime

from celery import current_task
from celery.exceptions import SoftTimeLimitExceeded

from celery_config import celery_app
from database import (
    create_job,
    update_job,
    save_scraped_item,
    get_job,
    clear_expired_cache as db_clear_cache,
)

logger = logging.getLogger(__name__)


@celery_app.task(bind=True, max_retries=2)
def scrape_websites_task(self, job_id: str, urls: List[str]) -> Dict[str, Any]:
    """
    Background task to scrape multiple websites.
    Uses Scrapy with optional Playwright for JS rendering.
    """
    from scrapy.crawler import CrawlerRunner
    from scrapy.utils.log import configure_logging
    from twisted.internet import reactor, defer
    from spiders.website_spider import WebsiteSpider
    import threading

    # Update job status
    update_job(job_id, status="running", message="Starting scrape...")

    results = []
    errors = []

    try:
        configure_logging(install_root_handler=False)

        # Build Scrapy settings
        settings = _build_scrapy_settings()
        runner = CrawlerRunner(settings)

        # Track progress
        completed = 0
        url_status = {url: "pending" for url in urls}

        def item_callback(item: Dict[str, Any], spider):
            """Called when an item is scraped."""
            nonlocal completed
            website = item.get("website", "")
            
            # Save to database
            save_scraped_item(job_id, item)
            results.append(item)

            # Update status
            if website in url_status:
                url_status[website] = "completed"
                completed += 1
                update_job(
                    job_id,
                    completed=completed,
                    message=f"Scraped {completed}/{len(urls)} websites",
                    url_status=url_status,
                )

        @defer.inlineCallbacks
        def crawl_all():
            for url in urls:
                url_status[url] = "scraping"
                update_job(job_id, url_status=url_status, message=f"Scraping {url}...")

                try:
                    yield runner.crawl(
                        WebsiteSpider,
                        start_urls=[url],
                        job_id=job_id,
                        item_callback=item_callback,
                    )
                except Exception as e:
                    logger.error(f"Error scraping {url}: {e}")
                    url_status[url] = "error"
                    errors.append({"url": url, "error": str(e)})

        def run_in_thread():
            d = crawl_all()
            d.addBoth(lambda _: reactor.stop())
            reactor.run(installSignalHandlers=False)

        # Run in separate thread to not block Celery worker
        thread = threading.Thread(target=run_in_thread)
        thread.start()
        thread.join(timeout=3600)  # 1 hour timeout

        # Update final status
        update_job(
            job_id,
            status="completed",
            completed=len(urls),
            message=f"Completed! Scraped {len(results)} websites.",
            url_status=url_status,
        )

        return {
            "job_id": job_id,
            "status": "completed",
            "total": len(urls),
            "scraped": len(results),
            "errors": len(errors),
        }

    except SoftTimeLimitExceeded:
        update_job(job_id, status="error", message="Task timed out")
        raise

    except Exception as e:
        logger.exception(f"Scrape task failed: {e}")
        update_job(job_id, status="error", message=f"Error: {str(e)}")
        raise self.retry(exc=e)


@celery_app.task
def scrape_single_website_task(job_id: str, url: str) -> Dict[str, Any]:
    """Scrape a single website (used for parallel processing)."""
    return scrape_websites_task.delay(job_id, [url]).get()


@celery_app.task
def clear_expired_cache_task():
    """Periodic task to clear expired search cache."""
    db_clear_cache()
    return {"status": "ok", "timestamp": datetime.utcnow().isoformat()}


def _build_scrapy_settings() -> dict:
    """Build Scrapy settings with Playwright support if available."""
    settings = {
        "LOG_LEVEL": "INFO",
        "ROBOTSTXT_OBEY": True,
        "DOWNLOAD_DELAY": 0.5,
        "CONCURRENT_REQUESTS": 16,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 4,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 1,
        "AUTOTHROTTLE_MAX_DELAY": 10,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 4.0,
        "COOKIES_ENABLED": True,
        "RETRY_ENABLED": True,
        "RETRY_TIMES": 2,
        "DEPTH_LIMIT": 3,
        "DEPTH_PRIORITY": 1,
        "SCHEDULER_DISK_QUEUE": "scrapy.squeues.PickleFifoDiskQueue",
        "SCHEDULER_MEMORY_QUEUE": "scrapy.squeues.FifoMemoryQueue",
        "USER_AGENT": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    }

    # Add Playwright settings if available
    try:
        import scrapy_playwright
        settings.update({
            "DOWNLOAD_HANDLERS": {
                "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
                "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
            },
            "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
            "PLAYWRIGHT_BROWSER_TYPE": "chromium",
            "PLAYWRIGHT_LAUNCH_OPTIONS": {
                "headless": True,
                "args": ["--no-sandbox", "--disable-dev-shm-usage"],
            },
            "PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT": 30000,
        })
    except ImportError:
        pass

    return settings
