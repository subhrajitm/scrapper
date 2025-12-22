"""
Helper module for running Scrapy spiders from Flask.
This module provides a synchronous interface to Scrapy's async framework.
"""
import os
import threading
import time
import uuid
from typing import List, Dict, Any

# Install asyncio reactor BEFORE importing reactor if Playwright is enabled
# This must happen before any other Twisted imports
_use_playwright = os.getenv("USE_PLAYWRIGHT", "false").lower() == "true"
if _use_playwright:
    try:
        import asyncio
        from twisted.internet import asyncioreactor
        # Only install if not already installed
        try:
            asyncioreactor.install(asyncio.new_event_loop())
        except Exception:
            pass  # Already installed
    except ImportError:
        pass

from scrapy.crawler import CrawlerRunner
from scrapy.settings import Settings
from twisted.internet import reactor
from spiders.website_spider import WebsiteSpider


# Global storage for scraped items (per job)
scraped_items_by_job: Dict[str, List[Dict[str, Any]]] = {}
items_lock = threading.Lock()

# Live intermediate results (updated during scraping, before spider closes)
live_results_by_job: Dict[str, Dict[str, Dict[str, Any]]] = {}  # job_id -> { website -> aggregated_data }
live_results_lock = threading.Lock()

# Global progress tracking (per job)
scraping_progress_by_job: Dict[str, Dict[str, Any]] = {}
progress_lock = threading.Lock()
_latest_job_id_lock = threading.Lock()
_latest_job_id: str | None = None

# Job cancellation + crawler handles (best-effort stop)
_job_cancelled: Dict[str, bool] = {}
_job_crawlers: Dict[str, Any] = {}
_job_control_lock = threading.Lock()


def _new_job_id() -> str:
    return uuid.uuid4().hex


def _default_progress_dict() -> Dict[str, Any]:
    return {
        'status': 'idle',  # idle, running, completed, error
        'total': 0,
        'completed': 0,
        'current_url': '',
        'urls': [],
        'url_status': {},  # url -> status (pending, scraping, completed, error)
        'message': '',
    }


def _resolve_job_id(job_id: str | None) -> str:
    """Return job_id or the latest job_id, else a stable fallback."""
    if job_id:
        return job_id
    with _latest_job_id_lock:
        return _latest_job_id or "default"


def _ensure_job_structures(job_id: str) -> None:
    """Create empty progress/items containers for the job if missing."""
    global scraping_progress_by_job, scraped_items_by_job, live_results_by_job
    with progress_lock:
        if job_id not in scraping_progress_by_job:
            scraping_progress_by_job[job_id] = _default_progress_dict()
    with items_lock:
        scraped_items_by_job.setdefault(job_id, [])
    with live_results_lock:
        live_results_by_job.setdefault(job_id, {})
    with _job_control_lock:
        _job_cancelled.setdefault(job_id, False)

# Twisted reactor can only be started once per process. We keep it running in a
# background thread and schedule crawls onto it.
_reactor_thread_lock = threading.Lock()
_reactor_thread: threading.Thread | None = None

_runner_lock = threading.Lock()
_runner: CrawlerRunner | None = None

# The current Flask app/progress tracking assumes only one crawl at a time.
_crawl_serial_lock = threading.Lock()


def _build_scrapy_settings() -> Settings:
    """Configure Scrapy settings (shared across crawls)."""
    import os
    
    settings = Settings()
    settings.set(
        'USER_AGENT',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    )
    settings.set('ROBOTSTXT_OBEY', False)  # Set to False to scrape more aggressively
    settings.set('DOWNLOAD_DELAY', 0.25)  # Low delay for faster parallel scraping
    settings.set('CONCURRENT_REQUESTS', 32)  # High concurrency for parallel sites
    settings.set('CONCURRENT_REQUESTS_PER_DOMAIN', 4)  # Limit per-site to spread across sites
    settings.set('AUTOTHROTTLE_ENABLED', True)
    settings.set('AUTOTHROTTLE_START_DELAY', 0.25)
    settings.set('AUTOTHROTTLE_MAX_DELAY', 3)
    settings.set('AUTOTHROTTLE_TARGET_CONCURRENCY', 8.0)  # Target concurrency per domain
    settings.set('LOG_LEVEL', 'INFO')  # Show info level logs

    # Breadth-first crawling: process all base URLs before following subpages
    # Higher DEPTH_PRIORITY means deeper pages get lower priority (processed later)
    settings.set('DEPTH_PRIORITY', 1)
    settings.set('SCHEDULER_DISK_QUEUE', 'scrapy.squeues.PickleFifoDiskQueue')
    settings.set('SCHEDULER_MEMORY_QUEUE', 'scrapy.squeues.FifoMemoryQueue')

    settings.set(
        'ITEM_PIPELINES',
        {
            'scrapy_scraper.ItemsCollectorPipeline': 300,
        },
    )
    settings.set('DOWNLOAD_TIMEOUT', 30)
    settings.set('RETRY_ENABLED', True)
    settings.set('RETRY_TIMES', 2)
    settings.set('REQUEST_FINGERPRINTER_IMPLEMENTATION', '2.7')  # Fix deprecation warning
    settings.set('HTTPERROR_ALLOWED_CODES', [403, 404])  # Allow some error codes
    settings.set(
        'DEFAULT_REQUEST_HEADERS',
        {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en',
        },
    )
    
    # Playwright settings for JavaScript rendering (if enabled)
    use_playwright = os.getenv("USE_PLAYWRIGHT", "false").lower() == "true"
    if use_playwright:
        try:
            import scrapy_playwright  # noqa: F401
            settings.set('DOWNLOAD_HANDLERS', {
                "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
                "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
            })
            settings.set('TWISTED_REACTOR', 'twisted.internet.asyncioreactor.AsyncioSelectorReactor')
            settings.set('PLAYWRIGHT_BROWSER_TYPE', 'chromium')
            settings.set('PLAYWRIGHT_LAUNCH_OPTIONS', {
                'headless': True,
                'args': ['--no-sandbox', '--disable-dev-shm-usage'],
            })
            settings.set('PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT', 30000)
            print("✓ Playwright enabled for JavaScript rendering")
        except ImportError:
            print("⚠ Playwright not installed, using standard HTTP requests")
    
    return settings


_SCRAPY_SETTINGS = _build_scrapy_settings()


def _ensure_reactor_running() -> None:
    """Start the Twisted reactor once and keep it running."""
    global _reactor_thread

    if reactor.running:
        return

    with _reactor_thread_lock:
        if reactor.running:
            return

        if _reactor_thread and _reactor_thread.is_alive():
            # Thread exists but reactor isn't marked running yet; fall through to wait.
            pass
        else:
            _reactor_thread = threading.Thread(
                target=reactor.run,
                kwargs={'installSignalHandlers': 0},
                daemon=True,
                name="twisted-reactor",
            )
            _reactor_thread.start()

    # Wait briefly for reactor to start.
    deadline = time.monotonic() + 2.0
    while not reactor.running and time.monotonic() < deadline:
        time.sleep(0.01)


def _get_runner() -> CrawlerRunner:
    """Create/reuse a single CrawlerRunner instance."""
    global _runner
    with _runner_lock:
        if _runner is None:
            _runner = CrawlerRunner(_SCRAPY_SETTINGS)
        return _runner


class ItemsCollectorPipeline:
    """Pipeline to collect scraped items and aggregate data from multiple pages"""
    
    def __init__(self):
        self.items_by_url = {}
    
    @classmethod
    def from_crawler(cls, crawler):
        """Create pipeline instance from crawler"""
        return cls()
    
    def open_spider(self, spider):
        """Called when spider opens"""
        self.items_by_url = {}
    
    def process_item(self, item, spider):
        job_id = getattr(spider, "job_id", None) or "default"
        _ensure_job_structures(job_id)

        website = item.get('website')
        print(f"Pipeline processing item for {website}: {len(item.get('emails', []))} emails, {len(item.get('phones', []))} phones")
        
        if website not in self.items_by_url:
            self.items_by_url[website] = {
                'website': website,
                'emails': set(),
                'phones': set(),
                'vcard_links': set(),
                'vcard_files': [],  # List of dicts
                'pdf_links': set(),
                'image_links': set(),
                'lawyer_profiles': [],  # List of profile dicts
            }
        
        # Aggregate data
        data = self.items_by_url[website]
        data['emails'].update(item.get('emails', []))
        data['phones'].update(item.get('phones', []))
        data['vcard_links'].update(item.get('vcard_links', []))
        data['pdf_links'].update(item.get('pdf_links', []))
        data['image_links'].update(item.get('image_links', []))
        
        # Aggregate vCard files (avoid duplicates by URL)
        vcard_files = item.get('vcard_files', [])
        existing_vcard_urls = {v.get('url') for v in data['vcard_files']}
        for vcard_file in vcard_files:
            if isinstance(vcard_file, dict) and vcard_file.get('url') not in existing_vcard_urls:
                data['vcard_files'].append(vcard_file)
                existing_vcard_urls.add(vcard_file.get('url'))
        
        # Aggregate lawyer profiles (avoid duplicates by profile_url)
        lawyer_profiles = item.get('lawyer_profiles', [])
        existing_profile_urls = {p.get('profile_url') for p in data['lawyer_profiles']}
        for profile in lawyer_profiles:
            if isinstance(profile, dict) and profile.get('profile_url') not in existing_profile_urls:
                data['lawyer_profiles'].append(profile)
                existing_profile_urls.add(profile.get('profile_url'))
        
        print(f"After aggregation for {website}: {len(data['emails'])} emails, {len(data['phones'])} phones, {len(data['vcard_files'])} vCard files, {len(data['lawyer_profiles'])} profiles")

        # Save live intermediate results for real-time UI display
        with live_results_lock:
            if job_id not in live_results_by_job:
                live_results_by_job[job_id] = {}
            # Store a snapshot of current aggregated data
            live_results_by_job[job_id][website] = {
                'website': data['website'],
                'emails': list(data['emails']),
                'phones': list(data['phones']),
                'vcard_links': list(data['vcard_links']),
                'vcard_files': list(data['vcard_files']),
                'pdf_links': list(data['pdf_links']),
                'image_links': list(data['image_links']),
                'lawyer_profiles': list(data['lawyer_profiles']),
            }

        return item
    
    def close_spider(self, spider):
        """Called when spider closes - save aggregated items"""
        job_id = getattr(spider, "job_id", None) or "default"
        _ensure_job_structures(job_id)

        print(f"Pipeline closing spider for job {job_id}. Items collected: {len(self.items_by_url)}")
        
        # Save items
        with items_lock:
            for website, data in self.items_by_url.items():
                final_item = {
                    'website': data['website'],
                    'emails': sorted(list(data['emails'])),
                    'phones': sorted(list(data['phones'])),
                    'vcard_links': sorted(list(data['vcard_links'])),
                    'vcard_files': data['vcard_files'],
                    'pdf_links': sorted(list(data['pdf_links'])),
                    'image_links': sorted(list(data['image_links'])),
                    'lawyer_profiles': data['lawyer_profiles'],
                }
                print(f"Final item for {website}: {len(final_item['emails'])} emails, {len(final_item['phones'])} phones, {len(final_item['vcard_files'])} vCards, {len(final_item['lawyer_profiles'])} profiles")
                scraped_items_by_job[job_id].append(final_item)
        
        # Clear live results
        with live_results_lock:
            live_results_by_job.pop(job_id, None)
        
        # Update progress to completed - CRITICAL for frontend to stop polling
        items_count = len(scraped_items_by_job.get(job_id, []))
        with progress_lock:
            prog = scraping_progress_by_job.get(job_id) or _default_progress_dict()
            # Mark all URLs as completed
            for url in prog.get('urls', []):
                prog.setdefault('url_status', {})[url] = 'completed'
            prog['completed'] = len(prog.get('urls', []))
            # Set final status
            if prog.get('status') != 'cancelled':
                prog['status'] = 'completed'
                prog['message'] = f"Scraping completed! Found data for {items_count} website(s)."
            scraping_progress_by_job[job_id] = prog
        
        print(f">>> JOB {job_id} COMPLETED - Status: completed, Items: {items_count} <<<")


def get_scraping_progress(job_id: str | None = None) -> Dict[str, Any]:
    """Get current scraping progress for a job (defaults to latest)."""
    resolved = _resolve_job_id(job_id)
    _ensure_job_structures(resolved)
    with progress_lock:
        return (scraping_progress_by_job.get(resolved) or _default_progress_dict()).copy()


def get_scraped_results(job_id: str | None = None) -> List[Dict[str, Any]]:
    """Get current scraped results for a job (may be partial if still running)."""
    resolved = _resolve_job_id(job_id)
    _ensure_job_structures(resolved)
    with items_lock:
        return list(scraped_items_by_job.get(resolved, []))


def reset_progress(job_id: str | None = None) -> None:
    """Reset progress tracking. If job_id is None, resets all jobs."""
    global scraping_progress_by_job, scraped_items_by_job, _latest_job_id
    if job_id is None:
        with progress_lock:
            scraping_progress_by_job = {}
        with items_lock:
            scraped_items_by_job = {}
        with _latest_job_id_lock:
            _latest_job_id = None
        return

    resolved = _resolve_job_id(job_id)
    with progress_lock:
        scraping_progress_by_job[resolved] = _default_progress_dict()
    with items_lock:
        scraped_items_by_job[resolved] = []


def update_progress(job_id: str | None = None, status=None, current_url=None, completed=None, message=None, url_status=None):
    """Update scraping progress for a job (defaults to latest)."""
    resolved = _resolve_job_id(job_id)
    _ensure_job_structures(resolved)
    with progress_lock:
        prog = scraping_progress_by_job.get(resolved) or _default_progress_dict()
        prog["job_id"] = resolved
        if status:
            prog['status'] = status
        if current_url:
            prog['current_url'] = current_url
        if completed is not None:
            prog['completed'] = completed
        if message:
            prog['message'] = message
        if url_status:
            url, url_stat = url_status
            prog.setdefault('url_status', {})[url] = url_stat
            # Auto-update completed count
            if url_stat == 'completed':
                prog['completed'] = sum(1 for s in prog.get('url_status', {}).values() if s == 'completed')
        scraping_progress_by_job[resolved] = prog


def is_job_cancelled(job_id: str) -> bool:
    with _job_control_lock:
        return bool(_job_cancelled.get(job_id, False))


def get_scraped_items(job_id: str) -> List[Dict[str, Any]]:
    """Get scraped items for a job. Returns live intermediate results while scraping, final results when done."""
    _ensure_job_structures(job_id)
    
    # Check if we have final results
    with items_lock:
        final_items = list(scraped_items_by_job.get(job_id, []))
    
    if final_items:
        return final_items
    
    # Return live intermediate results while scraping is in progress
    with live_results_lock:
        live_data = live_results_by_job.get(job_id, {})
        return list(live_data.values())


def get_job_urls(job_id: str) -> List[str]:
    _ensure_job_structures(job_id)
    with progress_lock:
        prog = scraping_progress_by_job.get(job_id) or _default_progress_dict()
        return list(prog.get("urls") or [])


def start_scrape_job(urls: List[str]) -> str:
    """
    Start a scrape asynchronously and return a job_id immediately.
    Only one crawl runs at a time (serialized by _crawl_serial_lock).
    """
    global _latest_job_id
    job_id = _new_job_id()
    with _latest_job_id_lock:
        _latest_job_id = job_id

    _ensure_job_structures(job_id)
    with items_lock:
        scraped_items_by_job[job_id] = []
    with _job_control_lock:
        _job_cancelled[job_id] = False

    if not urls:
        update_progress(job_id=job_id, status="error", message="No URLs provided.")
        return job_id

    with progress_lock:
        scraping_progress_by_job[job_id] = {
            'job_id': job_id,
            'status': 'running',
            'total': len(urls),
            'completed': 0,
            'current_url': '',
            'urls': urls,
            'url_status': {url: 'pending' for url in urls},
            'message': f'Starting to scrape {len(urls)} website(s)...',
        }

    def _background_start() -> None:
        with _crawl_serial_lock:
            if is_job_cancelled(job_id):
                update_progress(job_id=job_id, status="cancelled", message="Cancelled before start.")
                return

            update_progress(job_id=job_id, message="Initializing scraper...")
            _ensure_reactor_running()
            runner = _get_runner()

            def _start_in_reactor() -> None:
                try:
                    crawler = runner.create_crawler(WebsiteSpider)
                    with _job_control_lock:
                        _job_crawlers[job_id] = crawler
                    deferred = runner.crawl(crawler, urls=urls, job_id=job_id)
                except Exception as e:  # pragma: no cover
                    update_progress(job_id=job_id, status="error", message=f"Error during crawling: {e}")
                    return

                def _cleanup():
                    with _job_control_lock:
                        _job_crawlers.pop(job_id, None)

                def on_complete(_result):
                    print(f"[on_complete] Job {job_id} finished")
                    items_count = len(get_scraped_items(job_id))
                    update_progress(
                        job_id=job_id,
                        status="completed",
                        message=f"Scraping completed! Found data for {items_count} website(s).",
                    )
                    print(f"[on_complete] Status set to completed for {job_id}")
                    _cleanup()
                    return _result

                def on_error(failure):
                    error_msg = (
                        failure.getErrorMessage()
                        if hasattr(failure, "getErrorMessage")
                        else str(getattr(failure, "value", failure))
                    )
                    # If user hit stop, treat as cancelled.
                    if is_job_cancelled(job_id):
                        update_progress(job_id=job_id, status="cancelled", message="Scraping cancelled.")
                    else:
                        update_progress(job_id=job_id, status="error", message=f"Error during scraping: {error_msg}")
                    _cleanup()
                    return failure

                deferred.addCallbacks(on_complete, on_error)

            reactor.callFromThread(_start_in_reactor)

    t = threading.Thread(target=_background_start, daemon=True, name=f"scrape-job-{job_id}")
    t.start()
    return job_id


def stop_scrape_job(job_id: str) -> bool:
    """Best-effort stop for a running job."""
    _ensure_job_structures(job_id)
    with _job_control_lock:
        _job_cancelled[job_id] = True
        crawler = _job_crawlers.get(job_id)

    update_progress(job_id=job_id, status="cancelled", message="Stopping... (best effort)")

    if crawler is None:
        return True

    def _stop_in_reactor() -> None:
        try:
            # Crawler.stop() is the cleanest if available.
            if hasattr(crawler, "stop"):
                crawler.stop()
            elif hasattr(crawler, "engine") and getattr(crawler, "spider", None) is not None:
                crawler.engine.close_spider(crawler.spider, reason="cancelled")
        except Exception:
            pass

    reactor.callFromThread(_stop_in_reactor)
    return True


def scrape_websites_with_scrapy(urls: List[str], job_id: str | None = None) -> List[Dict[str, Any]]:
    """
    Scrape multiple websites using Scrapy framework.
    Returns a list of dictionaries with scraped data.
    
    Args:
        urls: List of URLs to scrape
        job_id: Optional job ID to isolate progress/results
        
    Returns:
        List of dictionaries containing scraped data
    """
    global _latest_job_id
    resolved_job_id = job_id or _new_job_id()
    with _latest_job_id_lock:
        _latest_job_id = resolved_job_id

    _ensure_job_structures(resolved_job_id)
    with items_lock:
        scraped_items_by_job[resolved_job_id] = []  # Reset for each call
    
    if not urls:
        return []
    
    # Initialize progress tracking
    with progress_lock:
        scraping_progress_by_job[resolved_job_id] = {
            'status': 'running',
            'total': len(urls),
            'completed': 0,
            'current_url': '',
            'urls': urls,
            'url_status': {url: 'pending' for url in urls},
            'message': f'Starting to scrape {len(urls)} website(s)...',
        }
    
    with _crawl_serial_lock:
        update_progress(job_id=resolved_job_id, message='Initializing scraper...')

        _ensure_reactor_running()
        runner = _get_runner()

        crawling_done = threading.Event()
        error_holder: Dict[str, Any] = {}

        def _start_crawl_in_reactor() -> None:
            """Runs inside the reactor thread."""
            try:
                deferred = runner.crawl(WebsiteSpider, urls=urls, job_id=resolved_job_id)
            except Exception as e:  # pragma: no cover
                error_holder["error"] = e
                update_progress(job_id=resolved_job_id, status='error', message=f'Error during crawling: {e}')
                crawling_done.set()
                return

            def on_complete(_result):
                with items_lock:
                    items_count = len(scraped_items_by_job.get(resolved_job_id, []))
                update_progress(
                    job_id=resolved_job_id,
                    status='completed',
                    message=f'Scraping completed! Found data for {items_count} website(s).',
                )
                crawling_done.set()
                return _result

            def on_error(failure):
                # Twisted Failure has getErrorMessage()
                error_msg = (
                    failure.getErrorMessage()
                    if hasattr(failure, "getErrorMessage")
                    else str(getattr(failure, "value", failure))
                )
                update_progress(job_id=resolved_job_id, status='error', message=f'Error during scraping: {error_msg}')
                error_holder["failure"] = failure
                crawling_done.set()
                return failure

            deferred.addCallbacks(on_complete, on_error)

        reactor.callFromThread(_start_crawl_in_reactor)

        # Wait for crawling to complete (with timeout)
        if not crawling_done.wait(timeout=300):  # 5 minute timeout
            update_progress(job_id=resolved_job_id, status='error', message='Scraping timed out (5 minutes).')

        # Return results collected so far
        with items_lock:
            result = list(scraped_items_by_job.get(resolved_job_id, []))
            if not result:
                print("Warning: No items were scraped. This might indicate an issue with the spider or pipeline.")
                update_progress(job_id=resolved_job_id, message='No data collected. Check if URLs are accessible.')

        return result
