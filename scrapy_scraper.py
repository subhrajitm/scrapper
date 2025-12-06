"""
Helper module for running Scrapy spiders from Flask.
This module provides a synchronous interface to Scrapy's async framework.
"""
import threading
from typing import List, Dict, Any
from scrapy.crawler import CrawlerRunner
from scrapy.settings import Settings
from twisted.internet import reactor
from spiders.website_spider import WebsiteSpider


# Global storage for scraped items
scraped_items: List[Dict[str, Any]] = []
items_lock = threading.Lock()

# Global progress tracking
scraping_progress: Dict[str, Any] = {
    'status': 'idle',  # idle, running, completed, error
    'total': 0,
    'completed': 0,
    'current_url': '',
    'urls': [],
    'url_status': {},  # url -> status (pending, scraping, completed, error)
    'message': '',
}
progress_lock = threading.Lock()


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
        website = item.get('website')
        print(f"Pipeline processing item for {website}: {len(item.get('emails', []))} emails, {len(item.get('phones', []))} phones")
        
        if website not in self.items_by_url:
            self.items_by_url[website] = {
                'website': website,
                'emails': set(),
                'phones': set(),
                'vcard_links': set(),
                'pdf_links': set(),
                'image_links': set(),
            }
        
        # Aggregate data
        data = self.items_by_url[website]
        data['emails'].update(item.get('emails', []))
        data['phones'].update(item.get('phones', []))
        data['vcard_links'].update(item.get('vcard_links', []))
        data['pdf_links'].update(item.get('pdf_links', []))
        data['image_links'].update(item.get('image_links', []))
        
        print(f"After aggregation for {website}: {len(data['emails'])} emails, {len(data['phones'])} phones")
        return item
    
    def close_spider(self, spider):
        """Called when spider closes - save aggregated items"""
        global scraped_items
        print(f"Pipeline closing spider. Items collected: {len(self.items_by_url)}")
        with items_lock:
            for website, data in self.items_by_url.items():
                final_item = {
                    'website': data['website'],
                    'emails': sorted(list(data['emails'])),
                    'phones': sorted(list(data['phones'])),
                    'vcard_links': sorted(list(data['vcard_links'])),
                    'pdf_links': sorted(list(data['pdf_links'])),
                    'image_links': sorted(list(data['image_links'])),
                }
                print(f"Final item for {website}: {len(final_item['emails'])} emails, {len(final_item['phones'])} phones")
                scraped_items.append(final_item)
                # Update progress - mark URL as completed
                update_progress(url_status=(website, 'completed'))
            
            # Update completed count
            with progress_lock:
                completed_count = sum(1 for status in scraping_progress['url_status'].values() if status == 'completed')
                scraping_progress['completed'] = completed_count
            print(f"Total items in scraped_items: {len(scraped_items)}")


def get_scraping_progress() -> Dict[str, Any]:
    """Get current scraping progress"""
    with progress_lock:
        return scraping_progress.copy()


def reset_progress():
    """Reset progress tracking"""
    global scraping_progress
    with progress_lock:
        scraping_progress = {
            'status': 'idle',
            'total': 0,
            'completed': 0,
            'current_url': '',
            'urls': [],
            'url_status': {},
            'message': '',
        }


def update_progress(status=None, current_url=None, completed=None, message=None, url_status=None):
    """Update scraping progress"""
    global scraping_progress
    with progress_lock:
        if status:
            scraping_progress['status'] = status
        if current_url:
            scraping_progress['current_url'] = current_url
        if completed is not None:
            scraping_progress['completed'] = completed
        if message:
            scraping_progress['message'] = message
        if url_status:
            url, url_stat = url_status
            scraping_progress['url_status'][url] = url_stat
            # Auto-update completed count
            if url_stat == 'completed':
                scraping_progress['completed'] = sum(1 for s in scraping_progress['url_status'].values() if s == 'completed')


def scrape_websites_with_scrapy(urls: List[str]) -> List[Dict[str, Any]]:
    """
    Scrape multiple websites using Scrapy framework.
    Returns a list of dictionaries with scraped data.
    
    Args:
        urls: List of URLs to scrape
        
    Returns:
        List of dictionaries containing scraped data
    """
    global scraped_items
    scraped_items = []  # Reset for each call
    
    if not urls:
        return []
    
    # Initialize progress tracking
    with progress_lock:
        scraping_progress['status'] = 'running'
        scraping_progress['total'] = len(urls)
        scraping_progress['completed'] = 0
        scraping_progress['urls'] = urls
        scraping_progress['url_status'] = {url: 'pending' for url in urls}
        scraping_progress['message'] = f'Starting to scrape {len(urls)} website(s)...'
    
    # Configure Scrapy settings manually
    settings = Settings()
    settings.set('USER_AGENT', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')
    settings.set('ROBOTSTXT_OBEY', False)  # Set to False to scrape more aggressively
    settings.set('DOWNLOAD_DELAY', 0.5)  # Reduced delay for faster scraping
    settings.set('CONCURRENT_REQUESTS', 16)  # More concurrent requests
    settings.set('CONCURRENT_REQUESTS_PER_DOMAIN', 8)
    settings.set('AUTOTHROTTLE_ENABLED', True)
    settings.set('AUTOTHROTTLE_START_DELAY', 0.5)
    settings.set('AUTOTHROTTLE_MAX_DELAY', 5)
    settings.set('LOG_LEVEL', 'INFO')  # Show info for debugging
    settings.set('ITEM_PIPELINES', {
        'scrapy_scraper.ItemsCollectorPipeline': 300,
    })
    settings.set('DOWNLOAD_TIMEOUT', 30)
    settings.set('RETRY_ENABLED', True)
    settings.set('RETRY_TIMES', 2)
    settings.set('REQUEST_FINGERPRINTER_IMPLEMENTATION', '2.7')  # Fix deprecation warning
    settings.set('HTTPERROR_ALLOWED_CODES', [403, 404])  # Allow some error codes
    settings.set('DEFAULT_REQUEST_HEADERS', {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en',
    })
    
    # Flag to track when crawling is done
    crawling_done = threading.Event()
    
    def crawl():
        """Run the spider in a separate thread using CrawlerRunner"""
        try:
            update_progress(message='Initializing scraper...')
            
            # Check if reactor is already running
            if reactor.running:
                print("Reactor is already running, stopping it first...")
                reactor.stop()
                import time
                time.sleep(0.5)
            
            runner = CrawlerRunner(settings)
            deferred = runner.crawl(WebsiteSpider, urls=urls)
            
            def on_complete(result):
                with items_lock:
                    items_count = len(scraped_items)
                update_progress(status='completed', message=f'Scraping completed! Found data for {items_count} website(s).')
                if reactor.running:
                    reactor.stop()
            
            def on_error(failure):
                error_msg = str(failure.value) if hasattr(failure, 'value') else str(failure)
                update_progress(status='error', message=f'Error during scraping: {error_msg}')
                print(f"Scraping error: {error_msg}")
                if reactor.running:
                    reactor.stop()
            
            deferred.addCallbacks(on_complete, on_error)
            
            # Don't install signal handlers - we're in a thread
            # Use callFromThread to ensure we're in the right thread context
            from twisted.internet import threads
            if not reactor.running:
                reactor.run(installSignalHandlers=0)
            else:
                print("Reactor already running, this might cause issues")
                # Try to stop and restart
                try:
                    reactor.stop()
                    import time
                    time.sleep(0.1)
                    reactor.run(installSignalHandlers=0)
                except:
                    pass
        except Exception as e:
            error_msg = str(e)
            update_progress(status='error', message=f'Error during crawling: {error_msg}')
            print(f"Exception during crawling: {error_msg}")
            import traceback
            traceback.print_exc()
            if reactor.running:
                try:
                    reactor.stop()
                except:
                    pass
        finally:
            crawling_done.set()
    
    # Run crawler in a separate thread
    crawler_thread = threading.Thread(target=crawl)
    crawler_thread.daemon = True
    crawler_thread.start()
    
    # Wait for crawling to complete (with timeout)
    crawling_done.wait(timeout=300)  # 5 minute timeout for more URLs
    
    # Check if we got any results
    with items_lock:
        result = scraped_items.copy()
        if not result:
            print("Warning: No items were scraped. This might indicate an issue with the spider or pipeline.")
            # Try to get items from pipeline if available
            update_progress(message='No data collected. Check if URLs are accessible.')
    
    return result
