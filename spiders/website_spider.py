import re
from urllib.parse import urljoin, urlparse
from scrapy import Spider, Request
from items import WebsiteItem


class WebsiteSpider(Spider):
    name = "website"
    
    def __init__(self, urls=None, *args, **kwargs):
        super(WebsiteSpider, self).__init__(*args, **kwargs)
        if urls:
            self.start_urls = urls if isinstance(urls, list) else [urls]
        else:
            self.start_urls = []
        # Store aggregated data for each base URL
        self.site_data = {}
        # Track which URLs we've processed
        self.processed_urls = set()
    
    def start_requests(self):
        """Start requests for each URL"""
        if not self.start_urls:
            self.logger.warning("No start URLs provided!")
            return
        
        for url in self.start_urls:
            base_url = url
            self.site_data[base_url] = {
                'website': base_url,
                'emails': set(),
                'phones': set(),
                'vcard_links': set(),
                'pdf_links': set(),
                'image_links': set(),
            }
            # Update progress
            try:
                from scrapy_scraper import update_progress
                update_progress(current_url=base_url, url_status=(base_url, 'scraping'), message=f'Scraping {base_url}...')
            except:
                pass
            yield Request(url, callback=self.parse, errback=self.errback, meta={'base_url': base_url, 'depth': 0}, dont_filter=True)
    
    def errback(self, failure):
        """Handle request errors"""
        request = failure.request
        base_url = request.meta.get('base_url', request.url)
        self.logger.error(f"Error scraping {request.url}: {failure.value}")
        # Update progress to show error
        try:
            from scrapy_scraper import update_progress
            update_progress(url_status=(base_url, 'error'), message=f'Error scraping {base_url}: {str(failure.value)}')
        except:
            pass
    
    def parse(self, response):
        """Parse the response and extract data"""
        base_url = response.meta.get('base_url', response.url)
        depth = response.meta.get('depth', 0)
        current_url = response.url
        
        self.logger.info(f"Parsing {current_url} (depth: {depth}, base: {base_url})")
        
        # Initialize data structure if needed
        if base_url not in self.site_data:
            self.site_data[base_url] = {
                'website': base_url,
                'emails': set(),
                'phones': set(),
                'vcard_links': set(),
                'pdf_links': set(),
                'image_links': set(),
            }
        
        data = self.site_data[base_url]
        
        # Check if we got a valid response
        if response.status != 200:
            self.logger.warning(f"Got status {response.status} for {current_url}")
            return
        
        # Extract emails from page text
        email_pattern = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
        emails = email_pattern.findall(response.text)
        if emails:
            self.logger.info(f"Found {len(emails)} emails on {current_url}")
        data['emails'].update(emails)
        
        # Extract phones - multiple patterns for better coverage
        phone_patterns = [
            r'\+?1?[-.\s]?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}',  # US: (123) 456-7890
            r'\+?\d{1,4}[-.\s]?\d{1,4}[-.\s]?\d{1,4}[-.\s]?\d{1,9}',  # International
            r'\+?\d[\d\-\s()]{7,}\d',  # General pattern
            r'\(\d{3}\)\s?\d{3}[-.\s]?\d{4}',  # (123) 456-7890
        ]
        for pattern in phone_patterns:
            phones = re.findall(pattern, response.text)
            data['phones'].update(phone.strip() for phone in phones)
        
        # Extract all links from the page
        links = response.css('a::attr(href)').getall()
        
        # Extract vcard links
        for link in links:
            if link:
                full_url = urljoin(response.url, link)
                link_lower = link.lower()
                if link_lower.endswith('.vcf') or 'vcard' in link_lower or 'contact.vcf' in link_lower:
                    data['vcard_links'].add(full_url)
        
        # Extract PDF links
        for link in links:
            if link and link.lower().endswith('.pdf'):
                full_url = urljoin(response.url, link)
                data['pdf_links'].add(full_url)
        
        # Extract image links from img tags
        for img in response.css('img::attr(src)').getall():
            if img:
                full_url = urljoin(response.url, img)
                data['image_links'].add(full_url)
        
        # Extract images from CSS background images
        bg_images = re.findall(r'url\(["\']?([^"\')]+)["\']?\)', response.text)
        for bg_img in bg_images:
            if bg_img:
                full_url = urljoin(response.url, bg_img)
                if any(bg_img.lower().endswith(ext) for ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.svg', '.ico']):
                    data['image_links'].add(full_url)
        
        # Extract images from data attributes
        for img_data in response.css('img::attr(data-src)').getall():
            if img_data:
                full_url = urljoin(response.url, img_data)
                data['image_links'].add(full_url)
        
        # Follow links to find more resources (limit depth to 2 levels)
        if depth < 2:
            base_domain = urlparse(base_url).netloc
            current_domain = urlparse(response.url).netloc
            
            # Only follow links on the main page or important pages
            if depth == 0 or any(keyword in current_url.lower() for keyword in ['contact', 'about', 'team', 'resources']):
                for link in links:
                    if link:
                        full_url = urljoin(response.url, link)
                        link_domain = urlparse(full_url).netloc
                        link_lower = full_url.lower()
                        
                        # Only follow links within the same domain
                        if link_domain == base_domain and full_url not in self.processed_urls:
                            # Follow links that might contain useful info
                            if (depth == 0 or 
                                any(keyword in link_lower for keyword in ['contact', 'about', 'team', 'pdf', 'vcard', 'download', 'resources', 'media', 'gallery'])):
                                self.processed_urls.add(full_url)
                                yield Request(
                                    full_url,
                                    callback=self.parse,
                                    meta={'base_url': base_url, 'depth': depth + 1},
                                    dont_filter=False
                                )
        
        # Update progress when starting to scrape a base URL
        if depth == 0 and current_url == base_url:
            # Import here to avoid circular import
            try:
                from scrapy_scraper import update_progress
                update_progress(current_url=base_url, url_status=(base_url, 'scraping'), message=f'Scraping {base_url}...')
            except:
                pass
        
        # Always yield item so pipeline can aggregate (yield after each page to update progress)
        item = WebsiteItem()
        item['website'] = base_url  # Always use base URL as identifier
        item['emails'] = list(data['emails'])  # Current state of emails
        item['phones'] = list(data['phones'])  # Current state of phones
        item['vcard_links'] = list(data['vcard_links'])  # Current state of vcards
        item['pdf_links'] = list(data['pdf_links'])  # Current state of PDFs
        item['image_links'] = list(data['image_links'])  # Current state of images
        
        # Log what we found
        if depth == 0:
            self.logger.info(f"Yielding item for {base_url}: {len(item['emails'])} emails, {len(item['phones'])} phones, {len(item['pdf_links'])} PDFs, {len(item['image_links'])} images")
        
        yield item
