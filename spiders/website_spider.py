import re
import base64
from urllib.parse import urljoin, urlparse
from scrapy import Spider, Request
from items import WebsiteItem, LawyerProfileItem


class WebsiteSpider(Spider):
    name = "website"
    
    # Generic email patterns to filter out
    GENERIC_EMAIL_PATTERNS = [
        r'info@', r'contact@', r'admin@', r'webmaster@', r'noreply@',
        r'support@', r'sales@', r'marketing@', r'newsletter@', r'hello@',
        r'general@', r'office@', r'firm@', r'lawfirm@', r'legal@'
    ]
    
    # Keywords that indicate a lawyer profile page
    PROFILE_KEYWORDS = [
        'attorney', 'lawyer', 'profile', 'bio', 'biography', 'team-member',
        'our-people', 'attorneys', 'lawyers', 'partner', 'associate',
        'counsel', 'staff', 'professional', 'member'
    ]
    
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
        # Track downloaded vCard files
        self.vcard_files = {}  # url -> {'content': base64, 'size': bytes}
    
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
                'vcard_files': [],  # List of dicts with url, content (base64), size
                'pdf_links': set(),
                'image_links': set(),
                'lawyer_profiles': [],  # List of lawyer profile data
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
    
    def _is_lawyer_profile_page(self, url, response):
        """Check if current page is a lawyer profile page"""
        url_lower = url.lower()
        text_lower = response.text.lower()
        
        # Check URL for profile keywords
        if any(keyword in url_lower for keyword in self.PROFILE_KEYWORDS):
            return True
        
        # Check page content for profile indicators
        profile_indicators = [
            'attorney profile', 'lawyer profile', 'biography', 'bio',
            'practice areas', 'bar admission', 'education', 'experience'
        ]
        if any(indicator in text_lower for indicator in profile_indicators):
            # Make sure it's not just a listing page
            if 'attorney' in text_lower or 'lawyer' in text_lower:
                # Check for individual name patterns (likely a profile)
                name_patterns = [
                    r'<h[1-3][^>]*>([A-Z][a-z]+ [A-Z][a-z]+)',  # Name in heading
                    r'class="[^"]*name[^"]*"[^>]*>([A-Z][a-z]+ [A-Z][a-z]+)',  # Name in class
                ]
                for pattern in name_patterns:
                    if re.search(pattern, response.text):
                        return True
        
        return False
    
    def _is_generic_email(self, email):
        """Check if email is generic (not lawyer-specific)"""
        email_lower = email.lower()
        return any(pattern.replace('@', '') in email_lower for pattern in self.GENERIC_EMAIL_PATTERNS)
    
    def _extract_lawyer_profile(self, response, base_url):
        """Extract lawyer-specific data from a profile page"""
        profile_data = {
            'website': base_url,
            'profile_url': response.url,
            'lawyer_name': '',
            'lawyer_email': '',
            'lawyer_phone': '',
            'profile_images': [],
            'vcard_content': '',
        }
        
        # Extract name from common patterns
        name_selectors = [
            'h1', 'h2', '.name', '.attorney-name', '.lawyer-name',
            '[class*="name"]', '[id*="name"]', '.profile-name'
        ]
        for selector in name_selectors:
            name = response.css(f'{selector}::text').get()
            if name:
                name = name.strip()
                # Check if it looks like a person's name (2-4 words, capitalized)
                words = name.split()
                if 2 <= len(words) <= 4 and all(w[0].isupper() if w else False for w in words):
                    profile_data['lawyer_name'] = name
                    break
        
        # Extract emails - filter out generic ones
        email_pattern = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
        emails = email_pattern.findall(response.text)
        for email in emails:
            if not self._is_generic_email(email):
                profile_data['lawyer_email'] = email
                break  # Take first non-generic email
        
        # Extract phones - prefer direct contact info
        phone_patterns = [
            r'\+?1?[-.\s]?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}',
            r'\+?\d{1,4}[-.\s]?\d{1,4}[-.\s]?\d{1,4}[-.\s]?\d{1,9}',
        ]
        for pattern in phone_patterns:
            phones = re.findall(pattern, response.text)
            if phones:
                profile_data['lawyer_phone'] = phones[0].strip()
                break
        
        # Extract profile images
        img_selectors = [
            'img.profile-photo', 'img.attorney-photo', 'img.lawyer-photo',
            '.profile-image img', '.attorney-image img', '[class*="photo"] img'
        ]
        for selector in img_selectors:
            imgs = response.css(f'{selector}::attr(src)').getall()
            for img in imgs:
                if img:
                    full_url = urljoin(response.url, img)
                    profile_data['profile_images'].append(full_url)
        
        # Also check data-src for lazy-loaded images
        for img_data in response.css('img::attr(data-src)').getall():
            if img_data:
                full_url = urljoin(response.url, img_data)
                if full_url not in profile_data['profile_images']:
                    profile_data['profile_images'].append(full_url)
        
        # Check for vCard on profile page
        vcard_links = response.css('a[href*=".vcf"], a[href*="vcard"]::attr(href)').getall()
        for vcard_link in vcard_links:
            if vcard_link:
                full_url = urljoin(response.url, vcard_link)
                # Will be downloaded in parse_vcard
        
        return profile_data
    
    def parse_vcard(self, response):
        """Parse downloaded vCard file"""
        base_url = response.meta.get('base_url', '')
        vcard_url = response.url
        
        try:
            # Read vCard content
            if hasattr(response, 'body'):
                vcard_content = response.body
            else:
                vcard_content = response.text.encode('utf-8')
            
            # Base64 encode for CSV storage
            vcard_base64 = base64.b64encode(vcard_content).decode('utf-8')
            vcard_size = len(vcard_content)
            
            # Store vCard file info
            vcard_info = {
                'url': vcard_url,
                'content': vcard_base64,
                'size': vcard_size
            }
            
            if base_url in self.site_data:
                # Check if we already have this vCard
                existing_urls = [v['url'] for v in self.site_data[base_url].get('vcard_files', [])]
                if vcard_url not in existing_urls:
                    self.site_data[base_url]['vcard_files'].append(vcard_info)
                    self.logger.info(f"Downloaded vCard from {vcard_url} ({vcard_size} bytes)")
            
            # If this vCard was found on a profile page, add to profile
            profile_url = response.meta.get('profile_url')
            if profile_url and base_url in self.site_data:
                for profile in self.site_data[base_url]['lawyer_profiles']:
                    if profile.get('profile_url') == profile_url:
                        profile['vcard_content'] = vcard_base64
                        break
            
        except Exception as e:
            self.logger.error(f"Error processing vCard {vcard_url}: {e}")
    
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
                'vcard_files': [],
                'pdf_links': set(),
                'image_links': set(),
                'lawyer_profiles': [],
            }
        
        data = self.site_data[base_url]
        
        # Check if we got a valid response
        if response.status != 200:
            self.logger.warning(f"Got status {response.status} for {current_url}")
            return
        
        # Check if this is a lawyer profile page
        is_profile_page = self._is_lawyer_profile_page(current_url, response)
        
        if is_profile_page:
            # Extract lawyer-specific profile data
            profile_data = self._extract_lawyer_profile(response, base_url)
            if profile_data.get('lawyer_name') or profile_data.get('lawyer_email') or profile_data.get('lawyer_phone'):
                # Check if we already have this profile
                existing_profiles = [p.get('profile_url') for p in data['lawyer_profiles']]
                if current_url not in existing_profiles:
                    data['lawyer_profiles'].append(profile_data)
                    self.logger.info(f"Found lawyer profile: {profile_data.get('lawyer_name', 'Unknown')} at {current_url}")
        else:
            # Extract general firm data
            email_pattern = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
            emails = email_pattern.findall(response.text)
            if emails:
                self.logger.info(f"Found {len(emails)} emails on {current_url}")
            data['emails'].update(emails)
            
            # Extract phones
            phone_patterns = [
                r'\+?1?[-.\s]?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}',
                r'\+?\d{1,4}[-.\s]?\d{1,4}[-.\s]?\d{1,4}[-.\s]?\d{1,9}',
                r'\+?\d[\d\-\s()]{7,}\d',
                r'\(\d{3}\)\s?\d{3}[-.\s]?\d{4}',
            ]
            for pattern in phone_patterns:
                phones = re.findall(pattern, response.text)
                data['phones'].update(phone.strip() for phone in phones)
        
        # Extract all links from the page
        links = response.css('a::attr(href)').getall()
        
        # Extract and download vcard links
        for link in links:
            if link:
                full_url = urljoin(response.url, link)
                link_lower = link.lower()
                if link_lower.endswith('.vcf') or 'vcard' in link_lower or 'contact.vcf' in link_lower:
                    data['vcard_links'].add(full_url)
                    # Download the vCard file
                    if full_url not in self.processed_urls:
                        self.processed_urls.add(full_url)
                        yield Request(
                            full_url,
                            callback=self.parse_vcard,
                            meta={'base_url': base_url, 'profile_url': current_url if is_profile_page else None},
                            dont_filter=True
                        )
        
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
        
        # Follow links to find more resources (increased depth to 3 for better profile discovery)
        if depth < 3:
            base_domain = urlparse(base_url).netloc
            current_domain = urlparse(response.url).netloc
            
            # Follow links more aggressively for profile pages
            if depth == 0 or any(keyword in current_url.lower() for keyword in ['contact', 'about', 'team', 'resources', 'attorney', 'lawyer', 'profile', 'bio', 'people']):
                for link in links:
                    if link:
                        full_url = urljoin(response.url, link)
                        link_domain = urlparse(full_url).netloc
                        link_lower = full_url.lower()
                        
                        # Only follow links within the same domain
                        if link_domain == base_domain and full_url not in self.processed_urls:
                            # Prioritize profile-related links
                            profile_priority = any(keyword in link_lower for keyword in self.PROFILE_KEYWORDS)
                            if (depth == 0 or profile_priority or 
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
            try:
                from scrapy_scraper import update_progress
                update_progress(current_url=base_url, url_status=(base_url, 'scraping'), message=f'Scraping {base_url}...')
            except:
                pass
        
        # Always yield item so pipeline can aggregate
        item = WebsiteItem()
        item['website'] = base_url
        item['emails'] = list(data['emails'])
        item['phones'] = list(data['phones'])
        item['vcard_links'] = list(data['vcard_links'])
        item['vcard_files'] = data['vcard_files']  # Already a list of dicts
        item['pdf_links'] = list(data['pdf_links'])
        item['image_links'] = list(data['image_links'])
        item['lawyer_profiles'] = data['lawyer_profiles']
        
        # Log what we found
        if depth == 0:
            self.logger.info(f"Yielding item for {base_url}: {len(item['emails'])} emails, {len(item['phones'])} phones, {len(item['pdf_links'])} PDFs, {len(item['image_links'])} images, {len(item['lawyer_profiles'])} profiles, {len(item['vcard_files'])} vCard files")
        
        yield item
