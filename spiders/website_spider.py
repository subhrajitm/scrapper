import re
import base64
import os
from urllib.parse import urljoin, urlparse
from scrapy import Spider, Request
from scrapy.exceptions import CloseSpider
from items import WebsiteItem


# Check if Playwright is available for JavaScript rendering
try:
    import scrapy_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False


class WebsiteSpider(Spider):
    name = "website"

    # Safety limit: don’t base64-encode extremely large responses.
    MAX_VCARD_BYTES = 250_000  # 250KB

    # Crawl safety limits to prevent “infinite” site walks on large firms.
    MAX_TOTAL_PAGES = 400  # across the whole spider run
    MAX_PAGES_PER_SITE = 200  # per base_url
    
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
    
    # Playwright settings for JavaScript rendering
    use_playwright = os.getenv("USE_PLAYWRIGHT", "false").lower() == "true"
    
    def __init__(self, urls=None, job_id=None, use_js=None, *args, **kwargs):
        super(WebsiteSpider, self).__init__(*args, **kwargs)
        self.job_id = job_id or "default"
        if urls:
            self.start_urls = urls if isinstance(urls, list) else [urls]
        else:
            self.start_urls = []
        # Store aggregated data for each base URL
        self.site_data = {}
        # Track which URLs we've processed
        self.processed_urls = set()
        
        # Enable Playwright if requested and available
        if use_js is not None:
            self.use_playwright = use_js and PLAYWRIGHT_AVAILABLE
        elif self.use_playwright:
            self.use_playwright = PLAYWRIGHT_AVAILABLE
            
        if self.use_playwright:
            self.logger.info("Playwright enabled for JavaScript rendering")
    
    def _get_playwright_meta(self) -> dict:
        """Get Playwright meta options for JavaScript rendering."""
        if not self.use_playwright or not PLAYWRIGHT_AVAILABLE:
            return {}
        return {
            "playwright": True,
            "playwright_include_page": False,
            "playwright_page_methods": [
                {"method": "wait_for_load_state", "args": ["networkidle"]},
            ],
        }

    def _visible_text(self, response) -> str:
        """
        Extract visible text from the page (excluding scripts/styles).
        This reduces false positives from embedded JS/CSS and asset URLs.
        """
        texts = response.xpath(
            "//body//text()[not(ancestor::script) and not(ancestor::style) and not(ancestor::noscript)]"
        ).getall()
        return " ".join(t.strip() for t in texts if t and t.strip())

    def _normalize_phone(self, raw: str, *, from_tel: bool = False) -> str | None:
        """Normalize and validate phone-ish strings into a compact form."""
        if not raw:
            return None

        raw = raw.strip()
        raw_no_tel = raw[4:] if raw.lower().startswith("tel:") else raw

        # Reject bare long digit strings coming from page text; allow for tel: links.
        raw_digits_only = re.sub(r"\D", "", raw_no_tel)
        if not raw_digits_only:
            return None

        # Most real-world phone numbers are 10-15 digits (E.164 max is 15).
        if len(raw_digits_only) < 10 or len(raw_digits_only) > 15:
            return None

        # Filter out obvious timestamps / IDs (common on websites).
        if not from_tel and raw_no_tel.lstrip("+").isdigit():
            return None

        # Filter out nonsense repeats.
        if raw_digits_only == raw_digits_only[0] * len(raw_digits_only):
            return None

        prefix_plus = raw_no_tel.strip().startswith("+")
        return ("+" if prefix_plus else "") + raw_digits_only
    
    def start_requests(self):
        """Start requests for each URL"""
        if not self.start_urls:
            self.logger.warning("No start URLs provided!")
            return

        # Respect stop/cancel requests.
        try:
            from scrapy_scraper import is_job_cancelled
            if is_job_cancelled(self.job_id):
                raise CloseSpider("cancelled")
        except Exception:
            pass
        
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
                'pages_seen': 0,
            }
            # Update progress
            try:
                from scrapy_scraper import update_progress
                update_progress(job_id=self.job_id, current_url=base_url, url_status=(base_url, 'scraping'), message=f'Scraping {base_url}...')
            except:
                pass
            # Priority 0 = highest; all base URLs get processed first in parallel
            # Include Playwright meta if enabled for JavaScript rendering
            meta = {'base_url': base_url, 'depth': 0}
            meta.update(self._get_playwright_meta())
            yield Request(url, callback=self.parse, errback=self.errback, meta=meta, dont_filter=True, priority=0)
    
    def errback(self, failure):
        """Handle request errors"""
        request = failure.request
        base_url = request.meta.get('base_url', request.url)
        self.logger.error(f"Error scraping {request.url}: {failure.value}")
        # Update progress to show error
        try:
            from scrapy_scraper import update_progress
            update_progress(job_id=self.job_id, url_status=(base_url, 'error'), message=f'Error scraping {base_url}: {str(failure.value)}')
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
        visible_text = self._visible_text(response)
        emails = email_pattern.findall(visible_text)
        for email in emails:
            if not self._is_generic_email(email):
                profile_data['lawyer_email'] = email
                break  # Take first non-generic email
        
        # Extract phones - prefer tel: links, then visible-text patterns.
        tel_hrefs = response.css('a[href^="tel:"]::attr(href)').getall()
        for href in tel_hrefs:
            normalized = self._normalize_phone(href, from_tel=True)
            if normalized:
                profile_data['lawyer_phone'] = normalized
                break

        if not profile_data['lawyer_phone']:
            phone_candidates = re.findall(r"\+?\d[\d\s().-]{7,}\d", visible_text)
            for cand in phone_candidates:
                normalized = self._normalize_phone(cand)
                if normalized:
                    profile_data['lawyer_phone'] = normalized
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

            if not vcard_content:
                return

            # Size guard (avoid storing huge/bogus responses)
            vcard_size = len(vcard_content)
            if vcard_size > self.MAX_VCARD_BYTES:
                self.logger.warning(f"Skipping oversized vCard from {vcard_url} ({vcard_size} bytes)")
                return

            # Basic content validation (avoid HTML error pages)
            head = vcard_content[:200].lstrip()
            if b"BEGIN:VCARD" not in vcard_content[:4096] and head.startswith(b"<"):
                self.logger.warning(f"Skipping non-vCard content from {vcard_url} (looks like HTML)")
                return
            if b"BEGIN:VCARD" not in vcard_content[:4096]:
                # Not strictly required, but reduces junk dramatically.
                self.logger.warning(f"Skipping non-vCard content from {vcard_url} (missing BEGIN:VCARD)")
                return
            
            # Base64 encode for CSV storage
            vcard_base64 = base64.b64encode(vcard_content).decode('utf-8')
            
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

        # Emit an updated item so the pipeline captures downloaded vCards even
        # if no further HTML pages are parsed for this base_url.
        data = self.site_data.get(base_url)
        if data:
            item = WebsiteItem()
            item["website"] = base_url
            item["emails"] = list(data.get("emails", []))
            item["phones"] = list(data.get("phones", []))
            item["vcard_links"] = list(data.get("vcard_links", []))
            item["vcard_files"] = data.get("vcard_files", [])
            item["pdf_links"] = list(data.get("pdf_links", []))
            item["image_links"] = list(data.get("image_links", []))
            item["lawyer_profiles"] = data.get("lawyer_profiles", [])
            yield item
    
    def parse(self, response):
        """Parse the response and extract data"""
        base_url = response.meta.get('base_url', response.url)
        depth = response.meta.get('depth', 0)
        current_url = response.url

        # Respect stop/cancel requests.
        try:
            from scrapy_scraper import is_job_cancelled
            if is_job_cancelled(self.job_id):
                raise CloseSpider("cancelled")
        except Exception:
            pass
        
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
                'pages_seen': 0,
            }
        
        data = self.site_data[base_url]

        # Track pages visited and enforce safety limits.
        data['pages_seen'] = int(data.get('pages_seen') or 0) + 1
        total_pages_seen = len(self.processed_urls) if self.processed_urls else 0
        cap_reached = (
            data['pages_seen'] >= self.MAX_PAGES_PER_SITE
            or total_pages_seen >= self.MAX_TOTAL_PAGES
        )
        if cap_reached:
            # Don’t stop the spider abruptly; just stop enqueueing new requests.
            data['cap_reached'] = True

        # Periodically update progress with activity so the UI doesn't look stuck.
        if data['pages_seen'] % 10 == 0:
            try:
                from scrapy_scraper import update_progress
                update_progress(
                    job_id=self.job_id,
                    message=(
                        f"Visited {data['pages_seen']} pages on {base_url}. "
                        f"Profiles: {len(data.get('lawyer_profiles', []))}. "
                        f"vCards: {len(data.get('vcard_files', []))}."
                    ),
                )
            except Exception:
                pass
        
        # Check if we got a valid response
        if response.status != 200:
            self.logger.warning(f"Got status {response.status} for {current_url}")
            return
        
        # Check if this is a lawyer profile page
        is_profile_page = self._is_lawyer_profile_page(current_url, response)
        
        # Always extract general emails and phones from all pages
        email_pattern = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
        visible_text = self._visible_text(response)
        
        # Debug: log visible text length
        self.logger.debug(f"Visible text length on {current_url}: {len(visible_text)} chars")
        
        emails = email_pattern.findall(visible_text)
        self.logger.debug(f"Raw emails found on {current_url}: {emails[:5]}")  # First 5
        
        # Filter out generic emails
        filtered_emails = [e for e in emails if not self._is_generic_email(e)]
        if filtered_emails:
            self.logger.info(f"Found {len(filtered_emails)} non-generic emails on {current_url}: {filtered_emails[:3]}")
        data['emails'].update(filtered_emails)
        
        # Extract phones from tel: links
        tel_hrefs = response.css('a[href^="tel:"]::attr(href)').getall()
        for href in tel_hrefs:
            normalized = self._normalize_phone(href, from_tel=True)
            if normalized:
                data["phones"].add(normalized)

        # Extract phones from visible text
        phone_candidates = re.findall(r"\+?\d[\d\s().-]{7,}\d", visible_text)
        for cand in phone_candidates:
            normalized = self._normalize_phone(cand)
            if normalized:
                data["phones"].add(normalized)
        
        # Additionally extract lawyer profile if this is a profile page
        if is_profile_page:
            profile_data = self._extract_lawyer_profile(response, base_url)
            if profile_data.get('lawyer_name') or profile_data.get('lawyer_email') or profile_data.get('lawyer_phone'):
                # Check if we already have this profile
                existing_profiles = [p.get('profile_url') for p in data['lawyer_profiles']]
                if current_url not in existing_profiles:
                    data['lawyer_profiles'].append(profile_data)
                    self.logger.info(f"Found lawyer profile: {profile_data.get('lawyer_name', 'Unknown')} at {current_url}")
                    # Also add profile email/phone to site-level data
                    if profile_data.get('lawyer_email'):
                        data['emails'].add(profile_data['lawyer_email'])
                    if profile_data.get('lawyer_phone'):
                        data['phones'].add(profile_data['lawyer_phone'])
        
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
                        meta = {'base_url': base_url, 'profile_url': current_url if is_profile_page else None}
                        # Don't use Playwright for vCard downloads (binary files)
                        yield Request(
                            full_url,
                            callback=self.parse_vcard,
                            meta=meta,
                            dont_filter=True,
                            priority=depth + 2,  # Lower priority than page scraping
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
        if depth < 3 and not data.get('cap_reached'):
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
                                # Priority increases with depth so base URLs of all sites finish first
                                meta = {'base_url': base_url, 'depth': depth + 1}
                                meta.update(self._get_playwright_meta())  # Add Playwright for JS rendering
                                yield Request(
                                    full_url,
                                    callback=self.parse,
                                    meta=meta,
                                    dont_filter=False,
                                    priority=depth + 1,
                                )
        
        # Update progress when starting to scrape a base URL
        if depth == 0 and current_url == base_url:
            try:
                from scrapy_scraper import update_progress
                update_progress(job_id=self.job_id, current_url=base_url, url_status=(base_url, 'scraping'), message=f'Scraping {base_url}...')
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
