"""
Module for importing URLs from external lists (WSJ, SuperLawyers, etc.)
"""
import re
from typing import List, Tuple
from urllib.parse import urljoin, urlparse, urlunparse, parse_qsl, urlencode
import requests
from bs4 import BeautifulSoup


_DROP_QUERY_KEYS_PREFIX = ("utm_",)
_DROP_QUERY_KEYS_EXACT = {
    "gclid",
    "fbclid",
    "msclkid",
    "igshid",
    "mc_cid",
    "mc_eid",
}


def normalize_url(url: str) -> str | None:
    """
    Normalize a URL for deduping:
    - ensure scheme (default https)
    - lowercase scheme/host
    - strip fragment
    - remove common tracking query params
    """
    if not url:
        return None
    url = url.strip().rstrip('.,;:')
    if not url:
        return None

    # If it's a bare domain, add scheme.
    if not url.startswith(("http://", "https://")):
        url = f"https://{url}"

    try:
        parsed = urlparse(url)
    except Exception:
        return None

    if not parsed.netloc:
        return None

    scheme = (parsed.scheme or "https").lower()
    netloc = parsed.netloc.lower()

    # Normalize www.
    if netloc.startswith("www."):
        netloc = netloc[4:]

    # Drop fragment.
    fragment = ""

    # Filter query params.
    query_pairs = parse_qsl(parsed.query, keep_blank_values=True)
    filtered_pairs = []
    for k, v in query_pairs:
        kl = k.lower()
        if any(kl.startswith(pfx) for pfx in _DROP_QUERY_KEYS_PREFIX):
            continue
        if kl in _DROP_QUERY_KEYS_EXACT:
            continue
        filtered_pairs.append((k, v))
    query = urlencode(filtered_pairs, doseq=True)

    # Normalize path: keep it, but collapse trailing slash.
    path = parsed.path or "/"
    if path != "/" and path.endswith("/"):
        path = path[:-1]

    normalized = urlunparse((scheme, netloc, path, "", query, fragment))
    return normalized


_SKIP_NETLOCS = {
    "facebook.com",
    "twitter.com",
    "x.com",
    "linkedin.com",
    "instagram.com",
    "youtube.com",
    "t.me",
    "webcache.googleusercontent.com",
    "accounts.google.com",
}


def _is_candidate_site(url: str, *, source_netloc: str | None = None) -> bool:
    """Heuristic filter to keep likely external firm sites."""
    try:
        parsed = urlparse(url)
    except Exception:
        return False

    host = (parsed.netloc or "").lower()
    if host.startswith("www."):
        host = host[4:]
    if not host or "." not in host:
        return False

    # Skip same-site links when importing from an article/list page.
    if source_netloc:
        sn = source_netloc.lower()
        if sn.startswith("www."):
            sn = sn[4:]
        if host == sn:
            return False

    # Skip obvious non-targets (social/logins).
    if host in _SKIP_NETLOCS:
        return False

    return True


def extract_urls_from_text(text: str) -> List[str]:
    """
    Extract URLs from plain text using regex.
    Returns a list of unique URLs found in the text.
    """
    # Pattern to match URLs
    url_pattern = re.compile(
        r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
    )
    urls = url_pattern.findall(text)
    
    # Also look for domain-like patterns
    domain_pattern = re.compile(
        r'(?:www\.)?[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)+'
    )
    domains = domain_pattern.findall(text)
    
    # Add any domain-like matches (don’t require law keywords; many firms don't include them).
    for domain in domains:
        if not domain.startswith('http'):
            urls.append(domain)
    
    # Remove duplicates and normalize
    unique_urls = []
    seen = set()
    for url in urls:
        normalized = normalize_url(url)
        if normalized and normalized not in seen:
            seen.add(normalized)
            unique_urls.append(normalized)
    
    return unique_urls


def extract_urls_from_url(article_url: str) -> List[str]:
    """
    Fetch a given article URL, parse its HTML content, and extract URLs.
    Filters URLs to identify likely law firm websites.
    
    Args:
        article_url: URL of the article/list page to parse
        
    Returns:
        List of URLs that appear to be law firm websites
    """
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(article_url, headers=headers, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        source_netloc = urlparse(article_url).netloc
        
        # Extract all href attributes
        urls = []
        for link in soup.find_all('a', href=True):
            href = link.get('href')
            if href:
                full_url = urljoin(article_url, href)
                urls.append(full_url)
        
        # Also extract URLs from text content
        text_urls = extract_urls_from_text(response.text)
        urls.extend(text_urls)
        
        # Normalize + filter to likely external target websites
        filtered_urls = []
        seen = set()
        for url in urls:
            normalized = normalize_url(url)
            if not normalized:
                continue
            if not _is_candidate_site(normalized, source_netloc=source_netloc):
                continue
            if normalized not in seen:
                seen.add(normalized)
                filtered_urls.append(normalized)

        return filtered_urls
        
    except Exception as e:
        print(f"Error extracting URLs from {article_url}: {e}")
        return []


def search_from_list(list_url: str = None, list_text: str = None) -> Tuple[List[str], int]:
    """
    Extract URLs from an external list (WSJ, SuperLawyers, etc.).
    
    Args:
        list_url: URL of the article/list page
        list_text: Plain text content of the list (alternative to list_url)
        
    Returns:
        Tuple of (list of URLs, count)
    """
    urls = []
    
    if list_url:
        urls = extract_urls_from_url(list_url)
    elif list_text:
        urls = extract_urls_from_text(list_text)
    
    # Remove duplicates while preserving order
    unique_urls = []
    seen = set()
    for url in urls:
        if url and url not in seen:
            seen.add(url)
            unique_urls.append(url)
    
    return unique_urls, len(unique_urls)
