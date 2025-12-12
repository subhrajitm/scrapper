"""
Module for importing URLs from external lists (WSJ, SuperLawyers, etc.)
"""
import re
from typing import List, Tuple
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup


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
    
    # Filter domains that look like law firm websites
    law_keywords = ['law', 'legal', 'attorney', 'lawyer', 'firm', 'llp', 'llc']
    for domain in domains:
        domain_lower = domain.lower()
        if any(keyword in domain_lower for keyword in law_keywords):
            if not domain.startswith('http'):
                urls.append(f'https://{domain}')
    
    # Remove duplicates and normalize
    unique_urls = []
    seen = set()
    for url in urls:
        url = url.strip().rstrip('.,;:')
        if url and url not in seen:
            seen.add(url)
            unique_urls.append(url)
    
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
        
        # Filter to likely law firm websites
        law_keywords = ['law', 'legal', 'attorney', 'lawyer', 'firm', 'llp', 'llc']
        filtered_urls = []
        seen = set()
        
        for url in urls:
            url_lower = url.lower()
            # Check if URL contains law-related keywords
            if any(keyword in url_lower for keyword in law_keywords):
                if url not in seen:
                    seen.add(url)
                    filtered_urls.append(url)
            # Also check domain
            try:
                parsed = urlparse(url)
                domain = parsed.netloc.lower()
                if any(keyword in domain for keyword in law_keywords):
                    if url not in seen:
                        seen.add(url)
                        filtered_urls.append(url)
            except:
                pass
        
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
