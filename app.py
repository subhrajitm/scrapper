import csv
import io
import os
import re
from typing import List, Tuple
from urllib.parse import urljoin

from flask import Flask, render_template, request, Response, jsonify
from dotenv import load_dotenv
import requests
from scrapy_scraper import scrape_websites_with_scrapy, get_scraping_progress, reset_progress

# Load environment variables from .env file
load_dotenv()

app = Flask(__name__)

# Google Custom Search configuration
GOOGLE_CSE_API_KEY = os.getenv(
    "GOOGLE_CSE_API_KEY",
    "AIzaSyDEKgDStf3W2sb2wmjDRPzIdj7khdqA0NA",
)
GOOGLE_CSE_CX = os.getenv("GOOGLE_CSE_CX")  # Search engine ID – must be set.


def extract_websites_from_text(text: str) -> List[str]:
    """
    Extract website URLs from a block of text.
    Kept for compatibility but not used by the Google Custom Search flow.
    """
    websites: List[str] = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        if "http://" in line or "https://" in line or ".com" in line or ".io" in line or ".ai" in line:
            tokens = line.replace(",", " ").replace(";", " ").split()
            for token in tokens:
                token = token.strip("()[]{}.,;")
                if (
                    token.startswith("http://")
                    or token.startswith("https://")
                    or any(token.endswith(tld) for tld in [".com", ".io", ".ai", ".co", ".org", ".net"])
                ):
                    if token not in websites:
                        websites.append(token)
    return websites


def get_websites_for_filters(
    industry: str,
    service: str,
    location: str,
    country: str,
    place: str,
    page: int = 1,
) -> Tuple[List[str], int]:
    """
    Use Google Custom Search to find relevant business / company websites
    for the given filters. Returns (websites, total_results).
    """
    if not GOOGLE_CSE_CX:
        raise RuntimeError(
            "Google Custom Search 'cx' (search engine ID) is not configured.\n\n"
            "To fix this:\n"
            "1. Go to https://programmablesearchengine.google.com/\n"
            "2. Create a new search engine (or use an existing one)\n"
            "3. Copy the 'Search engine ID' from the Setup page\n"
            "4. Set it as an environment variable: export GOOGLE_CSE_CX='your_id_here'\n\n"
            "See README.md for detailed setup instructions."
        )

    # Build a search query specifically for lawyers and law firms
    parts = []
    if industry:
        parts.append(f"{industry} lawyer")
    if service:
        parts.append(service)
    if location:
        parts.append(location)
    if country:
        parts.append(country)
    if place:
        parts.append(place)
    # Always include law firm/lawyer keywords
    parts.append("law firm")
    parts.append("attorney")
    query = " ".join(p for p in parts if p).strip()

    # Google CSE supports pagination via the "start" parameter (1-based index).
    # The API only returns up to 100 results. With page_size=10 this means
    # start_index cannot be greater than 91, otherwise a 400 is returned.
    page_size = 10
    requested_page = max(page, 1)
    start_index = (requested_page - 1) * page_size + 1
    if start_index > 91:
        start_index = 91

    resp = requests.get(
        "https://www.googleapis.com/customsearch/v1",
        params={
            "key": GOOGLE_CSE_API_KEY,
            "cx": GOOGLE_CSE_CX,
            "q": query,
            "num": page_size,
            "start": start_index,
        },
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()

    items = data.get("items", []) or []
    total_results_raw = data.get("searchInformation", {}).get("totalResults", "0")
    try:
        total_results = int(total_results_raw)
    except (TypeError, ValueError):
        total_results = len(items)
    websites: List[str] = []
    for item in items:
        link = item.get("link")
        if link and link not in websites:
            websites.append(link)

    return websites, total_results


# Old scrape_website function removed - now using Scrapy


@app.route("/api/progress", methods=["GET"])
def get_progress():
    """API endpoint to get scraping progress"""
    progress = get_scraping_progress()
    return jsonify(progress)


def build_csv_for_websites(websites: List[str]) -> str:
    """
    Scrape all given websites using Scrapy and return a CSV string with the results.
    This now uses Scrapy for concurrent, efficient scraping.
    """
    output = io.StringIO()
    writer = csv.DictWriter(
        output,
        fieldnames=[
            "website",
            "emails",
            "phones",
            "vcard_links",
            "pdf_links",
            "image_links",
        ],
    )
    writer.writeheader()

    # Use Scrapy to scrape websites concurrently
    print(f"Starting to scrape {len(websites)} website(s)...")
    print(f"URLs to scrape: {websites}")
    try:
        scraped_data = scrape_websites_with_scrapy(websites)
        print(f"Scraping completed. Found data for {len(scraped_data)} website(s).")
        if scraped_data:
            print(f"Sample data: {scraped_data[0] if scraped_data else 'None'}")
        else:
            print("WARNING: No data was scraped!")
    except Exception as e:
        print(f"Error during scraping: {e}")
        import traceback
        traceback.print_exc()
        scraped_data = []
    
    # Create a dict mapping URLs to scraped data for quick lookup
    data_by_url = {item['website']: item for item in scraped_data}
    
    # Write results - ensure all websites are included even if scraping failed
    for site in websites:
        data = data_by_url.get(site, {
            'website': site,
            'emails': [],
            'phones': [],
            'vcard_links': [],
            'pdf_links': [],
            'image_links': [],
        })
        
        # Ensure all fields are lists
        emails = data.get("emails", []) if isinstance(data.get("emails"), list) else []
        phones = data.get("phones", []) if isinstance(data.get("phones"), list) else []
        vcard_links = data.get("vcard_links", []) if isinstance(data.get("vcard_links"), list) else []
        pdf_links = data.get("pdf_links", []) if isinstance(data.get("pdf_links"), list) else []
        image_links = data.get("image_links", []) if isinstance(data.get("image_links"), list) else []
        
        writer.writerow(
            {
                "website": data["website"],
                "emails": "; ".join(str(e) for e in emails),
                "phones": "; ".join(str(p) for p in phones),
                "vcard_links": "; ".join(str(v) for v in vcard_links),
                "pdf_links": "; ".join(str(p) for p in pdf_links),
                "image_links": "; ".join(str(i) for i in image_links),
            }
        )

    return output.getvalue()


@app.route("/", methods=["GET", "POST"])
def index():
    # Practice areas for lawyers
    practice_areas = [
        "Criminal Law",
        "Corporate Law",
        "Family Law",
        "Personal Injury",
        "Real Estate Law",
        "Immigration Law",
        "Intellectual Property",
        "Employment Law",
        "Tax Law",
        "Estate Planning",
        "Bankruptcy Law",
        "Medical Malpractice",
        "Immigration",
        "DUI/DWI",
        "Workers Compensation",
    ]

    selected_practice_area = ""
    selected_location = ""
    selected_city = ""
    selected_state = ""
    selected_country = ""
    page = 1

    websites: List[str] = []
    total_results: int | None = None
    error: str | None = None

    if request.method == "POST":
        selected_practice_area = request.form.get("practice_area", "").strip()
        selected_location = request.form.get("location", "").strip()
        selected_city = request.form.get("city", "").strip()
        selected_state = request.form.get("state", "").strip()
        selected_country = request.form.get("country", "").strip()
        page_str = request.form.get("page") or "1"
        action = request.form.get("action", "search")

        try:
            page = max(int(page_str), 1)
        except ValueError:
            page = 1

        try:
            if not selected_practice_area:
                error = "Please select a practice area."
            else:
                # Combine location fields
                location_parts = [selected_city, selected_state, selected_location, selected_country]
                location_str = " ".join(p for p in location_parts if p).strip()
                
                websites, total_results = get_websites_for_filters(
                    selected_practice_area,
                    "",  # service (not used for lawyers)
                    location_str,
                    selected_country,
                    "",  # place (not used)
                    page=page,
                )
                if action == "scrape" and websites:
                    # Get selected URLs from form
                    selected_urls = request.form.getlist("selected_urls")
                    if not selected_urls:
                        error = "Please select at least one law firm website to scrape."
                    else:
                        # Reset progress before starting
                        reset_progress()
                        csv_data = build_csv_for_websites(selected_urls)
                        filename = f"law_firms_{selected_practice_area.replace(' ', '_').lower()}_{location_str.replace(' ', '_').lower() or 'all'}.csv"
                        return Response(
                            csv_data,
                            mimetype="text/csv",
                            headers={
                                "Content-Disposition": f"attachment; filename={filename}"
                            },
                        )
                elif action == "export" and websites:
                    csv_data = build_csv_for_websites(websites)
                    filename = f"law_firms_{selected_practice_area.replace(' ', '_').lower()}.csv"
                    return Response(
                        csv_data,
                        mimetype="text/csv",
                        headers={
                            "Content-Disposition": f"attachment; filename={filename}"
                        },
                    )
        except Exception as exc:  # noqa: BLE001
            error = f"Failed to fetch law firm websites: {exc}"

    page_size = 10
    total_pages = (
        (total_results + page_size - 1) // page_size if total_results is not None else None
    )

    return render_template(
        "index.html",
        practice_areas=practice_areas,
        selected_practice_area=selected_practice_area,
        selected_location=selected_location,
        selected_city=selected_city,
        selected_state=selected_state,
        selected_country=selected_country,
        websites=websites,
        error=error,
        page=page,
        total_pages=total_pages,
        total_results=total_results,
    )


if __name__ == "__main__":
    # Run local dev server
    app.run(debug=True)


