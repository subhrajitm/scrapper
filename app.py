import csv
import io
import os
import re
from typing import List, Tuple
from urllib.parse import urljoin

from flask import Flask, render_template, request, Response, jsonify
from dotenv import load_dotenv
import requests

# Load environment variables from .env file
load_dotenv()

# Import legacy functions (still used as fallback)
from scrapy_scraper import (
    scrape_websites_with_scrapy,
    get_scraping_progress,
    reset_progress,
    start_scrape_job as legacy_start_scrape_job,
    stop_scrape_job as legacy_stop_scrape_job,
    get_scraped_items as legacy_get_scraped_items,
    get_job_urls as legacy_get_job_urls,
)
from list_importer import search_from_list, normalize_url

# Import new job manager with database support
from job_manager import (
    start_job,
    stop_job,
    get_job_progress,
    get_job_results,
    get_cached_results,
    cache_results,
)
from database import init_db, get_job, get_recent_jobs

# Initialize database
init_db()

app = Flask(__name__)

# Google Custom Search configuration
GOOGLE_CSE_API_KEY = os.getenv("GOOGLE_CSE_API_KEY")
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
    use_cache: bool = True,
) -> Tuple[List[str], int]:
    """
    Use Google Custom Search to find relevant business / company websites
    for the given filters. Returns (websites, total_results).
    
    Results are cached in SQLite for 24 hours to reduce API calls.
    """
    if not GOOGLE_CSE_API_KEY:
        raise RuntimeError(
            "Google Custom Search API key is not configured.\n\n"
            "To fix this:\n"
            "1. Create/locate an API key in Google Cloud Console\n"
            "2. Set it as an environment variable: export GOOGLE_CSE_API_KEY='your_key_here'\n\n"
            "See README.md for detailed setup instructions."
        )

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

    # Build location string for caching
    location_str = " ".join(p for p in [location, place] if p).strip()
    
    # Check cache first
    if use_cache:
        cached = get_cached_results(industry, location_str, country, page)
        if cached:
            return cached["results"], cached["total_results"]

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
    
    # Cache results for 24 hours
    if use_cache and websites:
        cache_results(industry, location_str, country, page, websites, total_results, ttl_hours=24)

    return websites, total_results


# Old scrape_website function removed - now using Scrapy


@app.route("/api/progress", methods=["GET"])
def get_progress():
    """API endpoint to get scraping progress."""
    job_id = request.args.get("job_id")
    
    # Try new job manager first (uses database)
    progress = get_job_progress(job_id) if job_id else {}
    
    # If not found in DB, try legacy in-memory
    if not progress or progress.get("status") == "unknown":
        progress = get_scraping_progress(job_id=job_id)
    
    return jsonify(progress)


@app.route("/api/start-scrape", methods=["POST"])
def api_start_scrape():
    """Start a scrape job and return a job_id."""
    data = request.get_json(silent=True) or {}
    urls = data.get("urls") or []
    if not isinstance(urls, list):
        return jsonify({"error": "urls must be a list"}), 400

    normalized: List[str] = []
    seen: set[str] = set()
    for u in urls:
        nu = normalize_url(str(u)) or str(u).strip()
        if nu and nu not in seen:
            seen.add(nu)
            normalized.append(nu)

    if not normalized:
        return jsonify({"error": "No valid URLs provided"}), 400

    # Use new job manager (with database persistence)
    job_id = start_job(normalized)
    return jsonify({"success": True, "job_id": job_id, "count": len(normalized)})


@app.route("/api/stop-scrape", methods=["POST"])
def api_stop_scrape():
    """Stop/cancel a running scrape job."""
    data = request.get_json(silent=True) or {}
    job_id = str(data.get("job_id", "")).strip()
    if not job_id:
        return jsonify({"error": "job_id is required"}), 400
    # Use new job manager
    ok = stop_job(job_id)
    return jsonify({"success": bool(ok), "job_id": job_id})


@app.route("/api/results", methods=["GET"])
def api_results():
    """Get current scraped results for a job (may be partial if still running)."""
    job_id = request.args.get("job_id", "").strip()
    if not job_id:
        return jsonify({"error": "job_id is required"}), 400

    # Use new job manager (checks DB first, falls back to in-memory)
    items = get_job_results(job_id)

    # Flatten to rows for easier display (one row per profile, or one row per site if no profiles)
    rows = []
    for item in items:
        website = item.get("website", "")
        emails = item.get("emails", [])
        phones = item.get("phones", [])
        profiles = item.get("lawyer_profiles", [])

        if profiles:
            for p in profiles:
                rows.append({
                    "website": website,
                    "lawyer_name": p.get("lawyer_name", ""),
                    "lawyer_email": p.get("lawyer_email", ""),
                    "lawyer_phone": p.get("lawyer_phone", ""),
                    "profile_url": p.get("profile_url", ""),
                })
        else:
            # No profiles, show firm-level data
            rows.append({
                "website": website,
                "lawyer_name": "",
                "lawyer_email": "; ".join(emails[:3]) if emails else "",
                "lawyer_phone": "; ".join(phones[:3]) if phones else "",
                "profile_url": "",
            })

    return jsonify({"job_id": job_id, "count": len(rows), "rows": rows})


@app.route("/progress/<job_id>", methods=["GET"])
def progress_page(job_id: str):
    """Dedicated progress page for a scrape job."""
    return render_template("progress.html", job_id=job_id)


@app.route("/api/jobs", methods=["GET"])
def api_jobs():
    """Get list of recent jobs from database."""
    limit = request.args.get("limit", 50, type=int)
    jobs = get_recent_jobs(limit=limit)
    return jsonify({
        "count": len(jobs),
        "jobs": [job.to_dict() for job in jobs]
    })


@app.route("/history", methods=["GET"])
def history_page():
    """Job history page."""
    return render_template("history.html")


def build_csv_from_scraped_data(websites: List[str], scraped_data: List[dict]) -> str:
    """Build CSV from already-scraped data."""
    output = io.StringIO()
    writer = csv.DictWriter(
        output,
        fieldnames=[
            "website",
            "lawyer_name",
            "lawyer_email",
            "lawyer_phone",
            "profile_url",
            "profile_images",
            "vcard_content",
            "all_emails",
            "all_phones",
            "vcard_links",
            "vcard_files_count",
            "pdf_links",
            "image_links",
            "lawyer_profiles_count",
        ],
    )
    writer.writeheader()

    data_by_url = {item["website"]: item for item in scraped_data if isinstance(item, dict) and item.get("website")}

    for site in websites:
        data = data_by_url.get(
            site,
            {
                "website": site,
                "emails": [],
                "phones": [],
                "vcard_links": [],
                "vcard_files": [],
                "pdf_links": [],
                "image_links": [],
                "lawyer_profiles": [],
            },
        )

        emails = data.get("emails", []) if isinstance(data.get("emails"), list) else []
        phones = data.get("phones", []) if isinstance(data.get("phones"), list) else []
        vcard_links = data.get("vcard_links", []) if isinstance(data.get("vcard_links"), list) else []
        vcard_files = data.get("vcard_files", []) if isinstance(data.get("vcard_files"), list) else []
        pdf_links = data.get("pdf_links", []) if isinstance(data.get("pdf_links"), list) else []
        image_links = data.get("image_links", []) if isinstance(data.get("image_links"), list) else []
        lawyer_profiles = data.get("lawyer_profiles", []) if isinstance(data.get("lawyer_profiles"), list) else []

        if lawyer_profiles:
            for profile in lawyer_profiles:
                writer.writerow(
                    {
                        "website": data["website"],
                        "lawyer_name": profile.get("lawyer_name", ""),
                        "lawyer_email": profile.get("lawyer_email", ""),
                        "lawyer_phone": profile.get("lawyer_phone", ""),
                        "profile_url": profile.get("profile_url", ""),
                        "profile_images": "; ".join(profile.get("profile_images", [])),
                        "vcard_content": profile.get("vcard_content", ""),
                        "all_emails": "; ".join(str(e) for e in emails),
                        "all_phones": "; ".join(str(p) for p in phones),
                        "vcard_links": "; ".join(str(v) for v in vcard_links),
                        "vcard_files_count": len(vcard_files),
                        "pdf_links": "; ".join(str(p) for p in pdf_links),
                        "image_links": "; ".join(str(i) for i in image_links),
                        "lawyer_profiles_count": len(lawyer_profiles),
                    }
                )
        else:
            writer.writerow(
                {
                    "website": data["website"],
                    "lawyer_name": "",
                    "lawyer_email": "",
                    "lawyer_phone": "",
                    "profile_url": "",
                    "profile_images": "",
                    "vcard_content": "",
                    "all_emails": "; ".join(str(e) for e in emails),
                    "all_phones": "; ".join(str(p) for p in phones),
                    "vcard_links": "; ".join(str(v) for v in vcard_links),
                    "vcard_files_count": len(vcard_files),
                    "pdf_links": "; ".join(str(p) for p in pdf_links),
                    "image_links": "; ".join(str(i) for i in image_links),
                    "lawyer_profiles_count": 0,
                }
            )

    return output.getvalue()


@app.route("/download/<job_id>.csv", methods=["GET"])
def download_job_csv(job_id: str):
    """Download CSV for a completed scrape job."""
    urls = get_job_urls(job_id)
    items = get_scraped_items(job_id)
    csv_data = build_csv_from_scraped_data(urls, items)
    filename = f"law_firms_{job_id}.csv"
    return Response(
        csv_data,
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )

@app.route("/import-list", methods=["POST"])
def import_list():
    """API endpoint to import URLs from external lists (WSJ, SuperLawyers, etc.)"""
    try:
        data = request.get_json(silent=True) or {}
        list_url = str(data.get("listUrl", "")).strip()
        list_text = str(data.get("listText", "")).strip()

        if not list_url and not list_text:
            return (
                jsonify({"error": "Please provide either a list URL or list text"}),
                400,
            )

        urls, count = search_from_list(
            list_url=list_url if list_url else None,
            list_text=list_text if list_text else None,
        )

        return jsonify(
            {
                "success": True,
                "urls": urls,
                "count": count,
                "message": f"Found {count} law firm URL(s)",
            }
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500


def build_csv_for_websites(websites: List[str]) -> str:
    """
    Scrape all given websites using Scrapy and return a CSV string with the results.
    This now uses Scrapy for concurrent, efficient scraping.
    Creates multiple rows per website: one for each lawyer profile found.
    """
    output = io.StringIO()
    writer = csv.DictWriter(
        output,
        fieldnames=[
            "website",
            "lawyer_name",
            "lawyer_email",
            "lawyer_phone",
            "profile_url",
            "profile_images",
            "vcard_content",
            "all_emails",
            "all_phones",
            "vcard_links",
            "vcard_files_count",
            "pdf_links",
            "image_links",
            "lawyer_profiles_count",
        ],
    )
    writer.writeheader()

    # Normalize/dedupe input websites (keep original order)
    normalized_websites: List[str] = []
    seen_sites: set[str] = set()
    for w in websites:
        n = normalize_url(w) or w
        if n not in seen_sites:
            seen_sites.add(n)
            normalized_websites.append(n)

    # Use Scrapy to scrape websites concurrently
    websites = normalized_websites
    print(f"Starting to scrape {len(websites)} website(s)...")
    print(f"URLs to scrape: {websites}")
    try:
        scraped_data = scrape_websites_with_scrapy(websites)
        print(f"Scraping completed. Found data for {len(scraped_data)} website(s).")
        if scraped_data:
            sample = scraped_data[0] or {}
            print(
                "Sample item summary: "
                f"website={sample.get('website')}, "
                f"emails={len(sample.get('emails') or [])}, "
                f"phones={len(sample.get('phones') or [])}, "
                f"profiles={len(sample.get('lawyer_profiles') or [])}, "
                f"vcards={len(sample.get('vcard_files') or [])}"
            )
        else:
            print("WARNING: No data was scraped!")
    except Exception as e:
        print(f"Error during scraping: {e}")
        import traceback
        traceback.print_exc()
        scraped_data = []
    
    # Create a dict mapping URLs to scraped data for quick lookup
    data_by_url = {item['website']: item for item in scraped_data}
    
    # Write results - create one row per lawyer profile, or one row for firm-level data if no profiles
    for site in websites:
        data = data_by_url.get(site, {
            'website': site,
            'emails': [],
            'phones': [],
            'vcard_links': [],
            'vcard_files': [],
            'pdf_links': [],
            'image_links': [],
            'lawyer_profiles': [],
        })
        
        # Ensure all fields are lists
        emails = data.get("emails", []) if isinstance(data.get("emails"), list) else []
        phones = data.get("phones", []) if isinstance(data.get("phones"), list) else []
        vcard_links = data.get("vcard_links", []) if isinstance(data.get("vcard_links"), list) else []
        vcard_files = data.get("vcard_files", []) if isinstance(data.get("vcard_files"), list) else []
        pdf_links = data.get("pdf_links", []) if isinstance(data.get("pdf_links"), list) else []
        image_links = data.get("image_links", []) if isinstance(data.get("image_links"), list) else []
        lawyer_profiles = data.get("lawyer_profiles", []) if isinstance(data.get("lawyer_profiles"), list) else []
        
        # Write one row per lawyer profile
        if lawyer_profiles:
            for profile in lawyer_profiles:
                writer.writerow(
                    {
                        "website": data["website"],
                        "lawyer_name": profile.get("lawyer_name", ""),
                        "lawyer_email": profile.get("lawyer_email", ""),
                        "lawyer_phone": profile.get("lawyer_phone", ""),
                        "profile_url": profile.get("profile_url", ""),
                        "profile_images": "; ".join(profile.get("profile_images", [])),
                        "vcard_content": profile.get("vcard_content", ""),  # Base64 encoded
                        "all_emails": "; ".join(str(e) for e in emails),
                        "all_phones": "; ".join(str(p) for p in phones),
                        "vcard_links": "; ".join(str(v) for v in vcard_links),
                        "vcard_files_count": len(vcard_files),
                        "pdf_links": "; ".join(str(p) for p in pdf_links),
                        "image_links": "; ".join(str(i) for i in image_links),
                        "lawyer_profiles_count": len(lawyer_profiles),
                    }
                )
        else:
            # No profiles found, write firm-level data
            writer.writerow(
                {
                    "website": data["website"],
                    "lawyer_name": "",
                    "lawyer_email": "",
                    "lawyer_phone": "",
                    "profile_url": "",
                    "profile_images": "",
                    "vcard_content": "",
                    "all_emails": "; ".join(str(e) for e in emails),
                    "all_phones": "; ".join(str(p) for p in phones),
                    "vcard_links": "; ".join(str(v) for v in vcard_links),
                    "vcard_files_count": len(vcard_files),
                    "pdf_links": "; ".join(str(p) for p in pdf_links),
                    "image_links": "; ".join(str(i) for i in image_links),
                    "lawyer_profiles_count": 0,
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
        location_input = request.form.get("location", "").strip()
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
                # Use the simplified location field if provided, otherwise combine individual fields
                if location_input:
                    location_str = location_input
                    # Try to parse city/state from the location string for display
                    parts = location_input.split(',')
                    if len(parts) >= 2:
                        selected_city = parts[0].strip()
                        selected_state = parts[1].strip()
                else:
                    # Fall back to individual fields
                    location_parts = [selected_city, selected_state, selected_country]
                    location_str = " ".join(p for p in location_parts if p).strip()
                selected_location = location_str
                
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


