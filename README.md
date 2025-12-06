## Law Firm & Lawyer Finder (Flask + Scrapy + Google Custom Search)

This is a Flask application that uses Google Custom Search API to find law firm websites
and attorney information based on practice area, location, and other filters. It then uses 
Scrapy to scrape those websites for contact information (emails, phones, PDFs, images, etc.).

### Setup

1. **Create & activate a virtual environment** (optional but recommended):

   ```bash
   cd /Users/subhrajitmandal/Documents/Scrapper
   python -m venv .venv
   source .venv/bin/activate
   ```

2. **Install dependencies**:

   ```bash
   pip install -r requirements.txt
   ```

3. **Set up Google Custom Search API**:

   You need to configure two things:
   
   a. **Get a Google Custom Search API Key**:
      - Go to [Google Cloud Console](https://console.cloud.google.com/)
      - Create a new project or select an existing one
      - Enable the "Custom Search API"
      - Create credentials (API Key)
      - Copy your API key
   
   b. **Create a Custom Search Engine and get the CX (Search Engine ID)**:
      - Go to [Google Custom Search](https://programmablesearchengine.google.com/)
      - Click "Add" to create a new search engine
      - In "Sites to search", you can enter `*` to search the entire web, or specify domains
      - Click "Create"
      - Go to "Setup" → "Basics" and copy your "Search engine ID" (this is your CX)
   
   c. **Configure using .env file** (Recommended):
   
      Copy the example file and fill in your values:
      ```bash
      cp .env.example .env
      ```
      
      Then edit `.env` and add your credentials:
      ```
      GOOGLE_CSE_API_KEY=your_actual_api_key_here
      GOOGLE_CSE_CX=your_actual_search_engine_id_here
      ```
   
   **Alternative: Set environment variables directly** (if you prefer not to use .env):
      ```bash
      export GOOGLE_CSE_API_KEY="YOUR_API_KEY_HERE"
      export GOOGLE_CSE_CX="YOUR_SEARCH_ENGINE_ID_HERE"
      ```

4. **Run the Flask app**:

   ```bash
   python app.py
   ```

5. **Open the app**:

   Visit `http://127.0.0.1:5000` in your browser.

### How it works

- The main Flask endpoint (`/`) renders a UI where you can select a practice area
  (e.g., Criminal Law, Corporate Law) and optionally add location filters (city, state, country).
- When you submit the form, the app uses Google Custom Search API to find relevant
  law firm and attorney websites based on your filters.
- You can select specific law firm websites and click "Scrape Selected Law Firms" to extract data.
- When you click "Scrape Selected Law Firms", it uses Scrapy to concurrently scrape all selected
  websites for:
  - Email addresses
  - Phone numbers
  - vCard links
  - PDF links
  - Image links
- The results are exported as a CSV file with all contact information.

### Features

- **Concurrent Scraping**: Uses Scrapy for fast, parallel website scraping
- **Respects robots.txt**: Automatically follows website crawling rules
- **Rate Limiting**: Built-in throttling to be polite to servers
- **Error Handling**: Robust error handling and retries


