from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
import asyncio
import sys
import os

# Ensure the current directory is in the path for module imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Emoji-Shield: Force UTF-8 for Windows Console to prevent crashes on special characters
try:
    if sys.stdout.encoding.lower() != 'utf-8':
        sys.stdout.reconfigure(encoding='utf-8')
except AttributeError: pass

from scraper import AmazonScraper, GoodreadsScraper, AuthorScraper
from excel_utility import save_to_excel
from playwright.async_api import async_playwright

app = Flask(__name__)
CORS(app)

scraper = AmazonScraper(headless=False)

async def run_scrape_process(url, limit):
    # Industrial Stepper: Check current progress in Excel
    skip_offset = 0
    excel_path = "scraped_data.xlsx"
    if os.path.exists(excel_path):
        try:
            import pandas as pd
            df_existing = pd.read_excel(excel_path)
            skip_offset = len(df_existing)
            print(f"  [Stepper] Detected {skip_offset} existing books. Resuming at #{skip_offset + 1}...")
        except Exception as e:
            print(f"  [Warning] Could not read existing Excel: {e}")

    # Part 1: List extraction (Requesting exactly 50 books for the 'Step')
    book_list = await scraper.scrape_bestseller_list(url, limit=limit, skip_offset=skip_offset)

    if not book_list:
        return []

    # Extract base domain from the input URL (e.g. https://www.amazon.in)
    from urllib.parse import urlparse
    parsed = urlparse(url)
    base_url = f"{parsed.scheme}://{parsed.netloc}"
    print(f"Base URL for product pages: {base_url}")

    # Part 2: Deep extraction (CONCURRENT TABS)
    print(f"Part 2: Visiting {len(book_list)} pages in a single browser with 5 tabs...")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
        )
        
        # --- MANUAL LOGIN GATE (Goodreads) ---
        login_page = await context.new_page()
        print("\n" + "!" * 60)
        print("  ACTION REQUIRED: LOG IN TO GOODREADS MANUALLY")
        print("  1. Enter your credentials in the browser tab.")
        print("  2. Solve any CAPTCHAs if they appear.")
        print("  The automation will resume automatically once logged in.")
        print("!" * 60 + "\n")
        
        try:
            await login_page.goto("https://www.goodreads.com/", wait_until="networkidle")
            # Detect existing session or wait for new login
            if not await login_page.query_selector('a[href*="/user/sign_out"], .headerPersonalNav'):
                await login_page.goto("https://www.goodreads.com/user/sign_in")
                # Wait for login indicator (timeout 5 mins)
                # Success indicator: dropdown menu or sign out link
                try:
                    await login_page.wait_for_selector('a[href*="/user/sign_out"], .Header_userProfile, .headerPersonalNav', timeout=300000)
                    print("  [OK] Goodreads login detected! Starting automated enrichment...")
                except Exception:
                    print("  [Time Out] Manual login wait exceeded. Attempting to proceed anyway...")
            else:
                print("  [OK] Goodreads: Existing session detected. Proceeding...")
            
            await asyncio.sleep(1) # Small cushion
            await login_page.close()
        except Exception as le:
            print(f"  [Error] Manual login gate error: {le}")

        goodreads_scraper = GoodreadsScraper(headless=False)
        author_scraper = AuthorScraper(headless=False)
        sem = asyncio.Semaphore(30) # Industrial Speed Upgrade
        
        # Cache to avoid redundant searches for the same author in a single run
        author_cache = {}
        total_books = len(book_list)
        completed_count = 0
        final_results = []

        async def limited_tab_scrape(book):
            nonlocal completed_count
            async with sem:
                try:
                    title = book.get('Book Title', 'N/A')
                    # Ensure URL is absolute before visiting
                    url = book.get('Amazon URL', '')
                    if url and not url.startswith('http'):
                        url = base_url.rstrip('/') + (url if url.startswith('/') else '/' + url)
                        book['Amazon URL'] = url

                    completed_count += 1
                    print(f"[{completed_count}/{total_books}] Opening Tab: {title[:40].encode('ascii', 'ignore').decode('ascii')}...")
                    
                    # --- DEEP-DIVE TABS ARE HEADLESS FOR STABILITY ---
                    details = await scraper.scrape_product_details_tab(
                        context, url, base_url=base_url
                    )

                    # Prefer product-page author (more reliable), fall back to list-page author
                    list_author = book.get('Author Name', 'N/A')
                    detail_author = details.get('Author Name', 'N/A')
                    final_author = detail_author if detail_author and detail_author != 'N/A' else list_author

                    # Part 3: Goodreads enrichment
                    gr_data = await goodreads_scraper.scrape_goodreads_data(
                        context, title, final_author
                    )

                    # Part 4: Author Contact Details (Cache-enabled)
                    author_key = final_author.strip().lower()
                    if author_key not in author_cache:
                        author_info = await author_scraper.find_author_details(context, final_author)
                        author_cache[author_key] = author_info
                    
                    ath = author_cache[author_key]

                    # MAPPING TO 33-COLUMN SCHEMA
                    mapped = {
                        "Sub_Genre":                 gr_data.get('Sub_Genre', details.get('Sub_Genre_Candidate', 'N/A')),
                        "Price_Tier":                details.get('Price', book.get('Price', 'N/A')),
                        "Amazon URL":                details.get('Amazon URL', book.get('Amazon URL', 'N/A')),
                        "Book Title":                book.get('Book Title', 'N/A'),
                        "Book Number in Series":     details.get('Book Number', 'N/A'),
                        "Series Name":               details.get('Series Name', 'N/A'),
                        "Author Name":               final_author,
                        "Amazon Stars":              book.get('Rating', 0),
                        "Amazon Ratings":            book.get('Number of Reviews', 0),
                        "Number of Books in Series": details.get('Total Books', 'N/A'),
                        "Genre":                     gr_data.get('Genre', 'N/A'),
                        "Publisher":                 details.get('Publisher', 'N/A'),
                        "Publication Date":          details.get('Publication Date', 'N/A'),
                        "Print Length / Pages":      details.get('Pages', 'N/A'),
                        "Best Sellers Rank":         details.get('Inner Rank', book.get('Rank', 'N/A')),
                        "Licensing Status":          "N/A",
                        "Part of a Series?":         "Yes" if details.get('Series Name') and details.get('Series Name') != 'N/A' else "No",
                        "Part_of_Series":            "Yes" if details.get('Series Name') and details.get('Series Name') != 'N/A' else "No",
                        "GoodReads_Series_URL":      gr_data.get('GoodReads_Series_URL', 'N/A'),
                        "Num_Primary_Books":         gr_data.get('Num_Primary_Books', 'N/A'),
                        "Total_Pages_Primary_Books": gr_data.get('Total_Pages_Primary_Books', 'N/A'),
                        "Book1_Rating":              gr_data.get('Book1_Rating', 'N/A'),
                        "Book1_Num_Ratings":         gr_data.get('Book1_Num_Ratings', 'N/A'),
                        "Logline":                   details.get('Description', 'N/A'),
                        "One_Sentence_Logline":      (details.get('Description', '').split('.')[0] + '.') if details.get('Description') else 'N/A',
                        "Romantasy_Subgenre":        gr_data.get('Romantasy_Subgenre', 'N/A'),
                        "Author_Email":              ath.get('Author_Email', 'N/A'),
                        "Agent_Email":               ath.get('Agent_Email', 'N/A'),
                        "Facebook":                  ath.get('Facebook', 'N/A'),
                        "Twitter":                   ath.get('Twitter', 'N/A'),
                        "Instagram":                 ath.get('Instagram', 'N/A'),
                        "Website":                   ath.get('Website', 'N/A'),
                        "Other_Contact":             ath.get('Other_Contact', 'N/A')
                    }

                    return mapped
                except Exception as e:
                    title = book.get('Book Title', 'N/A')
                    print(f"Tab error for {title.encode('ascii', 'ignore').decode('ascii')}: {e}")
                    return {**book, "Description": "Error"}
        
        # --- CHUNKED PROCESSING FOR AUTO-SAVE (Every 100 books) ---
        chunk_size = 100
        for i in range(0, total_books, chunk_size):
            chunk = book_list[i:i + chunk_size]
            print(f"\nProcessing Batch {i//chunk_size + 1} ({len(chunk)} books)...")
            
            chunk_tasks = [limited_tab_scrape(book) for book in chunk]
            chunk_results = await asyncio.gather(*chunk_tasks)
            final_results.extend(chunk_results)
            
            # Auto-save intermediate results
            temp_path = save_to_excel(final_results, "scraped_data_recovery.xlsx")
            print(f"  [Auto-Save] Intermediate progress saved to {temp_path}")

        # PART 3: AUTOMATED DEEP SWEEP (PHASE 2)
        print("\n" + "="*40)
        print("PHASE 1 COMPLETE: STARTING PHASE 2 DEEP SWEEP")
        print("="*40)
        
        import pandas as pd
        from repair_goodreads import perform_deep_repair
        
        df_results = pd.DataFrame(final_results)
        df_final = await perform_deep_repair(df_results, context)
        
        # Final Return
        final_perfected = df_final.to_dict('records')
        await browser.close()
        return final_perfected

@app.route('/api/scrape-bestsellers', methods=['POST'])
def scrape():
    data = request.json
    url = data.get('url')
    limit = data.get('limit', 1000)

    if not url:
        return jsonify({"error": "URL is required"}), 400

    print(f"Starting scrape for category: {url}")
    
    try:
        # Use asyncio.run for a clean isolated loop
        results = asyncio.run(run_scrape_process(url, limit))
        
        # Save to Excel
        excel_path = save_to_excel(results, "scraped_data.xlsx")
        
        # Automatically open the file on the user's machine (Windows specific)
        if os.name == 'nt': # Only try on Windows
            try:
                os.startfile(excel_path)
                print(f"Opening Excel file: {excel_path}")
            except Exception as se:
                print(f"Could not open file automatically: {se}")
        
        return jsonify({
            "results": results,
            "excel_path": excel_path
        })
    except Exception as e:
        import traceback
        print("Scrape failed with traceback:")
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@app.route('/api/download', methods=['GET'])
def download():
    path = "scraped_data.xlsx"
    if os.path.exists(path):
        return send_file(path, as_attachment=True)
    return jsonify({"error": "File not found"}), 404

if __name__ == '__main__':
    app.run(port=5000, debug=True)
