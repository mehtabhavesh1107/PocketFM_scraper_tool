import sys
import os
# Ensure the current directory is in the path for module imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import asyncio
import re
import pandas as pd
import random
from playwright.async_api import async_playwright
from scraper import GoodreadsScraper, clean_numeric
from excel_utility import save_to_excel

# Configuration
INPUT_FILE = "scraped_data.xlsx"
OUTPUT_FILE = "scraped_data.xlsx" 
CHECK_COLUMN = "GoodReads_Series_URL"
MAX_CONCURRENT_TABS = 30  # Industrial Speed!

def extract_asin(url):
    """Extract ASIN from Amazon URL."""
    if not url or pd.isna(url) or url == "N/A":
        return "N/A"
    # Match /dp/ASIN or /product/ASIN or /gp/product/ASIN
    match = re.search(r'/(?:dp|product|gp/product)/([A-Z0-9]{10})', url)
    return match.group(1) if match else "N/A"

async def repair_row(index, row, context, semaphore, gr_scraper, df, total_missing, progress_counter):
    """Process a single row using the Amazon-Standard Mapping Technique."""
    async with semaphore:
        # Add random jitter
        await asyncio.sleep(random.uniform(0.5, 3.0))
        
        title = str(row["Book Title"])
        author = str(row["Author Name"])
        amazon_url = str(row.get("Amazon URL", "N/A"))
        asin = extract_asin(amazon_url)
        
        # Title cleanup for Ads
        search_title = title
        is_ad = str(row.get("is_ad_title", "False")) == "True"
        if is_ad:
            search_title = re.sub(r'^(Sponsored\s*(Ad|ad)?\s*[\-\:]?\s*|Ad\s*\-\s*)', '', title, flags=re.IGNORECASE).strip()
        
        progress_counter[0] += 1
        curr = progress_counter[0]
        print(f"[{curr}/{total_missing}] Tech-Sync Repair: {search_title} (ASIN: {asin})")
        
        try:
            gr_data = await gr_scraper.scrape_goodreads_data(context, search_title, author, asin=asin)
            
            if gr_data:
                # AMAZON TECHNIQUE: Standardized 33-Column Mapping Block
                # We prioritize freshly scraped Goodreads data (gr_data) 
                # but preserve existing Amazon data from the 'row'
                
                final_series_url = gr_data.get('GoodReads_Series_URL', 'N/A')
                if final_series_url == "N/A":
                    final_series_url = gr_data.get('GoodReads_Book_URL', 'N/A')

                # Using gr_data prioritized values
                mapped = {
                    "Sub_Genre":                 gr_data.get('Sub_Genre', row.get('Sub_Genre', 'N/A')),
                    "Price_Tier":                row.get('Price_Tier', 'N/A'),
                    "Amazon URL":                row.get('Amazon URL', 'N/A'),
                    "Book Title":                row.get('Book Title', 'N/A'),
                    "Book Number in Series":     row.get('Book Number in Series', 'N/A'),
                    "Series Name":               row.get('Series Name', 'N/A'),
                    "Author Name":               row.get('Author Name', 'N/A'),
                    "Amazon Stars":              row.get('Amazon Stars', 0),
                    "Amazon Ratings":            row.get('Amazon Ratings', 0),
                    "Number of Books in Series": row.get('Number of Books in Series', 'N/A'),
                    "Genre":                     gr_data.get('Genre', row.get('Genre', 'N/A')),
                    "Publisher":                 row.get('Publisher', 'N/A'),
                    "Publication Date":          row.get('Publication Date', 'N/A'),
                    "Print Length / Pages":      row.get('Print Length / Pages', 'N/A'),
                    "Best Sellers Rank":         row.get('Best Sellers Rank', 'N/A'),
                    "Licensing Status":          row.get('Licensing Status', 'N/A'),
                    "Part of a Series?":         row.get('Part of a Series?', 'No'),
                    "Part_of_Series":            row.get('Part_of_Series', 'No'),
                    "GoodReads_Series_URL":      final_series_url,
                    "Num_Primary_Books":         gr_data.get('Num_Primary_Books', 'N/A'),
                    "Total_Pages_Primary_Books": gr_data.get('Total_Pages_Primary_Books', 'N/A'),
                    "Book1_Rating":              gr_data.get('Book1_Rating', 'N/A'),
                    "Book1_Num_Ratings":         gr_data.get('Book1_Num_Ratings', 'N/A'),
                    "Logline":                   row.get('Logline', 'N/A'),
                    "One_Sentence_Logline":      row.get('One_Sentence_Logline', 'N/A'),
                    "Romantasy_Subgenre":        gr_data.get('Romantasy_Subgenre', 'N/A'),
                    "Author_Email":              row.get('Author_Email', 'N/A'),
                    "Agent_Email":               row.get('Agent_Email', 'N/A'),
                    "Facebook":                  row.get('Facebook', 'N/A'),
                    "Twitter":                   row.get('Twitter', 'N/A'),
                    "Instagram":                 row.get('Instagram', 'N/A'),
                    "Website":                   row.get('Website', 'N/A'),
                    "Other_Contact":             row.get('Other_Contact', 'N/A')
                }
                
                # Apply mapped dict to the DataFrame
                for col, val in mapped.items():
                    if col in df.columns:
                        df.at[index, col] = val

                print(f"  -> SUCCESS [{curr}]: Mapped 33 columns. Rating: {mapped['Book1_Rating']} | Count: {mapped['Book1_Num_Ratings']}")
            else:
                print(f"  -> FAILED [{curr}]: No data found for {search_title}")
        
        except Exception as e:
            print(f"  -> ERROR [{curr}]: {e}")

async def perform_deep_repair(df, context):
    """
    Standard entry point for automated Goodreads repair.
    Targets rows with missing URLs or ratings.
    """
    gr_scraper = GoodreadsScraper(headless=False)
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_TABS)
    progress_counter = [0]
    
    def is_missing(row):
        url_val = row.get(CHECK_COLUMN)
        url_missing = pd.isna(url_val) or "goodreads.com" not in str(url_val).lower()
        rating_val = row.get("Book1_Rating")
        rating_missing = pd.isna(rating_val) or str(rating_val).strip().lower() in ["n/a", "", "0", "nan"]
        return url_missing or rating_missing

    to_repair_mask = df.apply(is_missing, axis=1)
    to_repair = df[to_repair_mask].copy()
    
    if to_repair.empty:
        print("  [Deep Sweep] Coverage is already 100%. No repair needed.")
        return df

    to_repair["is_ad_title"] = to_repair["Book Title"].str.contains(r"Sponsored|Ad -|Shop now", case=False, na=False)
    total_missing = len(to_repair)
    
    print(f"\n[PHASE 2] Starting Automated Deep Sweep for {total_missing} books...")

    for col in df.columns:
        df[col] = df[col].astype(object)

    tasks = [repair_row(index, row, context, semaphore, gr_scraper, df, total_missing, progress_counter) for index, row in to_repair.iterrows()]
    await asyncio.gather(*tasks)
    return df

async def repair_goodreads_data():
    if not os.path.exists(INPUT_FILE):
        print(f"Error: {INPUT_FILE} not found.")
        return

    print(f"Loading {INPUT_FILE} for manual repair...")
    df = pd.read_excel(INPUT_FILE)
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")

        # Login Gate
        login_page = await context.new_page()
        await login_page.goto("https://www.goodreads.com/user/sign_in")
        print("\nACTION REQUIRED: Log in to Goodreads and navigate to Home.\n")
        
        login_selectors = ['a[href*="sign_out"]', '.Header_userProfile', '.headerPersonalNav', '[data-testid="notificationsIcon"]', 'a[href="/"]']
        logged_in = False
        for _ in range(60):
            for sel in login_selectors:
                try:
                    if await login_page.locator(sel).is_visible():
                        logged_in = True
                        break
                except: pass
            if logged_in: break
            print("  [Waiting] Still waiting for login detection...")
            await asyncio.sleep(5)
        
        if logged_in:
            print("  [OK] Login detected! Starting Turbo-Repair...")
            await login_page.close()

        # Chain the repair logic (Fixes the undefined total_missing/to_repair bug)
        df = await perform_deep_repair(df, context)

        print("Repair complete! Saving with Amazon-standard utility...")
        excel_path = save_to_excel(df.to_dict('records'), OUTPUT_FILE)
        
        await browser.close()
        print(f"FINISH! File updated: {excel_path}")
        if os.name == 'nt': os.startfile(excel_path)

if __name__ == "__main__":
    asyncio.run(repair_goodreads_data())
