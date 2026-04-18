import asyncio
import json
import os
import pandas as pd
import re
from scraper import AmazonScraper, GoodreadsScraper, AuthorScraper, clean_text
from excel_utility import save_to_excel
from playwright.async_api import async_playwright

# Configuration
STATE_FILE = "keyword_state_paranormal.json"
OUTPUT_FILE = "../scraped_data_paranormal.xlsx"
BATCH_SIZE = 50
MAX_TABS = 12
SEARCH_URL = "https://www.amazon.com/s?k=Paranormal+Romance&i=stripbooks&crid=2MOWCGE10UUZ2&sprefix=paranormal+romance%2Cstripbooks%2C349&ref=nb_sb_noss_1"

COLUMNS = [
    "Sub_Genre", "Price_Tier", "Amazon URL", "Book Title", "Book Number in Series",
    "Series Name", "Author Name", "Amazon Stars", "Amazon Ratings", "Number of Books in Series",
    "Genre", "Publisher", "Publication Date", "Print Length / Pages", "Best Sellers Rank",
    "Licensing Status", "Part of a Series?", "Part_of_Series", "GoodReads_Series_URL",
    "Num_Primary_Books", "Total_Pages_Primary_Books", "Book1_Rating", "Book1_Num_Ratings",
    "Logline", "One_Sentence_Logline", "Romantasy_Subgenre", "Author_Email", "Agent_Email",
    "Facebook", "Twitter", "Instagram", "Website", "Other_Contact"
]

def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            return json.load(f)
    return {
        "last_page_scanned": 0,
        "last_book_title": "N/A",
        "total_processed_global": 0,
        "next_batch_start": 1
    }

def save_state(state):
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=4)

async def process_book(context, book_data):
    """Full 33-column extraction for a single book (Amazon + Goodreads + Author)."""
    amazon = AmazonScraper()
    goodreads = GoodreadsScraper()
    author_scr = AuthorScraper()

    url = book_data.get("Amazon URL")
    discovery_title = book_data.get("Book Title", "N/A")
    asin = book_data.get("asin", "N/A")
    
    # 1. Amazon Details
    amz_details = await amazon.scrape_product_details_tab(context, url)
    
    # Final Title for Goodreads search
    actual_title = amz_details.get("Book Title") if (amz_details.get("Book Title") and amz_details.get("Book Title") != "N/A") else discovery_title
    author_name = amz_details.get("Author Name", "N/A")

    print(f"  [Task] Processing: {actual_title[:40]}... (ASIN: {asin})")

    # 2. Goodreads Details (INTEGRATED SCAN)
    if actual_title and actual_title != "N/A":
        print(f"    [Goodreads] Initiating search for '{actual_title[:25]}' by '{author_name}'...")
        gr_details = await goodreads.scrape_goodreads_data(
            context, 
            actual_title, 
            author_name, 
            asin=asin 
        )
        if gr_details.get("GoodReads_Book_URL"):
            print(f"    [Goodreads] Found: {gr_details.get('GoodReads_Rating')} stars | {gr_details.get('GoodReads_Book_URL')[:35]}...")
        else:
            print(f"    [Goodreads] Warning: No match found.")
    else:
        print(f"    [Goodreads] Error: Cannot search without a title.")
        gr_details = {}

    # 3. Author Details
    ath_details = await author_scr.find_author_details(context, author_name)

    # Mapping to 33 Columns
    desc = amz_details.get("Description", "N/A")
    one_sentence = desc.split('.')[0] + '.' if desc != "N/A" else "N/A"
    
    # Logic for Price Tier
    price_raw = amz_details.get("Price", "N/A")
    price_tier = price_raw.replace('\n', ' | ') if price_raw != "N/A" else "N/A"

    # Logic for Part of Series
    is_series = "Yes" if amz_details.get("Series Name") and amz_details.get("Series Name") != "N/A" else "No"
    part_of_series_text = f"{amz_details.get('Series Name')} Book {amz_details.get('Book Number')}" if is_series == "Yes" else "N/A"

    return {
        "Sub_Genre": gr_details.get("Sub_Genre", amz_details.get("Sub_Genre_Candidate", "N/A")),
        "Price_Tier": price_tier,
        "Amazon URL": url,
        "Book Title": actual_title,
        "Book Number in Series": amz_details.get("Book Number", "N/A"),
        "Series Name": amz_details.get("Series Name", "N/A"),
        "Author Name": author_name,
        "Amazon Stars": amz_details.get("Rating", "N/A"),
        "Amazon Ratings": amz_details.get("Number of Reviews", "N/A"),
        "Number of Books in Series": amz_details.get("Total Books", "N/A"),
        "Genre": gr_details.get("Genre", "N/A"),
        "Publisher": amz_details.get("Publisher", "N/A"),
        "Publication Date": amz_details.get("Publication Date", "N/A"),
        "Print Length / Pages": amz_details.get("Pages", "N/A"),
        "Best Sellers Rank": amz_details.get("Inner Rank", "N/A"),
        "Licensing Status": "Available",
        "Part of a Series?": is_series,
        "Part_of_Series": part_of_series_text,
        "GoodReads_Series_URL": gr_details.get("GoodReads_Series_URL", "N/A"),
        "Num_Primary_Books": gr_details.get("Num_Primary_Books", "N/A"),
        "Total_Pages_Primary_Books": gr_details.get("Total_Pages_Primary_Books", "N/A"),
        "Book1_Rating": gr_details.get("Book1_Rating", "N/A"),
        "Book1_Num_Ratings": gr_details.get("Book1_Num_Ratings", "N/A"),
        "Logline": desc[:1500] if desc != "N/A" else "N/A", 
        "One_Sentence_Logline": one_sentence,
        "Romantasy_Subgenre": gr_details.get("Romantasy_Subgenre", "No"),
        "Author_Email": ath_details.get("Author_Email", "N/A"),
        "Agent_Email": ath_details.get("Agent_Email", "N/A"),
        "Facebook": ath_details.get("Facebook", "N/A"),
        "Twitter": ath_details.get("Twitter", "N/A"),
        "Instagram": ath_details.get("Instagram", "N/A"),
        "Website": ath_details.get("Website", "N/A"),
        "Other_Contact": ath_details.get("Other_Contact", "N/A")
    }

async def run_keyword_mission():
    state = load_state()
    print(f"--- STARTING INTEGRATED KEYWORD MISSION (AMAZON + GOODREADS): Round {state['next_batch_start']} to {state['next_batch_start'] + BATCH_SIZE - 1} ---")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
        )
        page = await context.new_page()

        # Step 1: Discovery Phase (Collect 100 links)
        print(f"Navigating to Amazon Search (Starting from Page {state['last_page_scanned'] + 1})...")
        search_url = SEARCH_URL
        if state['last_page_scanned'] > 0:
            search_url += f"&page={state['last_page_scanned'] + 1}"
        
        await page.goto(search_url, wait_until="load", timeout=60000)
        
        # --- NEW: Set US Location (90016) ---
        amazon_scraper = AmazonScraper()
        await amazon_scraper.set_amazon_location(page, "90016")
        
        all_discovery_links = []
        page_count = state['last_page_scanned'] + 1
        
        while len(all_discovery_links) < BATCH_SIZE:
            print(f"\n[Scanning Page {page_count}] INDUSTRIAL SCROLL to reveal all titles...")
            
            # THE REVEAL: Fixed constant scroll to trigger lazy loading reliably
            for i in range(6):
                await page.evaluate("window.scrollBy(0, 1500)")
                await asyncio.sleep(2.0)
            
            await asyncio.sleep(2) 

            # RESILIENT SELECTOR MATRIX (Broadest containers first)
            items = await page.query_selector_all("[data-asin]")
            found_this_page = 0
            
            for item in items:
                asin = await item.get_attribute('data-asin')
                if not asin or asin == "N/A" or len(asin) < 5: continue
                
                if any(x.get("asin") == asin for x in all_discovery_links):
                    continue

                # Multi-selector Matrix for Title and Link
                title = "N/A"
                href = None
                
                # Try many common title locations
                title_selectors = ["h2 a span", ".a-size-medium", ".a-size-base-plus", "h2 a", ".p13n-sc-truncate"]
                for t_sel in title_selectors:
                    try:
                        t_el = await item.query_selector(t_sel)
                        if t_el:
                            title = clean_text(await t_el.inner_text())
                            if title and title != "N/A": break
                    except: continue
                
                # Try many common link locations
                link_selectors = ["h2 a", "a.a-link-normal[href*='/dp/']", "a.a-link-normal:first-child"]
                for l_sel in link_selectors:
                    try:
                        l_el = await item.query_selector(l_sel)
                        if l_el:
                            href = await l_el.evaluate("el => el.href")
                            if href and "/dp/" in href: break
                    except: continue

                if href and title != "N/A":
                    all_discovery_links.append({
                        "asin": asin,
                        "Amazon URL": href,
                        "Book Title": title
                    })
                    found_this_page += 1
                
                if len(all_discovery_links) >= BATCH_SIZE: break
            
            print(f"  -> SUCCESS: Captured {found_this_page} titles from Page {page_count}. (Total: {len(all_discovery_links)})")

            if len(all_discovery_links) < BATCH_SIZE:
                next_btn = await page.query_selector('a.s-pagination-next')
                if next_btn:
                    print(f"  Flipping to Search Page {page_count + 1}...")
                    await next_btn.click()
                    page_count += 1
                    await asyncio.sleep(10) # High-safety delay for search pagination
                else:
                    print("  [End of Search] No more results found.")
                    break
        
        print(f"\nDiscovery phase finished. Collected {len(all_discovery_links)} high-fidelity links.")
        last_book = all_discovery_links[-1]["Book Title"] if all_discovery_links else "N/A"

        # Step 2: Industrial Extraction Phase (Multi-tab)
        final_rows = []
        for i in range(0, len(all_discovery_links), MAX_TABS):
            batch = all_discovery_links[i : i + MAX_TABS]
            print(f"\nBatch Processor: Handling {i+1} to {min(i + MAX_TABS, len(all_discovery_links))} (Integrated Amazon + Goodreads + Author)...")
            
            tasks = [process_book(context, book) for book in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Resilience Filter: Keep only valid dicts, log errors
            valid_results = []
            for res in results:
                if isinstance(res, dict):
                    valid_results.append(res)
                else:
                    print(f"  [Resilience] Handled extraction error: {res}")
            
            final_rows.extend(valid_results)

            # Use the advanced excel utility for saving and formatting
            save_to_excel(final_rows, OUTPUT_FILE)
            print(f"  [Auto-Save] Synchronized and Formatted: {OUTPUT_FILE}")

        # Step 3: Global State Sync
        state['last_page_scanned'] = page_count
        state['last_book_title'] = last_book
        state['total_processed_global'] += len(final_rows)
        state['next_batch_start'] += len(final_rows)
        save_state(state)

        await browser.close()
        print(f"\n" + "="*50)
        print(f"MISSION COMPLETE: High-Discovery Integrated Pass Finished!")
        print(f"Total Processed: {len(final_rows)}")
        print("="*50)

        # Auto-Open Excel
        if os.path.exists(OUTPUT_FILE) and os.name == 'nt':
            print(f"\nOpening Excel: {OUTPUT_FILE}")
            os.startfile(os.path.abspath(OUTPUT_FILE))

if __name__ == "__main__":
    asyncio.run(run_keyword_mission())
