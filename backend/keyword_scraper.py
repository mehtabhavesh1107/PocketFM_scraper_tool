import asyncio
import json
import os
import pandas as pd
import re
from scraper import AmazonScraper, GoodreadsScraper, AuthorScraper, clean_text, normalize_title_for_search
from excel_utility import save_to_excel
from playwright.async_api import async_playwright

# Configuration
STATE_FILE = "keyword_state_shifters.json"
OUTPUT_FILE = "../scraped_data_shifters.xlsx"
BATCH_SIZE = 50
MAX_TABS = 12
SEARCH_URL = "https://www.amazon.com/s?k=Werewolves+%26+Shifters&i=stripbooks&crid=1VFS1NEXMRVWD&sprefix=werewolves+%26+shifters%2Cstripbooks%2C432&ref=nb_sb_noss_1"

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
    # MISSION TARGET: Scale by another 1,000 titles
    MISSION_TARGET = state['total_processed_global'] + 1000
    
    print(f"\n{'='*60}")
    print(f"INDUSTRIAL SCALING MISSION: Target {MISSION_TARGET} Titles")
    print(f"Current Progress: {state['total_processed_global']} | Starting from Page {state['last_page_scanned'] + 1}")
    print(f"{'='*60}\n")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
        )
        
        while state['total_processed_global'] < MISSION_TARGET:
            curr_batch_start = state['next_batch_start']
            print(f"\n>>> [MISSION BATCH] Processing {curr_batch_start} to {curr_batch_start + BATCH_SIZE - 1}...")
            
            page = await context.new_page()
            
            # --- Discovery Phase ---
            print(f"  Navigating to Amazon Search (Page {state['last_page_scanned'] + 1})...")
            search_url = SEARCH_URL
            if state['last_page_scanned'] > 0:
                search_url += f"&page={state['last_page_scanned'] + 1}"
            
            try:
                await page.goto(search_url, wait_until="load", timeout=60000)
                
                amazon_scraper = AmazonScraper()
                await amazon_scraper.set_amazon_location(page, "90016")
                
                all_discovery_links = []
                page_count = state['last_page_scanned'] + 1
                seen_titles = [] # Reset for this batch to ensure fresh lookup
                
                while len(all_discovery_links) < BATCH_SIZE:
                    for i in range(6):
                        await page.evaluate("window.scrollBy(0, 1500)")
                        await asyncio.sleep(2.0)
                    
                    items = await page.query_selector_all("[data-asin]")
                    found_this_page = 0
                    
                    for item in items:
                        asin = await item.get_attribute('data-asin')
                        if not asin or asin == "N/A" or len(asin) < 5: continue
                        if any(x.get("asin") == asin for x in all_discovery_links): continue

                        title = "N/A"
                        title_selectors = ["h2 a span", ".a-size-medium", ".a-size-base-plus", "h2 a"]
                        for t_sel in title_selectors:
                            try:
                                t_el = await item.query_selector(t_sel)
                                if t_el:
                                    title = clean_text(await t_el.inner_text())
                                    if title and title != "N/A": break
                            except: continue
                        
                        clean_title = normalize_title_for_search(title)
                        if clean_title in seen_titles: continue

                        href = None
                        link_selectors = ["h2 a", "a.a-link-normal[href*='/dp/']"]
                        for l_sel in link_selectors:
                            try:
                                l_el = await item.query_selector(l_sel)
                                if l_el:
                                    href = await l_el.evaluate("el => el.href")
                                    if href and "/dp/" in href: break
                            except: continue

                        if href and title != "N/A":
                            all_discovery_links.append({"asin": asin, "Amazon URL": href, "Book Title": title})
                            seen_titles.append(clean_title)
                            found_this_page += 1
                        if len(all_discovery_links) >= BATCH_SIZE: break
                    
                    print(f"    -> Captured {found_this_page} titles. (Total: {len(all_discovery_links)})")

                    if len(all_discovery_links) < BATCH_SIZE:
                        next_btn = await page.query_selector('a.s-pagination-next')
                        if next_btn:
                            await next_btn.click()
                            page_count += 1
                            await asyncio.sleep(8)
                        else: break
                
                # --- Extraction Phase ---
                final_rows = []
                for i in range(0, len(all_discovery_links), MAX_TABS):
                    batch = all_discovery_links[i : i + MAX_TABS]
                    print(f"  Batch {i//MAX_TABS + 1}: Handling {len(batch)} titles...")
                    tasks = [process_book(context, book) for book in batch]
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    valid = [res for res in results if isinstance(res, dict)]
                    final_rows.extend(valid)
                    save_to_excel(valid, OUTPUT_FILE)

                # --- State Sync ---
                state['last_page_scanned'] = page_count
                state['total_processed_global'] += len(final_rows)
                state['next_batch_start'] += len(final_rows)
                save_state(state)
                
                print(f"\n[OK] Batch Complete. Total Processed: {state['total_processed_global']}/{MISSION_TARGET}")
                
                # --- AUTO-OPEN (USER REQUESTED: Per Batch) ---
                if os.name == 'nt' and os.path.exists(OUTPUT_FILE):
                    print(f"  [Mission] Opening updated results for index {curr_batch_start}-{curr_batch_start+BATCH_SIZE-1}...")
                    os.startfile(os.path.abspath(OUTPUT_FILE))
                
            except Exception as e:
                print(f"[CRITICAL ERROR] Batch failed: {e}. Retrying after nap...")
                await asyncio.sleep(60)
            
            await page.close()
            
            if state['total_processed_global'] < MISSION_TARGET:
                wait_time = 90
                print(f"Cooling down for {wait_time}s to maintain industrial stealth...")
                await asyncio.sleep(wait_time)

        await browser.close()
        print(f"\n{'='*60}")
        print(f"MISSION ACCOMPLISHED: {state['total_processed_global']} Titles Total.")
        print(f"Final Data: {os.path.abspath(OUTPUT_FILE)}")
        print(f"{'='*60}\n")
        
        if os.name == 'nt' and os.path.exists(OUTPUT_FILE):
            os.startfile(os.path.abspath(OUTPUT_FILE))

if __name__ == "__main__":
    asyncio.run(run_keyword_mission())

if __name__ == "__main__":
    asyncio.run(run_keyword_mission())
