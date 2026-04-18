import asyncio
import os
import sys
import pandas as pd
from playwright.async_api import async_playwright

# Ensure backend folder is in path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from keyword_scraper import process_book, COLUMNS, SEARCH_URL
from scraper import AmazonScraper, clean_text
from excel_utility import save_to_excel

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_FILE = os.path.join(BASE_DIR, "../scraped_data_keywords.xlsx")
RECOVERY_FILE = os.path.join(BASE_DIR, "repair_recovery_40_100.json")

START_INDEX = 40
END_INDEX = 100
TARGET_COUNT = END_INDEX - START_INDEX + 1 # 61 books
MAX_TABS = 12

async def run_40_100_scrape():
    print(f"--- STARTING INDUSTRIAL SCRAPE (40-100): Targeted Search Pass ---")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
        )
        page = await context.new_page()

        # Step 1: Discovery Phase (Skip first 39, collect next 61)
        print(f"Navigating to Amazon Search...")
        amazon_scraper = AmazonScraper()
        
        discovery_links = []
        skip_count = START_INDEX - 1 # Skip 39
        
        page_num = 1
        while len(discovery_links) < TARGET_COUNT:
            url = SEARCH_URL
            if page_num > 1:
                url += f"&page={page_num}"
            
            print(f"\n[Page {page_num}] Scanning results...")
            await page.goto(url, wait_until="load", timeout=60000)
            
            if page_num == 1:
                await amazon_scraper.set_amazon_location(page, "90016")
            
            # Scroll to reveal all
            for i in range(6):
                await page.evaluate("window.scrollBy(0, 1500)")
                await asyncio.sleep(1.5)
            
            items = await page.query_selector_all("[data-asin]")
            found_this_page = 0
            
            for item in items:
                asin = await item.get_attribute('data-asin')
                if not asin or asin == "N/A" or len(asin) < 5: continue
                
                # Global skip counter
                if skip_count > 0:
                    skip_count -= 1
                    continue
                
                if any(x.get("asin") == asin for x in discovery_links): continue

                title = "N/A"
                href = None
                
                title_selectors = ["h2 a span", ".a-size-medium", ".a-size-base-plus", "h2 a", ".p13n-sc-truncate"]
                for t_sel in title_selectors:
                    try:
                        t_el = await item.query_selector(t_sel)
                        if t_el:
                            title = clean_text(await t_el.inner_text())
                            if title and title != "N/A": break
                    except: continue
                
                link_selectors = ["h2 a", "a.a-link-normal[href*='/dp/']", "a.a-link-normal:first-child"]
                for l_sel in link_selectors:
                    try:
                        l_el = await item.query_selector(l_sel)
                        if l_el:
                            href = await l_el.evaluate("el => el.href")
                            if href and "/dp/" in href: break
                    except: continue

                if href and title != "N/A":
                    discovery_links.append({
                        "asin": asin,
                        "Amazon URL": href,
                        "Book Title": title
                    })
                    found_this_page += 1
                
                if len(discovery_links) >= TARGET_COUNT: break
            
            print(f"  Captured {found_this_page} new links from Page {page_num}.")
            
            if len(discovery_links) < TARGET_COUNT:
                next_btn = await page.query_selector('a.s-pagination-next')
                if next_btn:
                    page_num += 1
                    await asyncio.sleep(5)
                else:
                    print("  [End of Results] No more pages.")
                    break
        
        print(f"\nDiscovery finished. Collected {len(discovery_links)} links for range 40-100.")

        # Step 2: Extraction Phase (with Recovery)
        final_new_data = []
        if os.path.exists(RECOVERY_FILE):
            print(f"Loading recovered data from {RECOVERY_FILE}...")
            import json
            with open(RECOVERY_FILE, 'r') as f:
                final_new_data = json.load(f)
        
        if len(final_new_data) < len(discovery_links):
            start_idx = len(final_new_data)
            for i in range(start_idx, len(discovery_links), MAX_TABS):
                batch = discovery_links[i : i + MAX_TABS]
                print(f"\nBatch Processor: Handling {i+1} to {min(i + MAX_TABS, len(discovery_links))}...")
                
                tasks = [process_book(context, book) for book in batch]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                for res in results:
                    if isinstance(res, dict):
                        final_new_data.append(res)
                    else:
                        print(f"  [Error] Extraction failed: {res}")
                
                # Auto-save for recovery
                import json
                with open(RECOVERY_FILE, 'w') as f:
                    json.dump(final_new_data, f, indent=4)

        await browser.close()

        # Step 3: Injection Phase
        if os.path.exists(OUTPUT_FILE):
            print(f"\nUpdating {OUTPUT_FILE} (Rows 40 to 100)...")
            df_master = pd.read_excel(OUTPUT_FILE)
            df_new = pd.DataFrame(final_new_data)
            df_new = df_new.reindex(columns=COLUMNS)
            
            # Rows 40-100 corresponds to 0-indexed indices 39 to 99
            start_row = START_INDEX - 1
            num_to_replace = len(df_new)
            
            print(f"  Replacing {num_to_replace} rows starting from index {start_row}.")
            
            for i in range(num_to_replace):
                target_idx = start_row + i
                if target_idx < len(df_master):
                    df_master.iloc[target_idx] = df_new.iloc[i]
                else:
                    # Append if past original length
                    df_master = pd.concat([df_master, df_new.iloc[i:i+1]], ignore_index=True)
            
            with pd.ExcelWriter(OUTPUT_FILE, engine='openpyxl') as writer:
                df_master.to_excel(writer, index=False, sheet_name='Amazon Scraped Books')
            
            # Re-apply formatting
            save_to_excel([], OUTPUT_FILE) 
            
            print(f"REPAIR COMPLETE. {OUTPUT_FILE} has been updated for range 40-100.")
            if os.name == 'nt':
                os.startfile(os.path.abspath(OUTPUT_FILE))
        else:
            print(f"Error: {OUTPUT_FILE} not found.")

if __name__ == "__main__":
    asyncio.run(run_40_100_scrape())
