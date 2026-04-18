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

# Use absolute paths to be safe when running from different directories
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_FILE = os.path.join(BASE_DIR, "../scraped_data_keywords.xlsx")
RECOVERY_FILE = os.path.join(BASE_DIR, "repair_recovery_50.json")
REPAIR_COUNT = 50
MAX_TABS = 12

async def run_0_50_repair():
    print(f"--- STARTING INDUSTRIAL REPAIR (0-50): Targeted Search Pass ---")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
        )
        page = await context.new_page()

        # Step 1: Discovery Phase (First 50 links from Page 1)
        print(f"Navigating to Amazon Search (Page 1)...")
        await page.goto(SEARCH_URL, wait_until="load", timeout=60000)
        
        amazon_scraper = AmazonScraper()
        await amazon_scraper.set_amazon_location(page, "90016")
        
        discovery_links = []
        
        print(f"\n[Scanning Page 1] INDUSTRIAL SCROLL to reveal all titles...")
        for i in range(6):
            await page.evaluate("window.scrollBy(0, 1500)")
            await asyncio.sleep(1.5)
        
        items = await page.query_selector_all("[data-asin]")
        for item in items:
            asin = await item.get_attribute('data-asin')
            if not asin or asin == "N/A" or len(asin) < 5: continue
            
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
            
            if len(discovery_links) >= REPAIR_COUNT: break
        
        # Fallback to next page if not enough links on page 1
        if len(discovery_links) < REPAIR_COUNT:
            next_btn = await page.query_selector('a.s-pagination-next')
            if next_btn:
                print(f"  Only found {len(discovery_links)} on Page 1. Flipping to Page 2...")
                await next_btn.click()
                await asyncio.sleep(5)
                # ... Simplified: just continue if needed, but Page 1 usually has ~48-60
        
        print(f"\nDiscovery finished. Collected {len(discovery_links)} links for repair.")

        # Step 2: Extraction Phase
        final_new_data = []
        
        # --- RECOVERY LOGIC ---
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

        # Step 3: Injection Phase (Merge back into Master Excel)
        if os.path.exists(OUTPUT_FILE):
            print(f"\nUpdating {OUTPUT_FILE} index 0-50...")
            df_master = pd.read_excel(OUTPUT_FILE)
            df_new = pd.DataFrame(final_new_data)
            
            # Reindex new data to match master columns
            df_new = df_new.reindex(columns=COLUMNS)
            
            # Replace the first N rows
            num_to_replace = min(len(df_new), len(df_master))
            print(f"  Replacing {num_to_replace} rows at the top.")
            
            # Using .iloc to replace the range
            for i in range(num_to_replace):
                df_master.iloc[i] = df_new.iloc[i]
            
            # If we got MORE than 50 (unlikely with REPAIR_COUNT=50 but safe), handle it
            if len(df_new) > len(df_master):
                df_master = pd.concat([df_new, df_master.iloc[len(df_new):]], ignore_index=True)
            
            # Save using the established utility for formatting
            from excel_utility import save_to_excel
            # save_to_excel normally appends/deduplicates, we need to bypass or use df to save directly
            # I'll update excel_utility to allow a direct DF overwrite or just save here.
            # Actually, I'll modify the save_to_excel to accept a dataframe directly or just save it here.
            
            # For now, I'll save it here and then re-open to apply formatting if needed
            with pd.ExcelWriter(OUTPUT_FILE, engine='openpyxl') as writer:
                df_master.to_excel(writer, index=False, sheet_name='Amazon Scraped Books')
            
            # Re-apply formatting by calling save_to_excel with an empty list (it will read the file and re-save with formatting)
            save_to_excel([], OUTPUT_FILE) 
            
            print(f"REPAIR COMPLETE. {OUTPUT_FILE} has been updated.")
            if os.name == 'nt':
                os.startfile(os.path.abspath(OUTPUT_FILE))
        else:
            print(f"Error: {OUTPUT_FILE} not found to repair.")

if __name__ == "__main__":
    asyncio.run(run_0_50_repair())
