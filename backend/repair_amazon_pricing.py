import asyncio
import os
import sys
import pandas as pd
from playwright.async_api import async_playwright

# Ensure backend folder is in path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from scraper import AmazonScraper
from excel_utility import save_to_excel

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_FILE = os.path.join(BASE_DIR, "../scraped_data_keywords.xlsx")
MAX_CONCURRENT_TABS = 3  # Controlled concurrency to handle 500 rows sustainably

async def repair_amazon_pricing():
    if not os.path.exists(OUTPUT_FILE):
        print(f"Error: {OUTPUT_FILE} not found.")
        return

    print("Loading master Excel file to sweep Rows 0 to 500...")
    df = pd.read_excel(OUTPUT_FILE)
    
    # Target Phase 1 Batch: 0 to 500
    START_INDEX = 0
    END_INDEX = 500
    
    to_repair = df.iloc[START_INDEX:END_INDEX+1].copy()
    
    if to_repair.empty:
        print("No rows found in this range!")
        return
        
    print(f"\nLocked onto: Rows {START_INDEX} through {END_INDEX} ({len(to_repair)} books).")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
        )
        
        amazon_scraper = AmazonScraper()
        
        # Phase 1: Set US Location & CAPTCHA Gate
        print("Spoofing Location to US (90016) to force USD rendering...")
        page = await context.new_page()
        await page.goto("https://www.amazon.com/", wait_until="load", timeout=60000)
        
        # --- CAPTCHA GATE ---
        print("\n" + "!"*60)
        print("  ACTION REQUIRED: The Amazon tab is open.")
        print("  If you see an Amazon CAPTCHA (puzzle or characters), please solve it NOW in the browser window.")
        print("  Waiting 30 seconds for manual bypass...")
        print("!"*60 + "\n")
        
        for i in range(30, 0, -5):
            print(f"  Resuming in {i} seconds...")
            await asyncio.sleep(5)
            
        print("\nRunning US Zip Code Spoof (90016)...")
        await amazon_scraper.set_amazon_location(page, "90016")
        await page.close()
        
        # Phase 2: Extraction Loop
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_TABS)
        progress = [0]
        total = len(to_repair)
        
        async def process_price_row(idx, row):
            async with semaphore:
                # Add random jitter
                await asyncio.sleep(0.5)
                
                url = str(row.get("Amazon URL", ""))
                title = str(row.get("Book Title", "N/A"))
                
                if not url or url == "nan" or "amazon.com" not in url:
                    # Construct URL from ASIN if missing link
                    asin = str(row.get("ASIN", ""))
                    if asin and asin != "nan":
                        url = f"https://www.amazon.com/dp/{asin}"
                    else:
                        print(f"  [{idx}] Skipped: No URL or ASIN for {title[:20]}")
                        return
                        
                progress[0] += 1
                curr = progress[0]
                print(f"[{curr}/{total}] Fetching USD Price for: {title[:30]}...")
                
                try:
                    details = await amazon_scraper.scrape_product_details_tab(context, url)
                    price_raw = details.get("Price", "N/A")
                    
                    if price_raw != "N/A":
                        # Standardize to our established multi-price format
                        price_tier = price_raw.replace('\n', ' | ')
                        df.at[idx, 'Price_Tier'] = price_tier
                        print(f"  -> SUCCESS [{curr}]: Found USD {price_tier}")
                    else:
                        print(f"  -> FAILED [{curr}]: No price detected.")
                except Exception as e:
                    print(f"  -> ERROR [{curr}]: {e}")
                    
        # Launch tasks
        tasks = [process_price_row(idx, row) for idx, row in to_repair.iterrows()]
        await asyncio.gather(*tasks)
        
        await browser.close()
        
        print("\nSaving updated prices into master Excel file...")
        save_to_excel(df.to_dict('records'), OUTPUT_FILE)
        
        print("USD CONVERSION COMPLETE.")
        if os.name == 'nt':
            os.startfile(os.path.abspath(OUTPUT_FILE))

if __name__ == "__main__":
    asyncio.run(repair_amazon_pricing())
