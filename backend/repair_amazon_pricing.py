import asyncio
import os
import sys
import re
import pandas as pd
from playwright.async_api import async_playwright

# Ensure backend folder is in path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from scraper import AmazonScraper, clean_text
from excel_utility import save_to_excel

# Configuration
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_FILE = r"e:\Internship\Amazon Keyword - Paranormal Romance.xlsx"
OUTPUT_FILE = r"e:\Internship\Amazon Keyword - Paranormal Romance.xlsx"
MAX_CONCURRENT_TABS = 5
BATCH_LIMIT = 500  # As requested

def needs_pricing_repair(val):
    """Detect if pricing needs fix: N/A, empty, or INR/₹."""
    if pd.isna(val):
        return True
    s = str(val).strip()
    if s == "" or s.lower() == "n/a" or s.lower() == "nan":
        return True
    # If contains INR or Rupee symbol
    if "INR" in s or "₹" in s or "\u20b9" in s:
        return True
    # If it DOES NOT contain a dollar sign and has currency-like text
    if "$" not in s and "USD" not in s:
        return True
    return False

async def repair_amazon_pricing():
    if not os.path.exists(INPUT_FILE):
        print(f"Error: {INPUT_FILE} not found.")
        return

    print(f"Loading industrial file: {INPUT_FILE}")
    df = pd.read_excel(INPUT_FILE)
    
    # Identify rows needing repair
    to_repair_mask = df['Price_Tier'].apply(needs_pricing_repair)
    to_repair_indices = df.index[to_repair_mask][:BATCH_LIMIT]
    
    if len(to_repair_indices) == 0:
        print("Coverage is already 100% USD. No repair needed.")
        return
        
    print(f"\nLocked onto: {len(to_repair_indices)} books for USD conversion.")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
        )
        
        amazon_scraper = AmazonScraper()
        
        # Phase 1: Set US Location & CAPTCHA Gate
        print("Spoofing Location to US (90016) to force USD rendering...")
        page = await context.new_page()
        try:
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
        except Exception as e:
            print(f"  Location setup error: {e}")
            
        await page.close()
        
        # Phase 2: Extraction Loop
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_TABS)
        progress = [0]
        total = len(to_repair_indices)
        
        async def process_price_row(idx):
            async with semaphore:
                row = df.loc[idx]
                # Add random jitter
                await asyncio.sleep(1.0)
                
                url = str(row.get("Amazon URL", ""))
                title = str(row.get("Book Title", "N/A"))
                
                if not url or url == "nan" or "amazon.com" not in url:
                    print(f"  [{idx}] Skipped: No valid URL for {title[:20]}")
                    return
                        
                progress[0] += 1
                curr = progress[0]
                safe_title = title[:30].encode('ascii', 'ignore').decode('ascii')
                print(f"[{curr}/{total}] Fetching USD Price for: {safe_title}...")
                
                try:
                    details = await amazon_scraper.scrape_product_details_tab(context, url)
                    price_raw = details.get("Price", "N/A")
                    
                    if price_raw != "N/A":
                        # Standardize to multi-price format
                        price_tier = price_raw.replace('\n', ' | ')
                        
                        # --- NEW: INR to USD Fallback ---
                        if "INR" in price_tier or "₹" in price_tier or "\u20b9" in price_tier:
                            try:
                                # Extract numbers, convert, and rebuild string
                                # Simple rate: 1 USD = 83 INR
                                def inr_to_usd(match):
                                    inr_val = float(match.group(1).replace(',', ''))
                                    usd_val = round(inr_val / 83.0, 2)
                                    return f"${usd_val}"
                                
                                price_tier = re.sub(r'(?:INR|₹|\u20b9)\s*([\d,]+\.?\d*)', inr_to_usd, price_tier)
                                print(f"  -> CONVERTED [{curr}]: {price_tier}")
                            except Exception as ce:
                                print(f"  -> Conversion warning: {ce}")
                        
                        df.at[idx, 'Price_Tier'] = price_tier
                        print(f"  -> SUCCESS [{curr}]: Found {price_tier}")
                    else:
                        print(f"  -> FAILED [{curr}]: No price detected.")
                except Exception as e:
                    print(f"  -> ERROR [{curr}]: {e}")
                    
        # Launch tasks
        tasks = [process_price_row(idx) for idx in to_repair_indices]
        await asyncio.gather(*tasks)
        
        await browser.close()
        
        print("\nSaving updated prices into master Excel file...")
        save_to_excel(df.to_dict('records'), OUTPUT_FILE)
        
        print("USD CONVERSION COMPLETE.")
        if os.name == 'nt':
            try:
                os.startfile(os.path.abspath(OUTPUT_FILE))
            except: pass

if __name__ == "__main__":
    asyncio.run(repair_amazon_pricing())
