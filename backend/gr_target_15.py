import asyncio
import os
import sys
import pandas as pd
from playwright.async_api import async_playwright

# Ensure backend folder is in path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from scraper import GoodreadsScraper, clean_numeric
from excel_utility import save_to_excel

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_FILE = os.path.join(BASE_DIR, "../scraped_data_keywords.xlsx")

TARGET_COUNT = 15
MAX_CONCURRENT_TABS = 5 # Small batch, keep it stable

async def run_targeted_gr_scan():
    if not os.path.exists(OUTPUT_FILE):
        print(f"Error: {OUTPUT_FILE} not found.")
        return

    print(f"--- STARTING TARGETED GOODREADS SCAN (Rows 1-15) ---")
    df = pd.read_excel(OUTPUT_FILE)
    
    # Process only first 15
    to_process = df.iloc[:TARGET_COUNT].copy()
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
        )
        
        # Mandatory Login
        login_page = await context.new_page()
        await login_page.goto("https://www.goodreads.com/user/sign_in")
        print("\nACTION REQUIRED: Please log in to Goodreads. Script will resume automatically once detected.\n")
        
        logged_in = False
        login_selectors = ['a[href*="sign_out"]', '.Header_userProfile', '[data-testid="notificationsIcon"]']
        for _ in range(120): # 10 minute wait
            for sel in login_selectors:
                try:
                    if await login_page.locator(sel).is_visible():
                        logged_in = True
                        break
                except: pass
            if logged_in: break
            await asyncio.sleep(5)
        
        if not logged_in:
            print("Login timeout. Exiting.")
            await browser.close()
            return
            
        print("Login detected! Processing records...")
        await login_page.close()
        
        gr_scraper = GoodreadsScraper()
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_TABS)

        async def process_row(idx, row):
            async with semaphore:
                title = str(row.get("Book Title", "N/A"))
                author = str(row.get("Author Name", "N/A"))
                asin = str(row.get("ASIN", "N/A"))
                if asin == "nan": asin = "N/A"
                
                print(f"  [Scan] {title[:30]} by {author}")
                
                # Combined Scan (Book -> Series)
                data = await gr_scraper.scrape_goodreads_data(context, title, author, asin=asin)
                
                if data:
                    print(f"    -> Result: Series={data.get('GoodReads_Series_URL') != 'N/A'} | Pages={data.get('Total_Pages_Primary_Books')}")
                    # Update specific cols S-W
                    # Excel Columns: S=19, T=20, U=21, V=22, W=23 (1-indexed)
                    # DF Columns by name
                    df.at[idx, "GoodReads_Series_URL"] = data.get("GoodReads_Series_URL", "N/A")
                    df.at[idx, "Num_Primary_Books"] = data.get("Num_Primary_Books", "N/A")
                    df.at[idx, "Total_Pages_Primary_Books"] = data.get("Total_Pages_Primary_Books", "N/A")
                    df.at[idx, "Book1_Rating"] = data.get("Book1_Rating", "N/A")
                    df.at[idx, "Book1_Num_Ratings"] = data.get("Book1_Num_Ratings", "N/A")
                else:
                    print(f"    -> Failed for {title[:30]}")

        tasks = [process_row(idx, row) for idx, row in to_process.iterrows()]
        await asyncio.gather(*tasks)

        await browser.close()

        # Save result
        print(f"\nSaving results to {OUTPUT_FILE}...")
        # Convert df back to records list for save_to_excel
        save_to_excel(df.to_dict('records'), OUTPUT_FILE)
        
        print("SCAN COMPLETE.")
        if os.name == 'nt':
            os.startfile(os.path.abspath(OUTPUT_FILE))

if __name__ == "__main__":
    asyncio.run(run_targeted_gr_scan())
