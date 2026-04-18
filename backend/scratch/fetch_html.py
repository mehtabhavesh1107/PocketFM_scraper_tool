import asyncio
from playwright.async_api import async_playwright

async def run():
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await browser.new_page()
        print("Navigating...")
        await page.goto('https://www.goodreads.com/book/show/249094067-scales-make-three', wait_until="domcontentloaded")
        content = await page.content()
        with open('book_page.html', 'w', encoding='utf-8') as f:
            f.write(content)
        print("Saved to book_page.html")
        await browser.close()

if __name__ == "__main__":
    asyncio.run(run())
