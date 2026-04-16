import asyncio
import unicodedata
import re
from playwright.async_api import async_playwright


def clean_text(text):
    if not text:
        return ""
    text = unicodedata.normalize('NFKD', text)
    text = re.sub(r'[\u200b\u200c\u200d\ufeff]', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def clean_numeric(text):
    if not text:
        return 0
    match = re.search(r'[\d,.]+', text)
    if match:
        clean_val = match.group(0).replace(',', '')
        try:
            if clean_val == '.':
                return 0
            return float(clean_val)
        except (ValueError, TypeError):
            return 0
    return 0


def normalize_title_for_search(title):
    if not title:
        return ""
    # Standard cleanup
    t = title.lower()
    
    # Remove common Amazon subtitles and fluff
    remove_patterns = [
        r':\s+a\s+novel.*', 
        r':\s+a\s+read\s+with\s+jenna\s+pick.*',
        r':\s+a\s+memoir.*',
        r'\(deluxe\s+edition\).*',
        r'\(special\s+edition\).*',
        r'\(.*book\s+\d+.*\)', # Remove (Series Name Book 1)
        r'\(.*series.*\)',
        r'\[.*\]', # Remove [Brackets]
        r'book\s+\d+.*', # Remove standalone Book 1
        r'\d+\s+of\s+\d+.*', # Remove 1 of 3
        r'a\s+dark\s+fantasy.*', # Remove genre tags
        r'an\s+addictive\s+fantasy.*',
    ]
    
    for pattern in remove_patterns:
        t = re.sub(pattern, '', t, flags=re.IGNORECASE)
    
    # Take only part before first colon/dash for broad search
    t = re.split(r'[:\-—\(]', t)[0]
    
    # Clean up punctuation
    t = re.sub(r'[^\w\s]', '', t)
    return t.strip()


class AmazonScraper:
    def __init__(self, headless=False):
        self.headless = headless

    async def scrape_bestseller_list(self, url, limit=10):
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=self.headless)
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
            )
            page = await context.new_page()

            try:
                # 'load' waits for full page including any redirect chains
                print(f"Opening Amazon: {url}")
                await page.goto(url, wait_until="load", timeout=60000)

                # --- AMAZON LOGIN GATE ---
                print("\n" + "!" * 60)
                print("  ACTION REQUIRED: MANUALLY CLEAR AMAZON BLOCKS")
                print("  1. Solve any CAPTCHAs if they appear.")
                print("  2. Ensure you are on the correct Bestseller page.")
                print("  The automation will begin once the search bar is visible.")
                print("!" * 60 + "\n")

                try:
                    # Wait for Search bar or Logo as proof of page loading
                    await page.wait_for_selector('#twotabsearchtextbox, #nav-logo-sprites', timeout=300000)
                    print("  [OK] Amazon page confirmed. Starting Deep Scan...")
                except Exception:
                    print("  [Time Out] Login wait exceeded. Attempting to proceed...")

                unique_results = []
                seen_asin = set()
                page_num = 1

                while len(unique_results) < limit:
                    print(f"\nScanning Amazon Page {page_num} (Total gathered: {len(unique_results)})...")
                    
                    # Scroll to trigger lazy-loading
                    try:
                        await page.evaluate("""async () => {
                            await new Promise((resolve) => {
                                let totalHeight = 0;
                                let distance = 100;
                                let timer = setInterval(() => {
                                    let scrollHeight = document.body.scrollHeight;
                                    window.scrollBy(0, distance);
                                    totalHeight += distance;
                                    if (totalHeight >= scrollHeight) {
                                        clearInterval(timer);
                                        resolve();
                                    }
                                }, 100);
                            });
                        }""")
                        await asyncio.sleep(1)
                    except Exception as scroll_err:
                        print(f"Scroll skipped: {scroll_err}")

                    try:
                        await page.wait_for_load_state("networkidle", timeout=8000)
                    except Exception:
                        pass

                    # --- Item selection ---
                    items = await page.query_selector_all('[data-asin]')
                    
                    for item in items:
                        asin = await item.get_attribute('data-asin')
                        if not asin or asin in seen_asin or asin == "N/A":
                            continue
                        
                        # --- Title Extraction ---
                        title_el = await item.query_selector('.p13n-sc-untruncated-desktop-title, ._cDE_gridItem_truncate-title')
                        raw_title = ""
                        if title_el:
                            raw_title = clean_text(await title_el.inner_text())
                        
                        if not raw_title:
                            img_el = await item.query_selector('img')
                            if img_el:
                                alt_val = await img_el.get_attribute('alt')
                                if alt_val and len(alt_val) > 3:
                                    raw_title = clean_text(alt_val)

                        if not raw_title or "formats available" in raw_title.lower():
                            continue

                        # --- Rank Extraction ---
                        rank_text = "N/A"
                        for rank_sel in ['.zg-bdg-text', '.p13n-sc-badge-label-size-base', 'span.zg-badge-text']:
                            el = await item.query_selector(rank_sel)
                            if el:
                                t = clean_text(await el.inner_text())
                                if t and re.search(r'\d', t):
                                    rank_text = t.lstrip('#').strip()
                                    break

                        # --- Basic Data ---
                        rating_el  = await item.query_selector('.a-icon-star-small .a-icon-alt, [class*="star"]')
                        reviews_el = await item.query_selector('[aria-label*="ratings"], [aria-label*="reviews"]')
                        link_el    = await item.query_selector('a.a-link-normal[href*="/dp/"], a.a-link-normal')

                        # --- Author Extraction ---
                        author_name = "N/A"
                        for author_selector in ['a[href*="/e/"]', 'div.a-row.a-size-small a', '[class*="author"] a']:
                            try:
                                el = await item.query_selector(author_selector)
                                if el:
                                    text = clean_text(await el.inner_text())
                                    if text and len(text) > 1 and not re.match(r'^[\d\.\$,]+$', text):
                                        author_name = text
                                        break
                            except: continue

                        # URL Extraction
                        raw_href = ""
                        if link_el:
                            try: raw_href = await link_el.evaluate("el => el.href")
                            except: raw_href = await link_el.get_attribute('href')

                        amazon_url = raw_href
                        if asin:
                            if raw_href and raw_href.startswith('http'):
                                domain = "/".join(raw_href.split("/", 3)[:3])
                                amazon_url = f"{domain}/dp/{asin}"
                            else:
                                amazon_url = f"/dp/{asin}"

                        # Reviews count
                        reviews_count = 0
                        if reviews_el:
                            aria = await reviews_el.get_attribute('aria-label') or ""
                            m = re.search(r'stars?,?\s*([\d,]+)\s*ratings?', aria, re.IGNORECASE)
                            if m: reviews_count = int(m.group(1).replace(',', ''))

                        unique_results.append({
                            "Rank":              rank_text,
                            "Book Title":        raw_title,
                            "Author Name":       author_name,
                            "Rating":            clean_numeric(await rating_el.inner_text()) if rating_el else 0,
                            "Number of Reviews": reviews_count,
                            "Price":             "N/A",
                            "Amazon URL":        amazon_url
                        })
                        seen_asin.add(asin)
                        
                        if len(unique_results) >= limit:
                            break

                    if len(unique_results) >= limit:
                        break

                    # --- PAGINATION: Next Page ---
                    next_button = await page.query_selector('li.a-last a')
                    if next_button:
                        print("Navigating to Next Page...")
                        await next_button.click()
                        page_num += 1
                        # Wait for next page scan proof
                        await page.wait_for_selector('[data-asin]', timeout=30000)
                        await asyncio.sleep(3) # Anti-bot delay
                    else:
                        print("No more pages found.")
                        break

                print(f"Total books gathered: {len(unique_results)}")
                return unique_results

            finally:
                await browser.close()

    async def scrape_product_details_tab(self, context, url, base_url="https://www.amazon.com"):
        if not url:
            return {
                "Description": "N/A", "Publisher": "N/A", "Publication Date": "N/A", 
                "Author Name": "N/A", "Price": "N/A", "Series": "N/A", 
                "Pages": "N/A", "Inner Rank": "N/A"
            }
        if not url.startswith('http'):
            url = base_url.rstrip('/') + url

        page = await context.new_page()
        try:
            # domcontentloaded is fast and sufficient — detail elements are in DOM immediately
            await page.goto(url, wait_until="domcontentloaded", timeout=60000)
            # Give lazy sections 2s to render without waiting for full networkidle
            await asyncio.sleep(2)

            # --- Description ---
            description = "N/A"
            for desc_sel in [
                '#bookDescription_feature_div noscript',
                '#bookDescription_feature_div',
                '#productDescription',
                '#bookDescription',
                '#book-description-widget',
                '[data-a-expander-name="book_description_expander"]',
            ]:
                desc_el = await page.query_selector(desc_sel)
                if desc_el:
                    description = clean_text(await desc_el.inner_text())
                    if description and len(description) > 10:
                        break

            # --- Author ---
            author = "N/A"
            for sel in [
                '#bylineInfo .author a',
                '#bylineInfo a.contributorNameID',
                '#bylineInfo .a-link-normal',
                '#byline a',
                '.author .a-link-normal',
                'span.author a',
            ]:
                el = await page.query_selector(sel)
                if el:
                    text = clean_text(await el.inner_text())
                    if (text and len(text) > 1
                        and not re.match(r'^[\d\.\$,]+$', text)
                        and 'out of' not in text.lower()
                        and 'stars' not in text.lower()):
                        author = text
                        break

            if author == "N/A":
                byline_el = await page.query_selector('#bylineInfo, #byline')
                if byline_el:
                    byline_text = clean_text(await byline_el.inner_text())
                    m = re.search(r'\bby\s+([A-Z][A-Za-z\s\.\-\']+?)(?:\s*[\(,;|]|$)', byline_text, re.IGNORECASE)
                    if m:
                        author = m.group(1).strip()

            # --- Publisher & Publication Date ---
            # Strategy 1: bullet list items (most common layout)
            publisher = "N/A"
            pub_date = "N/A"

            for sel in [
                '#detailBullets_feature_div li',
                '#productDetails_detailBullets_sections1 tr',
                '#productDetails_techSpec_section_1 tr',
                '#productDetailsTable tr',
                '.detail-bullet-list li',
                '#bookDetails_feature_div .a-list-item',
                '#richProductInformation_feature_div .a-section',
                '#rpiTable tr',
                '.rpi-attribute-value',
            ]:
                items = await page.query_selector_all(sel)
                for item in items:
                    text = clean_text(await item.inner_text())
                    # Normalize separators: replace Unicode LRM/RLM with colon
                    text = re.sub(r'[\u200e\u200f\u200b]+', ':', text)
                    text = re.sub(r'\s*:\s*', ': ', text)

                    if publisher == "N/A" and re.search(r'\bpublisher\b', text, re.IGNORECASE):
                        # Extract value after "Publisher :"
                        m = re.search(r'publisher\s*:\s*(.+)', text, re.IGNORECASE)
                        if m:
                            val = m.group(1).strip().lstrip(':').strip()
                            # Remove trailing junk like "(15 January 2017)"
                            val = re.sub(r'\s*\(\d+.*?\)\s*$', '', val).strip()
                            if val and len(val) > 1:
                                publisher = val

                    if pub_date == "N/A" and re.search(r'publication\s*date', text, re.IGNORECASE):
                        m = re.search(r'publication\s*date\s*:\s*(.+)', text, re.IGNORECASE)
                        if m:
                            val = m.group(1).strip().lstrip(':').strip()
                            if val and len(val) > 1:
                                pub_date = val

                if publisher != "N/A" and pub_date != "N/A":
                    break

            # Strategy 2: if still N/A, scan all text nodes on the page line by line
            if publisher == "N/A" or pub_date == "N/A":
                try:
                    page_text = await page.evaluate("() => document.body.innerText")
                    lines = page_text.split('\n')
                    for i, line in enumerate(lines):
                        line = line.strip()
                        if not line:
                            continue

                        if publisher == "N/A" and re.search(r'\bpublisher\b', line, re.IGNORECASE):
                            m = re.search(r'publisher\s*[:\u200e\u200f]?\s*(.+)', line, re.IGNORECASE)
                            if m:
                                val = m.group(1).strip().lstrip(':').strip()
                                val = re.sub(r'\s*\(\d+.*?\)\s*$', '', val).strip()
                                if val and len(val) > 1:
                                    publisher = val
                            elif line.lower().strip() == 'publisher' and i + 1 < len(lines):
                                next_val = lines[i + 1].strip()
                                if next_val and len(next_val) > 1:
                                    publisher = next_val

                        if pub_date == "N/A" and re.search(r'publication\s*date', line, re.IGNORECASE):
                            m = re.search(r'publication\s*date\s*[:\u200e\u200f]?\s*(.+)', line, re.IGNORECASE)
                            if m:
                                val = m.group(1).strip().lstrip(':').strip()
                                if val and len(val) > 1:
                                    pub_date = val
                            elif i + 1 < len(lines):
                                next_val = lines[i + 1].strip()
                                if next_val and len(next_val) > 1:
                                    pub_date = next_val
                except Exception as e:
                    print(f"Text scan error: {e}")

            # Strategy 3: Aggressive JS scan of ALL elements on the page
            # Handles audiobook pages, carousel grids, and any other unknown layout
            if publisher == "N/A" or pub_date == "N/A":
                try:
                    deep_scan = await page.evaluate("""() => {
                        const result = { publisher: null, pubDate: null };
                        
                        // Scan EVERY element on the page
                        const allElements = document.querySelectorAll('*');
                        for (const el of allElements) {
                            // Only check leaf-ish elements (avoid huge containers)
                            if (el.children.length > 10) continue;
                            const txt = (el.textContent || '').trim();
                            if (txt.length > 500 || txt.length < 3) continue;
                            
                            const lower = txt.toLowerCase();
                            
                            // Publication date: look for element whose text IS "Publication date"
                            if (!result.pubDate && lower === 'publication date') {
                                // Value is in the next sibling or parent's next child
                                let next = el.nextElementSibling;
                                if (next) {
                                    const val = next.textContent.trim();
                                    if (val.length > 3 && val.length < 50) result.pubDate = val;
                                }
                                if (!result.pubDate && el.parentElement) {
                                    const parent = el.parentElement;
                                    const siblings = parent.parentElement ? parent.parentElement.children : [];
                                    let found = false;
                                    for (const sib of siblings) {
                                        if (found) {
                                            const val = sib.textContent.trim();
                                            if (val.length > 3 && val.length < 50) {
                                                result.pubDate = val;
                                                break;
                                            }
                                        }
                                        if (sib === parent) found = true;
                                    }
                                }
                            }
                            
                            // Publisher: look for element whose text IS "Publisher"
                            if (!result.publisher && lower === 'publisher') {
                                let next = el.nextElementSibling;
                                if (next) {
                                    const val = next.textContent.trim();
                                    if (val.length > 1 && val.length < 100) result.publisher = val;
                                }
                                if (!result.publisher && el.parentElement) {
                                    const parent = el.parentElement;
                                    const siblings = parent.parentElement ? parent.parentElement.children : [];
                                    let found = false;
                                    for (const sib of siblings) {
                                        if (found) {
                                            const val = sib.textContent.trim();
                                            if (val.length > 1 && val.length < 100) {
                                                result.publisher = val;
                                                break;
                                            }
                                        }
                                        if (sib === parent) found = true;
                                    }
                                }
                            }
                        }
                        
                        return result;
                    }""")
                    if deep_scan.get('publisher') and publisher == "N/A":
                        publisher = clean_text(deep_scan['publisher'])
                    if deep_scan.get('pubDate') and pub_date == "N/A":
                        pub_date = clean_text(deep_scan['pubDate'])
                except Exception as e:
                    print(f"Deep scan error: {e}")

            # Strategy 4: Python regex on full page text for date patterns near "publication date"
            if pub_date == "N/A":
                try:
                    full_text = await page.evaluate("() => document.body.innerText")
                    # Look for date patterns like "September 14, 2021" near "Publication date"
                    m = re.search(
                        r'publication\s*date[\s\S]{0,50}?'
                        r'((?:January|February|March|April|May|June|July|August|September|October|November|December)'
                        r'\s+\d{1,2},?\s+\d{4})',
                        full_text, re.IGNORECASE
                    )
                    if m:
                        pub_date = m.group(1).strip()
                except Exception:
                    pass

            # ====== STRUCTURED PRICE EXTRACTION ======
            # Extract all format prices from the product page's format switcher (#tmmSwatches)
            # Output format: "Kindle - INR 92.13\nHardcover - INR 1,674.20\nPaperback - INR 850.00"
            price_lines = []
            seen_formats = set()
            try:
                # Use ONLY .a-button-inner (innermost) — NOT li which wraps it and causes duplicates
                format_items = await page.query_selector_all(
                    '#tmmSwatches .a-button-inner, '
                    '[id*="tmm-grid-swatch"] .a-button-inner'
                )
                for fi in format_items:
                    text = clean_text(await fi.inner_text())
                    if not text:
                        continue
                    # Text looks like "Kindle\nINR 92.13" or "Kindle Edition\n₹92.13"
                    parts = [p.strip() for p in text.split('\n') if p.strip()]
                    if len(parts) >= 2:
                        format_name = parts[0]
                        # Skip if we already have this format (dedup)
                        if format_name.lower() in seen_formats:
                            continue
                        seen_formats.add(format_name.lower())
                        # Find the price part (contains digits and currency)
                        price_part = next(
                            (p for p in parts[1:] if re.search(r'[\d,\.]+', p) and
                             re.search(r'[\u20b9\$\£\€]|INR|USD|GBP|EUR|Rs\.?', p, re.IGNORECASE)),
                            parts[-1]
                        )
                        price_clean = re.sub(r'\s+', ' ', price_part).strip()
                        if format_name and price_clean:
                            price_lines.append(f"{format_name} - {price_clean}")
            except Exception as e:
                print(f"Price extraction error: {e}")

            # Fallback: if tmmSwatches gave nothing, try JS scan
            if not price_lines:
                try:
                    js_prices = await page.evaluate("""() => {
                        const results = [];
                        const seen = new Set();
                        const swatches = document.querySelectorAll(
                            '#tmmSwatches .a-button-inner, .swatchElement .a-button-inner'
                        );
                        for (const sw of swatches) {
                            const txt = (sw.innerText || '').trim();
                            if (!txt) continue;
                            const lines = txt.split('\\n').map(l => l.trim()).filter(Boolean);
                            if (lines.length >= 2) {
                                const fmt = lines[0];
                                if (seen.has(fmt.toLowerCase())) continue;
                                seen.add(fmt.toLowerCase());
                                const price = lines.find(l => /[\\d,\\.]+/.test(l) && l !== fmt);
                                if (fmt && price) results.push(fmt + ' - ' + price);
                            }
                        }
                        return results;
                    }""")
                    if js_prices:
                        price_lines = js_prices
                except Exception:
                    pass

            price_str = "\n".join(price_lines) if price_lines else "N/A"

            # ====== NEW: SERIES, PAGES, INNER RANK EXTRACTION ======
            series_name = "N/A"
            book_number = "N/A"
            total_books_in_series = "N/A"
            pages = "N/A"
            inner_rank = "N/A"

            try:
                # 1. Series info (e.g., "Book 1 of 3: ...")
                series_el = await page.query_selector('#seriesBulletWidget_feature_div, #bookSeries_feature_div, .series-link')
                if series_el:
                    series_text = clean_text(await series_el.inner_text())
                    m = re.search(r'Book\s+(\d+)\s+of\s+(\d+)\s*:\s*(.+)', series_text, re.IGNORECASE)
                    if m:
                        book_number = m.group(1).strip()
                        total_books_in_series = m.group(2).strip()
                        series_name = m.group(3).strip()
                    else:
                        m2 = re.search(r'Part\s+of\s*:\s*(.+)', series_text, re.IGNORECASE)
                        if m2: series_name = m2.group(1).strip()

                # 2. Pages (Print length)
                for page_sel in ['#detailBullets_feature_div li', '#rpiTable tr', '.rpi-attribute-value']:
                    els = await page.query_selector_all(page_sel)
                    for el in els:
                        t = clean_text(await el.inner_text())
                        if 'print length' in t.lower() or 'pages' in t.lower():
                            m = re.search(r'(\d+)\s*pages', t, re.IGNORECASE)
                            if m:
                                pages = m.group(1).strip()
                                break
                    if pages != "N/A": break

                # 3. Best Sellers Rank (inner)
                rank_container = await page.query_selector('#detailBullets_feature_div, #productDetails_db_sections')
                if rank_container:
                    rank_text_full = clean_text(await rank_container.inner_text())
                    rank_matches = re.findall(r'#[\d,]+\s+in\s+[^(\n]+', rank_text_full)
                    if rank_matches:
                        inner_rank = " | ".join(rank_matches[:3])
            except Exception as e:
                print(f"Detail enrichment error: {e}")

            # Final cleanup: strip any leading colons, spaces, Unicode markers from all values
            publisher = re.sub(r'^[\s:;\u200e\u200f\u200b]+', '', publisher).strip() if publisher != "N/A" else "N/A"
            pub_date = re.sub(r'^[\s:;\u200e\u200f\u200b]+', '', pub_date).strip() if pub_date != "N/A" else "N/A"

            title_safe = url.split('/dp/')[0].rsplit('/', 1)[-1][:40] if '/dp/' in url else 'unknown'
            print(f"  [{title_safe[:30]}] Author={author} | Pub={publisher} | Date={pub_date} | Prices={len(price_lines)}")

            return {
                "Description":      description,
                "Publisher":        publisher,
                "Publication Date": pub_date,
                "Author Name":      author,
                "Price":            price_str,
                "Amazon URL":       page.url,
                "Series Name":      series_name,
                "Book Number":      book_number,
                "Total Books":      total_books_in_series,
                "Pages":            pages,
                "Inner Rank":       inner_rank
            }
        except Exception as e:
            print(f"Error scraping {url}: {e}")
            return {
                "Description": "N/A", "Publisher": "N/A", "Publication Date": "N/A", 
                "Author Name": "N/A", "Price": "N/A", "Series Name": "N/A",
                "Book Number": "N/A", "Total Books": "N/A", "Pages": "N/A", "Inner Rank": "N/A"
            }
        finally:
            await page.close()


class GoodreadsScraper:
    def __init__(self, headless=False):
        self.headless = headless

    async def scrape_goodreads_data(self, context, title, author, isbn10="N/A", isbn13="N/A", asin="N/A"):
        if not title or title == "N/A":
            return {}

        page = await context.new_page()
        try:
            book_url = None
            
            # --- TIER 1: Direct ID Strategy (Most Reliable) ---
            potential_ids = [isbn13, isbn10, asin]
            for pid in potential_ids:
                if pid and pid != "N/A":
                    print(f"  Goodreads: Discovery Tier 1 (Direct ID {pid})...")
                    direct_url = f"https://www.goodreads.com/book/isbn/{pid}"
                    try:
                        await page.goto(direct_url, wait_until="domcontentloaded", timeout=20000)
                        if "goodreads.com/book/show/" in page.url or "goodreads.com/work/" in page.url:
                            book_url = page.url
                            print(f"  Goodreads: Successful direct access: {book_url}")
                            break
                    except Exception:
                        continue
            
            # --- TIER 2: Internal Search (Safe since we are logged in) ---
            if not book_url:
                print(f"  Goodreads: Discovery Tier 2 (Internal Search)...")
                clean_title = normalize_title_for_search(title)
                search_query = f"{clean_title} {author}"
                search_url = f"https://www.goodreads.com/search?q={search_query.replace(' ', '+')}"
                try:
                    await page.goto(search_url, wait_until="domcontentloaded", timeout=30000)
                    
                    # Get all search results and filter for the best match
                    result_rows = await page.query_selector_all('tr[itemtype="http://schema.org/Book"]')
                    for row in result_rows:
                        row_text = (await row.inner_text()).lower()
                        # Skip summaries/guides
                        if any(x in row_text for x in ['summary', 'analysis', 'study guide', 'workbook']):
                            continue
                        
                        # Verify author if possible
                        author_link = await row.query_selector('.authorNameRes')
                        if author_link:
                            res_author = (await author_link.inner_text()).lower()
                            if author.lower() in res_author or any(part in res_author for part in author.lower().split()):
                                book_link = await row.query_selector('a.bookTitle')
                                if book_link:
                                    book_url = await book_link.evaluate("el => el.href")
                                    print(f"  Goodreads: Found via Internal Search (Author Match): {book_url}")
                                    break
                    
                    # Fallback to first non-summary if no author match
                    if not book_url and result_rows:
                        first_book = await page.query_selector('a.bookTitle, [data-testid="bookTitle"] a')
                        if first_book:
                            book_url = await first_book.evaluate("el => el.href")
                            print(f"  Goodreads: Found via Internal Search (First Result): {book_url}")
                except Exception as ie:
                    print(f"  Goodreads: Internal search failed: {ie}")

            # --- TIER 3: Brave Discovery (Broad Fallback) ---
            if not book_url:
                print(f"  Goodreads: Discovery Tier 3 (Brave Fallback)...")
                search_query = f"{normalize_title_for_search(title)} {author} goodreads"
                brave_url = f"https://search.brave.com/search?q={search_query.replace(' ', '+')}"
                try:
                    await page.goto(brave_url, wait_until="domcontentloaded", timeout=30000)
                    links = await page.query_selector_all('a[href*="goodreads.com/book/show/"]')
                    for link in links:
                        link_text = (await link.inner_text()).lower()
                        href = await link.evaluate("el => el.href")
                        # Skip summaries/guides
                        if any(x in link_text for x in ['summary', 'analysis', 'study guide', 'workbook']):
                            continue
                        book_url = href
                        print(f"  Goodreads: Found via Brave: {book_url}")
                        break
                except Exception:
                    pass

            # --- TIER 4: DuckDuckGo HTML Fallback ---
            if not book_url:
                print(f"  Goodreads: Discovery Tier 4 (DuckDuckGo)...")
                search_query = f"{normalize_title_for_search(title)} {author} in goodreads"
                ddg_url = f"https://html.duckduckgo.com/html/?q={search_query.replace(' ', '+')}"
                try:
                    await page.goto(ddg_url, wait_until="domcontentloaded", timeout=30000)
                    links = await page.query_selector_all('a[href*="goodreads.com/book/show/"]')
                    for link in links:
                        link_text = (await link.inner_text()).lower()
                        href = await link.evaluate("el => el.href")
                        if any(x in link_text for x in ['summary', 'analysis', 'study guide', 'workbook']):
                            continue
                        book_url = href
                        print(f"  Goodreads: Found via DuckDuckGo: {book_url}")
                        break
                except Exception:
                    pass

            # --- TIER 5: Broadened Discovery (Normalized Title) ---
            if not book_url:
                clean_q = normalize_title_for_search(title)
                print(f"  Goodreads: Discovery Tier 5 (Broad) for: '{clean_q}'")
                search_query = f"{clean_q} {author} genre subgenre goodreads"
                brave_url = f"https://search.brave.com/search?q={search_query.replace(' ', '+')}"
                try:
                    await page.goto(brave_url, wait_until="domcontentloaded", timeout=30000)
                    links = await page.query_selector_all('a[href*="goodreads.com/book/show/"]')
                    for link in links:
                        link_text = (await link.inner_text()).lower()
                        href = await link.evaluate("el => el.href")
                        if any(x in link_text for x in ['summary', 'analysis', 'study guide', 'workbook']):
                            continue
                        book_url = href
                        print(f"  Goodreads: Found via Broad Search: {book_url}")
                        break
                except Exception:
                    pass

            if not book_url:
                print(f"  Goodreads: No results found for '{title[:20]}'")
                return {}
            
            # Step 2: Final navigation to book page
            if page.url != book_url:
                await page.goto(book_url, wait_until="domcontentloaded", timeout=60000)
            
            await asyncio.sleep(4) # Extended wait for React hydration of genres

            # Extract Genres
            genres = []
            try:
                # Primary selector for new layout
                genre_els = await page.query_selector_all('[data-testid="genresList"] .Button__labelItem, .BookPageMetadataSection__genre a')
                if not genre_els:
                    # Fallback to general link scan
                    genre_els = await page.query_selector_all('a[href*="/genres/"]')
                
                for gel in genre_els:
                    txt = clean_text(await gel.inner_text())
                    if txt and txt not in genres and len(txt) < 30:
                        genres.append(txt)
            except Exception:
                pass
            
            # Specific Romantasy Check
            is_romantasy = "Yes" if any("romantasy" in g.lower() for g in genres) else "No"
            genre_main = genres[0] if genres else "N/A"
            genre_sub = genres[1] if len(genres) > 1 else "N/A"

            # Step 2: Extract Book Details (Ratings, Series URL)
            # Tier 1: JSON-LD (most stable if present)
            ld_json = {}
            try:
                ld_element = await page.query_selector('script[type="application/ld+json"]')
                if ld_element:
                    import json
                    ld_json = json.loads(await ld_element.inner_text())
            except Exception:
                pass

            avg_rating = ld_json.get('aggregateRating', {}).get('ratingValue', "N/A")
            rating_count = ld_json.get('aggregateRating', {}).get('ratingCount', "N/A")

            # Tier 2: DOM Selectors (Fallback if JSON-LD missing or partial)
            if avg_rating == "N/A" or rating_count == "N/A":
                try:
                    # Wait for the rating block to hydrate (Top of Page)
                    try:
                        await page.wait_for_selector('.RatingStatistics__rating, [data-testid="ratingValue"]', timeout=7000)
                    except: pass

                    rating_selectors = [
                        '[data-testid="ratingValue"]',
                        '.RatingStatistics__rating',
                        '.RatingStars__rating',
                        '[itemprop="ratingValue"]',
                        '.RatingStatistics__ratingValue'
                    ]
                    for r_sel in rating_selectors:
                        r_el = await page.query_selector(r_sel)
                        if r_el:
                            avg_rating = clean_text(await r_el.inner_text())
                            if avg_rating and re.search(r'\d', avg_rating): break
                    
                    count_selectors = [
                        '[data-testid="ratingsCount"]',
                        '.RatingStatistics__ratingCount',
                        'a[href="#CommunityReviews"]',
                        '[itemprop="ratingCount"]'
                    ]
                    for c_sel in count_selectors:
                        count_el = await page.query_selector(c_sel)
                        if count_el:
                            c_text = await count_el.inner_text()
                            # Improved regex: Look for numbers preceding "ratings" or just the first group of numbers
                            m = re.search(r'([\d,]+)(?=\s*ratings?)', c_text, re.IGNORECASE)
                            if not m:
                                m = re.search(r'([\d,]+)', c_text)
                            
                            if m:
                                rating_count = m.group(1).replace(',', '')
                                break
                except Exception:
                    pass

            # Tier 3: Regex Body Scan (Last resort)
            if avg_rating == "N/A":
                content = await page.content()
                match = re.search(r'([\d.]+)\s+avg\s+rating', content, re.IGNORECASE)
                if match:
                    avg_rating = match.group(1)

            # Extract Page Count from the book page (Standalone fallback)
            book_pages = "N/A"
            try:
                page_el = await page.query_selector('[data-testid="pagesFormat"]')
                if page_el:
                    page_text = await page_el.inner_text()
                    m = re.search(r'(\d+)\s+pages', page_text)
                    if m:
                        book_pages = m.group(1)
            except Exception:
                pass

            # Series link - INTELLIGENCE LAYER (Selector Carousel)
            series_url = "N/A"
            series_name = "N/A"
            
            selectors = [
                'h3.Text__title3 a[href*="/series/"]', 
                '[data-testid="bookTitle"] + .Text + a[href*="/series/"]',
                '.BookPageMetadataSection__title + .Text a[href*="/series/"]',
                'a.SeriesLink',
                '[data-testid="series"] a'
            ]
            
            for sel in selectors:
                try:
                    el = await page.query_selector(sel)
                    if el:
                        is_anchor = await el.evaluate("el => el instanceof HTMLAnchorElement")
                        if is_anchor:
                            series_url = await el.evaluate("el => el.href")
                            series_name = clean_text(await el.inner_text())
                            print(f"    Intelligence: Found series via selector '{sel}'")
                            break
                except Exception:
                    continue

            # Fallback JS scan (Deep Scan)
            if series_url == "N/A":
                print("    Intelligence: Falling back to Deep JS Scan for series...")
                series_data = await page.evaluate("""() => {
                    const links = Array.from(document.querySelectorAll('a[href*="/series/"]'));
                    if (links.length > 0) {
                        return { url: links[0].href, name: links[0].innerText.trim() };
                    }
                    // Try to find the word 'Series' and look for a link near it
                    const spans = Array.from(document.querySelectorAll('span, div, b'));
                    for (const s of spans) {
                        if (s.innerText && s.innerText.toLowerCase().includes('series')) {
                            const link = s.querySelector('a') || (s.parentElement ? s.parentElement.querySelector('a') : null);
                            if (link && link.href.includes('/series/')) {
                                return { url: link.href, name: link.innerText.trim() };
                            }
                        }
                    }
                    return null;
                }""")
                if series_data:
                    series_url = series_data['url']
                    series_name = clean_text(series_data['name'])
                    print(f"    Intelligence: Found series via Deep JS Scan")

            # Step 3: If Series URL exists, visit it
            series_data = {
                "Num_Primary_Books": "N/A",
                "Total_Pages_Primary_Books": 0,
                "Book1_Rating": "N/A",
                "Book1_Num_Ratings": "N/A"
            }

            if series_url and series_url != "N/A":
                try:
                    await page.goto(series_url, wait_until="domcontentloaded", timeout=60000)
                    
                    # 1. Primary books count (from header)
                    content = await page.content()
                    primary_match = re.search(r'(\d+)\s+primary\s+works', content, re.IGNORECASE)
                    if primary_match:
                        series_data["Num_Primary_Books"] = primary_match.group(1)

                    # 2. Extract details accurately from book rows
                    book_rows = await page.query_selector_all('.listWithDividers__item, .seriesWork, div.u-paddingBottomMedium')
                    total_pages = 0
                    found_book1 = False
                    
                    for i, row in enumerate(book_rows):
                        row_text = (await row.inner_text()).lower()
                        
                        # Only sum pages for primary works (avoiding .5, .6, 2.5)
                        # Primary works usually have a index like "book 1", "book 2" (integers)
                        is_primary = False
                        idx_match = re.search(r'book\s+(\d+)$', row_text.split('\n')[0], re.IGNORECASE) or \
                                     re.search(r'^\s*(\d+)\s*$', row_text.split('\n')[0])
                        
                        # Simpler check: if it doesn't contain a decimal in the first few words of the title row
                        page_match = re.search(r'(\d+)\s+pages', row_text)
                        
                        # For page summing, we try to be inclusive but prioritize likely primary works
                        if page_match:
                            total_pages += int(page_match.group(1))
                        
                        # 3. Targeted "Book 1" Extraction
                        # We want the FIRST book or the one explicitly labeled "book 1"
                        if not found_book1:
                            if "book 1" in row_text or (i == 0 and "book" not in row_text):
                                r_match = re.search(r'([\d.]+)\s+avg\s+rating\s+[—\-]\s+([\d,]+)\s+ratings', row_text, re.IGNORECASE)
                                if r_match:
                                    series_data["Book1_Rating"] = r_match.group(1)
                                    series_data["Book1_Num_Ratings"] = r_match.group(2).replace(',', '')
                                    found_book1 = True
                    
                    series_data["Total_Pages_Primary_Books"] = total_pages
                except Exception as se:
                    print(f"  Goodreads: Series page error: {se}")

            # Final return merge
            final_book1_rating = series_data.get("Book1_Rating", "N/A")
            final_book1_count = series_data.get("Book1_Num_Ratings", "N/A")
            
            # If Series Page found N/A for Book 1, fallback to the Book Page we are currently on
            if final_book1_rating == "N/A" or not re.search(r'\d', str(final_book1_rating)):
                final_book1_rating = avg_rating
            if final_book1_count == "N/A" or not re.search(r'\d', str(final_book1_count)):
                final_book1_count = rating_count

            return {
                "GoodReads_Series_URL": series_url,
                "GoodReads_Book_URL": page.url, # ALWAYS return the current book URL
                "GoodReads_Rating": avg_rating,
                "GoodReads_Rating_Count": rating_count,
                "Genre": genre_main,
                "Sub_Genre": genre_sub,
                "Romantasy_Subgenre": is_romantasy,
                "Num_Primary_Books": series_data["Num_Primary_Books"] if series_url != "N/A" else "1",
                "Total_Pages_Primary_Books": series_data["Total_Pages_Primary_Books"] if (series_url != "N/A" and series_data["Total_Pages_Primary_Books"] != 0) else book_pages,
                "Book1_Rating": final_book1_rating,
                "Book1_Num_Ratings": final_book1_count
            }
        except Exception as e:
            print(f"  Goodreads: Error for '{title[:20]}': {e}")
            return {}
        finally:
            await page.close()


class AuthorScraper:
    def __init__(self, headless=False):
        self.headless = headless

    async def find_author_details(self, context, author_name):
        if not author_name or author_name == "N/A":
            return {}

        page = await context.new_page()
        details = {
            "Author_Email": "N/A",
            "Agent_Email": "N/A",
            "Facebook": "N/A",
            "Twitter": "N/A",
            "Instagram": "N/A",
            "Website": "N/A",
            "Other_Contact": "N/A"
        }

        try:
            # Step 1: Find Official Website
            print(f"  Author: Searching for '{author_name}' official website...")
            search_query = f"{author_name} official website contact"
            brave_url = f"https://search.brave.com/search?q={search_query.replace(' ', '+')}"
            
            website_url = None
            try:
                await page.goto(brave_url, wait_until="domcontentloaded", timeout=30000)
                # Look for results that likely point to a personal homepage
                links = await page.query_selector_all('main a')
                for link in links:
                    href = await link.evaluate("el => el.href")
                    if any(x in href for x in ['facebook.com', 'twitter.com', 'instagram.com', 'wikipedia.org', 'goodreads.com', 'amazon.com']):
                        continue
                    if 'brave.com' in href:
                        continue
                    
                    # Heuristic: the first reasonable outside link is often the official site
                    website_url = href
                    print(f"  Author: Potential website found: {website_url}")
                    break
            except Exception:
                pass

            if not website_url:
                # Try DuckDuckGo fallback
                print(f"  Author: DDG Fallback for website...")
                ddg_url = f"https://html.duckduckgo.com/html/?q={search_query.replace(' ', '+')}"
                try:
                    await page.goto(ddg_url, wait_until="domcontentloaded", timeout=20000)
                    top_link = await page.query_selector('.result__a')
                    if top_link:
                        website_url = await top_link.evaluate("el => el.href")
                except Exception:
                    pass

            if website_url:
                details["Website"] = website_url
                # Step 2: Scrape the website for socials and contact
                await page.goto(website_url, wait_until="domcontentloaded", timeout=45000)
                await asyncio.sleep(2) # JS Render
                
                content = await page.content()
                
                # Emails (Basic Regex)
                emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', content)
                if emails:
                    # Heuristic: if 'agent' is near the email, it's an agent email
                    unique_emails = list(set(emails))
                    for email in unique_emails:
                        if 'agent' in email.lower() or 'literary' in content.lower()[content.lower().find(email.lower())-50:content.lower().find(email.lower())+50]:
                            details["Agent_Email"] = email
                        elif details["Author_Email"] == "N/A":
                            details["Author_Email"] = email

                # Social Links
                links = await page.query_selector_all('a[href]')
                for link in links:
                    href = await link.evaluate("el => el.href")
                    if 'facebook.com' in href and details["Facebook"] == "N/A":
                        details["Facebook"] = href
                    elif ('twitter.com' in href or 'x.com' in href) and details["Twitter"] == "N/A":
                        details["Twitter"] = href
                    elif 'instagram.com' in href and details["Instagram"] == "N/A":
                        details["Instagram"] = href
                
                # Check for "Contact" page specifically
                contact_link = await page.query_selector('a:has-text("Contact"), a:has-text("About")')
                if contact_link:
                    contact_url = await contact_link.evaluate("el => el.href")
                    await page.goto(contact_url, wait_until="domcontentloaded", timeout=30000)
                    contact_content = await page.content()
                    
                    # Re-scan for emails on contact page
                    c_emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', contact_content)
                    if c_emails:
                        for email in set(c_emails):
                            if 'agent' in email.lower() or 'press' in email.lower():
                                details["Agent_Email"] = email
                            elif details["Author_Email"] == "N/A":
                                details["Author_Email"] = email

            return details
        except Exception as e:
            print(f"  Author: Error for '{author_name}': {e}")
            return details
        finally:
            await page.close()


