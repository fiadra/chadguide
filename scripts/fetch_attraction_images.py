#!/usr/bin/env python3
"""
Fetch images for attractions using Playwright and Google Images.

This script downloads high-quality images for attractions that don't have
Wikimedia images. It uses headless Chrome to search Google Images and
extract the full-resolution image from the preview panel.

Usage:
    python scripts/fetch_attraction_images.py
    python scripts/fetch_attraction_images.py --city Warsaw --limit 10

Requirements:
    - playwright package installed
    - Chromium browser (install with: playwright install chromium)
"""

import json
import re
from pathlib import Path
from urllib.parse import quote_plus
from playwright.sync_api import sync_playwright


ATTRACTIONS_FILE = Path(__file__).parent.parent / "data" / "attractions.json"
IMAGES_DIR = Path(__file__).parent.parent / "data" / "images" / "attractions"


def sanitize_filename(name: str) -> str:
    """
    Create a safe filename from an attraction name.

    Removes special characters and replaces spaces with underscores.
    Truncates to 50 characters to avoid filesystem issues.
    """
    safe = re.sub(r'[^\w\s-]', '', name)
    safe = re.sub(r'\s+', '_', safe.strip())
    return safe[:50]


DEBUG_DIR = Path("/tmp/attraction_debug")


def extract_image_urls_from_page(page, debug: bool = False) -> list[str]:
    """
    Extract full-resolution image URLs from Google Images page source.

    Google embeds original image URLs in the page's script data,
    even before any click interaction. This avoids the fragile
    side-panel approach entirely.
    """
    urls = page.evaluate("""() => {
        const urls = [];

        // Method 1: Extract from page source text using regex
        // Google embeds full URLs in AF_initDataCallback JSON blocks
        const scripts = document.querySelectorAll('script');
        for (const script of scripts) {
            const text = script.textContent || '';
            // Match URLs that look like full-res image links
            const matches = text.matchAll(/\\["(https?:\\/\\/[^"]+\\.(?:jpg|jpeg|png|webp)(?:[^"]*?))",[0-9]+,[0-9]+\\]/gi);
            for (const m of matches) {
                const url = m[1];
                if (url.includes('encrypted-tbn') || url.includes('gstatic.com/images')) continue;
                if (url.includes('google.com')) continue;
                urls.push(url);
            }
        }

        // Method 2: Look for data attributes on image containers
        const dataItems = document.querySelectorAll('[data-tbnid]');
        for (const item of dataItems) {
            const metaUrl = item.querySelector('a[href]');
            if (metaUrl) {
                const href = metaUrl.getAttribute('href') || '';
                const imgMatch = href.match(/imgurl=([^&]+)/);
                if (imgMatch) {
                    try { urls.push(decodeURIComponent(imgMatch[1])); } catch(e) {}
                }
            }
        }

        // Deduplicate
        return [...new Set(urls)];
    }""")

    if debug:
        print(f"    [debug] Extracted {len(urls)} image URLs from page source")
        for i, url in enumerate(urls[:5]):
            print(f"      [{i}] {url[:100]}")

    return urls


def main(limit: int = None, city_filter: str = None, debug: bool = False):
    """
    Main entry point: fetch images for all attractions.

    Args:
        limit: Maximum number of attractions to process (for testing)
        city_filter: Only process attractions from this city
        debug: Run with visible browser and save screenshots
    """
    with open(ATTRACTIONS_FILE, 'r', encoding='utf-8') as f:
        attractions = json.load(f)

    print(f"Loaded {sum(len(v) for v in attractions.values())} attractions")

    IMAGES_DIR.mkdir(parents=True, exist_ok=True)

    if debug:
        DEBUG_DIR.mkdir(parents=True, exist_ok=True)
        print(f"Debug screenshots will be saved to {DEBUG_DIR}")

    processed = 0
    success = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=not debug)
        context = browser.new_context(
            viewport={'width': 1280, 'height': 900},
            locale='pl-PL',
            extra_http_headers={
                'Referer': 'https://www.google.com/',
                'Accept': 'image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8',
            },
        )
        page = context.new_page()

        # Accept cookies once (Polish Google)
        page.goto("https://www.google.com/search?q=test&udm=2")
        try:
            page.get_by_role("button", name="Zaakceptuj wszystko").click(timeout=5000)
        except:
            pass

        for city, city_attractions in attractions.items():
            if city_filter and city.lower() != city_filter.lower():
                continue

            print(f"\n=== {city} ({len(city_attractions)} attractions) ===")

            for attr in city_attractions:
                if limit and processed >= limit:
                    break

                name = attr.get('name', '')
                if not name:
                    continue

                # Check if image already exists
                city_dir = IMAGES_DIR / sanitize_filename(city)
                save_path = city_dir / f"{sanitize_filename(name)}.jpg"

                if save_path.exists():
                    print(f"  [skip] {name}")
                    continue

                print(f"  {name}...", end=" ", flush=True)

                try:
                    # Use Polish name for better search results
                    name_pl = attr.get('name_pl', name)
                    query = f"{name_pl} {city}"
                    page.goto(f"https://www.google.com/search?q={quote_plus(query)}&udm=2")
                    page.wait_for_load_state("networkidle")
                    page.wait_for_timeout(1000)

                    if debug:
                        slug = sanitize_filename(name)
                        page.screenshot(path=str(DEBUG_DIR / f"{slug}_1_results.png"))

                    # Extract image URLs directly from page source (no clicking needed)
                    image_urls = extract_image_urls_from_page(page, debug=debug)

                    if not image_urls:
                        print("FAIL (no image URL)")
                        processed += 1
                        continue

                    # Try up to 5 URLs until one downloads successfully
                    city_dir.mkdir(parents=True, exist_ok=True)
                    downloaded = False
                    for attempt, img_src in enumerate(image_urls[:5]):
                        try:
                            response = page.request.get(img_src, timeout=10000)
                            body = response.body()
                            if response.ok and len(body) > 1000:
                                save_path.write_bytes(body)
                                print(f"OK ({len(body) // 1024}KB)" + (f" [url #{attempt+1}]" if attempt > 0 else ""))
                                attr['local_image'] = f"images/attractions/{sanitize_filename(city)}/{sanitize_filename(name)}.jpg"
                                success += 1
                                downloaded = True
                                break
                            else:
                                if debug:
                                    print(f"\n    [debug] url #{attempt+1}: status={response.status}, size={len(body)}, url={img_src[:80]}")
                        except Exception as dl_err:
                            if debug:
                                print(f"\n    [debug] url #{attempt+1}: {dl_err}, url={img_src[:80]}")
                    if not downloaded:
                        print(f"FAIL (download error, tried {min(len(image_urls), 5)} urls)")

                except Exception as e:
                    print(f"FAIL: {e}")

                processed += 1

            if limit and processed >= limit:
                break

        context.close()
        browser.close()

    # Save updated JSON with local_image paths
    with open(ATTRACTIONS_FILE, 'w', encoding='utf-8') as f:
        json.dump(attractions, f, ensure_ascii=False, indent=2)

    print(f"\n=== DONE: {success}/{processed} ===")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Fetch attraction images from Google")
    parser.add_argument('--limit', type=int, help="Max attractions to process")
    parser.add_argument('--city', type=str, help="Only process this city")
    parser.add_argument('--debug', action='store_true', help="Run with visible browser and screenshots")
    args = parser.parse_args()

    main(limit=args.limit, city_filter=args.city, debug=args.debug)
