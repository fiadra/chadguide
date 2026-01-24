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


def main(limit: int = None, city_filter: str = None):
    """
    Main entry point: fetch images for all attractions.

    Args:
        limit: Maximum number of attractions to process (for testing)
        city_filter: Only process attractions from this city
    """
    with open(ATTRACTIONS_FILE, 'r', encoding='utf-8') as f:
        attractions = json.load(f)

    print(f"Loaded {sum(len(v) for v in attractions.values())} attractions")

    IMAGES_DIR.mkdir(parents=True, exist_ok=True)

    processed = 0
    success = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
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

                    # Click first image to open side panel
                    page.mouse.click(150, 400)
                    page.wait_for_timeout(2000)

                    # Find the large image in side panel
                    img_src = None
                    for selector in ['img.sFlh5c.FyHeAf', 'img.iPVvYb', 'img[jsname="kn3ccd"]']:
                        img = page.query_selector(selector)
                        if img:
                            src = img.get_attribute('src')
                            if src and src.startswith('http') and 'encrypted' not in src:
                                img_src = src
                                break

                    if not img_src:
                        print("FAIL (no image URL)")
                        processed += 1
                        continue

                    # Download the image
                    city_dir.mkdir(parents=True, exist_ok=True)
                    response = page.request.get(img_src)
                    if response.ok and len(response.body()) > 1000:
                        save_path.write_bytes(response.body())
                        print(f"OK ({len(response.body()) // 1024}KB)")
                        attr['local_image'] = f"images/attractions/{sanitize_filename(city)}/{sanitize_filename(name)}.jpg"
                        success += 1
                    else:
                        print("FAIL (download error)")

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
    args = parser.parse_args()

    main(limit=args.limit, city_filter=args.city)
