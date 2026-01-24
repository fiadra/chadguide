#!/usr/bin/env python3
"""
Fetch tourist attractions for all cities in the application.

This script:
1. Extracts unique city names from airport-index.js
2. For each city, fetches top attractions from Geoapify Places API
3. Enriches each attraction with image and Wikipedia link from Wikidata
4. Saves everything to a JSON file for runtime use

Usage:
    python scripts/fetch_attractions.py
    python scripts/fetch_attractions.py --city Warsaw --limit 10
"""

import json
import re
import time
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from geoapify_api.client import GeoapifyClient, get_wikidata_info


# Categories to exclude (not tourist attractions)
BAD_CATEGORIES = {
    "highway", "man_made", "commercial", "accommodation",
    "service", "office", "building.residential", "parking",
}

# Geoapify category priorities (higher = better for tourists)
CATEGORY_PRIORITY = {
    # Top priority - UNESCO and heritage sites
    "heritage.unesco": 100,
    "heritage": 95,
    # Castles, forts, ruins
    "tourism.sights.castle": 90,
    "tourism.sights.fort": 85,
    "tourism.sights.ruines": 80,
    "tourism.sights.archaeological_site": 80,
    # Monuments and memorials
    "tourism.sights.memorial.monument": 75,
    "tourism.sights.memorial.tomb": 70,
    "tourism.sights.memorial": 65,
    # Religious buildings
    "tourism.sights.place_of_worship.cathedral": 75,
    "tourism.sights.place_of_worship.church": 60,
    "tourism.sights.monastery": 70,
    # Other landmarks
    "tourism.sights.tower": 65,
    "tourism.sights.city_gate": 60,
    "tourism.sights.bridge": 55,
    "tourism.sights.city_hall": 55,
    "tourism.sights.lighthouse": 50,
    "tourism.sights.windmill": 45,
    "tourism.sights.square": 40,
    "tourism.sights.battlefield": 60,
    "tourism.sights": 50,
    # Museums and culture
    "entertainment.museum": 70,
    "entertainment.culture.gallery": 65,
    "entertainment.culture.theatre": 55,
    "entertainment.culture.arts_centre": 50,
    "entertainment.planetarium": 45,
    "entertainment.culture": 40,
    # Tourist attractions
    "tourism.attraction.artwork": 45,
    "tourism.attraction.viewpoint": 50,
    "tourism.attraction.fountain": 35,
    "tourism.attraction": 40,
    # Entertainment venues
    "entertainment.zoo": 55,
    "entertainment.aquarium": 50,
    "entertainment.theme_park": 45,
    # Generic fallbacks
    "tourism": 20,
    "entertainment": 15,
    "building": 5,
}


def _get_best_category(categories: list[str] | None) -> str:
    """Select the best tourist category from a list of Geoapify categories."""
    if not categories:
        return "other"

    best_cat = None
    best_priority = -1

    for cat in categories:
        # Check full category match
        priority = CATEGORY_PRIORITY.get(cat, 0)

        # Also check prefix (e.g., "historic" for "historic.castle.ruin")
        prefix = cat.split(".")[0] if "." in cat else cat
        prefix_priority = CATEGORY_PRIORITY.get(prefix, 0)

        cat_priority = max(priority, prefix_priority)

        if cat_priority > best_priority:
            best_priority = cat_priority
            best_cat = cat

    # If no good match found, return first non-bad category
    if best_cat is None:
        for cat in categories:
            if not any(bad in cat for bad in BAD_CATEGORIES):
                return cat
        return categories[0] if categories else "other"

    return best_cat


def _category_priority(category: str) -> int:
    """Return the priority score for a category (used for sorting)."""
    if not category:
        return 0
    priority = CATEGORY_PRIORITY.get(category, 0)
    prefix = category.split(".")[0] if "." in category else category
    return max(priority, CATEGORY_PRIORITY.get(prefix, 0))


def extract_cities_from_airport_index() -> list[dict]:
    """Extract unique cities from the airport-index.js file."""
    airport_js = Path(__file__).parent.parent / "web" / "js" / "airport-index.js"
    content = airport_js.read_text()

    # Parse the AIRPORTS array using regex
    # Match patterns like: { iata: 'WAW', city: 'Warsaw', country: 'Poland', ... }
    pattern = r"\{\s*iata:\s*'([^']+)',\s*city:\s*'([^']+)',\s*country:\s*'([^']+)'"
    matches = re.findall(pattern, content)

    # Create unique city-country pairs
    seen = set()
    cities = []
    for iata, city, country in matches:
        key = f"{city}|{country}"
        if key not in seen:
            seen.add(key)
            cities.append({
                "city": city,
                "country": country,
                "iata": iata
            })

    return cities


def fetch_attractions_for_city(client: GeoapifyClient, city: str, limit: int = 5) -> list[dict]:
    """
    Fetch tourist attractions for a single city with Wikidata enrichment.

    Args:
        client: Geoapify API client instance
        city: City name to search for
        limit: Maximum number of attractions to return

    Returns:
        List of attraction dictionaries with name, address, category,
        coordinates, image_url, and wikipedia_url
    """
    # Geoapify Places API categories for tourist attractions
    # Using parent categories to include all subcategories
    # Reference: https://apidocs.geoapify.com/docs/places/
    ATTRACTION_CATEGORIES = [
        "heritage",              # UNESCO and heritage sites
        "tourism.sights",        # Castles, forts, ruins, monuments, churches, towers
        "entertainment.museum",  # Museums
        "entertainment.culture", # Galleries, theatres
        "tourism.attraction",    # Viewpoints, fountains, sculptures
        "entertainment.zoo",     # Zoos
        "entertainment.aquarium", # Aquariums
    ]

    try:
        attractions_data = client.fetch_amenities(
            city=city,
            categories=ATTRACTION_CATEGORIES,
            limit=5  # Fetch 5 per category, then select the best ones
        )

        # Track seen names for deduplication
        seen_names = set()
        attractions = []

        for f in attractions_data.features:
            name = f.properties.name
            if not name:
                continue

            # Deduplicate by name (case insensitive)
            name_key = name.lower().strip()
            if name_key in seen_names:
                continue
            seen_names.add(name_key)

            # Select the best tourist category (not just the first one)
            category = _get_best_category(f.properties.categories)

            # Get image and Polish Wikipedia link from Wikidata
            image_url = None
            wikipedia_url = None
            wikidata_id = None
            if f.properties.wiki_and_media:
                wikidata_id = f.properties.wiki_and_media.wikidata
                if wikidata_id:
                    wiki_info = get_wikidata_info(wikidata_id)
                    image_url = wiki_info["image_url"]
                    wikipedia_url = wiki_info["wikipedia_url"]

            attractions.append({
                "name": name,
                "address": f.properties.address_line1,
                "category": category,
                "lat": f.properties.lat,
                "lon": f.properties.lon,
                "image_url": image_url,
                "wikipedia_url": wikipedia_url,
                "_has_wikidata": wikidata_id is not None,
            })

        # Sort by: Wikidata presence (more famous), image availability, category priority
        attractions.sort(key=lambda x: (
            -int(x["_has_wikidata"]),
            -int(x["image_url"] is not None),
            -_category_priority(x["category"]),
        ))

        # Take top `limit` and remove helper field
        top_attractions = []
        for attr in attractions[:limit]:
            del attr["_has_wikidata"]
            top_attractions.append(attr)

        return top_attractions

    except Exception as e:
        print(f"  Error fetching attractions for {city}: {e}")
        return []


def main():
    """Main entry point: fetch attractions for all cities and save to JSON."""
    print("=" * 60)
    print("Fetching attractions for all cities")
    print("=" * 60)

    cities = extract_cities_from_airport_index()
    print(f"\nFound {len(cities)} unique cities")

    client = GeoapifyClient()

    all_attractions = {}
    errors = []

    for i, city_info in enumerate(cities, 1):
        city = city_info["city"]
        country = city_info["country"]
        print(f"\n[{i}/{len(cities)}] {city}, {country}...")

        attractions = fetch_attractions_for_city(client, city, limit=5)

        if attractions:
            all_attractions[city] = attractions
            print(f"  Found {len(attractions)} attractions")

            for attr in attractions:
                has_img = "📷" if attr["image_url"] else "  "
                has_wiki = "📖" if attr["wikipedia_url"] else "  "
                print(f"    {has_img}{has_wiki} {attr['name']}")
        else:
            errors.append(city)
            print(f"  No attractions found")

        # Rate limiting
        time.sleep(0.5)

    # Save results
    output_path = Path(__file__).parent.parent / "data" / "attractions.json"
    output_path.parent.mkdir(exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(all_attractions, f, ensure_ascii=False, indent=2)

    # Print summary
    print("\n" + "=" * 60)
    print(f"Done! Saved to {output_path}")
    print(f"Total cities with attractions: {len(all_attractions)}")
    print(f"Cities without attractions: {len(errors)}")
    if errors:
        print(f"  {', '.join(errors)}")

    # Print statistics
    total_attractions = sum(len(v) for v in all_attractions.values())
    with_images = sum(
        1 for city_attrs in all_attractions.values()
        for attr in city_attrs if attr["image_url"]
    )
    with_wiki = sum(
        1 for city_attrs in all_attractions.values()
        for attr in city_attrs if attr["wikipedia_url"]
    )

    print(f"\nStats:")
    print(f"  Total attractions: {total_attractions}")
    print(f"  With images: {with_images} ({100*with_images/total_attractions:.1f}%)")
    print(f"  With Polish Wikipedia: {with_wiki} ({100*with_wiki/total_attractions:.1f}%)")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Fetch attractions for cities")
    parser.add_argument("--city", type=str, help="Test single city (e.g., 'Warsaw')")
    parser.add_argument("--limit", type=int, default=5, help="Attractions per city")
    args = parser.parse_args()

    if args.city:
        print(f"Testing attractions for: {args.city}")
        client = GeoapifyClient()
        attractions = fetch_attractions_for_city(client, args.city, limit=args.limit)
        print(f"\nFound {len(attractions)} attractions:\n")
        for i, attr in enumerate(attractions, 1):
            has_img = "📷" if attr["image_url"] else "  "
            has_wiki = "📖" if attr["wikipedia_url"] else "  "
            print(f"{i}. {has_img}{has_wiki} [{attr['category']}] {attr['name']}")
            if attr["address"]:
                print(f"      {attr['address']}")
    else:
        main()
