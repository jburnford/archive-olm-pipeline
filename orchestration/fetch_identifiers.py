#!/usr/bin/env python3
"""
Fetch all Archive.org identifiers matching a query and save to file.

This pre-fetches the complete list of identifiers to avoid pagination issues
with the Archive.org API.
"""

import argparse
import json
import sys
import time
from pathlib import Path

import requests


def fetch_all_identifiers(query: str, sort_order: str = None, max_items: int = None) -> list:
    """
    Fetch all identifiers matching the query.

    Args:
        query: Archive.org search query
        sort_order: Sort order (e.g., "date asc")
        max_items: Maximum number of identifiers to fetch

    Returns:
        List of identifier strings
    """
    url = "https://archive.org/advancedsearch.php"

    # First, get the total count
    params = {
        "q": query,
        "fl": "identifier",
        "rows": 0,  # Just get count
        "output": "json",
    }
    if sort_order:
        params["sort"] = sort_order

    print(f"Query: {query}")
    if sort_order:
        print(f"Sort: {sort_order}")

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()

    total_found = data["response"]["numFound"]
    print(f"Total items found: {total_found:,}")

    if max_items:
        total_to_fetch = min(total_found, max_items)
        print(f"Fetching: {total_to_fetch:,} (limited by --max-items)")
    else:
        total_to_fetch = total_found

    # Fetch in batches of 1000 (API maximum)
    identifiers = []
    batch_size = 1000

    for start in range(0, total_to_fetch, batch_size):
        rows = min(batch_size, total_to_fetch - start)

        params = {
            "q": query,
            "fl": "identifier",
            "rows": rows,
            "start": start,
            "output": "json",
        }
        if sort_order:
            params["sort"] = sort_order

        print(f"Fetching batch {start}-{start + rows}...", end="", flush=True)

        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            batch_data = response.json()

            docs = batch_data["response"]["docs"]
            batch_identifiers = [doc["identifier"] for doc in docs]
            identifiers.extend(batch_identifiers)

            print(f" got {len(batch_identifiers)} identifiers")

            # Be nice to the API
            time.sleep(0.5)

        except Exception as e:
            print(f" ERROR: {e}")
            print(f"Failed to fetch batch starting at {start}")
            break

    print(f"\nTotal identifiers fetched: {len(identifiers):,}")
    return identifiers


def main():
    parser = argparse.ArgumentParser(
        description="Fetch all Archive.org identifiers for a query"
    )
    parser.add_argument(
        "--query",
        required=True,
        help="Archive.org search query"
    )
    parser.add_argument(
        "--sort",
        help="Sort order (e.g., 'date asc')"
    )
    parser.add_argument(
        "--max-items",
        type=int,
        help="Maximum number of identifiers to fetch"
    )
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="Output file path (JSON)"
    )

    args = parser.parse_args()

    print("=" * 70)
    print("Archive.org Identifier Fetcher")
    print("=" * 70)

    try:
        identifiers = fetch_all_identifiers(
            query=args.query,
            sort_order=args.sort,
            max_items=args.max_items
        )

        # Save to file
        output_data = {
            "query": args.query,
            "sort_order": args.sort,
            "total_count": len(identifiers),
            "identifiers": identifiers
        }

        args.output.parent.mkdir(parents=True, exist_ok=True)

        with open(args.output, 'w') as f:
            json.dump(output_data, f, indent=2)

        print(f"\nSaved {len(identifiers):,} identifiers to: {args.output}")
        print(f"File size: {args.output.stat().st_size / 1024:.1f} KB")

    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
