#!/usr/bin/env python3
"""
Fetch all Archive.org identifiers matching a query and save to file.

This pre-fetches the complete list of identifiers to avoid pagination issues
with the Archive.org API.
"""

import argparse
import json
import sys
from pathlib import Path

try:
    from internetarchive import search_items
except ImportError:
    print("Error: internetarchive library not installed")
    print("Install with: pip install internetarchive")
    sys.exit(1)


def fetch_all_identifiers(query: str, sort_order: str = None, max_items: int = None) -> list:
    """
    Fetch all identifiers matching the query using internetarchive library.

    Args:
        query: Archive.org search query
        sort_order: Sort order (e.g., "date asc")
        max_items: Maximum number of identifiers to fetch

    Returns:
        List of identifier strings
    """
    print(f"Query: {query}")
    if sort_order:
        print(f"Sort: {sort_order}")

    # Use internetarchive library to search
    # This handles pagination automatically
    search_params = {}
    if sort_order:
        # Convert "date asc" to ["date asc"]
        search_params["sorts"] = [sort_order]

    print("Searching Archive.org...", flush=True)

    identifiers = []
    count = 0

    try:
        # search_items yields results one at a time, handling pagination internally
        for item in search_items(query, params=search_params):
            identifiers.append(item['identifier'])
            count += 1

            if count % 1000 == 0:
                print(f"Fetched {count:,} identifiers...", flush=True)

            if max_items and count >= max_items:
                print(f"Reached max_items limit: {max_items:,}")
                break

    except KeyboardInterrupt:
        print(f"\nInterrupted by user after fetching {count:,} identifiers")
    except Exception as e:
        print(f"\nError during search: {e}")
        print(f"Successfully fetched {count:,} identifiers before error")

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
