#!/usr/bin/env python3
"""
Quick diagnostic summary for pair-live-paper events.
Run after a live paper session to get the 4-class event distribution.

Usage:
  python3 analyze_diagnostics.py [events.jsonl_path]
"""
import json
import collections
import pathlib
import sys


def analyze(events_path: str = "var/pair_live_paper_blue_walnut_opening_hourly_10m/events.jsonl"):
    p = pathlib.Path(events_path)
    if not p.exists():
        print(f"File not found: {p}")
        print(f"Usage: python3 {sys.argv[0]} [events.jsonl_path]")
        sys.exit(1)

    kind_counter = collections.Counter()
    empty_slugs = []
    filtered_family = collections.Counter()
    filtered_duration = collections.Counter()
    decisions = collections.Counter()
    scan_total = 0

    for line in p.open():
        line = line.strip()
        if not line:
            continue
        rec = json.loads(line)
        k = rec.get("kind", "?")
        kind_counter[k] += 1
        payload = rec.get("payload", {})

        if k == "pair_event_descriptors_empty":
            empty_slugs.append(payload.get("event_slug", "?"))
        elif k == "pair_descriptor_filtered_family":
            filtered_family[(payload.get("market_slug", ""), payload.get("family", "?"))] += 1
        elif k == "pair_descriptor_filtered_duration":
            filtered_duration[(payload.get("market_slug", ""), payload.get("bucket", "?"))] += 1
        elif k == "pair_scanner_decision":
            decisions[(payload.get("reason", "?"), payload.get("market_family", "?"))] += 1
        elif k == "pair_market_scan":
            scan_total += 1

    print("=" * 60)
    print(f"Events file: {p}")
    print(f"Total scans: {scan_total}")
    print()

    print("--- All event kinds (sorted by count) ---")
    for k, cnt in kind_counter.most_common():
        print(f"  {cnt:5d}  {k}")
    print()

    print(f"--- pair_event_descriptors_empty: {len(empty_slugs)} events ---")
    slug_counter = collections.Counter(empty_slugs)
    for slug, cnt in slug_counter.most_common(10):
        print(f"  {cnt:4d}x  {slug}")
    print()

    print("--- pair_descriptor_filtered_family ---")
    for (slug, fam), cnt in filtered_family.most_common(10):
        print(f"  {cnt:4d}x  family={fam:8}  {slug}")
    print()

    print("--- pair_descriptor_filtered_duration ---")
    for (slug, bucket), cnt in filtered_duration.most_common(10):
        print(f"  {cnt:4d}x  bucket={bucket:8}  {slug}")
    print()

    print("--- pair_scanner_decision (first-leg decisions) ---")
    for (reason, fam), cnt in decisions.most_common(20):
        print(f"  {cnt:4d}x  {reason:45} [{fam}]")
    print()

    # Root cause diagnosis
    print("=" * 60)
    print("ROOT CAUSE DIAGNOSIS:")
    print()
    total_empty = kind_counter.get("pair_event_descriptors_empty", 0)
    total_loaded = kind_counter.get("pair_event_descriptors_loaded", 0)
    total_filtered_duration = sum(filtered_duration.values())
    total_filtered_family = sum(filtered_family.values())
    total_decisions = sum(decisions.values())

    if total_decisions > 0:
        print("  STATUS: first-leg IS entering decision layer!")
        print(f"  Decisions: {total_decisions}")
    elif total_empty > 0 and total_loaded == 0:
        print("  STATUS: all descriptors came back EMPTY")
        print(f"  This means get_events() returned no valid up/down markets")
        print(f"  for the hourly slugs discovered by public_search_events().")
    elif total_filtered_duration > 0:
        print(f"  STATUS: {total_filtered_duration} descriptors filtered by DURATION")
        print("  This means slugs have valid markets but wrong time bucket.")
    elif total_filtered_family > 0:
        print(f"  STATUS: {total_filtered_family} descriptors filtered by FAMILY")
        print("  This means market slugs don't match btc/eth/sol/xrp naming.")
    elif total_loaded > 0:
        print("  STATUS: descriptors loaded but no decisions recorded")
        print("  Something deeper is filtering silently. Add more diagnostic events.")
    else:
        print("  STATUS: no diagnostic events found. Did the paper run long enough?")

    print()


if __name__ == "__main__":
    events_path = sys.argv[1] if len(sys.argv) > 1 else None
    if events_path:
        analyze(events_path)
    else:
        # Try default path
        default = "var/pair_live_paper_blue_walnut_opening_hourly_10m/events.jsonl"
        if pathlib.Path(default).exists():
            analyze(default)
        else:
            print("No events file found. Run pair-live-paper first.")
            print(f"Usage: python3 {sys.argv[0]} [events.jsonl_path]")
