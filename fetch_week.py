#!/usr/bin/env python3
"""
Fetch a consecutive run of dates (a "week", or any N-day span) into the
central store in ONE sitting, rather than as separate fetch_day.py
invocations spread across different days.

This replaces the old "run fetch_day.py once a day for a week" workflow.
That workflow existed only to avoid exhausting OpenSky's daily API
credits -- but for two small regional airports the actual per-day cost is
small (see README), so there's no real need to spread it out. Fetching a
week normally uses on the order of a few hundred API calls total, well
inside the ~4,000/day credit budget for a registered OpenSky account.

------------------------------------------------------------------------
Usage
------------------------------------------------------------------------
    export OPENSKY_CLIENT_ID=...
    export OPENSKY_CLIENT_SECRET=...

    # Fetch the 7 most recent complete days (yesterday and the 6 before), all in
    # this run, and prune the store down to just those 7 afterwards:
    python3 fetch_week.py -o data/store.json

    # A specific end date and/or a different span:
    python3 fetch_week.py --end 2026-12-14 --days 7 -o data/store.json

    # Keep going even if one date hits a rate limit -- just move on to
    # the next date and come back to the incomplete one on a later run
    # (each date is independently resumable, same as fetch_day.py):
    python3 fetch_week.py -o data/store.json --continue-on-rate-limit

    # Synthetic data, for testing the pipeline without any API calls:
    python3 fetch_week.py -o data/store.json --synthetic

Every date is fetched using exactly the same logic as fetch_day.py
(same candidate/track caching, same resumability), just looped over a
date range in-process instead of run by hand N times. If a date was
already fully fetched in a previous run, it's skipped without spending
any API calls on it.

By default, --keep-days is set to the same value as --days, so after a
successful run the store contains exactly this run's date range and
nothing older -- a rolling window, not an ever-growing archive. Pass
--keep-days 0 to disable pruning and keep every date ever fetched.
------------------------------------------------------------------------
"""
import argparse
import sys
import time
from datetime import datetime, timedelta, timezone

import fetch_day as fd


def daterange_ending(end_date_str, days):
    end = datetime.strptime(end_date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return [(end - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(days - 1, -1, -1)]


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                  formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("-o", "--output", required=True,
                     help="Path to the central store JSON, e.g. data/store.json.")
    ap.add_argument("--end", metavar="YYYY-MM-DD",
                     help="Last (most recent) UTC date to fetch. Default: yesterday (UTC).")
    ap.add_argument("--days", type=int, default=7,
                     help="How many consecutive dates to fetch, ending at --end. Default: 7.")
    ap.add_argument("--keep-days", type=int, default=None,
                     help="Prune the store to this many most-recent days after the run. "
                          "Default: same as --days (a clean rolling window). Use 0 to "
                          "disable pruning entirely.")
    ap.add_argument("--synthetic", action="store_true",
                     help="Generate placeholder data instead of calling OpenSky, for every date.")
    ap.add_argument("--registry", metavar="CSV",
                     help="Path to OpenSky aircraftDatabase.csv for registration matching.")
    ap.add_argument("--max-tracks", type=int, metavar="N",
                     help="Cap new tracks fetched PER DATE this run (passed through to "
                          "each date's fetch). Leave unset for no cap.")
    ap.add_argument("--continue-on-rate-limit", action="store_true",
                     help="If a date's track quota runs out mid-run, move on to the next "
                          "date instead of stopping the whole run. The incomplete date "
                          "stays in the store flagged incomplete, same as fetch_day.py, "
                          "and can be resumed later with fetch_day.py or another fetch_week.py run.")
    args = ap.parse_args()

    # Default to YESTERDAY: today's flights are still in progress and OpenSky's
    # /flights data for the current day is not available yet.
    end_date = args.end or (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
    try:
        datetime.strptime(end_date, "%Y-%m-%d")
    except ValueError:
        print(f"ERROR: '{end_date}' isn't a valid YYYY-MM-DD date", file=sys.stderr)
        sys.exit(1)
    if args.days < 1:
        print("ERROR: --days must be at least 1", file=sys.stderr)
        sys.exit(1)

    dates = daterange_ending(end_date, args.days)
    keep_days = args.days if args.keep_days is None else args.keep_days

    print(f"Fetching {len(dates)} date(s) in one sitting: {dates[0]} .. {dates[-1]}",
          file=sys.stderr)
    print(f"Store: {args.output}" + (" (synthetic)" if args.synthetic else ""), file=sys.stderr)
    print("-" * 72, file=sys.stderr)

    registry = fd.load_registry(args.registry) if (args.registry and not args.synthetic) else {}

    # One client for the whole run, not one per date -- this is what lets
    # credit-bucket tracking (and the pre-flight skip when a bucket is
    # already known to be at 0) actually work across dates instead of
    # resetting its knowledge every iteration.
    client = None if args.synthetic else fd.make_client()

    results = []
    for i, date_str in enumerate(dates, 1):
        print(f"\n[{i}/{len(dates)}] {date_str}", file=sys.stderr)
        store = fd.load_store(args.output)
        already_present = date_str in store["days"]

        if args.synthetic:
            trips, meta = fd.build_synthetic(date_str)
        else:
            trips, meta = fd.fetch_date(
                client, date_str, registry, store.get("days", {}).get(date_str),
                max_tracks=args.max_tracks,
            )

        store["days"][date_str] = {"meta": meta, "trips": trips}
        fd.save_store(store, args.output)

        action = "Replaced" if already_present else "Added"
        status = "OK"
        if meta.get("incomplete_rate_limited"):
            status = "INCOMPLETE (rate limited)"
        elif not meta.get("synthetic") and not meta.get("complete", True):
            status = "INCOMPLETE"
        elif not meta.get("synthetic") and not meta.get("settled", True):
            status = "OK (unsettled -- will be refreshed by a later --revisit)"
        print(f"  {action} {date_str}: {len(trips)} trip(s) [{status}]", file=sys.stderr)
        results.append((date_str, len(trips), status))

        if meta.get("incomplete_rate_limited"):
            if args.continue_on_rate_limit:
                print("  Rate limit hit -- continuing to the next date "
                      "(--continue-on-rate-limit was set).", file=sys.stderr)
            else:
                print(
                    "\nStopping: OpenSky's track quota was exhausted on "
                    f"{date_str}. {i}/{len(dates)} date(s) attempted this run.\n"
                    "Re-run this same command later (after the quota resets) to pick up "
                    "where this stopped -- already-completed dates are skipped, and "
                    f"{date_str} resumes from its cached candidates.\n"
                    "Pass --continue-on-rate-limit if you'd rather skip ahead to the "
                    "remaining dates now and come back for this one separately.",
                    file=sys.stderr,
                )
                break

        # Be a reasonable citizen between dates even when nothing rate-limited.
        if i < len(dates):
            time.sleep(1)

    store = fd.load_store(args.output)
    dropped = fd.prune_store(
        store, keep_days if keep_days > 0 else None, reference_date=end_date
    )
    if dropped:
        fd.save_store(store, args.output)

    print("\n" + "=" * 72, file=sys.stderr)
    print(f"Done. {len(results)}/{len(dates)} date(s) attempted this run:", file=sys.stderr)
    for date_str, n_trips, status in results:
        flag = "" if status == "OK" else f"  [{status}]"
        print(f"  {date_str}: {n_trips} trip(s){flag}", file=sys.stderr)
    if dropped:
        print(f"\nPruned {len(dropped)} date(s) outside the {keep_days}-day window: "
              f"{', '.join(dropped)}", file=sys.stderr)
    print(f"\nStore now has {len(store['days'])} date(s): "
          f"{', '.join(sorted(store['days']))}", file=sys.stderr)

    incomplete = [d for d, _, s in results if s.startswith("INCOMPLETE")]
    if incomplete:
        print(f"\nNOTE: {len(incomplete)} date(s) are incomplete: {', '.join(incomplete)}. "
              "Re-run fetch_week.py or fetch_day.py for those dates later to finish them.",
              file=sys.stderr)


if __name__ == "__main__":
    main()
