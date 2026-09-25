#!/usr/bin/env python3
"""
Turn some or all dates from a central store (built up incrementally by
fetch_day.py) into a single embeddable HTML fragment you can paste
straight into a Jekyll post -- same shape as your usual pattern:

    <div style="height:1700px; width:100%;">
      <script>...</script>
    </div>

Usage:
    # Everything currently in the store:
    python3 build/make_fragment.py data/store.json -o fragment.html

    # Just a specific range (e.g. once you've built up a full week):
    python3 build/make_fragment.py data/store.json --from 2026-12-08 --to 2026-12-14 -o fragment.html

    # Just one date:
    python3 build/make_fragment.py data/store.json --from 2026-12-09 --to 2026-12-09 -o fragment.html

Then in your Jekyll post/markdown file, either:
  (a) paste the fragment's contents directly inline, or
  (b) save it under _includes/ and pull it in with:
      {% include fragment.html %}

No build step runs on GitHub Pages for this -- the fragment is fully
pre-rendered HTML+JS+data, so Jekyll just serves it as-is, exactly like a
static include. Run this once you've accumulated however many dates you
want in the store (e.g. after a week of daily fetch_day.py runs) -- there's
no need to regenerate the fragment after every single fetch_day.py run
unless you want to preview progress along the way.
"""
import argparse
import json
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).parent.parent


def daterange(from_date, to_date):
    d = from_date
    while d <= to_date:
        yield d.strftime("%Y-%m-%d")
        d += timedelta(days=1)


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                  formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("store_json", help="Path to the central store, e.g. data/store.json")
    ap.add_argument("-o", "--output", help="Output fragment path (default: fragment.html)")
    ap.add_argument("--from", dest="from_date", metavar="YYYY-MM-DD",
                     help="First date to include (default: earliest date in the store)")
    ap.add_argument("--to", dest="to_date", metavar="YYYY-MM-DD",
                     help="Last date to include, inclusive (default: latest date in the store)")
    ap.add_argument("--geo", default=str(ROOT / "data" / "basemap.json"))
    ap.add_argument("--template", default=str(ROOT / "build" / "fragment_template.html"))
    ap.add_argument("--widget-id", help="Fixed widget id instead of a random one (useful for reproducible builds)")
    args = ap.parse_args()

    with open(args.store_json) as f:
        store = json.load(f)
    with open(args.geo) as f:
        geo = json.load(f)

    all_dates = sorted(store.get("days", {}).keys())
    if not all_dates:
        raise SystemExit(f"{args.store_json} has no dates yet -- run fetch_day.py first.")

    from_date = args.from_date or all_dates[0]
    to_date = args.to_date or all_dates[-1]
    from_dt = datetime.strptime(from_date, "%Y-%m-%d")
    to_dt = datetime.strptime(to_date, "%Y-%m-%d")
    if to_dt < from_dt:
        raise SystemExit(f"--to ({to_date}) is before --from ({from_date})")

    wanted = list(daterange(from_dt, to_dt))
    selected = [d for d in wanted if d in store["days"]]
    missing = [d for d in wanted if d not in store["days"]]

    if not selected:
        raise SystemExit(
            f"None of the requested dates ({from_date}..{to_date}) are in "
            f"{args.store_json}. Dates available: {', '.join(all_dates)}"
        )
    if missing:
        print(f"Note: {len(missing)} date(s) in the requested range aren't in "
              f"the store yet, skipping: {', '.join(missing)}")

    num_days = len(selected)
    range_start_ts = int(
        datetime.strptime(selected[0], "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()
    )

    all_trips = []
    any_synthetic = False
    any_incomplete = False
    for date in selected:
        entry = store["days"][date]
        entry_meta = entry.get("meta", {})
        if entry_meta.get("synthetic"):
            any_synthetic = True
        if entry_meta.get("incomplete_rate_limited"):
            any_incomplete = True
        for trip in entry.get("trips", []):
            # Store holds absolute UTC epoch seconds; the fragment template
            # expects offsets relative to the selected range's start.
            shifted = dict(trip)
            shifted["path"] = [
                [pt[0] - range_start_ts, pt[1], pt[2]] for pt in trip["path"]
            ]
            all_trips.append(shifted)

    combined_meta = {
        "date": selected[0],
        "days": num_days,
        "synthetic": any_synthetic,
    }
    if any_incomplete:
        combined_meta["incomplete_rate_limited"] = True
    if len(selected) < len(wanted):
        combined_meta["missing_dates"] = missing

    flights = {"meta": combined_meta, "trips": all_trips}

    output_path = args.output or "fragment.html"
    widget_id = args.widget_id or uuid.uuid4().hex[:8]

    data = {"geo": geo, "flights": flights}
    data_js = json.dumps(data, separators=(",", ":"))

    with open(args.template) as f:
        template = f.read()

    if "__DATA_PLACEHOLDER__" not in template or "__WIDGET_ID__" not in template:
        raise SystemExit("fragment_template.html missing required placeholders")

    out = template.replace("__WIDGET_ID__", widget_id).replace("__DATA_PLACEHOLDER__", data_js)

    with open(output_path, "w") as f:
        f.write(out)

    size_kb = len(out.encode()) / 1024
    print(f"Wrote {output_path} ({size_kb:.1f} KB), widget id: {widget_id}")
    print(f"  {num_days} date(s): {', '.join(selected)}")
    print(f"  {len(all_trips)} total trip(s)")
    if any_incomplete:
        print("  NOTE: at least one included date was a partial/rate-limited "
              "fetch -- see the store's per-date meta for which one.")


if __name__ == "__main__":
    main()
