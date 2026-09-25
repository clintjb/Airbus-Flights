#!/usr/bin/env python3
"""
Manual, incremental data fetch for Airbus test/production/customer-
acceptance flights at Toulouse-Blagnac (LFBO) and Hamburg-Finkenwerder
(EDHI). Each run fetches ONE UTC date and merges it into a single central
JSON store, keyed by date -- so you can build up a week's worth of data
over several separate runs (e.g. one per day, to stay within OpenSky's
daily credit limits) rather than needing it all in one sitting.

This is NOT part of any automated pipeline. Run it yourself, by hand,
whenever you want to add a day. It talks to the OpenSky Network API,
merges that day into the store file, and does nothing else. You then
commit the store file to your Jekyll repo yourself.

------------------------------------------------------------------------
Usage
------------------------------------------------------------------------
    export OPENSKY_CLIENT_ID=...
    export OPENSKY_CLIENT_SECRET=...

    # Add one day to the store (run this once per day you want, on
    # however many separate occasions you like -- e.g. once daily for a
    # week):
    python3 fetch_day.py 2026-12-09 -o data/store.json
    python3 fetch_day.py 2026-12-10 -o data/store.json
    python3 fetch_day.py 2026-12-11 -o data/store.json
    ...

    # Re-running a date you've already fetched REPLACES that date's data
    # in the store (no duplicates) -- handy if a day came back incomplete
    # due to a rate limit and you want to retry it later.

    # Or, for a local-only setup, fill in OPENSKY_CLIENT_ID /
    # OPENSKY_CLIENT_SECRET directly near the top of this file instead of
    # using environment variables -- see the warning next to those
    # constants before doing that.

    # No OpenSky account yet, or just want to test the rest of the pipeline:
    python3 fetch_day.py 2026-12-09 --synthetic -o data/store.json

    # Optional: registration-based classification (in addition to callsign)
    curl -o /tmp/aircraftDatabase.csv \
      https://s3.opensky-network.org/data-samples/metadata/aircraftDatabase.csv
    python3 fetch_day.py 2026-12-09 --registry /tmp/aircraftDatabase.csv \
      -o data/store.json

    # See what's currently in the store without fetching anything:
    python3 fetch_day.py --list -o data/store.json

------------------------------------------------------------------------
Why one date per run, run by hand
------------------------------------------------------------------------
OpenSky's /tracks endpoint (actual flown trajectories) only covers the
last ~30 days from *now* -- there is no way to fetch a date further back,
free or otherwise, without an approved research account. So there is
nothing to automate: this only ever works for a date you pick that is
currently within 30 days.

Fetching one date per run (rather than a wide --days range in one call)
also naturally sidesteps OpenSky's daily rate limits: /tracks/all draws
from its own credit pool, separate from /flights/*, and a multi-day fetch
in one sitting can exhaust it partway through (see the README). Spreading
a week across several runs on different days keeps each run's usage well
within the daily allowance.

------------------------------------------------------------------------
Identifying Airbus test / production / customer-acceptance flights
------------------------------------------------------------------------
A flight counts if ANY of:
  - its registration starts with F-W (Airbus's own long-term test/
    development aircraft AND every customer aircraft still in flight
    test/acceptance before permanent registration and delivery), or
  - its ATC callsign starts with AIB (Airbus Industrie callsign block
    used for test, ferry, and delivery flights), or
  - its registration starts with F-GST or F-GXL, or its callsign starts
    with BGA or BCO (the Beluga ST and BelugaXL oversized-cargo fleet -- these
    are Airbus's own aircraft shuttling parts between sites, most
    visibly TLS<->XFW, but they don't use F-W registrations or AIB
    callsigns like the test/delivery fleet does, so they need their own
    rule to be picked up at all)
Registration matching requires --registry (see above); without it,
classification falls back to callsign-only matching.

Output is scoped to LFBO and EDHI departures/arrivals only.
"""
import argparse
import csv
import json
import math
import os
import random
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

AIRPORTS = {
    "TLS": {"icao": "LFBO", "lat": 43.6293, "lon": 1.3638, "name": "Toulouse-Blagnac"},
    "XFW": {"icao": "EDHI", "lat": 53.5358, "lon": 9.8356, "name": "Hamburg-Finkenwerder"},
}

REGISTRATION_PREFIXES = ["F-W", "F-GST", "F-GXL"]
CALLSIGN_PREFIXES = ["AIB", "BGA", "BCO"]

# ---------------------------------------------------------------------------
# OpenSky credentials -- fill these in directly if running this locally only.
#
# ONLY do this if fetch_day.py itself never gets committed/pushed to a public
# (or otherwise shared) repo -- this file is meant to sit in a Jekyll repo's
# tree, and GitHub Pages repos are commonly public. If there's any chance
# this file is tracked by git, either:
#   (a) leave these blank and use environment variables instead:
#         export OPENSKY_CLIENT_ID=...
#         export OPENSKY_CLIENT_SECRET=...
#   (b) or add this exact filename to .gitignore before filling these in.
# Environment variables always take priority below, so setting them still
# works even if you've also filled in values here.
# ---------------------------------------------------------------------------
OPENSKY_CLIENT_ID = ""       # e.g. "abc123..."
OPENSKY_CLIENT_SECRET = ""   # e.g. "xyz789..."

TOKEN_URL = (
    "https://auth.opensky-network.org/auth/realms/opensky-network"
    "/protocol/openid-connect/token"
)
API_ROOT = "https://opensky-network.org/api"

# If OpenSky's 429 response asks us to wait longer than this, treat it as a
# spent daily credit budget (not a short throttle) and stop instead of
# blocking -- OpenSky's rate-limit windows can be many hours long since the
# three credit pools (states/tracks/flights) reset once per day.
MAX_RATE_LIMIT_SLEEP_SECONDS = 120


class RateLimitExceeded(RuntimeError):
    pass


# ---------------------------------------------------------------------------
# OpenSky client (inlined here so this whole thing is one file to run)
# ---------------------------------------------------------------------------
class OpenSkyClient:
    def __init__(self):
        self.client_id = os.environ.get("OPENSKY_CLIENT_ID") or OPENSKY_CLIENT_ID
        self.client_secret = os.environ.get("OPENSKY_CLIENT_SECRET") or OPENSKY_CLIENT_SECRET
        self._token = None
        self._expires_at = 0
        # Last-seen X-Rate-Limit-Remaining per endpoint bucket (states/tracks/
        # flights are billed independently -- see OpenSky's "API Credits"
        # docs). None until we've made at least one successful call to that
        # bucket this run. This is a running record of what the server told
        # us on the last call to each bucket, not a live poll.
        self.credits_remaining = {"states": None, "tracks": None, "flights": None}

    @property
    def authenticated(self):
        return bool(self.client_id and self.client_secret)

    def _refresh_token(self):
        data = urllib.parse.urlencode({
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }).encode()
        req = urllib.request.Request(TOKEN_URL, data=data, method="POST")
        req.add_header("Content-Type", "application/x-www-form-urlencoded")
        with urllib.request.urlopen(req, timeout=30) as resp:
            payload = json.loads(resp.read().decode())
        self._token = payload["access_token"]
        self._expires_at = time.time() + payload.get("expires_in", 1800) - 30
        return self._token

    def _get_token(self):
        if self._token and time.time() < self._expires_at:
            return self._token
        return self._refresh_token()

    # Which credit bucket (per OpenSky's independent states/tracks/flights
    # pools) a given endpoint path draws from.
    @staticmethod
    def _bucket_for(path):
        if path.startswith("/tracks"):
            return "tracks"
        if path.startswith("/flights"):
            return "flights"
        if path.startswith("/states"):
            return "states"
        return None

    def _record_remaining(self, path, headers):
        bucket = self._bucket_for(path)
        if bucket is None:
            return
        raw = headers.get("X-Rate-Limit-Remaining")
        if raw is None:
            return
        try:
            self.credits_remaining[bucket] = int(raw)
        except ValueError:
            pass

    def _get(self, path, params, retries=3, debug_label=None):
        bucket = self._bucket_for(path)
        if bucket and self.credits_remaining.get(bucket) == 0:
            # We already know, from a previous response in this run, that
            # this bucket is at zero. Don't spend a real request just to
            # have the server tell us the same thing again with a 429 --
            # raise the same exception a 429 would, without the round trip.
            raise RateLimitExceeded(
                f"Skipping {path}: the {bucket} credit bucket was already at "
                f"0 as of the last response we saw from it this run. "
                f"Not spending a request to confirm what we already know."
            )
        url = f"{API_ROOT}{path}?" + urllib.parse.urlencode(params)
        for attempt in range(retries):
            req = urllib.request.Request(url)
            req.add_header("Authorization", f"Bearer {self._get_token()}")
            try:
                with urllib.request.urlopen(req, timeout=60) as resp:
                    self._record_remaining(path, resp.headers)
                    body = resp.read().decode()
                    if debug_label:
                        print(f"    [debug] {debug_label}: HTTP {resp.status}, "
                              f"{len(body)} bytes: {body[:200]!r}", file=sys.stderr)
                    if not body:
                        return None
                    return json.loads(body)
            except urllib.error.HTTPError as e:
                self._record_remaining(path, e.headers)
                body = e.read().decode(errors="replace")
                if debug_label:
                    print(f"    [debug] {debug_label}: HTTP {e.code}: {body[:200]!r}", file=sys.stderr)
                if e.code == 404:
                    return None
                if e.code == 401:
                    self._token = None
                    continue
                if e.code == 429:
                    if bucket:
                        self.credits_remaining[bucket] = 0
                    wait = int(e.headers.get("X-Rate-Limit-Retry-After-Seconds", "30"))
                    if wait > MAX_RATE_LIMIT_SLEEP_SECONDS:
                        raise RateLimitExceeded(
                            f"OpenSky rate limit hit on {path} -- server says "
                            f"wait {wait}s (~{wait / 3600:.1f}h) before retrying. "
                            f"That's almost certainly a daily credit budget "
                            f"(states/tracks/flights are separate pools), not a "
                            f"short-term throttle, so retrying now won't help. "
                            f"Stopping instead of blocking for {wait}s. "
                            f"Re-run this date later once the quota resets."
                        )
                    print(f"  rate limited, waiting {wait}s (attempt {attempt + 1}/{retries})...",
                          file=sys.stderr)
                    time.sleep(wait)
                    continue
                raise RuntimeError(f"GET {path} failed ({e.code}): {body}") from e
        raise RuntimeError(f"GET {path} failed after {retries} retries")

    def departures(self, icao, begin_ts, end_ts):
        return self._get("/flights/departure", {"airport": icao, "begin": begin_ts, "end": end_ts})

    def arrivals(self, icao, begin_ts, end_ts):
        return self._get("/flights/arrival", {"airport": icao, "begin": begin_ts, "end": end_ts})

    def track(self, icao24, at_ts, debug=False):
        return self._get("/tracks/all", {"icao24": icao24, "time": at_ts},
                          debug_label=f"tracks icao24={icao24} time={at_ts}" if debug else None)

    def print_credit_status(self, prefix=""):
        """Print what we currently know about remaining credits per bucket,
        based on the last X-Rate-Limit-Remaining header seen from each. A
        bucket shows '(unknown)' until at least one call has been made to it
        this run -- this is not a live poll, just a record of the last
        response.
        """
        parts = []
        for bucket in ("flights", "tracks", "states"):
            val = self.credits_remaining.get(bucket)
            parts.append(f"{bucket}={val if val is not None else '(unknown)'}")
        print(f"{prefix}Credits remaining this run (last seen): {', '.join(parts)}",
              file=sys.stderr)


# ---------------------------------------------------------------------------
def day_bounds_utc(date_str):
    """Returns (begin_ts, end_ts) for the UTC calendar day date_str."""
    start = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return int(start.timestamp()), int((start + timedelta(days=1)).timestamp())


def chunk_range(begin_ts, end_ts, max_span_seconds):
    """Split [begin_ts, end_ts) into consecutive chunks no longer than
    max_span_seconds, respecting OpenSky's per-call window limits. A single
    UTC day is always under this limit, but kept generic/defensive."""
    chunks = []
    t = begin_ts
    while t < end_ts:
        t2 = min(t + max_span_seconds, end_ts)
        chunks.append((t, t2))
        t = t2
    return chunks


def load_registry(csv_path):
    """icao24 -> basic aircraft metadata from OpenSky's aircraft database.

    Keeping the model as well as the registration lets the visualisation show
    a useful high-level identity without making another API request.
    """
    registry = {}
    if not csv_path:
        return registry
    with open(csv_path, newline="", encoding="utf-8", errors="replace") as f:
        for row in csv.DictReader(f):
            icao24 = (row.get("icao24") or "").strip().lower()
            if not icao24:
                continue
            registry[icao24] = {
                "registration": (row.get("registration") or "").strip().upper() or None,
                "model": (row.get("model") or row.get("typecode") or "").strip() or None,
            }
    print(f"  loaded {len(registry)} aircraft records from {csv_path}", file=sys.stderr)
    return registry


def classify(callsign, registration):
    callsign = (callsign or "").strip().upper()
    registration = (registration or "").strip().upper()
    for p in REGISTRATION_PREFIXES:
        if registration.startswith(p):
            return f"registration:{p}"
    for p in CALLSIGN_PREFIXES:
        if callsign.startswith(p):
            return f"callsign:{p}"
    return None


# OpenSky's documented limit: /flights/departure and /flights/arrival windows
# must not exceed 2 days (this script uses a conservative ~1.9 days per call
# to stay clear of the boundary).
MAX_FLIGHTS_WINDOW_SECONDS = int(1.9 * 86400)


def fetch_site_flights(client, icao, begin_ts, end_ts):
    all_flights = {}
    chunks = chunk_range(begin_ts, end_ts, MAX_FLIGHTS_WINDOW_SECONDS)
    for i, (c_begin, c_end) in enumerate(chunks):
        label = f" (chunk {i + 1}/{len(chunks)})" if len(chunks) > 1 else ""
        print(f"  fetching departures/arrivals for {icao}{label}...", file=sys.stderr)
        deps = client.departures(icao, c_begin, c_end) or []
        time.sleep(1)
        arrs = client.arrivals(icao, c_begin, c_end) or []
        time.sleep(1)
        for f in deps + arrs:
            all_flights[(f.get("icao24"), f.get("firstSeen"))] = f
    return list(all_flights.values())


def candidate_key(f):
    """Stable key for a flight record, used to resume interrupted track fetches."""
    return f"{(f.get('icao24') or '').strip().lower()}-{int(f.get('firstSeen') or 0)}"


def find_candidates(client, date_str, registry):
    begin_ts, end_ts = day_bounds_utc(date_str)
    candidates = []
    seen = set()

    for site, ap in AIRPORTS.items():
        flights = fetch_site_flights(client, ap["icao"], begin_ts, end_ts)
        print(f"  {site}: {len(flights)} total flights in window", file=sys.stderr)
        for f in flights:
            icao24 = (f.get("icao24") or "").strip().lower()
            aircraft = registry.get(icao24, {})
            reason = classify(f.get("callsign"), aircraft.get("registration"))
            if not reason:
                continue
            key = candidate_key(f)
            if not icao24 or not f.get("firstSeen") or key in seen:
                continue
            seen.add(key)
            candidates.append({
                "key": key,
                "site": site,
                "icao24": icao24,
                "callsign": (f.get("callsign") or "").strip(),
                "registration": aircraft.get("registration"),
                "model": aircraft.get("model"),
                "match_reason": reason,
                "firstSeen": int(f["firstSeen"]),
            })

    client.print_credit_status(prefix="  ")
    return candidates


def print_candidates(candidates):
    print(f"Matched {len(candidates)} candidate flights:", file=sys.stderr)
    for f in candidates:
        print(
            f"    {f['site']}  icao24={f['icao24']}  "
            f"callsign={f['callsign'] or '(none)'}  "
            f"registration={f['registration'] or '(unknown)'}  "
            f"model={f.get('model') or '(unknown)'}  "
            f"matched_via={f['match_reason']}  firstSeen={f['firstSeen']}",
            file=sys.stderr,
        )


def purpose_for(candidate):
    """A deliberately high-level description based on the identification rule."""
    reason = candidate.get("match_reason", "")
    reg = (candidate.get("registration") or "").upper()
    if reg.startswith("F-GXL"):
        return "Beluga transport"
    if reg.startswith("F-GST") or reason.startswith("callsign:BGA") or reason.startswith("callsign:BCO"):
        return "Beluga transport"
    if reason.startswith("registration:F-W"):
        return "Flight test"
    if reason.startswith("callsign:AIB"):
        return "Airbus operation"
    return "Airbus operation"


def fallback_model(trip):
    reg = (trip.get("registration") or "").upper()
    if reg.startswith("F-GXL"):
        return "BelugaXL"
    if reg.startswith("F-GST"):
        return "BelugaST"
    icao24 = (trip.get("icao24") or "").lower()
    if icao24 in KNOWN_BELUGAXL_ICAO24:
        return "BelugaXL"
    return trip.get("model") or "Aircraft"


# icao24 hex codes for the current BelugaXL fleet, used as a fallback ONLY
# when no --registry file was supplied (so trip["registration"] is None) --
# this lets the model label still say "BelugaXL" instead of the generic
# "Aircraft" without needing to download OpenSky's full aircraft database
# just for six known airframes.
#
# This is deliberately a set of icao24 codes only, NOT an icao24->registration
# mapping: which specific tail number (F-GXLH, F-GXLI, F-GXLJ, F-GXLN, F-GXLO,
# and a sixth) corresponds to which of these hex codes is inconsistently
# reported across sources at the time this was written, so guessing a specific
# registration risks mislabeling a real aircraft with the wrong tail number --
# worse than just not showing one. If you have a confirmed, current
# icao24->registration mapping for the fleet, add it to a --registry CSV
# instead (see load_registry) rather than editing this set, and it will take
# priority over this fallback automatically.
KNOWN_BELUGAXL_ICAO24 = {
    "395d67",  # F-GXLH, confirmed
    "395d68",  # F-GXLI, confirmed
    "395d69",
    "395d6d",
    "395d6e",
    "395d66",
}


def fetch_tracks(client, candidates, existing_entry, max_tracks=None):
    """Fetch only candidates that have not already been checked.

    This is deliberately resumable: a rate limit no longer forces us to repeat
    the expensive airport queries or re-fetch tracks that were already saved.
    """
    existing_trips = {
        trip["id"]: trip
        for trip in (existing_entry or {}).get("trips", [])
        if trip.get("id")
    }
    candidate_by_id = {f"{c['icao24']}-{c['firstSeen']}": c for c in candidates}
    for trip_id, trip in existing_trips.items():
        candidate = candidate_by_id.get(trip_id)
        if candidate:
            if not trip.get("model"):
                trip["model"] = fallback_model(candidate)
            if not trip.get("purpose"):
                trip["purpose"] = purpose_for(candidate)
    checked = set((existing_entry or {}).get("meta", {}).get("checked_candidates", []))
    trips = list(existing_trips.values())
    rate_limited = False
    attempted = 0

    pending = [c for c in candidates if c["key"] not in checked and
               f"{c['icao24']}-{c['firstSeen']}" not in existing_trips]

    if not pending:
        print("All candidate tracks have already been checked; nothing to fetch.", file=sys.stderr)
        return trips, checked, False

    print(
        f"Fetching tracks for {len(pending)} remaining candidate flight(s) "
        f"({len(candidates) - len(pending)} already resolved)...",
        file=sys.stderr,
    )

    for c in pending:
        if max_tracks is not None and attempted >= max_tracks:
            print(f"Stopping after --max-tracks={max_tracks}; run again to continue.", file=sys.stderr)
            break
        attempted += 1
        try:
            track = client.track(c["icao24"], c["firstSeen"], debug=True)
        except RateLimitExceeded as e:
            print(f"\n{e}", file=sys.stderr)
            client.print_credit_status(prefix="  ")
            rate_limited = True
            break
        except Exception as e:
            # Don't mark transient/server errors as checked: a later run can retry.
            print(f"  track fetch failed for {c['icao24']} ({c['callsign']}): {e}", file=sys.stderr)
            continue

        checked.add(c["key"])
        if not track or not track.get("path"):
            print(
                f"  no track data for {c['icao24']} ({c['callsign']}, {c['site']})",
                file=sys.stderr,
            )
            continue

        path = [
            [int(pt[0]), round(pt[1], 4), round(pt[2], 4)]
            for pt in track["path"]
            if len(pt) >= 3 and pt[1] is not None and pt[2] is not None
        ]
        if len(path) < 2:
            print(
                f"  track for {c['icao24']} ({c['callsign']}) had only "
                f"{len(path)} usable point(s) -- skipped",
                file=sys.stderr,
            )
            continue

        trip_id = f"{c['icao24']}-{c['firstSeen']}"
        existing_trips[trip_id] = {
            "id": trip_id,
            "site": c["site"],
            "icao24": c["icao24"],
            "callsign": c["callsign"],
            "registration": c["registration"],
            "model": fallback_model(c),
            "purpose": purpose_for(c),
            "match_reason": c["match_reason"],
            "path": path,
        }
        trips = list(existing_trips.values())

    return trips, checked, rate_limited


def build_real(date_str, registry_csv, existing_entry=None, max_tracks=None):
    client = OpenSkyClient()
    if not client.authenticated:
        print(
            "ERROR: OPENSKY_CLIENT_ID / OPENSKY_CLIENT_SECRET not set.",
            file=sys.stderr,
        )
        sys.exit(1)

    existing_meta = (existing_entry or {}).get("meta", {})
    candidates = existing_meta.get("candidates")
    registry = load_registry(registry_csv)

    # Cache the candidate list in the store. Airport flight queries are only
    # needed once per date; subsequent runs concentrate exclusively on tracks.
    flights_rate_limited = False
    if candidates is None:
        try:
            candidates = find_candidates(client, date_str, registry)
        except RateLimitExceeded as e:
            print(f"\n{e}", file=sys.stderr)
            client.print_credit_status(prefix="  ")
            print(
                f"Can't find candidates for {date_str} -- the flights/* credit "
                "bucket is exhausted. Re-run this date later once it resets.",
                file=sys.stderr,
            )
            candidates = []
            flights_rate_limited = True
        else:
            print_candidates(candidates)
    else:
        # Older cached runs may pre-date model metadata. Enrich them locally
        # when a registry is supplied without repeating any OpenSky API calls.
        for c in candidates:
            aircraft = registry.get(c.get("icao24"), {})
            if not c.get("registration"):
                c["registration"] = aircraft.get("registration")
            if not c.get("model"):
                c["model"] = aircraft.get("model")
        print(
            f"Resuming {date_str}: using {len(candidates)} cached candidate "
            "flight(s); skipping airport API queries.",
            file=sys.stderr,
        )
        print_candidates(candidates)

    if flights_rate_limited:
        trips = list((existing_entry or {}).get("trips", []))
        checked = set((existing_entry or {}).get("meta", {}).get("checked_candidates", []))
        rate_limited = True
    else:
        trips, checked, rate_limited = fetch_tracks(
            client, candidates, existing_entry, max_tracks=max_tracks
        )
    complete = len(checked) >= len(candidates) and not rate_limited
    meta = {
        "date": date_str,
        "synthetic": False,
        "candidates": candidates,
        "checked_candidates": sorted(checked),
        "complete": complete,
    }
    if rate_limited:
        meta["incomplete_rate_limited"] = True
    return trips, meta

def build_synthetic(date_str):
    random.seed(date_str)
    begin_ts, _ = day_bounds_utc(date_str)
    trips = []
    for site, ap in AIRPORTS.items():
        n = 14 if site == "TLS" else 9
        for i in range(n):
            start_s = begin_ts + random.randint(6 * 3600, 18 * 3600)
            dur_s = random.randint(20 * 60, 90 * 60)
            heading = random.uniform(0, 360)
            dist_deg = random.uniform(0.3, 1.5)
            dx = dist_deg * math.sin(math.radians(heading))
            dy = dist_deg * math.cos(math.radians(heading))
            n_pts = 12
            path = []
            for p in range(n_pts + 1):
                frac = p / n_pts
                arc = math.sin(frac * math.pi)
                lat = ap["lat"] + dy * arc * frac
                lon = ap["lon"] + dx * arc * frac
                t = start_s + int(dur_s * frac)
                path.append([t, round(lat, 4), round(lon, 4)])
            trips.append({
                "id": f"SYN-{site}-{date_str}-{i:03d}",
                "site": site,
                "icao24": f"synth{i:02x}{site.lower()}",
                "callsign": f"AIB{i:04d}"[:8],
                "registration": "F-WSYN" if i % 2 == 0 else "F-WTST",
                "model": "A350-900" if i % 2 == 0 else "A320neo",
                "purpose": "Flight test",
                "match_reason": "synthetic",
                "path": path,
            })
    return trips, {"date": date_str, "synthetic": True,
                   "note": "Placeholder data for testing. Not real flights."}


def load_store(path):
    """Load the central store file, or return an empty one if it doesn't exist yet."""
    if not os.path.exists(path):
        return {"airports": AIRPORTS, "days": {}}
    with open(path) as f:
        store = json.load(f)
    store.setdefault("airports", AIRPORTS)
    store.setdefault("days", {})
    return store


def save_store(store, path):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w") as f:
        json.dump(store, f, separators=(",", ":"))


def prune_store(store, keep_days, reference_date=None):
    """Keep only the `keep_days` most recent dates in the store, counting
    back from `reference_date` (default: today, UTC). Returns the list of
    dates that were dropped.

    This is what makes the store a rolling window: every successful fetch
    run trims anything older than `keep_days` back from the reference date,
    so the store never grows unbounded and always reflects "the last N
    days" relative to whatever date you just fetched up to -- not
    necessarily wall-clock "today" (e.g. fetch_week.py passes its --end
    date, so pruning stays consistent with the range that was just fetched
    even if --end wasn't today).
    """
    if keep_days is None:
        return []
    all_dates = sorted(store.get("days", {}).keys())
    if len(all_dates) <= keep_days:
        return []
    if reference_date is None:
        ref = datetime.now(timezone.utc).date()
    else:
        ref = datetime.strptime(reference_date, "%Y-%m-%d").date()
    cutoff = ref - timedelta(days=keep_days - 1)
    dropped = [d for d in all_dates if datetime.strptime(d, "%Y-%m-%d").date() < cutoff]
    for d in dropped:
        del store["days"][d]
    return dropped


def list_store(path):
    store = load_store(path)
    days = store.get("days", {})
    if not days:
        print(f"{path}: empty, or doesn't exist yet.")
        return
    print(f"{path}: {len(days)} date(s) stored")
    for date in sorted(days):
        entry = days[date]
        meta = entry.get("meta", {})
        n_trips = len(entry.get("trips", []))
        flags = []
        if meta.get("synthetic"):
            flags.append("SYNTHETIC")
        if meta.get("incomplete_rate_limited"):
            flags.append("INCOMPLETE (rate limited)")
        elif meta.get("complete") is False:
            flags.append("INCOMPLETE")
        flag_str = f"  [{', '.join(flags)}]" if flags else ""
        print(f"  {date}: {n_trips} trip(s){flag_str}")


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                  formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("date", nargs="?", help="UTC date to fetch, YYYY-MM-DD")
    ap.add_argument("-o", "--output", required=True,
                     help="Path to the central store JSON, e.g. data/store.json. "
                          "Created if it doesn't exist; existing dates are preserved "
                          "and merged with, not overwritten, except for the date "
                          "you're fetching this run.")
    ap.add_argument("--list", action="store_true",
                     help="List the dates currently in the store and exit "
                          "(no fetching, no date argument needed).")
    ap.add_argument("--synthetic", action="store_true",
                     help="Generate placeholder data instead of calling OpenSky")
    ap.add_argument("--registry", metavar="CSV",
                     help="Path to OpenSky aircraftDatabase.csv for registration matching")
    ap.add_argument("--max-tracks", type=int, metavar="N",
                     help="Fetch at most N new tracks this run. Useful for deliberately "
                          "spreading a date across quota windows; later runs resume automatically.")
    ap.add_argument("--keep-days", type=int, metavar="N", default=None,
                     help="After saving, drop any stored date older than N days ago (by "
                          "calendar date), keeping the store as a rolling N-day window. "
                          "Omit to keep every date ever fetched (old default behaviour).")
    args = ap.parse_args()

    if args.list:
        list_store(args.output)
        return

    if not args.date:
        print("ERROR: a date is required unless using --list", file=sys.stderr)
        sys.exit(1)

    try:
        datetime.strptime(args.date, "%Y-%m-%d")
    except ValueError:
        print(f"ERROR: '{args.date}' isn't a valid YYYY-MM-DD date", file=sys.stderr)
        sys.exit(1)

    store = load_store(args.output)
    already_present = args.date in store["days"]

    if args.synthetic:
        print(f"Building SYNTHETIC dataset for {args.date} (no API calls).", file=sys.stderr)
        trips, meta = build_synthetic(args.date)
    else:
        trips, meta = build_real(args.date, args.registry, store.get("days", {}).get(args.date), args.max_tracks)

    store["days"][args.date] = {"meta": meta, "trips": trips}
    dropped = prune_store(store, args.keep_days)
    save_store(store, args.output)

    action = "Replaced" if already_present else "Added"
    print(f"{action} {args.date} in {args.output}: {len(trips)} trip(s).", file=sys.stderr)
    if dropped:
        print(f"Pruned {len(dropped)} date(s) older than the {args.keep_days}-day "
              f"window: {', '.join(dropped)}", file=sys.stderr)
    print(f"Store now has {len(store['days'])} date(s): "
          f"{', '.join(sorted(store['days']))}", file=sys.stderr)

    if meta.get("incomplete_rate_limited"):
        remaining = len(meta.get("candidates", [])) - len(meta.get("checked_candidates", []))
        print(
            f"NOTE: {args.date} is incomplete because OpenSky's track quota was exhausted. "
            f"{remaining} candidate track(s) remain. Re-run the same command after the "
            "quota resets: the script will resume from the cached candidates and will not "
            "repeat airport queries or tracks already checked.",
            file=sys.stderr,
        )
    elif not meta.get("synthetic") and not meta.get("complete", True):
        print(
            f"NOTE: {args.date} is intentionally incomplete; run the same command again "
            "to continue from where it stopped.",
            file=sys.stderr,
        )


if __name__ == "__main__":
    main()
