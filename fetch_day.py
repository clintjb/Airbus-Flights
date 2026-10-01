#!/usr/bin/env python3
"""
Manual, incremental data fetch for Airbus test/production/customer-
acceptance flights at Toulouse-Blagnac (LFBO) and Hamburg-Finkenwerder
(EDHI). Each run fetches ONE UTC date and merges it into a single central
JSON store, keyed by date -- so you can build up a week's worth of data
over several separate runs (e.g. one per day, to stay within OpenSky's
daily credit limits) rather than needing it all in one sitting.

Run it by hand to add a day, or let the GitHub Actions workflow
(.github/workflows/update-flights.yml) call it on a schedule. It talks to
the OpenSky Network API and merges that day into the store file.

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

    # Finish / refresh any stored date that is incomplete or whose airport
    # query ran before OpenSky's nightly processing had settled (this is
    # what the workflow runs before fetching "yesterday"):
    python3 fetch_day.py --revisit -o data/store.json

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
classification falls back to callsign-only matching. The prefixes are read
from data/identification_rules.json (single source of truth); the constants
below are only a fallback if that file is missing.

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
from pathlib import Path

AIRPORTS = {
    "TLS": {"icao": "LFBO", "lat": 43.6293, "lon": 1.3638, "name": "Toulouse-Blagnac"},
    "XFW": {"icao": "EDHI", "lat": 53.5358, "lon": 9.8356, "name": "Hamburg-Finkenwerder"},
}

# F-W: French provisional registration, used for airframes built/tested at
# Toulouse before their permanent registration is assigned.
# D-AV/D-AX/D-AZ: the equivalent German pre-delivery test registration
# blocks used for airframes assembled at Hamburg-Finkenwerder (confirmed
# against multiple real Finkenwerder test-flight examples, e.g. D-AVVB,
# D-AZAE, D-AVXY, D-AVXZ). Deliberately NOT the bare "D-A" prefix: that
# covers Germany's entire civil register, including ordinary in-service
# aircraft (e.g. Lufthansa's own D-AIPA, or Condor's D-ANMZ, which are
# permanent operational registrations, not test ones) -- matching on "D-A"
# alone would pull in normal commercial Hamburg traffic, not just test
# flights.
_DEFAULT_REGISTRATION_PREFIXES = ["F-W", "F-GST", "F-GXL", "D-AV", "D-AX", "D-AZ"]
_DEFAULT_CALLSIGN_PREFIXES = ["AIB", "BGA", "BCO"]
RULES_PATH = Path(__file__).resolve().parent / "data" / "identification_rules.json"


def _load_rules():
    """Prefixes from data/identification_rules.json, so the documented rules
    and the code that applies them cannot drift apart."""
    try:
        with open(RULES_PATH, encoding="utf-8") as f:
            rules = json.load(f)
        regs = [r["prefix"].upper() for r in rules["registration_prefixes"]]
        calls = [r["prefix"].upper() for r in rules["callsign_prefixes"]]
        if regs and calls:
            return regs, calls
    except (OSError, ValueError, KeyError, TypeError) as e:
        print(f"WARNING: could not read {RULES_PATH} ({e}); using built-in rules.",
              file=sys.stderr)
    return list(_DEFAULT_REGISTRATION_PREFIXES), list(_DEFAULT_CALLSIGN_PREFIXES)


REGISTRATION_PREFIXES, CALLSIGN_PREFIXES = _load_rules()

# A site counts as "involved" in a flight if the flown track starts or ends
# within this distance of the airport. OpenSky's estimated departure/arrival
# airport is often missing for low-level ADS-B coverage (a known weakness at
# Finkenwerder), so the track itself is the more reliable evidence.
ENDPOINT_RADIUS_KM = 8.0
# Two records of the same aircraft starting within this many seconds are the
# same flight (OpenSky's departure- and arrival-side records for one flight
# can differ by several seconds, which defeated exact-match de-duplication).
DUPLICATE_WINDOW_SECONDS = 300
# Tracks that never move further than this from their first point are ground
# movements (engine runs, taxi tests), not flights.
MIN_FLIGHT_EXTENT_KM = 2.0
# OpenSky processes /flights data in a nightly batch. A candidate list taken
# soon after midnight can be partial, so it is refreshed once more later.
CANDIDATE_SETTLE_HOURS = 12
MAX_TRACK_ATTEMPTS = 3

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


def haversine_km(lat1, lon1, lat2, lon2):
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(min(1.0, math.sqrt(a)))


def sites_near(lat, lon):
    """Sites whose airport is within ENDPOINT_RADIUS_KM of a point."""
    return [site for site, ap in AIRPORTS.items()
            if haversine_km(lat, lon, ap["lat"], ap["lon"]) <= ENDPOINT_RADIUS_KM]


def path_extent_km(path):
    """Furthest distance any track point gets from the first point."""
    lat0, lon0 = path[0][1], path[0][2]
    return max(haversine_km(lat0, lon0, p[1], p[2]) for p in path)


def merge_sites(*lists):
    out = []
    for lst in lists:
        for s in lst or []:
            if s and s not in out:
                out.append(s)
    return out


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


def dedupe_candidates(cands, prefer_keys=frozenset()):
    """Merge records that describe the same flight.

    OpenSky returns a flight between two of our airports from BOTH airports'
    queries, and the two records can differ by a few seconds in firstSeen, so
    an exact (icao24, firstSeen) match is not enough. Records of the same
    aircraft starting within DUPLICATE_WINDOW_SECONDS are merged and the
    merged flight belongs to every site involved. `prefer_keys` (already
    checked candidates) win, so a track is never fetched twice.
    """
    out = []
    for c in sorted(cands, key=lambda c: (c["icao24"], c["firstSeen"])):
        prev = out[-1] if out else None
        if (prev and prev["icao24"] == c["icao24"]
                and abs(c["firstSeen"] - prev["firstSeen"]) <= DUPLICATE_WINDOW_SECONDS):
            sites = merge_sites(prev.get("sites") or [prev["site"]],
                                c.get("sites") or [c["site"]])
            keep = c if (c["key"] in prefer_keys and prev["key"] not in prefer_keys) else prev
            keep["sites"] = sites
            keep["site"] = sites[0]
            out[-1] = keep
        else:
            out.append(c)
    return sorted(out, key=lambda c: c["firstSeen"])


def find_candidates(client, date_str, registry):
    begin_ts, end_ts = day_bounds_utc(date_str)
    icao_to_site = {ap["icao"]: site for site, ap in AIRPORTS.items()}
    raw = []

    for site, ap in AIRPORTS.items():
        flights = fetch_site_flights(client, ap["icao"], begin_ts, end_ts)
        print(f"  {site}: {len(flights)} total flights in window", file=sys.stderr)
        for f in flights:
            icao24 = (f.get("icao24") or "").strip().lower()
            aircraft = registry.get(icao24, {})
            reason = classify(f.get("callsign"), aircraft.get("registration"))
            if not reason or not icao24 or not f.get("firstSeen"):
                continue
            # Every in-scope airport this flight touches, departure first. A
            # TLS<->XFW shuttle belongs on BOTH maps, whichever airport's
            # query happened to return it.
            sites = merge_sites(
                [icao_to_site.get(f.get("estDepartureAirport"))],
                [icao_to_site.get(f.get("estArrivalAirport"))],
                [site],
            )
            raw.append({
                "key": candidate_key(f),
                "site": sites[0],
                "sites": sites,
                "icao24": icao24,
                "callsign": (f.get("callsign") or "").strip(),
                "registration": aircraft.get("registration"),
                "model": aircraft.get("model"),
                "match_reason": reason,
                "firstSeen": int(f["firstSeen"]),
                "lastSeen": int(f["lastSeen"]) if f.get("lastSeen") else None,
                "dep": f.get("estDepartureAirport"),
                "arr": f.get("estArrivalAirport"),
            })

    candidates = dedupe_candidates(raw)
    client.print_credit_status(prefix="  ")
    return candidates


def print_candidates(candidates):
    print(f"Matched {len(candidates)} candidate flights:", file=sys.stderr)
    for f in candidates:
        print(
            f"    {'+'.join(f.get('sites') or [f['site']])}  icao24={f['icao24']}  "
            f"callsign={f['callsign'] or '(none)'}  "
            f"registration={f['registration'] or '(unknown)'}  "
            f"model={f.get('model') or '(unknown)'}  "
            f"matched_via={f['match_reason']}  firstSeen={f['firstSeen']}",
            file=sys.stderr,
        )


BELUGA_CALLSIGN_PREFIXES = ("BGA", "BCO")

# icao24 hex codes of the BelugaXL fleet. Used when no --registry file was
# supplied, so the label still says "BelugaXL" rather than "Aircraft".
#
# The tail letter of the BGA callsign matches the registration's last letter
# for every one of these codes seen in the stored data (e.g. BGA113N, BGA138N
# and BGA183N all use 395d6d; "I" is written "Y", as in BGA212Y/BGA243Y for
# 395d68). That gives the mapping below. 395d6e has not been seen in the data
# yet and is inferred from the sequence.
#   395d66 F-GXLG   395d67 F-GXLH   395d68 F-GXLI
#   395d69 F-GXLJ   395d6d F-GXLN   395d6e F-GXLO
# It is kept as a set (not a registration map) because registration is never
# displayed and a --registry file, when present, takes priority anyway.
KNOWN_BELUGAXL_ICAO24 = {"395d66", "395d67", "395d68", "395d69", "395d6d", "395d6e"}


def is_beluga(rec):
    reg = (rec.get("registration") or "").upper()
    call = (rec.get("callsign") or "").upper()
    icao24 = (rec.get("icao24") or "").lower()
    return (reg.startswith(("F-GXL", "F-GST"))
            or call.startswith(BELUGA_CALLSIGN_PREFIXES)
            or icao24 in KNOWN_BELUGAXL_ICAO24)


def purpose_for(rec):
    """A deliberately high-level description based on the identification rule."""
    if is_beluga(rec):
        return "Beluga transport"
    if (rec.get("match_reason") or "").startswith("registration:"):
        # F-W / D-AV / D-AX / D-AZ: provisional pre-delivery test registrations.
        return "Flight test"
    return "Airbus operation"


def fallback_model(rec):
    reg = (rec.get("registration") or "").upper()
    icao24 = (rec.get("icao24") or "").lower()
    if reg.startswith("F-GXL") or icao24 in KNOWN_BELUGAXL_ICAO24:
        return "BelugaXL"
    if reg.startswith("F-GST"):
        return "BelugaST"
    if (rec.get("callsign") or "").upper().startswith(BELUGA_CALLSIGN_PREFIXES):
        return "Beluga"  # BGA/BCO callsign, but not a known BelugaXL airframe
    return rec.get("model") or "Aircraft"


def fetch_tracks(client, candidates, existing_entry, max_tracks=None):
    """Fetch only candidates that have not already been checked.

    Resumable: a rate limit does not force us to repeat the expensive airport
    queries or re-fetch tracks that were already saved. Returns
    (trips, checked_keys, rate_limited, attempts).
    """
    existing_meta = (existing_entry or {}).get("meta", {})
    existing_trips = {
        trip["id"]: trip
        for trip in (existing_entry or {}).get("trips", [])
        if trip.get("id")
    }
    checked = set(existing_meta.get("checked_candidates", []))
    attempts = dict(existing_meta.get("track_attempts", {}))
    rate_limited = False
    fetched = 0

    pending = [c for c in candidates if c["key"] not in checked and
               f"{c['icao24']}-{c['firstSeen']}" not in existing_trips]

    if not pending:
        print("All candidate tracks have already been checked; nothing to fetch.", file=sys.stderr)
        return list(existing_trips.values()), checked, False, attempts

    print(
        f"Fetching tracks for {len(pending)} remaining candidate flight(s) "
        f"({len(candidates) - len(pending)} already resolved)...",
        file=sys.stderr,
    )

    for c in pending:
        if max_tracks is not None and fetched >= max_tracks:
            print(f"Stopping after --max-tracks={max_tracks}; run again to continue.", file=sys.stderr)
            break
        fetched += 1
        try:
            track = client.track(c["icao24"], c["firstSeen"], debug=True)
        except RateLimitExceeded as e:
            print(f"\n{e}", file=sys.stderr)
            client.print_credit_status(prefix="  ")
            rate_limited = True
            break
        except Exception as e:
            # Transient/server error: retry on a later run, but give up after
            # MAX_TRACK_ATTEMPTS so one bad flight can't keep a date
            # "incomplete" forever.
            attempts[c["key"]] = attempts.get(c["key"], 0) + 1
            print(f"  track fetch failed for {c['icao24']} ({c['callsign']}), "
                  f"attempt {attempts[c['key']]}/{MAX_TRACK_ATTEMPTS}: {e}", file=sys.stderr)
            if attempts[c["key"]] >= MAX_TRACK_ATTEMPTS:
                print("    giving up on this flight.", file=sys.stderr)
                checked.add(c["key"])
            continue

        checked.add(c["key"])
        attempts.pop(c["key"], None)
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
        if path_extent_km(path) < MIN_FLIGHT_EXTENT_KM:
            print(
                f"  track for {c['icao24']} ({c['callsign']}) never leaves a "
                f"{MIN_FLIGHT_EXTENT_KM:g} km radius -- ground movement, skipped",
                file=sys.stderr,
            )
            continue

        # Sites: what the flight record said, plus any airport the flown
        # track actually starts or ends at (robust to missing estimates).
        sites = merge_sites(c.get("sites") or [c["site"]],
                            sites_near(path[0][1], path[0][2]),
                            sites_near(path[-1][1], path[-1][2]))
        trip_id = f"{c['icao24']}-{c['firstSeen']}"
        existing_trips[trip_id] = {
            "id": trip_id,
            "site": sites[0],
            "sites": sites,
            "icao24": c["icao24"],
            "callsign": c["callsign"],
            "registration": c["registration"],
            "model": fallback_model(c),
            "purpose": purpose_for(c),
            "match_reason": c["match_reason"],
            "path": path,
        }

    return list(existing_trips.values()), checked, rate_limited, attempts


def make_client():
    client = OpenSkyClient()
    if not client.authenticated:
        print("ERROR: OPENSKY_CLIENT_ID / OPENSKY_CLIENT_SECRET not set.", file=sys.stderr)
        sys.exit(1)
    return client


def fetch_date(client, date_str, registry, existing_entry=None, max_tracks=None):
    """Fetch (or resume / refresh) one UTC date. Returns (trips, meta).

    Shared by fetch_day.py and fetch_week.py so both behave identically.
    """
    existing_meta = (existing_entry or {}).get("meta", {})
    candidates = existing_meta.get("candidates")
    checked_before = set(existing_meta.get("checked_candidates", []))
    fetched_at = existing_meta.get("candidates_fetched_at")
    day_end = day_bounds_utc(date_str)[1]

    def is_settled(ts):
        return ts is not None and ts >= day_end + CANDIDATE_SETTLE_HOURS * 3600

    flights_rate_limited = False
    if candidates is None or not is_settled(fetched_at):
        # First look at this date, or a look taken before OpenSky's nightly
        # processing had settled -- (re)query the airports and merge.
        try:
            fresh = find_candidates(client, date_str, registry)
        except RateLimitExceeded as e:
            print(f"\n{e}", file=sys.stderr)
            client.print_credit_status(prefix="  ")
            print(f"Can't query flights for {date_str} -- the flights/* credit "
                  "bucket is exhausted. Re-run this date later once it resets.",
                  file=sys.stderr)
            flights_rate_limited = candidates is None
            candidates = candidates or []
        else:
            merged = {c["key"]: c for c in (candidates or [])}
            merged.update({c["key"]: c for c in fresh})
            for c in merged.values():
                c.setdefault("sites", [c["site"]])
            candidates = dedupe_candidates(list(merged.values()), prefer_keys=checked_before)
            fetched_at = int(time.time())
            print_candidates(candidates)
    else:
        # Enrich cached candidates locally when a registry is supplied.
        for c in candidates:
            aircraft = registry.get(c.get("icao24"), {})
            if not c.get("registration"):
                c["registration"] = aircraft.get("registration")
            if not c.get("model"):
                c["model"] = aircraft.get("model")
        print(f"Resuming {date_str}: using {len(candidates)} settled cached "
              "candidate flight(s); skipping airport API queries.", file=sys.stderr)
        print_candidates(candidates)

    if flights_rate_limited:
        trips = list((existing_entry or {}).get("trips", []))
        checked = checked_before
        attempts = dict(existing_meta.get("track_attempts", {}))
        rate_limited = True
    else:
        trips, checked, rate_limited, attempts = fetch_tracks(
            client, candidates, existing_entry, max_tracks=max_tracks)

    trip_ids = {t["id"] for t in trips}
    remaining = [c for c in candidates
                 if c["key"] not in checked and f"{c['icao24']}-{c['firstSeen']}" not in trip_ids]
    meta = {
        "date": date_str,
        "synthetic": False,
        "candidates": candidates,
        "checked_candidates": sorted(checked),
        "candidates_fetched_at": fetched_at,
        "settled": is_settled(fetched_at),
        "complete": (not remaining) and not rate_limited and fetched_at is not None,
    }
    if attempts:
        meta["track_attempts"] = attempts
    if rate_limited:
        meta["incomplete_rate_limited"] = True
    return trips, meta


def needs_revisit(entry):
    """True if a stored date still has work to do (unfinished tracks, or a
    candidate list taken before OpenSky's data had settled)."""
    meta = (entry or {}).get("meta", {})
    if meta.get("synthetic"):
        return False
    return (not meta.get("complete", False)) or (not meta.get("settled", False))


def build_real(date_str, registry_csv, existing_entry=None, max_tracks=None):
    client = make_client()
    registry = load_registry(registry_csv)
    return fetch_date(client, date_str, registry, existing_entry, max_tracks)


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
                "sites": [site],
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


def normalize_store(store):
    """Idempotent clean-up applied every time the store is loaded. Returns the
    number of changes made. It repairs data written by earlier versions:

      * flights between TLS and XFW appeared on only one map (or twice in the
        totals): every trip now lists all sites it touches, judged from where
        the flown track starts/ends, and near-identical records are merged
        (also across midnight, where a flight shows up under two dates);
      * ground-only "flights" (engine runs) are dropped;
      * generic model labels are upgraded (Beluga callsigns etc.).
    """
    changes = 0
    kept_by_icao = {}  # icao24 -> list of kept trips, for cross-date de-duplication
    for date in sorted(store.get("days", {})):
        entry = store["days"][date]
        kept = []
        for trip in entry.get("trips", []):
            path = trip.get("path") or []
            if len(path) < 2 or path_extent_km(path) < MIN_FLIGHT_EXTENT_KM:
                changes += 1
                continue

            sites = merge_sites(trip.get("sites") or [trip.get("site")],
                                sites_near(path[0][1], path[0][2]),
                                sites_near(path[-1][1], path[-1][2]))
            if sites != trip.get("sites") or sites[0] != trip.get("site"):
                trip["sites"], trip["site"] = sites, sites[0]
                changes += 1

            if trip.get("model") in (None, "", "Aircraft"):
                better = fallback_model(trip)
                if better != trip.get("model"):
                    trip["model"] = better
                    changes += 1
            if not trip.get("purpose"):
                trip["purpose"] = purpose_for(trip)
                changes += 1

            start = path[0][0]
            twin = next((o for o in kept_by_icao.get(trip.get("icao24"), [])
                         if abs(o["path"][0][0] - start) <= DUPLICATE_WINDOW_SECONDS), None)
            if twin is not None:
                twin["sites"] = merge_sites(twin.get("sites"), trip["sites"])
                twin["site"] = twin["sites"][0]
                changes += 1
                continue
            kept_by_icao.setdefault(trip.get("icao24"), []).append(trip)
            kept.append(trip)
        entry["trips"] = kept
    return changes


def load_store(path):
    """Load the central store file, or return an empty one if it doesn't exist yet."""
    if not os.path.exists(path):
        return {"airports": AIRPORTS, "days": {}}
    with open(path) as f:
        store = json.load(f)
    store.setdefault("airports", AIRPORTS)
    store.setdefault("days", {})
    n = normalize_store(store)
    if n:
        print(f"  (store clean-up: {n} change(s) applied while loading {path})", file=sys.stderr)
    return store


def save_store(store, path):
    """Atomic write: a crash mid-write can no longer leave a truncated store."""
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, "w") as f:
        json.dump(store, f, separators=(",", ":"))
    os.replace(tmp, path)


def prune_store(store, keep_days, reference_date=None):
    """Keep only dates within the last `keep_days` days counting back from
    `reference_date` (default: today, UTC). Returns the dropped dates.

    The cut-off is calendar based, so stale dates are dropped even when the
    store holds fewer than `keep_days` entries (e.g. after missed runs).
    """
    if keep_days is None:
        return []
    if reference_date is None:
        ref = datetime.now(timezone.utc).date()
    else:
        ref = datetime.strptime(reference_date, "%Y-%m-%d").date()
    cutoff = ref - timedelta(days=keep_days - 1)
    dropped = [d for d in sorted(store.get("days", {}))
               if datetime.strptime(d, "%Y-%m-%d").date() < cutoff]
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
        if not meta.get("synthetic") and not meta.get("settled", False):
            flags.append("UNSETTLED (will be re-queried by --revisit)")
        flag_str = f"  [{', '.join(flags)}]" if flags else ""
        print(f"  {date}: {n_trips} trip(s){flag_str}")


def revisit(store_path, registry_csv, max_tracks, limit, keep_days):
    """Finish / refresh stored dates that need it, oldest first, at most
    `limit` dates per run (so today's fresh fetch keeps its credits)."""
    store = load_store(store_path)
    today = datetime.now(timezone.utc).date()
    todo = sorted(
        d for d, e in store["days"].items()
        if needs_revisit(e) and (today - datetime.strptime(d, "%Y-%m-%d").date()).days <= 28
    )[:limit]
    if not todo:
        print("Nothing to revisit: every stored date is complete and settled.", file=sys.stderr)
        return
    print(f"Revisiting: {', '.join(todo)}", file=sys.stderr)
    client = make_client()
    registry = load_registry(registry_csv)
    for d in todo:
        trips, meta = fetch_date(client, d, registry, store["days"].get(d), max_tracks)
        store["days"][d] = {"meta": meta, "trips": trips}
        save_store(store, store_path)
        state = ("rate limited" if meta.get("incomplete_rate_limited")
                 else "complete" if meta.get("complete") else "still incomplete")
        print(f"  {d}: {len(trips)} trip(s) [{state}]", file=sys.stderr)
        if meta.get("incomplete_rate_limited"):
            break
    dropped = prune_store(store, keep_days)
    if dropped:
        save_store(store, store_path)


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
    ap.add_argument("--revisit", action="store_true",
                     help="Finish incomplete dates and re-query dates whose airport "
                          "lists were taken before OpenSky's data settled, then exit.")
    ap.add_argument("--revisit-limit", type=int, default=2, metavar="N",
                     help="Max dates to revisit per run (default 2).")
    ap.add_argument("--keep-days", type=int, metavar="N", default=None,
                     help="After saving, drop any stored date older than N days ago (by "
                          "calendar date), keeping the store as a rolling N-day window. "
                          "Omit to keep every date ever fetched (old default behaviour).")
    args = ap.parse_args()

    if args.list:
        list_store(args.output)
        return

    if args.revisit:
        revisit(args.output, args.registry, args.max_tracks, args.revisit_limit, args.keep_days)
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
            f"NOTE: {args.date} is incomplete; run the same command again "
            "to continue from where it stopped.",
            file=sys.stderr,
        )


if __name__ == "__main__":
    main()
