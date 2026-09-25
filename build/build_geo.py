#!/usr/bin/env python3
"""
Extract land outlines near each Airbus site from a Natural-Earth-derived
world countries GeoJSON, crop to a bounding box around each site, and
simplify to keep the bundled HTML small.

Includes EVERY country whose geometry overlaps the crop box, not just
France/Germany -- otherwise, at a wide-enough zoom, neighbouring countries
(Spain, Belgium, Netherlands, Denmark, etc.) are simply missing from the
map, which reads as a blank void / makes France and Germany look like
islands. All included countries are rendered identically (no per-country
styling), so this is purely about having correct, complete coastlines and
borders in view -- it doesn't add any political emphasis.

Usage:
    python3 build/build_geo.py /tmp/countries.geojson -o data/basemap.json

Source: datasets/geo-countries (Natural Earth, public domain / ODbL),
https://github.com/datasets/geo-countries
"""
import argparse
import json


def ring_bbox(ring):
    lons = [p[0] for p in ring]
    lats = [p[1] for p in ring]
    return min(lons), min(lats), max(lons), max(lats)


def bbox_overlaps(a, b):
    return not (a[2] < b[0] or b[2] < a[0] or a[3] < b[1] or b[3] < a[1])


def simplify_ring(ring, tolerance_deg):
    """Very small dependency-free Douglas-Peucker simplification."""
    if len(ring) <= 2:
        return ring

    def perp_dist(pt, a, b):
        (x, y), (x1, y1), (x2, y2) = pt, a, b
        dx, dy = x2 - x1, y2 - y1
        if dx == dy == 0:
            return ((x - x1) ** 2 + (y - y1) ** 2) ** 0.5
        t = ((x - x1) * dx + (y - y1) * dy) / (dx * dx + dy * dy)
        t = max(0, min(1, t))
        px, py = x1 + t * dx, y1 + t * dy
        return ((x - px) ** 2 + (y - py) ** 2) ** 0.5

    def dp(points):
        if len(points) <= 2:
            return points
        a, b = points[0], points[-1]
        idx, dmax = -1, 0
        for i in range(1, len(points) - 1):
            d = perp_dist(points[i], a, b)
            if d > dmax:
                idx, dmax = i, d
        if dmax > tolerance_deg:
            left = dp(points[: idx + 1])
            right = dp(points[idx:])
            return left[:-1] + right
        return [a, b]

    return dp(ring)


def crop_ring_to_bbox(ring, pad_box):
    """Keep points inside an expanded bbox; drop rings that never enter it."""
    if not any(pad_box[0] <= p[0] <= pad_box[2] and pad_box[1] <= p[1] <= pad_box[3]
               for p in ring):
        return None
    return ring


def ring_area_deg2(bb):
    """Rough bbox-based 'size' of a ring in square degrees, used only to drop
    tiny slivers (small islands, skerries) that add file size without
    aiding recognition at this zoom level."""
    return (bb[2] - bb[0]) * (bb[3] - bb[1])


def rings_for_bbox(world, pad_box, tolerance_deg, min_area_deg2=0.0015):
    """All simplified rings, from ANY country in `world`, that overlap
    pad_box and are large enough to matter at this zoom (drops tiny
    islands/skerries below min_area_deg2). Returns a list of
    {"country": name, "points": [[lon, lat], ...]} so the frontend can
    stroke each country's own outline distinctly -- otherwise two
    countries that share a land border (not a coastline) render as one
    indistinguishable landmass."""
    rings_out = []
    for feature in world["features"]:
        geom = feature.get("geometry")
        if not geom:
            continue
        name = feature["properties"].get("name", "")
        polys = geom["coordinates"] if geom["type"] == "MultiPolygon" else [geom["coordinates"]]
        for poly in polys:
            if not poly:
                continue
            outer = poly[0]  # ignore holes -- irrelevant at this simplification level
            bb = ring_bbox(outer)
            if not bbox_overlaps(bb, pad_box):
                continue
            if ring_area_deg2(bb) < min_area_deg2:
                continue
            cropped = crop_ring_to_bbox(outer, pad_box)
            if cropped is None:
                continue
            simplified = simplify_ring(cropped, tolerance_deg)
            rings_out.append({
                "country": name,
                "points": [[round(x, 3), round(y, 3)] for x, y in simplified],
            })
    return rings_out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("countries_geojson")
    ap.add_argument("-o", "--output", default="data/basemap.json")
    ap.add_argument("--tolerance", type=float, default=0.01,
                     help="Douglas-Peucker tolerance in degrees")
    args = ap.parse_args()

    with open(args.countries_geojson) as f:
        world = json.load(f)

    sites = {
        "TLS": {"lat": 43.6293, "lon": 1.3638, "pad": 5.0, "country": "France"},
        "XFW": {"lat": 53.5358, "lon": 9.8356, "pad": 5.0, "country": "Germany"},
    }

    out = {}
    for site, s in sites.items():
        pad_box = (
            s["lon"] - s["pad"], s["lat"] - s["pad"],
            s["lon"] + s["pad"], s["lat"] + s["pad"],
        )
        rings_out = rings_for_bbox(world, pad_box, args.tolerance)

        out[site] = {
            "country": s["country"],
            "center": [s["lat"], s["lon"]],
            "rings": rings_out,
        }
        pts = sum(len(r["points"]) for r in rings_out)
        print(f"{site}: {len(rings_out)} ring(s) from all overlapping countries, {pts} points")

    with open(args.output, "w") as f:
        json.dump(out, f, separators=(",", ":"))
    print(f"Wrote {args.output}")


if __name__ == "__main__":
    main()
