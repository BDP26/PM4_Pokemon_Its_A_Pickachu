from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Optional


@dataclass(frozen=True)
class LocationResolution:
    kind: str  # location | location-area | parent_fallback | unmapped
    slug: str | None
    parent_slug: str | None = None
    source: str = ""
    reason: str = ""


class LocationMapper:
    LOCATION_KEYWORDS = [
        "route",
        "city",
        "cave",
        "forest",
        "mt.",
        "mountain",
        "road",
        "tunnel",
        "island",
        "tower",
        "sea route",
        "victory road",
        "safari zone",
    ]

    def __init__(self, location_index: dict[str, Any]):
        results = location_index.get("results", [])
        self.valid_location_slugs = {
            str(item.get("name") or "").strip()
            for item in results
            if isinstance(item, dict) and str(item.get("name") or "").strip()
        }
        area_results = location_index.get("location_area_results", [])
        self.valid_location_area_slugs = {
            str(item.get("name") or "").strip()
            for item in area_results
            if isinstance(item, dict) and str(item.get("name") or "").strip()
        }
        parent_map = location_index.get("location_area_parent_map", {})
        self.location_area_to_parent = {
            str(area).strip(): str(parent).strip()
            for area, parent in parent_map.items()
            if str(area).strip() and str(parent).strip()
        }
        # Backward-compatible, keeps old callers working.
        self.valid_slugs = set(self.valid_location_slugs)
        self.cache: dict[tuple[str, str], Optional[str]] = {}
        self.resolution_cache: dict[tuple[str, str], LocationResolution] = {}
        self.misses: list[dict] = []

        self.blacklist = [
            "edit section's source code",
            "file:",
            "badge",
            "pharmacy",
            "mart",
            "style",
            "sushi high roller",
            "house",
            "link trade",
            "day care",
            "hall of fame",
            "tower tycoon",
        ]

        self.hard_map = {
            "underground-path-kanto-routes-5-6": "kanto-route-5",
            "underground-path-kanto-routes-7-8": "kanto-route-7",
            "sinnoh-route-220": "sinnoh-sea-route-220",
            "sinnoh-route-223": "sinnoh-sea-route-223",
            "sinnoh-route-226": "sinnoh-sea-route-226",
            "sinnoh-route-230": "sinnoh-sea-route-230",
            "kanto-route-19": "kanto-sea-route-19",
            "kanto-route-20": "kanto-sea-route-20",
            "kanto-route-21": "kanto-sea-route-21",
            "johto-route-40": "johto-sea-route-40",
            "johto-route-41": "johto-sea-route-41",
            "digletts-cave": "digletts-cave",
            "challengers-cave": "challengers-cave",
            "mt-moon-square": "mt-moon",
            "ilex-forest-shrine": "ilex-forest",
            "victory-road": "kanto-victory-road-1",
            "tin-tower": "bell-tower",
            "kanto-radio-tower": "radio-tower",
        }

    def is_location_title(self, title: str) -> bool:
        title_lower = title.lower()
        return any(keyword in title_lower for keyword in self.LOCATION_KEYWORDS)

    @staticmethod
    def _clean_title(title: str) -> str:
        cleaned = re.sub(r"(?i)edit section's source code:\s*", "", title)
        if "Underground Path" not in cleaned:
            cleaned = re.sub(r"\(.*?\)", "", cleaned)
        cleaned = re.sub(r"(?i)^(back to|return to|outside|inside|via(?: the)?)\s+", "", cleaned).strip()
        cleaned = re.sub(r"(?i),\s*second visit$", "", cleaned).strip()
        cleaned = re.sub(r"(?i)\s+(entrance|interior)$", "", cleaned).strip()

        # Prefer a deterministic canonical side when a heading references multiple places.
        if "/" in cleaned:
            cleaned = cleaned.split("/", 1)[0].strip()

        if cleaned.lower().startswith("gate to "):
            cleaned = cleaned[8:].strip()
        return cleaned.strip()

    @staticmethod
    def _special_slug(clean_name: str, raw_title: str, route_prefix: str) -> Optional[str]:
        clean_lower = clean_name.lower()
        region = route_prefix.split("-", 1)[0]

        if clean_lower == "safari zone":
            return f"{region}-safari-zone"

        if clean_lower == "goldenrod radio tower":
            return "radio-tower"

        if clean_lower == "seaside cycling road" and route_prefix == "hoenn-route":
            return "hoenn-route-110"

        title_for_match = raw_title.lower().replace("é", "e")
        if clean_lower == "ghost" and "pokemon tower" in title_for_match:
            return "pokemon-tower"

        return None

    @staticmethod
    def _slugify(text: str) -> str:
        slug = text.lower().replace("\u00e9", "e").replace("\u2019", "").replace("'", "")
        slug = re.sub(r"[^a-z0-9]+", "-", slug)
        return slug.strip("-")

    def _record_miss(
        self,
        raw_title: str,
        clean_name: str,
        route_prefix: str,
        tried_slug: Optional[str],
        reason: str,
    ) -> None:
        self.misses.append(
            {
                "raw_title": raw_title,
                "clean_name": clean_name,
                "route_prefix": route_prefix,
                "tried_slug": tried_slug,
                "reason": reason,
                "resolver": "location_mapper_v2",
            }
        )

    def _record_resolution(
        self,
        raw_title: str,
        clean_name: str,
        route_prefix: str,
        resolution: LocationResolution,
    ) -> None:
        if resolution.kind != "unmapped":
            return
        self._record_miss(
            raw_title=raw_title,
            clean_name=clean_name,
            route_prefix=route_prefix,
            tried_slug=resolution.slug,
            reason=resolution.reason or "unmapped",
        )

    def _resolve_candidate(self, mapped_slug: str, slug_source: str) -> LocationResolution:
        if mapped_slug in self.valid_location_slugs:
            return LocationResolution(kind="location", slug=mapped_slug, source=slug_source)

        if mapped_slug in self.valid_location_area_slugs:
            parent = self.location_area_to_parent.get(mapped_slug)
            if parent:
                return LocationResolution(
                    kind="parent_fallback",
                    slug=parent,
                    parent_slug=parent,
                    source=f"{slug_source}:area_parent",
                    reason="resolved_location_area_with_parent",
                )
            return LocationResolution(kind="location-area", slug=mapped_slug, source=f"{slug_source}:area")

        parent_from_map = self.location_area_to_parent.get(mapped_slug)
        if parent_from_map:
            return LocationResolution(
                kind="parent_fallback",
                slug=parent_from_map,
                parent_slug=parent_from_map,
                source=f"{slug_source}:parent_map",
                reason="resolved_parent_from_area_map",
            )

        return LocationResolution(
            kind="unmapped",
            slug=mapped_slug,
            source=slug_source,
            reason=f"not_in_known_slugs:{slug_source}",
        )

    def resolve_with_kind(self, raw_title: str, route_prefix: str) -> LocationResolution:
        cache_key = (raw_title, route_prefix)
        if cache_key in self.resolution_cache:
            return self.resolution_cache[cache_key]

        clean_name = self._clean_title(raw_title)
        title_lower = clean_name.lower().strip()

        if title_lower == "cave":
            resolution = LocationResolution(kind="unmapped", slug=None, reason="generic_title_cave")
            self.resolution_cache[cache_key] = resolution
            self.cache[cache_key] = None
            self._record_resolution(raw_title, clean_name, route_prefix, resolution)
            return resolution

        matched_blacklist = next((bad for bad in self.blacklist if bad in title_lower), None)
        if matched_blacklist:
            resolution = LocationResolution(kind="unmapped", slug=None, reason=f"blacklisted:{matched_blacklist}")
            self.resolution_cache[cache_key] = resolution
            self.cache[cache_key] = None
            self._record_resolution(raw_title, clean_name, route_prefix, resolution)
            return resolution

        route_match = re.search(r"Routes?\s+(\d+)", clean_name, re.IGNORECASE)
        if route_match:
            route_number = route_match.group(1)
            if "Kanto" in raw_title:
                slug = f"kanto-route-{route_number}"
            elif "Johto" in raw_title:
                slug = f"johto-route-{route_number}"
            elif route_prefix == "johto-route" and int(route_number) <= 28:
                # Gold/Silver late-game routes in this range are Kanto routes in PokeAPI.
                slug = f"kanto-route-{route_number}"
            else:
                slug = f"{route_prefix}-{route_number}"
            slug_source = "route_rule"
        else:
            special_slug = self._special_slug(clean_name, raw_title, route_prefix)
            if special_slug is not None:
                slug = special_slug
                slug_source = "special_rule"
            else:
                slug = self._slugify(clean_name)
                slug_source = "slugify"

        mapped_slug = self.hard_map.get(slug, slug)
        if mapped_slug != slug:
            slug_source = f"{slug_source}+hard_map"

        resolution = self._resolve_candidate(mapped_slug, slug_source=slug_source)
        self.resolution_cache[cache_key] = resolution
        self.cache[cache_key] = resolution.slug if resolution.kind != "unmapped" else None
        self._record_resolution(raw_title, clean_name, route_prefix, resolution)
        return resolution

    def resolve(self, raw_title: str, route_prefix: str) -> Optional[str]:
        return self.resolve_with_kind(raw_title, route_prefix).slug
