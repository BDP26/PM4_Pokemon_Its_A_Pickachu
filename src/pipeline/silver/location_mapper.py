import re
from typing import Optional


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

    def __init__(self, location_index: dict):
        self.valid_slugs = {item["name"] for item in location_index["results"]}
        self.cache: dict[str, Optional[str]] = {}
        self.misses: list[dict] = []

        self.blacklist = [
            "edit section's source code",
            "file:",
            "badge",
            "pharmacy",
            "center",
            "mart",
            "interior",
            "entrance",
            "style",
            "sushi high roller",
            "house",
            "gate",
            "link trade",
            "day care",
            "hall of fame",
            "cave",
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
        }

    def is_location_title(self, title: str) -> bool:
        title_lower = title.lower()
        return any(keyword in title_lower for keyword in self.LOCATION_KEYWORDS)

    @staticmethod
    def _clean_title(title: str) -> str:
        cleaned = re.sub(r"(?i)edit section's source code:\s*", "", title)
        if "Underground Path" not in cleaned:
            cleaned = re.sub(r"\(.*?\)", "", cleaned)
        return cleaned.strip()

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
            }
        )

    def resolve(self, raw_title: str, route_prefix: str) -> Optional[str]:
        title_lower = raw_title.lower().strip()

        if raw_title in self.cache:
            return self.cache[raw_title]

        if title_lower == "cave":
            self.cache[raw_title] = None
            self._record_miss(
                raw_title=raw_title,
                clean_name=raw_title.strip(),
                route_prefix=route_prefix,
                tried_slug=None,
                reason="generic_title_cave",
            )
            return None

        matched_blacklist = next((bad for bad in self.blacklist if bad in title_lower), None)
        if matched_blacklist:
            self.cache[raw_title] = None
            self._record_miss(
                raw_title=raw_title,
                clean_name=raw_title.strip(),
                route_prefix=route_prefix,
                tried_slug=None,
                reason=f"blacklisted:{matched_blacklist}",
            )
            return None

        clean_name = self._clean_title(raw_title)

        route_match = re.search(r"Route\s+(\d+)", clean_name, re.IGNORECASE)
        if route_match:
            route_number = route_match.group(1)
            if "Kanto" in raw_title:
                slug = f"kanto-route-{route_number}"
            elif "Johto" in raw_title:
                slug = f"johto-route-{route_number}"
            else:
                slug = f"{route_prefix}-{route_number}"
            slug_source = "route_rule"
        else:
            slug = self._slugify(clean_name)
            slug_source = "slugify"

        mapped_slug = self.hard_map.get(slug, slug)
        if mapped_slug != slug:
            slug_source = f"{slug_source}+hard_map"

        if mapped_slug in self.valid_slugs:
            self.cache[raw_title] = mapped_slug
            return mapped_slug

        self.cache[raw_title] = None
        self._record_miss(
            raw_title=raw_title,
            clean_name=clean_name,
            route_prefix=route_prefix,
            tried_slug=mapped_slug,
            reason=f"not_in_valid_slugs:{slug_source}",
        )
        return None