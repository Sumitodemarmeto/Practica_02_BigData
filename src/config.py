from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import yaml

from .paths import SOURCES_YAML


@dataclass
class Source:
    id: str
    name: str
    format: str = "json"
    role: str = "indicator"
    transformer: str = "generic_passthrough"
    url: str | None = None
    url_template: str | None = None
    paginated: bool = False
    per_page: int = 1000
    meta: dict[str, Any] = field(default_factory=dict)

    def resolve_url(self, iso2: str = "", iso3: str = "") -> str:
        if self.url_template:
            return self.url_template.format(iso2=iso2, iso3=iso3)
        if self.url:
            return self.url
        raise ValueError(f"Fuente {self.id}: falta url o url_template")


@dataclass
class FocusCountry:
    iso2: str
    iso3: str


@dataclass
class CountryCriterion:
    """Criterio declarativo de 'qué es un país' (desde sources.yaml)."""

    source: str = "countries_profile"
    accept: dict[str, Any] = field(default_factory=dict)
    exclude_codes: list[str] = field(default_factory=list)

    def accept_entity(self, profile_item: dict) -> bool:
        iso3 = profile_item.get("cca3")
        if not iso3:
            return False
        if iso3 in self.exclude_codes:
            return False
        for key, expected in self.accept.items():
            if profile_item.get(key) != expected:
                return False
        return True


@dataclass
class Config:
    sources: list[Source]
    focus_countries: list[FocusCountry] = field(default_factory=list)
    code_equivalences: dict[str, str] = field(default_factory=dict)
    country_criterion: CountryCriterion = field(default_factory=CountryCriterion)


def load_config(path=SOURCES_YAML) -> Config:
    with open(path, encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    return Config(
        sources=[Source(**s) for s in raw.get("sources", [])],
        focus_countries=[FocusCountry(**c) for c in raw.get("focus_countries", [])],
        code_equivalences=raw.get("code_equivalences", {}),
        country_criterion=CountryCriterion(**raw.get("country_criterion", {})),
    )
