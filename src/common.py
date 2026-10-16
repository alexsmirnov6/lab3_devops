"""Shared rules for the DevOps vacancy analysis."""

from __future__ import annotations

import math
import os
import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path


DEVOPS_TITLE_RE = re.compile(
    r"(?:\bdev[\s-]*(?:sec[\s-]*)?ops\b|девопс|\bsite reliability\b|\bsre\b|\bplatform engineer\b)",
    re.IGNORECASE,
)


def is_devops_title(title: str | None) -> bool:
    """Return True only for titles that explicitly identify DevOps/SRE work."""
    return bool(DEVOPS_TITLE_RE.search(title or ""))


def skill_key(value: str) -> str:
    """Create a stable, case-insensitive key without changing semantics."""
    value = unicodedata.normalize("NFKC", value)
    value = re.sub(r"\s+", " ", value).strip(" ;,\t\r\n")
    return value.casefold()


def parse_skills(value: str | None) -> list[tuple[str, str]]:
    """Return unique (normalized key, original display label) pairs."""
    found: dict[str, str] = {}
    for raw in (value or "").split(";"):
        label = unicodedata.normalize("NFKC", raw)
        label = re.sub(r"\s+", " ", label).strip(" ;,\t\r\n")
        key = skill_key(label)
        if key and key not in found:
            found[key] = label
    return sorted(found.items())


def optional_float(value: str | None) -> float | None:
    if value is None or not value.strip():
        return None
    try:
        number = float(value)
    except ValueError:
        return None
    return number if math.isfinite(number) and number > 0 else None


def parse_bool(value: str | None) -> bool | None:
    if value is None:
        return None
    normalized = value.strip().lower()
    if normalized == "true":
        return True
    if normalized == "false":
        return False
    return None


def salary_midpoint_rur_gross(row: dict[str, str]) -> float | None:
    """Estimate monthly gross RUR salary using disclosed bounds only.

    Foreign currencies are excluded. A one-sided range uses its disclosed
    bound. Net salaries are converted to gross with the documented 13% NDFL
    assumption, which is appropriate for comparison rather than payroll.
    """
    if (row.get("currency") or "").upper() != "RUR":
        return None
    lower = optional_float(row.get("salary_from"))
    upper = optional_float(row.get("salary_to"))
    if lower is None and upper is None:
        return None
    midpoint = (lower + upper) / 2 if lower is not None and upper is not None else (lower or upper)
    gross = parse_bool(row.get("gross"))
    if gross is False:
        midpoint /= 0.87
    return round(midpoint, 2)


@dataclass(frozen=True)
class S3Config:
    endpoint: str
    access_key: str
    secret_key: str
    bucket: str = "hh-lake"

    @classmethod
    def from_env(cls) -> "S3Config":
        missing = [
            name
            for name in ("RUSTFS_ACCESS_KEY", "RUSTFS_SECRET_KEY")
            if not os.environ.get(name)
        ]
        if missing:
            raise RuntimeError("Missing environment variables: " + ", ".join(missing))
        return cls(
            endpoint=os.environ.get("RUSTFS_ENDPOINT", "http://127.0.0.1:9000"),
            access_key=os.environ["RUSTFS_ACCESS_KEY"],
            secret_key=os.environ["RUSTFS_SECRET_KEY"],
            bucket=os.environ.get("RUSTFS_BUCKET", "hh-lake"),
        )


def project_root() -> Path:
    return Path(__file__).resolve().parents[1]

