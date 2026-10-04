#!/usr/bin/env python3
"""Download and flatten openFDA FAERS reports for selected GLP-1 drugs.

Default scope:
    Semaglutide, liraglutide, dulaglutide, exenatide,
    lixisenatide, and tirzepatide; received 2018-01-01 through 2025-12-31.

Optional but recommended API key:
    export OPENFDA_API_KEY="your_key_here"

Run:
    python glp1_faers_pull.py --output glp1_faers_2018_2025.csv

Save the complete JSON responses as well as the flattened CSV:
    python glp1_faers_pull.py --raw-jsonl glp1_faers_raw.jsonl.gz

Important: FAERS is a spontaneous-reporting system. These data cannot estimate
incidence or establish that a drug caused an event.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Any, Iterable
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


ENDPOINT = "https://api.fda.gov/drug/event.json"
DEFAULT_DRUGS = (
    "SEMAGLUTIDE",
    "LIRAGLUTIDE",
    "DULAGLUTIDE",
    "EXENATIDE",
    "LIXISENATIDE",
    "TIRZEPATIDE",
)
PAGE_SIZE = 1000

SEX = {"0": "Unknown", "1": "Male", "2": "Female"}
REPORTER = {
    "1": "Physician",
    "2": "Pharmacist",
    "3": "Other health professional",
    "4": "Lawyer",
    "5": "Consumer/non-health professional",
}
DRUG_ROLE = {"1": "Suspect", "2": "Concomitant", "3": "Interacting"}

CSV_FIELDS = [
    "ingredient_query",
    "safetyreportid",
    "safetyreportversion",
    "receivedate",
    "receiptdate",
    "occurcountry",
    "primarysourcecountry",
    "reporter_qualification",
    "serious",
    "hospitalized",
    "death",
    "life_threatening",
    "disabling",
    "congenital_anomaly",
    "other_serious",
    "patient_age",
    "patient_age_unit",
    "patient_age_years",
    "patient_sex",
    "patient_weight_kg",
    "matching_medicinal_products",
    "matching_indications",
    "matching_routes",
    "matching_drug_roles",
    "reaction_terms",
    "n_reactions",
    "n_drugs_in_report",
]


def as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


def joined(values: Iterable[Any]) -> str:
    """Return distinct nonempty values joined for convenient CSV storage."""
    cleaned = {str(v).strip() for v in values if v is not None and str(v).strip()}
    return " | ".join(sorted(cleaned))


def flag(report: dict[str, Any], field: str) -> int:
    return int(str(report.get(field, "0")) == "1")


def age_in_years(age: Any, unit: Any) -> float | str:
    """Convert ICH age-unit codes to approximate years; blank if unavailable."""
    try:
        value = float(age)
    except (TypeError, ValueError):
        return ""

    factors = {
        "800": 10.0,          # decade
        "801": 1.0,           # year
        "802": 1.0 / 12.0,    # month
        "803": 7.0 / 365.25,  # week
        "804": 1.0 / 365.25,  # day
        "805": 1.0 / 8766.0,  # hour
        "806": 1.0 / 525960.0 # minute
    }
    factor = factors.get(str(unit))
    return round(value * factor, 4) if factor is not None else ""


def drug_matches_ingredient(drug: dict[str, Any], ingredient: str) -> bool:
    generic_names = as_list(drug.get("openfda", {}).get("generic_name"))
    return any(ingredient.casefold() in str(name).casefold() for name in generic_names)


def flatten_report(report: dict[str, Any], ingredient: str) -> dict[str, Any]:
    patient = report.get("patient") or {}
    drugs = as_list(patient.get("drug"))
    reactions = as_list(patient.get("reaction"))
    matching_drugs = [d for d in drugs if drug_matches_ingredient(d, ingredient)]
    source = report.get("primarysource") or {}

    age = patient.get("patientonsetage")
    age_unit = patient.get("patientonsetageunit")

    return {
        "ingredient_query": ingredient.title(),
        "safetyreportid": report.get("safetyreportid", ""),
        "safetyreportversion": report.get("safetyreportversion", ""),
        "receivedate": report.get("receivedate", ""),
        "receiptdate": report.get("receiptdate", ""),
        "occurcountry": report.get("occurcountry", ""),
        "primarysourcecountry": report.get("primarysourcecountry", ""),
        "reporter_qualification": REPORTER.get(
            str(source.get("qualification", "")), source.get("qualification", "")
        ),
        "serious": flag(report, "serious"),
        "hospitalized": flag(report, "seriousnesshospitalization"),
        "death": flag(report, "seriousnessdeath"),
        "life_threatening": flag(report, "seriousnesslifethreatening"),
        "disabling": flag(report, "seriousnessdisabling"),
        "congenital_anomaly": flag(report, "seriousnesscongenitalanomali"),
        "other_serious": flag(report, "seriousnessother"),
        "patient_age": age if age is not None else "",
        "patient_age_unit": age_unit if age_unit is not None else "",
        "patient_age_years": age_in_years(age, age_unit),
        "patient_sex": SEX.get(str(patient.get("patientsex", "")), "Unknown"),
        "patient_weight_kg": patient.get("patientweight", ""),
        "matching_medicinal_products": joined(
            d.get("medicinalproduct") for d in matching_drugs
        ),
        "matching_indications": joined(
            d.get("drugindication") for d in matching_drugs
        ),
        "matching_routes": joined(d.get("drugadministrationroute") for d in matching_drugs),
        "matching_drug_roles": joined(
            DRUG_ROLE.get(str(d.get("drugcharacterization", "")), d.get("drugcharacterization", ""))
            for d in matching_drugs
        ),
        "reaction_terms": joined(r.get("reactionmeddrapt") for r in reactions),
        "n_reactions": len(reactions),
        "n_drugs_in_report": len(drugs),
    }


def api_get(
    url: str,
    params: dict[str, Any] | None,
    attempts: int = 7,
) -> tuple[int, Any, dict[str, Any] | None]:
    """GET with simple exponential retry for throttling and server errors."""
    request_url = url
    if params:
        separator = "&" if "?" in request_url else "?"
        request_url = f"{request_url}{separator}{urlencode(params)}"

    for attempt in range(attempts):
        try:
            request = Request(request_url, headers={"User-Agent": "GLP1-class-project/1.0"})
            with urlopen(request, timeout=90) as response:
                payload = json.loads(response.read().decode("utf-8"))
                return response.status, response.headers, payload
        except HTTPError as error:
            if error.code == 404:
                # openFDA uses 404 when a valid query has no matches.
                return 404, error.headers, None
            if error.code not in {429, 500, 502, 503, 504}:
                detail = error.read().decode("utf-8", errors="replace")
                raise RuntimeError(f"openFDA returned HTTP {error.code}: {detail}") from error
            status = error.code
            headers = error.headers
        except URLError as error:
            status = "network error"
            headers = {}
            if attempt == attempts - 1:
                raise RuntimeError(f"Could not reach openFDA: {error}") from error

        if attempt == attempts - 1:
            raise RuntimeError(f"openFDA request failed after {attempts} attempts ({status})")
        wait_seconds = int(headers.get("Retry-After", 0) or 0)
        wait_seconds = max(wait_seconds, min(2 ** attempt, 60))
        print(f"  Retry in {wait_seconds}s ({status})", file=sys.stderr)
        time.sleep(wait_seconds)
    raise RuntimeError("API request failed after retries")


def next_link(headers: Any) -> str | None:
    """Extract the rel=next URL from the HTTP Link header."""
    link = headers.get("Link", "")
    match = re.search(r'<([^>]+)>;\s*rel=["\']?next["\']?', link, flags=re.IGNORECASE)
    return match.group(1) if match else None


def fetch_partition(
    ingredient: str,
    year: int,
    api_key: str | None,
    raw_file: Any | None,
) -> Iterable[dict[str, Any]]:
    """Yield every report using openFDA search-after pagination."""
    # Do not use `.exact` here: openFDA may store an ingredient inside a longer
    # generic-name value (for example, combination products or name variants).
    search = (
        f'patient.drug.openfda.generic_name:"{ingredient}" '
        f'AND receivedate:[{year}0101 TO {year}1231]'
    )
    params: dict[str, Any] | None = {
        "search": search,
        "limit": PAGE_SIZE,
        "sort": "receivedate:asc",
    }
    if api_key:
        params["api_key"] = api_key

    next_url = ENDPOINT
    page = 0
    while next_url:
        status, headers, payload = api_get(next_url, params)
        if status == 404:
            return

        assert payload is not None
        results = payload.get("results", [])
        page += 1
        total = payload.get("meta", {}).get("results", {}).get("total", "?")
        print(
            f"  {ingredient.title()} {year}: page {page}, "
            f"{len(results)} rows (partition total {total})",
            file=sys.stderr,
        )

        for report in results:
            if raw_file is not None:
                raw_file.write(
                    json.dumps(
                        {"ingredient_query": ingredient.title(), "report": report},
                        separators=(",", ":"),
                    )
                    + "\n"
                )
            yield report

        next_url = next_link(headers)
        # openFDA's rel=next URL contains the search-after cursor but may omit
        # api_key. Reattach only the key; all other parameters are in next_url.
        params = {"api_key": api_key} if next_url and api_key else None


def version_number(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return -1


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Pull 2018-2025 GLP-1 adverse-event reports from openFDA."
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("glp1_faers_2018_2025.csv"),
        help="Flattened, deduplicated CSV output path.",
    )
    parser.add_argument("--start-year", type=int, default=2018)
    parser.add_argument("--end-year", type=int, default=2025)
    parser.add_argument(
        "--drugs",
        nargs="+",
        default=list(DEFAULT_DRUGS),
        help="Generic ingredient names separated by spaces.",
    )
    parser.add_argument(
        "--raw-jsonl",
        type=Path,
        help="Optional .jsonl or .jsonl.gz output containing complete API reports.",
    )
    return parser.parse_args()


def open_raw(path: Path | None):
    if path is None:
        return None
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.suffix == ".gz":
        return gzip.open(path, "wt", encoding="utf-8")
    return path.open("w", encoding="utf-8")


def main() -> None:
    args = parse_args()
    if args.start_year > args.end_year:
        raise SystemExit("--start-year must be less than or equal to --end-year")

    api_key = "xnX9bsDCmLkn00qJBJdlSqcCJVPQsYwXENIX9sKr"
    if not api_key:
        print(
            "OPENFDA_API_KEY is not set. The pull can run without it, but the daily "
            "request allowance is much lower.",
            file=sys.stderr,
        )

    # Keyed by drug and safety report ID so a case involving two requested drugs
    # remains one row for each drug. For follow-ups, retain the newest version.
    newest: dict[tuple[str, str], dict[str, Any]] = {}
    raw_file = open_raw(args.raw_jsonl)
    try:
        for ingredient_value in args.drugs:
            ingredient = ingredient_value.strip().upper()
            for year in range(args.start_year, args.end_year + 1):
                for report in fetch_partition(ingredient, year, api_key, raw_file):
                    row = flatten_report(report, ingredient)
                    report_id = str(row["safetyreportid"])
                    key = (ingredient, report_id)
                    old = newest.get(key)
                    if old is None or version_number(row["safetyreportversion"]) > version_number(
                        old["safetyreportversion"]
                    ):
                        newest[key] = row
    finally:
        if raw_file is not None:
            raw_file.close()

    args.output.parent.mkdir(parents=True, exist_ok=True)
    rows = sorted(
        newest.values(),
        key=lambda row: (
            str(row["ingredient_query"]),
            str(row["receivedate"]),
            str(row["safetyreportid"]),
        ),
    )
    with args.output.open("w", newline="", encoding="utf-8") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Saved {len(rows):,} deduplicated rows to {args.output}")
    requested = {drug.strip().title() for drug in args.drugs}
    returned = {str(row["ingredient_query"]) for row in rows}
    missing = sorted(requested - returned)
    if missing:
        print(
            "WARNING: no reports were returned for: " + ", ".join(missing),
            file=sys.stderr,
        )
    if args.raw_jsonl:
        print(f"Saved complete API records to {args.raw_jsonl}")


if __name__ == "__main__":
    main()
