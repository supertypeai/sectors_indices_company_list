"""IDX index-announcement parsing, shared by the monthly scraper and the local reconcile.

IDX publishes constituents as "Lamp Peng-xxx-BEI POP - <INDEX> - <Month Year> <Mayor|Minor|
Insidental>.xlsx" inside announcement zips. Layout is stable: metadata rows 0-6 (index name,
evaluation type, effective period in column 6), table header row 7, constituents below.

One copy of this module per repo (they deploy independently); keep them identical.
"""
import io
import re
from datetime import date

import pandas as pd

# IDX keeps these counts fixed per period; a mismatch means the layout changed.
# Keyed by the normalised index name: upper case, no spaces (matches idx_company_profile.indices)
EXPECTED_SIZE = {
    "IDX30": 30, "LQ45": 45, "KOMPAS100": 100, "IDXBUMN20": 20, "IDXHIDIV20": 20,
    "IDXG30": 30, "IDXV30": 30, "IDXQ30": 30, "IDXESGL": 30, "SRIKEHATI": 25,
    "SMINFRA18": 18, "JII70": 70, "ECONOMIC30": 30, "IDXVESTA28": 28,
}
INDEX_CODES = frozenset(EXPECTED_SIZE)
# IDX mixes full Indonesian, 3-letter Indonesian and English month names across files.
MONTH_PREFIX = {
    "jan": 1, "feb": 2, "peb": 2, "mar": 3, "apr": 4, "mei": 5, "may": 5, "jun": 6,
    "jul": 7, "agu": 8, "agt": 8, "aug": 8, "sep": 9, "okt": 10, "oct": 10,
    "nov": 11, "des": 12, "dec": 12,
}


def normalise(name: str) -> str:
    """Canonical index code: upper case, alphanumerics only ("IDX BUMN20" -> "IDXBUMN20")."""
    return re.sub(r"[^A-Z0-9]", "", str(name).upper())


def index_field(filename: str) -> str | None:
    """The index an xlsx belongs to: the text between 'POP' and the month/year."""
    match = re.search(r"POP[\s-]*(.*?)[\s-]*([A-Za-z]{3,9})\s*\d{4}", filename)
    if not match or match.group(2)[:3].lower() not in MONTH_PREFIX:
        return None
    return normalise(match.group(1)) or None


def parse_date(text: str) -> date | None:
    """First date in a period text; the year is often written only on the second date."""
    text = str(text)
    match = re.search(r"(\d{1,2})\s+([A-Za-z]{3,9})(?:\s+(\d{4}))?", text)
    if not match or match.group(2)[:3].lower() not in MONTH_PREFIX:
        return None
    year = match.group(3) or re.search(r"(\d{4})", text)
    year = year if isinstance(year, str) else (year.group(1) if year else None)
    if not year:
        return None
    return date(int(year), MONTH_PREFIX[match.group(2)[:3].lower()], int(match.group(1)))


def read_sheet(blob: bytes) -> dict:
    """Constituents and effective period of one announcement xlsx."""
    meta = pd.read_excel(io.BytesIO(blob), header=None)
    period_row = next((i for i in range(10) if "Periode Efektif Kons" in str(meta.iloc[i, 1])), None)
    evaluasi_row = next((i for i in range(10) if str(meta.iloc[i, 1]).strip() == "Evaluasi"), None)
    period = str(meta.iloc[period_row, 6]).strip(" :") if period_row is not None else ""
    evaluasi = str(meta.iloc[evaluasi_row, 6]).strip(" :") if evaluasi_row is not None else ""

    frame = pd.read_excel(io.BytesIO(blob), skiprows=7).iloc[1:, 2:]
    # Footer rows repeat the header; only real constituents carry a numeric free float.
    frame["Rasio Free Float"] = pd.to_numeric(frame["Rasio Free Float"], errors="coerce")
    frame = frame.dropna(subset=["Rasio Free Float"])
    frame = frame[frame["Kode"].notna()]
    symbols = {f"{str(code).strip()}.JK" for code in frame["Kode"]}
    return {"symbols": symbols, "start": parse_date(period), "period": period, "evaluasi": evaluasi}


def read_sheet_path(path) -> dict:
    with open(path, "rb") as handle:
        return read_sheet(handle.read())


def newest_effective(candidates: list[dict], today: date | None = None) -> tuple[dict | None, dict | None]:
    """(currently effective, next upcoming) announcements from parsed candidates."""
    today = today or date.today()
    dated = sorted((c for c in candidates if c.get("start")), key=lambda c: c["start"])
    effective = [c for c in dated if c["start"] <= today]
    later = [c for c in dated if c["start"] > today]
    current = effective[-1] if effective else next((c for c in candidates if not c.get("start")), None)
    return current, (later[0] if later else None)
