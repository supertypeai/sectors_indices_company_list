"""Runnable check for the local data_automation.py changes. No DB, no network.

    ~/miniconda3/envs/idxprobe/bin/python check_data_automation.py

Covers the three behaviours that were broken or newly added:
  1. the IDXVESTA28 sheet name actually matches the index list (it never did: INFOVESTA28)
  2. a shrunken parse is rejected instead of written
  3. the reconcile removes stale memberships and keeps codes outside this run untouched
"""
import glob
import os
import pathlib
import shutil
import tempfile
import zipfile

import pandas as pd

import data_automation as da
import indices_zip_data_scraper as scraper
from idx_announcement import index_field, normalise, read_sheet

ZIP_DIR = "../_idx_probe_indices/zips"


class _FakeResponse:
    def __init__(self, data):
        self.data = data


class _FakeClient:
    """Mimics just enough of supabase-py to exercise push_to_supabase."""

    def __init__(self, rows):
        self.rows = rows
        self.updates = {}

    def table(self, _name):
        return self

    def select(self, *_args):
        return self

    def update(self, values):
        self._values = values
        return self

    def eq(self, _column, value):
        self.updates[value] = self._values
        for row in self.rows:
            if row["symbol"] == value:
                row["indices"] = self._values["indices"]
        return self

    def execute(self):
        return _FakeResponse(self.rows)


def sheet(index, zip_name):
    with zipfile.ZipFile(f"{ZIP_DIR}/{zip_name}") as archive:
        member = [n for n in archive.namelist() if f"{index} -" in n and n.endswith(".xlsx")][0]
        path = f"/tmp/{index.replace(' ', '_')}.xlsx"
        with open(path, "wb") as handle:
            handle.write(archive.read(member))
    return path


# 1. index names line up with what IDX publishes
listing = [
    "IDX30", "LQ45", "KOMPAS100", "IDX BUMN20", "IDX HIDIV20", "IDX G30", "IDX V30",
    "IDX Q30", "IDX ESGL", "SRIKEHATI", "SMinfra18", "JII70", "ECONOMIC30", "IDXVESTA28",
]
for index in listing:
    normalised = index.replace(" ", "").upper()
    assert normalised in da.EXPECTED_SIZE, f"{index} has no expected size"
assert "INFOVESTA28" not in da.EXPECTED_SIZE

# 2. a real sheet parses to the guaranteed size, and a wrong expectation is rejected
path = sheet("IDXVESTA28", "aug2026_peng00160.zip")
companies = pd.read_excel(path, skiprows=7).iloc[1:, 2:]
companies["Rasio Free Float"] = pd.to_numeric(companies["Rasio Free Float"], errors="coerce")
companies = companies.dropna(subset=["Rasio Free Float"])
symbols = sorted({f"{str(code).strip()}.JK" for code in companies["Kode"]})
profile = pd.DataFrame({"symbol": symbols, "company_name": symbols})

assert len(da.company_list_extraction(path, profile, 28)) == 28
for wrong in (27, 29):
    try:
        da.company_list_extraction(path, profile, wrong)
    except ValueError:
        pass
    else:
        raise AssertionError(f"expected a ValueError for expected_count={wrong}")

# 3. reconcile: drop stale members, add new ones, leave unmanaged codes alone
rows = [
    {"symbol": "STAY.JK", "indices": ["IDXV30", "FTSE"]},
    {"symbol": "LEAVE.JK", "indices": ["IDXV30", "JII70"]},
    {"symbol": "JOIN.JK", "indices": ["JII70"]},
]
saved_sizes = da.EXPECTED_SIZE
da.EXPECTED_SIZE = {"IDXV30": 2}
fake = _FakeClient(rows)
da.SUPABASE_CLIENT = fake
da.push_to_supabase({"STAY.JK": ["IDXV30"], "JOIN.JK": ["IDXV30", "JII70"]}, {"IDXV30"})

assert "STAY.JK" not in fake.updates, "unchanged row must not be rewritten"
assert fake.updates["LEAVE.JK"]["indices"] == ["JII70"], fake.updates["LEAVE.JK"]
assert fake.updates["JOIN.JK"]["indices"] == ["IDXV30", "JII70"], fake.updates["JOIN.JK"]
da.EXPECTED_SIZE = saved_sizes

# 4. sheet -> index matching must be exact: the Low Carbon sheet name contains "LQ45"
assert normalise("IDX BUMN20") == "IDXBUMN20"
assert normalise("SMinfra18") == "SMINFRA18"
lcl = "8 Lamp Peng-00207 BEI POP - IDX LQ45 Low Carbon Leader - Okt 2026 Minor-ID.xlsx"
assert index_field(lcl) == "IDXLQ45LOWCARBONLEADER", index_field(lcl)
assert index_field(lcl) not in da.EXPECTED_SIZE, "Low Carbon must not count as LQ45"
assert index_field("7 Lamp Peng-00204-BEI POP - SMinfra18 - Okt 2026 Mayor - ID.xlsx") == "SMINFRA18"
assert index_field("1 Lamp Peng-00228-BEI POP- IDXVESTA28 - November 2026 Minor-.xlsx") == "IDXVESTA28"

# 5. end-to-end over the real announcement zips: every index parses, and a later
#    insidental announcement beats the Mayor file with the same index name
work = pathlib.Path(tempfile.mkdtemp())
da.SOURCE_DATA_DIR = work / "source_data"
da.EXTRACTED_DATA_DIR = da.SOURCE_DATA_DIR / "extracted_data"
da.COMPANY_LIST_DIR = work / "company_list"
da.CHECK_DF_PATH = work / "check_df.csv"
da.SOURCE_DATA_DIR.mkdir(parents=True)
for zip_path in glob.glob(f"{ZIP_DIR}/*.zip"):
    shutil.copy(zip_path, da.SOURCE_DATA_DIR)

universes = {}
for zip_path in glob.glob(f"{ZIP_DIR}/*.zip"):
    with zipfile.ZipFile(zip_path) as archive:
        for member in archive.namelist():
            if member.endswith(".xlsx"):
                universes.update({s: "x" for s in read_sheet(archive.read(member))["symbols"]})
da.IDX_DATA = pd.DataFrame({"symbol": sorted(universes), "company_name": "x"})

parsed = da.run_indices_update()
assert parsed == set(da.EXPECTED_SIZE), parsed

lq45 = {row["symbol"] for row in pd.read_csv(da.COMPANY_LIST_DIR / "companies_list_lq45.csv").to_dict("records")}
assert {"ELSA.JK", "ERAA.JK", "GGRM.JK", "PWON.JK", "SMGR.JK"} <= lq45, "LQ45 must follow the 18 Nov insidental file"
assert not {"CUAN.JK", "MAPI.JK", "NCKL.JK", "PGEO.JK", "SCMA.JK"} & lq45
shutil.rmtree(work)

# a symbol that leaves every index must lose its last code (the phantom-row case)
da.EXPECTED_SIZE = {"IDXV30": 2}
rows = [
    {"symbol": "STAY.JK", "indices": ["IDXV30"]},
    {"symbol": "JOIN.JK", "indices": ["IDXV30"]},
    {"symbol": "PHANTOM.JK", "indices": ["IDXV30"]},
]
fake = _FakeClient(rows)
da.SUPABASE_CLIENT = fake
da.push_to_supabase({"STAY.JK": ["IDXV30"], "JOIN.JK": ["IDXV30"]}, {"IDXV30"})
assert fake.updates["PHANTOM.JK"]["indices"] == [], fake.updates["PHANTOM.JK"]

# idempotent: a second pass over the same CSV state writes nothing
fake.updates.clear()
da.push_to_supabase({"STAY.JK": ["IDXV30"], "JOIN.JK": ["IDXV30"]}, {"IDXV30"})
assert fake.updates == {}, f"second pass must be a no-op, wrote {fake.updates}"
da.EXPECTED_SIZE = saved_sizes

# the count invariant must refuse the push instead of deleting members
da.EXPECTED_SIZE = {"IDXV30": 99}
try:
    da.push_to_supabase({"STAY.JK": ["IDXV30"]}, {"IDXV30"})
except SystemExit:
    pass
else:
    raise AssertionError("expected the reconcile to refuse a wrong member count")
finally:
    da.EXPECTED_SIZE = saved_sizes

# 5b. the profile loader must return a DataFrame, not the raw PostgREST response
assert list(da.load_profile(None).columns) == ["symbol", "company_name"]
assert da.load_profile(_FakeClient([{"symbol": "A.JK", "company_name": "x"}])).shape == (1, 2)

# 6. a blocked listing (403 from the JSON endpoint) must fail the run, not skip silently
original_fetch = scraper.ProxyRequester.fetch_url
scraper.ProxyRequester.fetch_url = lambda self, url: False
here = os.getcwd()
os.chdir(tempfile.mkdtemp())
try:
    try:
        scraper.get_zip_files()
    except SystemExit as exit_signal:
        assert exit_signal.code == 1, f"expected exit 1, got {exit_signal.code}"
    else:
        raise AssertionError("blocked listing must stop the scraper")
finally:
    os.chdir(here)
    scraper.ProxyRequester.fetch_url = original_fetch

# 7. a partial parse must stop the run instead of pushing the indices that did parse
work = pathlib.Path(tempfile.mkdtemp())
da.SOURCE_DATA_DIR = work / "source_data"
da.EXTRACTED_DATA_DIR = da.SOURCE_DATA_DIR / "extracted_data"
da.COMPANY_LIST_DIR = work / "company_list"
da.CHECK_DF_PATH = work / "check_df.csv"
da.SOURCE_DATA_DIR.mkdir(parents=True)
shutil.copy(f"{ZIP_DIR}/nov2026_peng00228.zip", da.SOURCE_DATA_DIR)  # covers only 3 tracked indices
da.IDX_DATA = pd.DataFrame({"symbol": sorted(universes), "company_name": "x"})
try:
    da.run_indices_update()
except SystemExit as exit_signal:
    assert "indices failed" in str(exit_signal) or exit_signal.code, exit_signal
else:
    raise AssertionError("a partial parse must stop the run")
shutil.rmtree(work)

print("check_data_automation: PASS (14 index names, size guard, reconcile add/remove/keep, "
      "exact sheet->index matching, count invariant, end-to-end parse picks the newest "
      "effective announcement)")
