"""
OECD Tax Revenue — Main Pipeline
Outputs: output/OECD_TAXREVENUE_DATA_YYYYMMDD.xlsx
         output/OECD_TAXREVENUE_META_YYYYMMDD.xlsx
         output/OECD_TAXREVENUE_YYYYMMDD.ZIP
Usage  : python main.py
"""

import os
import sys
import zipfile
import subprocess
from datetime import date

import openpyxl
import pandas as pd

import scraper
import mapper

OUTPUT_DIR    = "output"
DOWNLOADS_DIR = "downloads"
OUTPUT_PREFIX = "OECD_TAXREVENUE"


def _datestamp() -> str:
    return date.today().strftime("%Y%m%d")


def _apply_number_format(filepath: str) -> None:
    """Apply comma number format to all numeric cells (data rows only)."""
    wb = openpyxl.load_workbook(filepath)
    ws = wb.active
    for row in ws.iter_rows(min_row=3, min_col=2):
        for cell in row:
            if isinstance(cell.value, (int, float)):
                cell.number_format = "#,##0.##"
    wb.save(filepath)


def _save_data(df: pd.DataFrame, datestamp: str) -> str:
    """Write DATA xlsx and apply number formatting. Returns file path."""
    filename = f"{OUTPUT_PREFIX}_DATA_{datestamp}.xlsx"
    filepath = os.path.join(OUTPUT_DIR, filename)
    df.to_excel(filepath, index=False, header=False)
    _apply_number_format(filepath)
    print(f"[main] DATA saved: {filepath}")
    return filepath


def _save_metadata(datestamp: str) -> str:
    """Write META xlsx. Returns file path."""
    filename = f"{OUTPUT_PREFIX}_META_{datestamp}.xlsx"
    filepath = os.path.join(OUTPUT_DIR, filename)
    meta_rows = mapper.build_metadata_rows()
    pd.DataFrame(meta_rows).to_excel(filepath, index=False)
    print(f"[main] META saved: {filepath}")
    return filepath


def _create_zip(data_path: str, meta_path: str, datestamp: str) -> str:
    """Bundle DATA + META into a ZIP. Returns ZIP path."""
    zip_name = f"{OUTPUT_PREFIX}_{datestamp}.ZIP"
    zip_path = os.path.join(OUTPUT_DIR, zip_name)
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.write(data_path, arcname=os.path.basename(data_path))
        zf.write(meta_path, arcname=os.path.basename(meta_path))
    print(f"[main] ZIP created: {zip_path}")
    return zip_path


def scrape() -> None:
    """Full pipeline: fetch → convert → map → save DATA + META + ZIP."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(DOWNLOADS_DIR, exist_ok=True)

    datestamp = _datestamp()

    print("[main] Step 1: Fetching raw data from OECD...")
    scraper.fetch_data(downloads_dir=DOWNLOADS_DIR)

    print("[main] Step 2: Converting Excel files to CSV...")
    converter = os.path.join(os.path.dirname(__file__), "universal_excel_converter.py")
    result = subprocess.run(
        [sys.executable, converter,
         "--source", DOWNLOADS_DIR,
         "--output", DOWNLOADS_DIR,
         "--verbose"],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        print(f"[main] Converter stderr:\n{result.stderr}")
        raise RuntimeError("universal_excel_converter.py failed")
    print(result.stdout)

    print("[main] Step 3: Mapping to output format...")
    out_df = mapper.map_to_output()
    print(f"[main] Output shape: {out_df.shape}  "
          f"(2 header rows + {out_df.shape[0] - 2} data rows)")

    print("[main] Step 4: Saving files...")
    data_path = _save_data(out_df, datestamp)
    meta_path = _save_metadata(datestamp)
    _create_zip(data_path, meta_path, datestamp)

    print("[main] Done.")


def main() -> None:
    scrape()


if __name__ == "__main__":
    main()
