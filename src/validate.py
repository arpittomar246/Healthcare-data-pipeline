#!/usr/bin/env python3
"""
Simple validation script for readable reports.

Usage:
    python -m src.validate local_data/artifacts/readable_reports [--out local_data/artifacts/validation_report.json]

Writes a JSON validation report summarizing:
 - file presence
 - row counts
 - missing values per column
 - duplicate key checks (if id columns exist)
"""

import json
import sys
from pathlib import Path
import argparse

try:
    import pandas as pd
except Exception as e:
    print("pandas is required: pip install pandas")
    raise

def inspect_file(p: Path):
    try:
        df = pd.read_csv(p)
    except Exception as e:
        return {"ok": False, "error": f"Could not read CSV: {e}"}
    summary = {
        "ok": True,
        "rows": int(df.shape[0]),
        "columns": list(df.columns),
        "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()},
        "missing_counts": {col: int(df[col].isna().sum()) for col in df.columns},
        "sample_head": df.head(3).to_dict(orient="records"),
    }
    # basic duplicate key checks for typical id columns
    for possible_id in ("presc_id", "drug_id", "id"):
        if possible_id in df.columns:
            dup = int(df.duplicated(subset=[possible_id]).sum())
            summary["duplicates_on_" + possible_id] = dup
    return summary

def main(readable_dir: Path, out_file: Path):
    out = {"readable_reports_dir": str(readable_dir.resolve()), "files": {}, "ok": True}
    if not readable_dir.exists():
        out["ok"] = False
        out["error"] = f"Directory {readable_dir} not found"
    else:
        csvs = sorted(readable_dir.glob("*.csv"))
        if not csvs:
            out["ok"] = False
            out["warning"] = "No CSV files found in readable_reports"
        for f in csvs:
            out["files"][f.name] = inspect_file(f)
            if not out["files"][f.name].get("ok", False):
                out["ok"] = False

    out_file.parent.mkdir(parents=True, exist_ok=True)
    out_file.write_text(json.dumps(out, indent=2), encoding="utf8")
    print(f"Wrote validation report to {out_file}")
    return 0 if out["ok"] else 2

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("readable_dir", nargs="?", default="local_data/artifacts/readable_reports")
    ap.add_argument("--out", default="local_data/artifacts/validation_report.json")
    args = ap.parse_args()
    code = main(Path(args.readable_dir), Path(args.out))
    sys.exit(code)
