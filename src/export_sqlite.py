
import argparse
from pathlib import Path
import sqlite3
import sys

try:
    import pandas as pd
except Exception:
    print("pandas required: pip install pandas")
    raise

def csvs_to_sqlite(readable_dir: Path, out_db: Path, if_exists="replace"):
    csvs = sorted(readable_dir.glob("*.csv"))
    if not csvs:
        print("No CSVs found in", readable_dir)
        return False
    out_db.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(out_db))
    try:
        for f in csvs:
            try:
                df = pd.read_csv(f)
            except Exception as e:
                print(f"Skipping {f.name}: read failed: {e}")
                continue
            table_name = f.stem.replace("-", "_").replace(" ", "_").lower()
            df.to_sql(table_name, conn, if_exists=if_exists, index=False)
            print(f"Wrote table `{table_name}` ({len(df)} rows) to {out_db.name}")

            cur = conn.cursor()
            for idcol in ("presc_id", "drug_id", "id"):
                if idcol in df.columns:
                    try:
                        cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_{idcol} ON {table_name}({idcol});")
                        print(f"Created index idx_{table_name}_{idcol}")
                    except Exception as e:
                        print("Index creation failed:", e)
            conn.commit()
    finally:
        conn.close()
    return True

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", default="local_data/artifacts/readable_reports")
    ap.add_argument("--out", default="local_data/artifacts/data.db")
    args = ap.parse_args()
    success = csvs_to_sqlite(Path(args.dir), Path(args.out))
    sys.exit(0 if success else 2)
