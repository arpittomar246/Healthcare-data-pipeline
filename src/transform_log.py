
from __future__ import annotations
import datetime
import json
from pathlib import Path
import sys
from typing import Optional

try:
    import pandas as pd  
except Exception:
    pd = None 

ARTIFACTS = Path.cwd() / "local_data" / "artifacts"
ARTIFACTS.mkdir(parents=True, exist_ok=True)
LOG_FILE = ARTIFACTS / "transform_log.txt"

def _now_ts() -> str:
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

class TransformLogger:
    def __init__(self, path: Optional[Path] = None):
        self.path = Path(path) if path is not None else LOG_FILE

        self.path.parent.mkdir(parents=True, exist_ok=True)

    def _append(self, text: str) -> None:
        try:
            with self.path.open("a", encoding="utf8") as f:
                f.write(text + "\n")
        except Exception as e:

            print(f"[transform_log] FAILED TO WRITE LOG: {e}", file=sys.stderr)
            print(text)

    def log_step(self, message: str, details: Optional[dict] = None) -> None:

        entry = {
            "ts": _now_ts(),
            "type": "step",
            "message": message,
        }
        if details:
            entry["details"] = details
        pretty = json.dumps(entry, ensure_ascii=False)
        self._append(pretty)

    def _df_summary(self, df) -> dict:
  
        summary = {}
        try:

            if pd is not None and isinstance(df, pd.DataFrame):
                summary["rows"] = int(df.shape[0])
                summary["cols"] = int(df.shape[1])
            
                nulls = df.isnull().sum()
                nulls = nulls[nulls > 0].sort_values(ascending=False)
                summary["nulls_count"] = int(nulls.sum()) if len(nulls) else 0
                summary["top_null_cols"] = nulls.head(5).to_dict()
  
                try:
                    dups = int(df.duplicated().sum())
                except Exception:
                    dups = None
                summary["duplicate_rows"] = dups
       
                try:
                    sample = df.head(3).to_dict(orient="records")
                    summary["sample_head"] = sample
                except Exception:
                    summary["sample_head"] = None
            else:
                summary["note"] = "pandas not available or object not a DataFrame"
        except Exception as e:
            summary["error"] = str(e)
        return summary

    def log_df_change(self, name: str, before, after, note: Optional[str] = None) -> None:
  
        entry = {
            "ts": _now_ts(),
            "type": "df_change",
            "name": name,
            "note": note,
            "before": None,
            "after": None,
            "delta": None,
        }
        try:
            if before is not None:
                entry["before"] = self._df_summary(before)
            if after is not None:
                entry["after"] = self._df_summary(after)

 
            try:
                rows_before = entry["before"].get("rows") if entry["before"] else None
                rows_after = entry["after"].get("rows") if entry["after"] else None
                cols_before = entry["before"].get("cols") if entry["before"] else None
                cols_after = entry["after"].get("cols") if entry["after"] else None
                delta = {}
                if rows_before is not None and rows_after is not None:
                    delta["rows_removed"] = rows_before - rows_after
                if cols_before is not None and cols_after is not None:
                    delta["cols_changed"] = cols_after - cols_before
                entry["delta"] = delta or None
            except Exception:
                entry["delta"] = None

        except Exception as e:
            entry["error"] = f"Could not produce df_change summary: {e}"

        self._append(json.dumps(entry, ensure_ascii=False))

    def log_json(self, obj: dict, message: Optional[str] = None) -> None:
        entry = {"ts": _now_ts(), "type": "json", "message": message or ""}
        entry["payload"] = obj
        self._append(json.dumps(entry, ensure_ascii=False))

    def tail_last(self, n: int = 200) -> str:
        """Return last n lines of the log file (best-effort)."""
        if not self.path.exists():
            return ""
        try:
            with self.path.open("rb") as f:
                avg = 200
                to_read = n * avg
                try:
                    f.seek(-to_read, 2)
                except Exception:
                    f.seek(0)
                data = f.read().decode(errors="replace")
                lines = data.splitlines()
                return "\n".join(lines[-n:])
        except Exception as e:
            return f"Failed to read log: {e}"


def _cli():
    if len(sys.argv) < 2:
        print("Usage: python -m src.transform_log \"Your message here\"")
        sys.exit(1)
    message = sys.argv[1]
    logger = TransformLogger()
    logger.log_step(message)
    print(f"Wrote message to {logger.path}")

if __name__ == "__main__":
    _cli()
