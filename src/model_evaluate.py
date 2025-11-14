
import argparse
from pathlib import Path
import json
import math
import sys

try:
    import numpy as np
    import pandas as pd
    import matplotlib.pyplot as plt
    from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
    import joblib
except Exception as e:
    print("Missing required packages. Install pandas, numpy, matplotlib, scikit-learn, joblib.")
    raise

def safe_read_predictions(preds_path: Path):
    
    df = pd.read_csv(preds_path)

    col_map = {c.lower(): c for c in df.columns}

    preferred_true = ["y_true", "actual", "true", "target"]
    preferred_pred = ["y_pred", "predicted", "pred", "yhat"]

    found_true = None
    found_pred = None

    for name in preferred_true:
        if name in col_map:
            found_true = col_map[name]
            break
    for name in preferred_pred:
        if name in col_map:
            found_pred = col_map[name]
            break


    def is_mostly_numeric(series, min_ratio=0.5):
        coerced = pd.to_numeric(series, errors="coerce")
        non_na = coerced.notna().sum()
        return non_na >= max(1, int(min_ratio * len(series)))

    if found_true and found_pred:
        if is_mostly_numeric(df[found_true]) and is_mostly_numeric(df[found_pred]):
            return df[[found_true, found_pred]].rename(columns={found_true: "y_true", found_pred: "y_pred"})

        found_true = None
        found_pred = None


    numeric_scores = {}
    for c in df.columns:
        coerced = pd.to_numeric(df[c], errors="coerce")
        numeric_scores[c] = int(coerced.notna().sum())


    numeric_candidates = [c for c, cnt in numeric_scores.items() if cnt > 0]

    if len(numeric_candidates) >= 2:
    
        best_pair = None
        best_score = -1
        for i in range(len(numeric_candidates)):
            for j in range(i+1, len(numeric_candidates)):
                a = numeric_candidates[i]
                b = numeric_candidates[j]
                score = numeric_scores[a] + numeric_scores[b]
                if score > best_score:
                    best_score = score
                    best_pair = (a, b)
        a, b = best_pair

        sub = df[[a, b]].copy()
        sub[a] = pd.to_numeric(sub[a], errors="coerce")
        sub[b] = pd.to_numeric(sub[b], errors="coerce")

        if sub[a].notna().sum() == 0 or sub[b].notna().sum() == 0:
      
            for i in range(len(numeric_candidates)):
                for j in range(i+1, len(numeric_candidates)):
                    a = numeric_candidates[i]
                    b = numeric_candidates[j]
                    sub = df[[a, b]].copy()
                    sub[a] = pd.to_numeric(sub[a], errors="coerce")
                    sub[b] = pd.to_numeric(sub[b], errors="coerce")
                    if sub[a].notna().sum() > 0 and sub[b].notna().sum() > 0:
                        return sub.rename(columns={a: "y_true", b: "y_pred"})
   
        else:
            return sub.rename(columns={a: "y_true", b: "y_pred"})


    sample = df.head(10).to_dict(orient="records")
    readable_cols = [{"name": c, "sample": list(df[c].dropna().unique()[:5]), "numeric_count": numeric_scores.get(c, 0)} for c in df.columns]
    msg = (
        "Could not automatically find numeric 'actual' and 'predicted' columns in the predictions CSV.\n"
        "Columns detected (name, numeric_count, sample_values_up_to_5):\n"
    )
    for info in readable_cols:
        msg += f"  - {info['name']}: numeric_count={info['numeric_count']}, sample={info['sample']}\n"
    msg += "\nFirst 10 rows (for debugging):\n"
    msg += json.dumps(sample, indent=2)
    raise ValueError(msg)

def ensure_dirs(out_dir: Path):
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "plots").mkdir(parents=True, exist_ok=True)

def save_plot(fig, path: Path):
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)

def plot_residuals_hist(y_true, y_pred, out_path: Path):
    resid = (y_true - y_pred)
    fig, ax = plt.subplots(figsize=(6,4))
    ax.hist(resid, bins=50)
    ax.set_title("Residuals distribution")
    ax.set_xlabel("Residual (actual - predicted)")
    ax.set_ylabel("Count")
    save_plot(fig, out_path)

def plot_pred_vs_actual(y_true, y_pred, out_path: Path):
    fig, ax = plt.subplots(figsize=(6,6))
    ax.scatter(y_true, y_pred, alpha=0.6, s=10)
    mn = min(min(y_true), min(y_pred))
    mx = max(max(y_true), max(y_pred))
    ax.plot([mn, mx], [mn, mx], color="red", linewidth=1, linestyle="--")
    ax.set_title("Predicted vs Actual")
    ax.set_xlabel("Actual")
    ax.set_ylabel("Predicted")
    save_plot(fig, out_path)

def plot_residuals_vs_pred(y_true, y_pred, out_path: Path):
    resid = y_true - y_pred
    fig, ax = plt.subplots(figsize=(6,4))
    ax.scatter(y_pred, resid, alpha=0.6, s=10)
    ax.axhline(0, color="red", linestyle="--", linewidth=1)
    ax.set_title("Residuals vs Predicted")
    ax.set_xlabel("Predicted")
    ax.set_ylabel("Residual (actual - predicted)")
    save_plot(fig, out_path)

def plot_feature_importances(model, feature_names, out_path: Path):

    if hasattr(model, "feature_importances_"):
        fi = np.array(model.feature_importances_)
    elif hasattr(model, "coef_"):
        coef = np.array(model.coef_)
 
        if coef.ndim > 1:
            fi = np.abs(coef).sum(axis=0)
        else:
            fi = np.abs(coef)
    else:
        return False


    order = np.argsort(-fi)
    labels = np.array(feature_names)[order][:30] if feature_names is not None else np.array([f"f{i}" for i in range(len(fi))])[order][:30]
    vals = fi[order][:30]

    fig, ax = plt.subplots(figsize=(6, min(8, 0.25 * len(labels) + 2)))
    y_pos = np.arange(len(labels))
    ax.barh(y_pos, vals[::-1])
    ax.set_yticks(y_pos)
    ax.set_yticklabels(labels[::-1])
    ax.invert_yaxis()
    ax.set_title("Top feature importances (absolute)")
    save_plot(fig, out_path)
    return True

def compute_metrics(y_true, y_pred):
    y_true = np.array(y_true).astype(float)
    y_pred = np.array(y_pred).astype(float)
    mse = mean_squared_error(y_true, y_pred)
    rmse = math.sqrt(mse)
    mae = mean_absolute_error(y_true, y_pred)
    r2 = r2_score(y_true, y_pred)
    return {"mse": float(mse), "rmse": float(rmse), "mae": float(mae), "r2": float(r2), "n_samples": int(len(y_true))}

def main(argv=None):
    parser = argparse.ArgumentParser(description="Model evaluation & plots for pipeline.")
    parser.add_argument("--model", type=str, default="local_data/artifacts/models/baseline_model.joblib",
                        help="Path to saved model (joblib). Optional but will be used for feature importances.")
    parser.add_argument("--preds", type=str, default="local_data/artifacts/predictions.csv",
                        help="CSV with true/pred columns.")
    parser.add_argument("--out", type=str, default="local_data/artifacts",
                        help="Output directory for metrics and plots.")
    args = parser.parse_args(argv)

    model_path = Path(args.model)
    preds_path = Path(args.preds)
    out_dir = Path(args.out)

    ensure_dirs(out_dir)
    plots_dir = out_dir / "plots"


    if not preds_path.exists():
        print(f"[ERROR] Predictions file not found: {preds_path}")
        sys.exit(2)

    try:
        df = safe_read_predictions(preds_path)
    except Exception as e:
        print(f"[ERROR] Could not read predictions CSV: {e}")
        sys.exit(2)

    y_true = df["y_true"].values
    y_pred = df["y_pred"].values

    metrics = compute_metrics(y_true, y_pred)
    metrics["preds_path"] = str(preds_path.resolve())
    metrics["model_path"] = str(model_path.resolve()) if model_path.exists() else None

    try:
        plot_residuals_hist(y_true, y_pred, plots_dir / "residuals_hist.png")
    except Exception as e:
        print("Warning: failed to create residuals histogram:", e)
    try:
        plot_pred_vs_actual(y_true, y_pred, plots_dir / "pred_vs_actual.png")
    except Exception as e:
        print("Warning: failed to create pred_vs_actual plot:", e)
    try:
        plot_residuals_vs_pred(y_true, y_pred, plots_dir / "residuals_vs_pred.png")
    except Exception as e:
        print("Warning: failed to create residuals_vs_pred plot:", e)


    fi_saved = False
    feature_names = None
    if model_path.exists():
        try:
            model = joblib.load(model_path)
            if hasattr(model, "feature_names_in_"):
                feature_names = list(model.feature_names_in_)
            fi_saved = plot_feature_importances(model, feature_names, plots_dir / "feature_importances.png")
        except Exception as e:
            print("Warning: failed to load model or plot feature importances:", e)


    metrics["plots"] = {p.name: str((plots_dir / p.name).resolve()) for p in list(plots_dir.glob("*.png"))}
    out_json = out_dir / "model_metrics.json"
    try:
        out_json.write_text(json.dumps(metrics, indent=2))
        print(f"Wrote metrics to: {out_json}")
    except Exception as e:
        print("Failed to write metrics json:", e)

    print("Model evaluation summary:")
    print(json.dumps(metrics, indent=2))

if __name__ == "__main__":
    main()
