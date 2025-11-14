
import sys
from pathlib import Path
import traceback

try:
    import pandas as pd
    import numpy as np
    from sklearn.model_selection import train_test_split
    from sklearn.ensemble import RandomForestRegressor
    from sklearn.pipeline import Pipeline
    from sklearn.compose import ColumnTransformer
    from sklearn.preprocessing import OneHotEncoder, StandardScaler
    from sklearn.metrics import mean_squared_error, r2_score
    import joblib
except Exception as e:
    print("Missing dependency. Install requirements: pandas scikit-learn joblib numpy")
    print("Exception:", e)
    sys.exit(1)


BASE_DIR = Path.cwd()
ARTIFACTS = BASE_DIR / "local_data" / "artifacts"
ARTIFACTS.mkdir(parents=True, exist_ok=True)
MODELS_DIR = ARTIFACTS / "models"
MODELS_DIR.mkdir(parents=True, exist_ok=True)


PRESCRIBER_DRUG = BASE_DIR / "local_data" / "raw" / "prescriber_drug.csv"
PRESCRIBER = BASE_DIR / "local_data" / "raw" / "prescriber.csv"
DRUG = BASE_DIR / "local_data" / "raw" / "drug.csv"

def load_data():
    dfs = {}
    for p, name in [(PRESCRIBER_DRUG, "prescriber_drug"), (PRESCRIBER, "prescriber"), (DRUG, "drug")]:
        if p.exists():
            try:
                dfs[name] = pd.read_csv(p)
                print(f"Loaded {name}: {len(dfs[name])} rows")
            except Exception as e:
                print(f"Failed to read {p}: {e}")
                dfs[name] = None
        else:
            print(f"File not found: {p}")
            dfs[name] = None
    return dfs

def prepare_dataset(dfs):
    pdg = dfs.get("prescriber_drug")
    pr = dfs.get("prescriber")
    drug = dfs.get("drug")

    if pdg is None:
        raise RuntimeError("prescriber_drug.csv is required")

    if pr is not None and "presc_id" in pr.columns:
        merged = pdg.merge(pr.drop(columns=[c for c in pr.columns if c not in ["presc_id", "presc_specialty", "presc_state_code"]]),
                            how="left", on="presc_id")
    else:
        merged = pdg.copy()
        merged["presc_specialty"] = np.nan
        merged["presc_state_code"] = np.nan


    if drug is not None and "drug_brand_name" in drug.columns:
        merged = merged.merge(drug[["drug_brand_name", "drug_type", "drug"]].drop_duplicates(),
                              how="left", on="drug_brand_name")
    else:
        merged["drug_type"] = np.nan
        merged["drug"] = np.nan

    
    merged["total_claims"] = pd.to_numeric(merged.get("total_claims", merged.get("total_claims", np.nan)), errors="coerce")

    merged["total_drug_cost"] = pd.to_numeric(merged.get("total_drug_cost", np.nan), errors="coerce")


    merged = merged[merged["total_claims"].notnull()].copy()
    if merged.shape[0] == 0:
        raise RuntimeError("No rows with numeric total_claims found to train on.")


    for c in ["presc_specialty", "presc_state_code", "drug_type", "drug_brand_name", "drug"]:
        if c in merged.columns:
            merged[c] = merged[c].fillna("unknown").astype(str)
        else:
            merged[c] = "unknown"


    brand_counts = merged.groupby("drug_brand_name").size().to_dict()
    merged["brand_prescriber_count"] = merged["drug_brand_name"].map(brand_counts).fillna(0)


    presc_claims = merged.groupby("presc_id")["total_claims"].sum().to_dict()
    merged["presc_total_claims_history"] = merged["presc_id"].map(presc_claims).fillna(0)

    return merged

def build_and_train(df):

    cat_cols = ["presc_specialty", "presc_state_code", "drug_type", "drug_brand_name"]
    num_cols = ["total_drug_cost", "brand_prescriber_count", "presc_total_claims_history"]


    cat_cols = [c for c in cat_cols if c in df.columns]
    num_cols = [c for c in num_cols if c in df.columns]

    X = df[cat_cols + num_cols].copy()
    y = df["total_claims"].values


    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)


    preprocessor = ColumnTransformer(transformers=[
        ("cat", OneHotEncoder(handle_unknown="ignore", sparse_output=False), cat_cols),
        ("num", StandardScaler(), num_cols)
    ], remainder="drop")


    model = Pipeline([
        ("pre", preprocessor),
        ("rf", RandomForestRegressor(n_estimators=100, random_state=42, n_jobs=-1))
    ])

    print("Training model...")
    model.fit(X_train, y_train)
    print("Training complete.")


    y_pred = model.predict(X_test)
    import numpy as np
    from sklearn.metrics import mean_squared_error


    mse = mean_squared_error(y_test, y_pred)
    rmse = float(np.sqrt(mse))
    r2 = r2_score(y_test, y_pred)

    print(f"Test RMSE: {rmse:.4f}")
    print(f"Test R2: {r2:.4f}")


    model_path = MODELS_DIR / "baseline_model.joblib"
    joblib.dump(model, model_path)
    print(f"Saved model to: {model_path}")

    preds_df = X_test.copy()
    preds_df["y_true_total_claims"] = y_test
    preds_df["y_pred_total_claims"] = y_pred
    out_preds = ARTIFACTS / "predictions.csv"
    preds_df.to_csv(out_preds, index=False)
    print(f"Saved test predictions to: {out_preds}")


    summary = {
        "test_rows": int(len(y_test)),
        "rmse": float(rmse),
        "r2": float(r2),
        "model_path": str(model_path),
        "predictions_csv": str(out_preds)
    }
    try:
        import json
        (ARTIFACTS / "model_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf8")
    except Exception:
        pass

def main():
    try:
        dfs = load_data()
        df = prepare_dataset(dfs)
        build_and_train(df)
        print("Baseline modeling finished successfully.")
    except Exception as e:
        print("Modeling failed:", e)
        traceback.print_exc()

if __name__ == "__main__":
    main()
