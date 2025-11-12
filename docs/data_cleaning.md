# Data Cleaning & Transformation Notes

This document summarizes the cleaning and transformation rules applied by the pipeline.

## 1. Overview
The pipeline ingests three CSVs:
- `drug.csv` (drug_id, drug_brand_name, drug, drug_type)
- `prescriber.csv` (presc_id, presc_fullname, presc_specialty, presc_state_code)
- `prescriber_drug.csv` (presc_id, drug_brand_name, total_claims, total_drug_cost)

Final transformed readable reports are written to:
`local_data/artifacts/readable_reports/`

A log of transformations is appended to: `local_data/artifacts/transform_log.txt`

## 2. Cleaning & Normalization Rules

### 2.1 Column normalisation
- Trim whitespace from string columns.
- Normalize casing for categorical labels where relevant:
  - `drug_type` is lowercased and then title-cased where appropriate.
  - Brand names preserved but leading/trailing spaces removed.

### 2.2 Missing values
- `presc_fullname`: if missing, leave `NULL` but flag in validation report.
- `total_claims`:
  - If missing, set to 0 and flag (only if source missing).
- `total_drug_cost`:
  - If missing, set to 0.0; if suspicious negative values are found, flagged for review.

### 2.3 Duplicates
- In ingestion: duplicates by full row are dropped.
- For key columns:
  - `presc_id` should be unique in `prescriber.csv`. If duplicates, the pipeline groups/aggregates as required for reports and logs a warning.

### 2.4 Type coercion
- `presc_id`, `drug_id`, `total_claims` → coerced to integer (where possible).
- `total_drug_cost` → coerced to float.

### 2.5 Aggregation
- `drug_report.csv`: aggregated by `drug_brand_name` summing `prescriptions` (or `total_claims`) depending on pipeline version.
- `prescriber_report.csv`: aggregated counts of prescriptions by `presc_id`.

## 3. Anonymization steps (if enabled)
- PII columns (names, addresses, emails) are replaced with hashed or pseudonymous IDs if anonymization is enabled.
- The pipeline has a `--skip-anonymize` option; by default anonymization is recommended for real PHI.

## 4. Imputation strategy (if any)
- Numeric missing values: median imputation (documented in `transform_log.txt`).
- Categorical missing values: replaced with `"UNKNOWN"` tag.

## 5. Logging
- Each major cleaning step writes an entry to `local_data/artifacts/transform_log.txt` containing:
  - timestamp, input file, action (e.g., "trim", "fillna", "dtype-coerce"), and summary counts (rows changed, nulls imputed).

## 6. Recommended improvements
- Use `pandera` for formal schema validation.
- Keep a versioned `data_dictionary.csv` describing final output columns and types.
