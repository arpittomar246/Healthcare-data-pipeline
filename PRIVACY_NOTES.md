# Privacy & Anonymization Notes

This file documents the project's approach to privacy and handling of potentially identifiable data.

## Purpose
This project processes prescriber and prescription-style CSVs. While the demo data is synthetic or anonymized, the pipeline must be configured carefully before processing real PHI.

## Key points
- **Default behavior**: The repository includes an anonymization toggle (`--skip-anonymize`). For production with real data, ensure anonymization is *enabled*.
- **PII handling**:
  - `presc_fullname` is considered direct identifier. When anonymization is enabled, it should be replaced with a pseudonymous ID or hashed value (e.g., SHA256 with salt).
  - Remove or obfuscate direct contact details (emails, phone numbers, addresses) prior to storage.
- **Access controls**:
  - Store artifacts in protected storage (encrypted disk or cloud storage with restricted IAM).
  - Do not commit raw production CSVs to the Git repository.
- **Audit logging**:
  - All transformations and anonymization events are logged in `local_data/artifacts/transform_log.txt` with timestamps.
- **Recommended improvements before production**:
  - Implement field-level hashing with per-environment salts (kept in secret store).
  - Add a data retention policy (automatic removal after X days) and access audit logs.
  - Obtain legal/ethical sign-off for PHI / healthcare data processing.


