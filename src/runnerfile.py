import os
import sys
import argparse
import configparser
import logging
import time
import json
import hashlib
import webbrowser
from pathlib import Path
from typing import Optional, Dict

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pandas import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

try:
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    USE_AESGCM = True
except Exception:
    USE_AESGCM = False

try:
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    HAVE_MATPLOTLIB = True
except Exception:
    HAVE_MATPLOTLIB = False


BASE_DIR = Path(os.getenv("LOCAL_DATA_DIR", "local_data")).absolute()
RAW_DIR = BASE_DIR / "raw"
CLEANSED_DIR = BASE_DIR / "cleansed"
CURATED_DIR = BASE_DIR / "curated"
ARTIFACTS_DIR = BASE_DIR / "artifacts"
for d in (RAW_DIR, CLEANSED_DIR, CURATED_DIR, ARTIFACTS_DIR):
    d.mkdir(parents=True, exist_ok=True)


PIPELINE_LOG = ARTIFACTS_DIR / "pipeline.log"


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s - %(message)s")
logger = logging.getLogger("runnerfile_local")


parser = argparse.ArgumentParser(description="Welcome to healthcare data pipeline logs")
parser.add_argument("--config", default="utils/project.cfg", help="Path to project.cfg")
parser.add_argument("--force-fresh", action="store_true", help="Force rebuild cleansed/curated from CSVs even if parquet exists")
parser.add_argument("--skip-anonymize", action="store_true", help="Skip anonymization step (for local debugging only)")
parser.add_argument("--open-ui", action="store_true", help="After run, automatically open EDA HTML in the default browser")
args, _ = parser.parse_known_args()


config = configparser.ConfigParser()
if Path(args.config).exists():
    config.read(args.config)
else:
    config["S3"] = {"RAW_BUCKET": "raw", "CLEANSED_BUCKET": "cleansed", "CURATED_BUCKET": "curated"}
    config["PRIVACY"] = {"COLUMNS": "presc_fullname,presc_name,patient_name", "KEY": ""}


class LocalS3Connector:
    def __init__(self, base_dir: Path = BASE_DIR):
        self.base_dir = Path(base_dir)

    def path(self, bucket: str, key: str = "") -> str:
        p = self.base_dir / bucket
        if key:
            p = p / key
        return str(p)

    def write_parquet(self, df, bucket, prefix, mode="overwrite"):
        p = Path(self.path(bucket, prefix))
        os.makedirs(p, exist_ok=True)
        df.write.mode(mode).parquet(str(p))

    def read_parquet(self, spark: SparkSession, bucket: str, prefix: str = ""):
        p = Path(self.path(bucket, prefix))
        if not p.exists():
            raise FileNotFoundError(f"Local path not found: {p}")
        return spark.read.parquet(str(p))

    def exists(self, bucket, prefix=""):
        return os.path.exists(self.path(bucket, prefix))


def create_spark_local():
    builder = SparkSession.builder.appName("HealthcarePipelineLocalMode").master("local[*]")
    builder = builder.config("spark.local.dir", str(BASE_DIR / "tmp"))
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def write_log(s: str):

    if not s.endswith("\n"):
        s = s + "\n"
    try:
        with open(PIPELINE_LOG, 'a', encoding='utf8') as f:
            f.write(s)
    except Exception:
        logger.exception("Failed to write to pipeline log")

    print(s, end='')


def show_preview(df, n=10):
    try:
        df.show(n, truncate=True)
    except Exception:
        try:
            rows = df.limit(n).collect()
            for r in rows:
                print(r.asDict() if hasattr(r, 'asDict') else r)
        except Exception:
            pass


def generate_data_dictionary(dfs):
    rows = []
    for name, df in dfs.items():
        try:
            for col in df.columns:
                null_count = df.filter(F.col(col).isNull()).count()
                try:
                    unique_count = df.select(col).distinct().count()
                except Exception:
                    unique_count = None
                sample_vals = [str(r[0]) for r in df.select(col).where(F.col(col).isNotNull()).limit(5).collect()]
                dtype = str(df.select(col).schema[0].dataType)
                rows.append({
                    'table': name,
                    'column': col,
                    'dtype': dtype,
                    'null_count': null_count,
                    'unique_count': unique_count,
                    'sample_values': '|'.join(sample_vals)
                })
        except Exception:
            logger.exception(f"Failed to introspect table {name}")
    out_csv = ARTIFACTS_DIR / 'data_dictionary.csv'
    with open(out_csv, 'w', encoding='utf8') as f:
        f.write('table,column,dtype,null_count,unique_count,sample_values\n')
        for r in rows:
            f.write(f"{r['table']},{r['column']},{r['dtype']},{r['null_count']},{r['unique_count']},\"{r['sample_values']}\"\n")
    write_log(f"Data dictionary saved to: {out_csv}")
    return out_csv

def generate_validation_report(dfs):
    report = {}
    for name, df in dfs.items():
        try:
            row_count = df.count()
            col_nulls = {c: int(df.filter(F.col(c).isNull()).count()) for c in df.columns}
            report[name] = {'rows': int(row_count), 'nulls': col_nulls}
        except Exception:
            logger.exception(f"Validation check failed for {name}")
            report[name] = {'error': 'validation_failed'}
    out = ARTIFACTS_DIR / 'validation_report.json'
    with open(out, 'w', encoding='utf8') as f:
        json.dump(report, f, indent=2)
    write_log(f"Validation report written to: {out}")
    return out




import base64
from io import BytesIO

def generate_eda_report(dfs: Dict[str, 'DataFrame']):
    images = []
    html_parts = ["<html><head><meta charset='utf-8'><title>EDA Report</title></head><body style='background:#0e1117;color:#fff;font-family:Arial,Helvetica,sans-serif'>",
                  "<div style='max-width:1100px;margin:16px auto;padding:12px'>",
                  "<h1 style='color:#fff'>Automated EDA Report</h1>"]

    for name, df in dfs.items():
        html_parts.append(f"<h2 style='color:#cbd5e1'>Table: {name}</h2>")
        try:
            pdf = df.limit(10000).toPandas()
            html_parts.append(f"<p style='color:#94a3b8'>Rows (sampled up to 10000): {len(pdf)}</p>")

      
            if HAVE_MATPLOTLIB:
                num_cols = pdf.select_dtypes(include=['number']).columns.tolist()
                for c in num_cols:
                    if pdf[c].dropna().shape[0] == 0:
                        continue
                    fig, ax = plt.subplots(figsize=(6,3))
                    pdf[c].dropna().hist(ax=ax)
                    ax.set_title(f"{name}.{c} distribution", color='#fff')
                    ax.set_xlabel('')
                    ax.set_ylabel('')
                    buf = BytesIO()
                    fig.savefig(buf, format='png', bbox_inches='tight')
                    plt.close(fig)
                    b64 = base64.b64encode(buf.getvalue()).decode('ascii')
                    html_parts.append(f"<div style='margin-bottom:12px'><img src='data:image/png;base64,{b64}' style='max-width:100%'></div>")

       
                cat_cols = pdf.select_dtypes(include=['object', 'category']).columns.tolist()
                for c in cat_cols:
                    top = pdf[c].value_counts().head(10)
                    if top.shape[0] == 0:
                        continue
                    fig, ax = plt.subplots(figsize=(6,3))
                    top.plot.bar(ax=ax)
                    ax.set_title(f"{name}.{c} top categories", color='#fff')
                    ax.set_xlabel('')
                    ax.set_ylabel('')
                    buf = BytesIO()
                    fig.savefig(buf, format='png', bbox_inches='tight')
                    plt.close(fig)
                    b64 = base64.b64encode(buf.getvalue()).decode('ascii')
                    html_parts.append(f"<div style='margin-bottom:12px'><img src='data:image/png;base64,{b64}' style='max-width:100%'></div>")
            else:
                html_parts.append("<p>Matplotlib not installed; charts skipped.</p>")

     
            missing = pdf.isnull().sum().rename('missing_count').to_frame()
            missing['missing_percent'] = (missing['missing_count'] / max(1, len(pdf)) * 100).round(2)
            html_parts.append("<h4 style='color:#e2e8f0'>Missing values (per column)</h4>")
            html_parts.append(missing.to_html(classes='missing-table', border=0))

        
            html_parts.append("<h4 style='color:#e2e8f0'>Sample rows</h4>")
            sample_html = pdf.head(10).to_html(index=False, classes='sample-table', border=0)
            html_parts.append(sample_html)

        except Exception:
            logger.exception(f"Failed to generate EDA for table {name}")
            html_parts.append(f"<p>Failed to generate EDA for {name}</p>")

    html_parts.append("</div></body></html>")
    report_path = ARTIFACTS_DIR / 'eda_report.html'
    with open(report_path, 'w', encoding='utf8') as f:
        f.write('\n'.join(html_parts))
    print(f"EDA html report written to: {report_path}")
    return report_path


def anonymize_dfs(dfs, config):
    cols_cfg = []
    if 'PRIVACY' in config and 'COLUMNS' in config['PRIVACY']:
        cols_cfg = [c.strip() for c in config['PRIVACY']['COLUMNS'].split(',') if c.strip()]
    if not cols_cfg:
        cols_cfg = ['name', 'fullname', 'patient_name', 'presc_fullname', 'prescriber_name']

    mapping_records = []
    for tbl, df in list(dfs.items()):
        for col in df.columns:
            if any(key in col.lower() for key in cols_cfg):
                write_log(f"Anonymizing {tbl}.{col}")
                try:
                    distinct_vals = [r[0] for r in df.select(col).distinct().where(F.col(col).isNotNull()).limit(100000).collect()]
                except Exception:
                    distinct_vals = []
                for val in distinct_vals:
                    try:
                        sval = str(val)
                        h = hashlib.sha256(sval.encode('utf8')).hexdigest()
                        mapping_records.append({'table': tbl, 'column': col, 'original': sval, 'hash': h})
                    except Exception:
                        continue
                dfs[tbl] = dfs[tbl].withColumn(col, F.sha2(F.col(col).cast('string'), 256))

    if mapping_records:
        plaintext = ''
        for r in mapping_records:
            orig = r['original'].replace('\n', ' ').replace('\r', ' ').replace(',', ' ')
            plaintext += f"{r['table']},{r['column']},\"{orig}\",{r['hash']}\n"
        timestamp = int(time.time())
        out_enc = ARTIFACTS_DIR / f"mapping_{timestamp}.enc"
        passphrase = None
        if 'PRIVACY' in config and 'KEY' in config['PRIVACY'] and config['PRIVACY']['KEY'].strip():
            passphrase = config['PRIVACY']['KEY'].strip()
        elif os.getenv('MAPPING_KEY'):
            passphrase = os.getenv('MAPPING_KEY')

        if USE_AESGCM and passphrase:
            try:
                key = hashlib.sha256(passphrase.encode('utf8')).digest()
                aesgcm = AESGCM(key)
                nonce = os.urandom(12)
                ct = aesgcm.encrypt(nonce, plaintext.encode('utf8'), None)
                with open(out_enc, 'wb') as f:
                    f.write(nonce + ct)
                write_log(f"Encrypted PII mapping (AES-GCM) written to: {out_enc}")
            except Exception:
                logger.exception("AES-GCM encryption failed; falling back to XOR method")
                key = hashlib.sha256((passphrase or "").encode('utf8')).digest()
                ks = (key * ((len(plaintext.encode('utf8')) // len(key)) + 1))[:len(plaintext.encode('utf8'))]
                cipher_bytes = bytes([a ^ b for a, b in zip(plaintext.encode('utf8'), ks)])
                with open(out_enc, 'wb') as f:
                    f.write(cipher_bytes)
                write_log(f"Encrypted PII mapping (XOR fallback) written to: {out_enc}")
        else:
            key = hashlib.sha256((passphrase or "").encode('utf8')).digest()
            ks = (key * ((len(plaintext.encode('utf8')) // len(key)) + 1))[:len(plaintext.encode('utf8'))]
            cipher_bytes = bytes([a ^ b for a, b in zip(plaintext.encode('utf8'), ks)])
            with open(out_enc, 'wb') as f:
                f.write(cipher_bytes)
            write_log(f"Encrypted PII mapping written to: {out_enc} (fallback)")
    else:
        write_log("No PII values found to map.")

    return dfs, mapping_records

def fallback_transform(spark, s3_connector, config, dfs, force_fresh=False):
    curated_bucket = config["S3"]["CURATED_BUCKET"] if "S3" in config and "CURATED_BUCKET" in config["S3"] else "curated"
    curated_outputs = {}

    def _resolve_key(dfs_local, candidates):
        for c in candidates:
            if c in dfs_local:
                return c
        for c in candidates:
            for k in dfs_local.keys():
                if c in k.lower():
                    return k
        tokens = []
        if isinstance(candidates, list):
            for c in candidates:
                tokens.extend([t for t in c.split('_') if t])
        for k in dfs_local.keys():
            lk = k.lower()
            if all(tok in lk for tok in tokens):
                return k
        return None

    prescriber_drug_key = _resolve_key(dfs, ['prescriber_drug', 'prescriberdrug', 'presc', 'prescriber-drug'])
    drug_key            = _resolve_key(dfs, ['drug', 'drugs'])
    prescriber_key      = _resolve_key(dfs, ['prescriber', 'prescribers', 'presc'])

    if 'prescriber_drug' not in dfs and prescriber_drug_key:
        dfs['prescriber_drug'] = dfs.get(prescriber_drug_key)
    if 'drug' not in dfs and drug_key:
        dfs['drug'] = dfs.get(drug_key)
    if 'prescriber' not in dfs and prescriber_key:
        dfs['prescriber'] = dfs.get(prescriber_key)

    write_log(f"Resolved dfs keys for transform: {list(dfs.keys())}")

    if 'prescriber_drug' not in dfs:
        try:
            dfs['prescriber_drug'] = s3_connector.read_parquet(spark, config["S3"]["CLEANSED_BUCKET"], 'prescriber_drug')
        except Exception:
            write_log('prescriber_drug parquet not found')
    if 'drug' not in dfs:
        try:
            dfs['drug'] = s3_connector.read_parquet(spark, config["S3"]["CLEANSED_BUCKET"], 'drug')
        except Exception:
            write_log('drug parquet not found')
    if 'prescriber' not in dfs:
        try:
            dfs['prescriber'] = s3_connector.read_parquet(spark, config["S3"]["CLEANSED_BUCKET"], 'prescriber')
        except Exception:
            write_log('prescriber parquet not found')

    if 'prescriber_drug' in dfs:
        pdf = dfs['prescriber_drug']
        cols = pdf.columns
        write_log(f"prescriber_drug columns detected: {cols}")
        drug_col = None
        count_col = None
        for c in cols:
            if 'drug' in c.lower() and ('id' in c.lower() or 'name' in c.lower()):
                drug_col = c
            if any(k in c.lower() for k in ('count', 'qty', 'quantity', 'num', 'total', 'claims', 'cost')):
                count_col = c
        if drug_col is None and len(cols) >= 2:
            drug_col = cols[1]
        if count_col is None:
            pdf = pdf.withColumn("_row_count", F.lit(1))
            count_col = "_row_count"
        try:
            drug_report = pdf.groupBy(drug_col).agg(F.sum(count_col).alias('prescriptions')).orderBy(F.desc('prescriptions'))
            write_log("--- Drug report sample (fresh) ---")
            show_preview(drug_report, n=10)
            curated_outputs['drug_report'] = drug_report

            try:
                s3_connector.write_parquet(drug_report, curated_bucket, config.get('READ_DATA_DB', {}).get('DRUG_REPORT', 'drug_report'), mode='overwrite')
            except Exception:
                write_log('Could not write drug_report parquet (continuing)')
        except Exception:
            logger.exception("Failed to create drug_report")
    else:
        write_log("No prescriber_drug data available for drug_report")


    if 'prescriber' in dfs and 'prescriber_drug' in dfs:
        try:
            pres = dfs['prescriber']
            pdg = dfs['prescriber_drug']
            pid_col = None
            for c in pres.columns:
                if c.lower().endswith('id') or c.lower() == 'id':
                    pid_col = c
                    break
            if pid_col is None and 'prescriber_id' in pdg.columns:
                pid_col = 'prescriber_id'
            if pid_col is None:
                pid_col = pres.columns[0]
            pdg_pid = None
            for c in pdg.columns:
                if 'prescriber' in c.lower() and ('id' in c.lower() or c.lower().endswith('_id')):
                    pdg_pid = c
                    break
            if pdg_pid is None:
                pdg_pid = pdg.columns[0]
            pres_report = pdg.groupBy(pdg_pid).agg(F.count('*').alias('num_prescriptions')).orderBy(F.desc('num_prescriptions'))
            name_col = None
            for c in pres.columns:
                if 'name' in c.lower():
                    name_col = c
                    break
            if name_col is not None:
                try:
                    pres_sel = pres.withColumnRenamed(pid_col, pdg_pid) if pid_col != pdg_pid else pres
                    pres_report = pres_report.join(pres_sel.select(pdg_pid, name_col), on=pdg_pid, how='left')
                except Exception:
                    pass
            write_log("--- Prescriber report sample (fresh) ---")
            show_preview(pres_report, n=10)
            curated_outputs['prescriber_report'] = pres_report
            try:
                s3_connector.write_parquet(pres_report, curated_bucket, config.get('READ_DATA_DB', {}).get('PRESCRIBER_REPORT', 'prescriber_report'), mode='overwrite')
            except Exception:
                write_log('Could not write prescriber_report parquet (continuing)')
        except Exception:
            logger.exception("Failed to create prescriber_report")
    else:
        write_log("Not enough data to create prescriber_report")

    return curated_outputs


def main():
    overall_start = time.perf_counter()
    write_log("\n=== WELCOME TO HEALTHCARE PIPELINE LIVE LOGS ===\n")
    spark = create_spark_local()
    s3_connector = LocalS3Connector()


    write_log("\nStarting preprocessing stage...\n")
    dfs = {}
    raw_dir_path = RAW_DIR.resolve()
    write_log(f"Looking for CSVs under raw directory: {raw_dir_path}")
    csv_files = list(raw_dir_path.rglob('*.csv')) if raw_dir_path.exists() else []
    if csv_files:
        write_log(f"Detected {len(csv_files)} CSV file(s): {[p.name for p in csv_files]}")
        read_opts = {"header": True, "inferSchema": True}
        for p in csv_files:
            if p.parent != raw_dir_path:
                candidate_table = p.parent.name.lower()
            else:
                candidate_table = p.stem.lower()
            write_log(f"Reading CSV for table '{candidate_table}' from: {p}")
            try:
                df = spark.read.options(**read_opts).csv(str(p))
                write_log(f"Schema for {candidate_table}:")
                df.printSchema()
                show_preview(df, n=5)
                dfs[candidate_table] = df
                try:
                    cleansed_exists = s3_connector.exists(config["S3"]["CLEANSED_BUCKET"], candidate_table)
                except Exception:
                    cleansed_exists = False
                if args.force_fresh or not cleansed_exists:
                    try:
                        s3_connector.write_parquet(df, config["S3"]["CLEANSED_BUCKET"], candidate_table, mode='overwrite')
                        write_log(f"Wrote cleansed parquet for {candidate_table}")
                    except Exception:
                        logger.exception(f"Failed to write cleansed parquet for {candidate_table} — continuing with in-memory DataFrame")
            except Exception:
                logger.exception(f"Failed to read CSV at {p}")
    else:
        write_log("No CSVs found in raw directory — attempting to read existing cleansed parquet (if any)")
        for tbl in ('state', 'drug', 'prescriber', 'prescriber_drug'):
            try:
                dfs[tbl] = s3_connector.read_parquet(spark, config["S3"]["CLEANSED_BUCKET"], tbl)
                write_log(f"Loaded cleansed parquet for {tbl}")
            except Exception:
                pass
    write_log("\nPreprocessing stage finished.\n")

    mapping_records = []
    if dfs:
        if args.skip_anonymize:
            write_log("Skipping anonymization (--skip-anonymize set). Readable reports will use original values.")
            mapping_records = []
        else:
            try:
                dfs, mapping_records = anonymize_dfs(dfs, config)
            except Exception:
                logger.exception("Anonymization failed; continuing without mapping")

    try:
        data_dict_path = generate_data_dictionary(dfs)
        val_path = generate_validation_report(dfs)
        eda_path = generate_eda_report(dfs)
    except Exception:
        logger.exception("Failed to generate data dictionary / validation / EDA")
        eda_path = None


    write_log("\nStarting transformation stage...\n")
    curated_dfs = {}
    try:
        curated_dfs = fallback_transform(spark, s3_connector, config, dfs, force_fresh=args.force_fresh)
        write_log("\nTransformation stage finished.\n")
    except Exception:
        logger.exception("Transformation failed")


    try:
        readable_dir = ARTIFACTS_DIR / 'readable_reports'
        readable_dir.mkdir(parents=True, exist_ok=True)
        map_exact = {}
        for r in mapping_records:
            key = (r['table'], r['column'])
            map_exact.setdefault(key, {})[r['hash']] = r['original']
        map_by_col = {}
        for r in mapping_records:
            map_by_col.setdefault(r['column'], {})[r['hash']] = r['original']
        report_source_priority = {'drug_report': ['prescriber_drug', 'drug'], 'prescriber_report': ['prescriber', 'prescriber_drug']}
        for name, df in curated_dfs.items():
            try:
                pdf = df.limit(10000).toPandas()
                for col in pdf.columns:
                    replaced = False
                    prefs = report_source_priority.get(name, [])
                    for src in prefs:
                        key = (src, col)
                        if key in map_exact:
                            m = map_exact[key]
                            pdf[col] = pdf[col].apply(lambda x: m.get(str(x), x))
                            replaced = True
                            break
                    if not replaced and col in map_by_col:
                        m = map_by_col[col]
                        pdf[col] = pdf[col].apply(lambda x: m.get(str(x), x))
                out_csv = readable_dir / f"{name}.csv"
                pdf.to_csv(out_csv, index=False)
                write_log(f"Readable report written to: {out_csv}")
            except Exception:
                logger.exception(f"Failed to produce readable report for {name}")
    except Exception:
        logger.exception("Failed to write readable reports")

    write_log("\nPreviewing curated outputs (fresh outputs preferred)...\n")
    try:
        if curated_dfs:
            for name, df in curated_dfs.items():
                write_log(f"--- Preview (fresh) for {name} ---")
                show_preview(df, n=10)
        else:
            curated_bucket = config["S3"]["CURATED_BUCKET"]
            for report_name in (config.get("READ_DATA_DB", {}).get("DRUG_REPORT", "drug_report"), config.get("READ_DATA_DB", {}).get("PRESCRIBER_REPORT", "prescriber_report")):
                try:
                    report_path = Path(s3_connector.path(curated_bucket, report_name)).resolve()
                    write_log(f"Looking for curated report at: {report_path}")
                    if report_path.exists():
                        df = spark.read.parquet(str(report_path))
                        write_log(f"--- Sample rows from {report_name} (parquet read) ---")
                        show_preview(df, n=10)
                    else:
                        write_log(f"Curated report not found at {report_path}")
                except Exception:
                    logger.exception(f"Could not preview report {report_name}")
    except Exception:
        logger.exception("Preview failed")


    if args.open_ui:
        try:
            if eda_path and Path(eda_path).exists():
                webbrowser.open_new_tab(str(Path(eda_path).resolve().as_uri()))
                write_log(f"Opened EDA report in browser: {eda_path}")
        except Exception:
            logger.exception("Failed to auto-open UI artifacts")

    spark.stop()
    overall_end = time.perf_counter()
    write_log("\n===== TIMINGS =====")
    try:
        write_log(f"Total runtime: {overall_end - overall_start:.2f}s")
    except Exception:
        write_log("Total runtime: n/a")
    write_log("====================\n")

    # Decrypt snippet for mapping
    write_log("To decrypt mapping_<ts>.enc locally, see README or use provided snippets in runnerfile.")

if __name__ == "__main__":
    try:
        with open(PIPELINE_LOG, 'w', encoding='utf8') as f:
            f.write('')
    except Exception:
        pass
    main()
