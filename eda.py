
import argparse
from pathlib import Path
import json
import textwrap
import datetime


import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator

try:
    import plotly.express as px
    _HAS_PLOTLY = True
except Exception:
    _HAS_PLOTLY = False


def safe_read_csv(p: Path):
    if not p.exists():
        return None
    try:
        df = pd.read_csv(p)
        return df
    except Exception as e:
        print(f"Warning: could not read {p}: {e}")
        return None


def short_table_html(df: pd.DataFrame, n=10):
    if df is None:
        return "<p><i>missing</i></p>"
    return df.head(n).to_html(index=False, classes='table', border=0)


def write_png(fig, path: Path, dpi=150):
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, bbox_inches='tight', dpi=dpi)
    plt.close(fig)


def make_distribution_plot(series, title, out_path: Path, bins=40, xlabel=None):
    fig, ax = plt.subplots(figsize=(8,4.6))
    clean = pd.to_numeric(series.dropna(), errors='coerce').dropna()
    if clean.shape[0] == 0:
        ax.text(0.5,0.5, 'No numeric data', ha='center', va='center')
    else:
        ax.hist(clean, bins=bins)
        ax.set_ylabel('Count')
        if xlabel:
            ax.set_xlabel(xlabel)
        ax.xaxis.set_major_locator(MaxNLocator(nbins=8))
    ax.set_title(title)
    write_png(fig, out_path)


def make_bar_plot(df, name_col, value_col, title, out_path: Path, topn=20, rotate_xticks=45):
    if df is None or df.shape[0] == 0:
        fig, ax = plt.subplots(figsize=(8,4))
        ax.text(0.5,0.5, 'No data', ha='center', va='center')
        ax.set_title(title)
        write_png(fig, out_path)
        return

    top = df.groupby(name_col)[value_col].sum().reset_index().sort_values(value_col, ascending=False).head(topn)
    fig, ax = plt.subplots(figsize=(9,5))
    ax.bar(top[name_col].astype(str), top[value_col])
    ax.set_title(title)
    ax.set_ylabel(value_col)
    ax.set_xlabel(name_col)
    plt.xticks(rotation=rotate_xticks)
    write_png(fig, out_path)


def make_pie_plot(df, name_col, value_col, title, out_path: Path, topn=6):
    if df is None or df.shape[0] == 0:
        fig, ax = plt.subplots(figsize=(6,4))
        ax.text(0.5,0.5, 'No data', ha='center', va='center')
        ax.set_title(title)
        write_png(fig, out_path)
        return

    top = df.groupby(name_col)[value_col].sum().reset_index().sort_values(value_col, ascending=False).head(topn)
    labels = top[name_col].astype(str).tolist()
    vals = top[value_col].tolist()
    fig, ax = plt.subplots(figsize=(6,6))
    ax.pie(vals, labels=labels, autopct='%1.1f%%', startangle=140, wedgeprops={'linewidth': 0.5, 'edgecolor':'white'})
    ax.set_title(title)
    write_png(fig, out_path)


def make_state_bar(prescriber_df: pd.DataFrame, title, out_path: Path):
    if prescriber_df is None:
        fig, ax = plt.subplots(figsize=(8,4))
        ax.text(0.5,0.5, 'No prescriber data', ha='center')
        write_png(fig, out_path)
        return
    # try common state column names
    candidates = [c for c in prescriber_df.columns if 'state' in c.lower() or 'code' in c.lower()]
    if not candidates:
        fig, ax = plt.subplots(figsize=(8,4))
        ax.text(0.5,0.5, 'No state column found', ha='center')
        write_png(fig, out_path)
        return
    col = candidates[0]
    counts = prescriber_df[col].fillna('NA').astype(str).value_counts().head(20)
    fig, ax = plt.subplots(figsize=(9,4.6))
    counts.plot.bar(ax=ax)
    ax.set_title(title)
    ax.set_xlabel(col)
    ax.set_ylabel('Count')
    plt.xticks(rotation=45)
    write_png(fig, out_path)


def generate_html(out_dir: Path, assets: dict, summaries: dict, samples: dict):

    now = datetime.datetime.utcnow().isoformat() + 'Z'
    html = textwrap.dedent(f"""
    <!doctype html>
    <html>
    <head>
      <meta charset="utf-8">
      <title>Automated EDA Report</title>
      <meta name="viewport" content="width=device-width, initial-scale=1">
      <style>
        body {{ background:#0e1117; color:#f8f8f8; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial; margin:18px; }}
        h1 {{ color:#fff }}
        .section {{ margin-bottom:28px; padding:12px; background:#0b0f13; border-radius:8px; border:1px solid #18202a; }}
        img.report-fig {{ max-width:100%; height:auto; border-radius:6px; border:1px solid #1f2937; display:block; margin:6px 0; }}
        table {{ border-collapse: collapse; width:100%; margin-top:8px; }}
        th, td {{ text-align:left; padding:6px; border-bottom:1px solid #18202a; }}
        th {{ background:#11141b; color:#9ad9b7; }}
        caption {{ text-align:left; font-size:0.9em; color:#9aa6b2; padding-bottom:6px; }}
        .small {{ color:#9aa6b2; font-size:0.9em }}
      </style>
    </head>
    <body>
      <h1>Automated EDA Report</h1>
      <p class="small">Generated: {now} — This file is self-contained and created by <code>eda_report_generator.py</code>.</p>

      <div class="section">
        <h2>Overview</h2>
        <p class="small">This report summarizes available tables and produces charts and short explanations. If some tables are missing the steps that rely on them are skipped.</p>
        <ul>
          <li>Tables discovered: {', '.join(summaries.get('tables', []))}</li>
          <li>Total distinct drugs (sample): {summaries.get('distinct_drugs', 'NA')}</li>
          <li>Total unique prescribers (sample): {summaries.get('distinct_prescribers', 'NA')}</li>
          <li>Total prescriptions (sample): {summaries.get('total_prescriptions', 'NA')}</li>
        </ul>
      </div>

      <div class="section">
        <h2>Top drugs by prescriptions</h2>
        <p class="small">Bar chart shows top drugs by aggregated prescription counts. Pie chart shows top-6 share.</p>
        <img class="report-fig" src="{assets.get('top_drugs_png', '')}" alt="Top drugs"/>
        <img class="report-fig" src="{assets.get('top_drugs_pie', '')}" alt="Top drugs pie"/>
      </div>

      <div class="section">
        <h2>Cost / Claims distribution</h2>
        <p class="small">Histogram of cost or claim counts (numeric fields found in prescriber_drug). Useful to spot skew and outliers.</p>
        <img class="report-fig" src="{assets.get('cost_hist', '')}" alt="Cost histogram"/>
      </div>

      <div class="section">
        <h2>Prescribers by state</h2>
        <p class="small">Distribution of prescribers across states (top 20 shown).</p>
        <img class="report-fig" src="{assets.get('prescriber_state', '')}" alt="Prescriber by state"/>
      </div>

      <div class="section">
        <h2>Small samples</h2>
        <p class="small">First few rows from each table (sample).</p>
        <h3>drug.csv (sample)</h3>
        {samples.get('drug', '<p><i>missing</i></p>')}
        <h3>prescriber.csv (sample)</h3>
        {samples.get('prescriber', '<p><i>missing</i></p>')}
        <h3>prescriber_drug.csv (sample)</h3>
        {samples.get('prescriber_drug', '<p><i>missing</i></p>')}
      </div>

      <div class="section">
        <h2>Notes & next steps</h2>
        <ol>
          <li>Use <code>--force-fresh</code> to regenerate curated outputs if you changed source CSVs.</li>
          <li>To get interactive charts in dashboards install <code>plotly</code> and run the dashboard which will display interactive versions of some charts.</li>
          <li>This report is intended as a quick review snapshot — for production auditing consider generating summary tables and storing them separately.</li>
        </ol>
      </div>

    </body>
    </html>
    """)

    out_file = out_dir / 'eda_report.html'
    out_file.write_text(html, encoding='utf8')
    return out_file


def main(raw_dir: Path, out_dir: Path):
    raw_dir = Path(raw_dir)
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    drug = safe_read_csv(raw_dir / 'drug.csv')
    prescriber = safe_read_csv(raw_dir / 'prescriber.csv')
    presc_drug = safe_read_csv(raw_dir / 'prescriber_drug.csv')

 
    tables = []
    if drug is not None: tables.append('drug')
    if prescriber is not None: tables.append('prescriber')
    if presc_drug is not None: tables.append('prescriber_drug')

    summaries = {'tables': tables}

    try:
        summaries['distinct_drugs'] = int(drug.shape[0]) if drug is not None else 'NA'
    except Exception:
        summaries['distinct_drugs'] = 'NA'
    try:
        summaries['distinct_prescribers'] = int(prescriber.shape[0]) if prescriber is not None else 'NA'
    except Exception:
        summaries['distinct_prescribers'] = 'NA'
    try:
        if presc_drug is not None and 'total_claims' in presc_drug.columns:
            summaries['total_prescriptions'] = int(presc_drug['total_claims'].sum())
        elif presc_drug is not None and 'prescriptions' in presc_drug.columns:
            summaries['total_prescriptions'] = int(presc_drug['prescriptions'].sum())
        else:
            summaries['total_prescriptions'] = 'NA'
    except Exception:
        summaries['total_prescriptions'] = 'NA'


    assets = {}


    try:
        if presc_drug is not None:
 
            name_cols = [c for c in presc_drug.columns if 'drug' in c.lower() and 'name' in c.lower()]
            count_cols = [c for c in presc_drug.columns if any(k in c.lower() for k in ('claim','count','total','prescript','qty','quantity','num'))]
            name_col = name_cols[0] if name_cols else presc_drug.columns[0]
            count_col = count_cols[0] if count_cols else presc_drug.columns[-1]
            top_df = presc_drug[[name_col, count_col]].copy()
            top_df.columns = ['drug', 'prescriptions']
            make_bar_plot(top_df, 'drug', 'prescriptions', 'Top drugs by prescriptions (sample)', out_dir / 'eda_top_drugs.png', topn=30)
            make_pie_plot(top_df, 'drug', 'prescriptions', 'Top 6 drugs share (pie)', out_dir / 'eda_top_drugs_pie.png')
            assets['top_drugs_png'] = 'eda_top_drugs.png'
            assets['top_drugs_pie'] = 'eda_top_drugs_pie.png'
            top_list = top_df.groupby('drug')['prescriptions'].sum().reset_index().sort_values('prescriptions', ascending=False).head(20).to_dict(orient='records')
        else:
            top_list = []
    except Exception as e:
        print('Top drugs generation failed:', e)
        top_list = []


    try:
        if presc_drug is not None:

            num_cols = [c for c in presc_drug.columns if presc_drug[c].dtype.kind in 'biufc']

            cost_cols = [c for c in num_cols if 'cost' in c.lower() or 'amount' in c.lower() or 'price' in c.lower()]
            target = cost_cols[0] if cost_cols else (num_cols[0] if num_cols else None)
            if target:
                make_distribution_plot(presc_drug[target], 'Distribution of ' + target, out_dir / 'eda_cost_hist.png', bins=40, xlabel=target)
                assets['cost_hist'] = 'eda_cost_hist.png'
            else:
                assets['cost_hist'] = ''
        else:
            assets['cost_hist'] = ''
    except Exception as e:
        print('Cost hist failed:', e)
        assets['cost_hist'] = ''

    try:
        make_state_bar(prescriber, 'Prescribers by state (top 20)', out_dir / 'eda_prescriber_state.png')
        assets['prescriber_state'] = 'eda_prescriber_state.png'
    except Exception as e:
        print('State plot failed:', e)
        assets['prescriber_state'] = ''

    samples = {}
    samples['drug'] = short_table_html(drug, n=10) if drug is not None else '<p><i>missing</i></p>'
    samples['prescriber'] = short_table_html(prescriber, n=10) if prescriber is not None else '<p><i>missing</i></p>'
    samples['prescriber_drug'] = short_table_html(presc_drug, n=10) if presc_drug is not None else '<p><i>missing</i></p>'


    summary_obj = {
        'generated_at': datetime.datetime.utcnow().isoformat() + 'Z',
        'tables': tables,
        'top_drugs': top_list,
        'summaries': summaries
    }
    try:
        with open(out_dir / 'eda_summary.json', 'w', encoding='utf8') as f:
            json.dump(summary_obj, f, indent=2)
    except Exception as e:
        print('Could not write eda_summary.json:', e)

    if _HAS_PLOTLY and presc_drug is not None and top_list:
        try:
            pdf = pd.DataFrame(top_list)
            fig = px.bar(pdf.head(30), x=pdf.columns[0], y='prescriptions', title='Top drugs (interactive)', template='plotly_dark')
            fig_html = fig.to_html(full_html=False, include_plotlyjs='cdn')
            with open(out_dir / 'eda_top_drugs_interactive.html', 'w', encoding='utf8') as f:
                f.write(fig_html)
            assets['top_drugs_interactive'] = 'eda_top_drugs_interactive.html'
        except Exception as e:
            print('Plotly interactive generation failed:', e)

    # final html
    out_file = generate_html(out_dir, assets, summary_obj['summaries'], samples)
    print('EDA written to:', out_file)
    print('Summary JSON:', out_dir / 'eda_summary.json')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--raw-dir', default='local_data/raw', help='path to raw csvs')
    parser.add_argument('--out-dir', default='local_data/artifacts', help='output directory for report and assets')
    args = parser.parse_args()
    main(Path(args.raw_dir), Path(args.out_dir))






