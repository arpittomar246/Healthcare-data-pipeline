import streamlit as st
from pathlib import Path
import glob
import os
import subprocess
import sys
import time
import textwrap
import json

try:
    import pandas as pd
except Exception:
    pd = None

try:
    import plotly.express as px
except Exception:
    px = None


st.set_page_config(page_title="Healthcare Pipeline — Dashboard", layout="wide")


DARK_CSS = """
body, .stApp { background-color: #0e1117; color: #fafafa; }
.block-container { padding-top:10px; padding-bottom:10px; padding-left:15px; padding-right:15px; max-width:1400px; }
h1,h2,h3,h4,h5 { color:#fff; font-weight:600; margin-bottom:0.5rem; }
[data-testid="stSidebar"] > div:first-child { padding-top:10px; padding-left:8px; padding-right:8px; background:#2e2e2e; color:#f1f1f1; }
.stButton>button { background:#1f2937; color:#fff; border:1px solid #2c3340; padding:8px 14px; border-radius:6px; }
.stButton>button:hover { background:#374151; border:1px solid #475569; }
.stMetric { background:#1a1d24; padding:6px 12px; border-radius:8px; box-shadow:0 1px 3px rgba(0,0,0,0.4); }
textarea[aria-label="Pipeline logs (live)"] { background:#11141b; color:#00ff99; font-family:ui-monospace, SFMono-Regular, Menlo, Monaco, "Roboto Mono", monospace; font-size:12px !important; }
.stDataFrame table, .stTable table { background:#11141b; color:#f8f8f8; font-size:12px; }
.js-plotly-plot .plotly { background-color: #0e1117 !important; }

/* smoother log font rendering & scrollbar */
textarea[aria-label="Pipeline logs (live)"], #log {
  -webkit-font-smoothing: antialiased;
  scrollbar-width: thin;
  scrollbar-color: rgba(255,255,255,0.12) transparent;
}
#log::-webkit-scrollbar { width: 10px; }
#log::-webkit-scrollbar-thumb { background: rgba(255,255,255,0.08); border-radius: 6px; }
"""
st.markdown(f"<style>{DARK_CSS}</style>", unsafe_allow_html=True)

BASE_DIR = Path.cwd()
ARTIFACTS = BASE_DIR / "local_data" / "artifacts"
ARTIFACTS.mkdir(parents=True, exist_ok=True)
PIPELINE_LOG = ARTIFACTS / "pipeline.log"
READABLE_DIR = ARTIFACTS / "readable_reports"
READABLE_DIR.mkdir(parents=True, exist_ok=True)
EDA_HTML = ARTIFACTS / "eda_report.html"
EDA_SUMMARY = ARTIFACTS / "eda_summary.json"


if "pipeline_proc" not in st.session_state:
    st.session_state.pipeline_proc = None
if "pipeline_logfile" not in st.session_state:
    st.session_state.pipeline_logfile = None
if "http_server_proc" not in st.session_state:
    st.session_state.http_server_proc = None
if "tail_lines" not in st.session_state:
    st.session_state.tail_lines = 500
if "topn" not in st.session_state:
    st.session_state.topn = 15
if "auto_start_http" not in st.session_state:
    st.session_state.auto_start_http = True


def tail_file(path: Path, n: int = 200):
    if not path.exists():
        return ""
    try:
        with path.open("rb") as f:
            avg = 120
            to_read = n * avg
            try:
                f.seek(-to_read, os.SEEK_END)
            except Exception:
                f.seek(0)
            bs = f.read()
            text = bs.decode(errors="replace")
            lines = text.splitlines()
            return "\n".join(lines[-n:])
    except Exception as e:
        return f"Failed to read log: {e}"

def _is_proc_running(proc):
    try:
        return proc is not None and proc.poll() is None
    except Exception:
        return False

def _start_pipeline_local(cmd_list, logfile_path: Path):
  
    if _is_proc_running(st.session_state.pipeline_proc):
        st.warning("Pipeline already running; stop it first.")
        return
    logfile_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        lf = open(logfile_path, "ab")
    except Exception as e:
        st.error(f"Cannot open logfile {logfile_path}: {e}")
        return
    try:
        full_cmd = [sys.executable] + cmd_list
        proc = subprocess.Popen(full_cmd, stdout=lf, stderr=subprocess.STDOUT, cwd=str(BASE_DIR))
    except Exception as e:
        try:
            lf.close()
        except Exception:
            pass
        st.error(f"Failed to start pipeline: {e}")
        return
    st.session_state.pipeline_proc = proc
    st.session_state.pipeline_logfile = lf
    st.success(f"Pipeline started (PID={proc.pid}) — logs appended to {logfile_path}")

def _stop_pipeline_local(timeout=5.0):
    proc = st.session_state.pipeline_proc
    lf = st.session_state.pipeline_logfile
    if not _is_proc_running(proc):
        if lf:
            try:
                lf.flush(); lf.close()
            except Exception:
                pass
        st.session_state.pipeline_proc = None
        st.session_state.pipeline_logfile = None
        st.info("No running pipeline found.")
        return
    try:
        proc.terminate()
        t0 = time.time()
        while _is_proc_running(proc) and (time.time() - t0) < timeout:
            time.sleep(0.1)
        if _is_proc_running(proc):
            proc.kill()
        if lf:
            try:
                lf.flush(); lf.close()
            except Exception:
                pass
    except Exception as e:
        st.error(f"Error stopping pipeline: {e}")
    finally:
        st.session_state.pipeline_proc = None
        st.session_state.pipeline_logfile = None
        st.success("Pipeline stopped.")

def _start_http_server(port=8502):
   
    p = st.session_state.http_server_proc
    if _is_proc_running(p):
        st.info("HTTP server already running.")
        return
    try:
        proc = subprocess.Popen([sys.executable, "-m", "http.server", str(port)],
                                cwd=str(ARTIFACTS),
                                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except Exception as e:
        st.error(f"Failed to start HTTP server: {e}")
        return
    st.session_state.http_server_proc = proc
    st.success(f"Serving {ARTIFACTS} at http://localhost:{port}/ (PID={proc.pid})")

def _stop_http_server():
    p = st.session_state.http_server_proc
    if not _is_proc_running(p):
        st.session_state.http_server_proc = None
        st.info("No HTTP server running.")
        return
    try:
        p.terminate()
        time.sleep(0.1)
        if _is_proc_running(p):
            p.kill()
    except Exception as e:
        st.error(f"Failed to stop HTTP server: {e}")
    st.session_state.http_server_proc = None
    st.success("HTTP server stopped.")

def _ensure_log_view_html():
  
    view = ARTIFACTS / "log_view.html"
    content = r"""<!doctype html>
<html>
  <head>
    <meta charset="utf-8"/>
    <title>Pipeline log (live)</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
      body { background:#0e1117; color:#e6ffed; font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, "Roboto Mono", monospace; margin:12px; }
      h3 { color:#fff; }
      #log { white-space: pre-wrap; word-break: break-word; background:#0b0f13; padding:12px; border-radius:6px; height:80vh; overflow:auto; font-size:12px; border:1px solid #18202a; }
      .meta { color:#9ad9b7; font-size:13px; margin-bottom:8px; }
      .small { font-size:12px; color:#9aa6b2; }
      a { color:#60a5fa; }
    </style>
  </head>
  <body>
    <h3>Pipeline log (live)</h3>
    <div class="meta">This view fetches <code>pipeline.log</code> every 1s. <span class="small">If empty, ensure pipeline writes to local_data/artifacts/pipeline.log and HTTP server is running.</span></div>
    <div id="log">Loading...</div>
    <script>
      const url = "pipeline.log";
      const el = document.getElementById("log");
      let autoScroll = true;
      el.addEventListener('scroll', () => {
        const atBottom = (el.scrollHeight - el.clientHeight - el.scrollTop) < 30;
        autoScroll = atBottom;
      });

      async function fetchLog() {
        try {
          const resp = await fetch(url + "?_=" + Date.now(), {cache: "no-store"});
          if (!resp.ok) {
            el.textContent = "Could not fetch log (HTTP " + resp.status + ").";
            return;
          }
          const txt = await resp.text();
          const tail = txt.length > 200000 ? txt.slice(-200000) : txt;
          el.textContent = tail;
          if (autoScroll) {
            el.scrollTop = el.scrollHeight;
          }
        } catch (e) {
          el.textContent = "Error fetching log: " + e;
        }
      }

      fetchLog();
      setInterval(fetchLog, 1000);
    </script>
  </body>
</html>
"""
    try:
        view.write_text(content, encoding="utf8")
    except Exception as e:
        st.warning(f"Could not write log_view.html: {e}")
    return view


st.title("Healthcare Pipeline — Dashboard")
st.write("Run pipeline, watch logs, open EDA. (EDA & log served via a tiny HTTP server for reliable browser display.)")


with st.sidebar:
    st.header("Pipeline Controls")
    cfg = st.text_input("Config file", value="utils/project.cfg", key="cfg_input")
    force_fresh = st.checkbox("Force rebuild (--force-fresh)", value=False, key="force_fresh_cb")
    skip_anonymize = st.checkbox("Skip anonymization (--skip-anonymize)", value=True, key="skip_anonymize_cb")
    st.markdown("---")
    st.checkbox("Auto-start HTTP server (serves EDA/logs)", value=st.session_state.auto_start_http, key="auto_start_http_cb")
    st.session_state.auto_start_http = st.session_state.get("auto_start_http_cb", st.session_state.auto_start_http)
    st.caption("If you run pipeline externally, redirect stdout to:")
    st.code(f"python -m src.runnerfile --config {cfg} > {PIPELINE_LOG} 2>&1", language="bash")


c1, c2, c3 = st.columns([1,1,1])
with c1:
    if st.button("Run pipeline (local)", key="btn_run_local"):
        runner_cmd = ["-m", "src.runnerfile", "--config", cfg]
        if force_fresh:
            runner_cmd.append("--force-fresh")
        if skip_anonymize:
            runner_cmd.append("--skip-anonymize")
        _start_pipeline_local(runner_cmd, PIPELINE_LOG)
        if st.session_state.auto_start_http:
            _ensure_log_view_html()
            _start_http_server(port=8502)
with c2:
    if st.button("Stop pipeline", key="btn_stop_local"):
        _stop_pipeline_local()
with c3:
    if st.button("Start/ensure HTTP server (serve artifacts)", key="btn_start_http"):
        _ensure_log_view_html()
        _start_http_server(port=8502)


left, right = st.columns([1.0, 1.4])

with left:
    st.subheader("Live pipeline log")
    st.session_state.tail_lines = st.number_input("Lines to show (if HTTP server off)", min_value=50, max_value=5000, value=st.session_state.tail_lines, step=50, key="tail_lines_input")

    server_running = _is_proc_running(st.session_state.http_server_proc)
    log_view = ARTIFACTS / "log_view.html"
    if server_running and log_view.exists():
        iframe_url = "http://localhost:8502/log_view.html"
        st.info(f"HTTP server running — showing live log iframe: {iframe_url}")
        st.components.v1.iframe(src=iframe_url, height=1550, scrolling=True)
    else:
        if st.button("Refresh logs (fallback tail)", key="btn_refresh_tail"):
            pass
        txt = tail_file(PIPELINE_LOG, n=st.session_state.tail_lines)
        st.text_area("Pipeline logs (live)", value=txt, height=1550, key="pipeline_logs_area_fallback", disabled=True)

with right:
    st.subheader("Reports & Visuals")
    rr_files = sorted(glob.glob(str(READABLE_DIR / "*.csv")))
    st.caption(f"Found {len(rr_files)} readable report(s) in `{READABLE_DIR}`")

    drug_df, pres_df = None, None
    if pd is not None:
        for f in rr_files:
            name = Path(f).stem.lower()
            try:
                df = pd.read_csv(f)
            except Exception:
                df = None
            if df is None:
                continue
            if "drug" in name:
                drug_df = df
            if "prescriber" in name:
                pres_df = df

 
    k1, k2, k3 = st.columns(3)
    distinct_drugs = int(drug_df.shape[0]) if drug_df is not None else "-"
    distinct_pres = int(pres_df.shape[0]) if pres_df is not None else "-"
    total_claims = "-"
    if drug_df is not None and "prescriptions" in drug_df.columns:
        try:
            total_claims = int(drug_df["prescriptions"].sum())
        except Exception:
            total_claims = "-"
    k1.metric("Distinct drugs", distinct_drugs)
    k2.metric("Unique prescribers", distinct_pres)
    k3.metric("Total prescriptions (sample)", total_claims)

    st.session_state.topn = st.slider("Top N to display", 5, 50, st.session_state.topn, key="topn_slider_v2")


    if drug_df is not None and "prescriptions" in drug_df.columns:
        st.markdown("**Top drugs by prescriptions**")
        topd = drug_df.sort_values("prescriptions", ascending=False).head(st.session_state.topn)
        if px is not None and not topd.empty:

            fig = px.bar(
                topd,
                x=topd.columns[0],
                y="prescriptions",
                color="prescriptions",
                template="plotly_dark",
                labels={topd.columns[0]: "drug_brand_name", "prescriptions": "prescriptions"},
            )

            fig.update_traces(
                marker_line_color="#0e1117",
                marker_line_width=1.5,
                hovertemplate="<b>%{x}</b><br>Prescriptions: %{y:,}<extra></extra>",
            )
            fig.update_layout(
                height=560,
                margin=dict(t=30, b=60, l=60, r=30),
                xaxis_tickangle=-35,
                transition={"duration": 600, "easing": "cubic-in-out"},
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.table(topd.head(10))

  
        try:
            pie_rendered = False
            if Path(EDA_SUMMARY).exists():
                with open(EDA_SUMMARY, "r", encoding="utf8") as _f:
                    _js = json.load(_f)
                _top = _js.get("top_drugs", [])
                if _top and len(_top) > 0 and 'prescriptions' in _top[0]:
                    import pandas as _pd
                    _pdf = _pd.DataFrame(_top)
                    label_col = _pdf.columns[0]
                    if 'prescriptions' not in _pdf.columns:
                        _pdf = _pdf.rename(columns={_pdf.columns[-1]: 'prescriptions'})
                    if 'prescriptions' in _pdf.columns and px is not None:
                        try:
                            label_col = _pdf.columns[0]
                            fig_pie = px.pie(
                                _pdf.head(6),
                                names=label_col,
                                values="prescriptions",
                                title="Top 6 drugs share (pie)",
                                template="plotly_dark",
                                hole=0.18,
                            )
                            fig_pie.update_traces(
                                textinfo="percent+label",
                                textposition="inside",
                                hovertemplate="%{label}<br>Prescriptions: %{value:,} (%{percent})<extra></extra>",
                                marker=dict(line=dict(color="#0e1117", width=1.5)),
                            )
                            fig_pie.update_layout(
                                height=520,
                                margin=dict(t=20, b=20, l=20, r=20),
                                legend=dict(traceorder="normal", orientation="v", y=0.9, x=1.02),
                                transition={"duration": 600, "easing": "cubic-in-out"},
                            )

                            st.plotly_chart(fig_pie, use_container_width=False, width=460)
                            pie_rendered = True
                        except Exception:
                            pie_rendered = False
            if not pie_rendered:

                if px is not None and not topd.empty:
                    try:
                        _pdf = topd.head(6)
                        fig_pie = px.pie(
                            _pdf,
                            names=_pdf.columns[0],
                            values='prescriptions',
                            title='Top 6 drugs share (pie)',
                            template='plotly_dark',
                            hole=0.18,
                        )
                        fig_pie.update_traces(
                            textinfo="percent+label",
                            textposition="inside",
                            hovertemplate="%{label}<br>Prescriptions: %{value:,} (%{percent})<extra></extra>",
                            marker=dict(line=dict(color="#0e1117", width=1.5)),
                        )
                        fig_pie.update_layout(
                            height=520,
                            margin=dict(t=20, b=20, l=20, r=20),
                            legend=dict(traceorder="normal", orientation="v", y=0.9, x=1.02),
                            transition={"duration": 600, "easing": "cubic-in-out"},
                        )
                        st.plotly_chart(fig_pie, use_container_width=False, width=460)
                    except Exception as _e:
                        st.write("Could not render pie chart:", _e)
        except Exception as _e:
            st.write("Could not render pie chart:", _e)


    if pres_df is not None and pres_df.shape[0] > 0:
        st.markdown("**Top prescribers (by prescriptions)**")
        col_name = pres_df.columns[0]
        count_col = pres_df.columns[1] if pres_df.shape[1] > 1 else pres_df.columns[-1]
        topp = pres_df.sort_values(count_col, ascending=False).head(st.session_state.topn)
        if px is not None and not topp.empty:

            fig2 = px.bar(
                topp,
                x=col_name,
                y=count_col,
                template="plotly_dark",
                labels={col_name: "presc_id", count_col: "num_prescriptions"},
            )
            fig2.update_traces(
                marker_line_color="#0e1117",
                marker_line_width=1,
                hovertemplate="<b>%{x}</b><br>Prescriptions: %{y:,}<extra></extra>",
            )
            fig2.update_layout(
                height=520,
                margin=dict(t=30, b=70, l=60, r=30),
                xaxis_tickangle=-35,
                transition={"duration": 600, "easing": "cubic-in-out"},
            )
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.table(topp.head(10))


st.markdown("---")
st.subheader("EDA")
if EDA_HTML.exists():
    st.success("EDA found: " + str(EDA_HTML.name))
    st.write("Start the HTTP server and click the button below to open EDA in a new tab (works reliably).")
    col1, col2 = st.columns([1,1])
    with col1:
        if st.button("Start HTTP server and open EDA", key="btn_start_and_open_eda"):
            _ensure = _ensure_log_view_html()
            _start_http_server(port=8502)
            js = """<script>window.open("http://localhost:8502/eda_report.html", "_blank");</script>"""
            st.components.v1.html(js, height=10)
    with col2:
        if st.button("Open EDA in new tab (server must be running)", key="btn_open_eda_only"):
            js = """<script>window.open("http://localhost:8502/eda_report.html", "_blank");</script>"""
            st.components.v1.html(js, height=10)
    st.write("If the new tab doesn't open, start the HTTP server (Start/ensure HTTP server) and open this link manually:")
    st.markdown("[http://localhost:8502/eda_report.html](http://localhost:8502/eda_report.html)")
else:
    st.info("EDA not found. Run the pipeline to generate local_data/artifacts/eda_report.html")


st.markdown("---")
st.subheader("Download readable reports")
rr_files = sorted(glob.glob(str(READABLE_DIR / "*.csv")))
if rr_files:
    for f in rr_files:
        name = Path(f).name
        with st.expander(name, expanded=False):
            try:
                data = open(f, "rb").read()
                st.download_button(f"Download {name}", data=data, file_name=name, key=f"dl_{name}")
            except Exception as e:
                st.write("Could not read file:", e)
else:
    st.write("No readable report CSVs found.")

st.markdown("---")
st.caption("If you want Chrome to open links by default: make Chrome your OS default browser or right-click the link and choose 'Open with' → Chrome.")
