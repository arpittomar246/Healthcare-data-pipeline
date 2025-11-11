import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import subprocess
import threading
import sys
import os
from pathlib import Path
import signal
import platform
import webbrowser
import time

PROJECT_ROOT = Path.cwd()
ARTIFACTS_DIR = PROJECT_ROOT / "local_data" / "artifacts"
RUN_CMD_BASE = [sys.executable, "-m", "src.runnerfile"]

class PipelineGUI(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Healthcare Pipeline Launcher")
        self.geometry("900x600")
        self.proc = None
        self._create_widgets()

    def _create_widgets(self):
        frm = ttk.Frame(self)
        frm.pack(fill="x", padx=8, pady=6)


        ttk.Label(frm, text="Config:").grid(row=0, column=0, sticky="w")
        self.config_var = tk.StringVar(value="utils/project.cfg")
        self.config_entry = ttk.Entry(frm, textvariable=self.config_var, width=50)
        self.config_entry.grid(row=0, column=1, sticky="w")
        ttk.Button(frm, text="Browse", command=self.browse_config).grid(row=0, column=2, padx=4)


        self.force_var = tk.BooleanVar(value=False)
        self.skip_var = tk.BooleanVar(value=False)
        self.openui_var = tk.BooleanVar(value=True)
        chk_frame = ttk.Frame(frm)
        chk_frame.grid(row=1, column=0, columnspan=3, sticky="w", pady=(6,0))
        ttk.Checkbutton(chk_frame, text="--force-fresh", variable=self.force_var).pack(side="left", padx=6)
        ttk.Checkbutton(chk_frame, text="--skip-anonymize", variable=self.skip_var).pack(side="left", padx=6)
        ttk.Checkbutton(chk_frame, text="--open-ui", variable=self.openui_var).pack(side="left", padx=6)

        btn_frame = ttk.Frame(frm)
        btn_frame.grid(row=2, column=0, columnspan=3, sticky="w", pady=(8,4))
        self.run_btn = ttk.Button(btn_frame, text="Run Pipeline", command=self.start_pipeline, width=16)
        self.run_btn.pack(side="left", padx=6)
        self.stop_btn = ttk.Button(btn_frame, text="Stop", command=self.stop_pipeline, state="disabled", width=10)
        self.stop_btn.pack(side="left", padx=6)
        ttk.Button(btn_frame, text="Open Artifacts Folder", command=self.open_artifacts).pack(side="left", padx=6)

        self.text = tk.Text(self, wrap="none", height=30, bg="black", fg="white")
        self.text.pack(fill="both", expand=True, padx=6, pady=(4,6))
        self.text.configure(state="disabled")

        ysb = ttk.Scrollbar(self, orient="vertical", command=self.text.yview)
        self.text['yscrollcommand'] = ysb.set
        ysb.place(relx=1.0, rely=0.1, relheight=0.8, anchor="ne")

    def browse_config(self):
        p = filedialog.askopenfilename(title="Select config file", initialdir=str(PROJECT_ROOT), filetypes=[("cfg","*.cfg"),("all","*.*")])
        if p:
            self.config_var.set(str(Path(p).relative_to(PROJECT_ROOT)))

    def start_pipeline(self):
        if self.proc and self.proc.poll() is None:
            messagebox.showinfo("Already running", "Pipeline is already running.")
            return
        cmd = RUN_CMD_BASE.copy()
        cmd += ["--config", self.config_var.get()]
        if self.force_var.get():
            cmd.append("--force-fresh")
        if self.skip_var.get():
            cmd.append("--skip-anonymize")
        if self.openui_var.get():
            cmd.append("--open-ui")

        self.append_text(f"Starting pipeline: {' '.join(cmd)}\n\n")
    
        try:
            self.proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, bufsize=1, universal_newlines=True)
        except Exception as e:
            messagebox.showerror("Failed", f"Failed to start pipeline: {e}")
            return

        self.run_btn.config(state="disabled")
        self.stop_btn.config(state="normal")


        t = threading.Thread(target=self._read_output, daemon=True)
        t.start()

   
        tw = threading.Thread(target=self._watch_process, daemon=True)
        tw.start()

    def _read_output(self):
        try:
            for line in self.proc.stdout:
                self.append_text(line)
        except Exception as e:
            self.append_text(f"\n[Error reading output]: {e}\n")

    def _watch_process(self):
        rc = self.proc.wait()
        self.append_text(f"\nProcess exited with return code {rc}\n")
        self.run_btn.config(state="normal")
        self.stop_btn.config(state="disabled")
    
        if self.openui_var.get():
            time.sleep(0.5)
            self.open_artifacts()
    
            eda = ARTIFACTS_DIR / "eda_report.html"
            if eda.exists():
                try:
                    webbrowser.open_new_tab(str(eda.resolve().as_uri()))
                except Exception:
                    try:
                        webbrowser.open_new_tab(str(eda.resolve()))
                    except Exception:
                        pass

    def stop_pipeline(self):
        if self.proc and self.proc.poll() is None:
            try:
                if platform.system() == "Windows":
                    subprocess.run(["taskkill", "/F", "/T", "/PID", str(self.proc.pid)], check=False)
                else:
                    os.killpg(os.getpgid(self.proc.pid), signal.SIGTERM)
                self.append_text("\nSent termination signal to pipeline.\n")
            except Exception as e:
                self.append_text(f"\nFailed to terminate process: {e}\n")
        else:
            self.append_text("No running process.\n")

    def open_artifacts(self):
        path = ARTIFACTS_DIR
        if not path.exists():
            messagebox.showinfo("Not found", f"Artifacts folder not found: {path}")
            return
        try:
            if platform.system() == "Windows":
                os.startfile(str(path.resolve()))
            elif platform.system() == "Darwin":
                subprocess.run(["open", str(path.resolve())], check=False)
            else:
                subprocess.run(["xdg-open", str(path.resolve())], check=False)
        except Exception as e:
            messagebox.showerror("Open failed", f"Could not open artifacts folder: {e}")

    def append_text(self, s):
        self.text.configure(state="normal")
        self.text.insert("end", s)
        self.text.see("end")
        self.text.configure(state="disabled")

if __name__ == "__main__":
    app = PipelineGUI()
    app.mainloop()
