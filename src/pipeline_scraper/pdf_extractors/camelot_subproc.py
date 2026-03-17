# src/pipeline_scraper/pdf_extractors/camelot_subproc.py
from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional


def _build_inline_camelot_code() -> str:
    """
    Child program (executed with `python -c`):

      • imports camelot ONLY in the child process
      • reconfigures stdout to UTF-8 when possible (Windows-safe)
      • calls camelot.read_pdf with suppress_stdout=True
      • prints ASCII-safe JSON (ensure_ascii=True) for guaranteed decoding

    Parent parses child's STDOUT as JSON. On failures, child prints a structured JSON error.
    """
    return r"""
import json, sys
# Prefer UTF-8 for stdout; if unavailable, at least avoid crashing on non-ASCII
try:
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
except Exception:
    pass

try:
    import camelot
except Exception as e:
    print(json.dumps({"error":"import_camelot_failed","detail":str(e)}, ensure_ascii=True))
    raise

pdf_path = sys.argv[1]
pages    = sys.argv[2]
flavor   = sys.argv[3]        # 'lattice' | 'stream' | 'network' | 'hybrid'
backend  = sys.argv[4]        # 'pdfium' | 'poppler' | 'ghostscript'
use_fb   = (sys.argv[5].lower() == "true")

extra = {}
if len(sys.argv) > 6 and sys.argv[6] != "":
    try:
        extra = json.loads(sys.argv[6])
    except Exception as e:
        print(json.dumps({"error":"bad_kwargs_json","detail":str(e)}, ensure_ascii=True))
        raise

try:
    tables = camelot.read_pdf(
        pdf_path,
        pages=pages,
        flavor=flavor,
        backend=backend,
        use_fallback=use_fb,
        suppress_stdout=True,  # keep child STDOUT clean for JSON
        **extra
    )
    out = []
    for t in tables:
        out.append({
            "page": getattr(t, "page", None),
            "parsing_report": getattr(t, "parsing_report", None),
            "data": getattr(t, "df", None).values.tolist() if getattr(t, "df", None) is not None else None
        })
    # ASCII-safe JSON to avoid console encoding pitfalls on Windows
    print(json.dumps({"ok": True, "tables": out}, ensure_ascii=True))
except Exception as e:
    print(json.dumps({"error":"camelot_read_failed","detail":str(e)}, ensure_ascii=True))
    raise
"""


def camelot_read_tables_subprocess(
    pdf_path: str | Path,
    *,
    pages: str = "all",
    flavor: str = "lattice",
    backend: str = "pdfium",
    use_fallback: bool = False,
    extra_kwargs: Optional[Dict[str, Any]] = None,
    tmp_root: Optional[Path] = None,
    timeout: int = 180,
) -> List[Dict[str, Any]]:
    """
    Run Camelot in a short-lived subprocess and return a list of table dicts:
      [{'page': int, 'parsing_report': {...}, 'data': [[...], ...]}, ...]

    Args:
      pdf_path: Path to a local PDF file (must exist on disk).
      pages: Camelot pages spec ('1', '1,3-4', 'all', '1-end', etc.)
      flavor: 'lattice' | 'stream' | 'network' | 'hybrid'
      backend: 'pdfium' (default) | 'poppler' | 'ghostscript'
      use_fallback: Allow Camelot to try alternative backends if needed.
      extra_kwargs: Additional read_pdf kwargs (JSON-serializable), e.g.:
                    {'line_scale': 50, 'strip_text': '\n', 'shift_text': ['l','t']}
      tmp_root: Optional base temp dir for the child (sets TMP/TEMP in env).
      timeout: Child process timeout (seconds).

    Returns:
      List of dicts describing tables, safe to convert to DataFrames in the parent.

    Raises:
      RuntimeError with detailed STDOUT/STDERR on child failure.
    """
    pdf_path = str(pdf_path)
    extra_json = json.dumps(extra_kwargs or {}, ensure_ascii=False)

    code = _build_inline_camelot_code()
    cmd = [
        sys.executable, "-c", code,
        pdf_path, pages, flavor, backend,
        "true" if use_fallback else "false",
        extra_json
    ]

    # Ensure child's stdio is UTF-8; optionally steer TEMP/TMP for its temp work
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "UTF-8"
    if tmp_root:
        tmp_root = Path(tmp_root)
        tmp_root.mkdir(parents=True, exist_ok=True)
        env["TMP"] = str(tmp_root)
        env["TEMP"] = str(tmp_root)

    proc = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=timeout,
        env=env,
    )

    if proc.returncode != 0:
        # Try to parse a structured error payload from STDOUT
        try:
            payload = json.loads(proc.stdout or "{}")
            if "error" in payload:
                raise RuntimeError(f"[Camelot child] {payload['error']}: {payload.get('detail')}")
        except Exception:
            pass
        # Fallback: include STDOUT/STDERR for debugging
        raise RuntimeError(
            f"[Camelot child] Non-zero exit {proc.returncode}\n"
            f"STDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}"
        )

    payload = json.loads(proc.stdout or "{}")
    if not payload.get("ok"):
        raise RuntimeError(f"[Camelot child] returned error payload: {payload}")

    return payload.get("tables", [])