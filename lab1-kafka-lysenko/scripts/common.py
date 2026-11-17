from pathlib import Path
import json
import sys

ROOT = Path(__file__).resolve().parents[1]
PACKAGES = ROOT / ".runtime" / "python-packages"
sys.path.insert(0, str(PACKAGES))
CONFIG = json.loads((ROOT / "config" / "student.json").read_text(encoding="utf-8"))

def configure_stdout():
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8")

