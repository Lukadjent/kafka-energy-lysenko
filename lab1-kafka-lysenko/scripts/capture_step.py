"""Execute an auxiliary study and preserve its unmodified console output."""
import argparse
import subprocess
import sys

from common import ROOT, configure_stdout


def main():
    configure_stdout()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("study", choices=("serialization", "warehouse"))
    options = parser.parse_args()
    script = ROOT / "scripts" / f"{options.study}_study.py"
    result = subprocess.run([sys.executable, "-X", "utf8", str(script)], cwd=ROOT,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, encoding="utf-8")
    (ROOT / "evidence" / f"{options.study}-stdout.txt").write_text(result.stdout, encoding="utf-8")
    print(result.stdout, end="", flush=True)
    raise SystemExit(result.returncode)


if __name__ == "__main__":
    main()
