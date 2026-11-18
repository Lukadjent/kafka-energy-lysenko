"""Run the cqlsh demonstration and Python workload against the local Cassandra node."""

import os
import subprocess
import sys

from serve import DIST, ROOT, RUNTIME, nodetool_command


def capture(args, label, destination, env):
    result = subprocess.run([sys.executable, *map(str, args)], cwd=ROOT, env=env,
                            capture_output=True, text=True, encoding="utf-8")
    log = f"> {label}\n{result.stdout}"
    destination.write_text(log, encoding="utf-8")
    print(log, end="")
    if result.stderr:
        print(result.stderr, end="", file=sys.stderr)
    if result.returncode:
        raise SystemExit(result.returncode)


def main():
    evidence = ROOT / "evidence"
    evidence.mkdir(exist_ok=True)
    env = dict(os.environ, CQLSH_NO_BUNDLED="1", CQL_HISTORY=str(RUNTIME / "cqlsh.history"),
               PYTHONIOENCODING="utf-8", PYTHONWARNINGS="ignore::DeprecationWarning")
    local_packages = RUNTIME / "site-packages"
    env["PYTHONPATH"] = os.pathsep.join(filter(None, (str(local_packages), env.get("PYTHONPATH"))))
    cqlsh = DIST / "bin" / "cqlsh.py"
    # SOURCE runs the schema in the same session before the cqlsh demonstration.
    script = RUNTIME / "cqlsh-run.cql"
    script.write_text("SOURCE 'schema.cql';\n" + (ROOT / "demo.cql").read_text(encoding="utf-8"),
                      encoding="utf-8")
    capture([cqlsh, "127.0.0.1", "9042", "-f", script],
            "cqlsh 127.0.0.1 9042 -f .runtime/cqlsh-run.cql", evidence / "cqlsh.txt", env)
    capture(["-u", "hydro.py"], "python hydro.py", evidence / "python.txt", env)
    with (evidence / "nodetool.txt").open("w", encoding="utf-8") as log:
        commands = [["status"], ["flush", "dnipro_hydro_lysenko"]]
        commands += [["tablehistograms", "dnipro_hydro_lysenko", table]
                     for table in ("turbine_readings", "seasonal_by_station", "seasonal_by_season")]
        for args in commands:
            result = subprocess.run(nodetool_command(args), cwd=DIST, check=True,
                                    capture_output=True, text=True, errors="replace")
            log.write(f"> nodetool {' '.join(args)}\n{result.stdout}\n")


if __name__ == "__main__":
    main()
