"""Launch the Apache Cassandra 4.0 distribution locally on Windows with Java 11."""

import os
from pathlib import Path
import shutil
import subprocess
import sys


ROOT = Path(__file__).resolve().parent
RUNTIME = ROOT / ".runtime"
DIST = RUNTIME / "apache-cassandra-4.0.21"


def java_path():
    if os.environ.get("JAVA_HOME"):
        configured = Path(os.environ["JAVA_HOME"]) / "bin" / "java.exe"
        if configured.is_file():
            return str(configured)
    java = next(RUNTIME.glob("jdk-11*/bin/java.exe"), None) or shutil.which("java")
    if not java:
        raise SystemExit("Set JAVA_HOME to a Java 11 installation.")
    return str(java)


def java_module_flags(role):
    options = (DIST / "conf" / f"jvm11-{role}.options").read_text(encoding="utf-8")
    return [flag for line in options.splitlines()
            if line.startswith(("--add-", "-Dio.netty.")) for flag in line.split()]


def server_command():
    return [
        java_path(), "-Xms512m", "-Xmx512m", "-XX:ActiveProcessorCount=4",
        "-XX:+ExitOnOutOfMemoryError", "-Djava.io.tmpdir=../../data/jvm-tmp",
        "-Dcassandra.storagedir=../../data/node40",
        "-Dcassandra.logdir=../../logs", "-Dcassandra.jmx.local.port=7199",
        "-Dcassandra-foreground=yes", "-javaagent:lib/jamm-0.3.2.jar",
        *java_module_flags("server"), "-cp", "conf;lib/*", "org.apache.cassandra.service.CassandraDaemon",
    ]


def nodetool_command(args):
    return [java_path(), *java_module_flags("clients"), "-Dcassandra.storagedir=../../data/node40",
            "-cp", "conf;lib/*", "org.apache.cassandra.tools.NodeTool",
            "-h", "127.0.0.1", "-p", "7199", *args]


def prepare_server():
    (ROOT / "data" / "jvm-tmp").mkdir(parents=True, exist_ok=True)
    (ROOT / "logs").mkdir(exist_ok=True)


if __name__ == "__main__":
    prepare_server()
    command = nodetool_command(sys.argv[2:]) if sys.argv[1:2] == ["nodetool"] else server_command()
    raise SystemExit(subprocess.call(command, cwd=DIST))
