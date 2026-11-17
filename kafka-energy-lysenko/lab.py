"""Portable Windows launcher for the Kafka 3.7.1 energy lab."""
from __future__ import annotations

import argparse
import json
import os
import secrets
import socket
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "scripts"))
from common import CONFIG, configure_stdout

RUNTIME = ROOT / ".runtime"
LOGS = ROOT / "logs"
DATA = ROOT / "data"
KAFKA = RUNTIME / "kafka_2.13-3.7.1"
STATE = RUNTIME / "services.json"
TOPICS = {
    CONFIG["topic"]: 3,
    "power-telemetry-lysenko": 3,
    "grid-alerts-lysenko": 2,
    "energy-consumption-lysenko": 4,
    "weather-data-lysenko": 2,
}
CLASSES = {
    "topics": "org.apache.kafka.tools.TopicCommand",
    "configs": "kafka.admin.ConfigCommand",
    "groups": "kafka.admin.ConsumerGroupCommand",
    "api-versions": "kafka.admin.BrokerApiVersionsCommand",
    "log-dirs": "org.apache.kafka.tools.LogDirsCommand",
    "console-producer": "kafka.tools.ConsoleProducer",
    "console-consumer": "kafka.tools.ConsoleConsumer",
    "producer-perf": "org.apache.kafka.tools.ProducerPerformance",
    "consumer-perf": "org.apache.kafka.tools.ConsumerPerformance",
}


def java_executable(name: str = "java.exe") -> Path:
    portable = sorted(RUNTIME.glob(f"jdk-*/bin/{name}"))
    if portable:
        return portable[-1]
    if os.environ.get("JAVA_HOME"):
        path = Path(os.environ["JAVA_HOME"]) / "bin" / name
        if path.exists():
            return path
    raise RuntimeError("Java 17 not found. Run scripts/download_dependencies.py first.")


def java_command(main_class: str, args: list[str], service: str = "tools") -> list[str]:
    if not (KAFKA / "libs").exists():
        raise RuntimeError("Kafka libraries not found. Run scripts/download_dependencies.py first.")
    LOGS.mkdir(exist_ok=True)
    classpath = str(KAFKA / "libs" / "*") + os.pathsep + str(RUNTIME / "launcher")
    return [str(java_executable()), "-Xms128m", "-Xmx512m", "-Dfile.encoding=UTF-8", "-Duser.language=en", "-Duser.country=US",
            f"-Dlog4j.configuration={(ROOT / 'config' / 'log4j.properties').as_uri()}",
            f"-Dlab.log.file={(LOGS / (service + '.log')).as_posix()}",
            "-cp", classpath, main_class, *args]


def run_tool(tool: str, args: list[str], **kwargs) -> subprocess.CompletedProcess:
    command = java_command(CLASSES[tool], args)
    try:
        return subprocess.run(command, cwd=ROOT, encoding="utf-8", errors="replace", check=True, **kwargs)
    except subprocess.CalledProcessError as error:
        if error.stdout:
            print(error.stdout, file=sys.stderr)
        if error.stderr:
            print(error.stderr, file=sys.stderr)
        raise


def port_open(port: int) -> bool:
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=0.25):
            return True
    except OSError:
        return False


def wait_port(port: int, expected: bool, timeout: int = 60, process=None) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if port_open(port) == expected:
            return
        if process is not None and process.poll() is not None:
            raise RuntimeError(f"Service exited with {process.returncode}; see logs/")
        time.sleep(0.25)
    raise TimeoutError(f"Port {port} did not become {'open' if expected else 'closed'}")


def start() -> None:
    if any(port_open(port) for port in (2181, 9092, 12181, 19092)):
        raise RuntimeError("A lab port is already in use. Inspect lab.py status before starting.")
    if STATE.exists():
        raise RuntimeError("A previous services.json exists. Run lab.py stop to reconcile it first.")
    DATA.mkdir(exist_ok=True)
    LOGS.mkdir(exist_ok=True)
    (RUNTIME / "launcher").mkdir(parents=True, exist_ok=True)
    subprocess.run([str(java_executable("javac.exe")), "-encoding", "UTF-8", "-d",
                    str(RUNTIME / "launcher"), str(ROOT / "tools" / "LabService.java")], check=True)
    server = (ROOT / "config" / "server.properties").read_text(encoding="utf-8")
    server += f"\nlog.dirs={(DATA / 'kafka-logs').as_posix()}\n"
    # Java Properties.load(InputStream) expects ISO-8859-1 plus Unicode escapes.
    (RUNTIME / "server.properties").write_text(server.encode("ascii", "backslashreplace").decode("ascii"), encoding="ascii")
    zoo = (ROOT / "config" / "zookeeper.properties").read_text(encoding="utf-8")
    zoo += f"\ndataDir={(DATA / 'zookeeper').as_posix()}\n"
    (RUNTIME / "zookeeper.properties").write_text(zoo.encode("ascii", "backslashreplace").decode("ascii"), encoding="ascii")
    services = []
    try:
        for name, cls, port, control in (
            ("zookeeper", "org.apache.zookeeper.server.quorum.QuorumPeerMain", 2181, 12181),
            ("kafka", "kafka.Kafka", 9092, 19092),
        ):
            token = secrets.token_hex(24)
            config_name = "server" if name == "kafka" else "zookeeper"
            args = [cls, str(control), token, str(RUNTIME / f"{config_name}.properties")]
            command = java_command("LabService", args, name)
            flags = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
            with (LOGS / f"{name}.console.log").open("a", encoding="utf-8") as output:
                process = subprocess.Popen(command, cwd=ROOT, stdout=output, stderr=subprocess.STDOUT, creationflags=flags)
            services.append({"name": name, "pid": process.pid, "port": port, "control": control, "token": token})
            STATE.write_text(json.dumps(services), encoding="utf-8")
            wait_port(port, True, process=process)
            print(f"STARTED {name} pid={process.pid} 127.0.0.1:{port}", flush=True)
        # A listening socket alone does not prove the broker is ready for metadata requests.
        run_tool("topics", ["--bootstrap-server", CONFIG["bootstrap_servers"], "--list"], capture_output=True, timeout=60)
        print("Kafka 3.7.1 + ZooKeeper ready", flush=True)
    except Exception:
        stop()
        raise


def stop() -> None:
    if not STATE.exists():
        print("No managed lab services")
        return
    services = json.loads(STATE.read_text(encoding="utf-8"))
    for service in reversed(services):
        if port_open(service["control"]):
            with socket.create_connection(("127.0.0.1", service["control"]), timeout=5) as control:
                control.sendall((service["token"] + "\n").encode())
                if control.recv(128).strip() != b"stopping":
                    raise RuntimeError(f"Shutdown rejected for {service['name']}")
        elif port_open(service["port"]):
            raise RuntimeError(f"Cannot control {service['name']}; refusing to stop an unknown process")
        wait_port(service["port"], False, timeout=60)
        wait_port(service["control"], False, timeout=60)
        print(f"STOPPED {service['name']} (JVM shutdown hooks)", flush=True)
    STATE.unlink()


def create_topics() -> None:
    for topic, partitions in TOPICS.items():
        run_tool("topics", ["--bootstrap-server", CONFIG["bootstrap_servers"], "--create", "--if-not-exists",
                           "--topic", topic, "--partitions", str(partitions), "--replication-factor", "1",
                           "--config", "retention.ms=604800000"])
    run_tool("topics", ["--bootstrap-server", CONFIG["bootstrap_servers"], "--describe", "--topic", CONFIG["topic"]])


def status() -> None:
    print(f"PROJECT={ROOT.name} STUDENT={CONFIG['student']} GROUP={CONFIG['group']}")
    print(f"ZooKeeper 2181: {'OPEN' if port_open(2181) else 'CLOSED'}")
    print(f"Kafka 9092: {'OPEN' if port_open(9092) else 'CLOSED'}")
    if port_open(9092):
        run_tool("topics", ["--bootstrap-server", CONFIG["bootstrap_servers"], "--list"])


def main() -> None:
    configure_stdout()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["start", "stop", "status", "create-topics", *CLASSES])
    args, rest = parser.parse_known_args()
    if args.command in CLASSES:
        run_tool(args.command, rest)
    else:
        if rest:
            parser.error("Unexpected arguments: " + " ".join(rest))
        {"start": start, "stop": stop, "status": status, "create-topics": create_topics}[args.command]()


if __name__ == "__main__":
    main()

