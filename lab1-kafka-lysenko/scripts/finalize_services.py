import json
import sys
from common import ROOT, CONFIG, configure_stdout
sys.path.insert(0, str(ROOT))
import lab
import subprocess


def main():
    configure_stdout()
    evidence = ROOT / "evidence"
    results = json.loads((evidence / "results.json").read_text(encoding="utf-8"))
    text = []
    for item in results:
        if item["config"]["family"] == "retention":
            topic = item["config"]["topic"]
            result = lab.run_tool("configs", ["--bootstrap-server", CONFIG["bootstrap_servers"], "--entity-type", "topics", "--entity-name", topic, "--describe"], capture_output=True, timeout=60)
            text.append("$ python lab.py configs --describe --entity-name " + topic + "\n" + result.stdout + result.stderr)
            assert str(item["config"]["retention_ms"]) in result.stdout
    result = lab.run_tool("topics", ["--bootstrap-server", CONFIG["bootstrap_servers"], "--describe", "--topic", CONFIG["topic"]], capture_output=True, timeout=60)
    assert "PartitionCount: 4" in result.stdout
    text.append(result.stdout)
    (evidence / "broker-config-verification.txt").write_text("\n".join(text), encoding="utf-8")
    environment_path = evidence / "environment.json"
    environment = json.loads(environment_path.read_text(encoding="utf-8"))
    java = subprocess.run([str(lab.java_executable()), "-version"], capture_output=True, text=True, encoding="utf-8", check=True)
    environment["java_version"] = java.stdout + java.stderr
    environment["disk_model"] = "KINGSTON SA400S37240G"
    environment["disk_interface"] = "SATA"
    environment_path.write_text(json.dumps(environment, indent=2), encoding="utf-8")
    result = subprocess.run([sys.executable, "-X", "utf8", str(ROOT / "lab.py"), "stop"], capture_output=True, text=True, encoding="utf-8", check=True)
    (evidence / "shutdown.txt").write_text("$ python lab.py stop\n" + result.stdout + result.stderr, encoding="utf-8")
    print(result.stdout)


if __name__ == "__main__":
    main()
