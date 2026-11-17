"""Download portable official distributions into this project, without global installs."""
from __future__ import annotations

import hashlib
import json
import re
import tarfile
import urllib.request
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RUNTIME = ROOT / ".runtime"


def fetch(url: str) -> bytes:
    request = urllib.request.Request(url, headers={"User-Agent": "KafkaEnergyLab/1.0"})
    with urllib.request.urlopen(request, timeout=120) as response:
        return response.read()


def download(url: str, path: Path, algorithm: str, expected: str) -> None:
    if path.exists() and hashlib.new(algorithm, path.read_bytes()).hexdigest() == expected:
        print(f"Verified cached {path.name}", flush=True)
        return
    print(f"Downloading {url}", flush=True)
    request = urllib.request.Request(url, headers={"User-Agent": "KafkaEnergyLab/1.0"})
    partial = path.with_suffix(path.suffix + ".part")
    digest = hashlib.new(algorithm)
    with urllib.request.urlopen(request, timeout=120) as response, partial.open("wb") as out:
        while chunk := response.read(1024 * 1024):
            digest.update(chunk)
            out.write(chunk)
    if digest.hexdigest().lower() != expected.lower():
        raise RuntimeError(f"Checksum mismatch for {path.name}; archive was not extracted")
    partial.replace(path)
    print(f"Verified {algorithm}: {path.name}", flush=True)


def safe_zip(archive: Path, destination: Path) -> None:
    base = destination.resolve()
    with zipfile.ZipFile(archive) as zipped:
        for name in zipped.namelist():
            if not (base / name).resolve().is_relative_to(base):
                raise RuntimeError(f"Unsafe archive path: {name}")
        zipped.extractall(base)


def main() -> None:
    RUNTIME.mkdir(exist_ok=True)
    manifest = {}
    kafka_base = "https://archive.apache.org/dist/kafka/3.7.1/kafka_2.13-3.7.1.tgz"
    checksum_text = fetch(kafka_base + ".sha512").decode()
    checksum = re.sub(r"\s", "", checksum_text.split(":")[-1]).lower()
    if not re.fullmatch(r"[0-9a-f]{128}", checksum):
        raise RuntimeError(f"Unrecognized Apache checksum: {checksum_text}")
    archive = RUNTIME / "kafka_2.13-3.7.1.tgz"
    download(kafka_base, archive, "sha512", checksum)
    if not (RUNTIME / "kafka_2.13-3.7.1" / "libs").exists():
        with tarfile.open(archive) as tar:
            tar.extractall(RUNTIME, filter="data")
    manifest["kafka"] = {"version": "3.7.1", "url": kafka_base, "sha512": checksum}

    # Microsoft's alias always points to an official supported Java 17 Windows build.
    java_url = "https://aka.ms/download-jdk/microsoft-jdk-17-windows-x64.zip"
    checksum_url = java_url + ".sha256sum.txt"
    java_checksum_text = fetch(checksum_url).decode()
    match = re.search(r"\b[0-9a-fA-F]{64}\b", java_checksum_text)
    if not match:
        raise RuntimeError("Cannot read Microsoft's SHA256 checksum")
    java_checksum = match.group().lower()
    java_archive = RUNTIME / "microsoft-jdk-17-windows-x64.zip"
    download(java_url, java_archive, "sha256", java_checksum)
    if not list(RUNTIME.glob("jdk-*/bin/java.exe")):
        safe_zip(java_archive, RUNTIME)
    manifest["java"] = {"url": java_url, "sha256": java_checksum}

    wheel_info = json.loads(fetch("https://pypi.org/pypi/kafka-python/2.2.9/json"))
    wheel = next(item for item in wheel_info["urls"] if item["filename"].endswith(".whl"))
    wheel_path = RUNTIME / wheel["filename"]
    download(wheel["url"], wheel_path, "sha256", wheel["digests"]["sha256"])
    packages = RUNTIME / "python-packages"
    packages.mkdir(exist_ok=True)
    safe_zip(wheel_path, packages)
    manifest["kafka-python"] = {"version": "2.2.9", "url": wheel["url"], "sha256": wheel["digests"]["sha256"]}
    (RUNTIME / "downloads.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print("Portable dependencies ready. No system settings changed.", flush=True)


if __name__ == "__main__":
    main()

