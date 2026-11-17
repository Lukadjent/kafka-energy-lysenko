from common import ROOT
import sys
import subprocess
sys.path.insert(0, str(ROOT))
import lab

destination = ROOT / '.runtime' / 'classes'
destination.mkdir(parents=True, exist_ok=True)
subprocess.run([str(lab.java_executable('javac.exe')), '-encoding', 'UTF-8', '-cp', str(lab.KAFKA / 'libs' / '*'),
    '-d', str(destination), str(ROOT / 'tools' / 'EnergyBenchmark.java')], check=True)

