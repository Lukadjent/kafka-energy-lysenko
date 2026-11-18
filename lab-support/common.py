"""Shared, local-only runtime and evidence helpers for laboratories 3-5."""
from pathlib import Path
import json
import sys

SUPPORT = Path(__file__).resolve().parent
WORKSPACE = SUPPORT.parent
RUNTIME = SUPPORT / '.runtime'
for directory in (WORKSPACE/'kafka-energy-lysenko/.runtime/python-packages',
                  WORKSPACE/'lab1-kafka-lysenko/.runtime/python-packages',
                  WORKSPACE/'lab2-cassandra-lysenko/.runtime/site-packages',
                  RUNTIME/'site-packages'):
    sys.path.insert(0, str(directory))
sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')
CASSANDRA_DIST = WORKSPACE/'lab2-cassandra-lysenko/.runtime/apache-cassandra-4.0.21'
JAVA11 = next((WORKSPACE/'lab2-cassandra-lysenko/.runtime').glob('jdk-11*/bin/java.exe'), WORKSPACE/'lab2-cassandra-lysenko/.runtime/jdk-11/bin/java.exe')
JDK17 = next((WORKSPACE/'kafka-energy-lysenko/.runtime').glob('jdk-17*'), WORKSPACE/'kafka-energy-lysenko/.runtime/jdk-17')
KAFKA = WORKSPACE/'kafka-energy-lysenko/.runtime/kafka_2.13-3.7.1'
CONTACT_POINTS = ['127.0.0.11','127.0.0.12','127.0.0.13']
BOOTSTRAP = '127.0.0.1:9092'
STUDENT = {'name':'Лисенко Лука Сергійович', 'group':'ТР-61МП', 'variant':'3Б', 'list_number':13}

def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')

def read_json(path):
    return json.loads(Path(path).read_text(encoding='utf-8'))
