"""Create a read-only HTML viewer of actual, unmodified command transcripts."""
from __future__ import annotations

import html
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
EVIDENCE = ROOT / "evidence"
PAGES = (
    ("01-topics", "Створення топіків", ["01-topics.txt"]),
    ("02-console", "Консольні producer і consumer", ["02-console-producer.txt", "03-console-consumer.txt"]),
    ("03-python-producer", "Python producer", ["04-python-producer.txt"]),
    ("04-python-consumer", "Python consumer", ["05-python-consumer.txt"]),
    ("05-offsets", "Offsets і повторний запуск", ["06-consumer-group.txt", "07-consumer-resume.txt"]),
    ("06-performance", "Тест продуктивності", ["09-producer-performance.txt", "10-consumer-performance.txt"]),
    ("07-shutdown", "Коректна зупинка", ["15-shutdown.txt"]),
)


def main():
    summary = json.loads((EVIDENCE / "verification.json").read_text(encoding="utf-8"))
    for slug, title, names in PAGES:
        panels = []
        for name in names:
            path = EVIDENCE / name
            if not path.exists():
                continue
            text = path.read_text(encoding="utf-8")
            lines = text.splitlines()
            # Full originals remain linked below; excerpts are explicitly labelled.
            if name == "04-python-producer.txt":
                lines = lines[:3] + [line for line in lines if line.startswith("SENT") or line.startswith("PRODUCER_SUMMARY")]
                note = "Фрагмент журналу: службові рядки й підтвердження всіх 20 записів. JSON доступний у повному файлі."
            elif name == "05-python-consumer.txt":
                lines = lines[:3] + [line for line in lines if line.startswith(("RECEIVED", "REJECTED", "CONSUMER_SUMMARY"))]
                note = "Фрагмент журналу: отримані записи, відхилені дані й підсумок. Повний аналіз доступний у файлі."
            else:
                note = "Повний вивід команди без змін."
            panels.append(f'<section><h2>{html.escape(name)}</h2><p>{html.escape(note)} <a href="{name}">Повний журнал</a></p><pre>{html.escape(chr(10).join(lines))}</pre></section>')
        page = '''<!doctype html><html lang="uk"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>''' + html.escape(title) + ''' | Kafka Lysenko</title><style>
*{box-sizing:border-box}body{margin:0;padding:26px 32px;background:#f3f5f8;color:#101828;font-family:Arial,sans-serif}
header{margin-bottom:22px}h1{font-size:25px;margin:0 0 10px}header p{margin:5px 0;font-size:15px;color:#344054}
section{background:white;border:1px solid #d0d5dd;border-radius:8px;margin:15px 0;padding:18px 22px}
h2{font-size:17px;margin:0 0 8px}section p{font-size:13px;color:#475467;margin:5px 0 12px}a{color:#175cd3}
pre{margin:0;font:14px/1.6 Consolas,'Courier New',monospace;white-space:pre-wrap;overflow-wrap:anywhere}
</style><header><h1>''' + html.escape(title) + '''</h1><p>Lysenko Luka · ТР-61МП · kafka-energy-lysenko</p><p>Знімок переглядача фактичних логів виконання · Kafka 3.7.1 + ZooKeeper · run ''' + summary["run_id"] + '''</p></header>''' + "".join(panels) + "</html>"
        (EVIDENCE / (slug + ".html")).write_text(page, encoding="utf-8")
    (EVIDENCE / "index.html").write_text('<meta charset="utf-8"><h1>Kafka Lysenko · фактичні результати</h1>' + "".join(f'<p><a href="{slug}.html">{title}</a></p>' for slug, title, _ in PAGES), encoding="utf-8")


if __name__ == "__main__":
    main()
