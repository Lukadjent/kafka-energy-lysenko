# ЛР5 · Prometheus/Grafana · Продуктивність турбін · 3Б

**Лисенко Лука Сергійович · ТР-61МП.** [PDF звіт](report/Lab5_Lysenko_3B.pdf).

Реальні portable Prometheus3.16.0/GrafanaOSS13.3.1/JMX exporter1.7.0. Python exporter проводить живий синтетичний потік через Kafka → Cassandra, рахує тільки підтверджені sends/upserts. Java metrics надходять із JMX справжніх Kafka/Cassandra JVM. Шість турбін; interval1s, явно прискорено×10.

## Запуск

Після [підготовки стенда](../lab-support/README.md), завершення ЛР3/4 та старту Kafka/Cassandra:

```powershell
python lab-support/capture.py 5 all
```

Відкрийте `http://127.0.0.1:13000/d/hydro-lysenko` або `/d/streams-lysenko`. Viewer доступний на loopback; admin credential генерується у `.runtime/grafana-secret.json` і не публікується.

`--phase collect` знімає HTTP evidence повторно; `--phase live-test` повторює fault→pending→firing→recovery. `--phase stop` зупиняє тільки власні monitoring PID (перевірка create_time). Java/DB зупиняються окремим services.py.

## Що перевірено

- `alerts.yml`: production η<88% **for2h**, cavitation for10m, offdesign, generator overheat, voltage/frequency і producer absent/down.
- `config/rule-tests.yml` + `evidence/promtool.json`: **справжній promtool**, virtual-time boundary/recovery для2h/10m та absent-series1m. Це не двогодинний живий soak.
- Окремий profile `accelerated-e2e`, for15s: фактичні pending/firing/resolved через Prometheus API. Production for не змінено.
- Grafana-managed alert provisioned окремо й отриманий через API. Зовнішні email/Slack повідомлення не надсилаються.
- Обидва dashboards provisioned із JSON, datasource Prometheus; реальні скріншоти браузера у `evidence/screenshots`.
- Hill chart у text panel — власна модельна характеристика η(H,Q), **41×41 точок на кожен із3 типів**, JSON `hill-grid.json`; це не паспортні дані ГЕС. Інші панелі виконують live PromQL.
- `/metrics`, targets, query/query_range, dashboard та alert API outputs з реальними timestamps збережено у evidence. Нуль pipeline errors та UP усіх5 targets перевіряється автоматично.

## Межі інтерпретації

Синтетичні H/Q/η, cavitation/offdesign є керованим сценарієм. Модель не розраховує NPSH/коефіцієнт Тома. Оцінка втрат проти94% за фіксованих H/Q — навчальний proxy, не вимір фізичної деградації. Локальний стенд не доводить production-доступність та мережеву відмовостійкість.

Пороги напруги/частоти з методички трактуються як умови моделі, без твердження про універсальні технічні норми. ККД і power формули, принципи Counter/Gauge/Histogram та межі алертів пояснені у звіті.
