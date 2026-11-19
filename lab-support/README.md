# Спільний стенд лабораторних 3–5

Windows x64, Python 3.12, Java 11 (Cassandra) і JDK 17 (Kafka/Streams). Усі сервери слухають loopback. Студент: **Лисенко Лука Сергійович, ТР-61МП, варіант 3Б**.

## Підготовка з чистого checkout

З кореня репозиторію (потрібен доступ до офіційних Apache/Microsoft/Adoptium/GitHub/Grafana downloads):

```powershell
python kafka-energy-lysenko/scripts/download_dependencies.py
./lab2-cassandra-lysenko/setup.ps1 -Python python
./lab-support/setup.ps1 -Python python
python lab-support/services.py start-cassandra
```

Перший скрипт завантажує Kafka/JDK у локальний runtime. `lab-support/setup.ps1` перевіряє SHA-256 Prometheus/Grafana/JMX і встановлює Python dependencies локально. `common.py` використовує спільні portable-дистрибутиви. Перед стартом переконайтеся, що достатньо RAM/диска (три JVM Cassandra до 1 GiB кожна; дистрибутиви, стиснені дані й snapshots займають кілька GiB).

## Послідовне виконання

```powershell
python lab-support/capture.py 3 all
# Для нового baseline за вже оптимізованої схеми:
# python lab-support/capture.py 3 all --reset
python lab-support/services.py start-kafka
python lab-support/capture.py 4 all
python lab-support/capture.py 5 all
```

`--reset` явно видаляє **лише власний keyspace `energy_l3_lysenko`**. Без нього повтор `all` відмовляється запускати baseline на вже оптимізованій схемі. Окремі фази призначені для діагностики/продовження; їх не слід видавати за чисте порівняння трьох схем. Спочатку завершують виміри ЛР3, потім ЛР4, потім запускають monitoring load ЛР5.

## Сервери та зупинка

| Сервіс | Адреса |
|---|---|
| Cassandra nodes | `127.0.0.11–13:9042`, DC `dc1`, rack1–3 |
| Cassandra JMX | `17191–17193`; Prometheus exporters `18081–18083` |
| Kafka / ZooKeeper | `127.0.0.1:9092` / `2181`; broker exporter `18080` |
| Python exporter | `127.0.0.1:18400` |
| Prometheus / Grafana | `127.0.0.1:19090` / `13000` |

```powershell
python lab5-monitoring-lysenko/run_lab.py --phase stop
python lab-support/services.py stop-kafka
python lab-support/services.py stop-cassandra
```

Зупиняються тільки власні PID з перевіркою create_time; Cassandra спочатку drain, Java-сервіси мають локальні control endpoints з runtime-токенами. Конфігурації, токени та паролі з `.runtime` не публікуються. Дані/snapshots після зупинки зберігаються для повторної перевірки. RocksDB ЛР4 використовує короткий ASCII-шлях `%TEMP%/lysenko-l4-rocksdb` через обмеження native Windows backend; для кожного тесту окремий каталог, для restart — той самий.

## Звіти й перевірки

```powershell
python lab-support/validate.py
python lab-support/build_reports.py 3
python lab-support/build_reports.py 4
python lab-support/build_reports.py 5
python lab-support/package.py
```

Звіти — `output/pdf/` і `labN-.../report/`. Для генерації потрібні Windows Times New Roman/Consolas; для візуальної перевірки PDF — Poppler `pdftoppm`. ZIP містять власні джерела, raw evidence, PDF та спільні утиліти; portable binaries, великі CSV/SSTables, персональні login/session secrets і матеріали викладача виключені. Дистрибутиви завантажуються повторно з перевіркою checksum.

## Межі експерименту

Три Cassandra JVM ділять один фізичний ПК; RF=3 не робить його незалежним тривузловим production-кластером. Kafka має один broker/RF=1. Телеметрія синтетична. У ЛР4 три години event time відтворюються прискорено, у ЛР5 telemetry interval=1s замість 10s. Production alert durations перевіряються promtool у віртуальному часі; живе правило 15s має окремий профіль. Це не двогодинне живе спостереження.
