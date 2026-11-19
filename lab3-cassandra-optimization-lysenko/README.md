# ЛР3 · Оптимізація Cassandra · 3Б

**Лисенко Лука Сергійович · ТР-61МП.** [PDF звіт](report/Lab3_Lysenko_3B.pdf).

Реальні три вузли Cassandra 4.0.21, NTS RF=3, LOCAL_QUORUM. У трьох схемах по **1 296 000 подій за 30 діб**. 15 гідроагрегатів, UTC, крок 30s, синтетичні параметри. `COUNT(*)` кожної таблиці підтверджено в raw stdout.

## Реалізація та evidence

- `schema.cql`: simple, hourly, daily й weekly_rollup.
- `optimize.cql`: вторинний індекс і **два реальні MV**. Замість недопустимого range-предиката у MV використано похідний `level_band`; actual CQL rejection збережено.
- `run_lab.py`: генерація, завантаження, 60 повторів × 12 read-планів, multi-partition parallel/sequential, index/MV comparison, independent CSV weekly oracle, TTL/TWCS, snapshots.
- `evidence/write-samples-*.csv`: кожен фактичний batch ack; throughput включає читання CSV та ack, генерацію виключено. Batch до60 в одній партиції, max8 in-flight.
- `evidence/read-samples.csv`: 720 основних вимірів. Інші JSON містять усі додаткові samples. p95/p99 nearest-rank, p50 median. Валідація digest поза таймером.
- `evidence/daily-aggregation.json`: ще60 вимірів на схему справжніх CQL COUNT/SUM/MIN/MAX за добу, злиття hourly partials та незалежна звірка з CSV. Поточну додаткову серію знято після MV; попередній raw-read baseline збережено окремо.
- `evidence/nodetool-*`: raw tablestats/histograms/compactionstats/status/snapshots усіх вузлів. Compression savings=(1−SSTableCompressionRatio)×100%, не disk/CSV.
- `evidence/console-all.txt`: повний прогін із write timeout 20s.

## Запуск

Підготуйте [спільний стенд](../lab-support/README.md), потім з кореня репозиторію:

```powershell
python lab-support/capture.py 3 all
```

Якщо попередня оптимізована схема вже існує, чистий повтор: `python lab-support/capture.py 3 all --reset`. Видаляється лише `energy_l3_lysenko`. Всі таблиці baseline починають порожніми; індекс/MV створюються після основного write/read benchmark. Одночасно з benchmark не запускати ЛР4/5.

Після baseline daily змінено на TTL30d/TWCS1d. Це стосується **нових** записів; існуючі не отримують TTL автоматично. Snapshot створено й перевірено наявність manifest/files; restore у цьому підваріанті не тестувався. Табличні метрики зняті після оптимізації/flush, тому це не ізольований тест storage overhead індексу.

Ціль p95<20ms досягнуто не всіма запитами. Висновки у звіті відображають виміри; жодного методичного діапазону не використано замість чисел.
