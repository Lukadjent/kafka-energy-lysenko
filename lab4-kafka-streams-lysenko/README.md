# ЛР4 · Kafka Streams, hopping 30/10 хв і Saga · 3Б

**Лисенко Лука Сергійович · ТР-61МП.** [PDF звіт](report/Lab4_Lysenko_3B.pdf).

Java Kafka Streams 3.7.1 + persistent RocksDB. Шість **модельних** ГЕС, 6480 коректних подій за3 години event time, 3 invalid, 1 watermark tick. Природна частота6/10=0.6 msg/s; replay прискорений. `KAKHOVKA_MODEL` — логічна модель задачі, не фактична телеметрія діючої ГЕС.

## Код

- `src/HydroStreams.java`: stateless validation/anomalies, GlobalKTable reference join, hopping windows30min/10min/grace1min, persistent aggregate history, final suppression, linear forecast та закритий розв’язок модельної convex-задачі координації.
- `src/DeliveryProbe.java`: окрема перевірка трьох семантик стандартними Kafka clients. At-most-once не видається за підтримувану Kafka Streams processing.guarantee.
- `run_lab.py`: реальні Java процеси, прискорений producer, Python oracle кожного поля кожного вікна, read_committed readers, manual-commit idempotent Cassandra sink, crash/restart.
- `saga.py`: durable state/audit, успіх/компенсація/crash-gap; ефект і command dedupe виконуються одним LWT над participant row. Це симулятор actuator, а не SCADA.
- `schema.cql`: cascade_flow_aggregates, station_coordination, saga_log та допоміжні таблиці.

## Прогони

Після [підготовки стенда](../lab-support/README.md) та завершення benchmark ЛР3:

```powershell
python lab-support/services.py start-kafka
python lab-support/capture.py 4 all
```

Три нормальні повтори EOS, три ALO, окремий EOS crash/restart. На кожен —20 вікон,19440 входжень; усі counts/means/forecasts/optima звірено незалежним oracle. Fault після state update до output/commit. Окрема фаза `recovery` повторює fault з детальним Streams log4j-журналом. Recovery використовує ті самі application.id/state.dir; інші тести — окремі IDs.

Delivery: по3 нормальні прогони +1 crash для кожної семантики,1000 ID/batch100. Очікувані lost/duplicate після збою: AMO100/0, ALO0/100, EOS0/0. EOS uncommitted reader бачить aborted100; committed — ні. Сирі timing batches і IDs/output збережені у evidence.

Sink: connection loss після Cassandra write до offset commit → replay тієї самої пачки →40 upsert attempts/20 unique windows. Kafka EOS **не робить зовнішню Cassandra частиною Kafka transaction**. Ідемпотентні ключі забезпечують кінцевий стан, а не атомарність усіх таблиць.

Saga: PREPARE_UPPER → OPEN_GATES → ACK_LOWER. На помилці reverse compensation; на crash після OPEN effect до checkpoint повторний command дедуплікується без повторної зміни version. Компенсація повертає модельне положення затвору; вже скинуту фізичну воду «відкотити» неможливо.

Вимір throughput — wall-clock replay до читання всіх output, не per-event latency. Один cascade key/partition і один broker — явні обмеження. Unbounded suppression придатний для скінченного тесту, production потребує ліміту буфера. Dashboard `http://127.0.0.1:13000/d/streams-lysenko` provisioned у ЛР5.
