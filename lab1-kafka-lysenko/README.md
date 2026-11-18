# Лабораторна робота 1 з Apache Kafka

**Лисенко Лука Сергійович · ТР-61МП · варіант 3B**

Гідроелектростанції Дніпровського каскаду. Підваріант B: аналітика, стиснення, історичні дані та зберігання. Номер 13 у списку відповідає варіанту 3 та підваріанту B.

**[Завантажити звіт PDF](report/Lab1_Lysenko_3B.pdf)**

Виконано 110 реальних Kafka-прогонів з перевіркою 4 270 210 JSON-повідомлень. Окремі штатні load-тести передали й прочитали 1 000 000 записів по 256 байт та 100 000 по 512 байт. Перевірено JSON, Apache Avro з локальним навчальним registry і Protobuf. У SQLite оброблено 302 400 історичних подій за сім синтетичних діб. Успішно виконано 21 перевірку повноти, цілісності та відповідності оригінальному stdout.

Головний висновок для 3B: zstd дав 92,3% економії record batches при масовій відправці, 15,7% при природних 0,5 запису/с та 88,1% у п'ятихвилинному мікропакеті. Результати синтетичні й локальні; короткі saturation-тести не є гарантією production throughput.

## Що виміряно

| Дослідження | Конфігурації |
|---|---|
| Batch / linger | 16 / 64 / 256 КіБ × 0 / 10 / 50 мс, три повтори; окремо 500 записів/с |
| Compression | none, gzip, snappy, lz4, zstd; три повтори |
| Партиції | 2 / 4 / 8 за варіантом та 3 / 6 / 12 за загальними вимогами; три повтори |
| Стратегії | device_id, географія, event-time, custom балансування байтів |
| Підваріант B | 512 КіБ / 1 МіБ; історія; 30 / 300 / 900 / 3600 с вікна; retention 7 / 30 / 90 діб |
| Додаткові параметри | buffer.memory 8 / 32 / 64 МіБ; max.in.flight 1 / 5 / 10; linger 5 / 100 мс |
| Природний режим | none і zstd на 0,5 запису/с; окремий 150-record архівний пакет |

## Повторний запуск у Windows

Потрібен Python 3.12. Java 17 і Kafka 3.7.1 завантажуються локально, з перевіркою контрольних сум. Програми використовують loopback-порти 2181 і 9092; навчальний registry тимчасово працює на 8081. Новий запуск створює окремі тестові топіки та кілька гігабайтів робочих даних. Робочі дані, логи та залежності у Git не включаються.

```powershell
cd lab1-kafka-lysenko
.\run.ps1 setup
.\run.ps1 data
.\run.ps1 start
.\run.ps1 create-topics
.\run.ps1 experiments
.\run.ps1 load
.\run.ps1 serialization
.\run.ps1 warehouse
.\run.ps1 natural
.\run.ps1 analyze
.\run.ps1 validate
.\run.ps1 finalize
```

`run.ps1 report` формує PDF із результатів; потрібні Windows-шрифти Times New Roman / Consolas. У репозиторії вже є перевірений готовий PDF; повторно запускати всі вимірювання для його читання не потрібно.

Лаунчер може використати вже завантажені Java/Kafka із сусідньої папки `kafka-energy-lysenko/.runtime`. Для чистого checkout команда `setup` встановлює їх усередині поточної лабораторної. `lab.py` запускає оригінальні Java CLI через wildcard classpath, щоб уникнути проблем `.bat` із довгими українськими шляхами.

## Файли для перевірки

- [Усі сирі результати](evidence/results.json), [CSV](evidence/results-flat.csv), [зведення](evidence/summary.json).
- [Конфігурації та оригінальний числовий stdout повторної серії](evidence/runs/lecture-review).
- [21 автоматична перевірка](evidence/validation.json).
- [Load-сценарії](evidence/load-scenarios.json), [серіалізація та registry](evidence/serialization.json).
- [Аналітичні вікна](evidence/warehouse.json), [погодинна історія](evidence/history-hourly.csv).
- [Прочитані з брокера налаштування](evidence/broker-config-verification.txt), [коректна зупинка](evidence/shutdown.txt).
- [Java benchmark](tools/EnergyBenchmark.java), [генератор](scripts/generate_data.py), [Avro схема](config/hydro.avsc), [Protobuf схема](config/hydro.proto), [SQL для DWH](config/warehouse.sql).

## Межі експерименту

Повний JSON має в середньому 314,373 байта. Заявлені методичкою 256 байт не підставлялися замість виміряного розміру; у двох raw-load сценаріях розміри 256 і 512 байт дотримано точно.

Тестовий кластер має один брокер і RF=1. CPU клієнтської JVM включає producer і consumer. Процесні I/O counters не дорівнюють фізичним записам SSD. Короткий тест retention перевіряє застосовані налаштування, а видалення за закінченням 7/30/90 діб не вимірює. Registry є локальною навчальною реалізацією потрібних REST endpoints, а не Confluent Schema Registry. Затримка закриття аналітичних вікон розрахована за event time; вона відокремлена від виміряної Kafka latency.

## Уточнення за лекціями

Повний оперативний профіль 64 КіБ / 10 мс / lz4 / 4 партиції / device_id перевірено у шести окремих прогонах з `enable.idempotence=true` і `max.in.flight=5`. Локальний Avro registry перевіряє BACKWARD-сумісність з останньою схемою subject: приймає поле з default та повертає HTTP 409 для несумісного типу. SQLite rollup зберігає count/sum/min/max; повторна агрегація звіряє всі 105 добових груп із raw.

Розігрів виключено з таймерів і приростів байтів, але середні Kafka producer metrics охоплюють його в поточному вікні метрик. `MEASUREMENT_RESULT` та `RESOURCE_RESULT` збережено під час реальних прогонів; валідатор звіряє всі числові результати з цим stdout. Окремі консольні виводи: [серіалізація](evidence/serialization-stdout.txt), [DWH](evidence/warehouse-stdout.txt). [Індекс походження Kafka-замірювань](evidence/traceability.csv) містить відносні шляхи й SHA-256. Попередні файли в `evidence/runs` залишено як історичну серію; таблиці звіту побудовано з актуального `evidence/results.json`.

Контрольні суми `manifest.sha256` описують точні байти ZIP-пакета. Для сирих evidence-файлів Git зберігає байти без перетворення кінців рядків, щоб SHA-256 у `traceability.csv` залишалися перевірними після завантаження.
