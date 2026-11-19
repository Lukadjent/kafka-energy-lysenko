# Як читати докази

Підсумки: `stream-trials.json`, `delivery-trials.json`, `sink.json`, `sagas.json`. Сирі закриті вікна й anomaly IDs — у JSON кожного Streams-прогону. `delivery-output-ids.csv` містить фактичні IDs/offsets для read_committed і read_uncommitted усіх12 delivery-тестів.

`console-all.txt` зберігає дослідний прогін, який завершив усі normal/recovery Streams і delivery-тести, але зупинився на серіалізації Cassandra map у Saga JSON. Помилку виправлено явним перетворенням map на dict; остаточні сценарії Saga виконано у `console-saga.txt`. `console-recovery.txt` і відповідний `*-restart.log` фіксують повтор EOS recovery з детальним журналом відновлення RocksDB/task; саме він включений до фінального stream-trials.json. `console-audit.txt` повторно звіряє реальні Kafka output IDs.

Повнота фінальних результатів перевіряється командою `python lab-support/validate.py`. Підсумок виводиться в консоль.
