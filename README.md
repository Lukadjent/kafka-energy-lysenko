# Проєктування систем з розподіленими базами даних

**Лисенко Лука Сергійович · ТР-61МП · варіант 3, підваріант Б**

Власні лабораторні роботи з модельним моніторингом ГЕС. Числа у звітах походять із фактичних запусків, raw evidence та незалежних перевірок. Лекційні матеріали викладача й практичні заняття до результатів лабораторних не включені.

| ЛР | Зміст | Звіт | Код і докази |
|---|---|---|---|
| 1 | Kafka: 110 прогонів, 4,27 млн JSON, batch/linger, кодеки/серіалізація | [PDF](lab1-kafka-lysenko/report/Lab1_Lysenko_3B.pdf) | [README](lab1-kafka-lysenko/README.md) |
| 2 | Cassandra: схема ГЕС, ручний/batch запис, добова/сезонна аналітика | [PDF](lab2-cassandra-lysenko/report/Lab2_Lysenko_3B.pdf) | [README](lab2-cassandra-lysenko/README.md) |
| 3 | RF=3 на трьох JVM; по1 296 000 рядків у3 схемах; 2 MV, індекс, weekly rollup | [PDF](lab3-cassandra-optimization-lysenko/report/Lab3_Lysenko_3B.pdf) | [README](lab3-cassandra-optimization-lysenko/README.md) |
| 4 | Java Kafka Streams/RocksDB, hopping30/10 хв, EOS recovery та3-крокова Saga | [PDF](lab4-kafka-streams-lysenko/report/Lab4_Lysenko_3B.pdf) | [README](lab4-kafka-streams-lysenko/README.md) |
| 5 | Реальні Prometheus/Grafana/JMX, turbine hill chart, production rule tests та live-алерти | [PDF](lab5-monitoring-lysenko/report/Lab5_Lysenko_3B.pdf) | [README](lab5-monitoring-lysenko/README.md) |

[Підготовка спільного стенда й команди запуску ЛР3–5](lab-support/README.md).

Три Cassandra JVM ділять один фізичний Windows-хост, Kafka має один broker. Телеметрія синтетична; прискорений event time та virtual-time alert tests явно відокремлені від живих вимірів. Ціль p95<20ms у ЛР3 досягнута не всіма запитами — звіт показує фактичний результат.

Історичний каталог `kafka-energy-lysenko` використовується лише як джерело portable Kafka/JDK/bootstrap dependencies; його первісне практичне завдання не входить до лабораторних3–5.
