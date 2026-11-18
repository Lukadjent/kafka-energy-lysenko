# Лабораторна робота № 2 · Основи Cassandra

**Лисенко Лука Сергійович · ТР-61МП · варіант 3 · підваріант Б**

Модель моніторингу 15 гідроагрегатів Дніпровського каскаду. Підваріант Б порівнює середню сумарну потужність KYIV і KANIV за ту саму синтетичну добу. Сезонний архів порівнює ключ `station_id` із `(season_year, season)`.

[Звіт PDF](report/Lab2_Lysenko_3B.pdf)

## Відтворення у Windows

Потрібен Python 3.12 x64. Скрипт встановлення розпаковує Cassandra 4.0.21 і Java 11 у `.runtime`, перевіряє контрольні суми і встановлює Python-пакети в `.runtime/site-packages`. Якщо вже є Java 11, можна вказати її у `JAVA_HOME`.

```powershell
.\setup.ps1
python serve.py
```

Після повідомлення `Startup complete` в іншому вікні PowerShell запустіть з каталогу лабораторної:

```powershell
python run_lab.py
```

Команда створює `dnipro_hydro_lysenko`, відпрацьовує ручний INSERT і UNLOGGED BATCH через cqlsh, завантажує дані та звіряє їх через Python. Після benchmark вона виконує `nodetool flush` і зберігає гістограми розміру партицій. Cassandra слухає `127.0.0.1:9042`, а локальне керування JMX працює на `7199`. Повторний запуск використовує ті самі ключі синтетичних записів. Завершіть сервер через Ctrl+C у вікні `serve.py`.

## Матеріали перевірки

- [Схема CQL](schema.cql), [ручний і пакетний записи](demo.cql), [генератор і вимірювання](hydro.py).
- [Вивід cqlsh](evidence/cqlsh.txt), [вивід Python](evidence/python.txt), [вивід nodetool](evidence/nodetool.txt).
- [Числові результати та 240 сирих замірів читання](evidence/results.json).

Звіт можна відтворити командою `python build_report.py` за наявності `reportlab`, `Pillow` та Windows-шрифтів Times New Roman і Consolas. Ілюстрації консолі сформовано зі збереженого виводу.

## Перевірка за лекціями

У звіті уточнено вибір добового бакета під конкретний запит, UTC, різницю RF і LOCAL_ONE та межі UNLOGGED BATCH. Додано рекомендації щодо count/sum/min/max і окремого дослідження TTL/TWCS. Ця редакція змінює пояснення; числові Cassandra-результати походять із попереднього фактичного прогону та збережених cqlsh/Python/nodetool логів.
