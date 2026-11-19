"""Build the final Ukrainian PDF report from measured evidence only."""
from __future__ import annotations
from collections import defaultdict
from datetime import datetime
import json
import re
from pathlib import Path
import textwrap
from xml.sax.saxutils import escape

from common import ROOT, CONFIG, configure_stdout
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image, PageBreak, Preformatted, KeepTogether

E = ROOT / "evidence"
OUT = ROOT / "report"
REPO = "https://github.com/Lukadjent/kafka-energy-lysenko/tree/master/lab1-kafka-lysenko"


def read(name):
    return json.loads((E / name).read_text(encoding="utf-8"))


def main():
    configure_stdout()
    OUT.mkdir(exist_ok=True)
    font_root = Path("C:/Windows/Fonts")
    for name, filename in (("TimesUA", "times.ttf"), ("TimesUABold", "timesbd.ttf"), ("TimesUAItalic", "timesi.ttf"), ("MonoUA", "consola.ttf")):
        pdfmetrics.registerFont(TTFont(name, str(font_root / filename)))
    pdfmetrics.registerFontFamily("TimesUA", normal="TimesUA", bold="TimesUABold", italic="TimesUAItalic", boldItalic="TimesUABold")
    styles = {
        "body": ParagraphStyle("Body", fontName="TimesUA", fontSize=11.4, leading=15.2, spaceAfter=7, allowWidows=0, allowOrphans=0),
        "h1": ParagraphStyle("H1", fontName="TimesUABold", fontSize=15, leading=18, spaceAfter=12, keepWithNext=True),
        "h2": ParagraphStyle("H2", fontName="TimesUABold", fontSize=12.3, leading=15, spaceBefore=9, spaceAfter=7, keepWithNext=True),
        "small": ParagraphStyle("Small", fontName="TimesUA", fontSize=9.2, leading=11.5, spaceAfter=7),
        "ref": ParagraphStyle("Reference", fontName="TimesUA", fontSize=9.2, leading=11.5, spaceAfter=2),
        "cell": ParagraphStyle("Cell", fontName="TimesUA", fontSize=9.3, leading=11.4),
        "head": ParagraphStyle("Head", fontName="TimesUABold", fontSize=9.3, leading=11.4, textColor=colors.white),
        "cover": ParagraphStyle("Cover", fontName="TimesUA", fontSize=13, leading=17, alignment=TA_CENTER, spaceAfter=5),
        "title": ParagraphStyle("Title", fontName="TimesUABold", fontSize=18, leading=23, alignment=TA_CENTER, spaceAfter=15),
        "code": ParagraphStyle("Code", fontName="MonoUA", fontSize=7.5, leading=9.7, spaceAfter=9),
    }
    s = read("summary.json")
    dataset, environment, serde, warehouse, validation = (read(name) for name in ("dataset.json", "environment.json", "serialization.json", "warehouse.json", "validation.json"))
    load = read("load-scenarios.json")
    story = []

    def p(text, style="body"):
        story.append(Paragraph(text, styles[style]))

    def h(text): p(text, "h1")
    def sub(text): p(text, "h2")
    def page(force=False): story.append(PageBreak() if force else Spacer(1, 9))
    def num(value, digits=1): return f"{value:,.{digits}f}".replace(",", " ")
    def code(text, keep=False):
        lines = []
        for line in text.splitlines():
            lines.extend(textwrap.wrap(line, 106, replace_whitespace=False, drop_whitespace=False) or [""])
        block = Preformatted("\n".join(lines), styles["code"])
        story.append(KeepTogether([block]) if keep else block)
    def table(headers, rows, widths=None):
        contents = [[Paragraph(escape(str(item)), styles["head"]) for item in headers]]
        for row in rows:
            contents.append([Paragraph(escape(str(item)), styles["cell"]) for item in row])
        tab = Table(contents, colWidths=widths, repeatRows=1, hAlign="LEFT", splitByRow=0 if len(rows) <= 8 else 1)
        tab.setStyle(TableStyle([
            ("BACKGROUND", (0,0), (-1,0), colors.HexColor("#264d6b")),
            ("ROWBACKGROUNDS", (0,1), (-1,-1), [colors.white, colors.HexColor("#f2f5f7")]),
            ("GRID", (0,0), (-1,-1), .4, colors.HexColor("#D9D9D9")),
            ("VALIGN", (0,0), (-1,-1), "MIDDLE"), ("LEFTPADDING", (0,0), (-1,-1), 6),
            ("RIGHTPADDING", (0,0), (-1,-1), 6), ("TOPPADDING", (0,0), (-1,-1), 5),
            ("BOTTOMPADDING", (0,0), (-1,-1), 5),
        ]))
        story.append(tab)
        story.append(Spacer(1, 8))
    def figure(name, width=170*mm):
        from PIL import Image as PILImage
        path = E / "figures" / name
        with PILImage.open(path) as im:
            height = width * im.height / im.width
        story.append(Image(str(path), width=width, height=height))
        story.append(Spacer(1, 7))

    story.append(Spacer(1, 12*mm))
    for line in ["НАЦІОНАЛЬНИЙ ТЕХНІЧНИЙ УНІВЕРСИТЕТ УКРАЇНИ", "КИЇВСЬКИЙ ПОЛІТЕХНІЧНИЙ ІНСТИТУТ", "ІМЕНІ ІГОРЯ СІКОРСЬКОГО", "Навчально-науковий інститут атомної та теплової енергетики", "Кафедра цифрових технологій в енергетиці"]:
        p(line, "cover")
    story.append(Spacer(1, 27*mm))
    p("ЛАБОРАТОРНА РОБОТА № 1", "title")
    p('з дисципліни «Проєктування систем з розподіленими базами даних в енергетиці»', "cover")
    story.append(Spacer(1, 5*mm))
    p("Дослідження продуктивності Apache Kafka для енергетичних IoT-потоків", "title")
    p("Варіант 3<br/>Гідроелектростанції Дніпровського каскаду", "cover")
    p("Підваріант B<br/>Аналітика та зберігання історичних даних", "cover")
    story.append(Spacer(1, 20*mm))
    p("Виконав: студент групи ТР-61МП<br/><b>Лисенко Лука Сергійович</b><br/><br/>Перевірив: ______________________________")
    story.append(Spacer(1, 15*mm))
    p("Київ 2026", "cover")
    page(True)

    h("1 Мета та специфіка варіанта")
    p("Експериментально дослідити вплив batch.size, linger.ms, стиснення й партиціонування на передавання синтетичної телеметрії ГЕС. Для підваріанта B оцінити великі пакети, історичні закономірності, аналітичні вікна та місткість сховища за різних retention policies.")
    p("Номер у списку 13 відповідає варіанту ((13 - 1) mod 10) + 1 = 3 і другій десятці списку, тобто підваріанту B.")
    table(["Параметр", "Варіант 3B"], [["Джерела", "15 гідроагрегатів, HYDRO_DN_001 ... HYDRO_DN_015"], ["Природна частота", "Один запис на агрегат кожні 30 с; разом 0,5 запису/с"], ["Навантажувальний профіль", "500 записів/с; у 1000 разів вище природного потоку"], ["Основні метрики", "water_flow, water_level, turbine_type; потужність у МВт"], ["Ключ партиціонування варіанта", "turbine_type; додатково device_id, географія, час і балансування"], ["Базові топіки", "hydro-main-lysenko: 4 партиції; hydro-batch-test-lysenko: 3"]], [46*mm,124*mm])
    sub("Ключовий результат")
    natural = next(row for row in s["natural"] if row["compression"] == "zstd")
    archive = s["archive-window"][0]
    zstd = next(row for row in s["compression"] if row["compression"] == "zstd")
    p(f"Zstd скоротив розмір Kafka record batches на {num(zstd['savings_pct'])}% у масовому тесті, але лише на {num(natural['savings_pct'])}% при 0,5 запису/с. Пакет зі 150 записів, що відповідає п'яти хвилинам природного потоку, дав {num(archive['savings_pct'])}% економії. Отже, для архіву ГЕС суттєвим є накопичення мікропакета, а не тільки зміна compression.type.")
    p(f"Серія містить {s['trial_count']} прогонів Kafka з перевіркою {s['total_measured_records']:,} JSON-повідомлень; додатково виконано два навантажувальні сценарії на 1 100 000 записів.".replace(",", " "))

    h("2 Тестове середовище та методика")
    java_line = environment["java_version"].splitlines()[0]
    table(["Компонент", "Конфігурація"], [["ОС / CPU / RAM", f"Windows x64; {environment['physical_cpu']} фізичних / {environment['logical_cpu']} логічних ядер; {num(environment['ram_bytes']/2**30)} ГіБ RAM"], ["Диск C", "KINGSTON SA400S37240G; SATA"], ["Java / Kafka / ZooKeeper", java_line + "; Kafka 3.7.1; ZooKeeper 3.8.4"], ["Python", "3.12.14; avro 1.12.0; protobuf 5.29.3; psutil 7.0.0"], ["Брокер", "Один брокер; RF=1; loopback 127.0.0.1:9092; heap до 512 МіБ"], ["Основний retention", "30 діб; окремо перевірено конфігурації 7 / 30 / 90 діб"]], [40*mm,130*mm])
    p("Producer і consumer реалізовано на Java Kafka Client 3.7.1. Producer надсилає асинхронно; consumer одночасно перевіряє sequence, відсутність дублікатів і збіг байтів із джерелом. Для кожного прогону створено окремий топік h3b-lysenko-..., а його роль і всі параметри записано в JSON. Так дані різних повторень не змішуються. Базові імена топіків збережено як точки входу лабораторного проєкту.")
    p("Перші 1000 записів основного прогону є розігрівом: вони виключені з таймерів, числа перевірених повідомлень і приростів мережевих та .log-байтів. Середні producer metrics, зокрема compression-rate-avg, зняті з того самого producer й охоплюють також розігрів у його поточному вікні метрик. Основна матриця містить 50 000 записів на прогін, codec-тести - 20 000, профіль 500 записів/с - 5000. Основні порівняння повторено тричі; порядок перемішано з seed=13, викиди не вилучалися. Медіана p95 повторів описує типовий прогін і не є p95 об'єднаної вибірки.")
    p("Ack latency вимірюється від send до callback; end-to-end - до читання й побайтової перевірки consumer, одним монотонним годинником JVM. acks=all; retries=3. У порівняльній матриці idempotence вимкнено для дослідження max.in.flight=10; повну рекомендовану конфігурацію окремо перевірено з idempotence=true та max.in.flight=5. Consumer стенда призначає партиції вручну; rebalance, відновлення offsets і exactly-once після відмов не тестуються.")
    sub("Тестові дані")
    p(f"Згенеровано 1000 повних JSON-записів: середній розмір {num(dataset['bytes_mean'],3)} байта, діапазон {dataset['bytes_min']} ... {dataset['bytes_max']} байт UTF-8. Поля методички з повними назвами не вміщуються в заявлені 256 байт; назви полів не скорочувалися. Значення відповідають діапазонам варіанта. Усі дані та географічні точки синтетичні, без прив'язки до реальних діючих об'єктів.")
    p("У stress-тестах один і той самий корпус із 1000 записів циклічно повторюється. Це контрольований набір для порівняння конфігурацій; його повторюваність може підвищувати стискання. Для історичного аналізу окремо побудовано 302 400 унікальних подій за сім діб. Дані описують задані діапазони, а не повну фізичну модель ГЕС.")
    page()

    h("3 Вплив batch.size та linger.ms")
    p("Таблиця 1 містить результати дев'яти комбінацій без обмеження offered rate. Кожне значення throughput є медіаною трьох прогонів. Показник затримки містить і очікування в черзі клієнта під час масової відправки.")
    table(["Batch, КіБ", "Linger, мс", "Тис. записів/с; min-max", "Ack avg, мс", "E2E p95, мс"], [[row["batch_size"]//1024,row["linger_ms"],f"{num(row['rec_s']/1000)}; {num(row['rec_s_min']/1000)}-{num(row['rec_s_max']/1000)}",num(row["ack_ms"]),num(row["e2e_p95_ms"])] for row in s["batch"]], [22*mm,22*mm,58*mm,31*mm,37*mm])
    figure("batch.png")
    p("Таблиця 2 відокремлює ефект накопичення пакета від насичення: offered rate=500 записів/с, один прогін кожної комбінації. В усіх випадках досягнута швидкість близька до заданих 500 записів/с; тому її не слід використовувати для ранжування максимальної пропускної здатності.")
    table(["Batch, КіБ", "Linger, мс", "Записів/с", "Ack avg, мс", "Ack p95, мс"], [[row["batch_size"]//1024,row["linger_ms"],num(row["rec_s"]),num(row["ack_ms"],2),num(row["ack_p95_ms"],2)] for row in s["rate500"]], [28*mm,28*mm,38*mm,38*mm,38*mm])
    p("При 500 записах/с найнижчу затримку дає linger=0; збільшення batch.size саме по собі не змушує чекати заповнення пакета. У масовому тесті відмінності коротких прогонів значні: min-max часто перекривається. Тому найбільшу медіану можна назвати кандидатом для цього стенда, але не доведеним універсальним оптимумом.")
    page()

    h("4 Порівняння алгоритмів стискання")
    p("Налаштування однакові: batch.size=256 КіБ, linger.ms=50, 20 000 JSON-записів, три повтори. Додатково до none/snappy/lz4/zstd перевірено gzip. Compression savings = 100 × (1 - compression-rate-avg) для record batches Kafka. Окремо обчислено економію довжини файлів .log проти none, із заголовками записів і пакетів.")
    order = {name:i for i,name in enumerate(("none","gzip","snappy","lz4","zstd"))}
    codec_rows = sorted(s["compression"], key=lambda row: order[row["compression"]])
    table(["Кодек", "Тис. записів/с", "Ack, мс", "Batch savings, %", ".log проти none, %", "CPU мс / 1000"], [[row["compression"],num(row["rec_s"]/1000),num(row["ack_ms"]),num(row["savings_pct"]),num(row["disk_savings_vs_none_pct"]),num(row["client_cpu_ms_per_1000"])] for row in codec_rows], [22*mm,30*mm,24*mm,31*mm,33*mm,30*mm])
    figure("compression.png")
    p("Zstd дав найбільше стискання цього корпусу. Lz4 і snappy мають меншу економію, проте придатні як кандидати для швидкої телеметрії. CPU у таблиці належить усій клієнтській JVM, тобто producer і consumer разом; це не ізольований час функції компресора. Порівняння також залежить від фактично сформованих пакетів і короткої тривалості прогонів.")
    table(["Режим zstd", "Записів", "Частота / спосіб", "Batch savings, %", ".log байт / запис"], [["Масовий",20000,"Без rate limit",num(zstd["savings_pct"]),num(zstd["disk_bytes_per_record"])], ["Природний",30,"0,5 запису/с",num(natural["savings_pct"]),num(natural["disk_bytes_per_record"])], ["Архівне вікно",150,"Одночасна відправка 5 хв даних",num(archive["savings_pct"]),num(archive["disk_bytes_per_record"])]], [34*mm,22*mm,52*mm,31*mm,31*mm])
    p("Критерій варіанта понад 70% економії для історичних даних досягнуто в масовому та архівному пакетах. На природному рідкому потоці цей критерій не досягається. Тест природної частоти містить лише 30 повідомлень, а архівний тест - один мікропакет; це демонстрація впливу режиму, не статистична гарантія майбутнього архіву.")
    page()

    h("5 Партиціонування та масштабованість")
    p("Основні числа партицій за варіантом - 2, 4, 8. Додатково виконано загальні 3, 6, 12. Ключ turbine_type має лише три значення: kaplan, francis, pelton. Для кожної кількості виконано три прогони по 50 000 записів; batch=64 КіБ, linger=10, compression=none. Scaling factor нижче обчислено відносно двох партицій.")
    table(["Партицій", "Активних", "Тис. записів/с; min-max", "Scaling", "E2E p95, мс"], [[row["partitions"],int(row["active_partitions"]),f"{num(row['rec_s']/1000)}; {num(row['rec_s_min']/1000)}-{num(row['rec_s_max']/1000)}",num(row["scaling_vs_2"],2),num(row["e2e_p95_ms"])] for row in s["partition"]], [24*mm,25*mm,63*mm,23*mm,35*mm])
    figure("partitions.png")
    p("Три значення ключа використовують не більше трьох партицій, іноді менше через збіг hash. Зміна кількості партицій змінює це відображення і фактичне пакетування; медіани та min-max слід читати разом. Брокер і consumer залишаються одними, а прогони короткі. Отриманий scaling factor описує цей локальний тест і не доводить лінійного масштабування багатоброкерного кластера.")
    sub("Порівняння стратегій за чотирьох партицій")
    table(["Стратегія", "Активних", "Тис. записів/с", "CV навантаження, %", "Приклад розподілу"], [[row["strategy"],int(row["active_partitions"]),num(row["rec_s"]/1000),num(row["cv_pct"]),str(row["distribution"])] for row in s["strategy"]], [29*mm,22*mm,32*mm,34*mm,53*mm])
    p("device використовує device_id як відповідник plant_id; region - округлені синтетичні координати; time - хвилину event timestamp. Custom balanced спрямовує наступний запис у партицію з найменшим запланованим обсягом байтів. Він вирівнює навантаження, але розділяє потік одного пристрою між партиціями й не гарантує його загального порядку. Для DWH з device-level аналітикою пріоритетним є стабільний device_id; turbine_type можна залишити полем групування.")
    page()

    h("6 Серіалізація JSON Avro та Protocol Buffers")
    p("Усі формати містять однакові 13 логічних полів, включно з location. Перевірено відновлення кожного з 1000 записів. Для часу серіалізації виконано п'ять повторів по 10 000 операцій; наведено медіану часу однієї операції. Парсинг схеми та перше отримання schema ID не входять у теплі вимірювання.")
    table(["Формат", "Середній розмір, байт", "Encode, мкс", "Decode, мкс", "Перевірено записів"], [[row["format"],num(row["mean_bytes"]),num(row["encode_us_median"],2),num(row["decode_us_median"],2),row["round_trip_records"]] for row in serde["formats"]], [38*mm,37*mm,28*mm,28*mm,39*mm])
    p(f"Avro перевірено з локальним навчальним REST registry: реєстрація schema_id={serde['registry']['schema_id']}, GET схеми за ID і 10 Avro-повідомлень через Kafka. У розмір включено 5 байтів wire header. Холодне читання схеми через loopback зайняло {num(serde['registry']['cold_lookup_ms'],2)} мс. Реалізовано BACKWARD-перевірку проти останньої схеми subject засобами Apache Avro: додавання sensor_quality з default=null прийнято, новий reader прочитав старий запис із null, зміну power_output з double на string відхилено HTTP 409. Це обмежений навчальний registry, не Confluent Schema Registry; транзитивну сумісність, автентифікацію та production HA він не реалізує.")
    p("Protobuf використано через стандартний runtime Google та дескриптор, що відповідає config/hydro.proto. Avro застосовує стандартну бібліотеку Apache Avro та config/hydro.avsc. Висновок про швидкість стосується саме цих реалізацій: порівнюються також реалізаційні витрати Python/нативного runtime, а не тільки абстрактні формати.")

    h("7 Обов'язкові навантажувальні сценарії")
    p("Нижче - окремі виконання штатних ProducerPerformance і ConsumerPerformance з дистрибутива Kafka. Тут використовуються тестові payload заданої довжини, а не повний JSON ГЕС. Ці сценарії не змішуються з варіантними вимірюваннями.")
    table(["Сценарій", "Відправлено", "Байт / запис", "Target, записів/с", "Отримано"], [[row["scenario"],f"{row['records']:,}".replace(","," "),row["record_bytes"],row["target_rate"],f"{row['received']:,}".replace(","," ")] for row in load], [40*mm,34*mm,29*mm,34*mm,33*mm])
    for row in load:
        p("<b>" + escape(row["scenario"]) + "</b>", "small")
        code(row["producer_summary"])
    p("Обидва consumer прочитали рівно задану кількість повідомлень. Додатково штатний perf producer передав 5000 записів із фактичного JSON-корпусу через --payload-file. Повні виводи команд збережено в evidence/load-*.txt та official-json-performance.txt.")
    page()

    h("8 Дослідження підваріанта B")
    sub("Великі пакети для аналітики")
    table(["Batch, КіБ", "Linger, мс", "Тис. записів/с", "Ack, мс", "Batch savings, %"], [[row["batch_size"]//1024,row["linger_ms"],num(row["rec_s"]/1000),num(row["ack_ms"]),num(row["savings_pct"])] for row in s["analytics"]], [30*mm,30*mm,40*mm,30*mm,40*mm])
    p("Додаткові batch.size=512 КіБ та 1 МіБ перевірено із zstd у трьох повтореннях. Усі ці пакети є верхньою межею розміру для однієї партиції. За природної частоти вони не заповнюються лише завдяки linger=100 або 300 мс: накопичення п'ятихвилинного архіву - окрема прикладна операція.")
    sub("Історія water_flow та water_level")
    figure("history.png")
    p("Для семи діб згенеровано 302 400 подій, тобто 15 × 2880 × 7. У синтетичній моделі water_flow має добову складову, water_level змінюється повільніше, а параметри різних turbine_type мають різні базові рівні. Графік демонструє закладені генератором закономірності; він не є висновком про поведінку реального Дніпровського каскаду.")
    sub("Інтеграція з аналітичним сховищем")
    p("Виконано прототип SQLite: landing-таблиця має унікальний ключ (device_id, event_time), а SQL формує середні, мінімальні й максимальні рівні води та середню витрату у часових вікнах. Усі 302 400 подій завантажено; сума лічильників у кожній агрегації збіглася з кількістю джерела. Для production запропоновано потік Kafka → landing у DWH → віконні агрегати; SQLite є навчальною перевіркою схеми та запиту.")
    p(f"Згідно з лекцією 6, п'ятихвилинна rollup-таблиця зберігає cnt, sum, min і max, тому добове середнє обчислюється як SUM(sum_power)/SUM(cnt). Після повторної агрегації звірено {warehouse['rollup_contract']['daily_groups_verified_against_raw']} пар «доба/пристрій» із сирими даними: count, sum, min і max збігаються. Це offline event-time SQL; запізнення, grace і Kafka Streams належать до окремого потокового дослідження.")
    page()

    h("9 Аналітичні вікна retention та місткість")
    table(["Вікно, с", "Рядків за 7 діб", "Скорочення рядків, %", "Середнє очікування, с", "SQL, с"], [[row["window_seconds"],row["aggregate_rows"],num(row["row_reduction_pct"]),num(row["modeled_mean_wait_seconds"]),num(row["query_seconds"],3)] for row in warehouse["windows"]], [25*mm,37*mm,36*mm,40*mm,32*mm])
    p("Очікування у таблиці обчислено за event timestamps і межею закриття вікна; це модель затримки повноти агрегату, не вимір реального очікування протягом години. Вікно 300 с скорочує кількість рядків на 90%, але додає в середньому 165 с до появи повного агрегату. Для повільної аналітики рівня води це можливий компроміс; критичні сповіщення мають проходити окремим негайним потоком.")
    sub("Перевірка retention policies")
    table(["Retention, діб", "Тис. записів/с", "Процесний I/O write, МіБ", "Peak RSS брокера, МіБ"], [[row["retention_ms"]//86400000,num(row["rec_s"]/1000),num(row["broker_io_write_mib"]),num(row["broker_peak_rss_mib"])] for row in s["retention"]], [37*mm,38*mm,48*mm,47*mm])
    p("Налаштування 7/30/90 діб застосовано до окремих топіків і перечитано через Kafka ConfigCommand. Kafka record timestamp задавався під час відправлення, окремо від історичного timestamp усередині JSON. У короткому тесті термін зберігання ще не минув, тому видалення сегментів не спостерігалося. Ці дані не доводять різниці Disk I/O через expiration. Процесні I/O counters також не є фізичним записом SSD через системне кешування. На нативній Windows окремі операції видалення логів можуть упиратися у блокування mmap; у цій серії видалення не запускалося.")
    sub("Прогноз обсягу даних")
    table(["Діб", "Повідомлень", "JSON, МіБ", "Zstd stream .log, МіБ", "Zstd archive .log, МіБ"], [[row["days"],f"{row['records']:,}".replace(","," "),num(row["json_mib"]),num(row["natural_zstd_log_mib"]),num(row["archive_zstd_log_mib"])] for row in s["storage"]], [20*mm,35*mm,31*mm,42*mm,42*mm])
    p("Формула: N = 15/30 × 86 400 × retention_days; data_bytes = N × виміряні байти .log на запис. Потік і архів використовують відповідні природний та мікропакетний тести. Це екстраполяція невеликої синтетичної вибірки; індекси, файлові сегменти, DWH і резервні копії додаються окремо. Для плану з RF=3 і умовним запасом 20% місткість даних множиться на 3,6. Вартість = планові ГіБ × тариф за ГіБ-місяць; актуальний тариф постачальника не задавався і грошова ціна не вигадувалася.")
    page()

    h("10 Ресурси buffer.memory та max.in.flight")
    table(["Тест", "Значення", "Тис. записів/с", "Ack, мс", "Peak RSS брокера, МіБ"], [["buffer.memory",f"{row['buffer_memory']//2**20} МіБ",num(row["rec_s"]/1000),num(row["ack_ms"]),num(row["broker_peak_rss_mib"])] for row in s["buffer"]] + [["max.in.flight",row["max_in_flight"],num(row["rec_s"]/1000),num(row["ack_ms"]),num(row["broker_peak_rss_mib"])] for row in s["inflight"]], [36*mm,29*mm,35*mm,27*mm,43*mm])
    p("Це поодинокі допоміжні прогони, тому різницю не можна без додаткових повторів причинно приписати лише одному параметру. max.in.flight=1 обмежує число одночасних запитів; значення понад 1 із retries та вимкненою ідемпотентністю можуть змінити порядок при помилках. У production для цього потоку доцільно повернути enable.idempotence=true і max.in.flight≤5.")
    p("Клієнтський CPU виміряно через OperatingSystemMXBean у вікні основної передачі; один процес містить producer і consumer. Для брокера й ZooKeeper кожні 100 мс знімалися RSS і лічильники CPU/I/O; підсумок цих лічильників включає запуск клієнта й розігрів. RSS є пам'яттю процесу, а не тільки JVM heap. Вихідні мережеві байти producer взято з його Kafka metrics; вони містять протокольні витрати й не є навантаженням усієї NIC хоста.")
    sub("Межі інтерпретації")
    p("Усі процеси працюють на одному Windows-комп'ютері; немає затримок реальної мережі та реплікації між брокерами. Короткі saturation-прогони мають помітний min-max розкид і не відтворюють тривалу роботу production. Повторюваний корпус впливає на стискання. Вимірювання не включають відмови, деградацію диска та стале видалення сегментів за retention. Очікувані у методичці коефіцієнти не підставлялися замість отриманих результатів.")

    h("11 Рекомендації для ГЕС")
    table(["Сценарій", "Початкова конфігурація", "Обґрунтування"], [["Негайний моніторинг", "batch=16 КіБ; linger=0; none; key=device_id", "Низький offered rate не потребує великих пакетів; не чекати архівного вікна"], ["Оперативна телеметрія", "batch=64 КіБ; linger=10; lz4; 4 партиції", "Повну конфігурацію перевірено нижче при 500 записах/с і без rate limit"], ["Архів water_flow", "Вікно 300 с; batch до 512 КіБ; linger=100; zstd", f"150 повідомлень дали {num(archive['savings_pct'])}% batch savings; DWH-агрегати скорочують число рядків на 90%"], ["Тривале зберігання", "Kafka retention за оперативною потребою; окремий історичний архів", "Обсяг рахувати за реальним режимом пакетування; врахувати RF, індекси й резервні копії"]], [38*mm,65*mm,67*mm])
    p("Окрема перевірка оперативного кандидата: key=device_id, 4 партиції, batch=65536, linger=10, lz4, acks=all, enable.idempotence=true, max.in.flight=5. Кожен профіль повторено тричі; наведено медіани. Це працездатна перевірена конфігурація для заданого stress-профілю 500/с; оптимальність при природних 0,5/с не заявляється.")
    table(["Offered rate", "Повторів", "Записів/с", "Ack p95, мс", "E2E p95, мс"], [["Без обмеження" if row["throughput"] == -1 else "500/с", row["repeats"], num(row["rec_s"]), num(row["ack_p95_ms"],2), num(row["e2e_p95_ms"],2)] for row in s["recommended"]], [38*mm,27*mm,35*mm,35*mm,35*mm])
    p("Для розгортання із резервуванням потрібні кілька брокерів, RF=3 та узгоджене min.insync.replicas, наприклад 2. Ці параметри є планом для окремого середовища; їхню відмовостійкість у цьому RF=1 стенді не перевірено. Стабільний device_id зберігає локальність пристрою, а turbine_type краще використовувати для аналітичного групування, якщо три ключі обмежують використання партицій.")
    page()

    h("12 Висновки")
    p("Експериментально виконано всі основні комбінації batch/linger, порівняно п'ять кодеків, перевірено variant-specific та загальні кількості партицій, три формати серіалізації, керування buffer/in-flight та два обов'язкові load-сценарії. Задані кількості повідомлень прочитано й перевірено; автоматична валідація підтвердила повноту матриці та діапазони джерела.")
    p(f"Для підваріанта B головний результат полягає у зв'язку режиму пакетування зі зберіганням: zstd дав {num(natural['savings_pct'])}% batch savings на природній частоті та {num(archive['savings_pct'])}% у мікропакеті за 5 хвилин. SQL-агрегація семи діб у п'ятихвилинні вікна скоротила 302 400 рядків до 30 240 без втрати обліку подій, з очікуваним збільшенням часу готовності агрегату.")
    p("Рекомендовано відділяти негайний моніторинг від архівної аналітики, обирати retention за потрібним горизонтом повторного читання й рахувати місткість за відповідним режимом стиснення. Збільшення числа партицій без збільшення різноманітності ключів та незалежних споживачів не гарантує прискорення.")

    h("13 Відповіді на контрольні питання")
    answers = [
        ("Як batch.size впливає на latency і throughput", "Це верхня межа пакета на партицію. Великі пакети можуть зменшити число запитів і підвищити ефективність compression, але фактичний результат залежить від offered rate та linger. Черга при saturation також збільшує latency."),
        ("Для чого потрібен linger.ms", "Він дозволяє коротко накопичувати записи до надсилання неповного пакета. При малому потоці це додає очікування; при швидкому заповненні пакета очікувати весь linger не потрібно."),
        ("Коли використовувати compression", "Коли економія мережі й диска переважає CPU та затримку. Для одиничних рідких повідомлень виграш цього тесту невеликий; для історичних мікропакетів він суттєвий."),
        ("Як партиції впливають на масштабування", "Вони дозволяють розподіляти зберігання та роботу споживачів. Паралелізм обмежують брокери, consumer-и та кардинальність ключів; три turbine_type не завантажать вісім партицій рівномірно."),
        ("Які переваги Kafka для енергетичних систем", "Буферизація, відокремлення джерел від обробки, незалежні consumer groups і повторне читання збереженої телеметрії. Реальна відмовостійкість потребує відповідної реплікації."),
        ("Як забезпечити низьку latency алертів", "Не накопичувати їх у довгому архівному вікні, застосовувати малий linger і контролювати end-to-end p95/p99 та lag. Вимоги до промислового захисту перевіряються окремо від цього стенда."),
        ("Яка стратегія архівування оптимальна", "Для цього набору - zstd і прикладні мікропакети з окремими часовими агрегатами; сирі події зберігати за потребою аудиту й повторної обробки. Retention і резервування узгоджувати з місткістю."),
        ("Які trade-offs між кодеками", "Сильніше стискання може коштувати більше CPU. У цьому наборі zstd найкомпактніший, а lz4/snappy є кандидатами на швидшу обробку; показники залежать від пакетів та реалізації."),
        ("Коли потрібне key-based партиціонування", "Коли потрібні стабільна локальність та порядок у потоці одного пристрою. Ключ device_id має більше значень, ніж turbine_type, і зазвичай краще використовує кілька партицій."),
        ("Як моніторити Kafka у production", "Спостерігати throughput, request/error/retry rates, producer queue/buffer wait, consumer lag і rebalance, broker CPU/RSS, disk latency/space та мережу. Додати end-to-end latency й алерти; клієнтські ack-метрики самі по собі недостатні."),
    ]
    for index,(question,answer) in enumerate(answers,1):
        p(f"<b>{index}. {question}.</b> {answer}")
    page()

    h("Додаток А Команди та приклади даних")
    p("Команди ProducerPerformance та ConsumerPerformance виконуються через lab.py. Нижче наведено параметри навантажувальних тестів.")
    for row in load:
        code(f"python lab.py producer-perf --topic {row['topic']}\n  --num-records {row['records']} --record-size {row['record_bytes']} --throughput {row['target_rate']}\n  --producer-props bootstrap.servers=127.0.0.1:9092 compression.type=none")
    actual_json_topic = re.search(r"--topic (\S+)", (E / "official-json-performance.txt").read_text(encoding="utf-8")).group(1)
    code(f"python lab.py producer-perf --topic {actual_json_topic} --num-records 5000\n  --payload-file data/hydro-1000.jsonl --throughput 500\n  --producer-props bootstrap.servers=127.0.0.1:9092 batch.size=65536 linger.ms=10 compression.type=zstd")
    sub("Три згенеровані записи")
    for sample in read("sample-records.json"):
        sample_text = json.dumps(sample, ensure_ascii=False, indent=2)
        sample_text = re.sub(r'"location": \{\s*"lat": ([^,\n]+),\s*"lon": ([^\n]+)\s*\}', r'"location": {"lat": \1, "lon": \2}', sample_text)
        code(sample_text, keep=True)
    page(True)

    h("Додаток Б. Скріншоти результатів")
    p("На рисунках Б.1-Б.4 показано браузерний перегляд збережених результатів експерименту. Для порівнянь наведено медіани трьох повторів, обчислені з оригінального stdout. Повні результати окремих прогонів збережено в каталозі evidence/runs/lecture-review, вивід штатного perf-test - у official-json-performance.txt.")
    screenshots=[('01_batch_linger.png','Б.1. Залежність throughput та latency від batch.size і linger.ms.'),
        ('02_compression.png','Б.2. Порівняння кодеків і виміряна економія record batches.'),
        ('03_partitions.png','Б.3. Пропускна здатність та розподіл повідомлень за партиціями.'),
        ('04_cli_and_analytics.png','Б.4. Вивід ProducerPerformance, серіалізація та аналітичні вікна.')]
    from PIL import Image as PILImage
    for index,(name,caption) in enumerate(screenshots):
        if index:page(True);h('Додаток Б. Скріншоти результатів (продовження)')
        path=E/'screenshots'/name
        with PILImage.open(path) as im:height=170*mm*im.height/im.width
        story.append(Image(str(path),width=170*mm,height=height));story.append(Spacer(1,7));p('Рисунок '+caption,'small')
    p(f'Програмний код і журнали тестування: <link href="{REPO}" color="#175592">GitHub / lab1-kafka-lysenko</link>.')
    page()
    h("Джерела")
    sources = [
        "Надана методичка «ЛР 1 РБД.docx.pdf», 46 сторінок, варіант 3 та підваріант B.",
        '<link href="https://classroom.google.com/c/ODc2NDg1MTUwODA5" color="#175592">Матеріали курсу</link>: лекція 4 (конспект, с. 8-17: ключ, підтвердження, registry); лекція 6 (с. 18-22: rollup, TTL); лекції 2-3 та 7-11 (архітектурні й семантичні межі).',
        '<link href="https://kafka.apache.org/37/configuration/producer-configs/" color="#175592">Apache Kafka 3.7 Producer Configs</link>.',
        '<link href="https://kafka.apache.org/37/getting-started/quickstart/" color="#175592">Apache Kafka 3.7 Quick Start</link>.',
        '<link href="https://avro.apache.org/docs/1.12.0/specification/" color="#175592">Apache Avro 1.12 Specification</link>.',
        '<link href="https://protobuf.dev/programming-guides/encoding/" color="#175592">Protocol Buffers Encoding</link>.',
        "Власні експериментальні результати: каталог evidence у репозиторії.",
    ]
    p(" ".join(sources), "ref")

    def footer(canvas, doc):
        if doc.page == 1: return
        canvas.saveState()
        canvas.setFont("TimesUA", 9)
        canvas.setFillColor(colors.HexColor("#444444"))
        canvas.drawString(20*mm, 12*mm, "Лисенко Лука Сергійович · ТР-61МП · ЛР 1 · 3B")
        canvas.drawRightString(190*mm, 12*mm, str(doc.page))
        canvas.restoreState()

    destination = OUT / "Lab1_Lysenko_3B.pdf"
    document = SimpleDocTemplate(str(destination), pagesize=(210*mm,297*mm), rightMargin=20*mm, leftMargin=20*mm,
        topMargin=18*mm, bottomMargin=20*mm, title="Лабораторна робота 1 Kafka Варіант 3B", author=CONFIG["student"])
    document.build(story, onFirstPage=footer, onLaterPages=footer)
    print("REPORT=" + str(destination), flush=True)


if __name__ == "__main__":
    main()
