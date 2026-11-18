"""Build the Ukrainian PDF report from the captured CQL and Python results."""

import json
from pathlib import Path
from xml.sax.saxutils import escape

from PIL import Image, ImageDraw, ImageFont
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_RIGHT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import (
    Image as PdfImage, PageBreak, Paragraph, Preformatted, SimpleDocTemplate, Spacer, Table, TableStyle,
)


ROOT = Path(__file__).resolve().parent
EVIDENCE = ROOT / "evidence"
FONTS = Path("C:/Windows/Fonts")
NAVY = colors.HexColor("#253c50")


def terminal_image(log, destination, label):
    lines = log.rstrip().splitlines()
    font = ImageFont.truetype(str(FONTS / "consola.ttf"), 23)
    header = ImageFont.truetype(str(FONTS / "consolab.ttf"), 22)
    width = max(1000, int(max(font.getlength(line) for line in lines)) + 48)
    image = Image.new("RGB", (width, 76 + 28 * len(lines)), "#101820")
    draw = ImageDraw.Draw(image)
    draw.rectangle((0, 0, width, 42), fill="#243442")
    draw.text((23, 8), label, font=header, fill="#eef4f8")
    for i, line in enumerate(lines):
        color = "#9ce1bc" if line.startswith((">", "PASS:")) else "#e7edf2"
        draw.text((23, 53 + 28 * i), line, font=font, fill=color)
    image.save(destination)
    return width, image.height


def number(value, precision=2):
    return f"{value:,.{precision}f}".replace(",", " ").replace(".", ",")


def main():
    output = ROOT / "report"
    output.mkdir(exist_ok=True)
    figures = EVIDENCE / "figures"
    figures.mkdir(exist_ok=True)
    data = json.loads((EVIDENCE / "results.json").read_text(encoding="utf-8"))
    writes = data["writes"]
    comparisons = data["archive_queries"]
    histogram = (EVIDENCE / "nodetool.txt").read_text(encoding="utf-8-sig")
    histogram_bounds = [int(line.split()[4]) for line in histogram.splitlines()
                        if line.startswith("Max ")]
    turbine_max, station_max, season_max = histogram_bounds

    pdfmetrics.registerFont(TTFont("UA", str(FONTS / "times.ttf")))
    pdfmetrics.registerFont(TTFont("UABold", str(FONTS / "timesbd.ttf")))
    pdfmetrics.registerFont(TTFont("Code", str(FONTS / "consola.ttf")))
    pdfmetrics.registerFontFamily("UA", normal="UA", bold="UABold")
    styles = {
        "body": ParagraphStyle("Body", fontName="UA", fontSize=11.7, leading=15.2, spaceAfter=8, allowWidows=0, allowOrphans=0),
        "title": ParagraphStyle("Title", fontName="UABold", fontSize=21, leading=25,
                                alignment=TA_CENTER, spaceAfter=12),
        "center": ParagraphStyle("Center", fontName="UA", fontSize=13, leading=18,
                                 alignment=TA_CENTER, spaceAfter=6),
        "author": ParagraphStyle("Author", fontName="UA", fontSize=12, leading=17,
                                 alignment=TA_RIGHT, spaceAfter=4),
        "heading": ParagraphStyle("Heading", fontName="UABold", fontSize=16, leading=19,
                                  textColor=NAVY, spaceAfter=12),
        "section": ParagraphStyle("Section", fontName="UABold", fontSize=12.5, leading=16,
                                  textColor=NAVY, spaceBefore=8, spaceAfter=6),
        "cell": ParagraphStyle("Cell", fontName="UA", fontSize=10.2, leading=12.4),
        "header": ParagraphStyle("TableHeader", fontName="UABold", fontSize=10.2, leading=12.4,
                                 textColor=colors.white),
        "code": ParagraphStyle("CodeBlock", fontName="Code", fontSize=9, leading=12, spaceAfter=8,
                               backColor=colors.HexColor("#eef2f5"), borderPadding=6),
        "caption": ParagraphStyle("Caption", fontName="UA", fontSize=10.3, leading=13,
                                  textColor=NAVY, spaceBefore=6, spaceAfter=9),
    }
    story = []

    def p(text, style="body"):
        story.append(Paragraph(text, styles[style]))

    def table(headings, rows, widths):
        content = [[Paragraph(escape(str(cell)), styles["header"]) for cell in headings]]
        content += [[Paragraph(escape(str(cell)), styles["cell"]) for cell in row] for row in rows]
        t = Table(content, colWidths=[w * mm for w in widths], repeatRows=1, hAlign="LEFT")
        t.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), NAVY),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f0f4f7")]),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("TOPPADDING", (0, 0), (-1, -1), 6),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
            ("LINEBELOW", (0, -1), (-1, -1), .5, colors.HexColor("#a8b8c6")),
        ]))
        story.extend([t, Spacer(1, 9)])

    def code(lines):
        story.append(Preformatted(lines, styles["code"]))

    def console(name, label, caption):
        log = (EVIDENCE / f"{name}.txt").read_text(encoding="utf-8-sig")
        path = figures / f"{name}.png"
        width, height = terminal_image(log, path, label)
        draw_width = 166 * mm
        story.append(PdfImage(str(path), width=draw_width, height=draw_width * height / width,
                              hAlign="LEFT"))
        p(caption, "caption")

    story.append(Spacer(1, 11 * mm))
    p("НАЦІОНАЛЬНИЙ ТЕХНІЧНИЙ УНІВЕРСИТЕТ УКРАЇНИ<br/>"
      "«КИЇВСЬКИЙ ПОЛІТЕХНІЧНИЙ ІНСТИТУТ<br/>ІМЕНІ ІГОРЯ СІКОРСЬКОГО»", "center")
    p("Навчально-науковий інститут атомної та теплової енергетики<br/>"
      "Кафедра цифрових технологій в енергетиці", "center")
    story.append(Spacer(1, 28 * mm))
    p("ЛАБОРАТОРНА РОБОТА № 2", "title")
    p("з дисципліни «Проєктування систем з розподіленими<br/>базами даних в енергетиці»", "center")
    story.append(Spacer(1, 7 * mm))
    p("Основи Cassandra<br/>Створення першої енергетичної<br/>системи моніторингу", "title")
    p("Варіант 3 · Гідроелектростанції Дніпровського каскаду<br/>"
      "Підваріант Б · Порівняльний", "center")
    story.append(Spacer(1, 30 * mm))
    p("Виконав студент групи ТР-61МП<br/><b>Лисенко Лука Сергійович</b><br/><br/>"
      "Перевірив ________________________", "author")
    story.append(Spacer(1, 25 * mm))
    p("Київ 2026", "center")
    story.append(PageBreak())

    p("1. Мета та схема даних", "heading")
    p("Мета роботи — навчитися проєктувати таблиці Apache Cassandra під запити енергетичного "
      "моніторингу, виконувати одиничні й пакетні записи та аналізувати телеметрію через Python.")
    p("Для варіанта 3 змодельовано 15 гідроагрегатів у трьох групах по п’ять. Групи позначено KYIV, "
      "KANIV і DNIPRO. Ідентифікатори HYDRO_DN_001–015 відповідають нумерації з ЛР № 1. "
      "Усі параметри та розподіл агрегатів у групах синтетичні.")
    p("Розгорнуто один локальний вузол Apache Cassandra " + data["node"]["release_version"] +
      " на Windows із Java 11. Адреса CQL — 127.0.0.1:9042. Підключення перевірено cqlsh та "
      "Python " + data["python"] + " із драйвером " + data["driver"] + ".")
    code("CREATE KEYSPACE IF NOT EXISTS dnipro_hydro_lysenko\n"
         "WITH replication = {'class': 'NetworkTopologyStrategy',\n"
         "                    'datacenter1': 1};\n"
         "USE dnipro_hydro_lysenko;")
    p("Чотири види даних", "section")
    table(("Таблиця", "Ключ партиції", "Ключ кластеризації", "Призначення"),
          [
              ("turbine_readings", "(unit_id, day)", "ts DESC",
               "Рівень верхнього б’єфа, витрата води, напір і потужність агрегату за добу."),
              ("reservoir_readings", "(station_id, day)", "ts DESC",
               "Рівень, притік і температура води для водосховища станції."),
              ("station_daily", "(station_id, year)", "day DESC",
               "Добова генерація, середня та пікова сумарна потужність станції."),
              ("seasonal_by_station", "station_id", "season_year DESC, season ASC",
               "Сезонна історія конкретної станції з енергією, середньою та піком."),
              ("seasonal_by_season", "(season_year, season)", "station_id ASC",
               "Ті самі сезонні показники для порівняння всіх станцій за один сезон."),
          ], (39, 31, 39, 57))
    p("Сезонний архів складається з двох фізичних таблиць із однаковими 36 рядками. "
      "Запити до обох таблиць задають повний ключ партиції. Оперативні партиції обмежено добою, "
      "щоб приплив даних з наступних дат не збільшував їх.")
    story.append(PageBreak())

    p("2. Виконання команд у cqlsh", "heading")
    p("Через cqlsh виконано <b>schema.cql</b> і демонстраційні команди <b>demo.cql</b>. "
      "Один рядок записано окремим INSERT, два наступні — одним UNLOGGED BATCH. "
      "Усі три рядки мали однаковий ключ партиції DEMO / 2025-07-15.")
    console("cqlsh", "cqlsh  /  Apache Cassandra", "Рисунок 1. Збережений вивід cqlsh з фактичного прогону.")
    code("SELECT unit_id, ts, power_mw FROM turbine_readings\n"
         "WHERE unit_id = 'DEMO' AND day = '2025-07-15' LIMIT 3;")
    p("Запит повернув три записи від 00:30 до 00:00 UTC. Сортування задано у схемі "
      "кластеризацією ts DESC. Після перевірки видалено партицію DEMO, щоб вона не увійшла "
      "до статистики 15 агрегатів.")
    story.append(PageBreak())

    p("3. Генерація і запис даних через Python", "heading")
    p("Скрипт hydro.py повторно перевіряє схему командами IF NOT EXISTS і готує параметризовані "
      "INSERT. На 15.07.2025 UTC згенеровано 96 вимірювань на агрегат з кроком 15 хвилин і "
      "24 вимірювання на водосховище з кроком одна година. Потужність розраховано за "
      "P = 0,00981 × η × Q × H [МВт], де η = 0,90, Q — витрата [м³/с], H — напір [м].")
    console("python", "Python  /  hydro.py", "Рисунок 2. Збережений вивід Python з фактичного прогону.")
    p("Добова потужність станції — середнє 96 сум за п’ятьма агрегатами. Генерацію отримано "
      "множенням суми цих потужностей на 0,25 години. У добовій таблиці також записано "
      "модельні зведення за період 01.12.2022–30.11.2025. З них утворено чотири сезони "
      "на рік для кожної станції. Грудень належить до зими наступного року.")
    p("Скрипт прочитав число рядків у кожній таблиці, перевірив порядок телеметрії та звірив "
      "денні агрегати з вихідними вимірами з Cassandra. Кожен парний архівний запит також "
      "перевірено на збіг з очікуваними рядками.")
    story.append(PageBreak())

    p("4. Результати запису та порівняння ГЕС", "heading")
    table(("Таблиця", "Рядки", "Запити", "Час, с", "Рядків/с"),
          [(name, number(r["rows"], 0), number(r["requests"], 0), number(r["seconds"], 3),
            number(r["rows_per_s"], 0)) for name, r in writes.items()], (58, 25, 26, 25, 32))
    p("Загалом записано " + number(sum(row["rows"] for row in writes.values()), 0) +
      " фізичні рядки. Вимірювання охоплює надсилання запитів і отримання підтверджень, "
      "із граничною паралельністю 16 і рівнем узгодженості LOCAL_ONE. Схему, генерацію "
      "та підготовку запитів виключено з часу запису.")
    p("Для телеметрії застосовано по 12 INSERT в одному UNLOGGED BATCH у межах партиції. "
      "Наприклад, 1 440 рядків вимірювань агрегатів передано 120 запитами. Для добових і "
      "сезонних зведень використано окремі prepared INSERT. Таблиці мають різний обсяг і "
      "структуру запису, тому пропускні здатності наведено для опису цього прогону.")
    p("Добова статистика з Cassandra", "section")
    table(("Станція", "Середня, МВт", "Пік, МВт", "Генерація, МВт·год"),
          [(s, number(r["mean_mw"]), number(r["peak_mw"]), number(r["energy_mwh"]))
           for s, r in data["station_daily"].items()], (39, 36, 35, 56))
    kyiv, kaniv = (data["station_daily"][station]["mean_mw"] for station in ("KYIV", "KANIV"))
    delta = data["comparison"]
    p(f"<b>Підваріант Б.</b> За одну модельну добу середня сумарна потужність групи KYIV "
      f"становила {number(kyiv)} МВт, а групи KANIV — {number(kaniv)} МВт. "
      f"KANIV виробляла в середньому на {number(delta['difference_mw'])} МВт "
      f"({number(delta['difference_pct'], 1)} %) більше за однакового числа агрегатів. "
      "Різниця відображає параметри моделі, зокрема більшу витрату води та більший напір для KANIV.")
    story.append(PageBreak())

    p("5. Вибір первинного ключа архіву", "heading")
    p("Порівняно два запити з однаковим результатом у двох фізичних схемах. "
      "Для історії однієї ГЕС за рік зчитували чотири сезони, для огляду каскаду за сезон "
      "— три станції. У схемі, що вимагає кількох партицій, клієнт надсилав "
      "послідовні точкові запити до кожної з них.")
    names = {"station_year": "Рік однієї ГЕС", "cascade_season": "Сезон каскаду"}
    layouts = {"by_station": "За станцією", "by_season": "За сезоном"}
    table(("Запит", "Ключ", "Партицій", "Рядків", "p50, мс", "p95, мс"),
          [(names[scenario], layouts[layout], r["requests"], r["rows"], number(r["p50_ms"], 3),
            number(r["p95_ms"], 3)) for scenario, profiles in comparisons.items()
           for layout, r in profiles.items()], (41, 33, 23, 19, 25, 25))
    p("Після 10 прогрівальних повторів зафіксовано по 60 вимірювань на схему і запит. "
      "Порядок схем чергувався. Час охоплює мережевий обмін через loopback, декодування "
      "і збирання відповіді у Python. p50 — медіана, p95 — 57-ме значення у впорядкованій "
      "вибірці з 60 замірів.")
    ratio_station = (comparisons["station_year"]["by_season"]["p50_ms"]
                     / comparisons["station_year"]["by_station"]["p50_ms"])
    ratio_season = (comparisons["cascade_season"]["by_station"]["p50_ms"]
                    / comparisons["cascade_season"]["by_season"]["p50_ms"])
    p(f"Для історії окремої ГЕС ключ station_id скоротив число звернень із чотирьох до одного "
      f"і зменшив медіанний час у {number(ratio_station, 1)} раза. Для сезонного зрізу каскаду "
      f"ключ (season_year, season) скоротив число звернень із трьох до одного і зменшив "
      f"медіанний час у {number(ratio_season, 1)} раза.")
    p("Для диспетчерського порівняння станцій за один сезон основний ключ — "
      "(season_year, season). Для історичного профілю окремої ГЕС потрібна проєкція за station_id. "
      "У тесті партиція станції містить 12 сезонних рядків, партиція сезону — 3 рядки. "
      "За багаторічного зростання історії до ключа станції можна додати часовий бакет "
      "відповідно до діапазону типового запиту.")
    p(f"Після flush гістограми nodetool оцінили максимальний розмір партиції в SSTable "
      f"у {number(turbine_max, 0)} Б для turbine_readings, {number(station_max, 0)} Б "
      f"для seasonal_by_station і {number(season_max, 0)} Б для seasonal_by_season. "
      "Ці значення відповідають верхнім межам інтервалів гістограми.")
    p("Обидві таблиці мають однакові логічні дані, а дублікат додає витрати запису й зберігання. "
      "Один локальний вузол із RF=1 не дає оцінки міжвузлового трафіку, масштабування "
      "і поведінки за відмови. Результат порівняння латентності стосується послідовних запитів.")
    p("Зіставлення з лекціями 5, 6, 10 і 11", "section")
    p("Схема реалізує query-first: окрема таблиця для доби агрегата, добових підсумків і "
      "кожного архівного запиту. Бакет обчислюється в UTC. Добова партиція з 96 рядків "
      "обрана під запит за добу; для потоку з кроком 15 хвилин вона невелика. За читання "
      "переважно річних інтервалів доцільно окремо перевірити більший бакет, врахувавши "
      "число звернень і виміряний розмір партиції.")
    p("LOCAL_ONE визначає число підтверджень, а RF - число копій: це різні налаштування. "
      "За RF=1 неможливо перевірити кворумні чи міжвузлові гарантії. UNLOGGED BATCH "
      "використовується лише всередині однієї партиції й не є розподіленою ACID-транзакцією. "
      "Денормалізовані проєкції та їхній повторний запис звіряються програмою; атомарне "
      "оновлення всіх таблиць не заявляється.")
    p("Для потоку з пропусками в rollup слід зберігати count, sum, min і max та рахувати "
      "середнє як sum/count. Тут повноту всіх 96 вимірів доби перевірено перед порівнянням. "
      "TTL і TWCS потрібні для окремої політики зберігання часових рядів; у цій роботі "
      "закінчення TTL та видалення SSTable не моделювалися.")

    p("6. Контрольні питання та висновки", "heading")
    answers = [
        ("Partition key і clustering key", "Хеш ключа партиції визначає токен, за яким кластер "
         "обирає репліки. Ключ кластеризації унікалізує й упорядковує рядки всередині партиції."),
        ("Денормалізація", "Cassandra моделює таблиці під конкретні запити. Копіювання потрібних "
         "полів у проєкції дозволяє виконувати читання з однієї таблиці без JOIN. Підтримка "
         "узгодженості проєкцій лишається завданням запису."),
        ("Ключ часового ряду", "Вибирають ідентифікатор джерела та часовий бакет за частотою "
         "запису, розміром рядка і типовим діапазоном читання. Мітку часу кладуть у кластерний ключ. "
         "Коли час може збігатися, додають ідентифікатор події."),
        ("Реплікаційний фактор", "RF задає кількість копій партиції. RF=1 має одну копію. "
         "RF=3 дає три копії за наявності достатньої кількості вузлів, а доступність операцій "
         "під час відмов залежить від заданого consistency level."),
        ("Сортування timestamp DESC", "Нові записи опиняються першими в партиції. "
         "Для останніх N вимірювань достатньо обмеженої вибірки за ключем і LIMIT."),
        ("Завелика партиція", "Перевіряють розподіл розмірів і кількості комірок через "
         "nodetool tablehistograms, сигнали в логах та p95/p99 запитів. Допустиму межу "
         "визначає навантаження. За потреби зменшують часові бакети."),
        ("Переваги для потокової енергетики", "Записи розподіляються між вузлами за токенами, "
         "реплікація підтримує роботу за відмов, а ключі й упорядкування оптимізують часові "
         "вибірки. Ціна — дублювання даних та обмеження довільних аналітичних запитів."),
    ]
    for i, (title, answer) in enumerate(answers, 1):
        p(f"<b>{i}. {title}.</b> {answer}")
    p("Висновки", "section")
    p("1. Для оперативного моніторингу ключ «агрегат / доба» дав 15 обмежених партицій "
      "по 96 рядків. Добові зведення дають середню потужність і генерацію без повторного "
      "читання телеметрії.")
    p("2. Архівний ключ визначається запитом. Для аналізу каскаду за сезон вигідніше "
      "групувати за сезоном, а для історії окремої ГЕС — за станцією.")
    p("3. Завантаження, перевірка агрегатів і порівняння ГЕС відтворюються на локальній Cassandra. "
      "Оцінка продуктивності на промисловому кластері потребує вимірювань на кількох вузлах.")
    p('Джерела — методичка ЛР № 2; лекції 5 (query-first), 6 (UTC і бакети, с. 11-14), '
      '10-11 (RF та consistency level) з '
      '<link href="https://classroom.google.com/c/ODc2NDg1MTUwODA5" color="#28597a">курсу</link>; документація Apache Cassandra '
      '<link href="https://cassandra.apache.org/doc/stable/cassandra/developing/cql/ddl.html" color="#28597a">CQL DDL</link>, '
      '<link href="https://cassandra.apache.org/doc/stable/cassandra/developing/cql/dml.html" color="#28597a">CQL DML</link>, '
      '<link href="https://cassandra.apache.org/doc/latest/cassandra/troubleshooting/use_nodetool.html" color="#28597a">nodetool</link>.', "caption")

    def footer(canvas, doc):
        if doc.page > 1:
            canvas.setFont("UA", 9)
            canvas.setFillColor(NAVY)
            canvas.drawString(22 * mm, 11 * mm, "ЛР № 2  ·  Лисенко Лука  ·  ТР-61МП")
            canvas.drawRightString(188 * mm, 11 * mm, str(doc.page))

    target = output / "Lab2_Lysenko_3B.pdf"
    doc = SimpleDocTemplate(str(target), pagesize=A4, leftMargin=20 * mm, rightMargin=20 * mm,
                            topMargin=17 * mm, bottomMargin=17 * mm,
                            title="Лабораторна робота № 2. Основи Cassandra",
                            author="Лисенко Лука Сергійович")
    doc.build(story, onFirstPage=footer, onLaterPages=footer)
    print(target)


if __name__ == "__main__":
    main()
