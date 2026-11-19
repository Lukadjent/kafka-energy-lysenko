"""Build three evidence-driven reports. Never substitute methodical example numbers."""
import argparse,csv,math,statistics
from common import *
from pdf_tools import Report,fmt
import matplotlib;matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np

def project(n):return WORKSPACE/{3:'lab3-cassandra-optimization-lysenko',4:'lab4-kafka-streams-lysenko',5:'lab5-monitoring-lysenko'}[n]
def load(n,name):return read_json(project(n)/'evidence'/name)
def figdir(n):
    p=project(n)/'evidence/figures';p.mkdir(exist_ok=True);return p
def stats(values):
    s=sorted(values);return [statistics.mean(s),statistics.median(s),s[math.ceil(.95*len(s))-1],s[math.ceil(.99*len(s))-1]]
def plot_bars(path,labels,values,ylabel):
    fig,ax=plt.subplots(figsize=(9,3.7));ax.bar(labels,values,color=['#087f8c','#226991','#586ba4']*(len(values)//3+1));ax.set_ylabel(ylabel);ax.grid(axis='y',alpha=.2);ax.set_axisbelow(True);fig.tight_layout();fig.savefig(path,dpi=160);plt.close(fig)

def lab3():
    folder=project(3);e=folder/'evidence';f=figdir(3);r=Report(3,'Оптимізація схеми даних та запитів Apache Cassandra',folder.name)
    writes=load(3,'write-benchmark.json');reads=load(3,'read-benchmark.json');multi=load(3,'multi-partition.json');mv=load(3,'index-mv.json');weekly=load(3,'weekly-analysis.json');maintenance=load(3,'maintenance.json')
    r.page('1. Завдання та відтворюваний стенд')
    r.p('Мета: порівняти три схеми часових рядів, перевірити індекси й матеріалізовані подання та оптимізувати аналітику кількох партицій. Варіант 3Б: витрата та рівень води, типи турбін, координація каскаду ГЕС.')
    r.p('Apache Cassandra 4.0.21: три окремі JVM Java 11 на 127.0.0.11–13, один DC dc1, різні rack1–3. NetworkTopologyStrategy, RF=3; читання й запис LOCAL_QUORUM. Кожна JVM має heap до 1 GiB. Усі вузли ділять один Windows-комп’ютер і диск: це перевірка реплік та CQL, а не доказ незалежної відмовостійкості фізичних хостів.')
    r.p('Набір синтетичний: 15 пристроїв × 30 діб × 2 880 подій/добу = 1 296 000 записів у КОЖНІЙ схемі; період 01–30.09.2026 UTC, крок 30 с. Сукупно 3 888 000 базових рядків. Повні COUNT(*) підтверджені stdout. SHA-256 CSV: '+load(3,'dataset.json')['sha256']+'.','small')
    r.flow([['Детермінований CSV','1 296 000 подій'],['Три схеми','RF=3, QUORUM'],['Індекси, два MV','тижневий rollup']])
    r.p('Перед завантаженням кожну базову таблицю очищено. Запис виконується UNLOGGED batch до 60 рядків однієї партиції, максимум 8 запитів одночасно. Серверний write timeout - 20 с. У швидкість входять читання CSV, формування batch і очікування підтверджень; генерація CSV не входить.')
    r.page('2. Фізичні ключі та методика вимірювань')
    r.table(['Схема','Partition key','Clustering','Рядків / логічну партицію'],[['simple','device_id','event_time DESC','86 400 за 30 діб'],['hourly','(device_id, hour UTC)','event_time DESC','120'],['daily','(device_id, day UTC)','event_time DESC','2 880'],['weekly_rollup','station_id','week DESC','mergeable агрегати']], [30,45,40,55])
    r.p('У трьох базових таблицях однакові поля та LZ4, chunk 16 KiB. Без bucket пристрій накопичує історію необмежено; на цьому 30-денному наборі він ще не обов’язково великий. Щогодинна схема збільшує кількість запитів для широкого діапазону; добова групує повільні гідрологічні зміни.')
    r.table(['Запит','Склад результату','Запитів simple / hourly / daily'],[['latest','Останні 100 у межах останньої години','1 / 1 / 1'],['six_hours','720 подій за 6 годин','1 / 6 / 1'],['daily','2 880 подій за останню добу','1 / 24 / 1'],['filtered','1 801 high-подія пристрою за добу','1 / 24 / 1']], [27,94,49])
    r.p('П’ять warm-up на кожен план, далі по 60 вимірів у перемішаному порядку (seed=13). Таймер охоплює повне отримання результату й усі сторінки, перевірка digest виконується після таймера. Для baseline погодинні bucket читаються послідовно; окремий тест порівнює паралельне читання тих самих шести bucket. Percentile: nearest-rank; p50 - медіана. read-samples.csv містить усі 720 основних вимірів.')
    dailyagg=load(3,'daily-aggregation.json')
    context='Окрему додаткову серію знято після створення MV; вона не замінює попередній raw-read baseline.' if dailyagg['measured_after_mv_creation'] else 'Серію знято до створення MV.'
    r.p('<b>Окрема добова агрегація:</b> реальні CQL COUNT/SUM/MIN/MAX з повним злиттям 24 hourly-результатів; 5 warm-up і 60 вимірів для кожної схеми. Count=2880, суми й min/max звірено незалежно з CSV. '+context,'small')
    r.table(['Схема','CQL-запитів','avg / p50 / p95 / p99, мс'],[[x['table'],x['requests'],' / '.join(fmt(x[k]) for k in ['avg_ms','p50_ms','p95_ms','p99_ms'])] for x in dailyagg['results']],[27,35,108])
    r.page('3. Швидкість запису та затримка читання')
    r.table(['Схема','Записів/с','Час, с','Batch p50 / p95 / p99, мс'],[[x['table'],fmt(x['rows_per_second'],0),fmt(x['load_seconds']),f"{fmt(x['batch_ack']['p50_ms'])} / {fmt(x['batch_ack']['p95_ms'])} / {fmt(x['batch_ack']['p99_ms'])}"] for x in writes],[27,34,27,82])
    r.table(['Схема / запит','avg','p50','p95','p99, мс'],[[x['table']+' / '+x['query'],*[fmt(x[k]) for k in ('avg_ms','p50_ms','p95_ms','p99_ms')]] for x in reads],[62,27,27,27,27])
    r.p('Batch ack - затримка запиту до підтвердження репліками, а не затримка одного рядка; ділити її на 60 як «реальну latency події» некоректно. Поріг p95 &lt;20 мс оцінюється окремо для кожного читання. Масові відповіді та 24 послідовні запити можуть не відповідати цілі.','small')
    plot_bars(f/'write-throughput.png',[x['table'] for x in writes],[x['rows_per_second'] for x in writes],'Rows / second')
    r.page('4. Порівняння схем і кількох партицій');r.figure(f/'write-throughput.png','Рис. 1. Фактичний load throughput. Три послідовні прогони, без одночасного стороннього навантаження від ЛР4/5.')
    fig,ax=plt.subplots(figsize=(9,3.6));kinds=['latest','six_hours','daily','filtered'];x=np.arange(4)
    for i,table in enumerate(['simple','hourly','daily']):ax.bar(x+(i-1)*.25,[next(v['p95_ms'] for v in reads if v['table']==table and v['query']==q) for q in kinds],width=.25,label=table)
    ax.set_xticks(x,kinds);ax.set_ylabel('p95, ms');ax.legend();ax.grid(axis='y',alpha=.2);fig.tight_layout();fig.savefig(f/'read-p95.png',dpi=160);plt.close(fig);r.figure(f/'read-p95.png','Рис. 2. p95 повного отримання результату. Той самий набір рядків звірено SHA-256.')
    r.table(['Шість hourly bucket','avg / p50 / p95 / p99, мс'],[[k,' / '.join(fmt(v) for v in [x['avg_ms'],x['p50_ms'],x['p95_ms'],x['p99_ms']])] for k,x in multi.items()],[62,108])
    r.p(f"Паралелізація зменшила середній час з {fmt(multi['sequential']['avg_ms'])} до {fmt(multi['parallel']['avg_ms'])} мс. Це fan-out із шістьма одночасними запитами, тож нижча затримка одного клієнта коштує додаткової одночасної роботи сервера.",'small')
    r.page('5. Вторинний індекс і матеріалізовані подання')
    r.p('На daily перевірено запит level_band без індексу: Cassandra відхилила його без ALLOW FILTERING. Після CREATE INDEX daily_level_idx той самий предикат виконується без ALLOW FILTERING. Індекс низької кардинальності не є універсальним рішенням; результат виміру важливіший за очікуване «прискорення».')
    r.p('MV1 by_level_band: ((level_band, day), device_id, event_time) - критичні рівні. MV2 by_type_hour: ((type_hour), day, device_id, event_time) - координація однакового типу турбін за годину; type_hour = тип + UTC-година. Обидва - реальні CREATE MATERIALIZED VIEW, а не ручні копії. Побудову завершено на всіх трьох репліках (SUCCESS).')
    r.p('У Cassandra 4.0 предикат MV за звичайним стовпцем може містити IS NOT NULL, але не water_level &gt; 18.3. Спробу такого CQL реально відхилено; рівень класифікується генератором у level_band. У кожному MV збережено всі компоненти primary key бази та додано один non-key стовпець. Пороги 18,3 / 15,5 м є умовами синтетичної моделі.')
    r.table(['План','Рядків','avg / p50 / p95 / p99, мс'],[[k,x['rows'],' / '.join(fmt(x[v]) for v in ('avg_ms','p50_ms','p95_ms','p99_ms'))] for k,x in mv.items() if isinstance(x,dict)],[62,20,88])
    r.p(f"Побудова двох MV тривала {fmt(mv['view_build_seconds'])} с. MV додають запис, диск і eventual convergence; перед вимірами стан і повноту результатів звірено. Складний запит по типу турбіни порівнює один MV з паралельним читанням п’яти партицій бази.")
    r.page('6. Тижнева аналітика, розмір і компресія')
    r.p(f"З CSV незалежно сформовано {weekly['groups']} груп station/week. Для KYIV, 21–27.09: 5 пристроїв × 7 діб = 35 партицій та 100 800 рядків. Суми витрати, min/max рівня, count і енергію звірено з weekly_rollup. Енергія = ΣP(MW) × 30/3600; сума витрати не є об’ємом без множення на інтервал.")
    r.table(['Аналітика (10 повторів)','avg / p50 / p95 / p99, мс'],[[k,' / '.join(fmt(x[v]) for v in ('avg_ms','p50_ms','p95_ms','p99_ms'))] for k,x in weekly['results'].items()],[70,100])
    allstats=[load(3,f'nodetool-tablestats-node{i}.json') for i in (1,2,3)]
    storage=[]
    for node,data in enumerate(allstats,1):
        tables=data['energy_l3_lysenko']['tables']
        for name in ('simple','hourly','daily'):
            x=tables[name];ratio=x.get('sstable_compression_ratio')
            storage.append([node,name,fmt(float(x['space_used_live'])/1048576),fmt(float(x['compacted_partition_maximum_bytes'])/1048576,3),fmt(float(ratio),4),fmt((1-float(ratio))*100)])
    r.table(['Вузол','Таблиця','Live MiB','Max partition MiB','Ratio','Економія %'],storage,[15,25,30,39,26,35])
    r.p('Джерело: nodetool tablestats після flush і після створення MV/індексу. Це знімок фактичного SSTable-стану; min/mean/max і histogram bounds повністю збережені в raw-файлах. Компресійний ratio - compressed/uncompressed, економія = (1-ratio)×100%; size_on_disk/CSV не видається за compression ratio. Індекс та два MV зберігаються додатково; одна RF=3-копія в кожному з трьох вузлів збільшує фізичний обсяг.','small')
    r.page('7. Розподіл партицій та вибір bucket')
    r.table(['Вузол / таблиця','Оцінка партицій','Min / mean / max, KiB'],[[str(i)+' / '+name,str(data['energy_l3_lysenko']['tables'][name]['number_of_partitions_estimate']),
        ' / '.join(fmt(data['energy_l3_lysenko']['tables'][name][key]/1024) for key in ['compacted_partition_minimum_bytes','compacted_partition_mean_bytes','compacted_partition_maximum_bytes'])]
        for i,data in enumerate(allstats,1) for name in ('simple','hourly','daily')],[50,42,78])
    r.p('Це оцінки Cassandra за поточними SSTables, а не точний COUNT(DISTINCT partition key). Одна логічна партиція може мати фрагменти в кількох SSTables; гістограма й estimate відрізняються від кількості логічних ключів (15 / 10 800 / 450). Досягнуто межі &lt;100 MiB для базових схем. Індекс level_band має лише три значення і може накопичувати широкі індексні партиції: це додатковий аргумент проти його неконтрольованого росту.')
    r.bucket_tree()
    r.p('Для цього набору обрано daily: повільні зміни та запити до доби. Simple допустима лише за обмеженої історії; weekly rollup зберігає count/sum/min/max і не замінює деталізацію raw.','small')
    r.page('8. Експлуатація та висновки')
    passed=sum(x['p95_ms']<20 for x in reads)
    r.p(f'З 12 основних комбінацій лише {passed} мають p95 &lt;20 мс; загальний критерій швидкодії не виконано для всіх запитів. На цьому наборі simple може бути швидким: його 30-денна історія ще невелика. Перевага daily - керований ріст і менше bucket-запитів порівняно з hourly, а не гарантія найменшого часу кожного SELECT.')
    r.p('Після вимірів daily переведено на TWCS (1 день) і default TTL 30 діб. Це політика майбутніх записів: ALTER не надає TTL вже наявним рядкам. Окремий TTL 3 с реально перевірено до/після зникнення. Змішувати старі безстрокові записи з TTL у production слід через контрольовану міграцію; tombstone/gc_grace не налаштовано агресивно.')
    r.p('На кожному вузлі виконано flush, compactionstats і snapshot lab3-3b; збережені manifests та розміри. Snapshot - локальна резервна копія, не перевірка restore і не незалежний off-host backup. Потрібні окреме сховище та тест відновлення. Реплікація не замінює backup.')
    r.p('За RF=3 на трьох вузлах усі дані реплікуються на кожен вузол; близький обсяг SSTable очікуваний. Це не доказ відсутності гарячого ключа під будь-яким навантаженням: fan-out, рівномірність пристроїв і реплікаційний обсяг перевірено лише для цього синтетичного набору. Моніторити p95/p99, dropped mutations, pending compactions, heap/GC, розміри партицій і дисковий запас; алерти реалізовано у ЛР5.')
    r.page('9. Контрольні питання')
    answers=[('1. Partition key і clustering key','Partition key визначає token, репліки й межу читання. Clustering key сортує рядки всередині вже вибраної партиції; не виправляє невдалий розподіл даних.'),('2. Bucketing і wide rows','Обмежує рядки та байти в партиції часовим відрізком. Bucket вибирають за частотою, розміром рядка й діапазоном запиту, а не лише за календарем.'),('3. Коли MV доречні','Для стабільного альтернативного primary key. Ціна - write amplification, диск, складніша експлуатація та асинхронна побудова/узгодження; Cassandra 4.0 позначає MV експериментальними.'),('4. Partition tombstones','Позначки видалення всієї партиції. Читання мусить врахувати їх під час злиття SSTables; велика кількість видалень і прострочених TTL може збільшувати latency.'),('5. TWCS','Групує SSTables за часовими вікнами та полегшує видалення повністю прострочених файлів. Найкраще працює для впорядкованих за часом TTL-даних без змішування дуже старих і нових записів.'),('6. WHERE за звичайним стовпцем','Без індексу/відповідного ключа Cassandra не може цільово знайти рядки й зазвичай вимагає ALLOW FILTERING; це згода на потенційно дорогу фільтрацію, а не оптимізація.'),('7. Hourly чи daily для сонячних станцій','Для високочастотних денних вимірювань і коротких запитів hourly часто доречний; daily зменшує fan-out добового звіту. Рішення підтверджують обсягом партицій і вимірами конкретного набору.'),('8. Вплив RF','Більше реплік збільшує запис/мережу/диск і потенційну доступність. Читання залежить від CL та кількості відповідей; RF саме по собі не гарантує прискорення. LOCAL_QUORUM за RF=3 потребує двох відповідей.')]
    for title,text in answers:r.p('<b>'+title+'.</b> '+text)
    r.p('Порядок запуску, CQL-схеми та конфігурації експерименту наведено в репозиторії. Для повторного порівняння потрібна початкова схема без індексу та MV.','small')
    r.sources([('Cassandra: Data modeling','https://cassandra.apache.org/doc/4.0/cassandra/data_modeling/intro.html'),('CQL Materialized Views: restrictions','https://cassandra.apache.org/doc/4.0/cassandra/cql/mvs.html')]);r.finish()

def lab4():
    folder=project(4);f=figdir(4);r=Report(4,'Потокова обробка Kafka Streams та розподілена Saga',folder.name)
    streams=load(4,'stream-trials.json');delivery=load(4,'delivery-trials.json');sagas=load(4,'sagas.json');sink=load(4,'sink.json')
    r.page('1. Завдання, дані та архітектура')
    r.p('Варіант 3Б: шість модельних ГЕС, hopping windows 30 хв / крок 10 хв; координація витрат, прогноз рівня та трикрокова Saga. Використано Apache Kafka Streams 3.7.1 / Java 17, RocksDB JNI на Windows та Cassandra 4.0.21 (RF=3, LOCAL_QUORUM). Один Kafka broker - навчальний стенд, без брокерної відмовостійкості.')
    r.p('Шість джерел із кроком 10 с дають 0,6 події/с, а не точно 1 подію/с. Для тесту 3 години event time відтворено прискорено: 6 480 коректних подій, три невалідні й один watermark tick. Поля: верхній/нижній рівень, витрата, потужність, положення затвору, ККД. Усі значення синтетичні; назви ГЕС - логічні ідентифікатори задачі. KAKHOVKA_MODEL позначає модель, а не твердження про роботу реальної станції у 2026 році.')
    r.flow([['Kafka input + reference','GlobalKTable join'],['Filter, anomalies','RocksDB 30/10 хв'],['Kafka read_committed','Cassandra + Saga']])
    r.p('Вхідний ключ DNIPRO забезпечує порядок у каскаді; одна партиція означає один активний task, а не необмежене горизонтальне масштабування. Довідник станцій приєднується через GlobalKTable. Невалідні значення відокремлюються; аномалії upper&gt;95 або η&lt;0,88 надходять до окремого topic.')
    r.p('Обробку реалізовано на Java Kafka Streams. Стан вікон зберігається у persistent RocksDB store; changelog використовується для відновлення після збою.')
    r.page('2. Вікна, історія та модель оптимізації')
    r.p('TimeWindows.ofSizeAndGrace(30 min, 1 min).advanceBy(10 min). Кожна подія належить трьом вікнам; через крайові часткові вікна отримано 20 результатів і 19 440 входжень. Suppress.untilWindowCloses видає закриті вікна після grace. Фінальний tick просуває stream time, але не збільшує count. Без нових подій stream time не є настінним годинником.')
    r.p('Persistent WindowStore hydro-history містить count, суми витрат/потужності/рівня/ККД, першу й останню точку кожної станції. Прогноз на 10 хв: lastUpper + (lastUpper-firstUpper)/(lastTs-firstTs) × 600000. Це проста лінійна модель історії, а не гідродинамічний прогноз або оцінка його точності на реальних спостереженнях.')
    r.p('Спрощена задача координації: для однакової каскадної витрати q мінімізувати Σwᵢ(q-dᵢ)², де dᵢ = meanFlowᵢ + 20(predictedLevelᵢ-targetLevelᵢ), 700≤q≤1900 м³/с. Ваги 1,0…2,0. Розв’язок q*=clip(Σwᵢdᵢ/Σwᵢ). Виводиться і значення цільової функції. Модель не враховує час добігання, площі водосховищ, притоки й реальні обмеження затворів; практичні команди ГЕС з неї не формуються.')
    r.p('Незалежний Python oracle повторно обчислює всі 20 вікон з початкового масиву подій. Перевіряються кожен count, середні значення, усі шість прогнозів, coordinated_flow та objective з допуском 1e-9 / 1e-7. Множина anomaly IDs і кількість invalid також звіряються.')
    r.table(['Параметр','Реалізація'],[['processing.guarantee','exactly_once_v2 або at_least_once'],['commit.interval.ms','100; state cache вимкнено для явного тесту'],['state.dir / application.id','Окремі для кожного нормального прогону; ті самі під час recovery'],['Вихідний consumer','enable_auto_commit=False, isolation_level=read_committed']], [50,120])
    r.page('3. Виміряна робота та відновлення Streams')
    r.table(['Гарантія / повтор','Час, с','Подій/с','Вікна / аномалії','Restart'],[[x['mode']+(' / recovery' if x['recovery'] else ' / '+str(i%3+1)),fmt(x['seconds_until_all_outputs_read']),fmt(x['valid_events_per_second'],1),f"{x['windows']} / {x['anomalies']}",x['restarts']] for i,x in enumerate(streams)],[61,24,28,38,19])
    r.p('Це wall-clock прискореного replay від початку producer.send до читання всіх закритих вікон; включає flush, Streams та polling. Він не дорівнює latency окремої події і не означає очікування 30 реальних хвилин. Три нормальні повтори кожної гарантії, один crash/restart EOS.')
    r.p('Примусовий Runtime.halt(17) виконується після state update/suppress перед третьою відправкою output і до завершення транзакції. Процес запускається з тим самим application.id та state.dir. Усі 20 вікон після recovery збігаються з oracle; немає дублікатів у read_committed output. Логи restart містять реальне відновлення task/store. Це перевірка збою процесу, а не втрати всього диска чи broker.')
    plot_bars(f/'stream-throughput.png',[('EOS' if x['mode']=='exactly_once_v2' else 'ALO')+('-R' if x['recovery'] else str(i%3+1)) for i,x in enumerate(streams)],[x['valid_events_per_second'] for x in streams],'Valid events / second')
    r.figure(f/'stream-throughput.png','Рис. 1. Replay throughput із врахуванням вихідного polling; R - примусовий crash і restart.')
    r.page('4. Три семантики доставки: перевірка зі збоєм')
    r.p('Kafka Streams підтримує at_least_once та exactly_once_v2. At-most-once не є допустимим значенням processing.guarantee: три семантики окремо порівняно стандартними Java KafkaConsumer/KafkaProducer на тих самих 1 000 ID та batch по 100.')
    r.table(['Семантика','Порядок операцій','Результат після збою'],[['at_most_once','commit offsets → send','Перша сотня втрачена'],['at_least_once','send + flush → commit offsets','Перша сотня дублюється'],['exactly_once','beginTxn → send → offsetsToTxn → commitTxn','Перша незавершена сотня прихована read_committed']], [36,76,58])
    r.table(['Сценарій','Input','Committed','Lost','Duplicate','Uncommitted'],[[x['mode']+(' / crash' if x['fault'] else ' / normal'),x['input'],x['committed_output'],x['missing'],x['duplicates'],x['uncommitted_output']] for x in delivery if x['fault'] or x['name'].split('-')[2]=='0'],[55,20,26,20,24,25])
    for mode in ('at_most_once','at_least_once','exactly_once'):
        trials=[x for x in delivery if x['mode']==mode and not x['fault']];values=[v for x in trials for v in x['timing']['batch_ms']]
        r.p(f"<b>{mode}</b>: 3 нормальні прогони; batch ack/commit avg / p50 / p95 / p99 = {' / '.join(fmt(v) for v in stats(values))} мс; середній час процесу з startup = {fmt(statistics.mean(x['seconds_including_startup_restart'] for x in trials))} с.")
    r.p('Fault injection: at-most після offset commit до send; ALO після flush до offset commit; EOS після flush до commitTransaction. Restart продовжує той самий group/transactional.id. У EOS read_uncommitted бачить 1 100 записів, а read_committed - 1 000: 100 записів незавершеної транзакції не видаються за валідний результат.','small')
    r.page('5. Cassandra sink і межа exactly-once')
    r.table(['Таблиця','Ключ / призначення'],[['cascade_flow_aggregates','((run_id), window_start): JSON закритого вікна'],['station_coordination','((run_id, station), window_start): прогноз і q*'],['anomalies','((run_id), event_id): схема для ідемпотентних аномалій'],['saga_state / saga_log','saga_id; (saga_id, sequence): durable checkpoint та audit'],['participant','station_id: модель стану + version + command IDs']], [57,113])
    r.p(f"Manual-commit sink: {sink['write_attempts']} спроб upsert, {sink['unique_rows']} унікальних вікон. Перший consumer закривається після запису всієї пачки до Cassandra, але до Kafka commit; другий читає її повторно. Deterministic primary key зберігає 20 вікон замість 40 дублікатів; для кожного вікна записано шість station_coordination.")
    r.p('Kafka exactly-once атомарно охоплює Kafka output, changelog і consumer offsets. Зовнішній запис Cassandra до цієї транзакції не належить. Тут досягнуто ідемпотентного кінцевого стану через upsert та commit лише після успіху БД; це не загальна розподілена транзакція Kafka+Cassandra. Часткове застосування між таблицями під час збою усувається replay, а не миттєвою атомарністю.')
    r.p('Для подальшого розвитку sink доцільно додати контроль версій, DLQ та політику retry/backoff. Невдалий запис не повинен спричиняти підтвердження Kafka offset.')
    r.page('6. Saga: успіх, компенсація та повтор команди')
    r.flow([['1. PREPARE_UPPER','підготувати модель'],['2. OPEN_GATES','20% → 60%'],['3. ACK_LOWER','підтвердити']])
    r.p('Оркестратор зберігає next_step/status у Cassandra. Кожен actuator command має детермінований ID. Стан учасника, version і карта виконаних command IDs оновлюються ОДНИМ conditional UPDATE IF version=old, LOCAL_SERIAL + LOCAL_QUORUM: ефект та дедуплікація атомарні в одній партиції. Окремий «журнал ідемпотентності» без такої атомарності залишив би crash gap.')
    r.table(['Сценарій','Status','Gate','Version','Audit rows'],[[x['scenario'],x['state']['status'],x['participant']['gate'],x['participant']['version'],len(x['log'])] for x in sagas],[40,45,25,25,35])
    r.p('Успіх: підготовка верхньої ГЕС → відкриття → підтвердження нижньої. Помилка підтвердження: у зворотному порядку COMPENSATE_OPEN (20%) та COMPENSATE_PREPARE (false), кінцевий status COMPENSATED. Crash: процес завершується після ефекту OPEN_GATES до checkpoint; повтор OPEN_GATES повертає deduplicated=true, version лишається 2, далі Saga завершується.')
    r.p('Це навчальні учасники в Cassandra, а не підключення до SCADA. Компенсація повертає задані gate/prepared, але не може фізично повернути вже скинуту воду. Saga забезпечує послідовність локальних транзакцій та компенсувальні дії, а не ACID-ізоляцію всього каскаду. Для реальних затворів потрібні незалежні перевірки, фізична модель та участь оператора.')
    r.page('7. Dashboard, висновки та відтворення')
    screenshot=project(5)/'evidence/screenshots/streams.jpg'
    if screenshot.exists():r.figure(screenshot,'Рис. 2. Реальний Grafana dashboard ЛР4: фактично перевірені вікна/Saga та живий Kafka → Cassandra pipeline.',170)
    r.p('Підтверджено stateless-фільтрацію, GlobalKTable join, persistent window aggregation, прогноз із історії, модельний оптимум, EOS recovery, ідемпотентний Cassandra sink і три сценарії Saga. Усі результати походять із реально запущених Java/Python процесів; часові ряди синтетичні та прискорені.')
    r.p('Компроміси: один broker і одна cascade partition обмежують доступність/паралелізм; unbounded suppression допустимий лише для скінченного набору (20 вікон). Для production потрібен ліміт пам’яті/вікон і план масштабування. За значної out-of-order затримки grace слід обирати зі спостережень; поточний 1 хв - параметр моделі.')
    r.p('Конфігурації Kafka/Cassandra, схеми таблиць і команди запуску наведено в репозиторії. Для кожного повтору використовуються окремі topic та application.id. Dashboard використовує конфігурацію моніторингу з ЛР5.','small')
    r.sources([('Kafka Streams configuration','https://kafka.apache.org/37/streams/developer-guide/config-streams/'),('Kafka Streams upgrade and persistent state','https://kafka.apache.org/37/streams/upgrade-guide/')]);r.finish()

def lab5():
    folder=project(5);f=figdir(5);r=Report(5,'Моніторинг Kafka/Cassandra: Prometheus, Grafana та алерти',folder.name)
    raw=load(5,'prometheus-queries.json');queries=raw['queries'];live=load(5,'live-alert-states.json');rules=load(5,'promtool.json')
    def value(expr):return float(queries[expr]['data']['result'][0]['value'][1])
    r.page('1. Мета, стенд і метрики')
    r.p('Варіант 3Б: продуктивність турбін ГЕС, hill chart, ККД різних типів, кавітація та стан генераторів. Стенд: Prometheus 3.16.0, Grafana OSS 13.3.1, JMX exporter 1.7.0, Kafka 3.7.1 та Cassandra 4.0.21 (три JVM, RF=3).')
    r.flow([['Модель 6 турбін','Kafka → Cassandra'],['Python + Java JMX','Prometheus scrape'],['Grafana dashboards','PromQL + alert rules']])
    r.table(['Endpoint (localhost)','Роль'],[['18400','Python exporter + hill chart; тільки loopback'],['18080','Kafka broker JMX metrics'],['18081 / 18082 / 18083','Cassandra node1 / node2 / node3 JMX'],['19090','Prometheus API / rule engine'],['13000','Grafana OSS / provisioned dashboards']], [67,103])
    r.p('Prometheus scrape_interval=5 s. Вхідні дані - синтетична модель шести турбін, крок 1 с (прискорення ×10 проти варіанта з 10 с). Producer Counter збільшується лише після Kafka ack; sink Counter - після успішного Cassandra upsert. Consumer: manual commit, read_committed. Це живий вимір конвеєра, а не вручну записані «успішні» числа.')
    r.p('Counter: підтверджені події, помилки, broker requests; Gauge: потужність/рівень/ККД, lag, стан; Histogram: Kafka ack і Cassandra write latency. Ідентифікатор події не використано як label: кардинальність обмежена станціями, типами та вузлами.')
    r.page('2. Модель турбіни та hill chart')
    r.p('Модель ККД: η%=max(50,94-16[(H/H₀-1)²+(Q/Q₀-1)²]). Оптимуми: Kaplan (20 м,1200 м³/с), Francis (30 м,1000 м³/с), Pelton (80 м,350 м³/с). Ці значення задані для навчального порівняння типів; вони не є паспортними характеристиками станцій Дніпровського каскаду.')
    r.figure(f/'hill.png','Рис. 1. Hill chart η(H,Q) з того самого коду моделі, що експортує живі метрики. 41×41 точок на тип, CSV/JSON сітка доступна в evidence.',170)
    r.p('Потужність: P=ρgHQη/10⁶ MW, ρ=1000 кг/м³, g=9,81 м/с², η як частка. Показники напруги 220 кВ і частоти 50 Гц - модельні збурення. Cavitation/offdesign - керовані сценарні індикатори, а не оцінка за реальним NPSH/коефіцієнтом Тома.')
    r.p('Нормальний режим тримається поблизу H₀,Q₀. Під час fault injection ККД=86%, cavitation=1, offdesign=1, температура=90°C, напруга=208 кВ, частота=49,2 Гц. Сценарій навмисно впливає на всі шість модельних турбін; це перевірка спостережуваності та умов алертів.')
    r.page('3. Фактичний стан конвеєра та PromQL')
    r.table(['Показник з API','Значення'],[['Підтверджено Kafka sends',fmt(value('sum(energy_messages_sent_total)'),0)],['Збережено Cassandra upserts',fmt(value('sum(energy_messages_stored_total)'),0)],['Pipeline errors',fmt(value('energy_pipeline_errors_total'),0)],['Sink throughput за 1 хв, подій/с',fmt(value('sum(rate(energy_messages_stored_total[1m]))'))],['Producer ack p95, мс',fmt(value('histogram_quantile(0.95,sum by(le)(rate(energy_send_ack_seconds_bucket[1m])))')*1000)],['Prometheus targets UP',f"{len(queries['up']['data']['result'])} / {len(queries['up']['data']['result'])}"]],[112,58])
    r.p('Джерело: prometheus-queries.json, з реальними epoch timestamps. Лічильники зняті близько, але запити API послідовні: різниця sends/stored може включати події in-flight; для backlog використовується endOffset−consumer.position, окрема метрика. Усі значення up=1 та pipeline_errors=0 перевірено програмно.')
    r.table(['PromQL','Що вимірює'],[['sum(rate(energy_messages_stored_total[1m]))','Середня швидкість успішних upsert; rate враховує counter resets'],['avg by(turbine_type)(hydro_efficiency_percent)','Порівняння модельного ККД типів турбін'],['histogram_quantile(0.95, sum by(le)(rate(energy_send_ack_seconds_bucket[1m])))','p95 гістограми ack; це оцінка за bucket, не точний percentile сирих спостережень'],['sum(hydro_power_mw * (94 / hydro_efficiency_percent - 1))','Модельний дефіцит потужності проти 94% за фіксованих H,Q'],['cassandra_client_latency_microseconds','Read/Write p95/p99 JMX; мікросекунди'],['energy_consumer_lag','Поточний lag партиції Kafka']], [103,67])
    r.p('JMX збирає реальні Count, Storage.Load та heap трьох Cassandra JVM; broker MessagesIn і under-replicated partitions. Python exporter не підміняє JMX синтетичними значеннями. Всі raw /metrics збережені разом із targets API.','small')
    r.page('4. Реальний Grafana dashboard')
    shot=folder/'evidence/screenshots/hydro-top.jpg'
    if shot.exists():r.figure(shot,'Рис. 2. Живий Grafana OSS: КПД, потужність, кавітація, throughput, напруга та частота. UTC; модельні дані.',170)
    r.p('Dashboard provisioned з JSON: ефективність, потужність, cavitation, throughput, напруга, частота, температура, generator state, consumer lag, Cassandra latency/heap, hill chart і ALERTS. Hill chart - згенерована моделлю характеристика, вбудована у text panel; решта панелей виконують живі PromQL. Другий dashboard показує результати Streams/Saga і роботу конвеєра.')
    r.p('Grafana отримує дані з Prometheus через джерело hydro-prom. Dashboard і правило Grafana-managed alert налаштовано файлами provisioning.')
    r.page('5. Hill chart та фактичні метрики вузлів')
    shot=folder/'evidence/screenshots/hydro-lower.jpg'
    if shot.exists():r.figure(shot,'Рис. 3. Повна модельна характеристика трьох типів турбін у реально відкритій панелі Grafana.',170)
    heap={x['metric']['instance']:float(x['value'][1]) for x in queries['cassandra_heap_used_bytes']['data']['result']}
    disk={x['metric']['instance']:float(x['value'][1]) for x in queries['cassandra_storage_bytes']['data']['result']}
    requests={x['metric']['instance']:float(x['value'][1]) for x in queries['cassandra_client_requests_total']['data']['result'] if x['metric']['operation']=='Write'}
    r.table(['Cassandra exporter','Heap MiB','Storage.Load MiB','Write requests total'],[[node,fmt(heap[node]/1048576),fmt(disk[node]/1048576),fmt(requests[node],0)] for node in sorted(heap)],[48,35,43,44])
    r.p('Це три окремі JVM та фактичні JMX counters/gauges на момент зняття API. Write requests total накопичує операції з моменту запуску вузла, включно з попередніми лабораторними; це не швидкість поточної телеметрії. Heap є поточним used, не Xmx і не RAM усього комп’ютера. Storage.Load - обсяг Cassandra, не вільний простір диска.')
    r.p('Hill chart показує поверхню заданої функції η(H,Q), а не виміряну паспортну криву. Живі часові ряди ефективності обчислюються тим самим eta() та експортуються окремо; червона точка позначає модельний design point.')
    r.page('6. Алерти: production та перевірка тривалості')
    r.table(['Умова','for','Профіль'],[['η&lt;88%','2h','production'],['cavitation=1','10m','production'],['offdesign=1 / T&gt;85°C','5m / 5m','production'],['|U−220|&gt;10 кВ','2m','production'],['|f−50|&gt;0,5 Гц','1m','production'],['producer_alive=0 або серія відсутня','1m','production'],['η&lt;88%','15s','accelerated-e2e, окремий alert']], [99,25,46])
    r.p('Promtool виконує справжній PromQL/rule engine над синтетичними вхідними рядами з віртуальним часом. HydroEfficiencyLow: не firing на 119m45s, firing на 120m, resolved на 121m. HydroCavitation: не firing на 9m45s, firing на 10m, resolved на 11m. Перевірено також producer absent: 45s не firing, 1m firing. Це підтверджує конфігурацію тривалості, але не є двогодинним живим спостереженням.')
    r.table(['Команда promtool','Exit code'],[[' '.join(x['command'][:2]),x['exit_code']] for x in rules],[140,30])
    r.p('Живий тест окремого правила: увімкнення контрольованого fault, захоплення pending і firing через API, вимкнення fault, автоматичне resolved. Production-правило в той самий час може бути pending; його for=2h не скорочувалось. Prometheus evaluated ALERTS та Grafana-managed alert зберігаються окремо.')
    r.p('Grafana-managed правило Hydro efficiency: accelerated 15s lab test створене file provisioning, запит A до Prometheus і threshold B&lt;88. Зовнішні email/Slack contact points не налаштовано; лабораторний тест не розсилає повідомлень. HTTP API містить фактичну історію станів.','small')
    r.page('7. Стан алертів та аналітичні висновки')
    rows=[];fault_epoch=live['samples'][0]['epoch']-live['samples'][0]['elapsed_seconds']
    for item in live['samples']:
        alerts=item['prometheus']['data']['alerts'];states=sorted({a['state'] for a in alerts if a['labels']['alertname']=='HydroEfficiencyLiveTest'})
        phase=item.get('phase','fault');state=', '.join(states) or ('resolved' if phase=='recovery' else 'inactive')
        if not rows or rows[-1][1]!=state:rows.append([phase,state,fmt(item.get('elapsed_seconds',item['epoch']-fault_epoch))])
    r.table(['Фаза','Live alert','Секунд від fault'],rows,[55,60,55])
    history=load(5,'efficiency-history.json');fig,ax=plt.subplots(figsize=(9,3.3))
    for series in history['data']['result']:
        points=series['values'];origin=points[0][0];ax.plot([(t-origin)/60 for t,v in points],[float(v) for t,v in points],label=series['metric'].get('turbine_type','type'))
    ax.axhline(88,color='#c43',ls='--',label='threshold');ax.set(xlabel='Minutes from first captured sample',ylabel='Efficiency %');ax.legend();ax.grid(alpha=.2);fig.tight_layout();fig.savefig(f/'efficiency-history.png',dpi=160);plt.close(fig);r.figure(f/'efficiency-history.png','Рис. 4. Фактична Prometheus-історія: fault injection і повернення до нормального модельного ККД.')
    r.p('За заданою характеристикою оптимальний режим поблизу H₀,Q₀; перерозподіл слід обмежувати паспортними діапазонами кожного типу. Порівняння різних турбін без нормалізації напору й витрати не доводить перевагу одного типу. Потужність, втрачена проти η=94%, є модельною оцінкою; короткий сценарний cavitation не дозволяє оцінити довгострокову деградацію або реальні втрати від кавітації.')
    r.p('Рекомендації: контролювати відхилення від design point, окремо підтверджувати фізичні причини низького ККД; JMX GC/heap/compaction поєднувати з pipeline latency і lag. Стенд на одному ПК не оцінює мережеві розділення, незалежну доступність чи production SLO.')
    r.page('8. Контрольні питання та відтворення')
    answers=[('Prometheus','TSDB і PromQL; періодично опитує HTTP /metrics, записує часові ряди з labels.'),('Counter і Gauge','Counter накопичує події та може reset при restart; Gauge вільно зростає/спадає. Для швидкості Counter застосовують rate.'),('PromQL','Мова вибірки й агрегації часових рядів, наприклад avg by(turbine_type)(hydro_efficiency_percent).'),('rate()','Оцінює середній приріст counter за секунду у range vector і враховує resets.'),('for: 2m','Умова має безперервно лишатися істинною на послідовних evaluations протягом двох хвилин.'),('Pending і Firing','Pending - умова вже істинна, але for ще не минув; firing - витримано for. За зникнення умови стан повертається inactive.'),('Критичні енергетичні метрики','Частота, напруга, температура, потужність і стан; для ГЕС також напір/витрата/ККД/кавітація; для конвеєра lag, errors і latency.'),('220±10 кВ','Це умовний допустимий діапазон саме навчальної моделі; реальні границі залежать від класу обладнання й застосовних норм.'),('48 Гц','Значне відхилення від номінальних 50 Гц; у моделі алерт FrequencyDeviation. Реальна дія захисту залежить від установлених налаштувань, а не цього Python-коду.'),('Grafana і Prometheus','Grafana виконує PromQL через HTTP datasource, перетворює отримані ряди на панелі; Prometheus зберігає метрики й сам оцінює власні alert rules.')]
    for title,text in answers:r.p('<b>'+title+'.</b> '+text)
    r.p('Конфігурації Prometheus, Grafana та alert rules, код exporter і порядок запуску наведено в репозиторії. Dashboard стенда доступний за адресою localhost:13000/d/hydro-lysenko після запуску сервісів.','small')
    r.sources([('Prometheus rule unit tests','https://prometheus.io/docs/prometheus/latest/configuration/unit_testing_rules/'),('Prometheus alerting rules','https://prometheus.io/docs/prometheus/latest/configuration/alerting_rules/'),('Grafana file provisioning','https://grafana.com/docs/grafana/latest/alerting/set-up/provision-alerting-resources/file-provisioning/')]);r.finish()

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('lab',type=int,choices=[3,4,5]);a=p.parse_args();{3:lab3,4:lab4,5:lab5}[a.lab]()
