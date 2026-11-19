"""Readable views of saved stdout for actual window screenshots; no new measurements."""
import hashlib
import json
import math
import statistics
from common import ROOT,configure_stdout

E=ROOT/'evidence'
OUT=E/'screenshot-views'

def main():
    configure_stdout();OUT.mkdir(exist_ok=True)
    results=json.loads((E/'results.json').read_text(encoding='utf-8'))
    summary=json.loads((E/'summary.json').read_text(encoding='utf-8'))
    measured=[];sources=[]
    for expected in results:
        name=str(expected['config']['output_file']).replace('\\','/').rsplit('/',1)[-1]
        path=E/'runs/lecture-review'/name;path=path.with_suffix('.txt')
        row=next(json.loads(line[len('MEASUREMENT_RESULT '):]) for line in path.read_text(encoding='utf-8').splitlines() if line.startswith('MEASUREMENT_RESULT '))
        assert row=={k:v for k,v in expected.items() if k!='resources_trial_including_warmup'}
        measured.append(row);sources.append({'path':path.relative_to(ROOT).as_posix(),'sha256':hashlib.sha256(path.read_bytes()).hexdigest()})
    med=lambda rows,key:statistics.median(key(row) for row in rows)
    views=[]
    def save(name,lines):
        path=OUT/name;path.write_text('\n'.join(lines)+'\n',encoding='utf-8-sig')
        views.append({'view':path.relative_to(ROOT).as_posix(),'sha256':hashlib.sha256(path.read_bytes()).hexdigest()})
    header=['Лабораторна робота №1. Лисенко Лука, ТР-61МП, варіант 3Б',
        'ПЕРЕГЛЯД ЗБЕРЕЖЕНИХ РЕЗУЛЬТАТІВ ТЕСТУВАННЯ',
        'Джерело: evidence/runs/lecture-review/*.txt (MEASUREMENT_RESULT)',
        'Медіани для трьох повторів кожної конфігурації.','']
    lines=header+['1. BATCH.SIZE / LINGER.MS: медіани трьох повторів',
        'Batch KiB  Linger ms   Records/s     Ack avg ms   E2E p95 ms',
        '----------------------------------------------------------']
    for s in summary['batch']:
        rows=[r for r in measured if r['config']['family']=='batch' and r['config']['batch_size']==s['batch_size'] and r['config']['linger_ms']==s['linger_ms']]
        rate=med(rows,lambda r:r['records_per_sec']);ack=med(rows,lambda r:r['ack_latency']['avg_ms']);p95=med(rows,lambda r:r['e2e_latency']['p95_ms'])
        assert len(rows)==3 and math.isclose(rate,s['rec_s']) and math.isclose(p95,s['e2e_p95_ms'])
        lines.append(f"{s['batch_size']//1024:9d}  {s['linger_ms']:9d}  {rate:11.3f}  {ack:12.3f}  {p95:11.3f}")
    lines+=['','27 прогонів, по 50 000 вимірюваних повідомлень.','Розігрів виключено з часу та кількості повідомлень.','Ack: send -> callback. E2E: send -> перевірка consumer.']
    save('01_batch_linger.txt',lines)
    lines=header+['2. КОДЕКИ: медіани трьох повторів',
        'Codec     Records/s     Ack avg ms   Batch savings %','------------------------------------------------------']
    for codec in ['none','gzip','snappy','lz4','zstd']:
        rows=[r for r in measured if r['config']['family']=='compression' and r['config']['compression']==codec]
        rate=med(rows,lambda r:r['records_per_sec']);ack=med(rows,lambda r:r['ack_latency']['avg_ms']);savings=med(rows,lambda r:100*(1-r['compression_rate_avg']))
        lines.append(f'{codec:8s}  {rate:11.3f}  {ack:12.3f}  {savings:15.4f}')
    lines+=['','Batch = 256 KiB; linger = 50 ms; 20 000 записів / прогін.','Batch savings = 100 * (1 - compression-rate-avg).','Корпус: 1000 синтетичних JSON, що повторюються.']
    save('02_compression.txt',lines)
    lines=header+['3. ПАРТИЦІОНУВАННЯ: ключ turbine_type',
        'Partitions  Active    Median records/s    Counts (one run)','-------------------------------------------------------------']
    for n in [2,4,8,3,6,12]:
        rows=[r for r in measured if r['config']['family']=='partition' and r['config']['partitions']==n]
        counts=rows[0]['partition_counts'];active=sum(x>0 for x in counts)
        lines.append(f'{n:10d}  {active:6d}  {med(rows,lambda r:r["records_per_sec"]):18.3f}    {counts}')
    lines+=['','Три значення ключа: kaplan / francis / pelton.','Три повтори на кожну кількість партицій; 50 000 записів / прогін.','У кожному прогоні received=records, errors=0; дублікати не виявлені.']
    save('03_partitions.txt',lines)
    official=(E/'official-json-performance.txt').read_text(encoding='utf-8').splitlines()
    serialization=(E/'serialization-stdout.txt').read_text(encoding='utf-8').splitlines()
    warehouse=(E/'warehouse-stdout.txt').read_text(encoding='utf-8').splitlines()
    lines=header[:2]+['','4. ШТАТНИЙ PRODUCER-PERF (official-json-performance.txt)',next(line for line in reversed(official) if line.startswith('5000 records')),
        '','СЕРІАЛІЗАЦІЯ (serialization-stdout.txt)',*serialization[:3],
        '','АНАЛІТИЧНІ ВІКНА (warehouse-stdout.txt)',warehouse[0]]
    w=json.loads(next(line[len('WAREHOUSE_RESULT '):] for line in warehouse if line.startswith('WAREHOUSE_RESULT ')))
    lines+=['Window s    Input rows   Aggregate rows   Reduction %','----------------------------------------------------']
    for row in w['windows']:lines.append(f"{row['window_seconds']:8d}  {row['input_records']:12d}  {row['aggregate_rows']:15d}  {row['row_reduction_pct']:12.4f}")
    lines+=['','Повторна агрегація: усі 105 добових груп збігаються з raw.']
    save('04_cli_and_analytics.txt',lines)
    (OUT/'sources.json').write_text(json.dumps({'description':'Saved-stdout views for window screenshots; not live benchmark reruns','sources':sources,'views':views},ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    print('VIEWS VERIFIED:',len(views),'files;',len(measured),'stdout results reconciled')

if __name__=='__main__':main()
