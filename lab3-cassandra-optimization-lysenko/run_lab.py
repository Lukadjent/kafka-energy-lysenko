"""Load 30 synthetic days into three real RF=3 Cassandra schemas and benchmark."""
import argparse
from collections import defaultdict,deque
from concurrent.futures import ThreadPoolExecutor
import csv
from datetime import date,datetime,timedelta,timezone
import hashlib
import json
import math
from pathlib import Path
import random
import statistics
import sys
import time

ROOT=Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT.parent/'lab-support'))
from common import *
from hydro_model import *
from services import nodetool
from cassandra import ConsistencyLevel,InvalidRequest
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement,BatchType

E=ROOT/'evidence'; E.mkdir(exist_ok=True)
DATA=ROOT/'data'; DATA.mkdir(exist_ok=True)
KS='energy_l3_lysenko'
REPEATS=60
READ_FIELDS='device_id,event_time,water_flow,water_level,power_output,efficiency'

def stats(values):
    ordered=sorted(values)
    return {'n':len(values),'avg_ms':statistics.mean(values),'p50_ms':statistics.median(values),
        'p95_ms':ordered[math.ceil(len(values)*.95)-1],
        'p99_ms':ordered[math.ceil(len(values)*.99)-1], 'min_ms':ordered[0],'max_ms':ordered[-1]}

def statement(session,text):
    prepared=session.prepare(text)
    prepared.consistency_level=ConsistencyLevel.LOCAL_QUORUM
    return prepared

def execute_script(session,path):
    text='\n'.join(line for line in path.read_text(encoding='utf-8').splitlines() if not line.strip().startswith('--'))
    for command in text.split(';'):
        if command.strip(): session.execute(command.strip(),timeout=300)

def generate():
    target=DATA/'hydro-30d.csv'
    if target.exists():
        return
    started=time.perf_counter()
    with target.open('w',encoding='utf-8',newline='') as output:
        writer=csv.writer(output); writer.writerow(FIELDS)
        for device in range(DEVICES):
            writer.writerows(record(device,step) for step in range(STEPS))
    meta={'records':DEVICES*STEPS,'devices':DEVICES,'days':DAYS,'interval_seconds':30,
        'start_utc':START.isoformat(),'end_exclusive_utc':(START+timedelta(days=DAYS)).isoformat(),
        'generation_seconds':time.perf_counter()-started,'csv_bytes':target.stat().st_size,
        'sha256':hashlib.sha256(target.read_bytes()).hexdigest(),'synthetic':True,
        'level_bands':{'high':'>=18.3 m','low':'<15.5 m','normal':'otherwise'}}
    write_json(E/'dataset.json',meta)
    write_json(E/'sample-records.json',[public_record(i,0) for i in range(3)])
    print('DATASET_RESULT '+json.dumps(meta),flush=True)

def rows():
    with (DATA/'hydro-30d.csv').open(encoding='utf-8',newline='') as stream:
        reader=csv.reader(stream);next(reader)
        for row in reader:
            yield (row[0],int(row[1]),date.fromisoformat(row[2]),int(row[3]),date.fromisoformat(row[4]),
                row[5],row[6],*[float(x) for x in row[7:11]],row[11],row[12])

def load(session,table,limit=None):
    # Each successful trial starts empty; failed exploratory attempts remain in console logs.
    session.execute(f'TRUNCATE {KS}.{table}',timeout=300)
    for node in range(3): nodetool(node,'flush',KS)
    time.sleep(3)
    insert=statement(session,f'INSERT INTO {KS}.{table} ({",".join(FIELDS)}) VALUES ({",".join("?" for _ in FIELDS)})')
    pending=deque();latencies=[];count=0;requests=0
    def send(batch):
        nonlocal requests
        start=time.perf_counter()
        future=session.execute_async(batch)
        future.add_callback(lambda result:latencies.append((time.perf_counter()-start)*1000))
        pending.append(future);requests+=1
        if len(pending)>=8: pending.popleft().result()
    began=time.perf_counter();batch=None;key=None
    for row in rows():
        current=(row[0],row[3] if table=='hourly' else row[2] if table=='daily' else None)
        if batch is None or current!=key or len(batch)==60:
            if batch is not None: send(batch)
            batch=BatchStatement(batch_type=BatchType.UNLOGGED,consistency_level=ConsistencyLevel.LOCAL_QUORUM)
            key=current
        batch.add(insert,row);count+=1
        if count%100000==0: print(f'LOAD {table}: {count:,}',flush=True)
        if limit and count>=limit:break
    if batch is not None:send(batch)
    while pending: pending.popleft().result()
    elapsed=time.perf_counter()-began
    assert len(latencies)==requests
    with (E/f'write-samples-{table}.csv').open('w',encoding='utf-8',newline='') as output:
        writer=csv.writer(output);writer.writerow(['ack_order','batch_ack_ms'])
        writer.writerows(enumerate(latencies,1))
    result={'table':table,'rows':count,'requests':requests,'max_batch_rows':60,'max_inflight':8,
        'load_seconds':elapsed,'rows_per_second':count/elapsed,'batch_ack':stats(latencies),
        'consistency':'LOCAL_QUORUM','scope':'CSV decoding, batch formation, async submission and acknowledgements; data generation excluded'}
    write_json(E/f'write-{table}.json',result)
    print('WRITE_RESULT '+json.dumps(result),flush=True)
    return result

def fingerprint(result):
    normalized=sorted((row.device_id,row.event_time.replace(tzinfo=timezone.utc).isoformat(),
        round(row.water_flow,7),round(row.water_level,7),round(row.power_output,7),round(row.efficiency,7)) for row in result)
    return len(normalized),hashlib.sha256(json.dumps(normalized).encode()).hexdigest()

def plans(session):
    day=DATES[-1];end=START_MS+STEPS*30000;device='HYDRO_DN_015'
    queries={}
    for table in ('simple','hourly','daily'):
        extra=' AND hour=?' if table=='hourly' else ' AND day=?' if table=='daily' else ''
        prefix=f'SELECT {READ_FIELDS} FROM {KS}.{table} WHERE device_id=?'+extra
        for kind,begin in [('latest',end-3600000),('six_hours',end-6*3600000),('daily',end-86400000),('filtered',end-86400000)]:
            suffix=' AND event_time>=? AND event_time<?'
            if kind=='latest':suffix+=' LIMIT 100'
            if kind=='filtered':suffix+=" AND level_band='high' ALLOW FILTERING"
            prepared=statement(session,prefix+suffix)
            args=[]
            if table=='hourly':
                for hour in range(begin//3600000,end//3600000):
                    lo=max(begin,hour*3600000);hi=min(end,(hour+1)*3600000)
                    args.append((device,hour*3600000,lo,hi))
            elif table=='daily':args=[(device,day,begin,end)]
            else:args=[(device,begin,end)]
            queries[(table,kind)]=(prepared,args)
    return queries

def run_query(session,plan,parallel=False):
    prepared,args=plan
    if parallel:
        futures=[session.execute_async(prepared,value) for value in args]
        return [row for future in futures for row in future.result()]
    return [row for value in args for row in session.execute(prepared,value)]

def benchmark(session):
    queries=plans(session)
    expected={kind:fingerprint(run_query(session,queries['daily',kind])) for kind in ('latest','six_hours','daily','filtered')}
    assert expected['latest'][0]==100 and expected['six_hours'][0]==720 and expected['daily'][0]==2880
    assert expected['filtered'][0]>0
    samples=defaultdict(list);raw=[]
    schedule=list(queries)*REPEATS;random.Random(13).shuffle(schedule)
    for key in queries:
        for _ in range(5):run_query(session,queries[key])
    for table,kind in schedule:
        start=time.perf_counter();result=run_query(session,queries[table,kind]);elapsed=(time.perf_counter()-start)*1000
        assert fingerprint(result)==expected[kind],(table,kind)
        samples[table,kind].append(elapsed)
        raw.append({'table':table,'query':kind,'ms':elapsed,'rows':len(result),'requests':len(queries[table,kind][1])})
    summary=[{'table':table,'query':kind,'requests':len(queries[table,kind][1]),'rows':expected[kind][0],**stats(values)} for (table,kind),values in sorted(samples.items())]
    with (E/'read-samples.csv').open('w',encoding='utf-8',newline='') as stream:
        writer=csv.DictWriter(stream,fieldnames=list(raw[0]));writer.writeheader();writer.writerows(raw)
    write_json(E/'read-benchmark.json',summary)
    print('READ_BENCHMARK_RESULT '+json.dumps(summary),flush=True)
    # B: compare sequential and concurrent reads of exactly the same six buckets.
    plan=queries['hourly','six_hours'];multi={}
    for parallel in (False,True):
        values=[]
        for _ in range(REPEATS):
            start=time.perf_counter();result=run_query(session,plan,parallel);values.append((time.perf_counter()-start)*1000)
            assert fingerprint(result)==expected['six_hours']
        multi['parallel' if parallel else 'sequential']={'samples_ms':values,**stats(values)}
    write_json(E/'multi-partition.json',multi)
    print('MULTI_PARTITION_RESULT '+json.dumps(multi),flush=True)
    return summary

def optimize(session):
    day=DATES[-1];device='HYDRO_DN_015';end=START_MS+STEPS*30000
    select=f'SELECT {READ_FIELDS} FROM {KS}.daily WHERE device_id=? AND day=? AND level_band=?'
    invalid_log=[]
    try:session.execute(statement(session,select),(device,day,'high'))
    except InvalidRequest as error:invalid_log.append('Before index: '+str(error))
    bad_mv=f'CREATE MATERIALIZED VIEW {KS}.invalid_range_mv AS SELECT * FROM {KS}.daily WHERE device_id IS NOT NULL AND day IS NOT NULL AND event_time IS NOT NULL AND water_level > 18.3 PRIMARY KEY ((day),device_id,event_time)'
    try:
        session.execute(bad_mv)
        raise AssertionError('Unexpected range-predicate MV support')
    except InvalidRequest as error:invalid_log.append('Rejected range MV: '+str(error))
    (E/'cql-restrictions.txt').write_text('\n'.join(invalid_log),encoding='utf-8')
    results={}
    baseline=statement(session,select+' ALLOW FILTERING')
    oracle=fingerprint(list(session.execute(baseline,(device,day,'high'))))
    def measured(name,prepared,args,expected=oracle):
        values=[]
        for index in range(REPEATS+5):
            start=time.perf_counter();output=list(session.execute(prepared,args));elapsed=(time.perf_counter()-start)*1000
            assert fingerprint(output)==expected,name
            if index>=5:values.append(elapsed)
        results[name]={'rows':expected[0],'samples_ms':values,**stats(values)}
    measured('allow_filtering',baseline,(device,day,'high'))
    session.execute(f'CREATE INDEX IF NOT EXISTS daily_level_idx ON {KS}.daily (level_band)',timeout=300)
    indexed=statement(session,select)
    # Index construction is asynchronous; require the indexed result to reconcile.
    deadline=time.monotonic()+600
    while True:
        try:
            if fingerprint(list(session.execute(indexed,(device,day,'high'))))==oracle:break
        except Exception:
            if time.monotonic()>deadline:raise
        if time.monotonic()>deadline:raise TimeoutError('Secondary index did not converge')
        time.sleep(2)
    measured('secondary_index',indexed,(device,day,'high'))
    started=time.perf_counter();execute_script(session,ROOT/'optimize.cql')
    deadline=time.monotonic()+1800
    while True:
        states={name:list(session.execute('SELECT status FROM system_distributed.view_build_status WHERE keyspace_name=%s AND view_name=%s',(KS,name))) for name in ('by_level_band','by_type_hour')}
        if all(len(values)==3 and all(row.status=='SUCCESS' for row in values) for values in states.values()):break
        if time.monotonic()>deadline:raise TimeoutError(str(states))
        print('MV BUILD '+str({k:[v.status for v in values] for k,values in states.items()}),flush=True);time.sleep(5)
    build_seconds=time.perf_counter()-started
    view=statement(session,f'SELECT {READ_FIELDS} FROM {KS}.by_level_band WHERE level_band=? AND day=? AND device_id=?')
    measured('materialized_level_band',view,('high',day,device))
    # Second real MV groups one turbine type across stations for one UTC hour.
    hour=end-3600000;kind='kaplan';devices=[f'HYDRO_DN_{i+1:03d}' for i in range(DEVICES) if TYPES[i%3]==kind]
    base=statement(session,f'SELECT {READ_FIELDS} FROM {KS}.daily WHERE device_id=? AND day=? AND event_time>=? AND event_time<?')
    base_plan=(base,[(d,day,hour,end) for d in devices]);type_oracle=fingerprint(run_query(session,base_plan,True))
    type_view=statement(session,f'SELECT {READ_FIELDS} FROM {KS}.by_type_hour WHERE type_hour=? AND day=?')
    measured('materialized_type_hour',type_view,(kind+':'+str(hour),day),type_oracle)
    values=[]
    for _ in range(REPEATS):
        start=time.perf_counter();output=run_query(session,base_plan,True);values.append((time.perf_counter()-start)*1000)
        assert fingerprint(output)==type_oracle
    results['coordination_five_partition_reads']={'rows':type_oracle[0],'requests':5,'samples_ms':values,**stats(values)}
    results['view_build_seconds']=build_seconds
    write_json(E/'index-mv.json',results);print('INDEX_MV_RESULT '+json.dumps(results),flush=True)
    return results

def weekly(session):
    # B: a multi-device, seven-day analytical query; persist mergeable statistics.
    grouped=defaultdict(lambda:{'cnt':0,'sum_flow':0.,'min_level':float('inf'),'max_level':-float('inf'),'energy_mwh':0.})
    for row in rows():
        target=grouped[row[5],row[4]];target['cnt']+=1;target['sum_flow']+=row[7]
        target['min_level']=min(target['min_level'],row[8]);target['max_level']=max(target['max_level'],row[8]);target['energy_mwh']+=row[9]/120
    insert=statement(session,f'INSERT INTO {KS}.weekly_rollup (station_id,week,cnt,sum_flow,min_level,max_level,energy_mwh) VALUES (?,?,?,?,?,?,?)')
    for (station,week),value in grouped.items():session.execute(insert,(station,week,*value.values()))
    week=date(2026,9,21);days=[week+timedelta(days=i) for i in range(7)];devices=[f'HYDRO_DN_{i+1:03d}' for i in range(5)]
    prepared=statement(session,f'SELECT {READ_FIELDS} FROM {KS}.daily WHERE device_id=? AND day=?')
    aggregate=statement(session,f'SELECT cnt,sum_flow,min_level,max_level,energy_mwh FROM {KS}.weekly_rollup WHERE station_id=? AND week=?')
    latencies={'raw_35_partitions':[],'weekly_rollup_one_partition':[]}
    for _ in range(10):
        start=time.perf_counter();raw=run_query(session,(prepared,[(d,day) for d in devices for day in days]),True);latencies['raw_35_partitions'].append((time.perf_counter()-start)*1000)
        start=time.perf_counter();stored=session.execute(aggregate,('KYIV',week)).one();latencies['weekly_rollup_one_partition'].append((time.perf_counter()-start)*1000)
        assert len(raw)==stored.cnt==100800
        assert math.isclose(math.fsum(r.water_flow for r in raw),stored.sum_flow,rel_tol=1e-10)
        assert math.isclose(math.fsum(r.power_output for r in raw)/120,stored.energy_mwh,rel_tol=1e-10)
        assert min(r.water_level for r in raw)==stored.min_level and max(r.water_level for r in raw)==stored.max_level
    result={'groups':len(grouped),'input_rows':sum(v['cnt'] for v in grouped.values()),'validated_raw_rows':100800,
        'results':{k:{'samples_ms':v,**stats(v)} for k,v in latencies.items()},'sample':stored._asdict()}
    write_json(E/'weekly-analysis.json',result);print('WEEKLY_RESULT '+json.dumps(result),flush=True)
    return result

def daily_aggregation(session):
    """Actual server-side CQL aggregates, with mergeable hourly results and CSV oracle."""
    day=DATES[-1];end=START_MS+STEPS*30000;begin=end-86400000;device='HYDRO_DN_015'
    source=[row for row in rows() if row[0]==device and row[2]==day]
    oracle={'n':len(source),'sum_flow':math.fsum(x[7] for x in source),'sum_power':math.fsum(x[9] for x in source),
        'min_level':min(x[8] for x in source),'max_level':max(x[8] for x in source)}
    results=[]
    fields='COUNT(*) AS n,SUM(water_flow) AS sum_flow,SUM(power_output) AS sum_power,MIN(water_level) AS min_level,MAX(water_level) AS max_level'
    for table in ('simple','hourly','daily'):
        where='device_id=?'
        if table=='hourly':where+=' AND hour=?'
        if table=='daily':where+=' AND day=?'
        where+=' AND event_time>=? AND event_time<?'
        prepared=statement(session,f'SELECT {fields} FROM {KS}.{table} WHERE {where}')
        args=[(device,h,h,h+3600000) for h in range(begin,end,3600000)] if table=='hourly' else [(device,day,begin,end)] if table=='daily' else [(device,begin,end)]
        samples=[]
        for rep in range(REPEATS+5):
            start=time.perf_counter();parts=[session.execute(prepared,value).one() for value in args]
            result={'n':sum(x.n for x in parts),'sum_flow':math.fsum(x.sum_flow for x in parts),'sum_power':math.fsum(x.sum_power for x in parts),
                'min_level':min(x.min_level for x in parts),'max_level':max(x.max_level for x in parts)}
            elapsed=(time.perf_counter()-start)*1000
            assert all(math.isclose(result[k],oracle[k],rel_tol=1e-10,abs_tol=1e-7) for k in oracle),(table,result,oracle)
            if rep>=5:samples.append(elapsed)
        results.append({'table':table,'requests':len(args),'samples_ms':samples,**stats(samples),'aggregates':result,
            'avg_flow':result['sum_flow']/result['n'],'energy_mwh':result['sum_power']/120})
    output={'csv_oracle':oracle,'results':results,'scope':'Server-side CQL aggregates + merging 24 hourly partials; 5 warm-ups + 60 measured repetitions; validation outside timer',
        'measured_after_mv_creation':bool(session.cluster.metadata.keyspaces[KS].views)}
    write_json(E/'daily-aggregation.json',output);print('DAILY_AGGREGATE_RESULT '+json.dumps(output),flush=True)
    return output

def maintenance(session):
    session.execute(f"ALTER TABLE {KS}.daily WITH default_time_to_live=2592000 AND compaction={{'class':'TimeWindowCompactionStrategy','compaction_window_unit':'DAYS','compaction_window_size':'1'}}")
    session.execute(f'CREATE TABLE IF NOT EXISTS {KS}.ttl_probe (id text PRIMARY KEY, value int)')
    session.execute(f"INSERT INTO {KS}.ttl_probe (id,value) VALUES ('expiry',13) USING TTL 3")
    before=session.execute(f"SELECT value,TTL(value) AS remaining FROM {KS}.ttl_probe WHERE id='expiry'").one()
    time.sleep(4)
    assert session.execute(f"SELECT value FROM {KS}.ttl_probe WHERE id='expiry'").one() is None
    snapshots=[]
    for i in range(3):
        (E/f'nodetool-flush-node{i+1}.txt').write_text(nodetool(i,'flush',KS),encoding='utf-8')
        (E/f'nodetool-tablestats-node{i+1}.json').write_text(nodetool(i,'tablestats','-F','json',KS),encoding='utf-8')
        (E/f'nodetool-compactionstats-node{i+1}.txt').write_text(nodetool(i,'compactionstats'),encoding='utf-8')
        (E/f'nodetool-snapshot-node{i+1}.txt').write_text(nodetool(i,'snapshot','-t','lab3-3b',KS),encoding='utf-8')
        files=list((SUPPORT/'data'/f'node{i+1}'/'sstables'/KS).glob('*/snapshots/lab3-3b/*'))
        snapshots.append({'node':i+1,'files':len(files),'bytes':sum(p.stat().st_size for p in files if p.is_file())})
    for table in ('simple','hourly','daily'):
        (E/f'nodetool-histogram-{table}.txt').write_text(nodetool(0,'tablehistograms',KS,table),encoding='utf-8')
    assert all(item['files']>0 for item in snapshots)
    result={'ttl_seconds':3,'initial_remaining':before.remaining,'expired_verified':True,'snapshots':snapshots,
        'policy':'daily new writes: TTL 30 days, TWCS 1 day; existing rows are not retroactively expired'}
    write_json(E/'maintenance.json',result);print('MAINTENANCE_RESULT '+json.dumps(result),flush=True)
    (E/'nodetool-status.txt').write_text(nodetool(0,'status',KS),encoding='utf-8')
    return result

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--phase',choices=['all','generate','load','benchmark','daily-aggregation','optimize','weekly','maintenance'],default='all')
    parser.add_argument('--reset',action='store_true',help='Drop ONLY the owned energy_l3_lysenko keyspace before a clean all run')
    options=parser.parse_args()
    generate()
    if options.phase=='generate':return
    with Cluster(CONTACT_POINTS,protocol_version=4,connect_timeout=30) as cluster:
        session=cluster.connect();session.default_timeout=300;session.default_fetch_size=5000
        if options.reset:
            assert options.phase=='all','--reset is allowed only with --phase all'
            session.execute(f'DROP KEYSPACE IF EXISTS {KS}',timeout=300)
        elif options.phase=='all' and KS in cluster.metadata.keyspaces and cluster.metadata.keyspaces[KS].views:
            raise RuntimeError('Optimized schema already exists. Use --reset for a fresh baseline; this drops only energy_l3_lysenko.')
        execute_script(session,ROOT/'schema.cql')
        assert len(cluster.metadata.all_hosts())==3
        write_json(E/'environment.json',{'nodes':[str(h.address) for h in cluster.metadata.all_hosts()],
            'version':session.execute('SELECT release_version FROM system.local').one().release_version,
            'replication':cluster.metadata.keyspaces[KS].replication_strategy.export_for_schema(),
            'read_write_consistency':'LOCAL_QUORUM','shared_physical_host':True,'finished_utc':datetime.now(timezone.utc).isoformat()})
        if options.phase in ('all','load'):
            for node in range(3):nodetool(node,'settimeout','write','20000')
            write_json(E/'load-settings.json',{'server_write_timeout_ms':20000,'max_inflight':8,'batch_rows':60,'note':'Shared-host exploratory run hit default 2s timeout; successful rerun starts each table empty; no client retry.'})
            written=[load(session,t) for t in ('simple','hourly','daily')];write_json(E/'write-benchmark.json',written)
            for table in ('simple','hourly','daily'):
                count=session.execute(f'SELECT COUNT(*) FROM {KS}.{table}',timeout=300).one()[0]
                assert count==DEVICES*STEPS,(table,count)
                print(f'COUNT VERIFIED {table}={count}',flush=True)
            print(nodetool(0,'flush',KS),flush=True)
        if options.phase in ('all','benchmark'):benchmark(session)
        if options.phase in ('all','benchmark','daily-aggregation'):daily_aggregation(session)
        if options.phase in ('all','optimize'):optimize(session)
        if options.phase in ('all','weekly'):weekly(session)
        if options.phase in ('all','maintenance'):maintenance(session)
    print('LAB3 PHASE PASS '+options.phase,flush=True)

if __name__=='__main__':main()
