"""Independent reconciliation of raw samples, output IDs and alert states."""
import collections,csv,hashlib,math,statistics
from common import *

checks=[]
def check(name,condition):
    if not condition:raise AssertionError(name)
    checks.append(name);print('PASS '+name)
def close(a,b):return math.isclose(a,b,rel_tol=1e-10,abs_tol=1e-7)
def quantile(v,p):return sorted(v)[math.ceil(len(v)*p)-1]
def reconcile(name,samples,result):
    check(name+' sample count',len(samples)==result['n'])
    check(name+' statistics',all(close(a,b) for a,b in zip([statistics.mean(samples),statistics.median(samples),quantile(samples,.95),quantile(samples,.99)],
        [result['avg_ms'],result['p50_ms'],result['p95_ms'],result['p99_ms']])))

def main():
    l3=WORKSPACE/'lab3-cassandra-optimization-lysenko/evidence';l4=WORKSPACE/'lab4-kafka-streams-lysenko/evidence';l5=WORKSPACE/'lab5-monitoring-lysenko/evidence'
    check('L3 deterministic dataset >=1M',read_json(l3/'dataset.json')['records']==1296000)
    with (l3/'read-samples.csv').open(encoding='utf-8',newline='') as f:rows=list(csv.DictReader(f))
    check('L3 all 12 read plans x60',len(rows)==720)
    for result in read_json(l3/'read-benchmark.json'):
        samples=[float(r['ms']) for r in rows if r['table']==result['table'] and r['query']==result['query']]
        reconcile(result['table']+'/'+result['query'],samples,result)
    for result in read_json(l3/'write-benchmark.json'):
        with (l3/f"write-samples-{result['table']}.csv").open(encoding='utf-8',newline='') as f:samples=[float(row['batch_ack_ms']) for row in csv.DictReader(f)]
        reconcile('write '+result['table'],samples,result['batch_ack'])
        check('write throughput '+result['table'],result['rows']==1296000 and close(result['rows']/result['load_seconds'],result['rows_per_second']))
    check('L3 full COUNTs recorded',all(f'COUNT VERIFIED {table}=1296000' in (l3/'console-all.txt').read_text(encoding='utf-8') for table in ['simple','hourly','daily']))
    dailyagg=read_json(l3/'daily-aggregation.json')
    check('L3 CQL daily aggregates match CSV',all(all(close(x['aggregates'][k],v) for k,v in dailyagg['csv_oracle'].items()) and x['aggregates']['n']==2880 for x in dailyagg['results']))
    for x in dailyagg['results']:reconcile('daily CQL aggregation '+x['table'],x['samples_ms'],x)
    for i in (1,2,3):
        tables=read_json(l3/f'nodetool-tablestats-node{i}.json')['energy_l3_lysenko']['tables']
        check(f'L3 real MV storage node{i}',all(tables[n]['local_write_count']>0 for n in ['by_level_band','by_type_hour']))
        check(f'L3 sizes/compression node{i}',all(tables[n]['compacted_partition_maximum_bytes']<100*1048576 and tables[n]['sstable_compression_ratio']<.6 for n in ['simple','hourly','daily']))
    check('L3 TTL expired',read_json(l3/'maintenance.json')['expired_verified'])
    streams=read_json(l4/'stream-trials.json');check('L4 6 normal +1 recovery',len(streams)==7 and sum(x['recovery'] for x in streams)==1)
    for t in streams:
        check('L4 windows '+t['name'],t['windows']==20 and t['window_memberships']==19440 and t['oracle_all_fields_match'])
        raw=read_json(l4/(t['name']+'.json'));check('L4 actual unique window outputs '+t['name'],len({x['value']['start'] for x in raw['outputs']})==20)
    recovery=next(t for t in streams if t['recovery']);log=(l4/(recovery['name']+'-restart.log')).read_text(encoding='utf-8')
    check('L4 persistent recovery log',recovery['restarts']==1 and 'restor' in log.lower())
    delivery=read_json(l4/'delivery-trials.json')
    with (l4/'delivery-output-ids.csv').open(encoding='utf-8',newline='') as f:ids=list(csv.DictReader(f))
    check('L4 delivery trial count',len(delivery)==12)
    for t in delivery:
        values=[int(x['event_id']) for x in ids if x['trial']==t['name'] and x['reader']=='read_committed'];unique=set(values)
        check('L4 IDs '+t['name'],len(values)==t['committed_output'] and 1000-len(unique)==t['missing'] and len(values)-len(unique)==t['duplicates'])
        target={'at_most_once':(100,0),'at_least_once':(0,100),'exactly_once':(0,0)}[t['mode']] if t['fault'] else (0,0)
        check('L4 delivery semantics '+t['name'],(t['missing'],t['duplicates'])==target)
    check('L4 idempotent Cassandra sink',read_json(l4/'sink.json')['write_attempts']==40 and read_json(l4/'sink.json')['unique_rows']==20)
    for t in read_json(l4/'sagas.json'):
        check('L4 Saga '+t['scenario'],t['state']['status']==('COMPENSATED' if t['scenario']=='compensation' else 'COMPLETED'))
        if t['scenario']=='recovery':check('L4 atomic effect deduplication',t['attempts'][0]['deduplicated'] and t['participant']['version']==2)
    check('L5 all production promtool checks',all(x['exit_code']==0 for x in read_json(l5/'promtool.json')))
    live=read_json(l5/'live-alert-states.json');check('L5 live pending/firing/recovery',set(live['seen_states'])=={'pending','firing'} and live['resolved'])
    check('L5 Grafana-managed rule fired','firing' in live['grafana_seen_states'])
    grafana_rules=read_json(l5/'grafana-runtime-rules.json')['data']['groups']
    check('L5 Grafana-managed rule recovered',all(rule['state']=='inactive' for group in grafana_rules for rule in group['rules']))
    queries=read_json(l5/'prometheus-queries.json')['queries'];check('L5 all queries nonempty',all(x['status']=='success' and x['data']['result'] for x in queries.values()))
    up=queries['up']['data']['result'];check('L5 five targets UP',len(up)==5 and all(float(x['value'][1])==1 for x in up))
    check('L5 no pipeline errors',float(queries['energy_pipeline_errors_total']['data']['result'][0]['value'][1])==0)
    check('L5 real Grafana rule provisioned',len(read_json(l5/'grafana-alert-api.json'))>=1)
    for uid,count in [('hydro-lysenko',13),('streams-lysenko',5)]:check('L5 dashboard '+uid,len(read_json(l5/(uid+'-dashboard-api.json'))['dashboard']['panels'])==count)
    write_json(SUPPORT/'validation.json',{'passed':len(checks),'checks':checks,'all_passed':True})
    print(f'ALL {len(checks)} CHECKS PASS')
if __name__=='__main__':main()
