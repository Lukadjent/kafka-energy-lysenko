"""Execute Streams, delivery, idempotent sink and Saga experiments against real services."""
import argparse,collections,concurrent.futures,csv,hashlib,json,math,os,secrets,socket,subprocess,sys,time,tempfile
from contextlib import closing
from pathlib import Path
from datetime import datetime,timezone
ROOT=Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT.parent/'lab-support'))
from common import *
from kafka import KafkaProducer,KafkaConsumer,TopicPartition
from kafka.admin import KafkaAdminClient,NewTopic
from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
from saga import query,KS
E=ROOT/'evidence';E.mkdir(exist_ok=True)
RUN=RUNTIME/'lab4';RUN.mkdir(exist_ok=True)
CLASSES=RUN/'classes';STATIONS=['KYIV','KANIV','KREMENCHUK','DNIPRO','KAMIANSKE','KAKHOVKA_MODEL']

def compile_java():
    CLASSES.mkdir(exist_ok=True)
    subprocess.run([str(JDK17/'bin/javac.exe'),'-encoding','UTF-8','-cp',str(KAFKA/'libs/*'),'-d',str(CLASSES),*[str(x) for x in (ROOT/'src').glob('*.java')]],check=True)

def topics(prefix,names):
    with closing(KafkaAdminClient(bootstrap_servers=BOOTSTRAP)) as admin:
        admin.create_topics([NewTopic(prefix+'-'+name,1,1,topic_configs={'retention.ms':'86400000'}) for name in names])

def produce(topic,items):
    with closing(KafkaProducer(bootstrap_servers=BOOTSTRAP,acks='all',linger_ms=5,key_serializer=lambda x:x.encode(),value_serializer=lambda x:json.dumps(x,separators=(',',':')).encode())) as producer:
        for key,value,ts in items:producer.send(topic,key=key,value=value,timestamp_ms=ts)
        producer.flush()

def read_topic(topic,isolation='read_committed',timeout=20):
    consumer=KafkaConsumer(bootstrap_servers=BOOTSTRAP,enable_auto_commit=False,auto_offset_reset='earliest',isolation_level=isolation,
        key_deserializer=lambda x:x.decode() if x else None,value_deserializer=lambda x:json.loads(x))
    tp=TopicPartition(topic,0);consumer.assign([tp]);consumer.seek_to_beginning(tp);items=[];deadline=time.monotonic()+timeout
    while time.monotonic()<deadline:
        for records in consumer.poll(500).values():items.extend({'key':r.key,'value':r.value,'offset':r.offset,'received_ns':time.perf_counter_ns()} for r in records)
        if consumer.position(tp)>=consumer.end_offsets([tp])[tp]:break
    consumer.close();return items

def start_java(cls,args,name,control=False):
    log=(E/(name+'.log')).open('w',encoding='utf-8');token=secrets.token_hex(20)
    cp=os.pathsep.join([str(CLASSES),str(RUNTIME/'classes'),str(KAFKA/'libs/*')])
    command=[str(JDK17/'bin/java.exe'),'-Xmx512m','-XX:ActiveProcessorCount=2','-Dfile.encoding=UTF-8',
        '-Dlog4j.configuration='+(ROOT/'log4j.properties').as_uri(),'-cp',cp]
    if control:command+=['LabService',cls,'18194',token]
    else:command+=[cls]
    process=subprocess.Popen(command+args,cwd=ROOT,stdout=log,stderr=subprocess.STDOUT,creationflags=subprocess.CREATE_NO_WINDOW)
    log.close();return process,token

def stop_java(process,token):
    if process.poll() is None:
        with socket.create_connection(('127.0.0.1',18194),timeout=3) as connection:
            connection.sendall((token+'\n').encode());assert connection.recv(100).strip()==b'stopping'
        assert process.wait(40)==0

def wait_file(path,process,timeout=100):
    end=time.monotonic()+timeout
    while not path.exists():
        if process.poll() is not None:raise RuntimeError(f'Java exited {process.returncode}; inspect its log')
        if time.monotonic()>end:raise TimeoutError(str(path))
        time.sleep(.2)

def dataset():
    base=(int(time.time()*1000)//600000)*600000-10800000
    refs={station:{'target':90-i*2,'minFlow':700,'maxFlow':1900,'weight':1+i*.2} for i,station in enumerate(STATIONS)}
    events=[]
    for step in range(1080):
        for i,station in enumerate(STATIONS):
            upper=90-i*2+.4*math.sin(step/120+i)+(6 if i==0 and step%97==0 else 0)
            events.append({'id':f'{station}-{step}','station':station,'ts':base+step*10000,'tick':False,
                'upper':upper,'lower':upper-18-i,'flow':1200+80*math.sin(step/90+i),'power':180+10*math.sin(step/90+i),
                'gate':45+5*math.sin(step/60),'efficiency':.82 if i==3 and step%149==0 else .915+.008*math.sin(step/100)})
    return base,refs,events

def oracle(base,refs,events):
    windows=collections.defaultdict(list)
    for e in events:
        floor=e['ts']//600000*600000
        for start in (floor-1200000,floor-600000,floor):windows[start].append(e)
    result={}
    for start,values in sorted(windows.items()):
        stations={};weighted=weights=0
        for station,ref in refs.items():
            points=[e for e in values if e['station']==station];first,last=points[0],points[-1]
            slope=(last['upper']-first['upper'])/(last['ts']-first['ts']) if len(points)>1 else 0
            prediction=last['upper']+slope*600000;flow=sum(p['flow'] for p in points)/len(points)
            desired=flow+20*(prediction-ref['target']);weighted+=ref['weight']*desired;weights+=ref['weight']
            stations[station]={'count':len(points),'mean_flow':flow,'mean_upper':sum(p['upper'] for p in points)/len(points),
                'mean_power':sum(p['power'] for p in points)/len(points),'mean_efficiency':sum(p['efficiency'] for p in points)/len(points),
                'prediction_10m':prediction,'desired_flow':desired,'weight':ref['weight']}
        q=max(700,min(1900,weighted/weights));result[start]={'start':start,'end':start+1800000,'count':len(values),'stations':stations,'coordinated_flow':q,
            'objective':sum(s['weight']*(q-s['desired_flow'])**2 for s in stations.values())}
    return result

def compare(expected,actual,path='root'):
    if isinstance(expected,dict):
        assert set(expected)==set(actual),(path,set(expected),set(actual))
        for key in expected:compare(expected[key],actual[key],path+'.'+str(key))
    elif isinstance(expected,(int,float)):assert math.isclose(expected,actual,rel_tol=1e-9,abs_tol=1e-7),(path,expected,actual)
    else:assert expected==actual,(path,expected,actual)

def stream_trial(mode,rep,fault=False):
    suffix=str(time.time_ns())[-10:]
    name=f'stream-{mode}-{rep}'+('-recovery' if fault else '')+'-'+suffix;prefix='l4-lysenko-s-'+suffix
    topics(prefix,['input','reference','output','invalid','anomaly'])
    base,refs,events=dataset();expected=oracle(base,refs,events)
    produce(prefix+'-reference',[(key,value,base) for key,value in refs.items()])
    ready=RUN/(name+'.ready');marker=RUN/(name+'.fault')
    # RocksDB's native Windows backend needs short paths; this is an owned ASCII temp directory.
    state=Path(tempfile.gettempdir())/'lysenko-l4-rocksdb'/suffix;state.mkdir(parents=True,exist_ok=True)
    args=[BOOTSTRAP,prefix,'ly4-'+suffix,mode,str(state),str(ready),str(marker) if fault else 'none']
    process,token=start_java('HydroStreams',args,name,True)
    try:wait_file(ready,process)
    except Exception:stop_java(process,token);raise
    invalid=[{**events[i], 'id':f'INVALID-{i}','flow':-1} for i in range(3)]
    inputs=sorted(events+invalid,key=lambda e:e['ts'])
    inputs.append({**events[0],'id':'WATERMARK','tick':True,'ts':base+10800000+1860000})
    began=time.perf_counter();produce(prefix+'-input',[('DNIPRO',e,e['ts']) for e in inputs])
    deadline=time.monotonic()+150;restarts=0
    try:
        while time.monotonic()<deadline:
            if process.poll() is not None:
                assert fault and process.returncode==17 and restarts==0,(process.returncode,name)
                process,token=start_java('HydroStreams',args,name+'-restart',True);restarts+=1
            outputs=read_topic(prefix+'-output',timeout=2)
            if len(outputs)>=len(expected):break
            time.sleep(.5)
        elapsed=time.perf_counter()-began
        assert len(outputs)==len(expected),(name,len(outputs),len(expected))
        seen={}
        for item in outputs:
            value=item['value'];assert value['start'] not in seen;compare(expected[value['start']],value);seen[value['start']]=value
        anomalies=read_topic(prefix+'-anomaly');invalid_actual=read_topic(prefix+'-invalid')
        expected_anomalies={e['id'] for e in events if e['upper']>95 or e['efficiency']<.88}
        assert {r['value']['id'] for r in anomalies}==expected_anomalies
        assert len(anomalies)==len(expected_anomalies);assert len(invalid_actual)==3
        result={'name':name,'prefix':prefix,'mode':mode,'recovery':fault,'restarts':restarts,'valid_input':len(events),'input_with_invalid_tick':len(inputs),
            'windows':len(outputs),'window_memberships':sum(x['count'] for x in seen.values()),'anomalies':len(anomalies),'invalid':len(invalid_actual),
            'seconds_until_all_outputs_read':elapsed,'valid_events_per_second':len(events)/elapsed,'oracle_all_fields_match':True,
            'event_time_start_ms':base,'event_time_hours':3,'natural_rate_per_second':.6,
            'application_id':args[2],'state_directory':str(state),'fault_marker':marker.read_text() if marker.exists() else None,
            'scope':'Accelerated 3h replay; wall-clock includes producer flush, Streams processing and output polling; not per-record latency'}
        write_json(E/(name+'.json'),{'summary':result,'outputs':outputs,'anomalies':anomalies,'expected_anomaly_ids':sorted(expected_anomalies),'input_samples':inputs[:3]})
        return result
    finally:stop_java(process,token)

def delivery_trial(mode,rep,fault=False):
    name=f'delivery-{mode}-{rep}'+('-crash' if fault else '')+'-'+str(time.time_ns())[-10:];prefix='l4-lysenko-'+name
    topics(prefix,['input','output']);n=1000;produce(prefix+'-input',[(str(i),{'id':i},int(time.time()*1000)) for i in range(n)])
    marker=RUN/(name+'.fault');result=E/(name+'-timing.json');args=[BOOTSTRAP,prefix,name,mode,str(marker) if fault else 'none',str(result)]
    began=time.perf_counter();process,_=start_java('DeliveryProbe',args,name);code=process.wait(90);restarts=0
    if fault:
        assert code==17;process,_=start_java('DeliveryProbe',args,name+'-restart');code=process.wait(90);restarts=1
    assert code==0,(name,code)
    elapsed=time.perf_counter()-began;output=read_topic(prefix+'-output');ids=[r['value']['id'] for r in output];counts=collections.Counter(ids)
    uncommitted=read_topic(prefix+'-output','read_uncommitted')
    missing=n-len(counts);duplicates=len(ids)-len(counts)
    if fault:
        assert (missing,duplicates)=={'at_most_once':(100,0),'at_least_once':(0,100),'exactly_once':(0,0)}[mode],(mode,missing,duplicates)
        if mode=='exactly_once':assert len(uncommitted)==n+100
    else:assert missing==duplicates==0
    result={'name':name,'mode':mode,'fault':fault,'restarts':restarts,'input':n,'committed_output':len(ids),'uncommitted_output':len(uncommitted),
        'missing':missing,'duplicates':duplicates,'seconds_including_startup_restart':elapsed,'timing':read_json(result),'prefix':prefix}
    write_json(E/(name+'.json'),result);return result

def sink_trial(session,trial):
    topic=trial['prefix']+'-output';group=trial['name']+'-sink';run=trial['name']
    attempts=0
    for crash in (True,False):
        consumer=KafkaConsumer(topic,bootstrap_servers=BOOTSTRAP,group_id=group,enable_auto_commit=False,isolation_level='read_committed',auto_offset_reset='earliest',max_poll_records=100)
        deadline=time.monotonic()+30;received=0
        while time.monotonic()<deadline:
            batch=consumer.poll(1000)
            for records in batch.values():
                for r in records:
                    v=json.loads(r.value);timestamp=datetime.fromtimestamp(v['start']/1000,timezone.utc)
                    query(session,f'INSERT INTO {KS}.cascade_flow_aggregates (run_id,window_start,payload) VALUES (%s,%s,%s)',(run,timestamp,r.value.decode()))
                    for station,s in v['stations'].items():query(session,f'INSERT INTO {KS}.station_coordination (run_id,station,window_start,predicted_level,coordinated_flow) VALUES (%s,%s,%s,%s,%s)',(run,station,timestamp,s['prediction_10m'],v['coordinated_flow']))
                    attempts+=1;received+=1
            if batch:
                if not crash:consumer.commit()
                if received>=20:break
        consumer.close(autocommit=False);assert received==20
    count=query(session,f'SELECT COUNT(*) FROM {KS}.cascade_flow_aggregates WHERE run_id=%s',(run,)).one()[0]
    assert count==20
    return {'run_id':run,'write_attempts':attempts,'unique_rows':count,'fault':'connection closed after Cassandra upserts, before Kafka commit','manual_commit':True,'isolation':'read_committed'}

def saga_trials(session):
    results=[]
    for name,fail,crash in [('success',False,False),('compensation',True,False),('recovery',False,True)]:
        saga='lysenko-'+name+'-'+str(time.time_ns())[-10:];target=E/f'saga-{name}.json';marker=RUN/(saga+'.fault')
        command=[sys.executable,str(ROOT/'saga.py'),saga,'--result',str(target)]
        if fail:command+=['--fail']
        if crash:command+=['--marker',str(marker)]
        process=subprocess.run(command)
        if crash:assert process.returncode==19;subprocess.run(command,check=True)
        else:assert process.returncode==0
        result=read_json(target);assert result['state']['status']==('COMPENSATED' if fail else 'COMPLETED')
        assert result['participant']['gate']==(20 if fail else 60)
        assert result['participant']['version']==(4 if fail else 2)
        assert result['lower_participant']['prepared']==(not fail)
        if crash:assert result['attempts'][0]['deduplicated'] is True
        results.append({'scenario':name,**result});print('SAGA PASS '+name,flush=True)
    write_json(E/'sagas.json',results)

def main():
    p=argparse.ArgumentParser();p.add_argument('--phase',choices=['all','compile','streams','recovery','delivery','saga','audit'],default='all');a=p.parse_args();compile_java()
    if a.phase=='compile':return
    with Cluster(CONTACT_POINTS,protocol_version=4) as cluster:
        session=cluster.connect()
        for cql in (ROOT/'schema.cql').read_text().split(';'):
            if cql.strip():session.execute(cql)
        if a.phase in ('all','streams'):
            trials=[]
            for mode in ('exactly_once_v2','at_least_once'):
                for rep in range(3):trials.append(stream_trial(mode,rep));print('STREAM PASS '+trials[-1]['name'],flush=True)
            trials.append(stream_trial('exactly_once_v2',0,True));write_json(E/'stream-trials.json',trials)
            write_json(E/'sink.json',sink_trial(session,trials[-1]))
        if a.phase in ('all','delivery'):
            trials=[]
            for mode in ('at_most_once','at_least_once','exactly_once'):
                for rep in range(3):trials.append(delivery_trial(mode,rep));print('DELIVERY PASS '+trials[-1]['name'],flush=True)
                trials.append(delivery_trial(mode,0,True));print('DELIVERY FAULT PASS '+mode,flush=True)
            write_json(E/'delivery-trials.json',trials)
        if a.phase=='recovery':
            trial=stream_trial('exactly_once_v2',0,True)
            trials=[t for t in read_json(E/'stream-trials.json') if not t['recovery']]+[trial]
            write_json(E/'stream-trials.json',trials);write_json(E/'sink.json',sink_trial(session,trial))
        if a.phase in ('all','saga'):saga_trials(session)
        if a.phase in ('all','audit'):
            with (E/'delivery-output-ids.csv').open('w',encoding='utf-8',newline='') as out:
                writer=csv.writer(out);writer.writerow(['trial','reader','offset','event_id'])
                for trial in read_json(E/'delivery-trials.json'):
                    for isolation,expected_count in [('read_committed',trial['committed_output']),('read_uncommitted',trial['uncommitted_output'])]:
                        records=read_topic(trial['prefix']+'-output',isolation)
                        assert len(records)==expected_count
                        writer.writerows((trial['name'],isolation,r['offset'],r['value']['id']) for r in records)
                    print('DELIVERY AUDIT '+json.dumps({k:trial[k] for k in ['mode','fault','input','committed_output','uncommitted_output','missing','duplicates']}),flush=True)
    print('LAB4 PHASE PASS '+a.phase,flush=True)
if __name__=='__main__':main()
