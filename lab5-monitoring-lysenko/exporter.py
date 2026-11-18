"""Local model telemetry through Kafka -> manual-commit Cassandra sink, with real counters."""
import json,math,sys,threading,time,traceback
from pathlib import Path
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
ROOT=Path(__file__).resolve().parent;sys.path.insert(0,str(ROOT.parent/'lab-support'))
from common import *
from prometheus_client import Counter,Gauge,Histogram,generate_latest,CONTENT_TYPE_LATEST
from kafka import KafkaProducer,KafkaConsumer
from kafka.admin import KafkaAdminClient,NewTopic
from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

STATIONS=['KYIV','KANIV','KREMENCHUK','DNIPRO','KAMIANSKE','KAKHOVKA_MODEL'];TYPES=['kaplan','francis','pelton']
fault=False;started=time.time();E=ROOT/'evidence';E.mkdir(exist_ok=True)
sent=Counter('energy_messages_sent','Acknowledged Kafka sends',['station'])
received=Counter('energy_messages_stored','Successful Cassandra upserts before offset commit',['station'])
errors=Counter('energy_pipeline_errors','Exceptions in the real pipeline')
ack=Histogram('energy_send_ack_seconds','Kafka send acknowledgement latency',buckets=(.001,.005,.01,.025,.05,.1,.25,.5,1,5))
sinktime=Histogram('energy_sink_write_seconds','Cassandra upsert latency',buckets=(.001,.005,.01,.025,.05,.1,.25,.5,1,5))
alive=Gauge('energy_producer_alive','Pipeline worker is alive')
lag=Gauge('energy_consumer_lag','End offset minus next consumed offset',['partition'])
metrics={name:Gauge('hydro_'+name,description,['station','turbine_type']) for name,description in {
    'efficiency_percent':'Synthetic modeled turbine efficiency percent','head_m':'Net hydraulic head metres','flow_m3s':'Water flow cubic metres per second',
    'power_mw':'Generated power MW from rho*g*H*Q*eta','voltage_kv':'Generator model voltage kV','frequency_hz':'Grid model frequency Hz',
    'cavitation':'Injected or off-design cavitation model risk 0/1','offdesign':'Distance from design point risk 0/1','generator_temperature_celsius':'Generator model winding temperature',
    'generator_running':'Generator state 0/1'}.items()}
stream_windows=Gauge('hydro_stream_windows_verified','Last actual verified Streams trial window count')
sagas=Gauge('hydro_saga_scenarios_verified','Actual validated Saga scenarios')

def eta(head,flow,kind):
    optimum={'kaplan':(20,1200),'francis':(30,1000),'pelton':(80,350)}[kind]
    return max(50,94-16*((head/optimum[0]-1)**2+(flow/optimum[1]-1)**2))

def pipeline():
    global fault
    topic='l5-lysenko-telemetry-'+str(time.time_ns())[-10:]
    admin=KafkaAdminClient(bootstrap_servers=BOOTSTRAP);admin.create_topics([NewTopic(topic,1,1)]);admin.close()
    cluster=Cluster(CONTACT_POINTS,protocol_version=4);s=cluster.connect()
    s.execute("CREATE KEYSPACE IF NOT EXISTS energy_l5_lysenko WITH replication={'class':'NetworkTopologyStrategy','dc1':3}")
    s.execute('CREATE TABLE IF NOT EXISTS energy_l5_lysenko.telemetry (station text, day date, ts timestamp, payload text, PRIMARY KEY ((station,day),ts)) WITH default_time_to_live=86400')
    prepared=s.prepare('INSERT INTO energy_l5_lysenko.telemetry (station,day,ts,payload) VALUES (?,?,?,?)');prepared.consistency_level=ConsistencyLevel.LOCAL_QUORUM
    producer=KafkaProducer(bootstrap_servers=BOOTSTRAP,acks='all',value_serializer=lambda x:json.dumps(x).encode())
    consumer=KafkaConsumer(topic,bootstrap_servers=BOOTSTRAP,group_id=topic+'-sink',enable_auto_commit=False,isolation_level='read_committed',auto_offset_reset='earliest')
    write_json(E/'live-pipeline.json',{'topic':topic,'interval_seconds':1,'stations':6,'time_acceleration_vs_10s_spec':10,'synthetic':True,'started_utc':time.time(),'consistency':'LOCAL_QUORUM'})
    from datetime import datetime,timezone
    iteration=0
    try:
        while True:
            began=time.monotonic();alive.set(1);iteration+=1
            for i,station in enumerate(STATIONS):
                kind=TYPES[i%3];optimal={'kaplan':(20,1200),'francis':(30,1000),'pelton':(80,350)}[kind]
                head=optimal[0]*(1+.04*math.sin(iteration/20+i));flow=optimal[1]*(1+.1*math.sin(iteration/15+i));eff=86 if fault else eta(head,flow,kind)
                values={'efficiency_percent':eff,'head_m':head,'flow_m3s':flow,'power_mw':1000*9.81*head*flow*eff/100/1e6,
                    'voltage_kv':208 if fault else 220+2*math.sin(iteration/13+i),'frequency_hz':49.2 if fault else 50+.02*math.sin(iteration/17),
                    'cavitation':int(fault),'offdesign':int(fault),'generator_temperature_celsius':90 if fault else 58+3*math.sin(iteration/30),'generator_running':1}
                event={'station':station,'turbine_type':kind,'ts':time.time(),**values}
                begin=time.perf_counter();producer.send(topic,key=station.encode(),value=event).get(timeout=30);ack.observe(time.perf_counter()-begin);sent.labels(station).inc()
                for key,value in values.items():metrics[key].labels(station,kind).set(value)
            batch=consumer.poll(500)
            for tp,records in batch.items():
                for r in records:
                    v=json.loads(r.value);ts=datetime.fromtimestamp(v['ts'],timezone.utc);begin=time.perf_counter()
                    s.execute(prepared,(v['station'],ts.date(),ts,r.value.decode()));sinktime.observe(time.perf_counter()-begin);received.labels(v['station']).inc()
                lag.labels(str(tp.partition)).set(consumer.end_offsets([tp])[tp]-consumer.position(tp))
            if batch:consumer.commit()
            trials=ROOT.parent/'lab4-kafka-streams-lysenko/evidence/stream-trials.json'
            if trials.exists():stream_windows.set(read_json(trials)[-1]['windows'])
            sagafile=ROOT.parent/'lab4-kafka-streams-lysenko/evidence/sagas.json'
            if sagafile.exists():sagas.set(len(read_json(sagafile)))
            time.sleep(max(0,1-(time.monotonic()-began)))
    except Exception:
        errors.inc();alive.set(0);(E/'pipeline-error.txt').write_text(traceback.format_exc(),encoding='utf-8');traceback.print_exc()
    finally:consumer.close(autocommit=False);producer.close();cluster.shutdown()

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path=='/metrics':body=generate_latest();ctype=CONTENT_TYPE_LATEST
        elif self.path=='/hill.png':body=(E/'figures/hill.png').read_bytes();ctype='image/png'
        elif self.path=='/health':body=json.dumps({'fault':fault,'started':started}).encode();ctype='application/json'
        else:self.send_error(404);return
        self.send_response(200);self.send_header('Content-Type',ctype);self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body)
    def do_POST(self):
        global fault
        if self.path not in ('/fault/on','/fault/off'):self.send_error(404);return
        fault=self.path.endswith('/on');self.send_response(200);self.end_headers();self.wfile.write(b'ok')
    def log_message(self,*args):pass

if __name__=='__main__':
    threading.Thread(target=pipeline,daemon=True).start();ThreadingHTTPServer(('127.0.0.1',18400),Handler).serve_forever()
