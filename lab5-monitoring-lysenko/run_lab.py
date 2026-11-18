"""Provision portable Prometheus/Grafana; verify rules and capture real HTTP evidence."""
import argparse,base64,json,os,secrets,subprocess,sys,time,urllib.parse,urllib.request
from pathlib import Path
ROOT=Path(__file__).resolve().parent;sys.path.insert(0,str(ROOT.parent/'lab-support'))
from common import *
import yaml,psutil
E=ROOT/'evidence';E.mkdir(exist_ok=True)
CONFIG=ROOT/'config';CONFIG.mkdir(exist_ok=True)
STATE=RUNTIME/'monitoring-services.json';SECRET=RUNTIME/'grafana-secret.json'
PROM=RUNTIME/'prometheus-3.16.0.windows-amd64';GRAFANA=RUNTIME/'grafana-13.3.1'

def http(path,method=None,admin=False):
    request=urllib.request.Request(path,method=method)
    if admin:
        credential=read_json(SECRET);token=base64.b64encode(('lysenko:'+credential['password']).encode()).decode();request.add_header('Authorization','Basic '+token)
    with urllib.request.urlopen(request,timeout=45) as response:
        raw=response.read();return json.loads(raw) if 'json' in response.headers.get('Content-Type','') else raw.decode()

def query(expr):return http('http://127.0.0.1:19090/api/v1/query?'+urllib.parse.urlencode({'query':expr}))

def panel(pid,title,expr,x,y,w=8,h=7,unit='short',kind='timeseries'):
    legend='{{station}}' if expr.startswith('hydro_') else '{{operation}} {{instance}}' if expr.startswith('cassandra_') else '{{alertname}} / {{alertstate}}' if 'ALERTS' in expr else 'Value'
    return {'id':pid,'title':title,'type':kind,'gridPos':{'x':x,'y':y,'w':w,'h':h},'datasource':{'type':'prometheus','uid':'hydro-prom'},
        'targets':[{'refId':'A','expr':expr,'legendFormat':legend}],
        'fieldConfig':{'defaults':{'unit':unit,'color':{'mode':'palette-classic'},'custom':{'lineWidth':2,'fillOpacity':8}},'overrides':[]},'options':{'legend':{'displayMode':'list','placement':'bottom'},'tooltip':{'mode':'multi'}}}

def configure():
    for path in (RUNTIME/'grafana-data',RUNTIME/'grafana-plugins'):
        path.mkdir(parents=True,exist_ok=True)
    config={'global':{'scrape_interval':'5s','evaluation_interval':'5s'},'rule_files':[str(ROOT/'alerts.yml')],
        'scrape_configs':[{'job_name':'hydro-python','static_configs':[{'targets':['127.0.0.1:18400']}]},
            {'job_name':'kafka-jmx','static_configs':[{'targets':['127.0.0.1:18080']}]},
            {'job_name':'cassandra-jmx','static_configs':[{'targets':['127.0.0.1:'+str(p) for p in (18081,18082,18083)]}]}]}
    (CONFIG/'prometheus.yml').write_text(yaml.safe_dump(config,allow_unicode=True),encoding='utf-8')
    provision=CONFIG/'provisioning'
    for folder in ('datasources','dashboards','alerting'):(provision/folder).mkdir(parents=True,exist_ok=True)
    (provision/'datasources/prometheus.yml').write_text(yaml.safe_dump({'apiVersion':1,'datasources':[{'name':'Hydro Prometheus','uid':'hydro-prom','type':'prometheus','access':'proxy','url':'http://127.0.0.1:19090','isDefault':True,'editable':False}]}),encoding='utf-8')
    dashboard_path=CONFIG/'dashboards';dashboard_path.mkdir(exist_ok=True)
    (provision/'dashboards/hydro.yml').write_text(yaml.safe_dump({'apiVersion':1,'providers':[{'name':'Hydro labs','folder':'Lysenko 3B','type':'file','options':{'path':str(dashboard_path)}}]}),encoding='utf-8')
    panels=[panel(1,'ККД турбін (%)','hydro_efficiency_percent',0,0,8,7,'percent'),panel(2,'Потужність (МВт)','hydro_power_mw',8,0,8,7,'suffix: MW'),
        panel(3,'Кавітація: 0 норма / 1 ризик','hydro_cavitation',16,0,8,7),
        panel(4,'Kafka → Cassandra: подій/с','sum(rate(energy_messages_stored_total[1m]))',0,7,8,7,'ops'),
        panel(5,'Напруга (кВ)','hydro_voltage_kv',8,7,8,7),panel(6,'Частота (Гц)','hydro_frequency_hz',16,7,8,7,'hertz'),
        panel(7,'Температура генератора (°C)','hydro_generator_temperature_celsius',0,14,8,7,'celsius'),
        panel(8,'Генератори: стан 0/1','hydro_generator_running',8,14,8,7),panel(9,'Consumer lag','energy_consumer_lag',16,14,8,7),
        panel(10,'Cassandra write p95, мкс','cassandra_client_latency_microseconds{operation="Write",percentile="95thPercentile"}',0,21,12,7,'µs'),
        panel(11,'JMX: JVM heap Cassandra','cassandra_heap_used_bytes',12,21,12,7,'bytes'),
        {'id':12,'title':'Hill chart: модель η(H,Q), три типи турбін','type':'text','gridPos':{'x':0,'y':28,'w':24,'h':16},'options':{'mode':'html','content':'<p>Синтетична характеристика; точки оптимуму задані моделлю, не паспортні дані ГЕС.</p><img src="http://127.0.0.1:18400/hill.png" width="100%" />'}},
        panel(13,'Prometheus ALERTS: production і прискорений тест','sum by(alertname,alertstate)(ALERTS)',0,44,24,8)]
    dashboards=[{'uid':'hydro-lysenko','title':'ЛР5 · ГЕС · Лисенко · 3Б','panels':panels},
        {'uid':'streams-lysenko','title':'ЛР4 · Streams і Saga · Лисенко · 3Б','panels':[
            panel(1,'Перевірені вікна останнього Streams тесту','hydro_stream_windows_verified',0,0,8,7,kind='stat'),
            panel(2,'Перевірені сценарії Saga','hydro_saga_scenarios_verified',8,0,8,7,kind='stat'),
            panel(3,'Kafka broker: MessagesIn total','kafka_broker_messages_in_total',16,0,8,7),
            panel(4,'Реальний потік Kafka → Cassandra','sum(rate(energy_messages_stored_total[1m]))',0,7,12,8,'ops'),
            panel(5,'Producer ack p95','histogram_quantile(0.95, sum by (le) (rate(energy_send_ack_seconds_bucket[1m])))',12,7,12,8,'s')]}]
    for d in dashboards:
        d.update({'schemaVersion':39,'version':1,'timezone':'utc','refresh':'5s','time':{'from':'now-5m','to':'now'},'tags':['Lysenko','3B','synthetic-model']})
        write_json(dashboard_path/(d['uid']+'.json'),d)
    alert={'apiVersion':1,'groups':[{'orgId':1,'name':'hydro-grafana','folder':'Lysenko 3B','interval':'10s','rules':[{
        'uid':'hydro-low-eff-test','title':'Hydro efficiency: accelerated 15s lab test','condition':'B','for':'15s','noDataState':'NoData','execErrState':'Error',
        'labels':{'profile':'accelerated-e2e'},'annotations':{'summary':'Local laboratory test; production Prometheus rule retains 2h'},
        'data':[{'refId':'A','datasourceUid':'hydro-prom','relativeTimeRange':{'from':60,'to':0},'model':{'refId':'A','expr':'hydro_efficiency_percent','instant':True,'range':False,'intervalMs':1000,'maxDataPoints':43200}},
            {'refId':'B','datasourceUid':'__expr__','relativeTimeRange':{'from':0,'to':0},'model':{'refId':'B','type':'threshold','expression':'A','conditions':[{'evaluator':{'params':[88],'type':'lt'},'operator':{'type':'and'},'query':{'params':['B']},'reducer':{'type':'last'},'type':'query'}]}}]}]}]}
    (provision/'alerting/hydro.yml').write_text(yaml.safe_dump(alert,allow_unicode=True),encoding='utf-8')
    if not SECRET.exists():write_json(SECRET,{'password':secrets.token_urlsafe(24)})
    password=read_json(SECRET)['password']
    (RUNTIME/'grafana.ini').write_text(f'''[paths]
data = {str(RUNTIME/'grafana-data')}
logs = {str(ROOT/'logs')}
plugins = {str(RUNTIME/'grafana-plugins')}
provisioning = {str(provision)}
[server]
http_addr = 127.0.0.1
http_port = 13000
root_url = http://127.0.0.1:13000/
[security]
admin_user = lysenko
admin_password = {password}
allow_embedding = true
[auth.anonymous]
enabled = true
org_role = Viewer
[analytics]
reporting_enabled = false
check_for_updates = false
check_for_plugin_updates = false
[log]
mode = console
[plugins]
preinstall_disabled = true
[unified_alerting]
enabled = true
min_interval = 10s
[smtp]
enabled = false
''',encoding='utf-8')
    # Native histogram surface is drawn from the same exported eta() model; not a screenshot.
    import matplotlib;matplotlib.use('Agg')
    import matplotlib.pyplot as plt,numpy as np
    from exporter import eta
    figures=E/'figures';figures.mkdir(exist_ok=True);fig,axes=plt.subplots(1,3,figsize=(14,4))
    grid=[]
    for axis,(kind,(h,q)) in zip(axes,{'kaplan':(20,1200),'francis':(30,1000),'pelton':(80,350)}.items()):
        hs=np.linspace(.6*h,1.4*h,41);qs=np.linspace(.6*q,1.4*q,41);x,y=np.meshgrid(hs,qs);z=np.vectorize(lambda a,b:eta(a,b,kind))(x,y)
        contour=axis.contourf(x,y,z,levels=np.arange(87,95,.5),cmap='viridis');axis.contour(x,y,z,levels=[88,90,92,93],colors='white',linewidths=.6)
        axis.scatter([h],[q],c='red',s=30);axis.set(title=kind,xlabel='Head H, m',ylabel='Flow Q, m³/s')
        grid.extend({'turbine_type':kind,'head_m':float(a),'flow_m3s':float(b),'efficiency_percent':float(c)} for a,b,c in zip(x.flat,y.flat,z.flat))
    fig.colorbar(contour,ax=axes,label='Efficiency %',shrink=.8);fig.savefig(figures/'hill.png',dpi=160,bbox_inches='tight');plt.close(fig);write_json(E/'hill-grid.json',grid)

def rule_tests():
    # Production time durations are evaluated by the actual PromQL/rule engine in virtual time.
    rules=yaml.safe_load((ROOT/'alerts.yml').read_text(encoding='utf-8'))
    production=rules['groups'][0];test_rules=CONFIG/'production-test-rules.yml';test_rules.write_text(yaml.safe_dump({'groups':[production]},allow_unicode=True),encoding='utf-8')
    def expected(name,labels):
        r=next(r for r in production['rules'] if r['alert']==name)
        return {'exp_labels':{**labels,**r['labels']},'exp_annotations':r.get('annotations',{})}
    labels={'station':'TEST','turbine_type':'kaplan'};tests=[]
    for name,metric,value,before,at,recover in [('HydroEfficiencyLow','hydro_efficiency_percent',85,'119m45s','120m','121m'),('HydroCavitation','hydro_cavitation',1,'9m45s','10m','11m')]:
        count=480 if name=='HydroEfficiencyLow' else 40
        normal=94 if name=='HydroEfficiencyLow' else 0
        tests.append({'name':name+' boundary/recovery','interval':'15s','input_series':[{'series':metric+'{station="TEST",turbine_type="kaplan"}','values':f'{value}+0x{count} {normal}+0x8'}],
            'alert_rule_test':[{'eval_time':before,'alertname':name,'exp_alerts':[]},{'eval_time':at,'alertname':name,'exp_alerts':[expected(name,labels)]},{'eval_time':recover,'alertname':name,'exp_alerts':[]}]})
    tests.append({'name':'producer absent boundary','interval':'15s','input_series':[],
        'alert_rule_test':[{'eval_time':'45s','alertname':'ProducerDown','exp_alerts':[]},{'eval_time':'1m','alertname':'ProducerDown','exp_alerts':[expected('ProducerDown',{})]}]})
    spec={'rule_files':[str(test_rules)],'evaluation_interval':'15s','tests':tests};target=CONFIG/'rule-tests.yml';target.write_text(yaml.safe_dump(spec,allow_unicode=True),encoding='utf-8')
    output=[]
    for args in [('check','config',str(CONFIG/'prometheus.yml')),('check','rules',str(ROOT/'alerts.yml')),('test','rules',str(target))]:
        result=subprocess.run([str(PROM/'promtool.exe'),*args],capture_output=True,text=True,encoding='utf-8',errors='replace');output.append({'command':list(args),'exit_code':result.returncode,'stdout':result.stdout,'stderr':result.stderr})
    write_json(E/'promtool.json',output);assert all(r['exit_code']==0 for r in output),output
    print('PROMTOOL PRODUCTION BOUNDARIES PASS',flush=True)

def start():
    if STATE.exists():raise RuntimeError('Monitoring state exists; inspect or stop owned services')
    configure();rule_tests();(ROOT/'logs').mkdir(exist_ok=True)
    commands=[('exporter',[sys.executable,'-u',str(ROOT/'exporter.py')]),
        ('prometheus',[str(PROM/'prometheus.exe'),'--config.file='+str(CONFIG/'prometheus.yml'),'--storage.tsdb.path='+str(ROOT/'data/prometheus'),'--storage.tsdb.retention.time=1d','--web.listen-address=127.0.0.1:19090','--web.enable-lifecycle']),
        ('grafana',[str(GRAFANA/'bin/grafana.exe'),'server','--homepath',str(GRAFANA),'--config',str(RUNTIME/'grafana.ini')])]
    states=[]
    for name,command in commands:
        with (ROOT/'logs'/f'{name}.log').open('a',encoding='utf-8') as log:
            p=subprocess.Popen(command,cwd=ROOT,stdout=log,stderr=subprocess.STDOUT,creationflags=subprocess.CREATE_NO_WINDOW)
        states.append({'name':name,'pid':p.pid,'created':psutil.Process(p.pid).create_time()});write_json(STATE,states)
    for url in ('http://127.0.0.1:18400/health','http://127.0.0.1:19090/-/ready','http://127.0.0.1:13000/api/health'):
        end=time.monotonic()+120
        while True:
            try:http(url);break
            except Exception:
                if time.monotonic()>end:raise
                time.sleep(1)
    print('MONITORING UP 127.0.0.1:13000, 19090, 18400',flush=True)

def collect():
    expressions=['up','energy_producer_alive','sum(energy_messages_sent_total)','sum(energy_messages_stored_total)','energy_pipeline_errors_total',
        'sum(rate(energy_messages_stored_total[1m]))','energy_consumer_lag','hydro_efficiency_percent','hydro_power_mw','hydro_generator_running',
        'cassandra_client_requests_total','cassandra_storage_bytes','cassandra_heap_used_bytes','kafka_broker_messages_in_total',
        'histogram_quantile(0.95,sum by(le)(rate(energy_send_ack_seconds_bucket[1m])))',
        'avg by(turbine_type)(hydro_efficiency_percent)','sum(hydro_power_mw * (94 / hydro_efficiency_percent - 1))','hydro_stream_windows_verified','hydro_saga_scenarios_verified']
    values={expr:query(expr) for expr in expressions};write_json(E/'prometheus-queries.json',{'captured_epoch':time.time(),'queries':values})
    assert all(values[x]['status']=='success' and values[x]['data']['result'] for x in expressions),[x for x in expressions if not values[x]['data']['result']]
    assert values['energy_pipeline_errors_total']['data']['result'][0]['value'][1]=='0'
    assert all(float(r['value'][1])==1 for r in values['up']['data']['result'])
    targets=http('http://127.0.0.1:19090/api/v1/targets');write_json(E/'prometheus-targets.json',targets)
    write_json(E/'grafana-health.json',http('http://127.0.0.1:13000/api/health'))
    for uid in ('hydro-lysenko','streams-lysenko'):write_json(E/(uid+'-dashboard-api.json'),http('http://127.0.0.1:13000/api/dashboards/uid/'+uid))
    write_json(E/'grafana-alert-api.json',http('http://127.0.0.1:13000/api/v1/provisioning/alert-rules',admin=True))
    write_json(E/'grafana-runtime-rules.json',http('http://127.0.0.1:13000/api/prometheus/grafana/api/v1/rules',admin=True))
    for port in (18080,18081,18082,18083,18400):
        (E/f'metrics-{port}.txt').write_text(http(f'http://127.0.0.1:{port}/metrics'),encoding='utf-8')
    end=time.time();expr='avg by(turbine_type)(hydro_efficiency_percent)';url='http://127.0.0.1:19090/api/v1/query_range?'+urllib.parse.urlencode({'query':expr,'start':end-600,'end':end,'step':5})
    write_json(E/'efficiency-history.json',http(url));print('PROMETHEUS GRAFANA EVIDENCE PASS',flush=True)

def live_test():
    samples=[];http('http://127.0.0.1:18400/fault/on','POST');begin=time.monotonic();seen=set();grafana_seen=set()
    try:
        while time.monotonic()-begin<60:
            alerts=http('http://127.0.0.1:19090/api/v1/alerts');grafana=http('http://127.0.0.1:13000/api/prometheus/grafana/api/v1/rules',admin=True)
            states={a['state'] for a in alerts['data']['alerts'] if a['labels']['alertname']=='HydroEfficiencyLiveTest'};seen|=states
            grafana_seen|={rule['state'] for group in grafana.get('data',{}).get('groups',[]) for rule in group.get('rules',[]) if rule.get('name','').startswith('Hydro efficiency:')}
            samples.append({'elapsed_seconds':time.monotonic()-begin,'epoch':time.time(),'prometheus':alerts,'grafana':grafana})
            if {'pending','firing'}<=seen and 'firing' in grafana_seen:break
            time.sleep(2)
        assert {'pending','firing'}<=seen,seen
        assert 'firing' in grafana_seen,grafana_seen
    finally:http('http://127.0.0.1:18400/fault/off','POST')
    deadline=time.monotonic()+30
    while time.monotonic()<deadline:
        alerts=http('http://127.0.0.1:19090/api/v1/alerts');samples.append({'phase':'recovery','epoch':time.time(),'prometheus':alerts})
        if not any(a['labels']['alertname']=='HydroEfficiencyLiveTest' for a in alerts['data']['alerts']):break
        time.sleep(2)
    else:raise AssertionError('Alert failed to resolve')
    write_json(E/'live-alert-states.json',{'production_for':'2h','live_profile_for':'15s','seen_states':sorted(seen),'grafana_seen_states':sorted(grafana_seen),'resolved':True,'samples':samples})
    print('LIVE PENDING FIRING RECOVERY PASS',flush=True)

def stop():
    if not STATE.exists():return
    for state in reversed(read_json(STATE)):
        try:p=psutil.Process(state['pid'])
        except psutil.NoSuchProcess:continue
        assert abs(p.create_time()-state['created'])<.01,'PID ownership changed'
        if state['name']=='prometheus':http('http://127.0.0.1:19090/-/quit','POST');p.wait(30)
        else:p.terminate();p.wait(30)
    STATE.unlink();print('OWNED MONITORING STOPPED',flush=True)

def restart_grafana():
    configure();states=read_json(STATE)
    for item in states:
        if item['name']!='grafana':continue
        try:old=psutil.Process(item['pid'])
        except psutil.NoSuchProcess:old=None
        if old:
            assert abs(old.create_time()-item['created'])<.01
            old.terminate();old.wait(30)
        with (ROOT/'logs/grafana.log').open('a',encoding='utf-8') as log:
            p=subprocess.Popen([str(GRAFANA/'bin/grafana.exe'),'server','--homepath',str(GRAFANA),'--config',str(RUNTIME/'grafana.ini')],cwd=ROOT,stdout=log,stderr=subprocess.STDOUT,creationflags=subprocess.CREATE_NO_WINDOW)
        item.update(pid=p.pid,created=psutil.Process(p.pid).create_time())
    write_json(STATE,states);print('OWNED GRAFANA RESTARTED',flush=True)

def main():
    p=argparse.ArgumentParser();p.add_argument('--phase',choices=['all','configure','start','rules','collect','live-test','stop','restart-grafana'],default='all');a=p.parse_args()
    if a.phase=='configure':configure()
    elif a.phase=='rules':rule_tests()
    elif a.phase=='stop':stop()
    elif a.phase=='start':start()
    elif a.phase=='collect':collect()
    elif a.phase=='live-test':live_test()
    elif a.phase=='restart-grafana':restart_grafana()
    else:start();time.sleep(25);live_test();collect()
if __name__=='__main__':main()
