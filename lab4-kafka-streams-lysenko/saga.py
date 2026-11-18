"""Durable orchestrator, with atomic participant state+deduplication in one LWT row."""
import argparse,json,os,sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'lab-support'))
from common import *
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
KS='energy_l4_lysenko'

def query(s,cql,args=(),lwt=False):
    statement=SimpleStatement(cql,consistency_level=ConsistencyLevel.LOCAL_QUORUM,
        serial_consistency_level=ConsistencyLevel.LOCAL_SERIAL if lwt else None)
    return s.execute(statement,args)

def effect(s,saga,action,gate,prepared,role='upper'):
    station=saga+'-'+role;command=saga+':'+action
    for _ in range(20):
        row=query(s,f'SELECT * FROM {KS}.participant WHERE station_id=%s',(station,)).one()
        commands=dict(row.commands or {})
        if command in commands:return {'deduplicated':True,'version':row.version}
        commands[command]=json.dumps({'gate':gate,'prepared':prepared})
        result=query(s,f'UPDATE {KS}.participant SET gate=%s,prepared=%s,version=%s,commands=%s WHERE station_id=%s IF version=%s',
            (gate,prepared,row.version+1,commands,station,row.version),True).one()
        if result.applied:return {'deduplicated':False,'version':row.version+1}
    raise RuntimeError('LWT contention')

def run(s,saga,fail=False,marker=None):
    query(s,f'INSERT INTO {KS}.saga_state (saga_id,next_step,status,fail_lower) VALUES (%s,0,%s,%s) IF NOT EXISTS',(saga,'RUNNING',fail),True)
    query(s,f'INSERT INTO {KS}.participant (station_id,gate,prepared,version,commands) VALUES (%s,20,false,0,%s) IF NOT EXISTS',(saga+'-upper',{}),True)
    query(s,f'INSERT INTO {KS}.participant (station_id,gate,prepared,version,commands) VALUES (%s,20,false,0,%s) IF NOT EXISTS',(saga+'-lower',{}),True)
    row=query(s,f'SELECT * FROM {KS}.saga_state WHERE saga_id=%s',(saga,)).one()
    attempts=[]
    if row.status=='RUNNING':
        for step,action in enumerate(['PREPARE_UPPER','OPEN_GATES','ACK_LOWER']):
            if step<row.next_step:continue
            if step==2 and fail:
                query(s,f'INSERT INTO {KS}.saga_log (saga_id,sequence,action,status) VALUES (%s,30,%s,%s)',(saga,action,'FAILED'))
                for seq,name,gate,prepared in [(40,'COMPENSATE_OPEN',20,True),(50,'COMPENSATE_PREPARE',20,False)]:
                    attempts.append({'action':name,**effect(s,saga,name,gate,prepared)})
                    query(s,f'INSERT INTO {KS}.saga_log (saga_id,sequence,action,status) VALUES (%s,%s,%s,%s)',(saga,seq,name,'DONE'))
                query(s,f'UPDATE {KS}.saga_state SET status=%s WHERE saga_id=%s',('COMPENSATED',saga));break
            if step<2:
                result=effect(s,saga,action,20 if step==0 else 60,True);attempts.append({'action':action,**result})
                if step==1 and marker and not marker.exists():
                    write_json(marker,{'after_effect_before_checkpoint':True,'effect':result});os._exit(19)
            else:
                attempts.append({'action':action,**effect(s,saga,action,20,True,'lower')})
            query(s,f'INSERT INTO {KS}.saga_log (saga_id,sequence,action,status) VALUES (%s,%s,%s,%s)',(saga,(step+1)*10,action,'DONE'))
            query(s,f'UPDATE {KS}.saga_state SET next_step=%s WHERE saga_id=%s',(step+1,saga))
        else:query(s,f'UPDATE {KS}.saga_state SET status=%s WHERE saga_id=%s',('COMPLETED',saga))
    state=query(s,f'SELECT * FROM {KS}.saga_state WHERE saga_id=%s',(saga,)).one()._asdict()
    participant=query(s,f'SELECT * FROM {KS}.participant WHERE station_id=%s',(saga+'-upper',)).one()._asdict()
    lower=query(s,f'SELECT * FROM {KS}.participant WHERE station_id=%s',(saga+'-lower',)).one()._asdict()
    participant['commands']=dict(participant['commands'] or {});lower['commands']=dict(lower['commands'] or {})
    log=[r._asdict() for r in query(s,f'SELECT * FROM {KS}.saga_log WHERE saga_id=%s',(saga,))]
    return {'state':state,'participant':participant,'lower_participant':lower,'log':log,'attempts':attempts}

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('saga');p.add_argument('--fail',action='store_true');p.add_argument('--marker',type=Path);p.add_argument('--result',type=Path,required=True);a=p.parse_args()
    with Cluster(CONTACT_POINTS,protocol_version=4) as cluster:write_json(a.result,run(cluster.connect(),a.saga,a.fail,a.marker))
