"""Own three Cassandra JVMs and reuse the previously verified Kafka launcher."""
import argparse
import importlib.util
import json
import os
import re
import secrets
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path

from common import *
import psutil
import yaml

STATE = RUNTIME/'cassandra-services.json'
CLASSES = RUNTIME/'classes'
JMX = RUNTIME/'jmx_prometheus_javaagent-1.7.0.jar'

def connected(host, port):
    try:
        with socket.create_connection((host,port),timeout=.3):
            return True
    except OSError:
        return False

def wait(host, port, process=None, seconds=120):
    deadline = time.monotonic()+seconds
    while time.monotonic()<deadline:
        if connected(host,port):
            return
        if process and process.poll() is not None:
            raise RuntimeError(f'Service exited {process.returncode}; inspect lab-support/logs')
        time.sleep(.5)
    raise TimeoutError(f'Not ready: {host}:{port}')

def configure_node(index):
    node = RUNTIME/f'node{index+1}'
    conf = node/'conf'
    if not conf.exists():
        shutil.copytree(CASSANDRA_DIST/'conf',conf)
    data = SUPPORT/'data'/f'node{index+1}'
    data.mkdir(parents=True,exist_ok=True)
    (data/'jvm-tmp').mkdir(exist_ok=True)
    config = yaml.safe_load((CASSANDRA_DIST/'conf/cassandra.yaml').read_text(encoding='utf-8'))
    config.pop('allocate_tokens_for_local_replication_factor',None)
    config.update(cluster_name='Lysenko 3B three-node lab', num_tokens=16,
        initial_token=','.join(str(-(2**63)+(2**64)*(index+3*i)//48) for i in range(16)),
        listen_address=CONTACT_POINTS[index],rpc_address=CONTACT_POINTS[index],
        broadcast_address=CONTACT_POINTS[index],broadcast_rpc_address=CONTACT_POINTS[index],
        endpoint_snitch='GossipingPropertyFileSnitch',enable_materialized_views=True,
        concurrent_reads=8,concurrent_writes=8,concurrent_counter_writes=4,
        concurrent_materialized_view_writes=8,concurrent_compactors=1,
        memtable_heap_space_in_mb=192,memtable_offheap_space_in_mb=64,
        data_file_directories=[(data/'sstables').as_posix()],
        commitlog_directory=(data/'commitlog').as_posix(),hints_directory=(data/'hints').as_posix(),
        saved_caches_directory=(data/'saved_caches').as_posix(),
        cdc_raw_directory=(data/'cdc_raw').as_posix(),
        auto_snapshot=False)
    config['seed_provider'][0]['parameters'][0]['seeds']=CONTACT_POINTS[0]+':7000'
    (conf/'cassandra.yaml').write_text(yaml.safe_dump(config,sort_keys=False,allow_unicode=True),encoding='utf-8')
    (conf/'cassandra-rackdc.properties').write_text(f'dc=dc1\nrack=rack{index+1}\n',encoding='ascii')
    return conf,data

def flags(role):
    text = (CASSANDRA_DIST/'conf'/f'jvm11-{role}.options').read_text(encoding='utf-8')
    return [flag for line in text.splitlines() if line.startswith(('--add-','-Dio.netty.')) for flag in line.split()]

def nodetool(index, *args):
    conf=RUNTIME/f'node{index+1}/conf'
    command=[str(JAVA11),*flags('clients'),'-cp',os.pathsep.join([str(conf),str(CASSANDRA_DIST/'lib/*')]),
        'org.apache.cassandra.tools.NodeTool','-h','127.0.0.1','-p',str(17191+index),*args]
    return subprocess.run(command,cwd=CASSANDRA_DIST,check=True,capture_output=True,text=True,encoding='utf-8',errors='replace').stdout

def start_cassandra():
    if STATE.exists() or any(connected(host,9042) for host in CONTACT_POINTS):
        raise RuntimeError('Cassandra lab state/ports already exist; inspect status or stop owned nodes first')
    CLASSES.mkdir(parents=True,exist_ok=True)
    subprocess.run([str(JDK17/'bin/javac.exe'),'--release','11','-d',str(CLASSES),
        str(WORKSPACE/'lab1-kafka-lysenko/tools/LabService.java')],check=True)
    logs=SUPPORT/'logs'; logs.mkdir(exist_ok=True)
    states=[]
    try:
        for index,host in enumerate(CONTACT_POINTS):
            conf,data=configure_node(index)
            token=secrets.token_hex(24)
            control=18191+index
            args=[str(JAVA11),'-Xms768m','-Xmx1024m','-XX:ActiveProcessorCount=3',
                '-Dfile.encoding=UTF-8','-Duser.language=en','-Duser.country=US',
                '-XX:+ExitOnOutOfMemoryError','-Djava.net.preferIPv4Stack=true',
                f'-Dcassandra.config={(conf/"cassandra.yaml").as_uri()}',
                f'-Dcassandra.storagedir=../../../lab-support/data/node{index+1}',
                f'-Djava.io.tmpdir=../../../lab-support/data/node{index+1}/jvm-tmp',
                f'-Dcassandra.logdir=../../../lab-support/logs/node{index+1}',
                f'-Dcassandra.jmx.local.port={17191+index}','-Dcassandra-foreground=yes',
                '-javaagent:lib/jamm-0.3.2.jar',
                f'-javaagent:../../../lab-support/.runtime/{JMX.name}=127.0.0.1:{18081+index}:../../../lab-support/jmx-cassandra.yml',
                *flags('server'),'-cp',os.pathsep.join([str(CLASSES),str(conf),str(CASSANDRA_DIST/'lib/*')]),
                'LabService','org.apache.cassandra.service.CassandraDaemon',str(control),token]
            with (logs/f'node{index+1}.console.log').open('a',encoding='utf-8') as output:
                process=subprocess.Popen(args,cwd=CASSANDRA_DIST,stdout=output,stderr=subprocess.STDOUT,
                    creationflags=subprocess.CREATE_NO_WINDOW)
            states.append({'index':index,'host':host,'pid':process.pid,'created':psutil.Process(process.pid).create_time(),
                'control':control,'token':token})
            write_json(STATE,states)
            wait(host,9042,process)
            print(f'CASSANDRA NODE UP {host}:9042 PID={process.pid}',flush=True)
        from cassandra.cluster import Cluster
        with Cluster(CONTACT_POINTS,protocol_version=4) as cluster:
            session=cluster.connect()
            deadline=time.monotonic()+60
            while time.monotonic()<deadline:
                nodes=list(session.execute('SELECT peer FROM system.peers'))
                if len(nodes)==2 and len(cluster.metadata.all_hosts())==3:
                    break
                time.sleep(1)
            assert len(nodes)==2,nodes
            assert len(cluster.metadata.all_hosts())==3
        print(nodetool(0,'status'),flush=True)
    except Exception:
        stop_cassandra(drain=False)
        raise

def stop_cassandra(drain=True):
    if not STATE.exists():
        return
    for state in reversed(read_json(STATE)):
        try:
            process=psutil.Process(state['pid'])
            if abs(process.create_time()-state['created'])>.01:
                raise RuntimeError('PID ownership changed; refusing to stop it')
        except psutil.NoSuchProcess:
            continue
        if drain and connected(state['host'],9042):
            print(f'DRAIN NODE {state["index"]+1}',flush=True)
            nodetool(state['index'],'drain')
        if connected('127.0.0.1',state['control']):
            with socket.create_connection(('127.0.0.1',state['control']),timeout=5) as control:
                control.sendall((state['token']+'\n').encode())
                assert control.recv(128).strip()==b'stopping'
            process.wait(timeout=120)
        else:
            raise RuntimeError('Owned control endpoint missing; inspect logs')
        print(f'STOPPED Cassandra node {state["index"]+1}',flush=True)
    STATE.unlink()

def kafka(action):
    spec=importlib.util.spec_from_file_location('lysenko_kafka_launcher',WORKSPACE/'lab1-kafka-lysenko/lab.py')
    module=importlib.util.module_from_spec(spec)
    # lab.py imports its own common.py; bind it without replacing this module.
    previous=sys.modules.get('common')
    source_common=importlib.util.spec_from_file_location('common',WORKSPACE/'lab1-kafka-lysenko/scripts/common.py')
    lab_common=importlib.util.module_from_spec(source_common); source_common.loader.exec_module(lab_common)
    sys.modules['common']=lab_common
    try:
        spec.loader.exec_module(module)
    finally:
        sys.modules['common']=previous
    original=module.java_command
    def command(cls,args,service='tools'):
        result=original(cls,args,service)
        if service=='kafka':
            result.insert(1,f'-javaagent:../lab-support/.runtime/{JMX.name}=127.0.0.1:18080:../lab-support/jmx-kafka.yml')
        return result
    module.java_command=command
    getattr(module,action)()

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action',choices=['start-cassandra','stop-cassandra','start-kafka','stop-kafka','status'])
    options=parser.parse_args()
    if options.action=='start-cassandra': start_cassandra()
    elif options.action=='stop-cassandra': stop_cassandra()
    elif options.action=='start-kafka': kafka('start')
    elif options.action=='stop-kafka': kafka('stop')
    else:
        print(json.dumps({'cassandra':{h:connected(h,9042) for h in CONTACT_POINTS},'kafka':connected('127.0.0.1',9092)}))

if __name__=='__main__': main()
