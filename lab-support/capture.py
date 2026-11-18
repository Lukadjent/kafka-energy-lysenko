"""Capture a real laboratory process stdout, while keeping progress concise."""
import argparse
import subprocess
import sys
import time
from common import *

PROJECTS={3:'lab3-cassandra-optimization-lysenko',4:'lab4-kafka-streams-lysenko',5:'lab5-monitoring-lysenko'}

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('lab',type=int,choices=PROJECTS)
    parser.add_argument('phase')
    parser.add_argument('--reset',action='store_true')
    args=parser.parse_args()
    project=WORKSPACE/PROJECTS[args.lab]
    logs=project/'evidence';logs.mkdir(exist_ok=True)
    command=[sys.executable,'-u',str(project/'run_lab.py'),'--phase',args.phase]
    if args.reset:
        assert args.lab==3,'Reset is scoped to Lab3'
        command+=['--reset']
    target=logs/f'console-{args.phase}.txt'
    if target.exists():target.rename(logs/f'console-{args.phase}-previous-{time.time_ns()}.txt')
    with target.open('w',encoding='utf-8') as output:
        output.write('COMMAND '+json.dumps(command)+'\n');output.flush()
        process=subprocess.Popen(command,cwd=project,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,
            encoding='utf-8',errors='replace',creationflags=subprocess.CREATE_NO_WINDOW)
        for line in process.stdout:
            output.write(line);output.flush()
            if '_RESULT ' in line:
                print(line.split(' ',1)[0]+' captured',flush=True)
            else:print(line,end='',flush=True)
        raise SystemExit(process.wait())

if __name__=='__main__':main()
