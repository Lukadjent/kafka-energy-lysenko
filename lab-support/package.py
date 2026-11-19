"""Package own sources/evidence plus the minimal portable bootstrap, with byte hashes."""
import hashlib,zipfile
from common import *

def main():
    selected={}
    for directory in ['lab-support','lab3-cassandra-optimization-lysenko','lab4-kafka-streams-lysenko','lab5-monitoring-lysenko']:
        for file in (WORKSPACE/directory).rglob('*'):
            rel=file.relative_to(WORKSPACE)
            if not file.is_file() or any(part in {'.runtime','data','logs','__pycache__'} for part in rel.parts):continue
            if file.name in {'release.json','report-release.json','pdf-qa.json','source-check.json','distributions.json','validation.json'} and rel.parts[0]=='lab-support':continue
            if '-previous-' in file.name:continue
            selected[rel.as_posix()]=file.read_bytes()
    prerequisites=['kafka-energy-lysenko/scripts/download_dependencies.py','lab2-cassandra-lysenko/setup.ps1','lab2-cassandra-lysenko/requirements.txt',
        'lab1-kafka-lysenko/lab.py','lab1-kafka-lysenko/scripts/common.py','lab1-kafka-lysenko/tools/LabService.java',
        'lab1-kafka-lysenko/config/student.json','lab1-kafka-lysenko/config/server.properties','lab1-kafka-lysenko/config/zookeeper.properties','lab1-kafka-lysenko/config/log4j.properties']
    for name in prerequisites:selected[name]=(WORKSPACE/name).read_bytes()
    selected['README.md']=(SUPPORT/'ARCHIVE_README.md').read_bytes()
    manifest='\n'.join(hashlib.sha256(data).hexdigest()+'  '+name for name,data in sorted(selected.items()))+'\n'
    selected['MANIFEST.sha256']=manifest.encode('utf-8')
    output=WORKSPACE/'output/Labs3-5_Lysenko_3B.zip';output.parent.mkdir(exist_ok=True)
    with zipfile.ZipFile(output,'w',zipfile.ZIP_DEFLATED,compresslevel=9) as archive:
        for name,data in sorted(selected.items()):archive.writestr(name,data)
    with zipfile.ZipFile(output) as archive:
        assert archive.testzip() is None
        assert all(archive.read(name)==data for name,data in selected.items())
        assert not any('course-materials/' in n or '/.runtime/' in n or '/logs/' in n or '/data/' in n for n in archive.namelist())
    result={'archive':output.name,'files':len(selected),'bytes':output.stat().st_size,'sha256':hashlib.sha256(output.read_bytes()).hexdigest(),'all_member_hashes_verified':True,
        'excluded':['portable binaries','raw generated CSV/SSTables','runtime credentials','teacher materials','video']}
    print(result)
if __name__=='__main__':main()
