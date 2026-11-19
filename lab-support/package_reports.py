"""Refresh individual source packages and create a reports-only submission ZIP."""
import hashlib,zipfile
from common import *

def digest(data):return hashlib.sha256(data).hexdigest()

def archive(path,files):
    with zipfile.ZipFile(path,'w',zipfile.ZIP_DEFLATED,compresslevel=9) as z:
        for name,data in sorted(files.items()):z.writestr(name,data)
    with zipfile.ZipFile(path) as z:
        assert z.testzip() is None
        assert all(z.read(name)==data for name,data in files.items())

def main():
    output=WORKSPACE/'output';output.mkdir(exist_ok=True)
    for n,folder in [(1,'lab1-kafka-lysenko'),(2,'lab2-cassandra-lysenko')]:
        files={}
        for p in (WORKSPACE/folder).rglob('*'):
            rel=p.relative_to(WORKSPACE)
            if not p.is_file() or any(x in {'.runtime','data','logs','__pycache__'} for x in rel.parts):continue
            if rel.name=='manifest.sha256':continue
            files[rel.as_posix()]=p.read_bytes()
        if n==1:
            for name in ['README.md','.gitignore','.gitattributes']:files[name]=(WORKSPACE/name).read_bytes()
        manifest='\n'.join(digest(data)+'  '+name for name,data in sorted(files.items()))+'\n'
        manifest_path=WORKSPACE/folder/'evidence/manifest.sha256';manifest_path.write_text(manifest,encoding='utf-8')
        files[manifest_path.relative_to(WORKSPACE).as_posix()]=manifest_path.read_bytes()
        archive(output/f'Lab{n}_Lysenko_3B.zip',files)
    files={}
    for n in range(1,6):
        name=f'Lab{n}_Lysenko_3B.pdf';data=(output/'pdf'/name).read_bytes();files[name]=data
    target=output/'Reports_Lysenko_3B.zip';archive(target,files)
    print('REPORTS PACKAGE PASS: exactly 5 PDFs; Lab1/Lab2 source packages refreshed')
if __name__=='__main__':main()
