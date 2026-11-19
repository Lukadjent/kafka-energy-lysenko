"""Render every final page and record PDF text/bounds checks; contact sheets for visual QA."""
import argparse,hashlib,shutil,subprocess
from common import *
from pypdf import PdfReader
from PIL import Image,ImageDraw
import pdfplumber

def main(labs=(3,4,5)):
    checks=[];render=WORKSPACE/'tmp/pdfs/labs3-5';render.mkdir(parents=True,exist_ok=True)
    poppler=shutil.which('pdftoppm') or r'C:/Users/Luka/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/Library/bin/pdftoppm.exe'
    for n in labs:
        pdf=WORKSPACE/'output/pdf'/f'Lab{n}_Lysenko_3B.pdf';reader=PdfReader(pdf);texts=[p.extract_text() for p in reader.pages]
        assert 'Лисенко Лука Сергійович' in texts[0] and 'ТР-61МП' in texts[0]
        image_counts=[len(page.images) for page in reader.pages]
        assert all(len(t)>180 or (len(t)>100 and image_counts[i]>0) for i,t in enumerate(texts)),(n,[(i+1,len(t)) for i,t in enumerate(texts)])
        bad=[]
        with pdfplumber.open(pdf) as document:
            for i,page in enumerate(document.pages,1):
                for ch in page.chars:
                    if ch['text'].strip() and (ch['x0']<45 or ch['x1']>page.width-45 or ch['top']<15 or ch['bottom']>page.height-15):bad.append((i,ch['text'],ch['x0'],ch['top']))
        assert not bad,(n,bad[:10])
        for old in render.glob(f'lab{n}-*.jpg'):
            assert old.resolve().is_relative_to(render.resolve());old.unlink()
        prefix=render/f'lab{n}';subprocess.run([poppler,'-jpeg','-r','100','-scale-to','1600',str(pdf),str(prefix)],check=True,capture_output=True)
        pages=sorted(render.glob(f'lab{n}-*.jpg'));assert len(pages)==len(reader.pages)
        for start in range(0,len(pages),4):
            sheet=Image.new('RGB',(1160,1700),'#dbe3e7');draw=ImageDraw.Draw(sheet)
            for i,path in enumerate(pages[start:start+4]):
                with Image.open(path) as original:img=original.copy();img.thumbnail((558,800))
                x=12+(i%2)*580;y=24+(i//2)*846;sheet.paste(img,(x,y));draw.text((x,y-18),f'Lab {n} / page {start+i+1}',fill='black')
            sheet.save(render/f'contact-lab{n}-{start//4+1}.jpg',quality=92)
        entry={'lab':n,'pages':len(reader.pages),'bytes':pdf.stat().st_size,'sha256':hashlib.sha256(pdf.read_bytes()).hexdigest(),'bounds_passed':True,
            'page_characters':[len(t) for t in texts],'page_images':image_counts,'visual_review':'pending'}
        checks.append(entry);print(f'LAB{n} pages={entry["pages"]} chars={entry["page_characters"]}')
    write_json(SUPPORT/'pdf-qa.json',checks)
if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--labs',type=int,nargs='+',choices=[1,2,3,4,5],default=[3,4,5]);main(parser.parse_args().labs)
