"""Consistent Ukrainian reports, generated only from captured evidence."""
import sys
from pathlib import Path
from xml.sax.saxutils import escape
from html import unescape
from common import *
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER,TA_RIGHT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import SimpleDocTemplate,Paragraph,Spacer,PageBreak,Table,TableStyle,Image,KeepTogether
from reportlab.graphics.shapes import Drawing,Rect,String,Line,Polygon

NAVY=colors.HexColor('#18364b');TEAL=colors.HexColor('#087f8c')
for name,file in [('UA','times.ttf'),('UABold','timesbd.ttf'),('Code','consola.ttf')]:pdfmetrics.registerFont(TTFont(name,str(Path('C:/Windows/Fonts')/file)))
pdfmetrics.registerFontFamily('UA',normal='UA',bold='UABold')

def fmt(value,digits=2):return f'{value:,.{digits}f}'.replace(',',' ').replace('.',',')

class Report:
    def __init__(self,number,title,project):
        self.number=number;self.project=project;self.story=[]
        self.styles={'body':ParagraphStyle('body',fontName='UA',fontSize=11.5,leading=15.1,spaceAfter=8),
            'heading':ParagraphStyle('heading',fontName='UABold',fontSize=16,leading=20,textColor=NAVY,spaceAfter=12),
            'small':ParagraphStyle('small',fontName='UA',fontSize=9.5,leading=12,spaceAfter=6),
            'code':ParagraphStyle('code',fontName='Code',fontSize=8.3,leading=11,spaceAfter=8,backColor=colors.HexColor('#edf3f5'),borderPadding=5),
            'cell':ParagraphStyle('cell',fontName='UA',fontSize=9.4,leading=11.8),
            'header':ParagraphStyle('header',fontName='UABold',fontSize=9.4,leading=11.8,textColor=colors.white),
            'center':ParagraphStyle('center',fontName='UA',fontSize=13,leading=19,alignment=TA_CENTER,spaceAfter=7),
            'title':ParagraphStyle('title',fontName='UABold',fontSize=22,leading=28,alignment=TA_CENTER,textColor=NAVY,spaceAfter=16),
            'author':ParagraphStyle('author',fontName='UA',fontSize=12,leading=18,alignment=TA_RIGHT,spaceAfter=5)}
        self.p('НАЦІОНАЛЬНИЙ ТЕХНІЧНИЙ УНІВЕРСИТЕТ УКРАЇНИ<br/>«КИЇВСЬКИЙ ПОЛІТЕХНІЧНИЙ ІНСТИТУТ<br/>імені ІГОРЯ СІКОРСЬКОГО»','center')
        self.story.append(Spacer(1,40*mm));self.p('ЛАБОРАТОРНА РОБОТА №'+str(number),'title');self.p(title,'center')
        self.p('з дисципліни «Проєктування систем<br/>з розподіленими базами даних»','center');self.p('Варіант 3, підваріант Б','center')
        self.story.append(Spacer(1,25*mm));self.p('Виконав: студент групи ТР-61МП','author');self.p('Лисенко Лука Сергійович','author');self.p('Перевірив: ____________________','author')
        self.story.append(Spacer(1,28*mm));self.p('Київ - 2026','center')
    def p(self,text,style='body'):
        for char in ('\u2010','\u2011','\u2012','\u2013','\u2014','\u2015'):text=text.replace(char,'-')
        self.story.append(Paragraph(text,self.styles[style]))
    def page(self,title):self.story.append(PageBreak());self.p(title,'heading')
    def table(self,headers,rows,widths=None):
        content=[[Paragraph(escape(unescape(str(v))),self.styles['header']) for v in headers]]+[[Paragraph(escape(unescape(str(v))),self.styles['cell']) for v in row] for row in rows]
        t=Table(content,colWidths=[v*mm for v in widths] if widths else None,repeatRows=1,hAlign='LEFT')
        t.setStyle(TableStyle([('BACKGROUND',(0,0),(-1,0),NAVY),('ROWBACKGROUNDS',(0,1),(-1,-1),[colors.white,colors.HexColor('#eef4f5')]),('VALIGN',(0,0),(-1,-1),'TOP'),('TOPPADDING',(0,0),(-1,-1),5),('BOTTOMPADDING',(0,0),(-1,-1),5),('LINEBELOW',(0,-1),(-1,-1),.4,TEAL)]));self.story.extend([t,Spacer(1,8)])
    def figure(self,path,caption,width=170):
        from PIL import Image as PILImage
        with PILImage.open(path) as img:w,h=img.size
        self.story.append(Image(str(path),width=width*mm,height=width*mm*h/w));self.p(caption,'small')
    def flow(self,boxes):
        w=170*mm;h=40*mm;d=Drawing(w,h);n=len(boxes);box=(w-(n-1)*14)/n
        for i,lines in enumerate(boxes):
            x=i*(box+14);d.add(Rect(x,12,box,h-24,rx=5,fillColor=colors.HexColor('#edf4f6'),strokeColor=TEAL))
            for j,line in enumerate(lines):d.add(String(x+box/2,h-32-j*14,line,fontName='UA',fontSize=10,textAnchor='middle',fillColor=NAVY))
            if i<n-1:
                end=x+box+12;cy=h/2;d.add(Line(x+box,cy,end,cy,strokeColor=TEAL));d.add(Polygon([end,cy,end-4,cy+3,end-4,cy-3],fillColor=TEAL,strokeColor=TEAL))
        self.story.append(d)
    def sources(self,items):
        self.p('Джерела','heading')
        for title,url in items:self.p(f'{escape(title)}: <link href="{escape(url)}" color="#087f8c">{escape(url)}</link>','small')
        url='https://github.com/Lukadjent/kafka-energy-lysenko/tree/master/'+self.project
        self.p(f'Програмний код і журнали тестування: <link href="{url}" color="#087f8c">GitHub / {self.project}</link>.','small')
    def bucket_tree(self):
        d=Drawing(170*mm,170)
        def box(x,y,w,h,lines):
            d.add(Rect(x,y,w,h,rx=5,fillColor=colors.HexColor('#edf4f6'),strokeColor=TEAL))
            for i,line in enumerate(lines):d.add(String(x+w/2,y+h-14-i*12,line,fontName='UA',fontSize=9.5,textAnchor='middle',fillColor=NAVY))
        box(135,120,210,45,['Денний обсяг r × s × 86 400','наближається до 100 MiB?'])
        box(5,60,180,35,['Так: hourly або менший bucket'])
        box(295,60,180,35,['Ні: daily для добових запитів'])
        box(135,0,210,38,['Для аналітики за 7+ діб','додати weekly rollup'])
        for x1,y1,x2,y2 in [(240,120,95,95),(240,120,385,95),(95,60,240,38),(385,60,240,38)]:d.add(Line(x1,y1,x2,y2,strokeColor=TEAL))
        d.add(String(152,111,'Так',fontName='UA',fontSize=9,fillColor=NAVY));d.add(String(319,111,'Ні',fontName='UA',fontSize=9,fillColor=NAVY))
        self.story.append(d);self.p('r - подій/с одного ключа; s - оцінка байтів рядка без компресії. Після вибору перевірити фактичні SSTables і fan-out за типовими запитами.','small')
    def finish(self):
        target=WORKSPACE/'output/pdf'/f'Lab{self.number}_Lysenko_3B.pdf';target.parent.mkdir(parents=True,exist_ok=True)
        def footer(canvas,doc):
            if doc.page>1:
                canvas.setFont('UA',9);canvas.setFillColor(NAVY);canvas.drawString(20*mm,12*mm,f'Лисенко Л. С. | ТР-61МП | ЛР{self.number} | 3Б');canvas.drawRightString(190*mm,12*mm,str(doc.page))
        SimpleDocTemplate(str(target),pagesize=A4,leftMargin=20*mm,rightMargin=20*mm,topMargin=18*mm,bottomMargin=20*mm,title=f'ЛР{self.number} - Лисенко - 3Б',author=STUDENT['name']).build(self.story,onFirstPage=footer,onLaterPages=footer)
        import shutil
        report=WORKSPACE/self.project/'report';report.mkdir(exist_ok=True);shutil.copy2(target,report/target.name);print(target)
