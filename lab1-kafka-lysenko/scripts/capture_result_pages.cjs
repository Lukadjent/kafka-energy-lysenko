/** Browser screenshots of a saved-results page; the page is not a terminal emulator. */
const fs=require('node:fs/promises');
const path=require('node:path');
const crypto=require('node:crypto');
const {createRequire}=require('node:module');
const root=path.resolve(__dirname,'..');
const bundled=process.env.LAB_BROWSER_NODE_MODULES;
const htmlOnly=process.argv.includes('--html-only');
const pw=htmlOnly?null:bundled?createRequire(path.join(bundled,'_entry.cjs'))('playwright'):require('playwright');
const escape=s=>s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
(async()=>{
  const output=path.join(root,'evidence','screenshots');await fs.mkdir(output,{recursive:true});
  const browser=htmlOnly?null:await pw.chromium.launch({headless:true,channel:'chrome'});
  const page=browser?await browser.newPage({viewport:{width:1100,height:700},deviceScaleFactor:1}):null;
  const entries=[];
  try{
    for(const name of ['01_batch_linger','02_compression','03_partitions','04_cli_and_analytics']){
      const source=path.join(root,'evidence','screenshot-views',name+'.txt');
      const text=(await fs.readFile(source,'utf8')).replace(/^\uFEFF/,'');
      const html=`<!doctype html><html lang="uk"><meta charset="utf-8"><title>Kafka - результати дослідження</title><style>html{background:#edf2f5}body{margin:0;padding:25px;font-family:Segoe UI,sans-serif;color:#172f40}header{font-size:23px;font-weight:600;margin-bottom:18px}article{background:white;padding:22px;border:1px solid #a9bac7;border-radius:8px}pre{font:18px/1.5 Consolas,monospace;margin:0;white-space:pre-wrap;overflow-wrap:anywhere}footer{font-size:14px;color:#425d70;margin-top:15px}</style><header>Apache Kafka · Результати експерименту</header><article><pre>${escape(text)}</pre></article><footer>Збережений вивід та зведення вимірювань · ЛР1 · Варіант 3Б</footer></html>`;
      await fs.writeFile(path.join(root,'evidence','screenshot-views',name+'.html'),html,'utf8');
      if(htmlOnly)continue;
      await page.setContent(html,{waitUntil:'load'});await page.screenshot({path:path.join(output,name+'.png'),fullPage:true});
      entries.push({screenshot:name+'.png',view:path.relative(root,source).split(path.sep).join('/'),source_sha256:crypto.createHash('sha256').update(await fs.readFile(source)).digest('hex'),screenshot_sha256:crypto.createHash('sha256').update(await fs.readFile(path.join(output,name+'.png'))).digest('hex'),capture:'Chromium screenshot of a saved-results HTML view; no benchmark rerun'});
    }
  }finally{if(browser)await browser.close();}
  const cards=[['01_batch_linger','Batch / linger','Пропускна здатність, ack latency та end-to-end p95.'],['02_compression','Кодеки','None, gzip, snappy, lz4 і zstd; частка стиснення.'],['03_partitions','Партиціонування','Швидкість і розподіл повідомлень за партиціями.'],['04_cli_and_analytics','Perf-test і аналітика','Вивід Kafka, серіалізація та часові вікна.']];
  const index=`<!doctype html><html lang="uk"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>ЛР1 · Kafka · Результати</title><style>*{box-sizing:border-box}body{margin:0;background:#edf2f5;color:#172f40;font:17px/1.55 Segoe UI,sans-serif}main{max-width:1000px;margin:48px auto;padding:0 24px}h1{font-size:34px;line-height:1.2;margin-bottom:10px}.note{color:#425d70}section{display:grid;grid-template-columns:repeat(auto-fit,minmax(270px,1fr));gap:18px;margin:30px 0}a.card{display:block;background:white;border:1px solid #a9bac7;border-radius:10px;padding:24px;text-decoration:none;color:inherit}a.card:hover{border-color:#087f8c;box-shadow:0 4px 16px #18364b15}h2{font-size:22px;margin:0 0 8px}p{margin:0 0 14px}a{color:#08647b}footer{border-top:1px solid #a9bac7;padding-top:20px;font-size:15px}</style><main><p class="note">Лисенко Лука Сергійович · ТР-61МП · Варіант 3Б</p><h1>Apache Kafka: результати експерименту</h1><p class="note">Збережені результати, наведені в додатку Б звіту. Виберіть розділ, щоб побачити ті самі таблиці, що на скриншотах.</p><section>${cards.map(([name,title,desc])=>`<a class="card" href="evidence/screenshot-views/${name}.html"><h2>${title}</h2><p>${desc}</p><span>Переглянути →</span></a>`).join('')}</section><footer><p>Порівняльні таблиці містять медіани трьох повторів. Результати нового запуску можуть відрізнятися.</p><a href="evidence/results-flat.csv">CSV з усіма вимірюваннями</a> · <a href="evidence/results.json">Початкові JSON-результати</a> · <a href="report/Lab1_Lysenko_3B.pdf">Звіт PDF</a></footer></main></html>`;
  await fs.writeFile(path.join(root,'results.html'),index,'utf8');
  if(!htmlOnly)await fs.writeFile(path.join(output,'capture.json'),JSON.stringify(entries,null,2)+'\n');
  console.log(htmlOnly?'HTML VIEWER READY: results.html':'CAPTURE PASS: '+entries.length+' browser screenshots; HTML viewer saved');
})().catch(e=>{console.error(e);process.exitCode=1;});
