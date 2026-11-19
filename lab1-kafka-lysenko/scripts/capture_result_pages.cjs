/** Browser screenshots of a saved-results page; the page is not a terminal emulator. */
const fs=require('node:fs/promises');
const path=require('node:path');
const crypto=require('node:crypto');
const {createRequire}=require('node:module');
const root=path.resolve(__dirname,'..');
const bundled=process.env.LAB_BROWSER_NODE_MODULES;
const pw=bundled?createRequire(path.join(bundled,'_entry.cjs'))('playwright'):require('playwright');
const escape=s=>s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
(async()=>{
  const output=path.join(root,'evidence','screenshots');await fs.mkdir(output,{recursive:true});
  const browser=await pw.chromium.launch({headless:true,channel:'chrome'});
  const page=await browser.newPage({viewport:{width:1100,height:700},deviceScaleFactor:1});
  const entries=[];
  try{
    for(const name of ['01_batch_linger','02_compression','03_partitions','04_cli_and_analytics']){
      const source=path.join(root,'evidence','screenshot-views',name+'.txt');
      const text=(await fs.readFile(source,'utf8')).replace(/^\uFEFF/,'');
      const html=`<!doctype html><html lang="uk"><meta charset="utf-8"><title>Kafka - результати дослідження</title><style>html{background:#edf2f5}body{margin:0;padding:25px;font-family:Segoe UI,sans-serif;color:#172f40}header{font-size:23px;font-weight:600;margin-bottom:18px}article{background:white;padding:22px;border:1px solid #a9bac7;border-radius:8px}pre{font:18px/1.5 Consolas,monospace;margin:0;white-space:pre-wrap;overflow-wrap:anywhere}footer{font-size:14px;color:#425d70;margin-top:15px}</style><header>Apache Kafka · Результати експерименту</header><article><pre>${escape(text)}</pre></article><footer>Збережений вивід та зведення вимірювань · ЛР1 · Варіант 3Б</footer></html>`;
      await page.setContent(html,{waitUntil:'load'});await page.screenshot({path:path.join(output,name+'.png'),fullPage:true});
      entries.push({screenshot:name+'.png',view:path.relative(root,source).split(path.sep).join('/'),source_sha256:crypto.createHash('sha256').update(await fs.readFile(source)).digest('hex'),screenshot_sha256:crypto.createHash('sha256').update(await fs.readFile(path.join(output,name+'.png'))).digest('hex'),capture:'Chromium screenshot of a saved-results HTML view; no benchmark rerun'});
    }
  }finally{await browser.close();}
  await fs.writeFile(path.join(output,'capture.json'),JSON.stringify(entries,null,2)+'\n');
  console.log('CAPTURE PASS: '+entries.length+' browser screenshots');
})().catch(e=>{console.error(e);process.exitCode=1;});
