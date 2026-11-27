'use strict';

const CACHE = 'psn-gameplay-files-v1';
const META_CACHE = 'psn-gameplay-files-meta-v1';
const PARTS_CACHE = 'psn-gameplay-ranges-v1';
const MIB = 1024 * 1024, GIB = 1024 * MIB;
const DEFAULT_BYTES = GIB, HIGH_BYTES = 2 * GIB;
const MAX_FILE = 32 * MIB, MAX_PART = 4 * MIB, MAX_PARTS_BYTES = 16 * MIB, MAX_PARTS = 24;
const LOCAL_PREFIX = new URL('./__psn_gameplay_cache__/',self.registration.scope).href;
const INDEX_KEY = new URL('./__psn_gameplay_cache_index__', self.registration.scope).href;
let indexPromise, indexQueue = Promise.resolve();
const ENDPOINTS_URL = 'https://raw.githubusercontent.com/PS3-Pro/PSN-Content/master/resources/server/endpoints.json';
let videoServerOrigins = new Set(), videoServerConfigRequest = null, videoServerConfigCheckedAt = 0;
function parsePsnServerEndpoints(data) {
  if (!Array.isArray(data?.servers) || !data.servers.length || data.servers.length > 16) throw new Error('Invalid server list');
  return [...new Set(data.servers.map(value => {
    if (typeof value !== 'string') throw new Error('Invalid server URL');
    const url = new URL(value);
    if (url.protocol !== 'https:' || !/^[a-z0-9-]+\.onrender\.com$/.test(url.hostname) || url.username || url.password || url.port || url.pathname !== '/' || url.search || url.hash) throw new Error('Invalid server origin');
    return url.origin;
  }))];
}
function isVideoServerCandidate(request, message=false) {
  if (request.method !== 'GET' || (!message && request.destination !== 'video') || request.mode !== 'cors') return false;
  try {
    const url = new URL(request.url);
    return url.protocol === 'https:' && /^[a-z0-9-]+\.onrender\.com$/.test(url.hostname) && !url.port && !url.username && !url.password && url.pathname === '/api/gameplay-video' && url.searchParams.has('token');
  } catch (_) { return false; }
}
function ensureVideoServerConfig() {
  if (videoServerConfigRequest) return videoServerConfigRequest;
  if (Date.now() - videoServerConfigCheckedAt < 30000) return Promise.resolve();
  videoServerConfigCheckedAt = Date.now();
  videoServerConfigRequest = (async () => {
    try {
      const response = await fetch(ENDPOINTS_URL,{cache:'no-cache',redirect:'error',signal:AbortSignal.timeout(5000)});
      if (!response.ok) throw new Error('HTTP '+response.status);
      const text = await response.text();
      if (text.length > 16384) throw new Error('Invalid configuration size');
      videoServerOrigins = new Set(parsePsnServerEndpoints(JSON.parse(text)));
    } catch (_) { videoServerOrigins.clear(); }
    finally { videoServerConfigRequest = null; }
  })();
  return videoServerConfigRequest;
}
let capturing = null, completion = null;
const confirmed = new Map(), completionAttempts = new Map();
const foregroundClients = new Set(), deferred = new Map();
let completionWake=null;
const IDLE_DELAY=900, MAX_DEFERRED=4;
function enqueueCompletion(url,id,retries=0){
  if(retries>2)return;
  const old=deferred.get(id);
  deferred.delete(id);deferred.set(id,{url,id,retries:Math.max(retries,old?.retries||0),until:Date.now()+300000});
  while(deferred.size>MAX_DEFERRED)deferred.delete(deferred.keys().next().value);
}
function interruptBackground(){
  if(completion&&!completion.interrupted&&!completion.networkDone){
    completion.interrupted=true;completion.controller.abort();
    record('background-paused',completion.key);
  }
  if(capturing?.background)capturing.interrupt();
}
function scheduleCompletion(){
  if(completionWake)return completionWake;
  if(!deferred.size||completion)return Promise.resolve();
  completionWake=(async()=>{
    await new Promise(resolve=>setTimeout(resolve,IDLE_DELAY));
    // A closed tab must not hold the priority lock forever.
    for(const id of [...foregroundClients]){
      try{if(!await self.clients.get(id))foregroundClients.delete(id);}catch(_){}
    }
    if(foregroundClients.size||completion||capturing)return;
    for(const [id,job] of deferred)if(job.until<=Date.now())deferred.delete(id);
    const job=[...deferred.values()].pop();if(!job)return;
    deferred.delete(job.id);
    return finishShownVideo(job.url,job.id,job.retries);
  })().finally(()=>{
    completionWake=null;
    if(!foregroundClients.size&&!capturing&&!completion&&deferred.size)return scheduleCompletion();
  });
  return completionWake;
}
const recentCacheEvents = [];
function record(kind,key,bytes=0,details={}){
  recentCacheEvents.push({kind,key:key?.split('/').pop(),bytes,...details});
  if(recentCacheEvents.length>40)recentCacheEvents.shift();
}
function confirmationKey(clientId,url){return clientId+'|'+url;}
function wasConfirmed(id){return (confirmed.get(id)||0)>Date.now();}

self.addEventListener('install',event=>event.waitUntil(self.skipWaiting()));
self.addEventListener('activate', event => event.waitUntil(self.clients.claim()));
self.addEventListener('message',event=>{
  if(event.data?.type==='psn-gameplay-cache-version')event.ports[0]?.postMessage({version:5});
  if(event.data?.type==='psn-gameplay-cache-priority'&&event.source?.id){
    if(event.data.busy===true){foregroundClients.add(event.source.id);interruptBackground();}
    else {foregroundClients.delete(event.source.id);event.waitUntil(scheduleCompletion());}
  }
  if(event.data?.type==='psn-gameplay-cache-diagnostics')event.ports[0]?.postMessage({events:recentCacheEvents.slice(),capturing:!!capturing,completing:!!completion,foreground:foregroundClients.size,deferred:deferred.size});
  if(event.data?.type==='psn-gameplay-cache-confirm'||event.data?.type==='psn-gameplay-cache-finish'){
    const url=event.data.url;
    if(typeof url!=='string'||!event.source?.id)return;
    const sourceId=event.source.id, type=event.data.type;
    let request;
    try{request=new Request(url,{mode:'cors'});if(!isVideoServerCandidate(request,true))return;}catch(_){return;}
    event.waitUntil((async()=>{
    await ensureVideoServerConfig();
    if(!mediaKey(request,true))return;
    const id=confirmationKey(sourceId,url);
    for(const [key,until] of confirmed)if(until<=Date.now())confirmed.delete(key);
    confirmed.set(id,Date.now()+30000);
    while(confirmed.size>64)confirmed.delete(confirmed.keys().next().value);
    if(capturing?.id===id){capturing.confirm();if(type==='psn-gameplay-cache-finish')capturing.finish?.();}
    if(type==='psn-gameplay-cache-finish'){enqueueCompletion(url,id);await scheduleCompletion();}
    })());
  }
});

function mediaKey(request, message=false) {
  if (request.method !== 'GET' || (!message && request.destination !== 'video') || request.mode !== 'cors') return null;
  try {
    const url = new URL(request.url);
    if (!videoServerOrigins.has(url.origin) || url.pathname !== '/api/gameplay-video') return null;
    const token = url.searchParams.get('token') || '';
    if (!/^[A-Za-z0-9_-]+\.[a-f0-9]{64}$/.test(token)) return null;
    const raw = token.split('.')[0].replace(/-/g, '+').replace(/_/g, '/');
    const v = JSON.parse(atob(raw));
    if (!Number.isSafeInteger(v.s) || v.s <= 0 || !Number.isSafeInteger(v.g) || v.g <= 0 ||
        !['video','video-normalized'].includes(v.t) || !Number.isSafeInteger(v.e) || v.e <= Date.now()/1000 ||
        !/^(?:[a-z0-9-]+\.)*screenscraper\.fr$/i.test(v.h || 'api.screenscraper.fr')) return null;
    // These public previews are keyed by media identity, not the rotating signed URL.
    return new URL('./__psn_gameplay_cache__/' + [v.s,v.g,v.t,v.h || 'api.screenscraper.fr'].join('-'), self.registration.scope).href;
  } catch (_) { return null; }
}
function parseRange(value, size) {
  const match = /^bytes=(\d*)-(\d*)$/.exec(value || '');
  if (!match || (!match[1] && !match[2])) return null;
  const a = match[1] ? Number(match[1]) : null;
  const b = match[2] ? Number(match[2]) : null;
  if ((a !== null && !Number.isSafeInteger(a)) || (b !== null && !Number.isSafeInteger(b))) return null;
  if (a === null) return b > 0 ? [Math.max(0,size-b),size-1] : null;
  return a < size && (b === null || b >= a) ? [a,Math.min(b === null ? size-1 : b,size-1)] : null;
}
function mp4Response(response){
  return /^video\/mp4(?:;|$)/i.test(response.headers.get('Content-Type')||'')&&
    (!response.headers.get('Content-Encoding')||response.headers.get('Content-Encoding')==='identity');
}
function completeLength(response) {
  if(!mp4Response(response))return 0;
  const declared=response.headers.get('Content-Length');
  if(response.status===200)return validSize(Number(declared))?Number(declared):0;
  if(response.status!==206)return 0;
  const range=/^bytes 0-(\d+)\/(\d+)$/.exec(response.headers.get('Content-Range')||'');
  const total=Number(range?.[2]);
  return range&&validSize(total)&&Number(range[1])===total-1&&(!declared||Number(declared)===total)?total:0;
}
function unknownComplete(response){return response.status===200&&mp4Response(response)&&!response.headers.has('Content-Length');}
function validSize(size) {
  return Number.isSafeInteger(size) && size >= 12 && size <= MAX_FILE;
}
function withIndex(task) {
  const result = indexQueue.then(task);
  indexQueue = result.catch(()=>{});
  return result;
}
async function loadIndex(cache) {
  if (!indexPromise) indexPromise = (async()=>{
    let meta, old = new Map();
    try {
      meta = await caches.open(META_CACHE);
      const response = await meta.match(INDEX_KEY);
      if (response) old = new Map(await response.json());
    } catch (_) {}
    const entries = new Map(), keys = await cache.keys();
    // Reconcile once per worker lifetime; preserve existing complete videos from v1.
    for (let i=0;i<keys.length;i+=16) {
      await Promise.all(keys.slice(i,i+16).map(async request=>{
        const response = await cache.match(request);
        const bytes = Number(response?.headers.get('Content-Length'));
        if (!validSize(bytes)) {await cache.delete(request);return;}
        const previous = old.get(request.url);
        const stored = Number(response.headers.get('X-PSN-Stored-At')) || 0;
        const used = previous?.bytes===bytes && Number.isFinite(previous.used) ? Math.max(stored,previous.used) : stored;
        entries.set(request.url,{bytes,used});
      }));
    }
    return {meta,entries};
  })().catch(error=>{indexPromise=null;throw error;});
  return indexPromise;
}
async function persistIndex(state) {
  try {
    if (!state.meta) state.meta = await caches.open(META_CACHE);
    await state.meta.put(INDEX_KEY,new Response(JSON.stringify([...state.entries]),{headers:{'Content-Type':'application/json'}}));
  } catch (_) {}
}
let touchFlush;
async function touch(cache,key) {
  const changed=await withIndex(async()=>{
    const state=await loadIndex(cache), saved=state.entries.get(key);
    if(!saved)return false;
    saved.used=Date.now();return true;
  });
  if(!changed)return;
  if(!touchFlush)touchFlush=new Promise(resolve=>setTimeout(resolve,500)).then(()=>{
    touchFlush=null;
    return withIndex(async()=>persistIndex(await loadIndex(cache)));
  });
  return touchFlush;
}
function storageBudget(estimate,total) {
  const quota=Number(estimate?.quota), usage=Number(estimate?.usage);
  if (!Number.isFinite(quota)||quota<=0||!Number.isFinite(usage)||usage<0) return DEFAULT_BYTES-MAX_PARTS_BYTES;
  const other=Math.max(0,usage-total);
  const reserve=Math.min(512*MIB,quota*.1);
  const available=Math.max(0,quota-other-reserve);
  const target=quota>=20*GIB&&available>=8*GIB?HIGH_BYTES:DEFAULT_BYTES;
  return Math.max(0,Math.floor(Math.min(target,quota*.1,available*.25)-MAX_PARTS_BYTES));
}
async function save(cache, key, chunks, size) {
  return withIndex(async()=>{
    const state = await loadIndex(cache), entries = state.entries;
    let total = 0;
    for (const entry of entries.values()) total += entry.bytes;
    let estimate;
    try {estimate=await self.navigator.storage?.estimate?.();} catch (_) {}
    const budget=storageBudget(estimate,total);
    const previous=entries.get(key);
    const candidates=[...entries].filter(([id])=>id!==key).sort((a,b)=>a[1].used-b[1].used);
    try {
      let needed=total-(previous?.bytes||0)+(size<=budget?size:0);
      for (const [id,entry] of candidates) {
        if (needed<=budget) break;
        await cache.delete(id);entries.delete(id);needed-=entry.bytes;
      }
      if (size>budget || needed>budget) {
        if (previous&&needed+previous.bytes>budget) {await cache.delete(key);entries.delete(key);}
        return;
      }
      const now=Date.now();
      await cache.put(key,new Response(new Blob(chunks,{type:'video/mp4'}),{headers:{
        'Content-Type':'video/mp4','Content-Length':String(size),'X-PSN-Stored-At':String(now)
      }}));
      entries.set(key,{bytes:size,used:now});
    } finally {await persistIndex(state);}
  });
}
async function cachedResponse(cache,key,request) {
  const saved = await cache.match(key);
  if (!saved) return null;
  const size = Number(saved.headers.get('Content-Length'));
  if (!validSize(size)) {await cache.delete(key);return null;}
  const headers = {'Content-Type':'video/mp4','Content-Length':String(size),'Accept-Ranges':'bytes',
    'Cache-Control':'no-store','Access-Control-Allow-Origin':'*','X-PSN-Video-Cache':'BROWSER'};
  const range = request.headers.get('Range');
  if (!range || request.headers.has('If-Range')) return new Response(saved.body,{headers});
  const bounds = parseRange(range,size);
  if (!bounds) return new Response(null,{status:416,headers:{'Content-Range':'bytes */'+size,'Accept-Ranges':'bytes','Access-Control-Allow-Origin':'*'}});
  const [start,end] = bounds;
  headers['Content-Length'] = String(end-start+1);
  headers['Content-Range'] = `bytes ${start}-${end}/${size}`;
  return new Response(sliceStream(saved.body,start,end+1),{status:206,headers});
}
function sliceStream(body,start,end){
  const reader=body.getReader();let offset=0,closed=false;
  const cancel=reason=>{if(closed)return Promise.resolve();closed=true;return reader.cancel(reason).catch(()=>{});};
  return new ReadableStream({
    async pull(controller){
      try{
        while(offset<end){
          const {done,value}=await reader.read();
          if(done)throw new Error('Incomplete cached range');
          const from=Math.max(0,start-offset),to=Math.min(value.byteLength,end-offset);
          offset+=value.byteLength;
          if(to>from){controller.enqueue(value.subarray(from,to));break;}
        }
        if(offset>=end){controller.close();cancel();}
      }catch(error){cancel();controller.error(error);}
    },cancel
  });
}
function partialInfo(response){
  if(response.status!==206||!/^video\/mp4(?:;|$)/i.test(response.headers.get('Content-Type')||'')||
    response.headers.get('Content-Encoding')&&response.headers.get('Content-Encoding')!=='identity')return null;
  const m=/^bytes (\d+)-(\d+)\/(\d+)$/.exec(response.headers.get('Content-Range')||'');
  if(!m)return null;
  const [start,end,total]=m.slice(1).map(Number),size=end-start+1;
  if(![start,end,total].every(Number.isSafeInteger)||!validSize(total)||start<0||end>=total||size<=0||size>MAX_PART||(response.headers.has('Content-Length')&&size!==Number(response.headers.get('Content-Length'))))return null;
  return {start,end,total,size};
}
function partPrefix(request,key){
  // The signature identifies a single signed media generation; never join ranges from different tokens.
  const sig=new URL(request.url).searchParams.get('token').split('.')[1];
  return key+'?part='+sig+'-';
}
function parsePart(url){
  const m=/[?]part=[a-f0-9]{64}-(\d+)-(\d+)-(\d+)-(\d+)$/.exec(url);
  if(!m)return null;
  const [start,end,total,stored]=m.slice(1).map(Number),size=end-start+1;
  if(![start,end,total,stored].every(Number.isSafeInteger)||!validSize(total)||start<0||end>=total||size<=0||size>MAX_PART)return null;
  return {start,end,total,stored,size};
}
async function cachedPart(request,key){
  const range=request.headers.get('Range');
  if(!range||request.headers.has('If-Range'))return null;
  const parts=await caches.open(PARTS_CACHE),prefix=partPrefix(request,key);
  const keys=await parts.keys();
  for(const req of keys){
    if(!req.url.startsWith(prefix))continue;
    const p=parsePart(req.url),bounds=p&&parseRange(range,p.total);
    if(!bounds||bounds[0]<p.start||bounds[1]>p.end)continue;
    const saved=await parts.match(req);
    if(!saved?.body||Number(saved.headers.get('Content-Length'))!==p.size)continue;
    const [start,end]=bounds;
    return new Response(sliceStream(saved.body,start-p.start,end-p.start+1),{status:206,headers:{
      'Content-Type':'video/mp4','Content-Length':String(end-start+1),'Content-Range':`bytes ${start}-${end}/${p.total}`,
      'Accept-Ranges':'bytes','Cache-Control':'no-store','Access-Control-Allow-Origin':'*','X-PSN-Video-Cache':'BROWSER-RANGE'
    }});
  }
  return null;
}
let partsQueue=Promise.resolve();
function savePart(request,key,part,chunks){
  const task=partsQueue.then(()=>writePart(request,key,part,chunks));
  partsQueue=task.catch(()=>{});return task;
}
async function writePart(request,key,part,chunks){
  let estimate;try{estimate=await self.navigator.storage?.estimate?.();}catch(_){}
  if(estimate?.quota>0&&estimate.quota-estimate.usage<64*MIB)return;
  const cache=await caches.open(PARTS_CACHE),prefix=partPrefix(request,key),keys=await cache.keys();
  let total=0,entries=[];
  for(const req of keys){
    const p=parsePart(req.url);
    if(!p){await cache.delete(req);continue;}
    if(req.url.startsWith(prefix)&&p.total===part.total&&p.start<=part.start&&p.end>=part.end)return;
    if(req.url.startsWith(prefix)&&p.total===part.total&&part.start<=p.start&&part.end>=p.end){await cache.delete(req);continue;}
    total+=p.size;entries.push({req,...p});
  }
  entries.sort((a,b)=>a.stored-b.stored);
  while(entries.length>=MAX_PARTS||total+part.size>MAX_PARTS_BYTES){const entry=entries.shift();if(!entry)return;await cache.delete(entry.req);total-=entry.size;}
  const url=prefix+[part.start,part.end,part.total,Date.now()].join('-');
  await cache.put(url,new Response(new Blob(chunks,{type:'video/mp4'}),{headers:{'Content-Type':'video/mp4','Content-Length':String(part.size)}}));
}
// Keep received bytes on an intentional priority interruption, without another request.
async function saveInterrupted(request,key,chunks,start,total){
  if(!validSize(total)||!chunks.length||start>=total)return;
  let offset=start,used=0,pieces=[],size=0;
  for(const bytes of chunks){
    let pos=0;
    while(pos<bytes.byteLength&&used<MAX_PARTS_BYTES&&offset+size<total){
      const take=Math.min(bytes.byteLength-pos,MAX_PART-size,MAX_PARTS_BYTES-used,total-offset-size);
      if(!take)break;
      pieces.push(bytes.subarray(pos,pos+take));pos+=take;used+=take;size+=take;
      if(size===MAX_PART){await savePart(request,key,{start:offset,end:offset+size-1,total,size},pieces);offset+=size;pieces=[];size=0;}
    }
    if(used>=MAX_PARTS_BYTES)break;
  }
  if(size)await savePart(request,key,{start:offset,end:offset+size-1,total,size},pieces);
  if(used)record('interrupted-range-saved',key,used);
}
async function readPrefix(request,key){
  await partsQueue;
  const cache=await caches.open(PARTS_CACHE),prefix=partPrefix(request,key);
  const entries=(await cache.keys()).filter(req=>req.url.startsWith(prefix)).map(req=>({req,p:parsePart(req.url)})).filter(x=>x.p);
  let offset=0,total=0;const chunks=[];
  while(true){
    const next=entries.filter(x=>x.p.start<=offset&&x.p.end>=offset&&(!total||x.p.total===total)).sort((a,b)=>b.p.end-a.p.end)[0];
    if(!next)break;
    const saved=await cache.match(next.req);if(!saved)break;
    const bytes=new Uint8Array(await saved.arrayBuffer());if(bytes.length!==next.p.size)break;
    total=next.p.total;chunks.push(bytes.subarray(offset-next.p.start));offset=next.p.end+1;
  }
  return {chunks,offset,total};
}
function removeParts(key){
  const task=partsQueue.then(async()=>{
    const cache=await caches.open(PARTS_CACHE);
    for(const req of await cache.keys())if(req.url.startsWith(key+'?part='))await cache.delete(req);
  });
  partsQueue=task.catch(()=>{});return task;
}
async function finishShownVideo(url,id,retries=0){
  let request,key;
  try{request=new Request(url,{mode:'cors',credentials:'omit'});key=mediaKey(request,true);}catch(_){return;}
  if(!key||completion||capturing||foregroundClients.size)return;
  const job={id,key,controller:new AbortController(),interrupted:false,promise:null};completion=job;
  const allowed=()=>!job.interrupted&&!foregroundClients.size&&!capturing;
  job.promise=(async()=>{
    let timer,downloaded=[],downloadStart=0,downloadTotal=0,attempted=false;
    try{
      const cache=await caches.open(CACHE);
      if(!allowed())return;
      const hit=await cache.match(key);if(hit){hit.body?.cancel().catch(()=>{});return;}
      const prefix=await readPrefix(request,key);
      if(!allowed())return;
      const second=await cache.match(key);if(second){second.body?.cancel().catch(()=>{});return;}
      if(!allowed())return;
      if(prefix.total&&prefix.offset===prefix.total){job.networkDone=true;await save(cache,key,prefix.chunks,prefix.total);const saved=await cache.match(key);if(saved){saved.body?.cancel().catch(()=>{});await removeParts(key);record('assembled',key,prefix.total);}return;}
      if((completionAttempts.get(key)||0)>Date.now())return;
      completionAttempts.delete(key);completionAttempts.set(key,Date.now()+60000);attempted=true;
      while(completionAttempts.size>128)completionAttempts.delete(completionAttempts.keys().next().value);
      timer=setTimeout(()=>job.controller.abort(),120000);
      const headers=new Headers();if(prefix.offset)headers.set('Range','bytes='+prefix.offset+'-');
      const backgroundUrl=new URL(url);backgroundUrl.searchParams.set('psn-background','1');
      const response=await fetch(new Request(backgroundUrl.href,{mode:'cors',credentials:'omit',headers,signal:job.controller.signal}));
      if(!allowed()){response.body?.cancel().catch(()=>{});return;}
      const full=completeLength(response),unknown=unknownComplete(response);
      const range=/^bytes (\d+)-(\d+)\/(\d+)$/.exec(response.headers.get('Content-Range')||'');
      const tail=!!(prefix.offset&&response.status===206&&mp4Response(response)&&range&&Number(range[1])===prefix.offset&&Number(range[3])===prefix.total&&Number(range[2])===prefix.total-1&&(!response.headers.has('Content-Length')||Number(response.headers.get('Content-Length'))===prefix.total-prefix.offset));
      if(!response.body||(!full&&!unknown&&!tail)){response.body?.cancel().catch(()=>{});return;}
      let size=tail?prefix.offset:0;const expected=tail?prefix.total:full;
      downloadStart=size;downloadTotal=expected;
      const reader=response.body.getReader();
      try{
        while(allowed()){
          const item=await reader.read();if(item.done)break;
          size+=item.value.byteLength;
          if(size>MAX_FILE||(expected&&size>expected)){await reader.cancel();downloaded=[];return;}
          downloaded.push(item.value);
        }
        if(!allowed()){reader.cancel().catch(()=>{});return;}
      }finally{reader.releaseLock();}
      if(!validSize(size)||(expected&&size!==expected))return;
      job.networkDone=true;
      await save(cache,key,tail?[...prefix.chunks,...downloaded]:downloaded,size);
      const saved=await cache.match(key);if(saved){saved.body?.cancel().catch(()=>{});await removeParts(key);record('completed',key,size);downloaded=[];}
    }catch(_){if(!job.interrupted)record('completion-error',key);}
    finally{
      clearTimeout(timer);
      if(job.interrupted){
        if(attempted)completionAttempts.delete(key);
        try{await saveInterrupted(request,key,downloaded,downloadStart,downloadTotal);}catch(_){}
        enqueueCompletion(url,id,retries+1);
      }
    }
  })().finally(()=>{if(completion===job)completion=null;});
  return job.promise;
}
async function handle(request,key,done,localOnly=false,clientId='') {
  let cache;
  try {
    cache = await caches.open(CACHE);
    const bypass=!localOnly&&new URL(request.url).searchParams.get('psn-cache-bypass')==='1';
    const hit=bypass?null:await cachedResponse(cache,key,request);
    if(hit){record('hit',key);touch(cache,key).catch(()=>{}).finally(done);return hit;}
    if(!localOnly&&!bypass){const partial=await cachedPart(request,key).catch(()=>null);if(partial){record('range-hit',key);done();return partial;}}
  } catch (_) {cache=null;}
  if(localOnly){done();return new Response(null,{status:404,headers:{'Cache-Control':'no-store'}});}
  const id=confirmationKey(clientId,request.url);
  interruptBackground();
  let response, upstreamAbort;
  const networkStarted=performance.now();
  let onAbort=()=>upstreamAbort?.abort();
  const abortListener=()=>onAbort();
  // Detach the single capture candidate from the media element's abort signal.
  // Speculative transfers are still aborted; a genuinely shown preview may finish its existing response.
  if(cache&&!capturing){
    upstreamAbort=new AbortController();
    request.signal.addEventListener('abort',abortListener,{once:true});
    if(request.signal.aborted)upstreamAbort.abort();
  }
  const cleanup=()=>request.signal.removeEventListener('abort',abortListener);
  try {response=await fetch(upstreamAbort?new Request(request,{signal:upstreamAbort.signal}):request);} catch (error) {cleanup();done();throw error;}
  const timing=String(response.headers.get('Server-Timing')||'');
  const duration=name=>{const value=new RegExp('(?:^|,\\s*)'+name+';dur=([0-9.]+)').exec(timing);return value?Number(value[1]):null;};
  record('network-headers',key,0,{status:response.status,range:request.headers.get('Range')||'',headersMs:Math.round(performance.now()-networkStarted),providerQueueMs:duration('provider_queue'),providerHeadersMs:duration('upstream_headers')});
  const completeSize=completeLength(response),unknown=unknownComplete(response),part=completeSize||unknown?null:partialInfo(response);
  const total=Number((response.headers.get('Content-Range')||'').split('/')[1]||response.headers.get('Content-Length'));
  if(total>MAX_FILE){completionAttempts.set(key,Date.now()+60000);while(completionAttempts.size>128)completionAttempts.delete(completionAttempts.keys().next().value);}
  const size=completeSize||part?.size||0;
  if (!cache || capturing || (!size&&!unknown) || !response.body || !upstreamAbort) {
    record('network-uncached',key,size||0);done();return response;
  }
  const reader=response.body.getReader();
  let chunks=[],received=0,collect=true,finished=false,playerClosed=false,playerController;
  let confirmedPlayback=wasConfirmed(id),pumping=null,drainRequested=false;
  let resolveCapture;const captureDone=new Promise(resolve=>{resolveCapture=resolve;});
  const capture={id,done:captureDone,background:false,
    confirm(){if(collect&&!finished)confirmedPlayback=true;},
    finish(){if(collect&&!finished){confirmedPlayback=true;capture.background=true;if(playerClosed&&!foregroundClients.size){drainRequested=true;pump();}}},
    interrupt(){
      if(finished||!collect||!capture.background)return;
      finished=true;drainRequested=false;
      const receivedChunks=chunks;
      if(!playerClosed){playerClosed=true;try{playerController.error(new DOMException('Preview superseded','AbortError'));}catch(_){}}
      cancelUpstream();record('capture-paused',key,received);
      const saved=saveInterrupted(request,key,receivedChunks,part?.start||0,part?.total||completeSize).catch(()=>{});
      release(saved);
    }};
  capturing=capture;
  const release=(saving=Promise.resolve())=>{
    if(!collect)return;
    collect=false;chunks=[];if(capturing===capture)capturing=null;clearTimeout(timer);
    Promise.resolve(saving).finally(()=>{resolveCapture();const wake=scheduleCompletion();Promise.resolve(wake).finally(done);});
  };
  const closePlayer=()=>{if(!playerClosed){playerClosed=true;try{playerController.close();}catch(_){}}};
  const cancelUpstream=()=>{upstreamAbort.abort();reader.cancel().catch(()=>{});cleanup();};
  const commit=async()=>{
    if(!collect)return;
    const complete=chunks;chunks=[];clearTimeout(timer);
    try{
      if(part){await savePart(request,key,part,complete);record('range-saved',key,received);}
      else{
        await save(cache,key,complete,received);
        const saved=await cache.match(key);
        if(saved){
          saved.body?.cancel().catch(()=>{});record('saved',key,received);await removeParts(key);
        }else {completionAttempts.set(key,Date.now()+60000);while(completionAttempts.size>128)completionAttempts.delete(completionAttempts.keys().next().value);record('storage-skipped',key,size);}
      }
    }catch(_){record('storage-error',key,size);}finally{release();}
  };
  const abandon=()=>{
    if(finished)return;
    playerClosed=true;
    if(confirmedPlayback&&collect){
      capture.background=true;
      if(foregroundClients.size){capture.interrupt();return;}
      drainRequested=true;pump();return;
    }
    finished=true;record('cancelled',key,received);cancelUpstream();release();
  };
  const timer=setTimeout(()=>{
    if(!collect)return;
    record('capture-timeout',key,received);release();drainRequested=false;
    if(playerClosed){finished=true;cancelUpstream();}
  },120000);
  function pump(){
    if(pumping||finished)return pumping;
    pumping=(async()=>{
      try{
        do{
          const item=await reader.read();
          if(finished)return;
          if(item.done){
            finished=true;cleanup();closePlayer();
            if(collect&&(unknown?validSize(received):received===size))await commit();else release();
            return;
          }
          if(collect){
            received+=item.value.byteLength;
            if(received<=(unknown?MAX_FILE:size))chunks.push(item.value);else{completionAttempts.set(key,Date.now()+60000);while(completionAttempts.size>128)completionAttempts.delete(completionAttempts.keys().next().value);release();}
          }
          if(!playerClosed){try{playerController.enqueue(item.value);}catch(_){playerClosed=true;}}
          // Do not depend on the player making another pull to observe EOF.
          if(collect&&!unknown&&received===size){
            finished=true;cleanup();closePlayer();
            reader.cancel().catch(()=>{});await commit();return;
          }
          if(playerClosed&&!(confirmedPlayback&&collect)){abandon();return;}
        }while(drainRequested&&collect);
      }catch(error){
        finished=true;cleanup();release();
        if(!playerClosed){playerClosed=true;try{playerController.error(error);}catch(_){}}
      }
    })().finally(()=>{pumping=null;if(drainRequested&&collect&&!finished)pump();});
    return pumping;
  }
  const stream=new ReadableStream({
    start(controller){playerController=controller;},
    pull(){return pump();},
    cancel(){abandon();}
  });
  onAbort=abandon;
  if(request.signal.aborted)abandon();
  record('capture-start',key,size);
  return new Response(stream,{status:response.status,statusText:response.statusText,headers:response.headers});
}
self.addEventListener('fetch',event=>{
  const request=event.request;
  const localOnly=request.method==='GET'&&request.destination==='video'&&request.url.startsWith(LOCAL_PREFIX)&&
    /^\d+-\d+-video(?:-normalized)?-(?:[a-z0-9-]+\.)*screenscraper\.fr$/i.test(request.url.slice(LOCAL_PREFIX.length));
  if(!localOnly&&!isVideoServerCandidate(request))return;
  let done;
  event.waitUntil(new Promise(resolve=>{done=resolve;}));
  event.respondWith((async()=>{
    if(!localOnly)await ensureVideoServerConfig();
    const key=localOnly?request.url:mediaKey(request);
    if(!key){done();return fetch(request);}
    return handle(request,key,done,localOnly,event.clientId);
  })().catch(error=>{done();throw error;}));
});
