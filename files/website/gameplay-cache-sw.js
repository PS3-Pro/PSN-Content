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
const SERVER = 'https://psn-content-jh5h.onrender.com';
let capturing = false;

self.addEventListener('install',event=>event.waitUntil(self.skipWaiting()));
self.addEventListener('activate', event => event.waitUntil(self.clients.claim()));
self.addEventListener('message',event=>{
  if(event.data?.type==='psn-gameplay-cache-version')event.ports[0]?.postMessage({version:2});
});

function mediaKey(request) {
  if (request.method !== 'GET' || request.destination !== 'video' || request.mode !== 'cors') return null;
  try {
    const url = new URL(request.url);
    if (url.origin !== SERVER || url.pathname !== '/api/gameplay-video') return null;
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
function completeLength(response) {
  const length = Number(response.headers.get('Content-Length'));
  if (!Number.isSafeInteger(length) || length < 12 || length > MAX_FILE ||
      !/^video\/mp4(?:;|$)/i.test(response.headers.get('Content-Type') || '') ||
      response.headers.get('Content-Encoding') && response.headers.get('Content-Encoding') !== 'identity') return 0;
  if (response.status === 200) return length;
  if (response.status !== 206) return 0;
  const range = /^bytes 0-(\d+)\/(\d+)$/.exec(response.headers.get('Content-Range') || '');
  return range && Number(range[1]) === length-1 && Number(range[2]) === length ? length : 0;
}
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
  if(![start,end,total].every(Number.isSafeInteger)||!validSize(total)||start<0||end>=total||size<=0||size>MAX_PART||size!==Number(response.headers.get('Content-Length')))return null;
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
async function savePart(request,key,part,chunks){
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
async function handle(request,key,done,localOnly=false) {
  let cache;
  try {
    cache = await caches.open(CACHE);
    const bypass=!localOnly&&new URL(request.url).searchParams.get('psn-cache-bypass')==='1';
    const hit=bypass?null:await cachedResponse(cache,key,request);
    if(hit){touch(cache,key).catch(()=>{}).finally(done);return hit;}
    if(!localOnly&&!bypass){const partial=await cachedPart(request,key).catch(()=>null);if(partial){done();return partial;}}
  } catch (_) {cache=null;}
  if(localOnly){done();return new Response(null,{status:404,headers:{'Cache-Control':'no-store'}});}
  let response;
  try {response=await fetch(request);} catch (error) {done();throw error;}
  const completeSize=completeLength(response),part=completeSize?null:partialInfo(response);
  const size=completeSize||part?.size;
  if (!cache || capturing || !size || !response.body) {done();return response;}
  // Pass bytes through as the player reads them. No second fetch or clone draining abandoned downloads.
  capturing=true;
  const reader=response.body.getReader();
  let chunks=[],received=0,collect=true;
  const release=()=>{if(!collect)return;collect=false;chunks=[];capturing=false;clearTimeout(timer);done();};
  const timer=setTimeout(release,120000);
  const stream=new ReadableStream({
    async pull(controller) {
      try {
        const item=await reader.read();
        if(item.done){
          controller.close();
          if(collect && received===size){
            const complete=chunks;chunks=[];
            clearTimeout(timer);
            try {
              if(part)await savePart(request,key,part,complete);
              else {
                await save(cache,key,complete,size);
                if(await cache.match(key)){
                  const parts=await caches.open(PARTS_CACHE);
                  for(const req of await parts.keys())if(req.url.startsWith(key+'?part='))await parts.delete(req);
                }
              }
            } catch (_) {} finally {release();}
          } else release();
          return;
        }
        if(collect){received+=item.value.byteLength;if(received<=size)chunks.push(item.value);else release();}
        controller.enqueue(item.value);
      } catch(error){release();controller.error(error);}
    },
    cancel(reason){release();return reader.cancel(reason).catch(()=>{});}
  });
  return new Response(stream,{status:response.status,statusText:response.statusText,headers:response.headers});
}
self.addEventListener('fetch',event=>{
  const request=event.request;
  const localOnly=request.method==='GET'&&request.destination==='video'&&request.url.startsWith(LOCAL_PREFIX)&&
    /^\d+-\d+-video(?:-normalized)?-(?:[a-z0-9-]+\.)*screenscraper\.fr$/i.test(request.url.slice(LOCAL_PREFIX.length));
  const key=localOnly?request.url:mediaKey(request);
  if(!key)return;
  let done;
  event.waitUntil(new Promise(resolve=>{done=resolve;}));
  event.respondWith(handle(request,key,done,localOnly));
});
