import assert from 'node:assert/strict';
import {test} from 'node:test';
import {createHash,createHmac} from 'node:crypto';
import {PassThrough,Writable} from 'node:stream';
import {createPiHTTPStdioChannel} from './http_stdio.mjs';

// Independent manual wire/HMAC oracle, no SDK, credentials, Source or sockets.
const limits=Object.freeze({frame_bytes:1024,initialize_body_bytes:128,request_body_bytes:256,final_body_bytes:256,
 request_header_bytes:512,response_header_bytes:512,response_body_bytes:1024,response_chunk_bytes:8,parent_bytes:8192,child_bytes:8192,frames:512});
const tuple=Buffer.concat([Buffer.alloc(32,1),Buffer.alloc(32,2),Buffer.alloc(32,3)]);
const body='{"original":"Ω"}',key='PUBLIC-NONCREDENTIAL-LITERAL';
const pairs=Object.freeze([['accept','application/json'],['content-type','application/json'],['user-agent','fixture'],
 ['x-stainless-arch','arm64'],['x-stainless-lang','js'],['x-stainless-os','MacOS'],['x-stainless-package-version','6.40.0'],
 ['x-stainless-retry-count','0'],['x-stainless-runtime','node'],['x-stainless-runtime-version','v24'],['x-stainless-timeout','30'],
 ['authorization','Bearer '+key]].map(Object.freeze));
function frame(kind,meta=Buffer.alloc(0),body=Buffer.alloc(0)){
 const h=Buffer.alloc(17);h.write('SPU3');h[4]=kind;h.writeUInt32BE(meta.length,5);h.writeBigUInt64BE(BigInt(body.length),9);return Buffer.concat([h,meta,body]);
}
function canonical(pairs){
 const out=[Buffer.alloc(2)];out[0].writeUInt16BE(pairs.length);
 for(const [k,v] of pairs){const a=Buffer.from(k),b=Buffer.from(v),n=Buffer.alloc(2),m=Buffer.alloc(4);n.writeUInt16BE(a.length);m.writeUInt32BE(b.length);out.push(n,a,m,b);}
 return Buffer.concat(out);
}
function response(bytes=Buffer.from(': original Ω\n\ndata: {}\n\n'),mutate=()=>{}){
 const count=Math.ceil(bytes.length/8),digest=createHash('sha256').update(bytes).digest(),start=Buffer.alloc(46),end=Buffer.alloc(44);
 start.writeUInt16BE(200);start.writeUInt32BE(count,2);start.writeBigUInt64BE(BigInt(bytes.length),6);digest.copy(start,14);
 end.writeUInt32BE(count);end.writeBigUInt64BE(BigInt(bytes.length),4);digest.copy(end,12);
 const frames=[frame(3,start,canonical([['content-type','text/event-stream']]))];
 for(let i=0;i<count;i++){const m=Buffer.alloc(4);m.writeUInt32BE(i);frames.push(frame(4,m,bytes.subarray(i*8,(i+1)*8)));}
 frames.push(frame(6,end));mutate(frames);return {frames,bytes,count};
}
function fixture(t,{onWrite,holdDestroyCallbacks=false}={}){
 const input=new PassThrough(),chunks=[],callbacks=new Set(),readWaits=new Set();let readBytes=0,onFrame,frameAt=0;
 const originalRead=input.read;input.read=function(n){const b=originalRead.call(this,n);if(b){readBytes+=b.length;for(const w of [...readWaits])if(readBytes>=w.bytes){readWaits.delete(w);w.resolve();}}return b;};
 const output=new Writable({write(chunk,_encoding,done){chunks.push(Buffer.from(chunk));
  const sent=Buffer.concat(chunks);while(sent.length-frameAt>=17){const size=17+sent.readUInt32BE(frameAt+5)+Number(sent.readBigUInt64BE(frameAt+9));if(sent.length-frameAt<size)break;const f=sent.subarray(frameAt,frameAt+size);frameAt+=size;onFrame?.(f);}
  const settle=error=>{callbacks.delete(settle);done(error);};callbacks.add(settle);if(onWrite)onWrite(chunk,settle);else settle();
 },destroy(_error,done){if(!holdDestroyCallbacks)for(const f of [...callbacks])f(new Error('synthetic cancel'));done();}});
 const original=new AbortController(),channel=createPiHTTPStdioChannel({input,output,limits,signal:original.signal});
 // Register cancellation + actual held callback release BEFORE assertions.
 // A failed test must not leave its own deliberately blocked writer alive.
 t.after(async()=>{original.abort();for(const done of [...callbacks])done(new Error('test cleanup joined held writer'));await channel.dispose();assert.equal(callbacks.size,0);for(const s of [input,output])for(const e of ['readable','end','close','error'])assert.equal(s.listenerCount(e),0,e);});
 return {channel,input,output,chunks,original,readBytes:()=>readBytes,onFrame(fn){onFrame=fn;},
  readUntil(bytes){if(readBytes>=bytes)return Promise.resolve();return new Promise(resolve=>readWaits.add({bytes,resolve}));}};
}
async function ready(f){
 const pending=f.channel.initialize();f.input.write(frame(1,tuple,Buffer.from('{"init":"Ω"}')));const {body:raw}=await pending;
 const fetch=f.channel.bindRequest(Object.freeze({endpoint:'https://synthetic.invalid/responses',profileDigest:tuple.subarray(64).toString('hex'),headers:pairs,body}));
 return {fetch,raw};
}
function init(signal){return {method:'POST',body,headers:new Headers(pairs),redirect:'error',signal};}
function feed(f,r,oneByte=false){
 const send=raw=>{if(oneByte)for(const b of raw)f.input.write(Buffer.from([b]));else f.input.write(raw);};
 f.onFrame(raw=>{if(raw[4]!==5)return;const i=raw.readUInt32BE(17);send(r.frames[i+1]);
  if(i+1===r.count){if(r.frames[i+2])send(r.frames[i+2]);else f.input.end();}
 });send(r.frames[0]);
}

test('actual request HMAC and exact original replay, terminal stream cleanup preserves Final', {timeout:10000},async t=>{
 const f=fixture(t),{fetch,raw}=await ready(f),sdk=new AbortController(),r=response();
 const pending=fetch.fetch('https://synthetic.invalid/responses',init(sdk.signal));feed(f,r,true);
 const received=await pending;assert.deepEqual(Buffer.from(await received.arrayBuffer()),r.bytes);
 // Existing SDK response guard aborts its own controller during normal cleanup.
 sdk.abort();assert.equal(f.channel.signal.aborted,false);
 await f.channel.publishFinal('{"answer":"Ω","artifacts":[]}');
 const sent=Buffer.concat(f.chunks),hq=canonical(pairs.filter(([k])=>k!=='authorization'));
 const mac=createHmac('sha256',key);mac.update('Sessionless/Pi/SPU3/Auth/1\0');
 for(const field of [tuple.subarray(0,32),tuple.subarray(32,64),tuple.subarray(64),Buffer.from('POST'),Buffer.from('https://synthetic.invalid/responses'),createHash('sha256').update(hq).digest(),createHash('sha256').update(body).digest()]){const n=Buffer.alloc(4);n.writeUInt32BE(field.length);mac.update(n);mac.update(field);}
 assert.equal(sent[4],2);assert.deepEqual(sent.subarray(17,49),tuple.subarray(64));assert.deepEqual(sent.subarray(49,81),mac.digest());
 assert.deepEqual(sent.subarray(85,85+hq.length),hq);assert.deepEqual(sent.subarray(85+hq.length,85+hq.length+Buffer.byteLength(body)),Buffer.from(body));
 assert.equal(sent.includes(Buffer.from(key)),false);assert.deepEqual(raw,Buffer.from('{"init":"Ω"}'));
 await f.channel.dispose();assert.ok(raw.every(b=>b===0));
});

test('opaque response parts split UTF8 and SSE without decoding or rewriting bytes',{timeout:10000},async t=>{
 const f=fixture(t),{fetch}=await ready(f),r=response(Buffer.from(': xyzz Ω\n\ndata: {}\n\n'));
 assert.equal(r.bytes[7],0xce);assert.equal(r.bytes[8],0xa9);
 const pending=fetch.fetch('https://synthetic.invalid/responses',init(f.original.signal));feed(f,r,true);
 assert.deepEqual(Buffer.from(await (await pending).arrayBuffer()),r.bytes);
});

for(const mode of ['count','bytes','digest','part index','part size','early end','bad magic','wrong direction','header value','end count','end bytes','end digest','eof']){
 test(`response ${mode} never exposes body or permits Final`,{timeout:10000},async t=>{
  const f=fixture(t),{fetch}=await ready(f),r=response(undefined,frames=>{
   if(mode==='count')frames[0].writeUInt32BE(99,19);
   if(mode==='bytes')frames[0].writeBigUInt64BE(1025n,23);
   if(mode==='digest')frames[0][31]^=1;
   if(mode==='part index')frames[1].writeUInt32BE(1,17);
   if(mode==='part size')frames[1].writeBigUInt64BE(7n,9);
   if(mode==='early end')frames[1]=frames.at(-1);
   if(mode==='bad magic')frames[0][0]|=128;
   if(mode==='wrong direction')frames[0][4]=2;
   if(mode==='header value')frames[0][frames[0].length-1]=255;
   if(mode==='end count')frames.at(-1).writeUInt32BE(99,17);
   if(mode==='end bytes')frames.at(-1).writeBigUInt64BE(999n,21);
   if(mode==='end digest')frames.at(-1)[29]^=1;
   if(mode==='eof')frames.pop();
  });
  const pending=fetch.fetch('https://synthetic.invalid/responses',init(f.original.signal));feed(f,r);
  await assert.rejects(pending,{message:'pi_http_stdio_denied'});await assert.rejects(f.channel.publishFinal('fake'));
 });
}

test('forged huge declaration rejected before body read; SPU2 does not initialize SPU3', {timeout:10000},async t=>{
 for(const magic of ['SPU3','SPU2']){
  const f=fixture(t),pending=f.channel.initialize(),h=frame(1,tuple,Buffer.from('x')).subarray(0,17);h.write(magic);h.writeBigUInt64BE((1n<<64n)-1n,9);
  f.input.write(h);await assert.rejects(pending);assert.equal(f.readBytes(),17);
 }
});

test('wrong part index checked before declared part body', {timeout:10000},async t=>{
 const f=fixture(t),{fetch}=await ready(f),r=response(),before=f.readBytes(),bad=Buffer.from(r.frames[1].subarray(0,21));bad.writeUInt32BE(1,17);
 f.onFrame(raw=>{if(raw[4]===5)f.input.write(bad);});
 const pending=fetch.fetch('https://synthetic.invalid/responses',init(f.original.signal));f.input.write(r.frames[0]);
 await assert.rejects(pending);assert.equal(f.readBytes()-before,r.frames[0].length+21);
});

test('second concurrent fetch spends channel; original abort joins held write before clearing', {timeout:10000},async t=>{
 let release,entered;const held=new Promise(r=>entered=r);
 const f=fixture(t,{holdDestroyCallbacks:true,onWrite(_chunk,done){if(!release){release=done;entered();}else done();}}),{fetch,raw}=await ready(f);
 const first=fetch.fetch('https://synthetic.invalid/responses',init(f.original.signal));await held;
 await assert.rejects(fetch.fetch('https://synthetic.invalid/responses',init(f.original.signal)));
 let disposed=false;const pending=f.channel.dispose().then(()=>disposed=true);await Promise.resolve();assert.equal(disposed,false);assert.ok(raw.some(b=>b!==0));
 release(new Error('synthetic held writer joined'));await assert.rejects(first);await pending;assert.ok(raw.every(b=>b===0));
});

test('normal response stream cancel after End is local; parent EOF still revokes Final', {timeout:10000},async t=>{
 const f=fixture(t),{fetch}=await ready(f),pending=fetch.fetch('https://synthetic.invalid/responses',init(f.original.signal));feed(f,response());
 const result=await pending;await result.body.cancel();assert.equal(f.channel.signal.aborted,false);
 const aborted=new Promise(resolve=>f.channel.signal.addEventListener('abort',resolve,{once:true}));f.input.end();await aborted;await assert.rejects(f.channel.publishFinal('fake'));
});

for(const control of ['Retire','EOF'])for(const phase of ['after Init','held Fetch','held Pull']){
 test(`${control} continuously observed during ${phase}, cancellation precedes write join`,{timeout:10000},async t=>{
  let release,entered;const held=new Promise(resolve=>entered=resolve);
  const f=fixture(t,{holdDestroyCallbacks:true,onWrite(chunk,done){
   const kind=phase==='held Pull'?5:2;
   if(phase!=='after Init'&&chunk.length===17&&chunk[4]===kind&&!release){release=done;entered();}else done();
  }}),{fetch,raw}=await ready(f);
  let pending;if(phase!=='after Init'){
   pending=fetch.fetch('https://synthetic.invalid/responses',init(f.original.signal));pending.catch(()=>{});
   if(phase==='held Pull')feed(f,response());await held;
  }
  const aborted=new Promise(resolve=>f.channel.signal.addEventListener('abort',resolve,{once:true}));
  if(control==='Retire')f.input.write(frame(8));else f.input.end();await aborted;
  assert.equal(f.output.destroyed,true);assert.ok(raw.some(b=>b!==0));
  if(release){let joined=false;const disposal=f.channel.dispose().then(()=>joined=true);await Promise.resolve();assert.equal(joined,false);
   release(new Error('synthetic actual callback joined'));await assert.rejects(pending);await disposal;assert.ok(raw.every(b=>b===0));
  }else await assert.rejects(fetch.fetch('https://synthetic.invalid/responses',init(f.original.signal)));
 });
}

for(const mode of ['exact','leading zeros','overflow','nondigit'])test(`Content-Length ${mode} preserves bounded parent semantics`,{timeout:10000},async t=>{
 const f=fixture(t),{fetch}=await ready(f),r=response(),value=mode==='leading zeros'?'0'.repeat(128)+r.bytes.length:mode==='overflow'?'99999999999999999999999999999':mode==='nondigit'?'1x':String(r.bytes.length);
 const m=Buffer.from(r.frames[0].subarray(17,63));r.frames[0]=frame(3,m,canonical([['content-length',value],['content-type','text/event-stream']]));
 const pending=fetch.fetch('https://synthetic.invalid/responses',init(f.original.signal));feed(f,r);
 if(mode==='overflow'||mode==='nondigit')await assert.rejects(pending);else assert.deepEqual(Buffer.from(await (await pending).arrayBuffer()),r.bytes);
});

test('independent canonical minimum-C maximum-N and exact/one-short directional ceilings',async()=>{
 const caps={...limits,response_body_bytes:2045,response_chunk_bytes:1,frames:4096,parent_bytes:65536,child_bytes:65536};
 const parent=128+2045+512+254+21*2045,child=256+256+512+102+21*2045;
 for(const mode of ['exact','parent one short','child one short','frame first over']){
  const l={...caps,parent_bytes:parent,child_bytes:child};if(mode==='parent one short')l.parent_bytes--;if(mode==='child one short')l.child_bytes--;if(mode==='frame first over')l.response_body_bytes++;
  const input=new PassThrough(),output=new Writable({write(_b,_e,done){done();}}),signal=new AbortController().signal;
  if(mode==='exact'){const channel=createPiHTTPStdioChannel({input,output,limits:Object.freeze(l),signal});await channel.dispose();}
  else{assert.throws(()=>createPiHTTPStdioChannel({input,output,limits:Object.freeze(l),signal}));input.destroy();output.destroy();}
 }
});

test('early fragmented Start prefix cannot inherit a later Fetch admission epoch',{timeout:10000},async t=>{
 const f=fixture(t),{fetch}=await ready(f),r=response(),before=f.readBytes();
 f.input.write(r.frames[0].subarray(0,16));await f.readUntil(before+1);
 const pending=fetch.fetch('https://synthetic.invalid/responses',init(f.original.signal));
 f.input.write(r.frames[0].subarray(16));await assert.rejects(pending);
 assert.equal(f.readBytes()-before,17,'stale Start metadata/body must remain unread');
});

test('early fragmented Part prefix across held Fetch cannot inherit later Pull credit',{timeout:10000},async t=>{
 let release,entered;const held=new Promise(resolve=>entered=resolve);
 const f=fixture(t,{holdDestroyCallbacks:true,onWrite(chunk,done){if(!release){release=done;entered();}else done();}}),{fetch}=await ready(f),r=response(),before=f.readBytes();
 const pending=fetch.fetch('https://synthetic.invalid/responses',init(f.original.signal));pending.catch(()=>{});await held;
 f.input.write(r.frames[0]);await f.readUntil(before+r.frames[0].length);
 f.input.write(r.frames[1].subarray(0,16));await f.readUntil(before+r.frames[0].length+1);
 // Only finish the stale Part header AFTER the actual outgoing Pull completed.
 f.onFrame(raw=>{if(raw[4]===5)f.input.write(r.frames[1].subarray(16));});release();
 await assert.rejects(pending);assert.equal(f.readBytes()-before,r.frames[0].length+17,'stale Part metadata/body must remain unread');
});
