// Private SPU3 child. No socket, global fetch, Source client or Use grant.
// The original Go owner authorizes the authenticated request and quarantines
// HTTP before replay. These finite payload bounds do NOT certify native fit.
import { Buffer, isUtf8 } from 'node:buffer';
import { createHash } from 'node:crypto';
import { Readable, Writable } from 'node:stream';
import { encodeRequestAuthentication } from './authentication.mjs';

const denied = () => new Error('pi_http_stdio_denied');
const names = ['frame_bytes','initialize_body_bytes','request_body_bytes','final_body_bytes',
  'request_header_bytes','response_header_bytes','response_body_bytes','response_chunk_bytes','parent_bytes','child_bytes','frames'];
const positive = n => Number.isSafeInteger(n) && n > 0;

export function createPiHTTPStdioChannel({input,output,limits,signal}) {
  if (!(input instanceof Readable) || !(output instanceof Writable) || input.destroyed || output.destroyed ||
      !(signal instanceof AbortSignal) || signal.aborted || !limits || !Object.isFrozen(limits) ||
      Reflect.ownKeys(limits).length !== names.length || names.some(k => {
        const d=Object.getOwnPropertyDescriptor(limits,k);return !d||!Object.hasOwn(d,'value')||!positive(d.value);
      }) ||
      limits.frame_bytes < 114 || limits.frame_bytes > 1<<20 ||
      limits.initialize_body_bytes > limits.frame_bytes-113 ||
      limits.request_header_bytes < 2 || limits.request_header_bytes > limits.frame_bytes-85 ||
      limits.request_body_bytes > limits.frame_bytes-85-limits.request_header_bytes ||
      limits.final_body_bytes > Math.min(128<<10,limits.frame_bytes-17) ||
      limits.response_header_bytes < 2 || limits.response_header_bytes > limits.frame_bytes-63 ||
      limits.response_body_bytes > limits.parent_bytes || limits.response_chunk_bytes > limits.response_body_bytes ||
      limits.parent_bytes > 4<<20 || limits.child_bytes > 16<<20 || limits.frames > 4096) throw denied();
  const C = Math.min(limits.response_chunk_bytes,limits.frame_bytes-21);
  const N = Math.ceil(limits.response_body_bytes/C);
  if (6+2*N > limits.frames || limits.initialize_body_bytes+limits.response_body_bytes+limits.response_header_bytes+254+21*N > limits.parent_bytes ||
      limits.request_body_bytes+limits.final_body_bytes+limits.request_header_bytes+102+21*N > limits.child_bytes) throw denied();
  // Fixed owned arenas only, never an allocation from an uninspected wire size.
  const header=Buffer.alloc(17), metadata=Buffer.alloc(96), outHeader=Buffer.alloc(17), outMetadata=Buffer.alloc(68+limits.request_header_bytes);
  const initArena=Buffer.alloc(limits.initialize_body_bytes), entity=Buffer.alloc(limits.response_body_bytes), headerArena=Buffer.alloc(limits.response_header_bytes);
  const use=new AbortController(), waits=new Set(), links=[];
  const notice=()=>{let resolve,reject;const promise=new Promise((yes,no)=>{resolve=yes;reject=no;});promise.catch(()=>{});return {promise,resolve,reject};};
  // Original-cap-sized fixed signals, never a queue allocated from peer count.
  const start=notice(),end=notice(),parts=Array.from({length:N},notice);
  let phase='new',closing=false,inputBytes=0,outputBytes=0,frames=0,initialization,exchange,publication,monitor;
  let tuple,expected,requestBytes,responseHeaders,responseSize,responseCount,fetchStarted=false,pullStarted=-1;
  const inputClosed=new Promise(resolve=>input.once('close',resolve)), outputClosed=new Promise(resolve=>output.once('close',resolve));
  const stop=()=>{
    if(closing)return; closing=true; phase='closed'; use.abort();
    for(const n of [start,end,...parts])n.reject(denied());
    for(const [s,fn] of links)s.removeEventListener('abort',fn); links.length=0;
    for(const cancel of [...waits])cancel();
    input.destroy();output.destroy();
  };
  const streamError=()=>stop(); input.on('error',streamError);output.on('error',streamError);
  const link=s=>{const fn=()=>stop(),pair=[s,fn];links.push(pair);s.addEventListener('abort',fn,{once:true});if(s.aborted)stop();
    return()=>{s.removeEventListener('abort',fn);const at=links.indexOf(pair);if(at>=0)links.splice(at,1);};};
  link(signal);
  const alive=()=>{if(closing||signal.aborted||use.signal.aborted)throw denied();};
  const readable=()=>new Promise((resolve,reject)=>{
    const clean=()=>{waits.delete(cancel);input.off('readable',ready);input.off('end',cancel);input.off('close',cancel);};
    const ready=()=>{clean();resolve();},cancel=()=>{clean();reject(denied());};
    waits.add(cancel);input.once('readable',ready);input.once('end',cancel);input.once('close',cancel);
    if(closing||input.destroyed||input.readableEnded)cancel();
  });
  const readFull=async(target,firstPositive)=>{
    let at=0;
    while(at<target.length){
      alive();const part=input.read(Math.min(target.length-at,16384));
      if(part===null){await readable();continue;}
      if(!Buffer.isBuffer(part)||part.length===0||part.length>target.length-at||part.length>limits.parent_bytes-inputBytes)throw denied();
      if(at===0&&firstPositive)firstPositive();
      part.copy(target,at);at+=part.length;inputBytes+=part.length;
    }
  };
  const writeFull=async bytes=>{
    if(bytes.length>limits.child_bytes-outputBytes)throw denied();
    for(let at=0;at<bytes.length;){
      alive();const part=bytes.subarray(at,Math.min(bytes.length,at+16384));
      // Cancellation destroys this stream, but buffers remain pinned until the
      // actual callback joins. No timeout/detach may manufacture that join.
      await new Promise((resolve,reject)=>output.write(part,error=>error||closing?reject(denied()):resolve()));
      at+=part.length;outputBytes+=part.length;
    }
  };
  const readHeader=async(kind,meta,maximum,exact,admitted=()=>true)=>{
    // Capture the admission epoch at the FIRST actual positive prefix byte,
    // not after a fragmented header finishes under a newer writer credit.
    let prefixAdmitted;
    await readFull(header.subarray(0,1),()=>{prefixAdmitted=admitted();});
    await readFull(header.subarray(1));
    if(header[0]!==83||header[1]!==80||header[2]!==85||header[3]!==51||header[4]!==kind||
      header.readUInt32BE(5)!==meta||!prefixAdmitted||++frames>limits.frames)throw denied();
    const n=header.readBigUInt64BE(9);
    if(n>BigInt(maximum)||(exact!==undefined&&n!==BigInt(exact))||BigInt(17+meta)+n>BigInt(limits.frame_bytes)||
      BigInt(meta)+n>BigInt(limits.parent_bytes-inputBytes))throw denied();
    await readFull(metadata.subarray(0,meta));return Number(n);
  };
  const writeFrame=async(kind,meta,body)=>{
    if(++frames>limits.frames||17+meta.length+body.length>limits.frame_bytes)throw denied();
    outHeader.write('SPU3');outHeader[4]=kind;outHeader.writeUInt32BE(meta.length,5);outHeader.writeBigUInt64BE(BigInt(body.length),9);
    await writeFull(outHeader);await writeFull(meta);await writeFull(body);
  };
  const headersFrom=raw=>{
    if(raw.length<2)throw denied();let at=2,previous='';const count=raw.readUInt16BE(0),pairs=[];
    if(count<1||count>3)throw denied();
    for(let i=0;i<count;i++){
      if(at+2>raw.length)throw denied();const n=raw.readUInt16BE(at);at+=2;
      if(n<1||n>raw.length-at)throw denied();const name=raw.subarray(at,at+n).toString('latin1');at+=n;
      if(!['content-encoding','content-length','content-type'].includes(name)||name<=previous||at+4>raw.length)throw denied();previous=name;
      const v=raw.readUInt32BE(at);at+=4;if(v<1||v>raw.length-at)throw denied();
      const bytes=raw.subarray(at,at+v);at+=v;
      if(bytes[0]<=32||bytes[v-1]<=32||bytes.some(c=>c<32||c>126))throw denied();
      const value=bytes.toString('ascii');
      if(name==='content-encoding'&&value!=='identity')throw denied();
      if(name==='content-type'&&!/^text\/event-stream(?:;\s*charset=utf-8)?$/i.test(value))throw denied();
      pairs.push([name,value]);
    }
    if(at!==raw.length||!pairs.some(([k])=>k==='content-type'))throw denied();return pairs;
  };
  const readResponse=async()=>{
    try{
      // The SOLE reader starts immediately after Init, including file/bootstrap
      // and held Fetch/Pull writes. Retire or EOF always revokes before join.
      const h=await readHeader(3,46,limits.response_header_bytes,undefined,()=>fetchStarted);
      responseCount=metadata.readUInt32BE(2);const size=metadata.readBigUInt64BE(6),digest=Buffer.from(metadata.subarray(14,46));
      if(h<2||metadata.readUInt16BE(0)!==200||size===0n||size>BigInt(limits.response_body_bytes)||responseCount!==Math.ceil(Number(size)/C))throw denied();
      responseSize=Number(size);await readFull(headerArena.subarray(0,h));responseHeaders=headersFrom(headerArena.subarray(0,h));
      for(const [k,v] of responseHeaders)if(k==='content-length'){
        // Same bounded accumulation as the Go HTTP owner, including leading
        // zeros. Never parse a peer-sized decimal into a huge BigInt.
        let length=0;
        for(const digit of v){const d=digit.charCodeAt(0)-48;
          if(d<0||d>9||length>Math.floor(responseSize/10)||length===Math.floor(responseSize/10)&&d>responseSize%10)throw denied();
          length=10*length+d;
        }
        if(length!==responseSize)throw denied();
      }
      start.resolve();const hash=createHash('sha256');let seen=0;
      for(let index=0;index<responseCount;index++){
        const n=await readHeader(4,4,C,Math.min(C,responseSize-seen),()=>pullStarted===index);
        if(metadata.readUInt32BE(0)!==index)throw denied();
        const part=entity.subarray(seen,seen+n);await readFull(part);hash.update(part);seen+=n;parts[index].resolve();
      }
      await readHeader(6,44,0,0);
      if(metadata.readUInt32BE(0)!==responseCount||metadata.readBigUInt64BE(4)!==size||!metadata.subarray(12,44).equals(digest)||!hash.digest().equals(digest))throw denied();
      alive();end.resolve();
      // No further ordinary parent frame is legal. Retire/EOF/malformed data
      // all stop this invocation; no second reader or detached payload queue.
      await readFull(header);stop();
    }catch{stop();}
  };
  const initialize=()=>{
    if(phase!=='new'){stop();return Promise.reject(denied());}phase='initializing';
    initialization=(async()=>{try{
      const n=await readHeader(1,96,limits.initialize_body_bytes);
      if(n===0)throw denied();tuple=Buffer.from(metadata);await readFull(initArena.subarray(0,n));
      if(!isUtf8(initArena.subarray(0,n)))throw denied();alive();phase='ready';
      monitor=readResponse();
      return Object.freeze({body:initArena.subarray(0,n),profileDigest:tuple.subarray(64,96).toString('hex')});
    }catch{stop();throw denied();}})();return initialization;
  };
  const bindRequest=admitted=>{
    try{
      alive();
      if(phase!=='ready'||!admitted||!Object.isFrozen(admitted)||
        Reflect.ownKeys(admitted).length!==4||['profileDigest','endpoint','body','headers'].some(k=>{
          const d=Object.getOwnPropertyDescriptor(admitted,k);return !d||!Object.hasOwn(d,'value');
        })||
        admitted.profileDigest!==tuple.subarray(64,96).toString('hex')||typeof admitted.endpoint!=='string'||
        typeof admitted.body!=='string'||!admitted.body.isWellFormed()||admitted.body.length===0||
        Buffer.byteLength(admitted.body)>limits.request_body_bytes||!Array.isArray(admitted.headers)||!Object.isFrozen(admitted.headers))throw denied();
      expected=admitted;phase='bound';return Object.freeze({fetch,dispose:stop});
    }catch{stop();throw denied();}
  };
  const fetch=(url,init)=>{
    if(phase!=='bound'||closing){stop();return Promise.reject(denied());}phase='fetching';
    exchange=(async()=>{try{
      alive();
      if(url!==expected.endpoint||!init||Reflect.ownKeys(init).length!==5||init.method!=='POST'||init.redirect!=='error'||
        init.body!==expected.body||!(init.headers instanceof Headers)||!(init.signal instanceof AbortSignal))throw denied();
      const unlinkFetch=link(init.signal);alive();const pairs=[...Headers.prototype.entries.call(init.headers)];
      if(pairs.length!==12||expected.headers.length!==12||pairs.some(([k,v])=>!expected.headers.some(([name,value])=>k===name&&v===value)))throw denied();
      const auth=encodeRequestAuthentication(tuple.subarray(0,32),tuple.subarray(32,64),tuple.subarray(64,96),
        url,pairs.filter(([k])=>k!=='authorization'),init.body,Headers.prototype.get.call(init.headers,'authorization'),limits.request_header_bytes);
      requestBytes=Buffer.from(init.body,'utf8');outMetadata.set(tuple.subarray(64,96));outMetadata.set(auth.mac,32);
      outMetadata.writeUInt32BE(auth.headers.length,64);outMetadata.set(auth.headers,68);auth.mac.fill(0);auth.headers.fill(0);
      fetchStarted=true;
      await writeFrame(2,outMetadata.subarray(0,68+outMetadata.readUInt32BE(64)),requestBytes);
      requestBytes.fill(0);requestBytes=undefined;expected=undefined;outMetadata.fill(0);
      await start.promise;
      for(let index=0;index<responseCount;index++){
        pullStarted=index;
        outMetadata.writeUInt32BE(index);await writeFrame(5,outMetadata.subarray(0,4),Buffer.alloc(0));
        await parts[index].promise;
      }
      await end.promise;
      alive();phase='response';
      // Native SSE guard aborts its own fetch controller at terminal cleanup.
      // After VERIFIED End that is stream-local, not invocation withdrawal.
      // The original signal and sole parent Retire/EOF monitor remain active.
      unlinkFetch();
      let at=0;
      return new Response(new ReadableStream({pull(controller){
        try{alive();if(at===responseSize){controller.close();return;}const end=Math.min(responseSize,at+C);controller.enqueue(entity.subarray(at,end));at=end;}
        catch{controller.error(denied());stop();}
      },cancel(){at=responseSize;}},{highWaterMark:0}),{status:200,headers:responseHeaders});
    }catch{stop();throw denied();}})();return exchange;
  };
  const publishFinal=text=>{
    if(phase!=='response'||closing){stop();return Promise.reject(denied());}phase='publishing';
    publication=(async()=>{let bytes;try{
      alive();if(typeof text!=='string'||text.length===0||!text.isWellFormed()||Buffer.byteLength(text)>limits.final_body_bytes)throw denied();
      bytes=Buffer.from(text,'utf8');await writeFrame(7,Buffer.alloc(0),bytes);alive();phase='final';
    }catch{stop();throw denied();}finally{bytes?.fill(0);}})();return publication;
  };
  const dispose=async()=>{
    stop();await Promise.allSettled([initialization,exchange,publication,monitor]);await Promise.all([inputClosed,outputClosed]);
    for(const bytes of [header,metadata,outHeader,outMetadata,initArena,entity,headerArena,tuple,requestBytes])bytes?.fill(0);
    expected=undefined;responseHeaders=undefined;input.off('error',streamError);output.off('error',streamError);
  };
  return Object.freeze({initialize,bindRequest,publishFinal,dispose,signal:use.signal});
}
