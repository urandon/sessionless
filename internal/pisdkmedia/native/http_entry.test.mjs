// SDK-free entry import under the repository's filesystem-only permission.
import assert from 'node:assert/strict';
import {test} from 'node:test';
import {PassThrough,Writable} from 'node:stream';
import {parsePreparedHTTPArguments,runPreparedPiHTTPStdio} from './http_entry.mjs';

const values=[1024,911,256,256,512,512,1024,8,8192,8192,512,911,65536,4096,32,35008,256].map(String);
test('seventeen closed SPU3 arguments; no ten-argument SPU2 interpretation',()=>{
 const parsed=parsePreparedHTTPArguments(values);assert.equal(Object.isFrozen(parsed.channelLimits),true);assert.equal(Object.isFrozen(parsed.decodeLimits),true);
 assert.equal(parsed.channelLimits.frame_bytes,1024);assert.equal(parsed.decodeLimits.body_bytes,911);
 for(const v of [values.slice(0,10),[...values,'1'],values.map((x,i)=>i===0?'01':x),values.map((x,i)=>i===3?'0':x),values.map((x,i)=>i===4?'9007199254740992':x)])assert.throws(()=>parsePreparedHTTPArguments(v));
});

test('malformed complete Init rejects before file and genuine SDK construction',{timeout:10000},async()=>{
 // Smaller initialized body keeps all canonical wire totals within parent cap.
 const args=parsePreparedHTTPArguments(values.map((x,i)=>i===1||i===11?'128':x));
 const input=new PassThrough(),chunks=[],output=new Writable({write(b,_encoding,done){chunks.push(Buffer.from(b));done();}});
 const pending=runPreparedPiHTTPStdio({input,output,signal:new AbortController().signal,...args,workDir:'/synthetic-nonexistent-work'});
 const raw=Buffer.alloc(115);raw.write('SPU3');raw[4]=1;raw.writeUInt32BE(96,5);raw.writeBigUInt64BE(2n,9);raw.write('{}',113);input.write(raw);
 await assert.rejects(pending,{message:'pi_http_entry_initialize_denied'});assert.equal(chunks.length,0);
 assert.equal(input.destroyed,true);assert.equal(output.destroyed,true);
});
