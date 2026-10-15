// Credential-free Go -> actual pinned Node decoder controls, not an SDK,
// model, native-fit, Source, clock or serving qualification. Input is the full
// original Go fixture; never print it, a path, a body or provider diagnostics.
import assert from 'node:assert/strict';
import { Buffer } from 'node:buffer';
import { decodePreparedInit } from './init.mjs';

const limits=Object.freeze(JSON.parse(process.argv[2]));
let size=0;const chunks=[];
for await(const chunk of process.stdin){size+=chunk.length;assert.ok(size<=limits.body_bytes);chunks.push(chunk);}
const raw=Buffer.concat(chunks,size),signal=new AbortController().signal;
const admitted=decodePreparedInit(raw,limits,signal);
assert.equal(admitted.init.version,1);
assert.equal(admitted.init.model.cost.input,0.125);
assert.equal(admitted.init.model.cost.cacheRead,0.0125);
assert.equal(admitted.init.messages[0].content.filter(b=>b.type==='image').length,2);
const text=admitted.init.messages[0].content.filter(b=>b.type==='text').map(b=>b.text).join('');
for(const marker of ['history turn 1','history turn 2','historic assistant summary','newest sentinel','90071992547409931234567890123','1.0000000000000000001e99'])assert.ok(text.includes(marker));
assert.ok(Object.hasOwn(admitted.init.schema.properties,'__proto__'));
assert.ok(Object.isFrozen(admitted.init.messages[0].content[0]));
assert.ok(admitted.remaining.copy_bytes>0&&admitted.remaining.json_nodes>=0);
let denied=0;
const bad=(b,l=limits,s=signal)=>{assert.throws(()=>decodePreparedInit(b,l,s),{message:'pi_init_denied'});denied++;};
const original=JSON.parse(raw);
for(const key of Object.keys(original)){
  const changed=JSON.parse(raw);delete changed[key];bad(Buffer.from(JSON.stringify(changed)));
}
for(const mutate of [
  v=>{v.version=2;},v=>{v.max_output_tokens=9007199254740992;},v=>{v.timeout_ms=0;},v=>{v.temperature=0.5;},
  v=>{v.model.provider=null;},v=>{v.model.id='different';},v=>{v.model.baseUrl='https://wrong.invalid';},
  v=>{delete v.model.cost.input;},v=>{v.model.cost.input=-0.1;},v=>{v.model.headers={authorization:'forbidden'};},
  v=>{v.messages[0].timestamp=1.5;},v=>{v.messages[0].content[0].text='history dropped';},
  v=>{const image=v.messages[0].content.find(b=>b.type==='image');image.data=(image.data[0]==='A'?'B':'A')+image.data.slice(1);},
  v=>{v.headers[0][0]='authorization';},v=>{v.headers[0][1]='application/json\r\nforbidden';},
  v=>{v.credential_file='../outside';},v=>{v.schema_name='different';},v=>{v.schema.properties.extra={type:'string'};},
  v=>{v.schema_source_digest='0'.repeat(64);},v=>{v.profile_digest='0'.repeat(64);},v=>{v.profile_json='{}';},
]){const v=JSON.parse(raw);mutate(v);bad(Buffer.from(JSON.stringify(v)));}
// A duplicate-key regression must fail for that predicate, not incidentally
// exhaust an exact node partition before nested parsing. These are explicit
// synthetic decoder-unit bounds only: no P change, Issue, SDK or serving fit.
const roomy=Object.freeze({...limits,json_nodes:limits.json_nodes+8});
assert.equal(decodePreparedInit(raw,roomy,signal).init.version,1);
for(const text of ['{"version":1,'+raw.toString('utf8').slice(1),'{"\\u0076ersion":1,'+raw.toString('utf8').slice(1)])bad(Buffer.from(text),roomy);
for(const text of [raw+'{}',raw.toString('utf8').slice(0,-1)])bad(Buffer.from(text));
bad(Buffer.from([0xff]));
bad(raw,Object.freeze({...limits,body_bytes:raw.length-1}));
bad(raw,Object.freeze({...limits,copy_bytes:1}));
bad(raw,Object.freeze({...limits,json_nodes:1}));
bad(raw,Object.freeze({...limits,json_depth:1}));
const cancelled=new AbortController();cancelled.abort();bad(raw,limits,cancelled.signal);
assert.ok(denied>=48);
// Decoding never imports a bootstrap at all, so every denial precedes SDK,
// auth/catalog/file operations by construction. Actual delayed entry is still
// a separate runner integration obligation, not simulated by this assertion.
console.log('complete Init Go -> Node PASS; negative controls='+denied);
