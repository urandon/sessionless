import assert from 'node:assert/strict';
import { Buffer } from 'node:buffer';
import { createFixedRequestContract } from './request_contract.mjs';
const freeze=v=>{if(v&&typeof v==='object'){for(const key of Object.keys(v))freeze(v[key]);Object.freeze(v);}return v;};
const limits=freeze({native_request_bytes:1<<20,json_nodes:10000,json_depth:32});
function expected() {
 const schema=JSON.parse('{"type":"object","properties":{"4294967295":{"type":"string"},"01":{"type":"string"},"4294967294":{"type":"string"},"__proto__":{"type":"string"},"answer":{"type":"string"},"artifacts":{"type":"array","items":{"type":"object","properties":{"name":{"type":"string"},"content":{"type":"string"}},"required":["name","content"],"additionalProperties":false}}},"required":["answer"],"additionalProperties":false}');
 return freeze({model:'synthetic',input:[{role:'system',content:'trusted <>&\u2028\u2029💬\n\t"\\'},
  {role:'user',content:[{type:'input_text',text:'full old history {"n":90071992547409931234567890,"x":1.00001e99}'},
  {type:'input_text',text:'first repeated-name source'}, {type:'input_image',detail:'auto',image_url:'data:image/png;base64,AQ=='},
  {type:'input_text',text:'second historical source'}, {type:'input_image',detail:'auto',image_url:'data:image/jpeg;base64,AQI='},
  {type:'input_text',text:'assistant summary then newest sentinel'}]}],stream:true,prompt_cache_options:{mode:'explicit'},store:false,max_output_tokens:16,
  tool_choice:'none',reasoning:{effort:'none'},truncation:'disabled',text:{format:{type:'json_schema',name:'result',schema,strict:true}}});
}
function payload(e){const p={model:e.model,input:e.input,stream:true,prompt_cache_key:undefined,prompt_cache_retention:undefined,prompt_cache_options:e.prompt_cache_options,
 store:false,max_output_tokens:e.max_output_tokens,tool_choice:'none',reasoning:e.reasoning};return p;}
const e=expected(),actual=createFixedRequestContract({limits,expectedRequest:e})(payload(e));
assert.equal(JSON.stringify(actual),JSON.stringify(e));
assert.equal(Object.isFrozen(actual),true);
assert.ok(JSON.stringify(actual).includes('<>&\u2028\u2029💬'));
assert.ok(JSON.stringify(actual).includes('90071992547409931234567890'));
assert.ok(Object.keys(actual.text.format.schema.properties).indexOf('4294967294')<Object.keys(actual.text.format.schema.properties).indexOf('4294967295'));
const callback=createFixedRequestContract({limits,expectedRequest:expected()});callback(payload(expected()));assert.throws(()=>callback(payload(expected())),{name:'PiRequestContractError'});
for(const [name,mutate] of [
 ['missing-history',p=>{p.input=[p.input[1]];}],['changed-model',p=>{p.model='different';}],['floor-change',p=>{p.max_output_tokens=15;}],
 ['unknown-null',p=>{p.metadata=null;}],['unknown-undefined',p=>{p.metadata=undefined;}],['tool',p=>{p.tools=[];}],
 ['native-session',p=>{p.previous_response_id='old';}],['schema-injection',p=>{p.text=e.text;}],['truncation',p=>{p.truncation='auto';}],
 ['field-order',p=>{const v=p.model;delete p.model;p.model=v;}],['undefined-vs-null',p=>{p.prompt_cache_key=null;}],
 ['getter',p=>{Object.defineProperty(p,'model',{get(){throw Error('raw diagnostic');},enumerable:true});}],
 ['symbol',p=>{p[Symbol('extension')]=true;}],['prototype',p=>{Object.setPrototypeOf(p,{inherited:true});}],
 ['unsafe-number',p=>{p.max_output_tokens=9007199254740992;}],['fraction',p=>{p.temperature=0.5;}]
]){
 const ex=expected(),p=payload(ex);mutate(p);
 assert.throws(()=>createFixedRequestContract({limits,expectedRequest:ex})(p),{name:'PiRequestContractError'},name);
}
for(const bad of [null,undefined,{limits,expectedRequest:{model:'toy'}}]){
 assert.throws(()=>createFixedRequestContract(bad),{name:'PiRequestContractError'});
}
for(const [key,n] of [['json_nodes',1],['json_depth',1],['native_request_bytes',1]]){
 const tiny=freeze({...limits,[key]:n});assert.throws(()=>createFixedRequestContract({limits:tiny,expectedRequest:expected()}),{name:'PiRequestContractError'});
}
const pure=expected();const p=payload(pure);Object.defineProperty(p,'input',{value:pure.input,enumerable:true});
const out=createFixedRequestContract({limits,expectedRequest:pure})(p);
assert.equal(out.input,pure.input); // same admitted immutable input; no reserialize.
assert.equal(Object.prototype.hasOwnProperty.call(out.text.format.schema.properties,'__proto__'),true);
// A separately owned real-style params tree stays identical until the sole SDK
// stringify, not merely a frozen root borrowing mutable source children.
const mutableExpected=expected(),mutablePayload=payload(mutableExpected);
mutablePayload.input=JSON.parse(JSON.stringify(mutableExpected.input));
const immutableOutput=createFixedRequestContract({limits,expectedRequest:mutableExpected})(mutablePayload);
assert.throws(()=>{mutablePayload.input[1].content[0].text='post-hook drift';},TypeError);
assert.equal(JSON.stringify(immutableOutput),JSON.stringify(mutableExpected));
// Dense own indices do not make an exotic array prototype inert. Neither
// callback nor accessor is invoked during denial; literal schema __proto__ DATA
// and ordinary independently cloned nested arrays remain supported above.
for(const kind of ['callback','accessor']){
 let called=0;const ex=expected(),p=payload(ex);
 p.input=JSON.parse(JSON.stringify(ex.input));
 const inherited=Object.create(Array.prototype);
 if(kind==='callback')Object.defineProperty(inherited,'toJSON',{value(){called++;return [];}});
 else Object.defineProperty(inherited,'toJSON',{get(){called++;throw Error('raw inherited accessor');}});
 Object.setPrototypeOf(p.input[1].content,inherited);
 assert.throws(()=>createFixedRequestContract({limits,expectedRequest:ex})(p),{name:'PiRequestContractError'},kind);
 assert.equal(called,0,kind+' inherited hook ran during inspect');
}
// Builtin reference for the EXACT same Go wire golden rules; no custom JS
// expected encoder/stringifier and no SDK import.
for(const [raw,want] of [
 ['{"s":"<>&\\u2028\\u2029\\ud83d\\udcac"}','{"s":"<>&\u2028\u2029💬"}'],
 ['{"4294967295":"last","01":"ordinary","4294967294":"index","0":"zero","__proto__":{"value":true}}','{"0":"zero","4294967294":"index","4294967295":"last","01":"ordinary","__proto__":{"value":true}}'],
 ['{"a":9007199254740991,"b":-9007199254740991,"negativeZero":-0,"v":[true,false,null,{},[]]}','{"a":9007199254740991,"b":-9007199254740991,"negativeZero":0,"v":[true,false,null,{},[]]}']
]) assert.equal(JSON.stringify(JSON.parse(raw)),want);
// Independent builtin UTF8 oracle for the inspect logical string/key envelope,
// not a serving/native/whole-wire fit certificate. No private observer/timer.
function payloadStringBytes(root){
 let n=0;const pending=[root];
 while(pending.length){const v=pending.pop();if(typeof v==='string')n+=Buffer.byteLength(v,'utf8');
  else if(v&&typeof v==='object'){if(Array.isArray(v))pending.push(...v);else for(const k of Object.keys(v)){n+=Buffer.byteLength(k,'utf8');pending.push(v[k]);}}
 }return n;
}
const unicodeBoundary=JSON.parse(JSON.stringify(expected()));
unicodeBoundary.input[0].content='<>&\u2028\u2029💬é';
Object.defineProperty(unicodeBoundary.text.format.schema.properties,'clé💬',{value:{type:'string'},enumerable:true});
freeze(unicodeBoundary);
const exactBytes=payloadStringBytes(unicodeBoundary),exactLimits=freeze({...limits,native_request_bytes:exactBytes});
const exactPayload=payload(unicodeBoundary);exactPayload.input=JSON.parse(JSON.stringify(unicodeBoundary.input));
assert.equal(JSON.stringify(createFixedRequestContract({limits:exactLimits,expectedRequest:unicodeBoundary})(exactPayload)),JSON.stringify(unicodeBoundary));
assert.throws(()=>createFixedRequestContract({limits:freeze({...exactLimits,native_request_bytes:exactBytes-1}),expectedRequest:unicodeBoundary}),{name:'PiRequestContractError'});
for(const kind of ['actual-ascii-string','actual-utf8-string','actual-key','actual-total','surrogate-high','surrogate-low']){
 const ex=unicodeBoundary,p=payload(ex);p.input=JSON.parse(JSON.stringify(ex.input));
 if(kind==='actual-ascii-string')p.input[0].content='x'.repeat(exactBytes+1);
 else if(kind==='actual-utf8-string')p.input[0].content='💬'.repeat(Math.floor(exactBytes/2));
 else if(kind==='actual-key')Object.defineProperty(p.input[1].content[0],'k'.repeat(exactBytes+1),{value:true,enumerable:true});
 else if(kind==='actual-total'){p.input[0].content='x'.repeat(Math.ceil(exactBytes/2));p.input[1].content[0].text='x'.repeat(Math.ceil(exactBytes/2));}
 else p.input[0].content=kind==='surrogate-high'?'\ud800':'\udc00';
 assert.throws(()=>createFixedRequestContract({limits:exactLimits,expectedRequest:ex})(p),{name:'PiRequestContractError'},kind);
}
for(const kind of ['expected-string','expected-key']){
 const ex=JSON.parse(JSON.stringify(unicodeBoundary));
 if(kind==='expected-string')ex.input[0].content='x'.repeat(exactBytes+1);
 else Object.defineProperty(ex.text.format.schema.properties,'k'.repeat(exactBytes+1),{value:{type:'string'},enumerable:true});
 freeze(ex);assert.throws(()=>createFixedRequestContract({limits:exactLimits,expectedRequest:ex}),{name:'PiRequestContractError'},kind);
}
console.log('Pi SDK-free complete payload contract: PASS');
