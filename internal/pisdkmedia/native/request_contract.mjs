// SDK-FREE private DATA contract. No SDK import, constructor, fetch, timer,
// environment/catalog read or currentness/credential authority.
// The actual standard provider constructs params; this hook validates all its
// existing own fields and appends only the original required fixed additions.
const denied = () => { const e = new Error('pi_request_payload_denied'); e.name = 'PiRequestContractError'; throw e; };
const required = ['model', 'input', 'stream', 'store', 'max_output_tokens', 'tool_choice', 'truncation', 'text'];
const fields = ['model', 'input', 'stream', 'prompt_cache_options', 'store', 'max_output_tokens', 'temperature', 'tool_choice', 'reasoning', 'include', 'truncation', 'text'];
const omitted = ['prompt_cache_key', 'prompt_cache_retention', 'prompt_cache_options'];
const plain = v => v !== null && typeof v === 'object' && [Object.prototype, null].includes(Object.getPrototypeOf(v));
function inertArrayPrototype(v) {
 const proto=Object.getPrototypeOf(v);
 if(proto===null)return true;
 // Source descriptors only: never read/call inherited toJSON. Exact ordinary
 // array/object chain or null; no custom prototype/accessor/callback extension.
 return proto===Array.prototype&&Object.getPrototypeOf(proto)===Object.prototype&&
  !Object.getOwnPropertyDescriptor(proto,'toJSON')&&!Object.getOwnPropertyDescriptor(Object.prototype,'toJSON');
}
function utf8(s, cap) {
 // UTF16 length is an O(1) lower bound on valid UTF8 bytes. Deny before
 // walking any borrowed string/key that cannot fit the shared remainder.
 if(s.length>cap)denied();
 let n = 0;
 for (let i=0;i<s.length;i++) { const c=s.charCodeAt(i); if(c<128)n++;else if(c<2048)n+=2;else if(c>=0xd800&&c<=0xdbff){const d=s.charCodeAt(++i);if(!(d>=0xdc00&&d<=0xdfff))denied();n+=4;}else{if(c>=0xdc00&&c<=0xdfff)denied();n+=3;} if(n>cap)denied(); }
 return n;
}
function inspect(root, limits, frozen, sourceOmissions = false, charge = () => {}, freeze = false) {
 let nodes=0,bytes=0; const pending=[{v:root,depth:1,parent:null}];
 while(pending.length) {
  const {v,depth,parent}=pending.pop();if(++nodes>limits.json_nodes)denied();charge(1);
  if(typeof v==='string'){bytes+=utf8(v,limits.native_request_bytes-bytes);continue;}
  if(v===null||typeof v==='boolean')continue;
  if(typeof v==='number'){if(!Number.isSafeInteger(v))denied();continue;}
  if(!v||typeof v!=='object'||depth>limits.json_depth||
   (Array.isArray(v)?!inertArrayPrototype(v):!plain(v)))denied();
  for(let p=parent;p;p=p.parent)if(p.v===v)denied();
  const nextParent={v,parent};if(frozen&&!Object.isFrozen(v))denied();
  const keys=Reflect.ownKeys(v);if(keys.length>limits.json_nodes-nodes+1)denied();
  if(Array.isArray(v)){
   const length=Object.getOwnPropertyDescriptor(v,'length');if(!length||!Object.hasOwn(length,'value')||!Number.isSafeInteger(length.value)||length.value<0||keys.length!==length.value+1)denied();
   for(let i=0;i<length.value;i++){const d=Object.getOwnPropertyDescriptor(v,String(i));if(!d||!Object.hasOwn(d,'value'))denied();pending.push({v:d.value,depth:depth+1,parent:nextParent});}
  }else{
   for(const key of keys){if(typeof key!=='string'||++nodes>limits.json_nodes)denied();charge(1);bytes+=utf8(key,limits.native_request_bytes-bytes);const d=Object.getOwnPropertyDescriptor(v,key);if(!d||!Object.hasOwn(d,'value'))denied();if(d.value===undefined&&sourceOmissions&&v===root&&omitted.includes(key))continue;pending.push({v:d.value,depth:depth+1,parent:nextParent});}
  }
  if(pending.length>limits.json_nodes-nodes)denied();
  if(freeze)Object.freeze(v);
 }
 return {nodes,bytes};
}
function equal(a,b,limits,charge) {
 const pending=[[a,b]];let nodes=0;
 while(pending.length){if(++nodes>limits.json_nodes)denied();charge(1);const [x,y]=pending.pop();if(Object.is(x,y)||x===0&&y===0)continue;
  if(x===null||y===null||typeof x!=='object'||typeof y!=='object'||Array.isArray(x)!==Array.isArray(y))return false;
  const keys=Object.keys(x),other=Object.keys(y);if(keys.length!==other.length||keys.some((key,i)=>key!==other[i]))return false;
  for(const key of keys){if(++nodes>limits.json_nodes)denied();charge(1);pending.push([Object.getOwnPropertyDescriptor(x,key).value,Object.getOwnPropertyDescriptor(y,key).value]);}
  if(pending.length>limits.json_nodes-nodes)denied();
 }return true;
}
function closedExpected(v) {
 const keys=Object.keys(v);
 if(!plain(v)||required.some(k=>!Object.hasOwn(v,k))||keys.some(k=>!fields.includes(k))||v.stream!==true||v.store!==false||
  typeof v.model!=='string'||!v.model||!Array.isArray(v.input)||!v.input.length||!Number.isSafeInteger(v.max_output_tokens)||v.max_output_tokens<16||
  v.tool_choice!=='none'||v.truncation!=='disabled'||!plain(v.text)||Object.keys(v.text).join(',')!=='format')denied();
 const f=v.text.format;if(!plain(f)||Object.keys(f).join(',')!=='type,name,schema,strict'||f.type!=='json_schema'||typeof f.name!=='string'||!/^[A-Za-z0-9_-]{1,64}$/.test(f.name)||!plain(f.schema)||f.strict!==true)denied();
 if(keys.at(-2)!=='truncation'||keys.at(-1)!=='text')denied();
 const base=['model','input','stream'];if(keys.slice(0,3).join(',')!==base.join(','))denied();
}
export function createFixedRequestContract(input) {
 try{
  if(!plain(input)||Object.keys(input).join(',')!=='limits,expectedRequest')denied();
  const {limits,expectedRequest}=input;
  if(!plain(limits)||!Object.isFrozen(limits)||Object.keys(limits).join(',')!=='native_request_bytes,json_nodes,json_depth')denied();
  for(const v of Object.values(limits))if(!Number.isSafeInteger(v)||v<=0)denied();
  let remaining=limits.json_nodes-inspect(expectedRequest,limits,true).nodes;closedExpected(expectedRequest);
  const charge=n=>{if(n>remaining)denied();remaining-=n;};let spent=false;
  const expectedKeys=Object.keys(expectedRequest).filter(k=>k!=='truncation'&&k!=='text');
  return function fixedPayload(payload) {
   try{
    if(spent)denied();spent=true;if(!plain(payload))denied();
    const keys=Reflect.ownKeys(payload),actualKeys=[];
    if(keys.length>fields.length+omitted.length)denied();
    for(const k of keys){
     if(typeof k!=='string')denied();const d=Object.getOwnPropertyDescriptor(payload,k);if(!d||!Object.hasOwn(d,'value'))denied();
     if(d.value===undefined){if(!omitted.includes(k))denied();continue;}
     if(k==='truncation'||k==='text'||!fields.includes(k))denied();actualKeys.push(k);
    }
    if(actualKeys.length!==expectedKeys.length||actualKeys.some((k,i)=>k!==expectedKeys[i]))denied();
    // Validate actual own trees BEFORE structural comparison/forwarding.
    inspect(payload,limits,false,true,charge);
    for(const k of actualKeys){const value=Object.getOwnPropertyDescriptor(payload,k).value;if(!equal(value,expectedRequest[k],limits,charge))denied();}
    // Source-owned actual params cannot drift between this synchronous hook
    // and the SDK's later sole serialization. Fund every freeze-visit node
    // before its work, from the same remaining counter; no second stringify.
    inspect(payload,limits,false,true,charge,true);
    // Safe own values already inspected; no user getter/toJSON callback.
    const result=Object.create(null);
    for(const k of keys)result[k]=Object.getOwnPropertyDescriptor(payload,k).value;
    result.truncation='disabled';result.text=expectedRequest.text;
    return Object.freeze(result);
   }catch{denied();}
  };
 }catch{denied();}
}
