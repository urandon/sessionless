// SDK-free closed Init DATA decoder. No fs, credentials, SDK, model discovery,
// constructor, fetch or currentness authority. Its limits are the partition
// already reserved by the SAME original Go request ledger before Issue.
import { Buffer, isUtf8 } from 'node:buffer';
import { createHash } from 'node:crypto';
import { isAbsolute, normalize, basename } from 'node:path';
import { inspectJSONGrammar } from '../../pisdktransport/request.mjs';

const deny = () => { throw new Error('pi_init_denied'); };
const own = (v,k) => Object.hasOwn(v,k);
const plain = v => v !== null && typeof v === 'object' && !Array.isArray(v) &&
  [Object.prototype,null].includes(Object.getPrototypeOf(v));
const positive = n => Number.isSafeInteger(n) && n > 0;
const closed = (v,required,optional=[]) => plain(v) &&
  required.every(k=>own(v,k)) && Object.keys(v).every(k=>required.includes(k)||optional.includes(k));
const exact = (v,names) => closed(v,names) && Object.keys(v).length === names.length;
const digest = s => typeof s === 'string' && /^[a-f0-9]{64}$/.test(s) && s !== '0'.repeat(64);
const limitNames = ['canonical_input_bytes','native_request_bytes','pre_send_allocation_bytes','request_header_bytes',
  'response_header_bytes','response_body_bytes','response_chunk_bytes','sse_line_bytes','sse_event_bytes','sse_events',
  'json_nodes','response_copy_work_bytes','response_allocation_bytes','process_memory_bytes','process_disk_bytes',
  'process_frame_bytes','process_stdout_bytes','process_events','json_depth','raw_final_bytes','structured_final_bytes',
  'answer_bytes','artifact_bytes','artifact_total_bytes'];
const sourceNames = ['pi_source_revision','pi_package_digest','pi_build_digest','pi_dependency_lock_digest',
  'pi_generated_catalog_digest','open_ai_sdk_version','open_ai_sdk_integrity','open_ai_sdk_package_digest',
  'node_version','node_source_digest','node_executable_digest','native_artifact_digest','bootstrap_digest',
  'stream_wrapper_digest','physical_transport_digest','request_schema_digest','final_schema_digest'];
const headerNames = ['accept','content-type','user-agent','x-stainless-lang','x-stainless-package-version',
  'x-stainless-os','x-stainless-arch','x-stainless-runtime','x-stainless-runtime-version','x-stainless-retry-count','x-stainless-timeout'];
const initNames = ['version','profile_json','profile_digest','model','system_prompt','max_output_tokens','timeout_ms',
  'cache_retention','reasoning_effort','reasoning_summary','temperature','schema_name','schema_source_digest',
  'schema','headers','messages','expected_body','credential_max_bytes','credential_file'];
const compatNames = ['supportsDeveloperRole','sessionAffinityFormat','supportsLongCacheRetention','supportsStrictMode',
  'supportsOpenAIGrammarTools','supportsAdditionalTools','supportsToolSearch','supportsExplicitPromptCacheMode'];

function freeze(root) {
  const pending=[root];
  while(pending.length) {
    const v=pending.pop();
    if(v===null||typeof v!=='object')continue;
    for(const k of Object.keys(v))pending.push(v[k]);
    Object.freeze(v);
  }
  return root;
}
function equal(a,b) {
  const pending=[[a,b]];
  while(pending.length) {
    const [x,y]=pending.pop();
    if(x===y)continue;
    if(x===null||y===null||typeof x!=='object'||typeof y!=='object'||Array.isArray(x)!==Array.isArray(y))return false;
    const keys=Object.keys(x);
    if(keys.length!==Object.keys(y).length)return false;
    for(const k of keys){if(!own(y,k))return false;pending.push([x[k],y[k]]);}
  }
  return true;
}
function validateProfile(v) {
  if(!exact(v,['profile','inherited_media']))deny();
  const p=v.profile;
  if(!exact(p,['version','profile_id','native_protocol_version','sources','model','platform','limits','behavior'])||
    p.version!==1||p.profile_id!=='PI-SDK-MEDIA-2026-10-V1'||p.native_protocol_version!=='pi-sdk-standard-responses.v1')deny();
  const s=p.sources,m=p.model,l=p.limits,b=p.behavior;
  if(!exact(s,sourceNames)||s.pi_source_revision!=='6c4f360264397c59801f6da2bdac13e3b1fcbe91'||s.open_ai_sdk_version!=='6.40.0'||
    s.open_ai_sdk_integrity!=='sha512-MWtTjd/gQt4jpbji61NTgFWJLoY/PdRJ6wG9/ZDRMYNMlBKrCrSlkLI+KgHP1vR1qT6LKSAyAqIxno6lcK9JiA=='||
    typeof s.node_version!=='string'||s.node_version.length===0||s.node_version.length>128)deny();
  for(const k of sourceNames)if(k.endsWith('_digest')&&!digest(s[k]))deny();
  if(!exact(m,['model_vendor_id','model_id','endpoint','vision_metadata_digest','context_accounting_digest',
    'model_context_tokens','max_billed_input_tokens','max_output_tokens'])||!digest(m.vision_metadata_digest)||!digest(m.context_accounting_digest)||
    typeof m.model_vendor_id!=='string'||!m.model_vendor_id||typeof m.model_id!=='string'||!m.model_id||
    ![m.model_context_tokens,m.max_billed_input_tokens,m.max_output_tokens].every(positive)||
    m.max_billed_input_tokens>m.model_context_tokens-m.max_output_tokens||typeof m.endpoint!=='string'||m.endpoint.length>2048)deny();
  const url=new URL(m.endpoint);
  if(url.protocol!=='https:'||!url.hostname||url.username||url.password||url.search||url.hash||/[?#]/.test(m.endpoint)||url.href!==m.endpoint)deny();
  const platform=p.platform;
  if(!exact(platform,['os','architecture','runtime_policy_digest','isolation_policy_digest','egress_policy_digest','cleanup_policy_digest']))deny();
  for(const k of ['os','architecture'])if(typeof platform[k]!=='string'||!platform[k]||platform[k].length>128)deny();
  for(const k of Object.keys(platform))if(k.endsWith('_digest')&&!digest(platform[k]))deny();
  if(!exact(l,limitNames)||!Object.values(l).every(positive)||l.canonical_input_bytes>(1<<20)||l.process_frame_bytes>(1<<20)||
    l.process_stdout_bytes>(16<<20)||l.process_events>4096||l.json_depth>32||l.raw_final_bytes>(256<<10)||l.structured_final_bytes>(128<<10)||
    l.structured_final_bytes>l.raw_final_bytes||l.answer_bytes>(32<<10)||l.answer_bytes>l.structured_final_bytes||
    l.artifact_bytes>(32<<10)||l.artifact_total_bytes>(64<<10)||l.artifact_bytes>l.artifact_total_bytes/2||
    l.sse_line_bytes>l.sse_event_bytes||l.sse_event_bytes>l.response_body_bytes||l.response_chunk_bytes>l.response_body_bytes||
    l.native_request_bytes>l.pre_send_allocation_bytes||l.pre_send_allocation_bytes>l.process_memory_bytes||
    l.response_body_bytes>l.response_allocation_bytes||l.response_allocation_bytes>l.process_memory_bytes||
    l.response_body_bytes>l.response_copy_work_bytes)deny();
  if(!exact(b,['provider_api','image_derivation','image_detail','transport','local_tool_registry','max_physical_sends','sdk_retries',
    'provider_retries','session_retries','fallback_transport','native_compaction','discovery'])||b.provider_api!=='openai-responses'||
    b.image_derivation!=='NoTransformOriginal'||b.image_detail!=='auto'||b.transport!=='http-sse'||b.local_tool_registry!=='empty'||
    b.max_physical_sends!==1||b.sdk_retries!==0||b.provider_retries!==0||b.session_retries!==0||
    b.fallback_transport!==false||b.native_compaction!==false||b.discovery!==false)deny();
  const inherited={text_count:2,text_bytes:131072,text_total_bytes:262144,image_count:2,image_bytes:1048576,image_total_bytes:2097152,
    image_pixels:4000000,decoded_rgba_bytes:33554432,go_validation_memory_bytes:100663296,artifact_count:2,answer_runes:32000};
  if(!exact(v.inherited_media,Object.keys(inherited))||!equal(v.inherited_media,inherited))deny();
  return p;
}
function validateModel(m,p,maxOutput) {
  if(!closed(m,['id','name','api','provider','baseUrl','reasoning','input','cost','contextWindow','maxTokens'],['thinkingLevelMap','compat'])||
    m.api!=='openai-responses'||m.id!==p.model.model_id||typeof m.name!=='string'||!m.name||typeof m.provider!=='string'||!m.provider||
    typeof m.baseUrl!=='string'||m.baseUrl.replace(/\/$/,'')+'/responses'!==p.model.endpoint||typeof m.reasoning!=='boolean'||
    !Array.isArray(m.input)||m.input.length!==2||!m.input.includes('text')||!m.input.includes('image')||
    !positive(m.contextWindow)||m.contextWindow!==p.model.model_context_tokens||!positive(m.maxTokens)||m.maxTokens<maxOutput)deny();
  const rates=['input','output','cacheRead','cacheWrite'];
  const validRate=n=>typeof n==='number'&&Number.isFinite(n)&&n>=0;
  if(!closed(m.cost,rates,['tiers'])||!rates.every(k=>validRate(m.cost[k])))deny();
  if(own(m.cost,'tiers')){
    if(!Array.isArray(m.cost.tiers))deny();
    for(const tier of m.cost.tiers){if(!exact(tier,[...rates,'inputTokensAbove'])||!rates.every(k=>validRate(tier[k]))||
      !Number.isSafeInteger(tier.inputTokensAbove)||tier.inputTokensAbove<0)deny();}
  }
  if(own(m,'thinkingLevelMap')){
    if(!closed(m.thinkingLevelMap,[],['off','minimal','low','medium','high','xhigh']))deny();
    for(const v of Object.values(m.thinkingLevelMap))if(v!==null&&typeof v!=='string')deny();
  }
  if(own(m,'compat')){
    if(!closed(m.compat,[],compatNames))deny();
    for(const k of Object.keys(m.compat))if(k==='sessionAffinityFormat'?!['openai','openai-nosession','openrouter'].includes(m.compat[k]):typeof m.compat[k]!=='boolean')deny();
  }
}

// The channel already admitted the SPU2 header and UTF8 body envelope. Repeat
// UTF8 here for standalone/private use; no JSON.parse before strict full grammar
// and prospective source/decoded-payload/node reservations. No budget restarts.
export function decodePreparedInit(raw,limits,signal) {
  try {
    if(!(signal instanceof AbortSignal)||signal.aborted||!Buffer.isBuffer(raw)||!plain(limits)||!Object.isFrozen(limits)||
      Reflect.ownKeys(limits).length!==6)deny();
    for(const k of ['body_bytes','copy_bytes','json_nodes','json_depth','profile_bytes','request_bytes']){
      const d=Object.getOwnPropertyDescriptor(limits,k);if(!d||!own(d,'value')||!positive(d.value))deny();
    }
    if(raw.length===0||raw.length>limits.body_bytes||limits.body_bytes>(1<<20)-17||limits.json_depth>32||
      limits.profile_bytes>35008||limits.request_bytes>(1<<20)||!isUtf8(raw))deny();
    let copies=limits.copy_bytes,nodes=limits.json_nodes;
    const charge=n=>{if(!Number.isSafeInteger(n)||n<0||n>copies)deny();copies-=n;};
    const visit=n=>{if(!Number.isSafeInteger(n)||n<0||n>nodes)deny();nodes-=n;};
    const parse=(text,cap)=>{
      if(typeof text!=='string'||text.length>cap||!text.isWellFormed()||Buffer.byteLength(text)>cap)deny();
      const count=inspectJSONGrammar(text,{native_request_bytes:cap,json_depth:limits.json_depth},charge,nodes);
      visit(count);visit(count);charge(text.length*2); // parsed own keys/strings BEFORE parse
      const tree=JSON.parse(text);
      visit(count);freeze(tree); // owned plain JSON, not caller objects/getters
      visit(count); // complete closed semantic visit, including comparisons below
      return tree;
    };
    charge(raw.length*2);
    const init=parse(raw.toString('utf8'),limits.body_bytes);
    if(!exact(init,initNames)||init.version!==1||typeof init.profile_json!=='string'||!digest(init.profile_digest)||
      typeof init.system_prompt!=='string'||!init.system_prompt||!positive(init.max_output_tokens)||init.max_output_tokens<16||
      !positive(init.timeout_ms)||init.timeout_ms>2147483647||init.cache_retention!=='none'||
      (init.reasoning_effort!==null&&typeof init.reasoning_effort!=='string')||
      (init.reasoning_summary!==null&&typeof init.reasoning_summary!=='string')||
      (init.reasoning_summary!==null&&init.reasoning_effort===null)||
      (init.temperature!==null&&!Number.isSafeInteger(init.temperature))||
      typeof init.schema_name!=='string'||!/^[A-Za-z0-9_-]{1,64}$/.test(init.schema_name)||!digest(init.schema_source_digest)||!plain(init.schema)||
      typeof init.expected_body!=='string'||!positive(init.credential_max_bytes)||typeof init.credential_file!=='string'||!isAbsolute(init.credential_file)||
      normalize(init.credential_file)!==init.credential_file||!basename(init.credential_file)||init.credential_file.includes('\0'))deny();
    const p=validateProfile(parse(init.profile_json,limits.profile_bytes));
    charge(Buffer.byteLength(init.profile_json));
    const hash=createHash('sha256').update('sessionless.attached-pi-sdk-native-profile.v1\0').update(init.profile_json).digest('hex');
    if(hash!==init.profile_digest||init.max_output_tokens>p.model.max_output_tokens||init.schema_source_digest!==p.sources.final_schema_digest||
      raw.length+17>p.limits.process_frame_bytes||init.credential_max_bytes>p.limits.request_header_bytes||Buffer.byteLength(init.expected_body)>p.limits.native_request_bytes)deny();
    validateModel(init.model,p,init.max_output_tokens);
    if(!Array.isArray(init.headers)||init.headers.length!==11)deny();
    for(let i=0;i<11;i++){
      const h=init.headers[i];if(!Array.isArray(h)||h.length!==2||h[0]!==headerNames[i]||typeof h[1]!=='string'||!h[1]||
        h[1].trim()!==h[1]||/[\x00-\x1f\x7f]/.test(h[1]))deny();
    }
    if(init.headers[0][1]!=='application/json'||init.headers[1][1]!=='application/json'||init.headers[3][1]!=='js'||
      init.headers[4][1]!=='6.40.0'||init.headers[7][1]!=='node'||init.headers[9][1]!=='0'||init.headers[10][1]!==String(init.timeout_ms/1000))deny();
    const request=parse(init.expected_body,limits.request_bytes);
    if(!closed(request,['model','input','stream','store','max_output_tokens','tool_choice','truncation','text'],
      ['prompt_cache_options','temperature','reasoning','include'])||request.model!==init.model.id||request.stream!==true||request.store!==false||
      request.max_output_tokens!==init.max_output_tokens||request.tool_choice!=='none'||request.truncation!=='disabled'||
      !exact(request.text,['format'])||!exact(request.text.format,['type','name','schema','strict'])||request.text.format.type!=='json_schema'||
      request.text.format.name!==init.schema_name||request.text.format.strict!==true||!equal(request.text.format.schema,init.schema))deny();
    if(!Array.isArray(init.messages)||init.messages.length!==1||!exact(init.messages[0],['role','content','timestamp'])||
      init.messages[0].role!=='user'||!Number.isSafeInteger(init.messages[0].timestamp)||init.messages[0].timestamp<0||
      !Array.isArray(init.messages[0].content)||init.messages[0].content.length===0||!Array.isArray(request.input)||request.input.length!==2)deny();
    const supportsDeveloper=init.model.compat?.supportsDeveloperRole??true;
    if(!exact(request.input[0],['role','content'])||request.input[0].content!==init.system_prompt||
      request.input[0].role!==(init.model.reasoning&&supportsDeveloper?'developer':'system')||!exact(request.input[1],['role','content'])||
      request.input[1].role!=='user'||!Array.isArray(request.input[1].content)||request.input[1].content.length!==init.messages[0].content.length)deny();
    let imageCount=0;
    for(let i=0;i<init.messages[0].content.length;i++){
      const b=init.messages[0].content[i],actual=request.input[1].content[i];
      if(b.type==='text'){
        if(!exact(b,['type','text'])||typeof b.text!=='string'||!exact(actual,['type','text'])||actual.type!=='input_text'||actual.text!==b.text)deny();
      }else if(b.type==='image'){
        if(++imageCount>2||!exact(b,['type','mimeType','data'])||!['image/png','image/jpeg'].includes(b.mimeType)||typeof b.data!=='string'||
          !/^[A-Za-z0-9+/]+={0,2}$/.test(b.data)||b.data.length%4!==0||!exact(actual,['type','detail','image_url'])||
          actual.type!=='input_image'||actual.detail!=='auto'||typeof actual.image_url!=='string')deny();
        const prefix='data:'+b.mimeType+';base64,';
        if(!actual.image_url.startsWith(prefix)||actual.image_url.length!==prefix.length+b.data.length)deny();
        for(let j=0;j<b.data.length;j++)if(b.data.charCodeAt(j)!==actual.image_url.charCodeAt(prefix.length+j))deny();
      }else deny();
    }
    signal.throwIfAborted();
    // These are independently parsed/owned JSON DATA. No key, lifecycle port,
    // SDK closure, authority Boolean or ambient HOME/catalog has been acquired.
    return Object.freeze({init,profile:p,request,remaining:Object.freeze({copy_bytes:copies,json_nodes:nodes})});
  }catch{deny();}
}
