// Bounded credential-free parity with the existing native closed policy and
// framer. No SDK/client, credentials, settings, provider or network discovery.
import {createResponsesFramePolicy} from '../../pisdktransport/policy.mjs';
import {createSSEFramer} from '../../pisdktransport/framing.mjs';
let input='';
for await(const chunk of process.stdin){input+=chunk;if(input.length>65536)throw new Error('fixture input bound');}
const cases=JSON.parse(input);
if(!Array.isArray(cases)||cases.length>32)throw new Error('fixture case bound');
const freeze=(v)=>{if(v&&typeof v==='object'){for(const c of Object.values(v))freeze(c);Object.freeze(v);}return v;};
const limits=freeze({json_nodes:2048,json_depth:8,sse_events:64,sse_line_bytes:8192,sse_event_bytes:8192,
  response_body_bytes:16384,response_chunk_bytes:128,response_copy_work_bytes:8388608,response_allocation_bytes:8388608});
const names=['model','max_output_tokens','store','truncation','tool_choice','instructions','metadata','text','reasoning',
  'temperature','top_p','parallel_tool_calls','prompt_cache_key','prompt_cache_retention','safety_identifier','user','service_tier'];
const results=[];
for(const value of cases){
  if(typeof value.request!=='string'||value.request.length>4096||typeof value.body!=='string'||value.body.length>16384)throw new Error('fixture value bound');
  const request=JSON.parse(value.request);
  const expected=freeze(Object.fromEntries(names.filter(n=>Object.hasOwn(request,n)).map(n=>[n,request[n]])));
  const body=new TextEncoder().encode(value.body);
  const outcomes=[];
  for(const size of [1,128]){
    const framer=createSSEFramer(limits);
    const policy=createResponsesFramePolicy({limits,expectedRequest:expected});
    let terminal=false,ok=true;
    try{
      for(let at=0;at<body.length;at+=size){
        for(const frame of framer.feed(body.subarray(at,at+size))){
          const verdict=policy(frame);
          if(verdict==='deny')throw new Error('deny');
          if(verdict==='terminal')terminal=true;
        }
      }
      framer.finish();
      if(!terminal)throw new Error('missing terminal');
    }catch{ok=false;}finally{framer.close();}
    outcomes.push(ok);
  }
  results.push(outcomes);
}
process.stdout.write(JSON.stringify(results));
