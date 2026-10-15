// Credential-free parity against the EXISTING native framer, not Pi/SDK or
// a serving qualification. No dependency install, network or configuration.
import {createSSEFramer} from '../../pisdktransport/framing.mjs';
let input='';
for await (const chunk of process.stdin) {
  input+=chunk;
  if (input.length>65536) throw new Error('fixture input bound');
}
const cases=JSON.parse(input);
if(!Array.isArray(cases)||cases.length>64)throw new Error('fixture case bound');
const results=[];
for(const value of cases){
  const body=new TextEncoder().encode(value);
  if(body.length>4096)throw new Error('fixture body bound');
  const outcomes=[];
  for(const chunk of [1,3,128]){
    const framer=createSSEFramer(Object.freeze({response_body_bytes:4096,response_chunk_bytes:128,
      sse_line_bytes:1024,sse_event_bytes:2048,sse_events:32,response_copy_work_bytes:1048576,response_allocation_bytes:1048576}));
    let frames=0,ok=true;
    try{for(let at=0;at<body.length;at+=chunk)for(const frame of framer.feed(body.subarray(at,at+chunk))){if(!frame.length)throw new Error('empty frame');frames++;}framer.finish();}
    catch{ok=false;framer.close();}
    outcomes.push({ok,frames});
  }
  results.push(outcomes);
}
process.stdout.write(JSON.stringify(results));
