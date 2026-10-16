// Separate private SPU3 entry. It imports no SDK until full Init and the fixed
// file reader succeed. No SPU2 alias, native/global fetch or Source client.
import process,{stdin,stdout,argv,cwd} from 'node:process';
import {join,isAbsolute,normalize} from 'node:path';
import {pathToFileURL} from 'node:url';
import {createPiHTTPStdioChannel} from '../../pisdktransport/http_stdio.mjs';
import {decodePreparedInit} from './init.mjs';
import {readPreparedCredentialFile} from './credential_file.mjs';
import {runPreparedPiHTTP} from './bootstrap.mjs';

const denied=()=>new Error('pi_http_entry_denied');
const channelNames=['frame_bytes','initialize_body_bytes','request_body_bytes','final_body_bytes',
 'request_header_bytes','response_header_bytes','response_body_bytes','response_chunk_bytes','parent_bytes','child_bytes','frames'];
const decodeNames=['body_bytes','copy_bytes','json_nodes','json_depth','profile_bytes','request_bytes'];

// Exactly seventeen new SPU3 source-owned decimal arguments. NEVER reinterpret
// the old ten-argument SPU2 entry/artifact. Original producer/native fit still
// must prepare/fund and bind these values before Issue; parsing grants nothing.
export function parsePreparedHTTPArguments(values){
 if(!Array.isArray(values)||values.length!==17)throw denied();
 const channel=Object.create(null),decode=Object.create(null);
 for(let i=0;i<17;i++){
  const text=values[i];if(typeof text!=='string'||text.length>16||!/^[1-9][0-9]*$/.test(text))throw denied();
  const n=Number(text);if(!Number.isSafeInteger(n)||n<1)throw denied();
  if(i<11)channel[channelNames[i]]=n;else decode[decodeNames[i-11]]=n;
 }
 return Object.freeze({channelLimits:Object.freeze(channel),decodeLimits:Object.freeze(decode)});
}

export async function runPreparedPiHTTPStdio({input,output,signal,channelLimits,decodeLimits,workDir}){
 let channel,key,boundary,stage='configuration';
 try{
  if(typeof workDir!=='string'||!isAbsolute(workDir)||normalize(workDir)!==workDir||workDir.includes('\0'))throw denied();
  channel=createPiHTTPStdioChannel({input,output,limits:channelLimits,signal});
  stage='initialize';const {body:raw,profileDigest}=await channel.initialize();
  const {init,profile,request}=decodePreparedInit(raw,decodeLimits,channel.signal),l=profile.limits;
  if(profileDigest!==init.profile_digest||channelLimits.frame_bytes!==l.process_frame_bytes||channelLimits.initialize_body_bytes!==l.process_frame_bytes-113||
    channelLimits.request_body_bytes!==Math.min(l.native_request_bytes,l.process_frame_bytes-85-l.request_header_bytes)||
    channelLimits.final_body_bytes!==Math.min(l.structured_final_bytes,l.raw_final_bytes,l.process_frame_bytes-17)||
    channelLimits.request_header_bytes!==l.request_header_bytes||channelLimits.response_header_bytes!==l.response_header_bytes||
    channelLimits.response_body_bytes!==l.response_body_bytes||channelLimits.response_chunk_bytes!==l.response_chunk_bytes||
    channelLimits.parent_bytes!==4<<20||channelLimits.child_bytes!==l.process_stdout_bytes||channelLimits.frames!==l.process_events||
    decodeLimits.body_bytes!==channelLimits.initialize_body_bytes)throw denied();
  stage='credential-file';key=await readPreparedCredentialFile(init.credential_file,init.credential_max_bytes,channel.signal);
  const headers=Object.freeze([...init.headers,Object.freeze(['authorization','Bearer '+key])]);
  boundary=channel.bindRequest(Object.freeze({endpoint:profile.model.endpoint,profileDigest:init.profile_digest,headers,body:init.expected_body}));
  const admittedRequest=Object.freeze({endpoint:profile.model.endpoint,method:'POST',headers,body:init.expected_body,request,schema:init.schema});
  const sdkProfile=Object.freeze({model:init.model,systemPrompt:init.system_prompt,maxOutputTokens:init.max_output_tokens,
   timeoutMs:init.timeout_ms,cacheRetention:init.cache_retention,limits:l,
   requestContractLimits:Object.freeze({native_request_bytes:l.native_request_bytes,json_nodes:l.json_nodes,json_depth:l.json_depth}),
   admittedRequest,temperature:init.temperature===null?undefined:init.temperature,
   reasoningEffort:init.reasoning_effort===null?undefined:init.reasoning_effort,
   reasoningSummary:init.reasoning_summary===null?undefined:init.reasoning_summary});
  stage='sdk-bootstrap';const final=await runPreparedPiHTTP({profile:sdkProfile,messages:init.messages,cwd:workDir,
   agentDir:join(workDir,'.pi-memory-only'),apiKey:key,signal:channel.signal,physicalFetch:boundary.fetch});
  stage='final';await channel.publishFinal(final);
 }catch{throw new Error('pi_http_entry_'+stage+'_denied');}
 finally{key=undefined;await channel?.dispose();}
}

if(argv[1]&&import.meta.url===pathToFileURL(argv[1]).href){
 const original=new AbortController(),stop=()=>original.abort();process.on('SIGTERM',stop);process.on('SIGINT',stop);
 try{await runPreparedPiHTTPStdio({input:stdin,output:stdout,signal:original.signal,...parsePreparedHTTPArguments(argv.slice(2)),workDir:cwd()});}
 catch(error){if(/^pi_http_entry_[a-z-]+_denied$/.test(error.message))process.stderr.write(error.message);process.exitCode=71;}
 finally{process.off('SIGTERM',stop);process.off('SIGINT',stop);}
}
