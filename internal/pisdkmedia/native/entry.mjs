// Private SPU2 entry. SDK-free imports only; complete closed Init precedes the
// fixed file reader and delayed genuine Pi construction. No Source client,
// second runner, caller-selected module, extension or alternate model client.
import process, { stdin, stdout, argv, cwd } from 'node:process';
import { join, isAbsolute, normalize } from 'node:path';
import { pathToFileURL } from 'node:url';
import { createPiStdioChannel } from '../../pisdktransport/stdio.mjs';
import { decodePreparedInit } from './init.mjs';
import { readPreparedCredentialFile } from './credential_file.mjs';
import { runPreparedPi } from './bootstrap.mjs';

const denied = () => new Error('pi_prepared_entry_denied');
const numericNames=['initialize_body_bytes','request_body_bytes','final_body_bytes','frame_bytes',
 'body_bytes','copy_bytes','json_nodes','json_depth','profile_bytes','request_bytes'];

// Exactly ten pre-Issue source-owned decimal arguments, no generic JSON graph,
// user input, secret, profile/Source selector or refreshed model deadline.
export function parsePreparedEntryArguments(values) {
  if(!Array.isArray(values)||values.length!==10)throw denied();
  const channel=Object.create(null),decode=Object.create(null);
  for(let i=0;i<values.length;i++){
    const text=values[i];
    if(typeof text!=='string'||text.length>16||!/^[1-9][0-9]*$/.test(text))throw denied();
    const n=Number(text);if(!Number.isSafeInteger(n)||n<1)throw denied();
    (i<4?channel:decode)[numericNames[i]]=n;
  }
  return Object.freeze({channelLimits:Object.freeze(channel),decodeLimits:Object.freeze(decode)});
}

// physicalFetch is a fixed trusted transport (CLI captures native fetch).
// Tests may supply a credential-free fake sink, NEVER an SDK factory/import.
// The original complete producer must reserve ALL these payload appearances
// and native/SDK/platform costs before Issue; this code supplies no fit grant.
export async function runPreparedPiStdio({input,output,signal,channelLimits,decodeLimits,physicalFetch,workDir}) {
  let channel,key,stage='configuration';
  try {
    if(typeof physicalFetch!=='function'||typeof workDir!=='string'||!isAbsolute(workDir)||normalize(workDir)!==workDir||workDir.includes('\0'))throw denied();
    channel=createPiStdioChannel({input,output,limits:channelLimits,signal});
    stage='initialize';
    const raw=await channel.initialize();
    const {init,profile,request}=decodePreparedInit(raw,decodeLimits,channel.signal);
    const l=profile.limits;
    if(channelLimits.frame_bytes!==l.process_frame_bytes||channelLimits.initialize_body_bytes!==l.process_frame_bytes-17||
      channelLimits.request_body_bytes!==Math.min(l.native_request_bytes,l.process_frame_bytes-146)||
      channelLimits.final_body_bytes!==Math.min(l.structured_final_bytes,l.raw_final_bytes,l.process_frame_bytes-17))throw denied();
    stage='credential-file';
    key=await readPreparedCredentialFile(init.credential_file,init.credential_max_bytes,channel.signal);
    const headers=Object.freeze([...init.headers,Object.freeze(['authorization','Bearer '+key])]);
    const admittedRequest=Object.freeze({endpoint:profile.model.endpoint,method:'POST',headers,body:init.expected_body,request,schema:init.schema});
    const sdkProfile=Object.freeze({model:init.model,systemPrompt:init.system_prompt,maxOutputTokens:init.max_output_tokens,
      timeoutMs:init.timeout_ms,cacheRetention:init.cache_retention,limits:l,
      requestContractLimits:Object.freeze({native_request_bytes:l.native_request_bytes,json_nodes:l.json_nodes,json_depth:l.json_depth}),
      admittedRequest,temperature:init.temperature===null?undefined:init.temperature,
      reasoningEffort:init.reasoning_effort===null?undefined:init.reasoning_effort,
      reasoningSummary:init.reasoning_summary===null?undefined:init.reasoning_summary});
    const parentUse=channel.bindRequest(init.expected_body,init.profile_digest);
    stage='sdk-bootstrap';
    const final=await runPreparedPi({profile:sdkProfile,messages:init.messages,cwd:workDir,agentDir:join(workDir,'.pi-memory-only'),
      apiKey:key,signal:channel.signal,parentUse,physicalFetch});
    stage='final';await channel.publishFinal(final);
  }catch{throw new Error('pi_prepared_entry_'+stage+'_denied');}
  finally{key=undefined;await channel?.dispose();}
}

if(argv[1]&&import.meta.url===pathToFileURL(argv[1]).href){
  const original=new AbortController();
  const stop=()=>original.abort();
  process.on('SIGTERM',stop);process.on('SIGINT',stop);
  try{
    const limits=parsePreparedEntryArguments(argv.slice(2));
    const nativeFetch=globalThis.fetch.bind(globalThis);
    await runPreparedPiStdio({input:stdin,output:stdout,signal:original.signal,...limits,physicalFetch:nativeFetch,workDir:cwd()});
  }catch{process.exitCode=71;} // fixed exit only, no raw diagnostics/key/task
  finally{process.off('SIGTERM',stop);process.off('SIGINT',stop);}
}
