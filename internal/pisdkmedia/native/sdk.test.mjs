// NEW genuine pinned Pi/ModelRuntime/standard Responses/SDK functional test.
// Full canonical DATA compiled by Go; no live model, credential, Source grant.
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { getPlatformHeaders } from '../pi/node_modules/openai/internal/detect-platform.mjs';
import { getPiUserAgent } from '../pi/packages/ai/dist/utils/pi-user-agent.js';
import { runPreparedPi } from './bootstrap.mjs';
const freeze=v=>{if(v&&typeof v==='object'){for(const child of Object.values(v))freeze(child);Object.freeze(v);}return v;};
const raw=readFileSync(new URL('./functional-fixture.json',import.meta.url));
assert.ok(raw.length<=1<<20);
const fixture=JSON.parse(raw),request=freeze(JSON.parse(fixture.body));
const apiKey='PUBLIC-NONCREDENTIAL-FUNCTIONAL-LITERAL',original=new AbortController(),use=new AbortController();
let authorizations=0,sends=0,cancels=0;
const headers=new Headers({accept:'application/json','content-type':'application/json','user-agent':getPiUserAgent(),
 'x-stainless-retry-count':'0','x-stainless-timeout':String(fixture.timeoutMs/1000),...getPlatformHeaders(),authorization:'Bearer '+apiKey});
const pairs=freeze([...headers]),messages=freeze(fixture.messages),model=freeze(fixture.model);
const admitted=freeze({endpoint:fixture.endpoint,method:'POST',headers:pairs,body:fixture.body,request,schema:request.text.format.schema});
const profile=freeze({model,systemPrompt:fixture.systemPrompt,maxOutputTokens:request.max_output_tokens,
 timeoutMs:fixture.timeoutMs,cacheRetention:'none',limits:fixture.limits,requestContractLimits:{
 native_request_bytes:fixture.limits.native_request_bytes,json_nodes:fixture.limits.json_nodes,json_depth:fixture.limits.json_depth},admittedRequest:admitted});
assert.equal(messages.length,1);assert.equal(messages[0].role,'user');
const originals=messages[0].content.filter(v=>v.type==='image');assert.equal(originals.length,2);
const text=messages[0].content.filter(v=>v.type==='text').map(v=>v.text).join('');
for(const marker of ['90071992547409931234567890123','historic assistant summary','newest sentinel','same.json','same.png'])assert.ok(text.includes(marker),marker);
const final=JSON.stringify({answer:'functional native answer',artifacts:[{name:'data.json',media_type:'application/json',content:' {"n":900719925474099312345,"e":1e+100} '}]});
const part={type:'output_text',text:final,annotations:[]},item={id:'msg-functional',type:'message',role:'assistant',status:'completed',content:[part]};
const response=status=>({id:'resp-functional',object:'response',created_at:0,status,error:null,incomplete_details:null,
 instructions:null,metadata:{},model:request.model,output:status==='completed'?[item]:[],parallel_tool_calls:true,
 temperature:1,tool_choice:'none',tools:[],top_p:1,store:false,max_output_tokens:request.max_output_tokens,
 truncation:'disabled',text:request.text,usage:status==='completed'?{input_tokens:1,output_tokens:1,total_tokens:2,input_tokens_details:{cached_tokens:0},output_tokens_details:{reasoning_tokens:0}}:null});
const events=[
 {type:'response.created',sequence_number:0,response:response('in_progress')},
 {type:'response.output_item.added',sequence_number:1,output_index:0,item:{...item,status:'in_progress',content:[]}},
 {type:'response.content_part.added',sequence_number:2,output_index:0,content_index:0,item_id:item.id,part:{...part,text:''}},
 {type:'response.output_text.delta',sequence_number:3,output_index:0,content_index:0,item_id:item.id,delta:final,logprobs:[],obfuscation:'PUBLIC'},
 {type:'response.output_text.done',sequence_number:4,output_index:0,content_index:0,item_id:item.id,text:final,logprobs:[]},
 {type:'response.content_part.done',sequence_number:5,output_index:0,content_index:0,item_id:item.id,part},
 {type:'response.output_item.done',sequence_number:6,output_index:0,item},
 {type:'response.completed',sequence_number:7,response:response('completed')}];
const bytes=new TextEncoder().encode(events.map(v=>'event: '+v.type+'\ndata: '+JSON.stringify(v)+'\n\n').join(''));let at=0;
const parentUse=Object.freeze({authorizePhysicalSend(...args){authorizations++;assert.equal(args.length,0);
 return Promise.resolve(Object.freeze({signal:use.signal,assertUsable(){assert.equal(use.signal.aborted,false);}}));}});
const physicalFetch=async(url,init)=>{
 sends++;assert.equal(url,fixture.endpoint);assert.equal(init.method,'POST');assert.equal(init.redirect,'error');
 assert.equal(init.body,fixture.body,'actual SDK bytes equal the Go complete original emission');assert.deepEqual([...init.headers],pairs);
 assert.notEqual(init.signal,original.signal);
 const sent=JSON.parse(init.body),images=sent.input.flatMap(m=>Array.isArray(m.content)?m.content:[]).filter(v=>v.type==='input_image');
 assert.equal(images.length,originals.length);
 for(let i=0;i<images.length;i++){const prefix='data:'+originals[i].mimeType+';base64,';assert.ok(images[i].image_url.startsWith(prefix));
 assert.equal(images[i].image_url.slice(prefix.length),originals[i].data);assert.equal(images[i].detail,'auto');}
 return new Response(new ReadableStream({pull(c){if(at===bytes.length){c.close();return;}c.enqueue(bytes.slice(at,at+1));at++;},cancel(){cancels++;}},{highWaterMark:0}),
 {status:200,headers:{'content-type':'text/event-stream'}});
};
const input={profile,messages,cwd:fixture.cwd,agentDir:fixture.agentDir,apiKey,signal:original.signal,parentUse,physicalFetch};
assert.equal(await runPreparedPi(input),final);assert.equal(sends,1);assert.equal(authorizations,1);
await assert.rejects(runPreparedPi(input),{message:'prepared Pi invocation denied'});
assert.equal(sends,1);assert.ok(cancels<=1);assert.equal(final.includes(apiKey),false);
console.log('NEW pinned Pi full canonical/native fake-transport functional PASS');
