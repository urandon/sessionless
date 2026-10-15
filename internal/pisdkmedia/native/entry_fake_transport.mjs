// NEW actual entry/SPU2/genuine SDK functional sink. Explicit public literal;
// no SDK factory, model client, socket, provider, environment credential or
// native profile proof. Only the HTTP sink is fake; real entry owns all else.
import assert from 'node:assert/strict';
import { stdin,stdout,cwd,argv } from 'node:process';
import { runPreparedPiStdio,parsePreparedEntryArguments } from './entry.mjs';
const literal='PUBLIC-NONCREDENTIAL-INIT-CONTROL';
const final='{"answer":"native entry answer","artifacts":[]}';
let sends=0;
const physicalFetch=async(url,init)=>{
 assert.equal(++sends,1);assert.equal(init.method,'POST');assert.equal(init.redirect,'error');
 assert.equal(init.headers.get('authorization'),'Bearer '+literal);
 const req=JSON.parse(init.body);assert.equal(url,'https://synthetic.invalid/responses');
 const input=req.input[1].content,images=input.filter(v=>v.type==='input_image');assert.equal(images.length,2);
 const text=input.filter(v=>v.type==='input_text').map(v=>v.text).join('');
 for(const marker of ['historic assistant summary','newest sentinel','90071992547409931234567890123','same.json','same.png'])assert.ok(text.includes(marker));
 const part={type:'output_text',text:final,annotations:[]},item={id:'msg-entry',type:'message',role:'assistant',status:'completed',content:[part]};
 const response=status=>({id:'resp-entry',object:'response',created_at:0,status,error:null,incomplete_details:null,instructions:null,metadata:{},
  model:req.model,output:status==='completed'?[item]:[],parallel_tool_calls:true,temperature:1,tool_choice:'none',tools:[],top_p:1,store:false,
  max_output_tokens:req.max_output_tokens,truncation:'disabled',text:req.text,usage:status==='completed'?{input_tokens:1,output_tokens:1,total_tokens:2,
  input_tokens_details:{cached_tokens:0},output_tokens_details:{reasoning_tokens:0}}:null});
 const events=[{type:'response.created',sequence_number:0,response:response('in_progress')},
 {type:'response.output_item.added',sequence_number:1,output_index:0,item:{...item,status:'in_progress',content:[]}},
 {type:'response.content_part.added',sequence_number:2,output_index:0,content_index:0,item_id:item.id,part:{...part,text:''}},
 {type:'response.output_text.delta',sequence_number:3,output_index:0,content_index:0,item_id:item.id,delta:final,logprobs:[],obfuscation:'PUBLIC'},
 {type:'response.output_text.done',sequence_number:4,output_index:0,content_index:0,item_id:item.id,text:final,logprobs:[]},
 {type:'response.content_part.done',sequence_number:5,output_index:0,content_index:0,item_id:item.id,part},
 {type:'response.output_item.done',sequence_number:6,output_index:0,item},
 {type:'response.completed',sequence_number:7,response:response('completed')}];
 const bytes=new TextEncoder().encode(events.map(v=>'event: '+v.type+'\ndata: '+JSON.stringify(v)+'\n\n').join(''));let at=0;
 return new Response(new ReadableStream({pull(c){if(at===bytes.length){c.close();return;}c.enqueue(bytes.slice(at,at+1));at++;}},{highWaterMark:0}),
  {status:200,headers:{'content-type':'text/event-stream'}});
};
try{
 const limits=parsePreparedEntryArguments(argv.slice(2));
 await runPreparedPiStdio({input:stdin,output:stdout,signal:new AbortController().signal,...limits,physicalFetch,workDir:cwd()});
 assert.equal(sends,1);
}catch(error){if(/^pi_prepared_entry_[a-z-]+_denied$/.test(error.message))process.stderr.write(error.message);process.exitCode=71;}
