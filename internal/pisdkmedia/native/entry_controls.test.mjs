import assert from 'node:assert/strict';
import { Buffer } from 'node:buffer';
import { chmod, mkdir, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { argv } from 'node:process';
import { PassThrough, Writable } from 'node:stream';
import { test } from 'node:test';
import { parsePreparedEntryArguments, runPreparedPiStdio } from './entry.mjs';
import { readPreparedCredentialFile } from './credential_file.mjs';

// This test runs without an SDK tree or account files in its staged namespace.
// Importing the actual entry is itself a causal control for delayed SDK import.
const root=argv[2], literal='PUBLIC-FILE-ONLY-CONTROL';
assert.ok(root?.startsWith('/'));
const args=['512','1024','512','2048','512','4096','1024','32','512','512'];
test('closed pre-Issue decimal arguments, no graph/path/secret selectors',()=>{
 const got=parsePreparedEntryArguments(args);
 assert.equal(got.channelLimits.frame_bytes,2048);assert.equal(got.decodeLimits.json_nodes,1024);
 assert.ok(Object.isFrozen(got)&&Object.isFrozen(got.channelLimits)&&Object.isFrozen(got.decodeLimits));
 for(const values of [args.slice(1),[...args,'1'],...['0','01','-1','1.0','1e3','Infinity','9007199254740992',1,null,'x'.repeat(100)].map(v=>[v,...args.slice(1)])])
  assert.throws(()=>parsePreparedEntryArguments(values),{message:'pi_prepared_entry_denied'});
});

test('invalid complete Init fails before file/SDK/HTTP effects, joins owned IO',{timeout:10000},async()=>{
 const input=new PassThrough();let writes=0,sends=0;
 const output=new Writable({write(_chunk,_encoding,done){writes++;done();}});
 const h=Buffer.alloc(17);h.write('SPU2');h[4]=1;h.writeBigUInt64BE(2n,9);
 const result=runPreparedPiStdio({input,output,signal:new AbortController().signal,...parsePreparedEntryArguments(args),
  physicalFetch:()=>{sends++;throw new Error('unexpected HTTP');},workDir:root});
 input.end(Buffer.concat([h,Buffer.from('{}')]));
 await assert.rejects(result,{message:'pi_prepared_entry_initialize_denied'});
 assert.equal(sends,0);assert.equal(writes,0);
 for(const stream of [input,output])for(const event of ['error','close','readable','end'])
  assert.equal(stream.listenerCount(event),0,`remaining owned ${event} listener`);
});

test('fixed file reader exact bytes and bounded denials, public literal only',{timeout:10000},async()=>{
 const signal=new AbortController().signal;
 const file=join(root,'valid');await writeFile(file,literal,{mode:0o600});
 assert.equal(await readPreparedCredentialFile(file,literal.length,signal),literal);
 await assert.rejects(readPreparedCredentialFile(file,literal.length-1,signal),{message:'pi_credential_file_denied'});
 const cases=[['empty','',0o600],['newline',literal+'\n',0o600],['space','a b',0o600],
  ['nul',Buffer.from([65,0,66]),0o600],['high-bit',Buffer.from([65,0xe9]),0o600],['public-mode',literal,0o644]];
 for(const [name,body,mode] of cases){const path=join(root,name);await writeFile(path,body,{mode});await chmod(path,mode);
  await assert.rejects(readPreparedCredentialFile(path,128,signal),{message:'pi_credential_file_denied'});}
 const soft=join(root,'soft');
 await assert.rejects(readPreparedCredentialFile(soft,128,signal),{message:'pi_credential_file_denied'});
 for(const path of [join(root,'hard'),join(root,'link-target')])
  await assert.rejects(readPreparedCredentialFile(path,128,signal),{message:'pi_credential_file_denied'});
 const directory=join(root,'directory');await mkdir(directory,{mode:0o700});
 await assert.rejects(readPreparedCredentialFile(directory,128,signal),{message:'pi_credential_file_denied'});
 const aborted=new AbortController();aborted.abort();
 for(const [path,cap,sig] of [[file,128,aborted.signal],['relative',128,signal],[root+'/directory/../valid',128,signal],
  [join(root,'missing'),128,signal],[file,0,signal],[file,65537,signal],[file,128,null]])
  await assert.rejects(readPreparedCredentialFile(path,cap,sig),{message:'pi_credential_file_denied'});
});
