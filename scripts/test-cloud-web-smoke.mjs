import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, mkdirSync, writeFileSync, readFileSync, readdirSync, rmSync, symlinkSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { spawnSync } from 'node:child_process';

const script = join(dirname(fileURLToPath(import.meta.url)), 'cloud-web-smoke.sh');
const check = join(dirname(script), 'cloud-web-login-check.mjs');
const state = 'STATE_LEAK_MARKER', nonce = 'NONCE_LEAK_MARKER', token = 'IAM_LEAK_MARKER';
const web = 'https://web.dev.example.invalid', container = 'https://container.example.invalid';
const environment = provider => ({ CLOUD_WEB_URL: web, WEB_CONTAINER_URL: container, WEB_IMAGE_REF: `cr.yandex/registry/web-bff@sha256:${'a'.repeat(64)}`, WEB_PREPARED_INSTANCES:'0', WEB_CONCURRENCY:'1', WEB_LOGIN_PROVIDER:provider, YANDEX_LOGIN_CLIENT_ID:'yandex-client', TELEGRAM_OIDC_CLIENT_ID:'telegram-client', WEB_COLD_START_WAIT_SECONDS:'0' });
function authorization(provider) {
  const u = new URL(provider === 'yandex' ? 'https://oauth.yandex.ru/authorize' : 'https://oauth.telegram.org/auth');
  const values = { client_id:`${provider}-client`, redirect_uri:web+(provider === 'yandex' ? '/auth/login/callback' : '/auth/telegram/callback'), response_type:'code', scope:provider === 'yandex' ? 'login:info' : 'openid profile', state, code_challenge:Buffer.alloc(32,7).toString('base64url'), code_challenge_method:'S256' };
  if (provider === 'telegram') values.nonce = nonce;
  for (const [k,v] of Object.entries(values)) u.searchParams.set(k,v);
  return u;
}
const headers = (status, entries) => `HTTP/2 ${status}\r\n${entries.map(([k,v])=>`${k}: ${v}`).join('\r\n')}\r\n\r\n`;
function fixture(provider) {
  return { login:headers(303,[['Cache-Control','no-store'],['Location',authorization(provider).href],['Set-Cookie',`login=${state}; Secure; HttpOnly`]]), root:headers(200,[['Strict-Transport-Security','max-age=31536000; includeSubDomains'],['X-Content-Type-Options','nosniff'],['Referrer-Policy','strict-origin-when-cross-origin'],['Cache-Control','no-store'],['Content-Security-Policy',"default-src 'self'; frame-ancestors 'none'"]]), me:headers(401,[['Cache-Control','no-store']]), version:'{"component":"web-bff"}', anonymous:'403', forbidden:'404' };
}
const fakeCurl = `#!/usr/bin/env node
const fs=require('node:fs');const a=process.argv.slice(2), f=JSON.parse(fs.readFileSync(process.env.FIXTURE,'utf8'));const entry={client:'curl',argv:a};
const option=k=>{let i=a.indexOf(k);return i<0?undefined:a[i+1]};const u=new URL(a[a.length-1]);
if(option('--config')) {entry.configMode=fs.statSync(option('--config')).mode&511;entry.directoryMode=fs.statSync(require('node:path').dirname(option('--config'))).mode&511;entry.privateAuthorization=fs.readFileSync(option('--config'),'utf8').includes('Authorization: Bearer IAM_LEAK_MARKER');}
fs.appendFileSync(process.env.TRACE,JSON.stringify(entry)+'\\n');
if(f.curlFailure&&u.pathname===f.curlFailure){console.error('ACCESS_LEAK_MARKER '+f.login);process.exit(7)}
let body='ok',h='',status='200';
if(u.host==='container.example.invalid')status=option('--config')?(f.privateStatus||'200'):f.anonymous;
else if(u.pathname==='/version')body=f.version;
else if(u.pathname==='/'){h=f.root;body='<html></html>'}
else if(u.pathname==='/api/web/v1/me'){h=f.me;status='401'}
else if(u.pathname==='/auth/login/start'){h=f.login;status='303'}
else if(u.pathname==='/telegram/webhook'||u.pathname==='/api/not-allowed')status=f.forbidden;
else if(u.pathname!=='/healthz'&&u.pathname!=='/readyz')process.exit(9);
const fail=a.includes('--fail')||(option('--config')&&fs.readFileSync(option('--config'),'utf8').split('\\n').includes('fail'));
if(fail&&Number(status)>=400){console.error('IAM_LEAK_MARKER');process.exit(22)}
if(option('--dump-header'))fs.writeFileSync(option('--dump-header'),h,{mode:384});
if(option('--output')&&option('--output')!=='/dev/null')fs.writeFileSync(option('--output'),body,{mode:384});
if(option('--write-out'))process.stdout.write(option('--write-out').includes('time_starttransfer')?'0.100':status);else if(!option('--output'))process.stdout.write(body);
`;
const fakeYC = `#!/usr/bin/env node
const fs=require('node:fs');fs.appendFileSync(process.env.TRACE,JSON.stringify({client:'yc',argv:process.argv.slice(2)})+'\\n');const f=JSON.parse(fs.readFileSync(process.env.FIXTURE,'utf8'));if(f.ycFailure){console.error('IAM_LEAK_MARKER');process.exit(1)};console.log(f.token||'IAM_LEAK_MARKER');
`;
function run(t, provider='yandex', mutate=()=>{}, overrides={}) {
  const dir=mkdtempSync(join(tmpdir(),'smoke-test-')); t.after(()=>rmSync(dir,{recursive:true,force:true}));
  const bin=join(dir,'bin'), scratch=join(dir,'scratch');mkdirSync(bin,{mode:0o700});mkdirSync(scratch,{mode:0o700});
  writeFileSync(join(bin,'curl'),fakeCurl,{mode:0o700});writeFileSync(join(bin,'yc'),fakeYC,{mode:0o700});symlinkSync(process.execPath,join(bin,'node'));
  const data=fixture(provider);mutate(data);const fixturePath=join(dir,'fixture.json'),trace=join(dir,'trace');writeFileSync(fixturePath,JSON.stringify(data),{mode:0o600});
  const env={...process.env,...environment(provider),...overrides,PATH:`${bin}:/usr/bin:/bin`,TMPDIR:scratch,FIXTURE:fixturePath,TRACE:trace};
  const result=spawnSync('/bin/sh',[script],{env,encoding:'utf8',timeout:10000,maxBuffer:65536});
  assert.equal(result.error,undefined);const calls=readdirSync(dir).includes('trace')?readFileSync(trace,'utf8').trim().split('\n').map(JSON.parse):[];
  assert.deepEqual(readdirSync(scratch),[],'smoke must remove every owned temporary file');
  for(const marker of [state,nonce,token,'ACCESS_LEAK_MARKER','oauth.yandex.ru','oauth.telegram.org'])assert.ok(!result.stdout.includes(marker)&&!result.stderr.includes(marker),`public output contains ${marker}`);
  for(const call of calls){const argv=call.argv.join(' ');for(const marker of [state,nonce,token,'oauth.yandex.ru','oauth.telegram.org'])assert.ok(!argv.includes(marker),`client argv contains ${marker}`);if(call.client==='curl'){assert.equal(call.argv[0],'--disable','ambient curlrc must not enable redirects or retries');assert.ok(!call.argv.some(a=>a==='-L'||a==='--location'||a.startsWith('--retry')));for(const arg of ['--connect-timeout','--max-time','--proto','--max-filesize','--suppress-connect-headers'])assert.ok(call.argv.includes(arg));}}
  return {...result,calls};
}

for(const provider of ['yandex','telegram'])test(`selected ${provider} succeeds privately without redirect follow`,t=>{
  const r=run(t,provider);assert.equal(r.status,0,r.stderr);assert.match(r.stdout,/selected login/);
  const privateCall=r.calls.find(c=>c.configMode!==undefined);assert.equal(privateCall.configMode,0o600);assert.equal(privateCall.directoryMode,0o700);assert.equal(privateCall.privateAuthorization,true);
  assert.equal(r.calls.filter(c=>c.client==='yc').length,1);assert.ok(r.calls.some(c=>c.argv.at(-1)===web+'/auth/login/start?return_to=%2F'));assert.ok(!r.calls.some(c=>c.argv.at(-1)?.includes('/auth/telegram/start')));
});
for(const [name,env] of [
  ['missing selector',{WEB_LOGIN_PROVIDER:''}],['unknown selector',{WEB_LOGIN_PROVIDER:'other'}],['missing Yandex client',{YANDEX_LOGIN_CLIENT_ID:''}],['missing Telegram client',{WEB_LOGIN_PROVIDER:'telegram',TELEGRAM_OIDC_CLIENT_ID:''}],['invalid client',{YANDEX_LOGIN_CLIENT_ID:'secret invalid'}],
  ['wrong public origin',{CLOUD_WEB_URL:'https://other.example'}],['public query',{CLOUD_WEB_URL:web+'?secret=value'}],['public userinfo',{CLOUD_WEB_URL:'https://secret@web.dev.example.invalid'}],['public path',{CLOUD_WEB_URL:web+'/path'}],['public HTTP',{CLOUD_WEB_URL:'http://web.dev.example.invalid'}],['container query',{WEB_CONTAINER_URL:container+'?x=1'}],['container HTTP',{WEB_CONTAINER_URL:'http://container.example.invalid'}],
  ['nonhex digest',{WEB_IMAGE_REF:`cr.yandex/registry/web-bff@sha256:${'z'.repeat(64)}`}],['short digest',{WEB_IMAGE_REF:'cr.yandex/registry/web-bff@sha256:a'}],['image tag',{WEB_IMAGE_REF:'cr.yandex/registry/web-bff:latest'}],['prepared instances',{WEB_PREPARED_INSTANCES:'1'}],['large concurrency',{WEB_CONCURRENCY:'9'}],['invalid concurrency',{WEB_CONCURRENCY:'1x'}],['unbounded wait',{WEB_COLD_START_WAIT_SECONDS:'999'}],
])test(`preflight ${name} makes zero client calls`,t=>{const r=run(t,'yandex',()=>{},env);assert.equal(r.status,2);assert.equal(r.calls.length,0)});

const invalidLogin = [
 ['wrong host',u=>{u.hostname='evil.example'}],['wrong provider',u=>{u.hostname='oauth.telegram.org'}],['userinfo',u=>{u.username='secret'}],['fragment',u=>{u.hash='secret'}],['wrong path',u=>{u.pathname='/token'}],['wrong client',u=>u.searchParams.set('client_id','other')],['wrong callback',u=>u.searchParams.set('redirect_uri','https://evil.example/auth/login/callback')],['old callback',u=>u.searchParams.set('redirect_uri',web+'/auth/telegram/callback')],['wrong scope',u=>u.searchParams.set('scope','openid profile')],['extra scope',u=>u.searchParams.set('scope','login:info login:email')],['empty state',u=>u.searchParams.set('state','')],['oversized state',u=>u.searchParams.set('state','x'.repeat(161))],['nonopaque state',u=>u.searchParams.set('state','a b')],['plain method',u=>u.searchParams.set('code_challenge_method','plain')],['missing method',u=>u.searchParams.delete('code_challenge_method')],['bad challenge',u=>u.searchParams.set('code_challenge','abc')],['noncanonical challenge',u=>u.searchParams.set('code_challenge',Buffer.alloc(32,7).toString('base64url').slice(0,-1)+'d')],['wrong response type',u=>u.searchParams.set('response_type','token')],['access token',u=>u.searchParams.set('access_token','ACCESS_LEAK_MARKER')],['nonce on OAuth',u=>u.searchParams.set('nonce',nonce)],['duplicate state',u=>u.searchParams.append('state',state)],['duplicate client',u=>u.searchParams.append('client_id','yandex-client')],
];
for(const [name,change]of invalidLogin)test(`authorization rejects ${name}`,t=>{const r=run(t,'yandex',f=>{const u=authorization('yandex');change(u);f.login=headers(303,[['Cache-Control','no-store'],['Location',u.href]])});assert.equal(r.status,1)});
for(const [name,mutate]of [
 ['non303',f=>{f.login=f.login.replace('303','302')}],['duplicate Location',f=>{f.login=f.login.replace('\r\n\r\n',`\r\nLOCATION: ${authorization('yandex').href}\r\n\r\n`)}],['folded Location',f=>{f.login=f.login.replace('Location:',' Location:')}],['malformed header',f=>{f.login=f.login.replace('Location:','Location ')}],['malformed percent',f=>{f.login=f.login.replace('state=','state=%ZZ')}],['second response',f=>{f.login+=f.login}],['oversized headers',f=>{f.login='x'.repeat(65537)}],['missing no-store',f=>{f.login=f.login.replace('no-store','public')}],['empty nonce',f=>{const u=authorization('telegram');u.searchParams.set('nonce','');f.login=headers(303,[['Cache-Control','no-store'],['Location',u.href]])}],
])test(`headers reject ${name}`,t=>{const provider=name==='empty nonce'?'telegram':'yandex';assert.equal(run(t,provider,mutate).status,1)});
for(const [name,mutate]of [
 ['public direct invocation',f=>{f.anonymous='200'}],['wrong version',f=>{f.version='{"component":"other"}'}],['missing HSTS',f=>{f.root=f.root.replace('Strict-Transport-Security:','Other:')}],['missing CSP',f=>{f.root=f.root.replace("frame-ancestors 'none'","frame-ancestors *")}],['cacheable root',f=>{f.root=f.root.replace('no-store','public')}],['me success',f=>{f.me=f.me.replace('401','200')}],['cacheable me',f=>{f.me=f.me.replace('no-store','public')}],['permitted forbidden route',f=>{f.forbidden='200'}],['yc failure',f=>{f.ycFailure=true}],['unsafe IAM token',f=>{f.token='IAM_LEAK_MARKER\nheader="bad"'}],
])test(`existing guard rejects ${name}`,t=>{assert.equal(run(t,'yandex',mutate).status,1)});
for(const status of ['401','403'])test(`authenticated private invocation rejects HTTP ${status}`,t=>{
  const r=run(t,'yandex',f=>{f.privateStatus=status});assert.equal(r.status,1);
  assert.equal(r.calls.filter(c=>c.client==='curl').length,2,'must stop before public probes');
  assert.equal(r.calls.filter(c=>c.client==='yc').length,1);
});
for(const path of ['/healthz','/version','/','/api/web/v1/me','/auth/login/start','/api/not-allowed'])test(`curl failure at ${path} stays private and cleans up`,t=>{assert.equal(run(t,'yandex',f=>{f.curlFailure=path}).status,1)});
test('validator does not expose malformed auth input or excessive stdin',()=>{
  for(const input of ['Location: ACCESS_LEAK_MARKER',Buffer.alloc(65537,65)]){
    const r=spawnSync(process.execPath,[check,'login'],{env:{...process.env,...environment('yandex')},input,encoding:'utf8',timeout:3000,maxBuffer:65536});assert.equal(r.status,1);assert.equal(r.stdout,'');assert.equal(r.stderr,'cloud Web smoke validation failed\n');
  }
});

const rootSecurityFields = [
  ['X-Content-Type-Options','nosniff'], ['Referrer-Policy','strict-origin-when-cross-origin'],
  ['Cache-Control','no-store'], ['Content-Security-Policy',"default-src 'self'; frame-ancestors 'none'"],
];
const rootWithHSTS = (policies, extra=[]) => headers(200,[
  ...policies.map(value=>['Strict-Transport-Security',value]), ...rootSecurityFields, ...extra,
]);
function checkRoot(input) {
  const r=spawnSync(process.execPath,[check,'root'],{env:{...process.env,...environment('yandex')},input,encoding:'utf8',timeout:3000,maxBuffer:65536});
  assert.equal(r.error,undefined);assert.equal(r.stdout,'');
  assert.equal(r.stderr,r.status===0?'':'cloud Web smoke validation failed\n','invalid headers must never be reflected');
  return r.status;
}
const appHSTS = 'max-age=31536000; includeSubDomains';
const gatewayHSTS = 'max-age=31536000; includeSubdomains; preload';
for(const [name,policies] of [
  ['singleton',[appHSTS]], ['exact managed wire fields',[appHSTS,gatewayHSTS]],
  ['reversed managed fields',[gatewayHSTS,appHSTS]],
  ['longer max-age',['max-age=63072000; includeSubDomains']],
  ['case and whitespace',['\tPRELOAD ; IncludeSubDomains ; MAX-AGE = 31536000\t']],
])test(`HSTS accepts ${name}`,()=>{assert.equal(checkRoot(rootWithHSTS(policies)),0)});
for(const [name,policies] of [
  ['missing',[]], ['empty',['']], ['weak',['max-age=31535999; includeSubDomains']],
  ['weak first field',['max-age=0; includeSubDomains',appHSTS]],
  ['weak later field',[appHSTS,'max-age=0; includeSubDomains']],
  ['missing includeSubDomains',['max-age=31536000; preload']],
  ['missing max-age',['includeSubDomains; preload']],
  ['comma combined',[`${appHSTS}, ${gatewayHSTS}`]],
  ['comma in later field',[appHSTS,`${appHSTS}, ${gatewayHSTS}`]],
  ['duplicate max-age',['max-age=31536000; MAX-AGE=31536000; includeSubDomains']],
  ['conflicting max-age',['max-age=31536000; max-age=0; includeSubDomains']],
  ['duplicate includeSubDomains',['max-age=31536000; includeSubDomains; INCLUDESUBDOMAINS']],
  ['duplicate preload',['max-age=31536000; includeSubDomains; preload; PRELOAD']],
  ['negative max-age',['max-age=-31536000; includeSubDomains']],
  ['fractional max-age',['max-age=31536000.5; includeSubDomains']],
  ['exponent max-age',['max-age=3.1536e7; includeSubDomains']],
  ['quoted max-age',['max-age="31536000"; includeSubDomains']],
  ['empty directive',['max-age=31536000;; includeSubDomains']],
  ['valued includeSubDomains',['max-age=31536000; includeSubDomains=true']],
  ['valued preload',['max-age=31536000; includeSubDomains; preload=true']],
  ['unknown directive',[`${appHSTS}; HSTS_LEAK_MARKER`]],
  ['malformed later field',[appHSTS,'HSTS_LEAK_MARKER']],
])test(`HSTS rejects ${name}`,()=>{assert.equal(checkRoot(rootWithHSTS(policies)),1)});
for(const [name,value] of rootSecurityFields)test(`root keeps singleton ${name}`,()=>{
  assert.equal(checkRoot(rootWithHSTS([appHSTS,gatewayHSTS],[[name.toLowerCase(),value]])),1);
});
test('managed separate HSTS smoke succeeds and cleans owned files',t=>{
  const r=run(t,'yandex',f=>{f.root=rootWithHSTS([appHSTS,gatewayHSTS])});
  assert.equal(r.status,0,r.stderr);
  const rootCall=r.calls.find(c=>c.client==='curl'&&c.argv.at(-1)===web+'/');
  assert.ok(rootCall.argv.includes('Accept: text/html'),'root probe must request HTML navigation');
});
