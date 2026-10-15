// Fixed local API-key file delivery only. No HOME/env/OAuth/refresh/discovery,
// path fallback, second file or authority. The parent retains immutable
// Finalize/Release and original handle ownership after the process joins.
import { Buffer } from 'node:buffer';
import { constants } from 'node:fs';
import { open,realpath } from 'node:fs/promises';
import { isAbsolute, normalize } from 'node:path';
import { getuid } from 'node:process';

const denied = () => new Error('pi_credential_file_denied');
const same = (a,b) => ['dev','ino','uid','gid','mode','nlink','size','mtimeNs','ctimeNs'].every(k=>a[k]===b[k]);
const valid = s => s.isFile() && (s.mode & 0o7777n)===0o600n && s.nlink===1n && s.uid===BigInt(getuid());

export async function readPreparedCredentialFile(path,maxBytes,signal) {
  let file, bytes;
  try {
    if(!(signal instanceof AbortSignal)||signal.aborted||typeof path!=='string'||!isAbsolute(path)||normalize(path)!==path||path.includes('\0')||
      !Number.isSafeInteger(maxBytes)||maxBytes<1||maxBytes>64<<10||!Number.isInteger(constants.O_NOFOLLOW)||constants.O_NOFOLLOW===0)throw denied();
    if(await realpath(path)!==path)throw denied();
    file=await open(path,constants.O_RDONLY|constants.O_NOFOLLOW|constants.O_NONBLOCK);
    signal.throwIfAborted();
    const before=await file.stat({bigint:true});
    if(!valid(before)||before.size<1n||before.size>BigInt(maxBytes))throw denied();
    // Capacity and the source's logical string/header copies must be funded
    // by the original Go producer before Issue, not claimed as V8/native fit.
    bytes=Buffer.alloc(Number(before.size)+1);
    let used=0;
    while(used<bytes.length) {
      signal.throwIfAborted();
      const {bytesRead}=await file.read(bytes,used,bytes.length-used,used);
      signal.throwIfAborted();
      if(bytesRead===0)break;
      used+=bytesRead;
    }
    const after=await file.stat({bigint:true});
    if(!valid(after)||!same(before,after)||BigInt(used)!==before.size||await realpath(path)!==path)throw denied();
    // Exact visible ASCII token, never trim/coerce JSON, mask high bits or
    // reinterpret a subscription auth.json as a standard Responses API key.
    for(let i=0;i<used;i++)if(bytes[i]<33||bytes[i]>126)throw denied();
    signal.throwIfAborted();
    return bytes.toString('ascii',0,used);
  }catch{throw denied();}
  finally{bytes?.fill(0);if(file)await file.close().catch(()=>{throw denied();});}
}
