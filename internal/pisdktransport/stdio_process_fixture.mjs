// Test-only local process fixture. NOT a bootstrap, SDK/model client, serving
// artifact, native fit proof or closed Init producer. No filesystem/key/network.
import { stdin, stdout } from 'node:process';
import { createPiStdioChannel } from './stdio.mjs';
const limits = Object.freeze({ initialize_body_bytes: (1 << 20) - 17,
  request_body_bytes: (1 << 20) - 146, final_body_bytes: 128 << 10, frame_bytes: 1 << 20 });
const original = new AbortController();
const channel = createPiStdioChannel({ input: stdin, output: stdout, limits, signal: original.signal });
try {
  const initial = await channel.initialize();
  const profile = initial.toString('utf8'); // opaque bounded fixture, NOT real Init schema
  const body = '{"model":"synthetic","input":[{"role":"user","content":[{"type":"input_text","text":"whole history 9007199254740993"},{"type":"input_image","image_url":"data:image/png;base64,AAAA"}]}],"tools":[]}';
  const owner = channel.bindRequest(body, profile);
  const use = await owner.authorizePhysicalSend();
  use.assertUsable();
  await channel.publishFinal('{"answer":"synthetic","artifacts":[]}');
} catch { process.exitCode = 71; }
finally { await channel.dispose(); }
