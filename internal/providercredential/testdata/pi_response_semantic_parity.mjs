// Credential-free maintainer probe, NOT native/SDK qualification. Executes the
// EXACT pinned processResponsesStream body with inert pricing/signature/terminal
// mapping collaborators. No import of SDK, provider, auth, settings or discovery.
// Invoke with Node24 permission allow-read for this file and the exact source.
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {createHash} from 'node:crypto';
import {stripTypeScriptTypes} from 'node:module';
import vm from 'node:vm';

assert.equal(process.argv.length, 3);
const raw = readFileSync(process.argv[2]);
assert.equal(createHash('sha256').update(raw).digest('hex'),
  'e86649ae0ccc20478df9919dc57e7bb3d8f2fc02e343cc1f87db16d8ca1ca2bb');
const source = raw.toString('utf8');
const from = source.indexOf('export async function processResponsesStream');
const to = source.indexOf('\nfunction mapStopReason(', from);
assert(from >= 0 && to > from);
const script = new vm.Script(stripTypeScriptTypes(source.slice(from, to).replace('export ', '')) + '\nprocessResponsesStream;');
const parse = script.runInNewContext({
  calculateCost() {},
  encodeTextSignatureV1() { return ''; }, // not exported by bootstrap
  mapStopReason() { return {stopReason: 'stop'}; },
}, {timeout: 1000});

const add = (type, index = 0) => ({type: 'response.output_item.added', output_index: index, item: {type}});
const delta = (type, text, index = 0) => ({type: `response.${type}.delta`, output_index: index, delta: text});
const done = (type, content, index = 0) => ({type: 'response.output_item.done', output_index: index, item: {type, ...content}});
const cases = [
  ['mixed text/refusal', true, [add('message'), delta('output_text', 'fixture-'), delta('refusal', 'secret')]],
  ['mixed thinking', true, [add('reasoning'), delta('reasoning_summary_text', 'fixture-'), delta('reasoning_text', 'secret')]],
  ['intermediate overwritten', true, [add('message'), delta('output_text', 'fixture-'), delta('refusal', 'secret'), done('message', {content: [{type: 'output_text', text: 'safe'}]})]],
  ['done join', true, [done('message', {content: [{type: 'output_text', text: 'fixture-'}, {type: 'refusal', refusal: 'secret'}]})]],
  ['no delta slot', false, [delta('output_text', 'fixture-'), add('message'), delta('refusal', 'secret')]],
  ['different slots', false, [add('message'), add('message', 1), delta('output_text', 'fixture-'), delta('refusal', 'secret', 1)]],
  ['replace active mapping', false, [add('message'), delta('output_text', 'fixture-'), add('message'), delta('refusal', 'secret')]],
  ['done/reuse', false, [done('message', {content: [{type: 'output_text', text: 'fixture-'}]}), add('message'), delta('refusal', 'secret')]],
  ['wrong-type done keeps mapping', true, [add('message'), delta('output_text', 'fixture-'), done('reasoning', {summary: [{text: 'safe'}]}), delta('refusal', 'secret')]],
  ['summary separator', false, [add('reasoning'), delta('reasoning_text', 'fixture-'), {type: 'response.reasoning_summary_part.done', output_index: 0}, delta('reasoning_text', 'secret')]],
  ['empty summary keeps old', true, [add('reasoning'), delta('reasoning_text', 'fixture-'), delta('reasoning_text', 'secret'), done('reasoning', {summary: [], content: []})]],
  ['replacement not append', false, [add('message'), delta('output_text', 'fixture-'), done('message', {content: [{type: 'refusal', refusal: 'secret'}]})]],
];
for (const [name, expected, events] of cases) {
  let leaked = false;
  const output = {content: [], usage: {}, stopReason: 'stop'};
  const stream = {push() {
    leaked ||= output.content.some((block) => (block.text ?? block.thinking ?? '').includes('fixture-secret'));
  }};
  async function* input() {
    yield* events;
    yield {type: 'response.completed', response: {id: 'fixture', status: 'completed', output: []}};
  }
  await parse(input(), output, stream, {});
  assert.equal(leaked, expected, name);
}
process.stdout.write(JSON.stringify({cases: cases.length, status: 'PASS', scope: 'pinned parser body with inert collaborators, no SDK/provider/native qualification'}) + '\n');
