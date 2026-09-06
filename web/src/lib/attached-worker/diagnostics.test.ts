import { describe, expect, it } from 'vitest';

import { attachedWorkerDiagnostics } from './test-fixtures';
import {
  buildDiagnosticBundleV1,
  diagnosticFactExplanations,
  explainDiagnostic,
  serializeDiagnosticBundleV1,
} from './diagnostics';

describe('attached-worker public diagnostics', () => {
  it('has stable help for all 23 V1 facts and a safe unknown fallback', () => {
    expect(Object.keys(diagnosticFactExplanations)).toHaveLength(23);
    expect(explainDiagnostic('canonical_terminal').title).toBe('Canonical terminal');
    expect(explainDiagnostic('<script>secret</script>').title).toBe(
      'Unknown diagnostic (scriptsecretscript)',
    );
  });

  it('serializes only the explicit public allowlist without losing microseconds or explicit states', () => {
    const input = attachedWorkerDiagnostics() as ReturnType<typeof attachedWorkerDiagnostics> & {
      private_token?: string;
    };
    input.private_token = 'must-not-leak';
    (input.facts[0] as (typeof input.facts)[number] & { provider_body?: string }).provider_body =
      'must-not-leak';

    const serialized = serializeDiagnosticBundleV1(input);
    expect(serialized.endsWith('\n')).toBe(true);
    expect(serialized).toContain('2026-08-26T08:00:00.123456Z');
    expect(serialized).toContain('not_evaluated');
    expect(serialized).not.toContain('must-not-leak');
    expect(JSON.parse(serialized)).toEqual(buildDiagnosticBundleV1(input));
  });

  it('fails closed on duplicate, malformed, or oversized evidence', () => {
    const duplicate = attachedWorkerDiagnostics();
    duplicate.facts.push({ ...duplicate.facts[0]! });
    expect(() => serializeDiagnosticBundleV1(duplicate)).toThrow('invalid_diagnostics');

    const malformed = attachedWorkerDiagnostics();
    malformed.evaluated_at = 'not-a-time';
    expect(() => serializeDiagnosticBundleV1(malformed)).toThrow('invalid_diagnostics');

    const oversized = attachedWorkerDiagnostics();
    oversized.worker_id = 'w'.repeat(161);
    expect(() => serializeDiagnosticBundleV1(oversized)).toThrow('invalid_diagnostics');
  });

  it('rejects replaced catalog entries, invalid freshness, and unknown warnings at runtime', () => {
    const replaced = attachedWorkerDiagnostics();
    replaced.facts[0] = {
      cohort: 'governance',
      code: 'remote_erase',
      state: 'not_acknowledged',
    };
    expect(() => serializeDiagnosticBundleV1(replaced)).toThrow('invalid_diagnostics');

    const freshness = attachedWorkerDiagnostics();
    (freshness.facts[9] as { freshness?: string }).freshness = 'recent_enough';
    expect(() => serializeDiagnosticBundleV1(freshness)).toThrow('invalid_diagnostics');

    const warning = attachedWorkerDiagnostics();
    (warning.warnings as string[])[0] = 'provider_token';
    expect(() => serializeDiagnosticBundleV1(warning)).toThrow('invalid_diagnostics');
  });

  it('rejects safe-looking unknown states and forbidden or missing observation fields', () => {
    const unknownState = attachedWorkerDiagnostics();
    unknownState.facts[8]!.state = 'connected';
    expect(() => serializeDiagnosticBundleV1(unknownState)).toThrow('invalid_diagnostics');

    const borrowedContact = attachedWorkerDiagnostics();
    borrowedContact.facts[8]!.observed_at = '2026-08-26T07:59:58Z';
    borrowedContact.facts[8]!.freshness = 'fresh';
    expect(() => serializeDiagnosticBundleV1(borrowedContact)).toThrow('invalid_diagnostics');

    const missingCapabilityTime = attachedWorkerDiagnostics();
    delete missingCapabilityTime.facts[11]!.observed_at;
    expect(() => serializeDiagnosticBundleV1(missingCapabilityTime)).toThrow('invalid_diagnostics');
  });

  it('accepts production-reachable expired contact and retained terminal replay evidence', () => {
    const stale = attachedWorkerDiagnostics();
    stale.facts[9]!.freshness = 'expired';
    expect(JSON.parse(serializeDiagnosticBundleV1(stale)).facts[9]).toMatchObject({
      code: 'last_contact',
      state: 'recorded',
      freshness: 'expired',
    });

    const replay = attachedWorkerDiagnostics();
    replay.facts[15]!.state = 'retired';
    replay.facts[17]!.state = 'acknowledged';
    replay.facts[17]!.observed_at = '2026-08-26T07:59:55Z';
    replay.facts[19]!.state = 'received';
    replay.facts[20]!.state = 'committed';
    replay.facts[20]!.observed_at = '2026-08-26T07:59:57Z';
    replay.warnings = replay.warnings.filter((warning) => warning !== 'attempt_active');
    const facts = JSON.parse(serializeDiagnosticBundleV1(replay)).facts as Array<{
      code: string;
      state: string;
    }>;
    expect(facts.find((fact) => fact.code === 'attempt_state')?.state).toBe('retired');
    expect(facts.find((fact) => fact.code === 'canonical_terminal')?.state).toBe('committed');
  });
});
