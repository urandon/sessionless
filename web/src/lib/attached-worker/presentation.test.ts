import { describe, expect, it } from 'vitest';

import {
  actionExplanations,
  exactUTCTime,
  explainAction,
  explainReason,
  explainUnavailable,
  reasonExplanations,
  unavailableExplanations,
  validTimestamp,
} from './presentation';

describe('attached-worker presentation vocabulary', () => {
  it('renders every generated reason and action with public-safe help', () => {
    expect(Object.keys(reasonExplanations)).toHaveLength(27);
    expect(Object.keys(actionExplanations)).toHaveLength(14);
    expect(Object.keys(unavailableExplanations)).toHaveLength(13);
    expect(explainReason('quota_zero').title).toBe('Quota is zero');
    expect(explainAction('request_cancel')).toBe('Request cancellation');
    expect(explainUnavailable('control_contract_unavailable').title).toBe(
      'Control contract is unavailable',
    );
  });

  it('keeps unknown runtime codes visible without reflecting unsafe content', () => {
    expect(explainReason('<script>private token</script>').title).toBe(
      'Unknown reason (scriptprivatetokenscript)',
    );
    expect(explainAction('future_action')).toBe('Unknown action (future_action)');
    expect(explainUnavailable(undefined).title).toBe('Unavailable');
  });

  it('formats only valid exact UTC timestamps', () => {
    expect(validTimestamp('2026-08-26T08:00:00Z')).toBe(true);
    expect(validTimestamp('2026-08-26T08:00:00.123456Z')).toBe(true);
    expect(validTimestamp(undefined)).toBe(false);
    expect(validTimestamp('not-a-time')).toBe(false);
    expect(validTimestamp('2026-08-26T08:00:00+03:00')).toBe(false);
    expect(exactUTCTime('2026-08-26T08:00:00.123456Z')).toBe('2026-08-26 08:00:00.123456 UTC');
  });
});
