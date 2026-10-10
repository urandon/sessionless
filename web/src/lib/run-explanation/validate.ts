import type { components } from '../api/generated';

type Explanation = components['schemas']['RunExplanationV1'];
type RecordValue = Record<string, unknown>;

export function parseRunExplanationText(
  text: string,
  runId: string,
  sessionId: string,
): Explanation {
  const value: unknown = JSON.parse(text);
  // Skip complete JSON strings (including escaped property names). Every numeric
  // field in this manifest is an unsigned integer. Reject lexical fractions and
  // exponents before JSON.parse rounding can make an invalid token look exact.
  const tokens = /"(?:\\.|[^"\\])*"|(-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)/g;
  for (const token of text.matchAll(tokens)) {
    const number = token[1];
    if (number === undefined) continue;
    requireValid(/^\d+$/.test(number) && BigInt(number) <= BigInt(Number.MAX_SAFE_INTEGER));
  }
  return validateRunExplanation(value, runId, sessionId);
}

function requireValid(ok: unknown): asserts ok {
  if (!ok) throw new TypeError('Invalid run evidence.');
}

function closed(value: unknown, required: string[], optional: string[] = []): RecordValue {
  requireValid(typeof value === 'object' && value !== null && !Array.isArray(value));
  const record = value as RecordValue;
  requireValid(required.every((key) => Object.hasOwn(record, key)));
  requireValid(
    Object.keys(record).every((key) => required.includes(key) || optional.includes(key)),
  );
  return record;
}

function choice(value: unknown, options: string): boolean {
  return typeof value === 'string' && options.split(' ').includes(value);
}

function opaque(value: unknown): boolean {
  return (
    typeof value === 'string' && value.length <= 160 && /^[A-Za-z0-9][A-Za-z0-9._:-]*$/.test(value)
  );
}

function positive(value: unknown, maximum = Number.MAX_SAFE_INTEGER): boolean {
  return typeof value === 'number' && Number.isSafeInteger(value) && value > 0 && value <= maximum;
}

// Compare UTC instants at Go's nanosecond precision, not Date's milliseconds.
function instant(value: unknown): bigint {
  requireValid(typeof value === 'string');
  const match = /^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?Z$/.exec(value);
  requireValid(match);
  const seconds = Date.parse(`${match[1]}Z`);
  requireValid(
    Number.isFinite(seconds) && new Date(seconds).toISOString().slice(0, 19) === match[1],
  );
  const nanos = BigInt((match[2] ?? '').padEnd(9, '0'));
  requireValid(match[1] !== '0001-01-01T00:00:00' || nanos !== 0n);
  return BigInt(seconds) * 1000000n + nanos;
}

function terminal(value: unknown): boolean {
  return choice(value, 'succeeded failed cancelled');
}

function recorded(value: unknown, fields: string[], optional: string[] = []): RecordValue {
  const group = closed(value, ['availability'], fields.concat(optional));
  requireValid(choice(group.availability, 'recorded unknown'));
  if (group.availability === 'unknown') return closed(value, ['availability']);
  return closed(value, ['availability', ...fields], optional);
}

// Closed manifest and semantic invariants mirror internal/webcontract/run_explanation.go.
// uint64 values outside JavaScript's exact integer range fail closed; never round/coerce.
export function validateRunExplanation(
  value: unknown,
  runId: string,
  sessionId: string,
): Explanation {
  const v = closed(
    value,
    [
      'version',
      'run_id',
      'session_id',
      'read_at',
      'status',
      'created_at',
      'updated_at',
      'admission',
      'terminal',
      'attempt',
      'attached',
      'coverage',
    ],
    ['finished_at'],
  );
  requireValid(
    v.version === 1 &&
      opaque(v.run_id) &&
      opaque(v.session_id) &&
      v.run_id === runId &&
      v.session_id === sessionId,
  );
  requireValid(
    choice(v.status, 'created admitted queued running succeeded failed cancelled quota_blocked'),
  );
  const read = instant(v.read_at),
    created = instant(v.created_at),
    updated = instant(v.updated_at);
  requireValid(
    created <= updated && updated <= read && terminal(v.status) === Object.hasOwn(v, 'finished_at'),
  );
  const finished = v.finished_at === undefined ? undefined : instant(v.finished_at);
  requireValid(finished === undefined || (created <= finished && finished <= updated));

  const a = recorded(v.admission, [
    'outcome',
    'reason_code',
    'observed_at',
    'decision_revision',
    'coverage',
  ]);
  if (a.availability === 'recorded') {
    requireValid(
      choice(a.outcome, 'admitted denied') &&
        choice(
          a.reason_code,
          'admitted subscription_attention_required capacity_draining quota_reset_pending quota_exhausted_reset_unknown runtime_limit_exceeded turn_limit_exceeded input_limit_exceeded context_limit_exceeded artifact_limit_exceeded capacity_busy workspace_queue_limit workspace_active_run_limit',
        ),
    );
    requireValid(
      positive(a.decision_revision) &&
        a.coverage === 'last_recorded_decision' &&
        (a.outcome === 'admitted') === (a.reason_code === 'admitted'),
    );
    const observed = instant(a.observed_at);
    requireValid(created <= observed && observed <= read);
    requireValid(
      a.outcome === 'denied'
        ? v.status === 'quota_blocked'
        : !choice(v.status, 'created quota_blocked'),
    );
  }

  const t = closed(
    v.terminal,
    ['availability'],
    ['reason_code', 'observed_at', 'event_id', 'event_sequence'],
  );
  requireValid(choice(t.availability, 'recorded unknown not_applicable'));
  if (t.availability === 'recorded') {
    closed(t, ['availability', 'reason_code', 'observed_at', 'event_id', 'event_sequence']);
    requireValid(
      choice(
        t.reason_code,
        'execution_failed result_persistence_failed canonical_cancelled unclassified_failure',
      ) &&
        opaque(t.event_id) &&
        positive(t.event_sequence),
    );
    requireValid(
      choice(v.status, 'failed cancelled') &&
        (v.status === 'cancelled') === (t.reason_code === 'canonical_cancelled'),
    );
    requireValid(instant(t.observed_at) === finished);
  } else {
    closed(t, ['availability']);
    requireValid((t.availability === 'not_applicable') === !terminal(v.status));
  }

  const p = recorded(v.attempt, ['attempt_id', 'number', 'status', 'updated_at'], ['finished_at']);
  if (p.availability === 'recorded') {
    requireValid(
      opaque(p.attempt_id) &&
        positive(p.number, 4294967295) &&
        choice(p.status, 'created running succeeded failed cancelled'),
    );
    const at = instant(p.updated_at);
    requireValid(
      created <= at && at <= read && terminal(p.status) === Object.hasOwn(p, 'finished_at'),
    );
    if (p.finished_at !== undefined) {
      const end = instant(p.finished_at);
      requireValid(created <= end && end <= at);
    }
  }

  const o = recorded(v.attached, ['fact', 'observed_at', 'freshness'], ['valid_until']);
  if (o.availability === 'recorded') {
    requireValid(
      choice(
        o.fact,
        'offer_recorded claim_recorded terminal_candidate_recorded terminal_commit_recorded fenced_outcome_unknown receipt_retired',
      ) &&
        choice(o.freshness, 'within_lease expired durable') &&
        p.availability === 'recorded',
    );
    const observed = instant(o.observed_at);
    requireValid(created <= observed && observed <= read);
    if (choice(o.fact, 'terminal_commit_recorded fenced_outcome_unknown receipt_retired')) {
      requireValid(o.freshness === 'durable' && !Object.hasOwn(o, 'valid_until'));
    } else {
      const until = instant(o.valid_until);
      requireValid(
        created < until &&
          o.freshness !== 'durable' &&
          (o.freshness === 'within_lease') === read < until,
      );
    }
  }
  const c = closed(v.coverage, ['admission', 'terminal', 'attempt', 'operational']);
  requireValid(
    c.admission === a.availability &&
      c.terminal === t.availability &&
      c.attempt === p.availability &&
      c.operational === o.availability,
  );
  requireValid(
    (a.availability !== 'recorded' && t.availability !== 'recorded') ||
      p.availability === 'recorded',
  );
  return value as Explanation;
}
