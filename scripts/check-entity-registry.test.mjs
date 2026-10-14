import test from "node:test";
import assert from "node:assert/strict";
import { deriveSchema, validateRegistry } from "./check-entity-registry.mjs";

const created = `-- +goose Up
CREATE TABLE IF NOT EXISTS ` + "`" + `tenant_records` + "`" + ` (
  tenant_id Utf8,
  record_id Utf8,
  PRIMARY KEY (tenant_id, record_id)
);
`;
const alterOnly = `-- +goose Up
ALTER TABLE ` + "`" + `tenant_records` + "`" + ` ADD COLUMN note Utf8;
`;
function record(overrides = {}) {
  return {
    table: "tenant_records",
    migration: "00001_create_tenant_records.sql",
    role: "Canonical fixture entity.",
    primaryKey: ["tenant_id", "record_id"],
    observedKeyScope: { basis: "primary_key", columns: ["tenant_id", "record_id"] },
    retention: { kind: "unknown", statement: "No TTL clause in fixture CREATE." },
    knownRefs: {
      readers: { paths: ["internal/store.go"], total: 1 },
      writers: { paths: ["internal/store.go"], total: 1 },
    },
    entityGroup: "fixture",
    durabilityReview: "pending",
    proposedDisposition: "review",
    ...overrides,
  };
}
function registry(records) {
  return { schema: "sessionless-ydb-entity-registry/v1", recordCount: records.length, records };
}
const sources = {
  "00001_create_tenant_records.sql": created,
  "00002_add_note.sql": alterOnly,
};

test("derives CREATE names and does not treat ALTER-only files as tables", () => {
  const schema = deriveSchema({
    "00001_create_tenant_records.sql": created.replace(
      "-- +goose Up",
      "-- +goose Up\n-- CREATE TABLE IF NOT EXISTS `not_a_table` (",
    ) + `-- +goose Down
CREATE TABLE IF NOT EXISTS ` + "`" + `down_table` + "`" + ` (
  wrong_id Utf8,
  PRIMARY KEY (wrong_id)
);
`,
    "00002_add_note.sql": alterOnly,
  });
  assert.deepEqual([...schema.tables.keys()], ["tenant_records"]);
  assert.equal(schema.createStatements, 1);
  assert.equal(schema.alterStatements, 1);
  assert.equal(schema.alterOnlyMigrations, 1);
  assert.deepEqual(schema.tables.get("tenant_records").primaryKey, ["tenant_id", "record_id"]);
  assert.deepEqual(schema.errors, []);
});

test("fails closed on CREATE without IF NOT EXISTS and split CREATE keywords", () => {
  const schema = deriveSchema({
    "00001_no_idempotence.sql": `-- +goose Up
CREATE TABLE ` + "`" + `unsafe_table` + "`" + ` (id Utf8, PRIMARY KEY (id));
`,
    "00002_split_keyword.sql": `-- +goose Up
CRE
ATE TABLE IF NOT EXISTS ` + "`" + `split_table` + "`" + ` (id Utf8, PRIMARY KEY (id));
`,
  });
  assert.match(schema.errors.join("\n"), /unsupported CREATE TABLE syntax/);
  assert.match(schema.errors.join("\n"), /unsupported split CREATE keyword/);
  assert.equal(schema.tables.size, 0);
});

test("fails closed on DROP and multiple executable Up operations", () => {
  const schema = deriveSchema({
    "00001_drop.sql": `-- +goose Up
DROP TABLE ` + "`" + `tenant_records` + "`" + `;
`,
    "00002_multiple.sql": created + "DROP TABLE `tenant_records`;",
  });
  assert.match(schema.errors.join("\n"), /unsupported DROP operation/);
  assert.match(schema.errors.join("\n"), /expected exactly one executable Up DDL statement; found 2/);
});

test("supports whitespace between CREATE and its table name", () => {
  const schema = deriveSchema({
    "00001_create_tenant_records.sql": created.replace(
      "CREATE TABLE IF NOT EXISTS `tenant_records`",
      "CREATE\nTABLE IF NOT EXISTS\n`tenant_records`",
    ),
  });
  assert.deepEqual(schema.errors, []);
  assert.deepEqual(schema.tables.get("tenant_records").primaryKey, ["tenant_id", "record_id"]);
  assert.deepEqual(validateRegistry(registry([record()]), schema), []);
});

test("rejects unsupported block comments rather than deriving a comment's primary key", () => {
  const schema = deriveSchema({
    "00001_block_comment.sql": "-- +goose Up\nCREATE TABLE IF NOT EXISTS `a` ( /* PRIMARY KEY (ghost) */ x Utf8, PRIMARY KEY (x));",
  });
  assert.equal(schema.tables.size, 0);
  assert.match(validateRegistry(registry([]), schema).join("\n"), /unsupported block-comment syntax/);
});

test("rejects multiple CREATEs without transferring one table's primary key", () => {
  const schema = deriveSchema({
    "00001_multiple.sql": created + "CREATE TABLE IF NOT EXISTS `second` (id Utf8, PRIMARY KEY (id));",
  });
  assert.deepEqual(schema.tables.get("second").primaryKey, ["id"]);
  assert.match(validateRegistry(registry([]), schema).join("\n"), /expected exactly one executable Up DDL statement; found 2/);
  const mixed = deriveSchema({ "00001_mixed.sql": created + alterOnly.replace("-- +goose Up", "") });
  assert.match(validateRegistry(registry([record({ migration: "00001_mixed.sql" })]), mixed).join("\n"), /expected exactly one executable Up DDL statement; found 2/);
});

test("accepts an exact fixture registry", () => {
  const schema = deriveSchema(sources);
  assert.deepEqual(validateRegistry(registry([record()]), schema), []);
});

test("rejects missing, extra, and duplicate registry entries", () => {
  const schema = deriveSchema(sources);
  assert.match(validateRegistry(registry([]), schema).join("\n"), /missing registry entry/);
  assert.match(validateRegistry(registry([record(), record({ table: "ghost" })]), schema).join("\n"), /not created by a migration/);
  assert.match(validateRegistry(registry([record(), record()]), schema).join("\n"), /duplicate registry entry/);
});

test("rejects invalid migration, observed key metadata, and references", () => {
  const schema = deriveSchema(sources);
  const bad = record({
    migration: "99999_missing.sql",
    primaryKey: ["record_id"],
    observedKeyScope: { basis: "primary_key", columns: ["record_id"] },
    knownRefs: { readers: { paths: ["outside/file.go"], total: 1 }, writers: { paths: [], total: 0 } },
  });
  const errors = validateRegistry(registry([bad]), schema, (ref) => ref === "internal/store.go");
  assert.match(errors.join("\n"), /migration mismatch/);
  assert.match(errors.join("\n"), /primaryKey does not match/);
  assert.match(errors.join("\n"), /observedKeyScope must mirror the source primary key/);
  assert.match(errors.join("\n"), /invalid readers source reference/);
});

test("rejects a missing file for an otherwise valid source reference", () => {
  const schema = deriveSchema(sources);
  const errors = validateRegistry(registry([record()]), schema, (ref) => ref !== "internal/store.go");
  assert.match(errors.join("\n"), /invalid readers source reference internal\/store\.go/);
  assert.match(errors.join("\n"), /invalid writers source reference internal\/store\.go/);
});

test("rejects blank required fields and incomplete retention metadata", () => {
  const schema = deriveSchema(sources);
  const bad = record({ role: " ", retention: { kind: "schema_ttl", interval: "", column: "" } });
  const errors = validateRegistry(registry([bad]), schema);
  assert.match(errors.join("\n"), /required field role is blank/);
  assert.match(errors.join("\n"), /schema_ttl requires interval and column/);
});

