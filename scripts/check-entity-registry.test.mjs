import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
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
  return { schema: "sessionless-ydb-entity-registry/v2", recordCount: records.length, activeTableCount: records.filter(r => !r.retirement).length, records };
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

function retirementFixture() {
  const dir = new URL("../migrations/ydb/", import.meta.url);
  const manifest = JSON.parse(fs.readFileSync(new URL("retirements.json", dir), "utf8"));
  const sources = Object.fromEntries(fs.readdirSync(dir).filter(n => n.endsWith(".sql")).map(n => [n, fs.readFileSync(new URL(n, dir), "utf8")]));
  return { manifest, sources };
}

function nextFixtureMigration(sources, suffix) {
  assert.match(suffix, /^[a-z][a-z0-9_]*\.sql$/);
  let next = 1n;
  for (const name of Object.keys(sources)) {
    const match = name.match(/^(\d+)_/);
    assert.ok(match, `fixture source must have a numeric version: ${name}`);
    const candidate = BigInt(match[1]) + 1n;
    if (candidate > next) next = candidate;
  }
  return `${String(next).padStart(5, "0")}_${suffix}`;
}

test("future fixture versions follow occupied migrations without modifying source", () => {
  const sources = { "00130_existing.sql": "original130", "00131_existing.sql": "original131" };
  const before = structuredClone(sources);
  assert.equal(nextFixtureMigration(sources, "test_entity.sql"), "00132_test_entity.sql");
  assert.deepEqual(sources, before);
});

test("derives four owned retirements without losing historical lineage", () => {
  const {manifest,sources}=retirementFixture();
  assert.deepEqual(manifest.retirements.map(r=>r.table).sort(), ["dispatch_ready","lease_expiry","quota_expiry","telegram_delivery_ready"]);
  const schema=deriveSchema(sources,manifest);
  assert.deepEqual(schema.errors,[]);
  assert.equal(schema.tables.size,105);
  assert.equal(schema.activeTables.size,101);
  assert.equal(schema.dropStatements,4);
  for(const r of manifest.retirements){
    assert.equal(schema.tables.get(r.table).migration,r.createMigration);
    assert.equal(schema.activeTables.has(r.table),false);
    assert.equal(schema.activeTables.get(r.successor).migration,r.successorMigration);
    assert.deepEqual(schema.retirements.get(r.table),{migration:r.migration,successor:r.successor});
  }
  const unrelated=nextFixtureMigration(sources,"new_entity.sql");
  const added=deriveSchema({...sources,[unrelated]:created.replaceAll("tenant_records","unrelated_entity")},manifest);
  assert.deepEqual(added.errors,[]);
  assert.equal(added.tables.size,106);
  assert.equal(added.activeTables.size,102);
  for(const target of ["runs","`runs`"]){
    const legitimate=deriveSchema({...sources,[nextFixtureMigration(sources,"alter_unrelated.sql")]:`-- +goose Up\nALTER TABLE ${target} ADD COLUMN later_note Utf8;\n-- +goose Down\n`},manifest);
    assert.deepEqual(legitimate.errors,[],`legitimate unrelated ALTER ${target}`);
    assert.equal(legitimate.activeTables.size,101);
  }
});

test("rejects unsafe retirement partitions, including later ALTER/reactivation",()=>{
  for(const [name,mutate,want] of [
    ["missing file",f=>delete f.sources["00126_retire_lease_expiry.sql"],/missing declared retirement/],
    ["missing all",f=>{for(const r of f.manifest.retirements)delete f.sources[r.migration]},/missing declared retirement/],
    ["foreign",f=>f.sources["00126_retire_lease_expiry.sql"]=f.sources["00126_retire_lease_expiry.sql"].replace("`lease_expiry`","`runs`"),/unsupported DROP/],
    ["no IF",f=>f.sources["00126_retire_lease_expiry.sql"]=f.sources["00126_retire_lease_expiry.sql"].replace(" IF EXISTS",""),/unsupported DROP/],
    ["extra SQL",f=>f.sources["00126_retire_lease_expiry.sql"]=f.sources["00126_retire_lease_expiry.sql"].replace("-- +goose Down","DELETE FROM runs;\n-- +goose Down"),/exactly one executable/],
    ["duplicate version",f=>f.sources["126_collision.sql"]=f.sources["00126_retire_lease_expiry.sql"],/duplicate migration version/],
    ["duplicate drop",f=>f.sources[nextFixtureMigration(f.sources,"again.sql")]=f.sources["00126_retire_lease_expiry.sql"],/unsupported DROP/],
    ["early DROP",f=>{const r=f.manifest.retirements[0];f.sources["00018_early.sql"]=f.sources[r.migration];delete f.sources[r.migration];r.migration="00018_early.sql"},/early.*retirement/],
    ["missing successor",f=>delete f.sources["00023_create_lease_expiry_v2.sql"],/lineage mismatch/],
    ["missing original",f=>delete f.sources["00019_create_lease_expiry.sql"],/lineage mismatch/],
    ["wrong CREATE",f=>f.manifest.retirements[0].createMigration="00020_create_dispatch_ready.sql",/lineage mismatch/],
    ["missing baseline",f=>delete f.sources["00125_add_owned_operation_residual.sql"],/early.*retirement/],
    ["down SQL",f=>f.sources["00126_retire_lease_expiry.sql"]+="DROP TABLE IF EXISTS `runs`;",/comment-only Down/],
    ["duplicate ownership",f=>f.manifest.retirements.push({...f.manifest.retirements[0]}),/duplicate retirement manifest/],
    ["reactivation",f=>f.sources[nextFixtureMigration(f.sources,"recreate.sql")]=f.sources["00019_create_lease_expiry.sql"],/duplicate CREATE/],
    ["later ALTER",f=>f.sources[nextFixtureMigration(f.sources,"alter.sql")]="-- +goose Up\nALTER TABLE `lease_expiry` ADD COLUMN x Utf8;\n-- +goose Down\n",/cannot ALTER retired/],
    ["bare later ALTER",f=>f.sources[nextFixtureMigration(f.sources,"alter.sql")]="-- +goose Up\nALTER TABLE lease_expiry ADD COLUMN x Utf8;\n-- +goose Down\n",/cannot ALTER retired/],
    ["unparsed ALTER target",f=>f.sources[nextFixtureMigration(f.sources,"alter.sql")]="-- +goose Up\nALTER TABLE namespace.lease_expiry ADD COLUMN x Utf8;\n-- +goose Down\n",/unsupported ALTER target/],
  ]){
    const fixture=retirementFixture();mutate(fixture);
    const schema=deriveSchema(fixture.sources,fixture.manifest);
    const errors=validateRegistry(registry([]),schema);
    assert.match(errors.join("\n"),want,name);
  }
});

test("numeric ordering is not lexical ordering, and retirement registry drift fails closed",()=>{
  const {manifest,sources}=retirementFixture();
  const reordered=Object.fromEntries(Object.entries(sources).reverse().map(([name,sql])=>[name.replace(/^0+/,""),sql]));
  // Bind the declaration to the equally renamed source; numeric ordering must
  // still put original19/successor23 before retirement126.
  const unpadded=structuredClone(manifest);
  for(const r of unpadded.retirements)for(const key of ["migration","createMigration","successorMigration"])r[key]=r[key].replace(/^0+/,"");
  assert.deepEqual(deriveSchema(reordered,unpadded).errors,[]);
  const schema=deriveSchema(sources,manifest);
  const actual=JSON.parse(fs.readFileSync(new URL("../docs/architecture/entity-registry.json",import.meta.url),"utf8"));
  assert.deepEqual(validateRegistry(actual,schema),[]);
  const wrongCount=structuredClone(actual);wrongCount.activeTableCount=105;
  assert.match(validateRegistry(wrongCount,schema).join("\n"),/activeTableCount/);
  const missing=structuredClone(actual);delete missing.records.find(r=>r.table==="lease_expiry").retirement;
  assert.match(validateRegistry(missing,schema).join("\n"),/retirement metadata/);
  const foreign=structuredClone(actual);foreign.records.find(r=>r.table==="runs").retirement={migration:"00126_retire_lease_expiry.sql",successor:"lease_expiry_v2"};
  assert.match(validateRegistry(foreign,schema).join("\n"),/retirement metadata/);
});
