#!/usr/bin/env node
import fs from "node:fs";
import path from "node:path";

const REQUIRED_STRINGS = [
  "role",
  "entityGroup",
  "durabilityReview",
  "proposedDisposition",
];

export function deriveSchema(migrationSources, manifest = { schema: "sessionless-ydb-retirements/v1", retirements: [] }) {
  const tables = new Map();
  const activeTables = new Map();
  const retirements = new Map();
  const duplicates = [];
  const errors = [];
  let createStatements = 0;
  let alterStatements = 0;
  let alterOnlyMigrations = 0;
  let dropStatements = 0;
  const owned = new Map();
  const ownedTables = new Set();
  if (manifest?.schema !== "sessionless-ydb-retirements/v1" || !Array.isArray(manifest.retirements)) {
    errors.push("unsupported retirement manifest");
  } else {
    for (const r of manifest.retirements) {
      const fields = ["table", "createMigration", "successor", "successorMigration", "migration", "afterVersion"];
      if (!r || Object.keys(r).sort().join() !== fields.sort().join() ||
          ![r.table, r.successor].every(x => typeof x === "string" && /^[a-z][a-z0-9_]*$/.test(x)) ||
          ![r.createMigration, r.successorMigration, r.migration].every(x => typeof x === "string" && /^\d+_[a-z0-9_]+\.sql$/.test(x)) ||
          r.table === r.successor || !Number.isSafeInteger(r.afterVersion) || r.afterVersion <= 0 ||
          owned.has(r.migration) || ownedTables.has(r.table)) {
        errors.push("invalid/duplicate retirement manifest identity");
        continue;
      }
      owned.set(r.migration, r);
      ownedTables.add(r.table);
    }
  }
  const versions = new Map();
  const ordered = [];
  for (const [migration, source] of Object.entries(migrationSources)) {
    const name = migration.match(/^(\d+)_[a-z0-9_]+\.sql$/);
    if (!name || BigInt(name[1]) <= 0n) {
      errors.push(`${migration}: invalid migration filename/version`);
      continue;
    }
    const version = BigInt(name[1]);
    if (versions.has(version)) errors.push(`${migration}: duplicate migration version ${version}`);
    versions.set(version, migration);
    ordered.push({ migration, source, version });
  }
  ordered.sort((a, b) => a.version < b.version ? -1 : a.version > b.version ? 1 : a.migration.localeCompare(b.migration));

  for (const { migration, source, version } of ordered) {
    let section = "";
    let sawUp = false;
    let upLines = [];
    for (const line of source.split(/\r?\n/)) {
      const directive = line.match(/^\s*--\s*\+goose\s+(Up|Down)\s*$/i);
      if (directive) {
        section = directive[1].toLowerCase();
        if (section === "up") sawUp = true;
        if (section === "down") break;
        continue;
      }
      if (section !== "up") continue;
      if (line.trimStart().startsWith("--")) continue;
      const commentAt = line.indexOf("--");
      upLines.push(commentAt >= 0 ? line.slice(0, commentAt) : line);
    }

    if (!sawUp) {
      errors.push(`${migration}: missing Goose Up section`);
      continue;
    }
    const upSql = upLines.join("\n").trim();
    if (upSql.includes("/*") || upSql.includes("*/")) {
      errors.push(`${migration}: unsupported block-comment syntax in executable Up; use line comments`);
      continue;
    }
    const statements = upSql.split(";").map((statement) => statement.trim()).filter(Boolean);
    if (statements.length !== 1) {
      errors.push(`${migration}: expected exactly one executable Up DDL statement; found ${statements.length}`);
      if (statements.length === 0) continue;
    }

    for (const statement of statements) {
      const normalized = statement.replace(/\s+/g, " ").trim();
      if (/^CRE\s+ATE\s+TABLE\b/i.test(normalized)) {
        errors.push(`${migration}: unsupported split CREATE keyword`);
        continue;
      }
      if (/^DROP\b/i.test(normalized)) {
        const r = owned.get(migration);
        const match = normalized.match(/^DROP TABLE IF EXISTS `([a-z][a-z0-9_]*)`$/i);
        const down = source.split(/--\s*\+goose\s+Down\s*\r?\n/i)[1];
        const executableDown = down?.split(/\r?\n/).map(line => line.split("--")[0].trim()).filter(Boolean).join(" ");
        if (!r || !match || match[1] !== r.table || statements.length !== 1 || down === undefined || executableDown) {
          errors.push(`${migration}: unsupported DROP operation; expected one owned idempotent DROP and comment-only Down`);
          continue;
        }
        if (version <= BigInt(r.afterVersion) || !versions.has(BigInt(r.afterVersion)) ||
            tables.get(r.table)?.migration !== r.createMigration ||
            activeTables.get(r.successor)?.migration !== r.successorMigration ||
            !activeTables.has(r.table)) {
          errors.push(`${migration}: missing/early/duplicate retirement or CREATE/successor lineage mismatch`);
          continue;
        }
        activeTables.delete(r.table);
        retirements.set(r.table, { migration, successor: r.successor });
        dropStatements += 1;
        continue;
      }

      if (owned.has(migration)) {
        errors.push(`${migration}: declared retirement is not its exact DROP operation`);
        continue;
      }

      const createMatch = normalized.match(/^CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+`([^\`]+)`\s*\(/i);
      if (createMatch) {
        const table = createMatch[1];
        const primaryKeyMatch = statement.match(/\bPRIMARY\s+KEY\s*\(([^)]+)\)/i);
        const primaryKey = primaryKeyMatch
          ? primaryKeyMatch[1].split(",").map((item) => item.trim()).filter(Boolean)
          : [];
        if (primaryKey.length === 0) errors.push(`${migration}: CREATE TABLE ${table} has no supported PRIMARY KEY`);
        createStatements += 1;
        if (tables.has(table)) duplicates.push(table);
        tables.set(table, { migration, primaryKey });
        activeTables.set(table, { migration, primaryKey });
        continue;
      }

      if (/^CREATE\s+TABLE\b/i.test(normalized)) {
        errors.push(`${migration}: unsupported CREATE TABLE syntax; expected CREATE TABLE IF NOT EXISTS`);
        continue;
      }
      if (/^ALTER\s+TABLE\b/i.test(normalized)) {
        const target = normalized.match(/^ALTER TABLE (?:`([a-z][a-z0-9_]*)`|([a-z][a-z0-9_]*)) /i);
        if (!target) {
          errors.push(`${migration}: unsupported ALTER target syntax`);
          continue;
        }
        const name = target[1] ?? target[2];
        if (retirements.has(name.toLowerCase())) errors.push(`${migration}: cannot ALTER retired table ${name}`);
        alterStatements += 1;
        continue;
      }
      errors.push(`${migration}: unsupported executable Up statement (expected owned CREATE/ALTER/DROP)`);
    }
    if (statements.length === 1 && /^ALTER\s+TABLE\b/i.test(statements[0].replace(/\s+/g, " "))) {
      alterOnlyMigrations += 1;
    }
  }
  for (const r of owned.values()) {
    if (retirements.get(r.table)?.migration !== r.migration) errors.push(`${r.table}: missing declared retirement ${r.migration}`);
  }

  return { tables, activeTables, retirements, duplicates, errors, createStatements, alterStatements, alterOnlyMigrations, dropStatements };
}

export function validateRegistry(registry, schema, fileExists = () => true) {
  const errors = [...(schema.errors ?? [])];
  if (!registry || registry.schema !== "sessionless-ydb-entity-registry/v2") {
    errors.push("unsupported or missing registry schema");
  }
  if (!Array.isArray(registry?.records)) {
    errors.push("records must be an array");
    return errors;
  }
  if (registry.recordCount !== registry.records.length) {
    errors.push(`recordCount ${registry.recordCount} does not match records length ${registry.records.length}`);
  }
  if (registry.activeTableCount !== schema.activeTables.size) {
    errors.push(`activeTableCount ${registry.activeTableCount} does not match effective schema ${schema.activeTables.size}`);
  }

  const seen = new Set();
  for (const record of registry.records) {
    const table = record?.table;
    if (typeof table !== "string" || !table.trim()) {
      errors.push("record has blank table name");
      continue;
    }
    if (seen.has(table)) errors.push(`duplicate registry entry: ${table}`);
    seen.add(table);
    if (JSON.stringify(record.retirement ?? null) !== JSON.stringify(schema.retirements.get(table) ?? null)) {
      errors.push(`${table}: retirement metadata does not match ordered source`);
    }

    const actual = schema.tables.get(table);
    if (!actual) errors.push(`registry table is not created by a migration: ${table}`);
    else {
      if (record.migration !== actual.migration) {
        errors.push(`${table}: migration mismatch (registry ${record.migration}; source ${actual.migration})`);
      }
      if (JSON.stringify(record.primaryKey) !== JSON.stringify(actual.primaryKey)) {
        errors.push(`${table}: primaryKey does not match CREATE TABLE source`);
      }
      if (
        record.observedKeyScope?.basis !== "primary_key" ||
        JSON.stringify(record.observedKeyScope?.columns) !== JSON.stringify(actual.primaryKey)
      ) {
        errors.push(`${table}: observedKeyScope must mirror the source primary key`);
      }
    }

    for (const field of REQUIRED_STRINGS) {
      if (typeof record[field] !== "string" || !record[field].trim()) {
        errors.push(`${table}: required field ${field} is blank`);
      }
    }
    if (!Array.isArray(record.primaryKey) || record.primaryKey.length === 0) {
      errors.push(`${table}: primaryKey must contain source columns`);
    }
    if (!record.retention || !["schema_ttl", "unknown"].includes(record.retention.kind)) {
      errors.push(`${table}: retention must state schema_ttl or unknown`);
    } else if (
      record.retention.kind === "schema_ttl" &&
      (!record.retention.interval?.trim() || !record.retention.column?.trim())
    ) {
      errors.push(`${table}: schema_ttl requires interval and column`);
    } else if (
      record.retention.kind === "unknown" &&
      !record.retention.statement?.trim()
    ) {
      errors.push(`${table}: unknown retention requires an explanation`);
    }

    for (const role of ["readers", "writers"]) {
      const refs = record.knownRefs?.[role];
      if (!refs || !Array.isArray(refs.paths) || !Number.isInteger(refs.total) || refs.total < refs.paths.length) {
        errors.push(`${table}: knownRefs.${role} must contain bounded paths and total count`);
        continue;
      }
      for (const ref of refs.paths) {
        if (
          typeof ref !== "string" ||
          !ref.startsWith("internal/") ||
          ref.split("/").includes("..") ||
          !fileExists(ref)
        ) {
          errors.push(`${table}: invalid ${role} source reference ${ref}`);
        }
      }
    }
  }

  for (const table of schema.tables.keys()) {
    if (!seen.has(table)) errors.push(`missing registry entry: ${table}`);
  }
  for (const duplicate of schema.duplicates) {
    errors.push(`duplicate CREATE TABLE in migrations: ${duplicate}`);
  }
  return errors;
}

function main() {
  const root = process.cwd();
  const migrationDir = path.join(root, "migrations/ydb");
  const registryPath = path.join(root, "docs/architecture/entity-registry.json");
  const migrationSources = Object.fromEntries(
    fs.readdirSync(migrationDir)
      .filter((name) => name.endsWith(".sql"))
      .sort()
      .map((name) => [name, fs.readFileSync(path.join(migrationDir, name), "utf8")]),
  );
  const registry = JSON.parse(fs.readFileSync(registryPath, "utf8"));
  const manifest = JSON.parse(fs.readFileSync(path.join(migrationDir, "retirements.json"), "utf8"));
  const schema = deriveSchema(migrationSources, manifest);
  const errors = validateRegistry(
    registry,
    schema,
    (ref) => fs.existsSync(path.join(root, ref)),
  );

  console.log(
    `entity registry: ${schema.tables.size} historical CREATE entries; ${schema.activeTables.size} effective active tables; ${schema.dropStatements} owned DROP operations; ${schema.alterStatements} ALTER-only migrations`,
  );
  if (errors.length) {
    for (const error of errors) console.error(`- ${error}`);
    process.exitCode = 1;
    return;
  }
  console.log("entity registry matches migration names, primary keys, and source references");
}

if (process.argv[1] && path.resolve(process.argv[1]) === path.resolve(new URL(import.meta.url).pathname)) {
  main();
}
