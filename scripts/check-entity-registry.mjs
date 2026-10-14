#!/usr/bin/env node
import fs from "node:fs";
import path from "node:path";

const REQUIRED_STRINGS = [
  "role",
  "entityGroup",
  "durabilityReview",
  "proposedDisposition",
];

export function deriveSchema(migrationSources) {
  const tables = new Map();
  const duplicates = [];
  const errors = [];
  let createStatements = 0;
  let alterStatements = 0;
  let alterOnlyMigrations = 0;

  for (const [migration, source] of Object.entries(migrationSources).sort(([a], [b]) => a.localeCompare(b))) {
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
        errors.push(`${migration}: unsupported DROP operation`);
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
        continue;
      }

      if (/^CREATE\s+TABLE\b/i.test(normalized)) {
        errors.push(`${migration}: unsupported CREATE TABLE syntax; expected CREATE TABLE IF NOT EXISTS`);
        continue;
      }
      if (/^ALTER\s+TABLE\b/i.test(normalized)) {
        alterStatements += 1;
        continue;
      }
      errors.push(`${migration}: unsupported executable Up statement (expected one CREATE TABLE IF NOT EXISTS or ALTER TABLE)`);
    }
    if (statements.length === 1 && /^ALTER\s+TABLE\b/i.test(statements[0].replace(/\s+/g, " "))) {
      alterOnlyMigrations += 1;
    }
  }

  return { tables, duplicates, errors, createStatements, alterStatements, alterOnlyMigrations };
}

export function validateRegistry(registry, schema, fileExists = () => true) {
  const errors = [...(schema.errors ?? [])];
  if (!registry || registry.schema !== "sessionless-ydb-entity-registry/v1") {
    errors.push("unsupported or missing registry schema");
  }
  if (!Array.isArray(registry?.records)) {
    errors.push("records must be an array");
    return errors;
  }
  if (registry.recordCount !== registry.records.length) {
    errors.push(`recordCount ${registry.recordCount} does not match records length ${registry.records.length}`);
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
  const schema = deriveSchema(migrationSources);
  const errors = validateRegistry(
    registry,
    schema,
    (ref) => fs.existsSync(path.join(root, ref)),
  );

  console.log(
    `entity registry: ${schema.tables.size} CREATE TABLE entries; ${schema.alterStatements} ALTER statements; ${schema.alterOnlyMigrations} ALTER-only migrations`,
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
