#!/usr/bin/env node

import { access, readdir, readFile, stat } from 'node:fs/promises';
import { dirname, extname, relative, resolve, sep } from 'node:path';

const repoRoot = process.cwd();
const ignoredDirectories = new Set([
  '.build',
  '.git',
  '.gitcode',
  'build',
  'dist',
  'node_modules',
]);

async function collectMarkdown(directory) {
  const entries = await readdir(directory, { withFileTypes: true });
  const files = [];
  for (const entry of entries) {
    if (entry.isDirectory() && ignoredDirectories.has(entry.name)) continue;
    const absolute = resolve(directory, entry.name);
    if (entry.isDirectory()) files.push(...(await collectMarkdown(absolute)));
    if (entry.isFile() && entry.name.endsWith('.md')) files.push(absolute);
  }
  return files;
}

function stripFencedCode(markdown) {
  return markdown.replace(/^\s*(```|~~~)[\s\S]*?^\s*\1.*$/gm, '');
}

function destinations(markdown) {
  const clean = stripFencedCode(markdown);
  const values = [];
  const patterns = [
    /!?\[[^\]]*\]\((<[^>]+>|[^)\s]+)(?:\s+["'][^)]*)?\)/g,
    /^\s*\[[^\]]+\]:\s*(<[^>]+>|\S+)/gm,
    /<(?:a|img)\b[^>]*?\b(?:href|src)=["']([^"']+)["'][^>]*>/gi,
  ];
  for (const pattern of patterns) {
    for (const match of clean.matchAll(pattern)) values.push(match[1]);
  }
  return values.map((value) => value.replace(/^<|>$/g, ''));
}

function githubSlug(value) {
  return value
    .replace(/<[^>]+>/g, '')
    .replace(/[`*_~]/g, '')
    .toLocaleLowerCase('en-US')
    .trim()
    .replace(/[^\p{Letter}\p{Number}\s_-]/gu, '')
    .replace(/\s+/g, '-');
}

function anchors(markdown) {
  const result = new Set();
  const seen = new Map();
  for (const match of stripFencedCode(markdown).matchAll(/^#{1,6}\s+(.+?)\s*#*\s*$/gm)) {
    const base = githubSlug(match[1]);
    const count = seen.get(base) ?? 0;
    result.add(count === 0 ? base : `${base}-${count}`);
    seen.set(base, count + 1);
  }
  for (const match of markdown.matchAll(/<a\b[^>]*?\bid=["']([^"']+)["'][^>]*>/gi)) {
    result.add(match[1]);
  }
  return result;
}

function repoPath(absolute) {
  return relative(repoRoot, absolute).split(sep).join('/');
}

const markdownFiles = (await collectMarkdown(repoRoot)).sort();
const markdownByPath = new Map();
for (const file of markdownFiles) markdownByPath.set(resolve(file), await readFile(file, 'utf8'));

const failures = [];
const inbound = new Map(markdownFiles.map((file) => [resolve(file), new Set()]));

for (const [source, markdown] of markdownByPath) {
  for (const rawDestination of destinations(markdown)) {
    if (!rawDestination || /^(?:[a-z][a-z0-9+.-]*:|\/\/)/i.test(rawDestination)) continue;
    if (rawDestination.startsWith('/')) continue;

    const hashIndex = rawDestination.indexOf('#');
    const rawPath = hashIndex >= 0 ? rawDestination.slice(0, hashIndex) : rawDestination;
    const rawAnchor = hashIndex >= 0 ? rawDestination.slice(hashIndex + 1) : '';
    let decodedPath;
    let decodedAnchor;
    try {
      decodedPath = decodeURIComponent(rawPath.split('?')[0]);
      decodedAnchor = decodeURIComponent(rawAnchor);
    } catch {
      failures.push(`${repoPath(source)}: invalid URL encoding in ${rawDestination}`);
      continue;
    }

    let target = decodedPath ? resolve(dirname(source), decodedPath) : source;
    if (!target.startsWith(`${repoRoot}${sep}`) && target !== repoRoot) {
      failures.push(`${repoPath(source)}: link escapes the repository: ${rawDestination}`);
      continue;
    }

    try {
      const targetStat = await stat(target);
      if (targetStat.isDirectory()) target = resolve(target, 'README.md');
      await access(target);
    } catch {
      failures.push(`${repoPath(source)}: missing target ${rawDestination}`);
      continue;
    }

    if (inbound.has(target) && target !== source) inbound.get(target).add(source);

    if (decodedAnchor && extname(target).toLowerCase() === '.md') {
      const targetMarkdown = markdownByPath.get(target) ?? (await readFile(target, 'utf8'));
      if (!anchors(targetMarkdown).has(decodedAnchor)) {
        failures.push(`${repoPath(source)}: missing anchor #${decodedAnchor} in ${repoPath(target)}`);
      }
    }
  }
}

for (const file of markdownFiles) {
  const path = repoPath(file);
  if (!path.startsWith('docs/') || path === 'docs/README.md' || path === 'docs/assets/README.md') continue;
  if ((inbound.get(file)?.size ?? 0) === 0) failures.push(`${path}: no inbound Markdown link`);
}

if (failures.length > 0) {
  console.error('Markdown documentation check failed:');
  for (const failure of failures) console.error(`- ${failure}`);
  process.exit(1);
}

console.log(`Markdown documentation check passed (${markdownFiles.length} files).`);
