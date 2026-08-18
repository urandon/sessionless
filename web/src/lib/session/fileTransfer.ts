import { md5 } from '@noble/hashes/legacy.js';
import { sha256 } from '@noble/hashes/sha2.js';

import type { DownloadCapability, UploadIntent } from '$lib/api/client';

const hashChunkBytes = 2 * 1024 * 1024;
const maxDownloadBytes = 100 * 1024 * 1024;

export interface FileDigests {
  sha256: string;
  contentMD5: string;
}

export function boundedUTF8(value: string, maxBytes: number, fallback: string): string {
  if (!Number.isSafeInteger(maxBytes) || maxBytes < 1) throw new TypeError('Invalid byte limit.');
  const source = value.trim() || fallback;
  const encoder = new TextEncoder();
  let result = '';
  let bytes = 0;
  for (const character of source) {
    const width = encoder.encode(character).byteLength;
    if (bytes + width > maxBytes) break;
    result += character;
    bytes += width;
  }
  if (result) return result;
  const boundedFallback = Array.from(fallback).find(
    (character) => encoder.encode(character).byteLength <= maxBytes,
  );
  return boundedFallback ?? '_';
}

export async function hashFile(
  file: Blob,
  onProgress: (fraction: number) => void = () => undefined,
): Promise<FileDigests> {
  if (file.size === 0) throw new TypeError('Empty files cannot be uploaded.');
  const sha256Hash = sha256.create();
  const md5Hash = md5.create();
  for (let offset = 0; offset < file.size; offset += hashChunkBytes) {
    const bytes = new Uint8Array(await file.slice(offset, offset + hashChunkBytes).arrayBuffer());
    sha256Hash.update(bytes);
    md5Hash.update(bytes);
    onProgress(Math.min(1, (offset + bytes.byteLength) / file.size));
  }
  return {
    sha256: bytesToHex(sha256Hash.digest()),
    contentMD5: bytesToBase64(md5Hash.digest()),
  };
}

export function putUpload(
  intent: UploadIntent,
  file: Blob,
  onProgress: (fraction: number) => void = () => undefined,
  createXHR: () => XMLHttpRequest = () => new XMLHttpRequest(),
  signal?: AbortSignal,
): Promise<void> {
  return new Promise((resolve, reject) => {
    const xhr = createXHR();
    const cleanup = (): void => signal?.removeEventListener('abort', abort);
    const abort = (): void => xhr.abort();
    xhr.open('PUT', intent.url, true);
    for (const [name, value] of Object.entries(intent.headers)) xhr.setRequestHeader(name, value);
    xhr.upload.addEventListener('progress', (event) => {
      if (event.lengthComputable && event.total > 0) onProgress(event.loaded / event.total);
    });
    xhr.addEventListener('load', () => {
      cleanup();
      if (xhr.status >= 200 && xhr.status < 300) resolve();
      else reject(new Error('Object storage rejected the upload.'));
    });
    xhr.addEventListener('error', () => {
      cleanup();
      reject(new Error('The upload connection failed.'));
    });
    xhr.addEventListener('abort', () => {
      cleanup();
      reject(new DOMException('Upload aborted.', 'AbortError'));
    });
    signal?.addEventListener('abort', abort, { once: true });
    if (signal?.aborted) abort();
    else xhr.send(file);
  });
}

export async function downloadCapability(
  capability: DownloadCapability,
  expectedSize: number,
  suggestedName: string,
  fetcher: typeof fetch = globalThis.fetch.bind(globalThis),
  signal?: AbortSignal,
): Promise<void> {
  const allowedBytes = Math.min(maxDownloadBytes, Math.max(0, expectedSize) + 1);
  if (expectedSize > maxDownloadBytes)
    throw new Error('This file is too large for browser download.');
  const response = await fetcher(capability.url, {
    method: capability.method,
    headers: capability.headers,
    credentials: 'omit',
    referrerPolicy: 'no-referrer',
    signal,
  });
  if (!response.ok || !response.body) throw new Error('The file is temporarily unavailable.');
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      total += value.byteLength;
      if (total > allowedBytes) {
        await reader.cancel();
        throw new Error('The file response was larger than expected.');
      }
      chunks.push(value);
    }
  } finally {
    reader.releaseLock();
  }
  const blob = new Blob(chunks as BlobPart[]);
  const objectURL = URL.createObjectURL(blob);
  try {
    const link = document.createElement('a');
    link.href = objectURL;
    link.download = safeDownloadName(suggestedName);
    link.rel = 'noopener noreferrer';
    link.click();
  } finally {
    URL.revokeObjectURL(objectURL);
  }
}

function bytesToBase64(bytes: Uint8Array): string {
  let binary = '';
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary);
}

function bytesToHex(bytes: Uint8Array): string {
  let hex = '';
  for (const byte of bytes) hex += byte.toString(16).padStart(2, '0');
  return hex;
}

function safeDownloadName(value: string): string {
  const normalized = Array.from(value, (character) => {
    const code = character.charCodeAt(0);
    return character === '/' || character === '\\' || code < 32 || code === 127 ? '_' : character;
  })
    .join('')
    .trim();
  return normalized || 'attachment';
}
