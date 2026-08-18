import { describe, expect, it, vi } from 'vitest';

import { boundedUTF8, hashFile, putUpload } from './fileTransfer';

describe('file transfer', () => {
  it('hashes incrementally and returns canonical digests', async () => {
    const progress = vi.fn();
    const result = await hashFile(new Blob(['abc']), progress);

    expect(result).toEqual({
      sha256: 'ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad',
      contentMD5: 'kAFQmDzST7DWlj99KOF/cg==',
    });
    expect(progress).toHaveBeenLastCalledWith(1);
  });

  it('PUTs with only the exact intent headers and reports progress', async () => {
    const listeners = new Map<string, EventListener>();
    const uploadListeners = new Map<string, EventListener>();
    const xhr = {
      status: 204,
      upload: {
        addEventListener: vi.fn((name: string, listener: EventListener) =>
          uploadListeners.set(name, listener),
        ),
      },
      open: vi.fn(),
      setRequestHeader: vi.fn(),
      addEventListener: vi.fn((name: string, listener: EventListener) =>
        listeners.set(name, listener),
      ),
      send: vi.fn(() => listeners.get('load')?.(new Event('load'))),
    } as unknown as XMLHttpRequest;
    const progress = vi.fn();

    await putUpload(
      {
        upload_id: 'up-1',
        method: 'PUT',
        url: 'https://objects.example/signed',
        headers: { 'Content-MD5': 'digest', 'Content-Type': 'text/plain' },
        expires_at: '2026-08-18T00:00:00Z',
      },
      new Blob(['payload']),
      progress,
      () => xhr,
    );

    expect(xhr.open).toHaveBeenCalledWith('PUT', 'https://objects.example/signed', true);
    expect(xhr.setRequestHeader).toHaveBeenCalledTimes(2);
    expect(xhr.setRequestHeader).toHaveBeenCalledWith('Content-MD5', 'digest');
    expect(xhr.setRequestHeader).toHaveBeenCalledWith('Content-Type', 'text/plain');
    expect(xhr.send).toHaveBeenCalledOnce();
  });

  it('aborts an in-flight object-storage PUT when its lifecycle signal ends', async () => {
    const listeners = new Map<string, EventListener>();
    const xhr = {
      status: 0,
      upload: { addEventListener: vi.fn() },
      open: vi.fn(),
      setRequestHeader: vi.fn(),
      addEventListener: vi.fn((name: string, listener: EventListener) =>
        listeners.set(name, listener),
      ),
      send: vi.fn(),
      abort: vi.fn(() => listeners.get('abort')?.(new Event('abort'))),
    } as unknown as XMLHttpRequest;
    const controller = new AbortController();
    const transfer = putUpload(
      {
        upload_id: 'up-1',
        method: 'PUT',
        url: 'https://objects.example/signed',
        headers: {},
        expires_at: '2026-08-18T00:00:00Z',
      },
      new Blob(['payload']),
      undefined,
      () => xhr,
      controller.signal,
    );

    controller.abort();

    await expect(transfer).rejects.toMatchObject({ name: 'AbortError' });
    expect(xhr.abort).toHaveBeenCalledOnce();
  });

  it('bounds upload metadata by UTF-8 bytes without splitting a Unicode scalar', () => {
    const name = boundedUTF8('😀'.repeat(100), 255, 'attachment');

    expect(new TextEncoder().encode(name)).toHaveLength(252);
    expect(name).toBe('😀'.repeat(63));
    expect(boundedUTF8('   ', 255, 'attachment')).toBe('attachment');
    expect(boundedUTF8('', 127, 'application/octet-stream')).toBe('application/octet-stream');
  });
});
