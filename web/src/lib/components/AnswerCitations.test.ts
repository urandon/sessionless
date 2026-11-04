import { fireEvent, render, screen } from '@testing-library/svelte';
import { describe, expect, it, vi } from 'vitest';
import { citationManifest, sourceId } from '$lib/citations/test-fixtures';
import AnswerCitations from './AnswerCitations.svelte';

describe('AnswerCitations', () => {
  it('renders escaped accessible source metadata without automatic I/O', async () => {
    const fetcher = vi.fn();
    vi.stubGlobal('fetch', fetcher);
    try {
      const v = citationManifest();
      v.sources[0]!.title_truncated = true;
      const view = render(AnswerCitations, { manifest: v, runId: v.run_id });
      const link = screen.getByRole('link', { name: /Source 1: <img/ });
      expect(link).toHaveAttribute('href', v.sources[0]!.url);
      expect(link).toHaveAttribute('target', '_blank');
      expect(link).toHaveAttribute('rel', 'external noopener noreferrer');
      expect(link).toHaveAttribute('referrerpolicy', 'no-referrer');
      expect(link).toHaveAttribute('data-sveltekit-preload-data', 'off');
      expect(view.container.querySelector('img, iframe, script, link')).toBeNull();
      expect(screen.getByText('(title abbreviated)')).toBeInTheDocument();
      expect(screen.getByText(/do not establish that the pages were read/)).toBeInTheDocument();
      expect(screen.getByText('2026-11-04T00:00:00.000123Z')).toHaveAttribute(
        'datetime',
        v.sources[0]!.retrieved_at,
      );
      expect(screen.getByText('2026-11-03T12:00:00Z')).toHaveAttribute(
        'datetime',
        v.sources[0]!.published_at,
      );
      link.focus();
      expect(link).toHaveFocus();
      await fireEvent.keyDown(link, { key: 'Tab' });
      expect(fetcher).not.toHaveBeenCalled();
      await view.rerender({ manifest: undefined, runId: undefined });
      expect(screen.queryByRole('link')).not.toBeInTheDocument();
    } finally {
      vi.unstubAllGlobals();
    }
  });

  it('shows no verified badge or source list for an explicit empty manifest', () => {
    const v = citationManifest(0);
    const view = render(AnswerCitations, { manifest: v, runId: v.run_id });
    expect(view.container.textContent).toBe('');
    expect(screen.queryByRole('link')).not.toBeInTheDocument();
  });

  it('does not render a malformed or foreign manifest as trusted links', () => {
    const v = citationManifest();
    v.sources[0]!.url = 'javascript:alert(1)';
    v.sources[0]!.source_id = sourceId(v.sources[0]!.operation_digest, 1, v.sources[0]!.url);
    render(AnswerCitations, { manifest: v, runId: v.run_id });
    expect(screen.queryByRole('link')).not.toBeInTheDocument();
    expect(screen.getByRole('status')).toHaveTextContent('Source references are unavailable.');
  });
});
