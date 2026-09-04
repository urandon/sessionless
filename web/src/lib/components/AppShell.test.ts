import { render, screen } from '@testing-library/svelte';
import { describe, expect, it } from 'vitest';

import AppShell from './AppShell.svelte';

describe('AppShell', () => {
  it('provides keyboard and landmark navigation', () => {
    render(AppShell);

    expect(screen.getByRole('link', { name: 'Skip to content' })).toHaveAttribute(
      'href',
      '#main-content',
    );
    expect(screen.getByRole('navigation', { name: 'Primary navigation' })).toBeInTheDocument();
    expect(screen.getByRole('link', { name: 'Workers' })).toHaveAttribute('href', '/workers');
    expect(screen.getByRole('main')).toHaveAttribute('tabindex', '-1');
  });
});
