import { defineConfig } from 'vitest/config';
import base from './vite.config.ts';

// Explicit cross-language smoke; never silently skipped by the normal suite.
// The Make target must first generate real synthetic Go/BFF response bytes.
export default defineConfig({
  ...base,
  test: { ...base.test, include: ['smoke/account-link.test.ts'] },
});
