import adapter from '@sveltejs/adapter-static';
import { vitePreprocess } from '@sveltejs/vite-plugin-svelte';

/** @type {import('@sveltejs/kit').Config} */
const config = {
  preprocess: vitePreprocess(),
  kit: {
    version: {
      name: process.env.SESSIONLESS_WEB_VERSION || 'dev',
    },
    adapter: adapter({
      fallback: '200.html',
      precompress: true,
      strict: true,
    }),
  },
};

export default config;
