import { defineConfig } from 'vitest/config';

// Firestore emulator transactions retry under contention, and the
// concurrency suites deliberately create contention. The 5s default is not
// enough: a timed-out test leaves in-flight transactions running, which then
// write into the next test's fixtures and produce failures that look like
// product defects but are test bleed.
//
// Suites also run one file at a time. They share emulator collections, so
// running them in parallel would have them wipe each other's fixtures.
export default defineConfig({
  test: {
    testTimeout: 30000,
    hookTimeout: 30000,
    fileParallelism: false,
    include: ['tests/**/*.test.mjs'],
  },
});
