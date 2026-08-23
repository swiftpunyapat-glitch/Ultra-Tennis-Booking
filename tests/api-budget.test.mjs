import { describe, expect, test } from 'vitest';
import { readdirSync, readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const root = dirname(fileURLToPath(new URL('../package.json', import.meta.url)));
const apiDir = join(root, 'api');
const topLevelFunctions = readdirSync(apiDir).filter(name => name.endsWith('.js')).sort();
const vercel = JSON.parse(readFileSync(join(root, 'vercel.json'), 'utf8'));
const callers = ['admin.html', 'coach.html', 'index.html', ...readdirSync(join(root, 'tests')).filter(name => name.endsWith('.mjs') && name !== 'api-budget.test.mjs')]
  .map(name => name.endsWith('.html') ? join(root, name) : join(root, 'tests', name));

describe('Vercel API budget and legacy-route cutover', () => {
  test('stays below the 12 direct-function Hobby limit after adding AI Report', () => {
    expect(topLevelFunctions).toHaveLength(11);
    expect(topLevelFunctions).toContain('ai-report.js');
  });

  test('removes the stale underscore function but keeps its URL compatible', () => {
    expect(topLevelFunctions).toContain('admin-ops.js');
    expect(topLevelFunctions).not.toContain('admin_ops.js');
    expect(vercel.rewrites).toContainEqual({ source: '/api/admin_ops', destination: '/api/admin-ops' });
  });

  test('all application and test callers use the current hyphenated function', () => {
    const source = callers.map(path => readFileSync(path, 'utf8')).join('\n');
    expect(source).toContain('/api/admin-ops');
    expect(source).not.toContain('/api/admin_ops');
  });
});
