import assert from 'node:assert/strict';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const projectRoot = fileURLToPath(new URL('..', import.meta.url));

test('queryviz-checks exits non-zero when findings cross the fail threshold', (t) => {
  const fixtureDir = mkdtempSync(path.join(tmpdir(), 'queryviz-checks-'));
  t.after(() => {
    rmSync(fixtureDir, { recursive: true, force: true });
  });

  const sqlPath = path.join(fixtureDir, 'query.sql');
  writeFileSync(
    sqlPath,
    [
      'SELECT *',
      'FROM orders',
      "WHERE LOWER(country) = 'romania';",
    ].join('\n'),
    'utf8',
  );

  const result = spawnSync(
    process.execPath,
    ['scripts/queryviz-checks.mjs', sqlPath, '--fail-on', 'medium'],
    {
      cwd: projectRoot,
      encoding: 'utf8',
    },
  );

  assert.equal(result.status, 1);
  assert.match(result.stdout, /Wildcard select/i);
  assert.match(result.stdout, /Function inside WHERE/i);
});

test('queryviz-checks can emit JSON output with schema summary', (t) => {
  const fixtureDir = mkdtempSync(path.join(tmpdir(), 'queryviz-checks-'));
  t.after(() => {
    rmSync(fixtureDir, { recursive: true, force: true });
  });

  const sqlPath = path.join(fixtureDir, 'query.sql');
  const schemaPath = path.join(fixtureDir, 'schema.sql');

  writeFileSync(
    sqlPath,
    [
      'SELECT o.id, oi.quantity',
      'FROM orders o',
      'LEFT JOIN order_items oi ON oi.order_id = o.id;',
    ].join('\n'),
    'utf8',
  );

  writeFileSync(
    schemaPath,
    [
      'CREATE TABLE orders (id BIGINT PRIMARY KEY);',
      'CREATE TABLE order_items (id BIGINT PRIMARY KEY, order_id BIGINT NOT NULL REFERENCES orders(id));',
      'CREATE INDEX idx_order_items_order_id ON order_items(order_id);',
    ].join('\n'),
    'utf8',
  );

  const result = spawnSync(
    process.execPath,
    ['scripts/queryviz-checks.mjs', sqlPath, '--format', 'json', '--schema', schemaPath, '--fail-on', 'none'],
    {
      cwd: projectRoot,
      encoding: 'utf8',
    },
  );

  assert.equal(result.status, 0);
  const parsed = JSON.parse(result.stdout);
  assert.equal(parsed.results.length, 1);
  assert.equal(parsed.results[0].schema.tableCount, 2);
  assert.equal(parsed.results[0].schema.foreignKeyCount, 1);
  assert.equal(parsed.results[0].statements[0].statementType, 'select');
});

test('queryviz-checks can emit SARIF output', (t) => {
  const fixtureDir = mkdtempSync(path.join(tmpdir(), 'queryviz-checks-'));
  t.after(() => {
    rmSync(fixtureDir, { recursive: true, force: true });
  });

  const sqlPath = path.join(fixtureDir, 'query.sql');
  writeFileSync(
    sqlPath,
    [
      'SELECT *',
      'FROM orders',
      "WHERE LOWER(country) = 'romania';",
    ].join('\n'),
    'utf8',
  );

  const result = spawnSync(
    process.execPath,
    ['scripts/queryviz-checks.mjs', sqlPath, '--format', 'sarif', '--fail-on', 'none'],
    {
      cwd: projectRoot,
      encoding: 'utf8',
    },
  );

  assert.equal(result.status, 0);
  const parsed = JSON.parse(result.stdout);
  assert.equal(parsed.version, '2.1.0');
  assert.ok(Array.isArray(parsed.runs));
  assert.ok(parsed.runs[0].results.length >= 2);
  assert.ok(parsed.runs[0].tool.driver.rules.some((rule) => rule.id.includes('wildcard-select')));
  assert.ok(parsed.runs[0].results.some((entry) => entry.level === 'warning' || entry.level === 'note'));
});
