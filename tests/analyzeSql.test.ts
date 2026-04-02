import assert from 'node:assert/strict';
import test from 'node:test';

import {
  analyzeSql,
  detectSqlDialect,
  extractStatements,
} from '../src/lib/analyzeSql.ts';

test('analyzeSql keeps SQL Server UPDATE ... FROM write targets and flags write statements', () => {
  const sql = [
    'UPDATE o',
    'SET total = s.total',
    'FROM dbo.orders o',
    'JOIN (',
    '  SELECT order_id, SUM(amount) AS total',
    '  FROM payments',
    '  GROUP BY order_id',
    ') s ON s.order_id = o.id',
  ].join('\n');

  const analysis = analyzeSql(sql, 0, 'sqlserver');

  assert.equal(analysis.statementType, 'update-from');
  assert.equal(analysis.writeTarget, 'dbo.orders');
  assert.deepEqual(
    analysis.tables.map((table) => [table.alias, table.role]),
    [['o', 'target'], ['s', 'join']],
  );
  assert.ok(analysis.flags.some((flag) => flag.title === 'Write statement'));
  assert.ok(analysis.flags.some((flag) => flag.title === 'Subquery detected'));
});

test('detectSqlDialect and analyzeSql handle BigQuery UNNEST plus QUALIFY signals', () => {
  const sql = [
    'SELECT *',
    'FROM `proj.dataset.orders` o',
    'CROSS JOIN UNNEST(o.items) item',
    'QUALIFY ROW_NUMBER() OVER (PARTITION BY o.id ORDER BY item.created_at DESC) = 1',
  ].join('\n');

  const detection = detectSqlDialect(sql);
  const analysis = analyzeSql(sql, 0, 'bigquery');

  assert.equal(detection.dialect, 'bigquery');
  assert.equal(extractStatements(sql, 'bigquery').length, 1);
  assert.ok(analysis.flags.some((flag) => flag.title === 'QUALIFY filter'));
  assert.ok(analysis.flags.some((flag) => flag.title === 'Windowed QUALIFY'));
  assert.ok(
    analysis.tables.some((table) => table.alias === 'item' && table.specialType === 'unnest'),
  );
});
