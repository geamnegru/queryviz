import assert from 'node:assert/strict';
import test from 'node:test';

import {
  findForeignKeyPairMatch,
  findSchemaTable,
  getColumnSetCoverage,
  hasForeignKeyPairMatch,
  parseSchemaInput,
} from '../src/lib/schemaMetadata.ts';

test('parseSchemaInput supports ALTER TABLE constraints and composite foreign keys', () => {
  const schema = [
    'CREATE TABLE order_items (',
    '  order_id BIGINT NOT NULL,',
    '  line_no INTEGER NOT NULL,',
    '  product_id BIGINT,',
    '  quantity INTEGER',
    ');',
    'ALTER TABLE order_items ADD CONSTRAINT order_items_pk PRIMARY KEY (order_id, line_no);',
    '',
    'CREATE TABLE shipment_items (',
    '  shipment_id BIGINT NOT NULL,',
    '  order_id BIGINT NOT NULL,',
    '  line_no INTEGER NOT NULL',
    ');',
    'ALTER TABLE shipment_items ADD CONSTRAINT shipment_items_fk FOREIGN KEY (order_id, line_no) REFERENCES order_items(order_id, line_no);',
    'CREATE INDEX idx_shipment_items_keys ON shipment_items(order_id, line_no);',
  ].join('\n');

  const parsed = parseSchemaInput(schema);
  const orderItems = findSchemaTable(parsed.tables, 'order_items');
  const shipmentItems = findSchemaTable(parsed.tables, 'shipment_items');

  assert.equal(parsed.summary.tableCount, 2);
  assert.equal(parsed.summary.foreignKeyCount, 1);
  assert.ok(orderItems);
  assert.ok(shipmentItems);
  assert.deepEqual(orderItems.primaryKey, ['order_id', 'line_no']);
  assert.equal(getColumnSetCoverage(orderItems, ['order_id', 'line_no']), 'primary-key');
  assert.equal(getColumnSetCoverage(shipmentItems, ['order_id', 'line_no']), 'index');
  assert.equal(
    hasForeignKeyPairMatch(
      shipmentItems,
      [
        { column: 'order_id', referenceColumn: 'order_id' },
        { column: 'line_no', referenceColumn: 'line_no' },
      ],
      orderItems,
    ),
    true,
  );
});

test('findSchemaTable resolves qualified and short names', () => {
  const schema = [
    'CREATE TABLE analytics.orders (',
    '  id BIGINT PRIMARY KEY,',
    '  customer_id BIGINT',
    ');',
  ].join('\n');

  const parsed = parseSchemaInput(schema);

  assert.equal(findSchemaTable(parsed.tables, 'analytics.orders')?.normalizedName, 'analytics.orders');
  assert.equal(findSchemaTable(parsed.tables, 'orders')?.normalizedName, 'analytics.orders');
});

test('parseSchemaInput supports dbt manifest metadata', () => {
  const manifest = JSON.stringify(
    {
      nodes: {
        'model.queryviz.orders': {
          resource_type: 'model',
          unique_id: 'model.queryviz.orders',
          schema: 'analytics',
          name: 'orders',
          alias: 'orders',
          columns: {
            id: {
              tests: ['unique'],
              constraints: [{ type: 'primary_key' }],
            },
          },
        },
        'model.queryviz.order_items': {
          resource_type: 'model',
          unique_id: 'model.queryviz.order_items',
          schema: 'analytics',
          name: 'order_items',
          alias: 'order_items',
          columns: {
            order_id: {},
          },
        },
        'test.queryviz.relationships_order_items_order_id__orders_id': {
          resource_type: 'test',
          unique_id: 'test.queryviz.relationships_order_items_order_id__orders_id',
          column_name: 'order_id',
          test_metadata: {
            name: 'relationships',
            kwargs: {
              column_name: 'order_id',
              field: 'id',
            },
          },
          depends_on: {
            nodes: ['model.queryviz.order_items', 'model.queryviz.orders'],
          },
        },
      },
    },
    null,
    2,
  );

  const parsed = parseSchemaInput(manifest);
  const orders = findSchemaTable(parsed.tables, 'analytics.orders');
  const orderItems = findSchemaTable(parsed.tables, 'analytics.order_items');

  assert.equal(parsed.sourceKind, 'dbt-manifest');
  assert.equal(parsed.summary.tableCount, 2);
  assert.ok(orders);
  assert.ok(orderItems);
  assert.deepEqual(orders.primaryKey, ['id']);
  assert.equal(
    findForeignKeyPairMatch(
      orderItems,
      [{ column: 'order_id', referenceColumn: 'id' }],
      orders,
    )?.pairCount,
    1,
  );
});

test('parseSchemaInput supports dbt schema YAML metadata', () => {
  const schemaYaml = [
    'version: 2',
    'models:',
    '  - name: orders',
    '    columns:',
    '      - name: id',
    '        tests:',
    '          - unique',
    '  - name: order_items',
    '    columns:',
    '      - name: order_id',
    '        tests:',
    '          - relationships:',
    '              to: ref("orders")',
    '              field: id',
  ].join('\n');

  const parsed = parseSchemaInput(schemaYaml);
  const orders = findSchemaTable(parsed.tables, 'orders');
  const orderItems = findSchemaTable(parsed.tables, 'order_items');

  assert.equal(parsed.sourceKind, 'dbt-schema-yml');
  assert.equal(parsed.summary.tableCount, 2);
  assert.ok(orders);
  assert.ok(orderItems);
  assert.equal(getColumnSetCoverage(orders, ['id']), 'unique');
  assert.equal(
    hasForeignKeyPairMatch(
      orderItems,
      [{ column: 'order_id', referenceColumn: 'id' }],
      orders,
    ),
    true,
  );
});
