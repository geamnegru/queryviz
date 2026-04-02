import { chromium } from 'playwright';

const encodeBase64Url = (value) => Buffer.from(value, 'utf8').toString('base64url');

const sql = [
  'SELECT',
  '  oi.order_id,',
  '  oi.line_no,',
  '  si.shipment_id',
  'FROM order_items oi',
  'LEFT JOIN shipment_items si',
  '  ON si.order_id = oi.order_id',
  ' AND si.line_no = oi.line_no;',
].join('\n');

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

const reviewPayload = {
  status: 'needs_changes',
  summary: 'Composite FK is verified. Review link should stay read-only.',
  notes: {
    'flag:Join-heavy query': 'Inspect the fanout before changing the grain.',
  },
};

const browser = await chromium.launch({ headless: true });
const page = await browser.newPage({ viewport: { width: 1600, height: 1200 } });

try {
  await page.goto('http://127.0.0.1:4173/', { waitUntil: 'networkidle' });
  await page.locator('textarea.sql-input').fill(sql);
  await page.locator('textarea.schema-input').fill(schema);
  await page.locator('.join-list-item').first().click();
  await page.locator('.inspector-panel .inspector-badge').getByText('Verified', { exact: true }).waitFor({ state: 'visible' });
  await page.locator('.inspector-panel').getByText('Rewrite guidance').waitFor({ state: 'visible' });
  await page.getByRole('button', { name: 'Review', exact: true }).click();
  await page.getByRole('button', { name: 'Needs changes' }).click();
  await page.locator('textarea.note-input--summary').fill(
    'Composite FK is verified. Review still needs a grain check before shipping.',
  );
  await page.locator('.review-panel').getByText('Rewrite guidance').waitFor({ state: 'visible' });
  await page.locator('.review-panel').getByText('Aggregate si before joining if the parent grain matters').waitFor({
    state: 'visible',
  });
  await page.screenshot({ path: '/tmp/queryviz-playwright-review-mode.png', fullPage: true });

  const reviewUrl = `http://127.0.0.1:4173/#sql=${encodeBase64Url(sql)}&statement=0&dialect=postgres&mode=review&schema=${encodeBase64Url(schema)}&review=${encodeBase64Url(JSON.stringify(reviewPayload))}`;
  const reviewPage = await browser.newPage({ viewport: { width: 1600, height: 1200 } });
  await reviewPage.goto(reviewUrl, { waitUntil: 'networkidle' });
  await reviewPage.getByText('Read-only review page', { exact: true }).waitFor({ state: 'visible' });
  await reviewPage.getByRole('button', { name: 'Review', exact: true }).click();

  const sqlReadOnly = await reviewPage.locator('textarea.sql-input').evaluate((element) => element.readOnly);
  const summaryReadOnly = await reviewPage.locator('textarea.note-input--summary').evaluate((element) => element.readOnly);
  const statusDisabled = await reviewPage.getByRole('button', { name: 'Needs changes' }).evaluate((element) => element.disabled);

  if (!sqlReadOnly || !summaryReadOnly || !statusDisabled) {
    throw new Error('Read-only review page did not lock editing controls.');
  }

  await reviewPage.screenshot({ path: '/tmp/queryviz-playwright-review-readonly.png', fullPage: true });
  await reviewPage.close();
  console.log('queryviz-review-smoke: ok');
} finally {
  await browser.close();
}
