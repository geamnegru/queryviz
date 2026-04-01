import {
  useEffect,
  useDeferredValue,
  useEffectEvent,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
  type ChangeEvent,
  type PointerEvent as ReactPointerEvent,
} from 'react';
import { toPng, toSvg } from 'html-to-image';
import './App.css';
import {
  analyzeSql,
  buildGraphvizDot,
  detectSqlDialect,
  diagnoseSqlInput,
  extractStatements,
  SUPPORTED_SQL_DIALECTS,
  type DerivedRelation,
  type JoinRef,
  type Severity,
  type SqlDiagnostic,
  type SqlDialectDetection,
  type SqlDialect,
  type TableRef,
} from './lib/analyzeSql';
import {
  findSchemaTable,
  getColumnSetCoverage,
  hasForeignKeyMatch,
  parseSchemaInput,
  type SchemaTableMetadata,
} from './lib/schemaMetadata';

const POSTGRES_SAMPLE_SQL = `SELECT
  o.id,
  o.created_at,
  c.name AS customer_name,
  SUM(oi.quantity * oi.unit_price) AS total_revenue,
  COUNT(DISTINCT p.id) AS product_count
FROM orders o
INNER JOIN customers c ON c.id = o.customer_id
LEFT JOIN order_items oi ON oi.order_id = o.id
LEFT JOIN products p ON p.id = oi.product_id
WHERE o.created_at >= DATE_TRUNC('month', CURRENT_DATE)
  AND LOWER(c.country) = 'romania'
GROUP BY o.id, o.created_at, c.name
ORDER BY total_revenue DESC
LIMIT 50;`;
const SAMPLE_SQL_BY_DIALECT: Record<DialectMode, string> = {
  postgres: POSTGRES_SAMPLE_SQL,
  mysql: `SELECT
  o.id,
  o.created_at,
  c.name AS customer_name,
  SUM(oi.quantity * oi.unit_price) AS total_revenue
FROM orders o
INNER JOIN customers c ON c.id = o.customer_id
LEFT JOIN order_items oi ON oi.order_id = o.id
WHERE o.created_at >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
  AND LOWER(c.country) = 'romania'
GROUP BY o.id, o.created_at, c.name
ORDER BY total_revenue DESC
LIMIT 50 OFFSET 0;`,
  mariadb: `SELECT
  o.id,
  c.name AS customer_name,
  SUM(oi.quantity) AS total_qty
FROM orders o
STRAIGHT_JOIN customers c ON c.id = o.customer_id
LEFT JOIN order_items oi ON oi.order_id = o.id
WHERE o.created_at >= DATE_SUB(CURDATE(), INTERVAL 14 DAY)
GROUP BY o.id, c.name
ORDER BY total_qty DESC
LIMIT 25;`,
  sqlite: `SELECT
  o.id,
  strftime('%Y-%m-%d', o.created_at) AS order_day,
  c.name AS customer_name,
  COUNT(oi.id) AS item_count
FROM orders o
INNER JOIN customers c ON c.id = o.customer_id
LEFT JOIN order_items oi ON oi.order_id = o.id
WHERE date(o.created_at) >= date('now', '-30 day')
GROUP BY o.id, order_day, c.name
ORDER BY item_count DESC
LIMIT 50;`,
  bigquery: `SELECT
  o.id,
  item.product_id,
  c.name AS customer_name
FROM \`analytics.orders\` o
INNER JOIN \`analytics.customers\` c ON c.id = o.customer_id
LEFT JOIN UNNEST(o.items) AS item
QUALIFY ROW_NUMBER() OVER (PARTITION BY o.id ORDER BY item.product_id) = 1;`,
  sqlserver: `SELECT TOP (25)
  o.id,
  o.created_at,
  c.name AS customer_name
FROM [dbo].[Orders] o WITH (NOLOCK)
INNER JOIN [dbo].[Customers] c ON c.id = o.customer_id
LEFT JOIN [dbo].[OrderItems] oi ON oi.order_id = o.id
WHERE o.created_at >= DATEADD(day, -30, CAST(GETDATE() AS date))
ORDER BY o.created_at DESC
OFFSET 0 ROWS FETCH NEXT 25 ROWS ONLY;`,
  oracle: `SELECT
  o.id,
  o.created_at,
  c.name AS customer_name
FROM orders o
INNER JOIN customers c ON c.id = o.customer_id
LEFT JOIN order_items oi ON oi.order_id = o.id
WHERE TRUNC(o.created_at) >= TRUNC(SYSDATE) - 30
  AND ROWNUM <= 25
ORDER BY o.created_at DESC;`,
  snowflake: `SELECT
  o.id,
  c.name AS customer_name,
  oi.order_id
FROM orders o
INNER JOIN customers c ON c.id = o.customer_id
LEFT JOIN order_items oi ON oi.order_id = o.id
QUALIFY ROW_NUMBER() OVER (PARTITION BY o.id ORDER BY o.created_at DESC) = 1;`,
  duckdb: `SELECT
  o.id,
  c.name AS customer_name
FROM read_parquet('orders.parquet') o
LEFT JOIN customers c ON c.id = o.customer_id
WHERE date_trunc('month', o.created_at) = date_trunc('month', current_date)
LIMIT 25;`,
  redshift: `SELECT
  o.id,
  c.name AS customer_name
FROM orders o
INNER JOIN customers c ON c.id = o.customer_id
QUALIFY ROW_NUMBER() OVER (PARTITION BY o.id ORDER BY o.created_at DESC) = 1;`,
  trino: `SELECT
  o.id,
  item.item_id
FROM hive.sales.orders o
CROSS JOIN UNNEST(o.item_ids) AS item(item_id)
FETCH FIRST 25 ROWS ONLY;`,
};
const SAMPLE_SCHEMA_SQL = `CREATE TABLE customers (
  id BIGINT PRIMARY KEY,
  name TEXT,
  country TEXT
);

CREATE TABLE orders (
  id BIGINT PRIMARY KEY,
  customer_id BIGINT NOT NULL REFERENCES customers(id),
  created_at TIMESTAMP
);

CREATE INDEX idx_orders_customer_id ON orders(customer_id);

CREATE TABLE order_items (
  id BIGINT PRIMARY KEY,
  order_id BIGINT NOT NULL REFERENCES orders(id),
  product_id BIGINT,
  quantity INTEGER,
  unit_price NUMERIC
);

CREATE INDEX idx_order_items_order_id ON order_items(order_id);
`;

const NODE_WIDTH = 188;
const NODE_HEIGHT = 92;
const SQL_EDITOR_LINE_HEIGHT = 24;
const SQL_EDITOR_PADDING = 14;

interface GraphLayout {
  width: number;
  height: number;
  positions: Record<string, { x: number; y: number }>;
}

type LayoutMode = 'horizontal' | 'vertical' | 'radial';
type NodeSide = 'left' | 'right' | 'top' | 'bottom';
type DialectMode = SqlDialect;

interface EdgePort {
  startX: number;
  startY: number;
  endX: number;
  endY: number;
  controlOneX: number;
  controlOneY: number;
  controlTwoX: number;
  controlTwoY: number;
  labelX: number;
  labelY: number;
}

type JoinCardinality = 'N:1' | '1:N' | 'M:N';

interface JoinInsight {
  cardinality: JoinCardinality;
  tone: 'good' | 'caution' | 'review';
  badgeLabel: JoinCardinality;
  badgeHint: string;
  confidence: 'heuristic' | 'verified';
  confidenceLabel: 'Heuristic' | 'Verified';
  labelWidth: number;
  joinLabel: string;
  compactCondition: string;
  fullConditionLabel: string;
  summary: string;
  pairs: JoinConditionPair[];
  indexHints: string[];
  fanoutSeverity: 'none' | 'caution' | 'high';
  fanoutSummary: string;
}

interface SavedQuery {
  id: string;
  title: string;
  sql: string;
  updatedAt: number;
  selectedStatementIndex: number;
  dialect: DialectMode;
}

interface WorkspaceViewState {
  layoutMode: LayoutMode;
  dialect: DialectMode;
  schemaSql: string;
  expandedDerivedIds: string[];
  nodeOffsets: Record<string, { x: number; y: number }>;
  pan: { x: number; y: number };
  zoom: number;
  entityNotes: Record<string, string>;
  compareSql: string;
  compareExplainInput: string;
  updatedAt: number;
}

interface DiagnosticSummary {
  blocking: SqlDiagnostic | null;
  items: SqlDiagnostic[];
}

interface JoinConditionPair {
  sourceAlias: string;
  sourceColumn: string;
  targetAlias: string;
  targetColumn: string;
}

interface AliasFilterContext {
  plainColumns: string[];
  wrappedColumns: string[];
  expressions: string[];
}

interface SearchState {
  active: boolean;
  query: string;
  matchCount: number;
  matchedAliases: Set<string>;
  matchedJoinIds: Set<string>;
}

interface ExplainSignal {
  id: string;
  severity: 'high' | 'medium' | 'low';
  title: string;
  detail: string;
  relationName?: string;
  relationAliases?: string[];
  joinIds?: string[];
  costStart?: number;
  costEnd?: number;
  rowsEstimate?: number;
  actualRows?: number;
  loops?: number;
  actualTimeEnd?: number;
  estimateFactor?: number;
}

interface ExplainSummary {
  items: ExplainSignal[];
  relationSignals: Record<string, ExplainSignal[]>;
  joinSignals: Record<string, ExplainSignal[]>;
  summary: {
    seqScans: number;
    indexedReads: number;
    joinNodes: number;
    sorts: number;
    mappedJoins: number;
    maxCost: number;
    maxRows: number;
    estimateWarnings: number;
  };
}

interface CompareSummary {
  tableDelta: number;
  joinDelta: number;
  flagDelta: number;
  complexityDelta: number;
  planSignalDelta: number;
  seqScanDelta: number;
  joinNodeDelta: number;
  maxCostDelta: number | null;
  maxRowsDelta: number | null;
  addedTables: string[];
  removedTables: string[];
  addedJoins: string[];
  removedJoins: string[];
  addedFlags: string[];
  removedFlags: string[];
  hasPlanComparison: boolean;
}

interface ColumnLineage {
  id: string;
  label: string;
  expression: string;
  alias?: string;
  references: Array<{ alias: string; column: string }>;
  relatedAliases: string[];
  relatedJoinIds: string[];
  functionNames: string[];
  hasAggregation: boolean;
}

interface FanoutImpact {
  severity: 'caution' | 'high';
  viaJoinId: string;
  reason: string;
}

type DetailTab = 'joins' | 'clauses' | 'lineage';

const severityLabel: Record<Severity, string> = {
  high: 'High',
  medium: 'Medium',
  low: 'Low',
};
const layoutModeLabel: Record<LayoutMode, string> = {
  horizontal: 'Horizontal',
  vertical: 'Vertical',
  radial: 'Radial',
};
const DIALECT_OPTIONS = SUPPORTED_SQL_DIALECTS as readonly DialectMode[];
const isSupportedDialect = (value: unknown): value is DialectMode =>
  typeof value === 'string' && DIALECT_OPTIONS.includes(value as DialectMode);
const dialectLabel: Record<DialectMode, string> = {
  postgres: 'Postgres',
  mysql: 'MySQL',
  mariadb: 'MariaDB',
  sqlite: 'SQLite',
  bigquery: 'BigQuery',
  sqlserver: 'SQL Server',
  oracle: 'Oracle',
  snowflake: 'Snowflake',
  duckdb: 'DuckDB',
  redshift: 'Redshift',
  trino: 'Trino',
};

const EDGE_CONDITION_MAX = 30;
const SAVED_QUERIES_KEY = 'queryviz.savedQueries.v1';
const WORKSPACE_VIEW_STATE_KEY = 'queryviz.workspaceState.v1';
const MAX_SAVED_QUERIES = 10;
const MAX_WORKSPACE_VIEW_STATES = 18;
const FLAG_COPY: Record<string, { title: string; description: string }> = {
  'join-heavy query': {
    title: 'Join-heavy query',
    description: 'Run EXPLAIN ANALYZE. Look for Seq Scans on join nodes. If indexes exist, this may be fine.',
  },
  'function inside where': {
    title: 'Function in WHERE',
    description: 'DATE_TRUNC / LOWER in WHERE prevents index use. Extract to a computed column or rewrite the condition.',
  },
  'subquery detected': {
    title: 'Correlated subquery',
    description: 'Executes once per row. Consider rewriting as a LEFT JOIN with aggregation.',
  },
  'wildcard select': {
    title: 'Wildcard SELECT',
    description: 'Replace SELECT * with explicit column list to reduce I/O and avoid schema drift.',
  },
  'straight_join hint': {
    title: 'STRAIGHT_JOIN hint',
    description: 'STRAIGHT_JOIN forces join order. Verify the plan still improves after checking fanout and index usage.',
  },
  'nolock hint': {
    title: 'NOLOCK hint',
    description: 'NOLOCK can return dirty or duplicated rows. Keep it only if stale or inconsistent reads are acceptable.',
  },
  'rownum filter': {
    title: 'ROWNUM filter',
    description: 'ROWNUM is applied before the final ORDER BY in Oracle plans. Confirm the row cap matches what users expect.',
  },
  'qualify filter': {
    title: 'QUALIFY filter',
    description: 'QUALIFY usually implies a window sort. Inspect partition size and sort cost in the plan before calling it cheap.',
  },
  'repeated unnest': {
    title: 'Repeated UNNEST',
    description: 'Multiple UNNEST operations can explode row counts. Validate fanout and aggregate correctness downstream.',
  },
  'flatten relation': {
    title: 'FLATTEN relation',
    description: 'FLATTEN can multiply rows very quickly. Filter nested arrays early and verify row counts before aggregating.',
  },
  'external file scan': {
    title: 'External file scan',
    description: 'File-backed scans are great for exploration, but push filters down early so you do not read more data than needed.',
  },
  'write statement': {
    title: 'Write statement',
    description: 'This statement writes data. Validate the source-side plan and predicates before running it against production tables.',
  },
  'apply operator': {
    title: 'APPLY operator',
    description: 'APPLY can behave like a row-by-row nested loop. Check SHOWPLAN for repeated scans before keeping it.',
  },
  'top without order by': {
    title: 'TOP without ORDER BY',
    description: 'TOP without ORDER BY is nondeterministic. Add an order if callers expect stable or reproducible rows.',
  },
  'connect by recursion': {
    title: 'CONNECT BY recursion',
    description: 'CONNECT BY hierarchies can fan out fast. Inspect row growth and sort cost before assuming the tree is cheap.',
  },
  'wildcard table scan': {
    title: 'Wildcard table scan',
    description: 'Wildcard tables can read many shards at once. Prune with _TABLE_SUFFIX or a tighter source pattern first.',
  },
  'windowed qualify': {
    title: 'Windowed QUALIFY',
    description: 'QUALIFY with ORDER BY usually introduces a window sort. Inspect partition width, repartitioning, and bytes scanned.',
  },
  'unfiltered warehouse join': {
    title: 'Unfiltered warehouse join',
    description: 'Warehouse joins without selective filters can trigger distribution-heavy scans. Check DS_BCAST or repartition steps in the plan.',
  },
  'cross join unnest': {
    title: 'CROSS JOIN UNNEST',
    description: 'CROSS JOIN UNNEST can expand rows aggressively. Validate fanout and downstream aggregate correctness.',
  },
};

const createDotFileName = (statementIndex: number) => `queryviz-statement-${statementIndex + 1}.dot`;
const createPngFileName = (statementIndex: number) => `queryviz-statement-${statementIndex + 1}.png`;
const createSvgFileName = (statementIndex: number) => `queryviz-statement-${statementIndex + 1}.svg`;

const summarizeStatement = (statement: string, index: number) => {
  const normalized = statement.replace(/\s+/g, ' ').trim();
  return `#${index + 1} ${normalized.slice(0, 78)}${normalized.length > 78 ? '...' : ''}`;
};

const getDialectSampleSql = (dialect: DialectMode) => SAMPLE_SQL_BY_DIALECT[dialect] ?? POSTGRES_SAMPLE_SQL;
const formatDialectEvidence = (detection: SqlDialectDetection) =>
  detection.evidence.length > 0 ? detection.evidence.join(' · ') : 'No strong dialect markers yet';
const formatStatementTypeLabel = (statementType: string) =>
  statementType
    .split('-')
    .map((part) => part.toUpperCase())
    .join(' ');
const getSpecialNodeLabel = (table: TableRef) => {
  if (!table.specialType) {
    return null;
  }

  const specialLabels: Record<NonNullable<TableRef['specialType']>, string> = {
    unnest: 'UNNEST',
    flatten: 'FLATTEN',
    function: 'FUNCTION',
    external: 'EXTERNAL',
    temp: 'TEMP',
  };

  return specialLabels[table.specialType];
};

const clamp = (value: number, min: number, max: number) => Math.min(max, Math.max(min, value));

const buildDepthMap = (tables: TableRef[], joins: JoinRef[]) => {
  const depthMap = new Map<string, number>();
  const source = tables[0];

  if (source) {
    depthMap.set(source.alias, 0);
  }

  for (let iteration = 0; iteration < tables.length + joins.length; iteration += 1) {
    let changed = false;

    joins.forEach((join) => {
      const sourceDepth = depthMap.get(join.sourceAlias);
      const targetDepth = depthMap.get(join.targetAlias);
      const nextDepth = (sourceDepth ?? 0) + 1;

      if (targetDepth === undefined || nextDepth > targetDepth) {
        depthMap.set(join.targetAlias, nextDepth);
        changed = true;
      }
    });

    if (!changed) {
      break;
    }
  }

  tables.forEach((table, index) => {
    if (!depthMap.has(table.alias)) {
      depthMap.set(table.alias, index === 0 ? 0 : 1);
    }
  });

  return depthMap;
};

const createHorizontalLayout = (tables: TableRef[], joins: JoinRef[]): GraphLayout => {
  const depthMap = buildDepthMap(tables, joins);
  const columns = new Map<number, TableRef[]>();

  tables.forEach((table) => {
    const depth = depthMap.get(table.alias) ?? 0;
    const bucket = columns.get(depth) ?? [];
    bucket.push(table);
    columns.set(depth, bucket);
  });

  const sortedDepths = Array.from(columns.keys()).sort((left, right) => left - right);
  const maxColumnSize = Math.max(...Array.from(columns.values()).map((bucket) => bucket.length), 1);
  const joinCount = Math.max(0, tables.length - 1);
  const columnGap = joinCount <= 4 ? 320 : joinCount <= 9 ? 360 : 410;
  const rowGap = maxColumnSize <= 3 ? 170 : maxColumnSize <= 6 ? 195 : 225;
  const leftPadding = 110;
  const topPadding = 100;
  const width = Math.max(1500, leftPadding + sortedDepths.length * columnGap + NODE_WIDTH + 200);
  const height = Math.max(960, topPadding * 2 + (maxColumnSize - 1) * rowGap + NODE_HEIGHT);
  const positions: Record<string, { x: number; y: number }> = {};

  sortedDepths.forEach((depth, columnIndex) => {
    const bucket = columns.get(depth) ?? [];
    const bucketHeight = NODE_HEIGHT + rowGap * Math.max(bucket.length - 1, 0);
    const startY = Math.max(topPadding, (height - bucketHeight) / 2);

    bucket.forEach((table, rowIndex) => {
      positions[table.alias] = {
        x: leftPadding + columnIndex * columnGap,
        y: startY + rowIndex * rowGap,
      };
    });
  });

  return { width, height, positions };
};

const createVerticalLayout = (tables: TableRef[], joins: JoinRef[]): GraphLayout => {
  const depthMap = buildDepthMap(tables, joins);
  const rows = new Map<number, TableRef[]>();

  tables.forEach((table) => {
    const depth = depthMap.get(table.alias) ?? 0;
    const bucket = rows.get(depth) ?? [];
    bucket.push(table);
    rows.set(depth, bucket);
  });

  const sortedDepths = Array.from(rows.keys()).sort((left, right) => left - right);
  const maxRowSize = Math.max(...Array.from(rows.values()).map((bucket) => bucket.length), 1);
  const joinCount = Math.max(0, tables.length - 1);
  const columnGap = maxRowSize <= 3 ? 260 : maxRowSize <= 6 ? 228 : 204;
  const rowGap = joinCount <= 4 ? 210 : joinCount <= 9 ? 235 : 260;
  const leftPadding = 120;
  const topPadding = 100;
  const width = Math.max(1500, leftPadding * 2 + NODE_WIDTH + Math.max(0, maxRowSize - 1) * columnGap);
  const height = Math.max(960, topPadding * 2 + NODE_HEIGHT + Math.max(0, sortedDepths.length - 1) * rowGap);
  const positions: Record<string, { x: number; y: number }> = {};

  sortedDepths.forEach((depth, rowIndex) => {
    const bucket = rows.get(depth) ?? [];
    const bucketWidth = NODE_WIDTH + columnGap * Math.max(bucket.length - 1, 0);
    const startX = Math.max(leftPadding, (width - bucketWidth) / 2);

    bucket.forEach((table, columnIndex) => {
      positions[table.alias] = {
        x: startX + columnIndex * columnGap,
        y: topPadding + rowIndex * rowGap,
      };
    });
  });

  return { width, height, positions };
};

const createRadialLayout = (tables: TableRef[], joins: JoinRef[]): GraphLayout => {
  const depthMap = buildDepthMap(tables, joins);
  const rings = new Map<number, TableRef[]>();

  tables.forEach((table) => {
    const depth = depthMap.get(table.alias) ?? 0;
    const bucket = rings.get(depth) ?? [];
    bucket.push(table);
    rings.set(depth, bucket);
  });

  const sortedDepths = Array.from(rings.keys()).sort((left, right) => left - right);
  const maxDepth = Math.max(...sortedDepths, 0);
  const ringGap = maxDepth <= 1 ? 220 : maxDepth <= 3 ? 196 : 172;
  const padding = 240;
  const width = Math.max(1480, padding * 2 + NODE_WIDTH + maxDepth * ringGap * 2);
  const height = Math.max(1120, padding * 2 + NODE_HEIGHT + maxDepth * ringGap * 2);
  const centerX = width / 2;
  const centerY = height / 2;
  const positions: Record<string, { x: number; y: number }> = {};

  sortedDepths.forEach((depth) => {
    const bucket = rings.get(depth) ?? [];

    if (depth === 0) {
      bucket.forEach((table) => {
        positions[table.alias] = {
          x: centerX - NODE_WIDTH / 2,
          y: centerY - NODE_HEIGHT / 2,
        };
      });
      return;
    }

    const radius = depth * ringGap;
    const step = bucket.length <= 1 ? 0 : (Math.PI * 2) / bucket.length;
    const startAngle = bucket.length <= 1 ? -Math.PI / 2 : -Math.PI / 2 + (depth % 2 === 0 ? 0 : step / 2);

    bucket.forEach((table, index) => {
      const angle = startAngle + step * index;

      positions[table.alias] = {
        x: centerX + Math.cos(angle) * radius - NODE_WIDTH / 2,
        y: centerY + Math.sin(angle) * radius - NODE_HEIGHT / 2,
      };
    });
  });

  return { width, height, positions };
};

const createNodeLayout = (tables: TableRef[], joins: JoinRef[], layoutMode: LayoutMode): GraphLayout => {
  if (layoutMode === 'vertical') {
    return createVerticalLayout(tables, joins);
  }

  if (layoutMode === 'radial') {
    return createRadialLayout(tables, joins);
  }

  return createHorizontalLayout(tables, joins);
};

const getPreferredSide = (source: { x: number; y: number }, target: { x: number; y: number }): NodeSide => {
  const dx = target.x + NODE_WIDTH / 2 - (source.x + NODE_WIDTH / 2);
  const dy = target.y + NODE_HEIGHT / 2 - (source.y + NODE_HEIGHT / 2);

  if (Math.abs(dx) >= Math.abs(dy)) {
    return dx >= 0 ? 'right' : 'left';
  }

  return dy >= 0 ? 'bottom' : 'top';
};

const getSideVector = (side: NodeSide) => {
  if (side === 'left') {
    return { x: -1, y: 0 };
  }

  if (side === 'right') {
    return { x: 1, y: 0 };
  }

  if (side === 'top') {
    return { x: 0, y: -1 };
  }

  return { x: 0, y: 1 };
};

const getAnchorPoint = (
  position: { x: number; y: number },
  side: NodeSide,
  index: number,
  count: number,
) => {
  if (side === 'left' || side === 'right') {
    const step = NODE_HEIGHT / (count + 1);

    return {
      x: side === 'right' ? position.x + NODE_WIDTH : position.x,
      y: position.y + step * (index + 1),
    };
  }

  const step = NODE_WIDTH / (count + 1);

  return {
    x: position.x + step * (index + 1),
    y: side === 'bottom' ? position.y + NODE_HEIGHT : position.y,
  };
};

const getBezierMidpoint = (
  start: { x: number; y: number },
  controlOne: { x: number; y: number },
  controlTwo: { x: number; y: number },
  end: { x: number; y: number },
) => {
  const t = 0.5;
  const inverse = 1 - t;

  return {
    x:
      inverse ** 3 * start.x +
      3 * inverse ** 2 * t * controlOne.x +
      3 * inverse * t ** 2 * controlTwo.x +
      t ** 3 * end.x,
    y:
      inverse ** 3 * start.y +
      3 * inverse ** 2 * t * controlOne.y +
      3 * inverse * t ** 2 * controlTwo.y +
      t ** 3 * end.y,
  };
};

const createEdgePortMap = (
  joins: JoinRef[],
  positions: Record<string, { x: number; y: number }>,
  nodeOffsets: Record<string, { x: number; y: number }>,
) => {
  const sides = new Map<string, { sourceSide: NodeSide; targetSide: NodeSide }>();
  const sourceBuckets = new Map<string, JoinRef[]>();
  const targetBuckets = new Map<string, JoinRef[]>();

  joins.forEach((join) => {
    const sourceBase = positions[join.sourceAlias] ?? { x: 0, y: 0 };
    const targetBase = positions[join.targetAlias] ?? { x: 0, y: 0 };
    const sourceOffset = nodeOffsets[join.sourceAlias] ?? { x: 0, y: 0 };
    const targetOffset = nodeOffsets[join.targetAlias] ?? { x: 0, y: 0 };
    const source = { x: sourceBase.x + sourceOffset.x, y: sourceBase.y + sourceOffset.y };
    const target = { x: targetBase.x + targetOffset.x, y: targetBase.y + targetOffset.y };
    const sourceSide = getPreferredSide(source, target);
    const targetSide = getPreferredSide(target, source);

    sides.set(join.id, { sourceSide, targetSide });

    const sourceKey = `${join.sourceAlias}:${sourceSide}`;
    const targetKey = `${join.targetAlias}:${targetSide}`;
    sourceBuckets.set(sourceKey, [...(sourceBuckets.get(sourceKey) ?? []), join]);
    targetBuckets.set(targetKey, [...(targetBuckets.get(targetKey) ?? []), join]);
  });

  const sortBucket = (
    bucket: JoinRef[],
    getSide: (join: JoinRef) => NodeSide,
    getCounterpart: (join: JoinRef) => { x: number; y: number },
  ) => {
    bucket.sort((left, right) => {
      const side = getSide(left);
      const leftPoint = getCounterpart(left);
      const rightPoint = getCounterpart(right);

      return side === 'left' || side === 'right'
        ? leftPoint.y - rightPoint.y
        : leftPoint.x - rightPoint.x;
    });
  };

  sourceBuckets.forEach((bucket) => {
    sortBucket(
      bucket,
      (join) => sides.get(join.id)?.sourceSide ?? 'right',
      (join) => {
        const base = positions[join.targetAlias] ?? { x: 0, y: 0 };
        const offset = nodeOffsets[join.targetAlias] ?? { x: 0, y: 0 };
        return { x: base.x + offset.x, y: base.y + offset.y };
      },
    );
  });

  targetBuckets.forEach((bucket) => {
    sortBucket(
      bucket,
      (join) => sides.get(join.id)?.targetSide ?? 'left',
      (join) => {
        const base = positions[join.sourceAlias] ?? { x: 0, y: 0 };
        const offset = nodeOffsets[join.sourceAlias] ?? { x: 0, y: 0 };
        return { x: base.x + offset.x, y: base.y + offset.y };
      },
    );
  });

  const portMap = new Map<string, EdgePort>();

  joins.forEach((join) => {
    const sourceBase = positions[join.sourceAlias] ?? { x: 0, y: 0 };
    const targetBase = positions[join.targetAlias] ?? { x: 0, y: 0 };
    const sourceOffset = nodeOffsets[join.sourceAlias] ?? { x: 0, y: 0 };
    const targetOffset = nodeOffsets[join.targetAlias] ?? { x: 0, y: 0 };
    const source = { x: sourceBase.x + sourceOffset.x, y: sourceBase.y + sourceOffset.y };
    const target = { x: targetBase.x + targetOffset.x, y: targetBase.y + targetOffset.y };
    const currentSides = sides.get(join.id) ?? { sourceSide: 'right', targetSide: 'left' };
    const sourceKey = `${join.sourceAlias}:${currentSides.sourceSide}`;
    const targetKey = `${join.targetAlias}:${currentSides.targetSide}`;
    const outgoingEdges = sourceBuckets.get(sourceKey) ?? [join];
    const incomingEdges = targetBuckets.get(targetKey) ?? [join];
    const outgoingIndex = Math.max(0, outgoingEdges.findIndex((item) => item.id === join.id));
    const incomingIndex = Math.max(0, incomingEdges.findIndex((item) => item.id === join.id));
    const start = getAnchorPoint(source, currentSides.sourceSide, outgoingIndex, outgoingEdges.length);
    const end = getAnchorPoint(target, currentSides.targetSide, incomingIndex, incomingEdges.length);
    const distance = Math.hypot(end.x - start.x, end.y - start.y);
    const span = clamp(distance * 0.34, 84, 220);
    const sourceVector = getSideVector(currentSides.sourceSide);
    const targetVector = getSideVector(currentSides.targetSide);
    const controlOne = {
      x: start.x + sourceVector.x * span,
      y: start.y + sourceVector.y * span,
    };
    const controlTwo = {
      x: end.x + targetVector.x * span,
      y: end.y + targetVector.y * span,
    };
    const labelPoint = getBezierMidpoint(start, controlOne, controlTwo, end);

    portMap.set(join.id, {
      startX: start.x,
      startY: start.y,
      endX: end.x,
      endY: end.y,
      controlOneX: controlOne.x,
      controlOneY: controlOne.y,
      controlTwoX: controlTwo.x,
      controlTwoY: controlTwo.y,
      labelX: labelPoint.x,
      labelY: labelPoint.y - 10,
    });
  });

  return portMap;
};

const normalizeSpaces = (value: string) => value.replace(/\s+/g, ' ').trim();

const truncateText = (value: string, maxLength: number) => {
  if (value.length <= maxLength) {
    return value;
  }

  return `${value.slice(0, Math.max(0, maxLength - 1)).trimEnd()}…`;
};

const formatPlural = (count: number, singular: string, plural = `${singular}s`) =>
  `${count} ${count === 1 ? singular : plural}`;

const formatCompactNumber = (value: number) => {
  if (!Number.isFinite(value)) {
    return '0';
  }

  if (value >= 1_000_000) {
    return `${(value / 1_000_000).toFixed(value >= 10_000_000 ? 0 : 1)}m`;
  }

  if (value >= 1_000) {
    return `${(value / 1_000).toFixed(value >= 10_000 ? 0 : 1)}k`;
  }

  return value >= 100 ? Math.round(value).toString() : value.toFixed(value >= 10 ? 1 : 2).replace(/\.0+$/, '');
};

const formatPlanMetrics = (signal: ExplainSignal, variant: 'compact' | 'full' = 'compact') => {
  const parts: string[] = [];

  if (signal.costEnd !== undefined) {
    if (signal.costStart !== undefined) {
      parts.push(`cost ${formatCompactNumber(signal.costStart)}..${formatCompactNumber(signal.costEnd)}`);
    } else {
      parts.push(`cost ${formatCompactNumber(signal.costEnd)}`);
    }
  }

  if (signal.rowsEstimate !== undefined) {
    parts.push(`${signal.actualRows !== undefined && variant === 'full' ? 'est' : 'rows'} ${formatCompactNumber(signal.rowsEstimate)}`);
  }

  if (signal.actualRows !== undefined) {
    parts.push(`actual ${formatCompactNumber(signal.actualRows)}`);
  }

  const estimateLabel = getEstimateBadgeLabel(signal.estimateFactor);
  if (estimateLabel) {
    parts.push(variant === 'full' ? estimateLabel.replace(/^Est /, 'estimate ') : estimateLabel);
  }

  if (signal.loops !== undefined && variant === 'full') {
    parts.push(`loops ${formatCompactNumber(signal.loops)}`);
  }

  return parts.join(' · ');
};

const escapeMarkdownCell = (value: string) => value.replace(/\|/g, '\\|').replace(/\n/g, '<br />');

const formatJoinTypeLabel = (joinType: string) => {
  const normalized = normalizeSpaces(joinType);
  if (!normalized) {
    return 'JOIN';
  }

  if (/^(merge|update target)$/i.test(normalized)) {
    return normalized.toUpperCase();
  }

  return /join$/i.test(normalized) ? normalized.toUpperCase() : `${normalized.toUpperCase()} JOIN`;
};

const cleanColumnName = (value: string) => value.replace(/[`"'[\]]/g, '').trim();

const cleanExplainRelationName = (value: string) => {
  const bracketParts = Array.from(value.matchAll(/\[([^\]]+)\]/g)).map((match) => match[1]);
  if (bracketParts.length > 0) {
    const objectParts =
      bracketParts.length >= 4 && /^(?:PK|IX|AK|UQ|FK|IDX)[_A-Z0-9-]*/i.test(bracketParts[bracketParts.length - 1])
        ? bracketParts.slice(0, -1)
        : bracketParts;
    return cleanColumnName(objectParts.join('.').replace(/:/g, '.'));
  }

  return cleanColumnName(
    value
      .replace(/^table\s*=\s*/i, '')
      .replace(/^object:\(/i, '')
      .replace(/[)\]]+$/g, '')
      .replace(/:/g, '.'),
  );
};

const extractExplainRelationName = (line: string) => {
  const patterns = [
    /\b(?:XN|DS_[A-Z_]+)?\s*Seq Scan on ([a-zA-Z0-9_."`[\]#@]+)/i,
    /\b(?:XN|DS_[A-Z_]+)?\s*Seq Scan\s+([a-zA-Z0-9_."`[\]#@]+)/i,
    /\b(?:Index Scan|Index Only Scan|Bitmap Heap Scan|Bitmap Index Scan) (?:using [^\s]+ )?on ([a-zA-Z0-9_."`[\]#@]+)/i,
    /\b(?:Table Scan|Index Seek|Index Scan|Clustered Index Scan|Clustered Index Seek|Columnstore Index Scan|Columnstore Index Seek|Remote Scan|Key Lookup|RID Lookup)\(OBJECT:\(([^)]+)\)\)/i,
    /\bTABLE ACCESS (?:FULL|BY INDEX ROWID|STORAGE FULL)\s+([A-Z0-9_.$#@"`]+)/i,
    /\bTableScan\[(?:[^\]]*?table\s*=\s*)?([^\]]+)\]/i,
    /\bScanFilterProject\[(?:[^\]]*?table\s*=\s*)?([^\]]+)\]/i,
    /\b(?:JoinBuild|JoinProbe)\[(?:[^\]]*?table\s*=\s*)?([^\]]+)\]/i,
    /\bREAD\s+(?:FROM\s+)?([a-zA-Z0-9_."`[\]#@:-]+)/i,
    /\b(?:S3|TABLE|FILE)\s+SCAN\s+(?:ON\s+)?([a-zA-Z0-9_."`[\]#@:/-]+)/i,
    /\b(?:FROM|TABLE):\s*([a-zA-Z0-9_."`[\]#@:-]+)/i,
    /\bExternal Scan on ([a-zA-Z0-9_."`[\]#@]+)/i,
  ] as const;

  for (const pattern of patterns) {
    const match = line.match(pattern);
    if (match?.[1]) {
      return cleanExplainRelationName(match[1]);
    }
  }

  return null;
};

const getEstimateFactor = (rowsEstimate?: number, actualRows?: number) => {
  if (
    rowsEstimate === undefined ||
    actualRows === undefined ||
    !Number.isFinite(rowsEstimate) ||
    !Number.isFinite(actualRows) ||
    rowsEstimate <= 0 ||
    actualRows < 0
  ) {
    return undefined;
  }

  if (actualRows === 0) {
    return 0;
  }

  return actualRows / rowsEstimate;
};

const getEstimateSeverity = (factor?: number) => {
  if (factor === undefined) {
    return null;
  }

  if (factor >= 10 || factor <= 0.1) {
    return 'high';
  }

  if (factor >= 4 || factor <= 0.25) {
    return 'medium';
  }

  return 'low';
};

const formatEstimateDelta = (factor?: number) => {
  if (factor === undefined || !Number.isFinite(factor) || factor <= 0) {
    return '';
  }

  return factor >= 1
    ? `${factor.toFixed(factor >= 10 ? 0 : 1)}x high`
    : `${(1 / factor).toFixed(1)}x low`;
};

const getEstimateBadgeLabel = (factor?: number) => {
  const severity = getEstimateSeverity(factor);
  if (!severity || severity === 'low') {
    return null;
  }

  return `Est ${formatEstimateDelta(factor)}`;
};

const getExplainNodeLabel = (node: Record<string, unknown>) => {
  const candidates = [
    node['Node Type'],
    node.NodeType,
    node.operation,
    node.Operation,
    node.name,
    node.Name,
    node.displayName,
    node.kind,
    node.stepType,
    node['@type'],
    node['@name'],
  ];

  const label = candidates.find((value) => typeof value === 'string' && value.trim().length > 0);
  return typeof label === 'string' ? label.trim() : '';
};

const getExplainRelationNameFromNode = (node: Record<string, unknown>) => {
  const extractNestedRelation = (value: unknown): string => {
    if (typeof value === 'string') {
      return extractExplainRelationName(value) ?? '';
    }

    if (Array.isArray(value)) {
      for (const item of value) {
        const candidate = extractNestedRelation(item);
        if (candidate) {
          return candidate;
        }
      }
      return '';
    }

    if (isRecord(value)) {
      for (const nestedValue of Object.values(value)) {
        const candidate = extractNestedRelation(nestedValue);
        if (candidate) {
          return candidate;
        }
      }
    }

    return '';
  };

  const directCandidates = [
    node['Relation Name'],
    node.RelationName,
    node.relationName,
    node.table,
    node.tableName,
    node.object,
    node.objectName,
    node.scanTarget,
    node.alias,
    node.Alias,
    node.source,
  ];

  const direct = directCandidates.find((value) => typeof value === 'string' && value.trim().length > 0);
  if (typeof direct === 'string') {
    return cleanExplainRelationName(direct);
  }

  const listCandidates = [node.tables, node.relations, node.objects];
  for (const candidate of listCandidates) {
    if (!Array.isArray(candidate)) {
      continue;
    }

    const firstString = candidate.find((value) => typeof value === 'string' && value.trim().length > 0);
    if (typeof firstString === 'string') {
      return cleanExplainRelationName(firstString);
    }
  }

  const nestedStatistics = isRecord(node.statistics) ? node.statistics : null;
  const nestedTable = nestedStatistics?.tableName;
  if (typeof nestedTable === 'string' && nestedTable.trim()) {
    return cleanExplainRelationName(nestedTable);
  }

  const nestedRelation = extractNestedRelation(node.steps) || extractNestedRelation(node.substeps) || extractNestedRelation(node.statistics);
  if (nestedRelation) {
    return nestedRelation;
  }

  const stringValues = Object.values(node).filter((value): value is string => typeof value === 'string');
  for (const candidate of stringValues) {
    const extracted = extractExplainRelationName(candidate);
    if (extracted) {
      return extracted;
    }
  }

  return '';
};

const getExplainNumericValue = (node: Record<string, unknown>, keys: string[]) => {
  for (const key of keys) {
    const rawValue = node[key];
    const numericValue =
      typeof rawValue === 'number'
        ? rawValue
        : typeof rawValue === 'string'
          ? Number(rawValue.replace(/,/g, '').trim())
          : Number.NaN;

    if (Number.isFinite(numericValue)) {
      return numericValue;
    }
  }

  return undefined;
};

const getExplainNodeChildren = (node: Record<string, unknown>) => {
  const childKeys = [
    'Plans',
    'plans',
    'children',
    'Children',
    'inputStages',
    'subStages',
    'sources',
    'inputs',
    'Nodes',
    'nodes',
    'operations',
    'Operations',
    'queryPlan',
    'stages',
    'Stage List',
  ];

  const children: Record<string, unknown>[] = [];
  childKeys.forEach((key) => {
    const value = node[key];
    if (!Array.isArray(value)) {
      return;
    }

    value.forEach((candidate) => {
      if (isRecord(candidate)) {
        children.push(candidate);
      }
    });
  });

  return children;
};

const extractExplainPlanRoots = (parsed: unknown): Record<string, unknown>[] => {
  if (!isRecord(parsed) && !Array.isArray(parsed)) {
    return [];
  }

  if (Array.isArray(parsed)) {
    return parsed.flatMap((item) => extractExplainPlanRoots(item));
  }

  if (isRecord(parsed.Plan)) {
    return [parsed.Plan];
  }

  if (Array.isArray(parsed.queryPlan)) {
    const roots = parsed.queryPlan.filter(isRecord);
    if (roots.length > 0) {
      return roots;
    }
  }

  const childRoots = getExplainNodeChildren(parsed);
  if (childRoots.length > 0 && getExplainNodeLabel(parsed)) {
    return [parsed];
  }

  if (getExplainNodeLabel(parsed)) {
    return [parsed];
  }

  const nestedCandidates = ['plan', 'Plan', 'executionPlan', 'ExecutionPlan', 'profile', 'Profile'];
  for (const key of nestedCandidates) {
    const value = parsed[key];
    if (!value) {
      continue;
    }
    const roots = extractExplainPlanRoots(value);
    if (roots.length > 0) {
      return roots;
    }
  }

  return [];
};

const classifyExplainOperation = (label: string, detail: string) => {
  const source = `${label} ${detail}`;

  if (/\b(seq(?:[_\s-]+)?scan|table(?:[_\s-]+)?scan|tablescan|remote(?:[_\s-]+)?scan|external(?:[_\s-]+)?scan|table access full|table access storage full|full table scan|distributed scan|read\b|scanfilterproject|s3(?:[_\s-]+)?scan|file(?:[_\s-]+)?scan)\b/i.test(source)) {
    return 'scan';
  }

  if (/\b(index(?:[_\s-]+)?scan|index(?:[_\s-]+)?only(?:[_\s-]+)?scan|bitmap(?:[_\s-]+)?heap(?:[_\s-]+)?scan|bitmap(?:[_\s-]+)?index(?:[_\s-]+)?scan|index(?:[_\s-]+)?seek|clustered(?:[_\s-]+)?index(?:[_\s-]+)?scan|clustered(?:[_\s-]+)?index(?:[_\s-]+)?seek|columnstore(?:[_\s-]+)?index(?:[_\s-]+)?scan|columnstore(?:[_\s-]+)?index(?:[_\s-]+)?seek|key(?:[_\s-]+)?lookup|rid(?:[_\s-]+)?lookup|table access by index rowid|index range scan|index unique scan|index fast full scan|index full scan)\b/i.test(source)) {
    return 'index';
  }

  if (/\b(nested(?:[_\s-]+)?loops?|nested(?:[_\s-]+)?loop|hash(?:[_\s-]+)?join|merge(?:[_\s-]+)?join|broadcast(?:[_\s-]+)?hash(?:[_\s-]+)?join|broadcast(?:[_\s-]+)?join|left(?:[_\s-]+)?semi(?:[_\s-]+)?join|right(?:[_\s-]+)?semi(?:[_\s-]+)?join|left(?:[_\s-]+)?anti(?:[_\s-]+)?join|right(?:[_\s-]+)?anti(?:[_\s-]+)?join|hash(?:[_\s-]+)?left(?:[_\s-]+)?join|hash(?:[_\s-]+)?right(?:[_\s-]+)?join|hash(?:[_\s-]+)?full(?:[_\s-]+)?join|hash match\s*\(\s*(?:inner join|left semi join|right semi join)\s*\)|left(?:[_\s-]+)?outer(?:[_\s-]+)?join|right(?:[_\s-]+)?outer(?:[_\s-]+)?join|full(?:[_\s-]+)?outer(?:[_\s-]+)?join|joinbuild|joinprobe|join\b|apply\b|ds_bcast_[a-z_]+|ds_dist_[a-z_]+|xn hash join|xn merge join|xn nested loop)\b/i.test(source)) {
    return 'join';
  }

  if (/\b(sort|top(?:[_\s-]+)?n(?:[_\s-]+)?sort|order by|analytic[_\s-]?sort|partition(?:[_\s-]+)?sort|window(?:[_\s-]+)?sort)\b/i.test(source)) {
    return 'sort';
  }

  if (/\b(aggregate|groupaggregate|hashaggregate|stream(?:[_\s-]+)?aggregate|hash match\s*\(\s*aggregate\s*\)|group by aggregate|scalar(?:[_\s-]+)?aggregate)\b/i.test(source)) {
    return 'aggregate';
  }

  if (/\b(filter|predicate|qualify)\b/i.test(source)) {
    return 'filter';
  }

  return 'other';
};

const collectAliasFilterContext = (filters: string[]) => {
  const context: Record<string, AliasFilterContext> = {};

  filters.forEach((filter) => {
    const expressions = normalizeSpaces(filter);
    const wrappedMatches = Array.from(expressions.matchAll(/([a-zA-Z_][\w$]*)\s*\(\s*([a-zA-Z_][\w$]*)\.([a-zA-Z_][\w$`"'[\]]*)/gi));
    const plainMatches = Array.from(expressions.matchAll(/([a-zA-Z_][\w$]*)\.([a-zA-Z_][\w$`"'[\]]*)/gi));

    plainMatches.forEach((match) => {
      const alias = match[1];
      const column = cleanColumnName(match[2]);
      const next = context[alias] ?? { plainColumns: [], wrappedColumns: [], expressions: [] };
      if (!next.plainColumns.includes(column)) {
        next.plainColumns.push(column);
      }
      if (!next.expressions.includes(expressions)) {
        next.expressions.push(expressions);
      }
      context[alias] = next;
    });

    wrappedMatches.forEach((match) => {
      const alias = match[2];
      const column = cleanColumnName(match[3]);
      const next = context[alias] ?? { plainColumns: [], wrappedColumns: [], expressions: [] };
      if (!next.wrappedColumns.includes(column)) {
        next.wrappedColumns.push(column);
      }
      if (!next.expressions.includes(expressions)) {
        next.expressions.push(expressions);
      }
      context[alias] = next;
    });
  });

  return context;
};

const buildLineagePath = (joins: JoinRef[], rootAlias: string, targetAlias: string) => {
  if (rootAlias === targetAlias) {
    return {
      aliases: [rootAlias],
      joinIds: [],
    };
  }

  const adjacency = new Map<string, Array<{ alias: string; joinId: string }>>();
  joins.forEach((join) => {
    adjacency.set(join.sourceAlias, [...(adjacency.get(join.sourceAlias) ?? []), { alias: join.targetAlias, joinId: join.id }]);
    adjacency.set(join.targetAlias, [...(adjacency.get(join.targetAlias) ?? []), { alias: join.sourceAlias, joinId: join.id }]);
  });

  const queue: Array<{ alias: string; aliases: string[]; joinIds: string[] }> = [
    { alias: rootAlias, aliases: [rootAlias], joinIds: [] },
  ];
  const visited = new Set<string>([rootAlias]);

  while (queue.length > 0) {
    const current = queue.shift();
    if (!current) {
      break;
    }

    for (const next of adjacency.get(current.alias) ?? []) {
      if (visited.has(next.alias)) {
        continue;
      }

      const candidate = {
        alias: next.alias,
        aliases: [...current.aliases, next.alias],
        joinIds: [...current.joinIds, next.joinId],
      };

      if (next.alias === targetAlias) {
        return candidate;
      }

      visited.add(next.alias);
      queue.push(candidate);
    }
  }

  return {
    aliases: [targetAlias],
    joinIds: [],
  };
};

const buildColumnLineage = (columns: Array<{ expression: string; alias?: string }>, joins: JoinRef[], rootAlias: string) =>
  columns.map((column, index) => {
    const references = Array.from(
      column.expression.matchAll(/([a-zA-Z_][\w$]*)\.([a-zA-Z_][\w$`"'[\]]*)/g),
    ).map((match) => ({
      alias: match[1],
      column: cleanColumnName(match[2]),
    }));
    const uniqueAliases = Array.from(new Set(references.map((reference) => reference.alias)));
    const relatedAliases = new Set<string>();
    const relatedJoinIds = new Set<string>();

    if (/\*/.test(column.expression) && uniqueAliases.length === 0) {
      joins.forEach((join) => {
        relatedAliases.add(join.sourceAlias);
        relatedAliases.add(join.targetAlias);
        relatedJoinIds.add(join.id);
      });
      relatedAliases.add(rootAlias);
    }

    uniqueAliases.forEach((alias) => {
      const path = buildLineagePath(joins, rootAlias, alias);
      path.aliases.forEach((item) => relatedAliases.add(item));
      path.joinIds.forEach((item) => relatedJoinIds.add(item));
    });

    const functionNames = Array.from(
      new Set(Array.from(column.expression.matchAll(/\b([a-zA-Z_][\w$]*)\s*\(/g)).map((match) => match[1].toUpperCase())),
    );

    return {
      id: `lineage-${index}`,
      label: column.alias ?? truncateText(normalizeSpaces(column.expression), 42),
      expression: column.expression,
      alias: column.alias,
      references,
      relatedAliases: Array.from(relatedAliases),
      relatedJoinIds: Array.from(relatedJoinIds),
      functionNames,
      hasAggregation: /\b(count|sum|avg|min|max)\s*\(/i.test(column.expression),
    } satisfies ColumnLineage;
  });

const normalizeEntityBase = (value: string) => {
  const cleaned = cleanColumnName(value).toLowerCase();
  const withoutSchema = cleaned.split('.').pop() ?? cleaned;
  const tokens = withoutSchema.split(/[^a-z0-9]+/).filter(Boolean);
  const base = tokens[tokens.length - 1] ?? withoutSchema;

  if (base.endsWith('ies') && base.length > 3) {
    return `${base.slice(0, -3)}y`;
  }

  if (base.endsWith('sses')) {
    return base.slice(0, -2);
  }

  if (base.endsWith('s') && !base.endsWith('ss') && base.length > 3) {
    return base.slice(0, -1);
  }

  return base;
};

const inferColumnRole = (columnName: string, ownerTableName?: string, counterpartTableName?: string) => {
  const normalized = cleanColumnName(columnName).toLowerCase();
  const ownerBase = ownerTableName ? normalizeEntityBase(ownerTableName) : '';
  const counterpartBase = counterpartTableName ? normalizeEntityBase(counterpartTableName) : '';
  const ownerPrefixes = ownerBase ? [ownerBase, ownerBase.replace(/_/g, '')] : [];
  const counterpartPrefixes = counterpartBase ? [counterpartBase, counterpartBase.replace(/_/g, '')] : [];

  const isOwnerNamedKey = ownerPrefixes.some((prefix) =>
    prefix && [`${prefix}_id`, `${prefix}_uuid`, `${prefix}_key`, `${prefix}_pk`].includes(normalized),
  );
  const isCounterpartNamedKey = counterpartPrefixes.some((prefix) =>
    prefix && [`${prefix}_id`, `${prefix}_uuid`, `${prefix}_key`, `${prefix}_fk`].includes(normalized),
  );

  if (
    normalized === 'id' ||
    normalized === 'uuid' ||
    normalized === 'guid' ||
    normalized === 'pk' ||
    normalized.endsWith('_pk') ||
    isOwnerNamedKey
  ) {
    return 'pk';
  }

  if (
    normalized === 'fk' ||
    normalized.endsWith('_fk') ||
    normalized.endsWith('_id') ||
    normalized.endsWith('_uuid') ||
    (normalized.endsWith('_key') && !isOwnerNamedKey) ||
    isCounterpartNamedKey
  ) {
    return 'fk';
  }

  return 'unknown';
};

const extractJoinConditionPairs = (join: JoinRef): JoinConditionPair[] => {
  const equalityRegex =
    /([a-zA-Z_][\w$]*)\.([`"'[\]a-zA-Z_][\w$`"'[\]]*)\s*=\s*([a-zA-Z_][\w$]*)\.([`"'[\]a-zA-Z_][\w$`"'[\]]*)/gi;

  return Array.from(join.condition.matchAll(equalityRegex))
    .map((match) => {
      const leftAlias = match[1];
      const leftColumn = match[2];
      const rightAlias = match[3];
      const rightColumn = match[4];

      if (leftAlias === join.sourceAlias && rightAlias === join.targetAlias) {
        return {
          sourceAlias: leftAlias,
          sourceColumn: cleanColumnName(leftColumn),
          targetAlias: rightAlias,
          targetColumn: cleanColumnName(rightColumn),
        };
      }

      if (leftAlias === join.targetAlias && rightAlias === join.sourceAlias) {
        return {
          sourceAlias: rightAlias,
          sourceColumn: cleanColumnName(rightColumn),
          targetAlias: leftAlias,
          targetColumn: cleanColumnName(leftColumn),
        };
      }

      return null;
    })
    .filter((value): value is JoinConditionPair => value !== null);
};

const describeCoverage = (coverage: ReturnType<typeof getColumnSetCoverage>) => {
  if (coverage === 'primary-key') {
    return 'PRIMARY KEY';
  }

  if (coverage === 'unique') {
    return 'UNIQUE';
  }

  if (coverage === 'index') {
    return 'INDEX';
  }

  return null;
};

const buildIndexHints = (
  pairs: JoinConditionPair[],
  cardinality: JoinCardinality,
  filterContext: Record<string, AliasFilterContext>,
  dialect: DialectMode,
  schemaByAlias: Record<string, SchemaTableMetadata | null> = {},
) => {
  if (pairs.length === 0) {
    return ['Consider indexes on the columns used in the ON condition.'];
  }

  const hints = new Set<string>();
  const recommendedColumns = new Map<string, Set<string>>();

  const collectColumns = (alias: string, column: string) => {
    const next = recommendedColumns.get(alias) ?? new Set<string>();
    next.add(column);
    recommendedColumns.set(alias, next);
  };

  pairs.forEach((pair) => {
    if (cardinality === 'N:1') {
      collectColumns(pair.sourceAlias, pair.sourceColumn);
      return;
    }

    if (cardinality === '1:N') {
      collectColumns(pair.targetAlias, pair.targetColumn);
      return;
    }

    collectColumns(pair.sourceAlias, pair.sourceColumn);
    collectColumns(pair.targetAlias, pair.targetColumn);
  });

  recommendedColumns.forEach((columns, alias) => {
    const aliasFilters = filterContext[alias];
    const candidates = [...columns];
    const schemaTable = schemaByAlias[alias];

    aliasFilters?.plainColumns.forEach((column) => {
      if (!candidates.includes(column) && candidates.length < 3) {
        candidates.push(column);
      }
    });

    const coverage = describeCoverage(getColumnSetCoverage(schemaTable, candidates));
    if (coverage) {
      hints.add(`Imported schema already shows ${coverage} coverage on ${alias}(${candidates.join(', ')}).`);
    } else {
      hints.add(`Index likely helpful on ${alias}(${candidates.join(', ')}).`);
      if (schemaTable) {
        hints.add(`Imported schema does not currently show index coverage on ${alias}(${candidates.join(', ')}).`);
      }
    }

    if (aliasFilters?.wrappedColumns.length) {
      hints.add(
        `${dialectLabel[dialect]} note: function-wrapped filters on ${alias}(${aliasFilters.wrappedColumns.join(', ')}) may still block index use.`,
      );
    }
  });

  if (pairs.length >= 2) {
    const pairAliases = Array.from(new Set(pairs.map((pair) => pair.sourceAlias)));
    pairAliases.forEach((alias) => {
      const columns = pairs.filter((pair) => pair.sourceAlias === alias).map((pair) => pair.sourceColumn);
      if (columns.length >= 2) {
        hints.add(`Composite index worth testing on ${alias}(${Array.from(new Set(columns)).join(', ')}).`);
      }
    });
  }

  return Array.from(hints);
};

const classifyColumnPair = (
  sourceColumn: string,
  targetColumn: string,
  sourceTableName?: string,
  targetTableName?: string,
  sourceSchema?: SchemaTableMetadata | null,
  targetSchema?: SchemaTableMetadata | null,
): JoinCardinality => {
  if (hasForeignKeyMatch(sourceSchema, [sourceColumn], targetSchema, [targetColumn])) {
    return 'N:1';
  }

  if (hasForeignKeyMatch(targetSchema, [targetColumn], sourceSchema, [sourceColumn])) {
    return '1:N';
  }

  const sourceRole = inferColumnRole(sourceColumn, sourceTableName, targetTableName);
  const targetRole = inferColumnRole(targetColumn, targetTableName, sourceTableName);
  const normalizedSourceColumn = cleanColumnName(sourceColumn).toLowerCase();
  const normalizedTargetColumn = cleanColumnName(targetColumn).toLowerCase();
  const sourceBase = sourceTableName ? normalizeEntityBase(sourceTableName) : '';
  const targetBase = targetTableName ? normalizeEntityBase(targetTableName) : '';

  const sourceReferencesTarget = targetBase
    ? [`${targetBase}_id`, `${targetBase}_uuid`, `${targetBase}_key`, `${targetBase}_fk`].includes(normalizedSourceColumn)
    : false;
  const targetReferencesSource = sourceBase
    ? [`${sourceBase}_id`, `${sourceBase}_uuid`, `${sourceBase}_key`, `${sourceBase}_fk`].includes(normalizedTargetColumn)
    : false;

  if ((sourceRole === 'fk' && targetRole === 'pk') || sourceReferencesTarget) {
    return 'N:1';
  }

  if ((sourceRole === 'pk' && targetRole === 'fk') || targetReferencesSource) {
    return '1:N';
  }

  return 'M:N';
};

const inferJoinInsight = (
  join: JoinRef,
  sourceTableName?: string,
  targetTableName?: string,
  filterContext: Record<string, AliasFilterContext> = {},
  dialect: DialectMode = 'postgres',
  sourceSchema?: SchemaTableMetadata | null,
  targetSchema?: SchemaTableMetadata | null,
) => {
  const pairs = extractJoinConditionPairs(join);
  const pairClassifications = pairs.map((pair) =>
    classifyColumnPair(pair.sourceColumn, pair.targetColumn, sourceTableName, targetTableName, sourceSchema, targetSchema),
  );
  const hasVerifiedPair = pairs.some(
    (pair) =>
      hasForeignKeyMatch(sourceSchema, [pair.sourceColumn], targetSchema, [pair.targetColumn]) ||
      hasForeignKeyMatch(targetSchema, [pair.targetColumn], sourceSchema, [pair.sourceColumn]),
  );

  const cardinality =
    pairClassifications.length > 0 && pairClassifications.every((value) => value === pairClassifications[0])
      ? pairClassifications[0]
      : 'M:N';
  const joinLabel = formatJoinTypeLabel(join.type);
  const compactCondition = truncateText(normalizeSpaces(`ON ${join.condition}`), EDGE_CONDITION_MAX);
  const fullConditionLabel = normalizeSpaces(`ON ${join.condition}`);
  const labelWidth = Math.max(110, Math.min(268, Math.max(joinLabel.length * 8.2, compactCondition.length * 6.4) + 24));
  const indexHints = buildIndexHints(
    pairs,
    cardinality,
    filterContext,
    dialect,
    {
      [join.sourceAlias]: sourceSchema ?? null,
      [join.targetAlias]: targetSchema ?? null,
    },
  );
  const confidence = hasVerifiedPair ? 'verified' : 'heuristic';
  const confidenceLabel = hasVerifiedPair ? 'Verified' : 'Heuristic';

  if (cardinality === 'N:1') {
    return {
      cardinality,
      tone: 'good',
      badgeLabel: cardinality,
      badgeHint: hasVerifiedPair
        ? 'Verified: imported schema shows source FK -> target key coverage. Preferred join shape.'
        : 'Heuristic: source looks like FK -> target PK. Preferred join shape.',
      confidence,
      confidenceLabel,
      labelWidth,
      joinLabel,
      compactCondition,
      fullConditionLabel,
      summary: hasVerifiedPair
        ? 'Imported schema confirms an FK -> key join. This is the healthiest join direction and usually preserves row counts.'
        : 'FK -> PK join. This is usually the healthiest join direction and tends to preserve row counts.',
      pairs,
      indexHints,
      fanoutSeverity: 'none',
      fanoutSummary: 'This join shape is unlikely to multiply rows on its own.',
    };
  }

  if (cardinality === '1:N') {
    return {
      cardinality,
      tone: 'caution',
      badgeLabel: cardinality,
      badgeHint: hasVerifiedPair
        ? 'Verified: imported schema shows target FK back to the source key. This can multiply rows downstream.'
        : 'Heuristic: source looks like PK -> target FK. This can multiply rows downstream.',
      confidence,
      confidenceLabel,
      labelWidth,
      joinLabel,
      compactCondition,
      fullConditionLabel,
      summary: hasVerifiedPair
        ? 'Imported schema confirms a PK -> FK join. This can multiply rows downstream, especially before aggregates.'
        : 'PK -> FK join. This can multiply rows downstream, especially before aggregates.',
      pairs,
      indexHints,
      fanoutSeverity: 'caution',
      fanoutSummary: 'Possible row multiplication starts here and propagates to downstream joins and aggregates.',
    };
  }

  return {
    cardinality,
    tone: 'review',
    badgeLabel: cardinality,
    badgeHint: 'Heuristic: non-key columns or mixed predicates. Worth reviewing for fanout risk.',
    confidence,
    confidenceLabel,
    labelWidth,
    joinLabel,
    compactCondition,
    fullConditionLabel,
    summary: 'Non-key or mixed join predicates. Treat the fanout as ambiguous until you inspect it.',
    pairs,
    indexHints,
    fanoutSeverity: 'high',
    fanoutSummary: 'Ambiguous join keys can create fanout quickly. Validate row counts before aggregating.',
  };
};

const getFlagContent = (title: string, fallbackDescription: string) => {
  const mapped = FLAG_COPY[title.toLowerCase()];
  return mapped ?? { title, description: fallbackDescription };
};

const encodeBase64Url = (value: string) => {
  const bytes = new TextEncoder().encode(value);
  let binary = '';

  bytes.forEach((byte) => {
    binary += String.fromCharCode(byte);
  });

  return btoa(binary).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/g, '');
};

const decodeBase64Url = (value: string) => {
  const normalized = value.replace(/-/g, '+').replace(/_/g, '/');
  const padding = '='.repeat((4 - (normalized.length % 4 || 4)) % 4);
  const binary = atob(`${normalized}${padding}`);
  const bytes = Uint8Array.from(binary, (char) => char.charCodeAt(0));
  return new TextDecoder().decode(bytes);
};

const readSavedQueries = (): SavedQuery[] => {
  if (typeof window === 'undefined') {
    return [];
  }

  try {
    const stored = window.localStorage.getItem(SAVED_QUERIES_KEY);
    if (!stored) {
      return [];
    }

    const parsed = JSON.parse(stored);
    if (!Array.isArray(parsed)) {
      return [];
    }

    return parsed
      .filter((item): item is SavedQuery | (Omit<SavedQuery, 'dialect'> & { dialect?: DialectMode }) =>
        typeof item?.id === 'string' &&
        typeof item?.title === 'string' &&
        typeof item?.sql === 'string' &&
        typeof item?.updatedAt === 'number' &&
        typeof item?.selectedStatementIndex === 'number' &&
        (item?.dialect === undefined || isSupportedDialect(item?.dialect)),
      )
      .map((item) => ({
        ...item,
        dialect: item.dialect ?? 'postgres',
      }));
  } catch {
    return [];
  }
};

const persistSavedQueries = (queries: SavedQuery[]) => {
  if (typeof window === 'undefined') {
    return;
  }

  window.localStorage.setItem(SAVED_QUERIES_KEY, JSON.stringify(queries));
};

const createQueryTitle = (sql: string) => {
  const normalized = normalizeSpaces(sql);

  if (!normalized) {
    return 'Untitled query';
  }

  return truncateText(normalized, 56);
};

const createSavedQueryId = () =>
  globalThis.crypto?.randomUUID?.() ?? `query-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;

const createShareUrl = (sql: string, statementIndex: number, dialect: DialectMode) => {
  if (typeof window === 'undefined') {
    return '';
  }

  const params = new URLSearchParams();
  params.set('sql', encodeBase64Url(sql));
  params.set('statement', String(statementIndex));
  params.set('dialect', dialect);

  return `${window.location.origin}${window.location.pathname}#${params.toString()}`;
};

const readSharedWorkspace = (): { sql: string; selectedStatementIndex: number; dialect: DialectMode } | null => {
  if (typeof window === 'undefined' || !window.location.hash) {
    return null;
  }

  const params = new URLSearchParams(window.location.hash.slice(1));
  const encodedSql = params.get('sql');
  if (!encodedSql) {
    return null;
  }

  try {
    const sql = decodeBase64Url(encodedSql);
    const selectedStatementIndex = Math.max(0, Number(params.get('statement') ?? '0') || 0);
    const rawDialect = params.get('dialect');
    const dialect = isSupportedDialect(rawDialect) ? rawDialect : 'postgres';

    return { sql, selectedStatementIndex, dialect };
  } catch {
    return null;
  }
};

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === 'object' && value !== null && !Array.isArray(value);

const isPoint = (value: unknown): value is { x: number; y: number } =>
  isRecord(value) && typeof value.x === 'number' && typeof value.y === 'number';

const sanitizeNoteRecord = (value: unknown) => {
  if (!isRecord(value)) {
    return {};
  }

  return Object.fromEntries(
    Object.entries(value).filter((entry): entry is [string, string] => typeof entry[1] === 'string'),
  );
};

const sanitizeOffsets = (value: unknown) => {
  if (!isRecord(value)) {
    return {};
  }

  return Object.fromEntries(
    Object.entries(value).filter((entry): entry is [string, { x: number; y: number }] => isPoint(entry[1])),
  );
};

const readWorkspaceViewStates = (): Record<string, WorkspaceViewState> => {
  if (typeof window === 'undefined') {
    return {};
  }

  try {
    const stored = window.localStorage.getItem(WORKSPACE_VIEW_STATE_KEY);
    if (!stored) {
      return {};
    }

    const parsed = JSON.parse(stored);
    if (!isRecord(parsed)) {
      return {};
    }

    return Object.fromEntries(
      Object.entries(parsed)
        .map(([key, rawValue]) => {
          if (!isRecord(rawValue)) {
            return null;
          }

          const layoutMode =
            rawValue.layoutMode === 'horizontal' ||
            rawValue.layoutMode === 'vertical' ||
            rawValue.layoutMode === 'radial'
              ? rawValue.layoutMode
              : 'horizontal';
          const dialect = isSupportedDialect(rawValue.dialect) ? rawValue.dialect : 'postgres';
          const zoom = typeof rawValue.zoom === 'number' ? rawValue.zoom : 1;
          const updatedAt = typeof rawValue.updatedAt === 'number' ? rawValue.updatedAt : 0;

          return [
            key,
            {
              layoutMode,
              dialect,
              schemaSql: typeof rawValue.schemaSql === 'string' ? rawValue.schemaSql : '',
              expandedDerivedIds: Array.isArray(rawValue.expandedDerivedIds)
                ? rawValue.expandedDerivedIds.filter((item): item is string => typeof item === 'string')
                : [],
              nodeOffsets: sanitizeOffsets(rawValue.nodeOffsets),
              pan: isPoint(rawValue.pan) ? rawValue.pan : { x: 0, y: 0 },
              zoom,
              entityNotes: sanitizeNoteRecord(rawValue.entityNotes),
              compareSql: typeof rawValue.compareSql === 'string' ? rawValue.compareSql : '',
              compareExplainInput: typeof rawValue.compareExplainInput === 'string' ? rawValue.compareExplainInput : '',
              updatedAt,
            } satisfies WorkspaceViewState,
          ] as const;
        })
        .filter((entry): entry is readonly [string, WorkspaceViewState] => entry !== null),
    );
  } catch {
    return {};
  }
};

const persistWorkspaceViewStates = (states: Record<string, WorkspaceViewState>) => {
  if (typeof window === 'undefined') {
    return;
  }

  window.localStorage.setItem(WORKSPACE_VIEW_STATE_KEY, JSON.stringify(states));
};

const createWorkspaceStateKey = (sql: string, statementIndex: number, dialect: DialectMode) => {
  const normalized = normalizeSpaces(sql);
  if (!normalized) {
    return '';
  }

  const encoded = encodeBase64Url(`${dialect}:${statementIndex}:${normalized}`);
  return `ws-${encoded.slice(0, 72)}-${normalized.length}`;
};

const saveWorkspaceViewState = (
  key: string,
  state: Omit<WorkspaceViewState, 'updatedAt'>,
) => {
  if (!key || typeof window === 'undefined') {
    return;
  }

  const current = readWorkspaceViewStates();
  current[key] = {
    ...state,
    updatedAt: Date.now(),
  };

  const trimmedEntries = Object.entries(current)
    .sort((left, right) => right[1].updatedAt - left[1].updatedAt)
    .slice(0, MAX_WORKSPACE_VIEW_STATES);

  persistWorkspaceViewStates(Object.fromEntries(trimmedEntries));
};

const readWorkspaceViewState = (key: string) => {
  if (!key) {
    return null;
  }

  return readWorkspaceViewStates()[key] ?? null;
};

const getTableLookupKeys = (table: TableRef) => [
  table.alias.toLowerCase(),
  table.name.toLowerCase(),
  table.name.split('.').pop()?.toLowerCase() ?? table.name.toLowerCase(),
];

const createEmptyExplainSummary = (): ExplainSummary => ({
  items: [],
  relationSignals: {},
  joinSignals: {},
  summary: {
    seqScans: 0,
    indexedReads: 0,
    joinNodes: 0,
    sorts: 0,
    mappedJoins: 0,
    maxCost: 0,
    maxRows: 0,
    estimateWarnings: 0,
  },
});

const deriveExplainSeverity = (defaultSeverity: ExplainSignal['severity'], costEnd?: number, rowsEstimate?: number) => {
  if ((costEnd ?? 0) >= 1000 || (rowsEstimate ?? 0) >= 100000) {
    return 'high';
  }

  if ((costEnd ?? 0) >= 120 || (rowsEstimate ?? 0) >= 5000) {
    return defaultSeverity === 'low' ? 'medium' : defaultSeverity;
  }

  return defaultSeverity;
};

const createExplainContext = (tables: TableRef[]) => {
  const tableIndex = new Map<string, Set<string>>();

  tables.forEach((table) => {
    getTableLookupKeys(table)
      .map((key) => cleanColumnName(key).toLowerCase())
      .filter(Boolean)
      .forEach((key) => {
        const aliases = tableIndex.get(key) ?? new Set<string>();
        aliases.add(table.alias);
        tableIndex.set(key, aliases);
      });
  });

  return {
    getAliasesForRelation: (relationName: string) => {
      const normalized = cleanColumnName(relationName).toLowerCase();
      const shortName = normalized.split('.').pop() ?? normalized;
      const aliases = new Set<string>();

      (tableIndex.get(normalized) ?? new Set()).forEach((alias) => aliases.add(alias));
      (tableIndex.get(shortName) ?? new Set()).forEach((alias) => aliases.add(alias));

      return Array.from(aliases);
    },
  };
};

const mapJoinSignals = (
  items: ExplainSignal[],
  relationSignals: Record<string, ExplainSignal[]>,
  joins: JoinRef[],
) => {
  const joinSignals: Record<string, ExplainSignal[]> = {};
  const pushSignal = (key: string, signal: ExplainSignal) => {
    const nextList = joinSignals[key] ?? [];
    if (nextList.some((existing) => existing.id === signal.id)) {
      return;
    }
    joinSignals[key] = [...nextList, signal];
  };

  items.forEach((signal) => {
    const normalizedTitle = signal.title.toLowerCase();
    if (!normalizedTitle.includes('join') && !normalizedTitle.includes('loop')) {
      return;
    }

    let matchedJoinIds = signal.joinIds ?? [];

    if (matchedJoinIds.length === 0 && signal.relationAliases && signal.relationAliases.length >= 2) {
      const aliasSet = new Set(signal.relationAliases);
      matchedJoinIds = joins
        .filter((join) => aliasSet.has(join.sourceAlias) && aliasSet.has(join.targetAlias))
        .map((join) => join.id);
    }

    if (matchedJoinIds.length === 0) {
      const nearbyAliases = new Set<string>();

      Object.entries(relationSignals).forEach(([alias, signals]) => {
        if (signals.some((candidate) => candidate.id === signal.id)) {
          nearbyAliases.add(alias);
        }
      });

      if (nearbyAliases.size >= 2) {
        matchedJoinIds = joins
          .filter((join) => nearbyAliases.has(join.sourceAlias) && nearbyAliases.has(join.targetAlias))
          .map((join) => join.id);
      }
    }

    matchedJoinIds.forEach((joinId) => {
      pushSignal(joinId, { ...signal, joinIds: matchedJoinIds });
    });
  });

  return joinSignals;
};

const parseSqlServerShowplanXml = (input: string, tables: TableRef[], joins: JoinRef[]): ExplainSummary | null => {
  if (!/showplanxml/i.test(input) || typeof DOMParser === 'undefined') {
    return null;
  }

  let documentRoot: Document;

  try {
    documentRoot = new DOMParser().parseFromString(input, 'application/xml');
  } catch {
    return null;
  }

  if (documentRoot.querySelector('parsererror')) {
    return null;
  }

  const relOps = Array.from(documentRoot.getElementsByTagName('RelOp'));
  if (relOps.length === 0) {
    return null;
  }

  const { getAliasesForRelation } = createExplainContext(tables);
  const relationSignals: Record<string, ExplainSignal[]> = {};
  const items: ExplainSignal[] = [];
  let seqScans = 0;
  let indexedReads = 0;
  let joinNodes = 0;
  let sorts = 0;
  let maxCost = 0;
  let maxRows = 0;
  let estimateWarnings = 0;
  let counter = 0;

  const pushRelationSignal = (alias: string, signal: ExplainSignal) => {
    const nextList = relationSignals[alias] ?? [];
    if (nextList.some((existing) => existing.id === signal.id)) {
      return;
    }
    relationSignals[alias] = [...nextList, signal];
  };

  relOps.forEach((relOp) => {
    const physicalOp = relOp.getAttribute('PhysicalOp')?.trim() ?? '';
    const logicalOp = relOp.getAttribute('LogicalOp')?.trim() ?? '';
    const estimateRowsRaw = Number(relOp.getAttribute('EstimateRows'));
    const subtreeCostRaw = Number(relOp.getAttribute('EstimatedTotalSubtreeCost'));
    const actualRowsRaw = Number(relOp.querySelector('RunTimeCountersPerThread')?.getAttribute('ActualRows'));
    const actualExecutionsRaw = Number(relOp.querySelector('RunTimeCountersPerThread')?.getAttribute('ActualExecutions'));
    const costEnd = Number.isFinite(subtreeCostRaw) ? subtreeCostRaw : undefined;
    const rowsEstimate = Number.isFinite(estimateRowsRaw) ? estimateRowsRaw : undefined;
    const actualRows = Number.isFinite(actualRowsRaw) ? actualRowsRaw : undefined;
    const loops = Number.isFinite(actualExecutionsRaw) ? actualExecutionsRaw : undefined;
    const objectNodes = Array.from(relOp.getElementsByTagName('Object'));
    const relationNames = objectNodes
      .map((node) => {
        const table = node.getAttribute('Table');
        const schema = node.getAttribute('Schema');
        const database = node.getAttribute('Database');
        const alias = node.getAttribute('Alias');
        const relation = [database, schema, table].filter(Boolean).join('.');
        return cleanExplainRelationName(relation || alias || '');
      })
      .filter(Boolean);
    const descendantAliases = new Set<string>();

    relationNames.forEach((relationName) => {
      getAliasesForRelation(relationName).forEach((alias) => descendantAliases.add(alias));
    });

    const estimateFactor = getEstimateFactor(rowsEstimate, actualRows);
    if (getEstimateSeverity(estimateFactor) && getEstimateSeverity(estimateFactor) !== 'low') {
      estimateWarnings += 1;
    }

    if (costEnd !== undefined) {
      maxCost = Math.max(maxCost, costEnd);
    }

    if (rowsEstimate !== undefined) {
      maxRows = Math.max(maxRows, rowsEstimate);
    }

    const opLabel = physicalOp || logicalOp;
    const baseDetail = relationNames.length > 0 ? `${opLabel} on ${relationNames.join(', ')}` : opLabel;
    let signal: ExplainSignal | null = null;

    if (/(table scan|clustered index scan|scan)$/i.test(physicalOp) && !/index seek|key lookup|rid lookup/i.test(physicalOp)) {
      seqScans += 1;
      signal = {
        id: `xml-seq-${counter += 1}`,
        severity: deriveExplainSeverity('high', costEnd, rowsEstimate),
        title: physicalOp || logicalOp || 'Table Scan',
        detail: baseDetail,
        relationName: relationNames[0],
        relationAliases: Array.from(descendantAliases),
        costEnd,
        rowsEstimate,
        actualRows,
        loops,
        estimateFactor,
      };
    } else if (/index seek|index scan|key lookup|rid lookup|columnstore/i.test(physicalOp)) {
      indexedReads += 1;
      signal = {
        id: `xml-index-${counter += 1}`,
        severity: deriveExplainSeverity('low', costEnd, rowsEstimate),
        title: physicalOp || logicalOp || 'Index access',
        detail: baseDetail,
        relationName: relationNames[0],
        relationAliases: Array.from(descendantAliases),
        costEnd,
        rowsEstimate,
        actualRows,
        loops,
        estimateFactor,
      };
    } else if (/join|nested loops|apply|merge/i.test(physicalOp) || /join/i.test(logicalOp)) {
      joinNodes += 1;
      signal = {
        id: `xml-join-${counter += 1}`,
        severity: deriveExplainSeverity(/nested loops/i.test(physicalOp) ? 'high' : 'medium', costEnd, rowsEstimate),
        title: physicalOp || logicalOp || 'Join',
        detail: baseDetail,
        relationAliases: Array.from(descendantAliases),
        costEnd,
        rowsEstimate,
        actualRows,
        loops,
        estimateFactor,
      };
    } else if (/sort/i.test(physicalOp) || /sort/i.test(logicalOp)) {
      sorts += 1;
      signal = {
        id: `xml-sort-${counter += 1}`,
        severity: deriveExplainSeverity('medium', costEnd, rowsEstimate),
        title: physicalOp || logicalOp || 'Sort',
        detail: baseDetail,
        relationAliases: Array.from(descendantAliases),
        costEnd,
        rowsEstimate,
        actualRows,
        loops,
        estimateFactor,
      };
    }

    if (signal) {
      items.push(signal);
      Array.from(descendantAliases).forEach((alias) => pushRelationSignal(alias, signal as ExplainSignal));
    }
  });

  const joinSignals = mapJoinSignals(items, relationSignals, joins);

  return {
    items,
    relationSignals,
    joinSignals,
    summary: {
      seqScans,
      indexedReads,
      joinNodes,
      sorts,
      mappedJoins: Object.keys(joinSignals).length,
      maxCost,
      maxRows,
      estimateWarnings,
    },
  };
};

const parseExplainJsonInput = (input: string, tables: TableRef[], joins: JoinRef[]): ExplainSummary | null => {
  if (!/^(?:\[|{)/.test(input.trim())) {
    return null;
  }

  let parsed: unknown;

  try {
    parsed = JSON.parse(input);
  } catch {
    return null;
  }

  const roots = extractExplainPlanRoots(parsed);
  if (roots.length === 0) {
    return null;
  }

  const { getAliasesForRelation } = createExplainContext(tables);
  const relationSignals: Record<string, ExplainSignal[]> = {};
  const items: ExplainSignal[] = [];
  let seqScans = 0;
  let indexedReads = 0;
  let joinNodes = 0;
  let sorts = 0;
  let maxCost = 0;
  let maxRows = 0;
  let estimateWarnings = 0;
  let counter = 0;

  const pushRelationSignal = (alias: string, signal: ExplainSignal) => {
    const nextList = relationSignals[alias] ?? [];
    if (nextList.some((existing) => existing.id === signal.id)) {
      return;
    }
    relationSignals[alias] = [...nextList, signal];
  };

  const visit = (node: Record<string, unknown>): Set<string> => {
    const nodeType = getExplainNodeLabel(node);
    const relationName = getExplainRelationNameFromNode(node);
    const costStart = getExplainNumericValue(node, ['Startup Cost', 'StartupCost', 'startupCost', 'startCost']);
    const costEnd = getExplainNumericValue(node, [
      'Total Cost',
      'TotalCost',
      'totalCost',
      'cumulativeCost',
      'cost',
      'estimatedCost',
      'bytesRead',
      'estimatedBytesRead',
    ]);
    const rowsEstimate = getExplainNumericValue(node, [
      'Plan Rows',
      'PlanRows',
      'planRows',
      'rows',
      'Rows',
      'estimatedRows',
      'EstimatedRows',
      'recordsRead',
      'statistics.rows',
    ]);
    const actualRows =
      getExplainNumericValue(node, ['Actual Rows', 'ActualRows', 'actualRows', 'recordsWritten']) ??
      (isRecord(node.statistics) ? getExplainNumericValue(node.statistics, ['outputRows', 'rowsProduced']) : undefined);
    const loops = getExplainNumericValue(node, ['Actual Loops', 'ActualLoops', 'actualLoops', 'loops']);
    const actualTimeEnd = getExplainNumericValue(node, [
      'Actual Total Time',
      'ActualTotalTime',
      'actualTotalTime',
      'elapsedMs',
      'elapsedTime',
      'executionTime',
    ]);
    const estimateFactor = getEstimateFactor(rowsEstimate, actualRows);
    const childPlans = getExplainNodeChildren(node);
    const relationAliases = relationName ? getAliasesForRelation(relationName) : [];
    const descendantAliases = new Set<string>(relationAliases);

    childPlans.forEach((child) => {
      visit(child).forEach((alias) => descendantAliases.add(alias));
    });

    let signal: ExplainSignal | null = null;
    const baseDetail = `${nodeType}${relationName ? ` on ${relationName}` : ''}`;
    const operationKind = classifyExplainOperation(nodeType, baseDetail);

    if (operationKind === 'scan') {
      seqScans += 1;
      signal = {
        id: `json-seq-${counter += 1}`,
        severity: deriveExplainSeverity('high', costEnd, rowsEstimate),
        title: nodeType || 'Seq Scan',
        detail: baseDetail,
        relationName,
        relationAliases,
        costStart,
        costEnd,
        rowsEstimate,
        actualRows,
        loops,
        actualTimeEnd,
        estimateFactor,
      };
    } else if (operationKind === 'index') {
      indexedReads += 1;
      signal = {
        id: `json-index-${counter += 1}`,
        severity: deriveExplainSeverity('low', costEnd, rowsEstimate),
        title: nodeType || 'Index access',
        detail: baseDetail,
        relationName,
        relationAliases,
        costStart,
        costEnd,
        rowsEstimate,
        actualRows,
        loops,
        actualTimeEnd,
        estimateFactor,
      };
    } else if (operationKind === 'join') {
      joinNodes += 1;
      signal = {
        id: `json-join-${counter += 1}`,
        severity: deriveExplainSeverity(/nested loop/i.test(nodeType) ? 'high' : 'medium', costEnd, rowsEstimate),
        title: nodeType || 'Join',
        detail: baseDetail,
        relationAliases: Array.from(descendantAliases),
        costStart,
        costEnd,
        rowsEstimate,
        actualRows,
        loops,
        actualTimeEnd,
        estimateFactor,
      };
    } else if (operationKind === 'sort') {
      sorts += 1;
      signal = {
        id: `json-sort-${counter += 1}`,
        severity: deriveExplainSeverity('medium', costEnd, rowsEstimate),
        title: nodeType || 'Sort',
        detail: baseDetail,
        relationAliases: Array.from(descendantAliases),
        costStart,
        costEnd,
        rowsEstimate,
        actualRows,
        loops,
        actualTimeEnd,
        estimateFactor,
      };
    } else if (operationKind === 'aggregate') {
      signal = {
        id: `json-agg-${counter += 1}`,
        severity: deriveExplainSeverity('low', costEnd, rowsEstimate),
        title: nodeType || 'Aggregate',
        detail: baseDetail,
        relationAliases: Array.from(descendantAliases),
        costStart,
        costEnd,
        rowsEstimate,
        actualRows,
        loops,
        actualTimeEnd,
        estimateFactor,
      };
    }

    if (getEstimateSeverity(estimateFactor) && getEstimateSeverity(estimateFactor) !== 'low') {
      estimateWarnings += 1;
    }

    if (costEnd !== undefined) {
      maxCost = Math.max(maxCost, costEnd);
    }

    if (rowsEstimate !== undefined) {
      maxRows = Math.max(maxRows, rowsEstimate);
    }

    if (signal) {
      items.push(signal);
      relationAliases.forEach((alias) => {
        pushRelationSignal(alias, signal as ExplainSignal);
      });
    }

    return descendantAliases;
  };

  roots.forEach((root) => {
    visit(root);
  });
  const joinSignals = mapJoinSignals(items, relationSignals, joins);

  return {
    items,
    relationSignals,
    joinSignals,
    summary: {
      seqScans,
      indexedReads,
      joinNodes,
      sorts,
      mappedJoins: Object.keys(joinSignals).length,
      maxCost,
      maxRows,
      estimateWarnings,
    },
  };
};

const parseExplainInput = (input: string, tables: TableRef[], joins: JoinRef[]): ExplainSummary => {
  const trimmed = input.trim();
  if (!trimmed) {
    return createEmptyExplainSummary();
  }

  const sqlServerXmlSummary = parseSqlServerShowplanXml(trimmed, tables, joins);
  if (sqlServerXmlSummary) {
    return sqlServerXmlSummary;
  }

  const jsonSummary = parseExplainJsonInput(trimmed, tables, joins);
  if (jsonSummary) {
    return jsonSummary;
  }

  const relationSignals: Record<string, ExplainSignal[]> = {};
  const joinSignals: Record<string, ExplainSignal[]> = {};
  const items: ExplainSignal[] = [];
  let seqScans = 0;
  let indexedReads = 0;
  let joinNodes = 0;
  let sorts = 0;
  let maxCost = 0;
  let maxRows = 0;
  let estimateWarnings = 0;

  const { getAliasesForRelation } = createExplainContext(tables);

  const pushSignal = (
    record: Record<string, ExplainSignal[]>,
    key: string,
    signal: ExplainSignal,
  ) => {
    const nextList = record[key] ?? [];
    if (nextList.some((existing) => existing.id === signal.id)) {
      return;
    }

    record[key] = [...nextList, signal];
  };

  const rawEntries = trimmed
    .split(/\r?\n/)
    .map((rawLine, index) => ({
      rawLine,
      line: normalizeSpaces(rawLine),
      indent: Math.max(0, rawLine.search(/[A-Za-z[]/)),
      index,
    }))
    .filter((entry) => entry.line.length > 0);

  const nodes: Array<{
    id: string;
    line: string;
    indent: number;
    kind: 'scan' | 'join' | 'operation' | 'other';
    signal: ExplainSignal | null;
    relationAliases: string[];
    childIndices: number[];
    descendantAliases: Set<string>;
  }> = [];
  const stack: number[] = [];

  rawEntries.forEach((entry) => {
    const relationName = extractExplainRelationName(entry.line);
    const seqMatch = entry.line.match(/\b((?:XN|DS_[A-Z_]+)?\s*Seq Scan|Table Scan|TableScan|Remote Scan|External Scan|TABLE ACCESS FULL|TABLE ACCESS STORAGE FULL|Full Table Scan|Distributed Scan|READ|S3 Scan|File Scan|ScanFilterProject)\b/i);
    const indexMatch = entry.line.match(/\b(Index Scan|Index Only Scan|Bitmap Heap Scan|Bitmap Index Scan|Index Seek|Clustered Index Scan|Clustered Index Seek|Columnstore Index Scan|Columnstore Index Seek|Key Lookup|RID Lookup|TABLE ACCESS BY INDEX ROWID|INDEX RANGE SCAN|INDEX UNIQUE SCAN|INDEX FAST FULL SCAN|INDEX FULL SCAN)\b/i);
    const nestedLoopMatch = entry.line.match(/\b(Nested Loops?|Hash Join|Merge Join|Broadcast Hash Join|Broadcast Join|Left Semi Join|Right Semi Join|Left Anti Join|Right Anti Join|Hash Left Join|Hash Right Join|Hash Full Join|Hash Match\s*\(\s*(?:Inner Join|Left Semi Join|Right Semi Join)\s*\)|Left Outer Join|Right Outer Join|Full Outer Join|JoinBuild|JoinProbe|DS_BCAST_[A-Z_]+|DS_DIST_[A-Z_]+|XN Hash Join|XN Merge Join|XN Nested Loop)\b/i);
    const sortMatch = entry.line.match(/\b(Sort|Top N Sort|Order By|ANALYTIC_SORT|Window Sort)\b/i);
    const aggregateMatch = entry.line.match(/\b(Aggregate|GroupAggregate|HashAggregate|Stream Aggregate|Hash Match\s*\(\s*Aggregate\s*\)|Scalar Aggregate)\b/i);
    const filterMatch = entry.line.match(/\bRows Removed by Filter:\s*([\d,]+)/i);
    const costRowsMatch = entry.line.match(/\bcost=([0-9.,]+)\.\.([0-9.,]+)\s+rows=([0-9.,]+)/i);
    const sqlServerCostMatch = entry.line.match(/\bEstimated Total Subtree Cost\s*=\s*([0-9.,]+)/i);
    const sqlServerRowsMatch = entry.line.match(/\bEstimated Number of Rows\s*=\s*([0-9.,]+)/i);
    const actualRowsMatch = entry.line.match(/\bactual [^)]+ rows=([0-9.,]+)\s+loops=([0-9.,]+)/i);
    const oracleRowsMatch = entry.line.match(/\bA-Rows\s*[:=]?\s*([0-9.,]+)\b/i);
    const oracleEstimateRowsMatch = entry.line.match(/\bE-Rows\s*[:=]?\s*([0-9.,]+)\b/i);
    const costStart = costRowsMatch ? Number(costRowsMatch[1].replace(/,/g, '')) : undefined;
    const costEnd = costRowsMatch
      ? Number(costRowsMatch[2].replace(/,/g, ''))
      : sqlServerCostMatch
        ? Number(sqlServerCostMatch[1].replace(/,/g, ''))
        : undefined;
    const rowsEstimate = costRowsMatch
      ? Number(costRowsMatch[3].replace(/,/g, ''))
      : sqlServerRowsMatch
        ? Number(sqlServerRowsMatch[1].replace(/,/g, ''))
        : oracleEstimateRowsMatch
          ? Number(oracleEstimateRowsMatch[1].replace(/,/g, ''))
        : undefined;
    const actualRows = actualRowsMatch ? Number(actualRowsMatch[1].replace(/,/g, '')) : undefined;
    const loops = actualRowsMatch ? Number(actualRowsMatch[2].replace(/,/g, '')) : undefined;
    const actualTimeMatch = entry.line.match(/\bactual [a-z ]*=([0-9.,]+)\.\.([0-9.,]+)/i);
    const actualTimeEnd = actualTimeMatch ? Number(actualTimeMatch[2].replace(/,/g, '')) : undefined;
    const resolvedActualRows =
      actualRows !== undefined
        ? actualRows
        : oracleRowsMatch
          ? Number(oracleRowsMatch[1].replace(/,/g, ''))
          : undefined;
    const estimateFactor = getEstimateFactor(rowsEstimate, resolvedActualRows);
    const heatSeverity =
      costEnd !== undefined || rowsEstimate !== undefined
        ? (costEnd !== undefined && costEnd >= 1000) || (rowsEstimate !== undefined && rowsEstimate >= 100000)
          ? 'high'
          : (costEnd !== undefined && costEnd >= 120) || (rowsEstimate !== undefined && rowsEstimate >= 5000)
            ? 'medium'
            : 'low'
        : null;
    let signal: ExplainSignal | null = null;
    let kind: 'scan' | 'join' | 'operation' | 'other' = 'other';
    let relationAliases: string[] = [];

    if (seqMatch) {
      seqScans += 1;
      relationAliases = relationName ? getAliasesForRelation(relationName) : [];
        signal = {
          id: `seq-${entry.index}`,
          severity: 'high',
          title: seqMatch[1],
          detail: entry.line,
          relationName: relationName ?? undefined,
          relationAliases,
          costStart,
          costEnd,
          rowsEstimate,
          actualRows: resolvedActualRows,
          loops,
          actualTimeEnd,
          estimateFactor,
        };
        kind = 'scan';
      } else if (indexMatch) {
        indexedReads += 1;
        relationAliases = relationName ? getAliasesForRelation(relationName) : [];
        signal = {
          id: `index-${entry.index}`,
          severity: heatSeverity === 'high' ? 'high' : heatSeverity === 'medium' ? 'medium' : 'low',
          title: indexMatch[1],
          detail: entry.line,
          relationName: relationName ?? undefined,
          relationAliases,
          costStart,
          costEnd,
          rowsEstimate,
          actualRows: resolvedActualRows,
          loops,
          actualTimeEnd,
          estimateFactor,
        };
        kind = 'scan';
      } else if (nestedLoopMatch) {
        joinNodes += 1;
        signal = {
          id: `join-${entry.index}`,
          severity:
            heatSeverity === 'high'
              ? 'high'
              : nestedLoopMatch[1].toLowerCase() === 'nested loop'
                ? 'high'
                : 'medium',
          title: nestedLoopMatch[1],
          detail: entry.line,
          costStart,
          costEnd,
          rowsEstimate,
          actualRows: resolvedActualRows,
          loops,
          actualTimeEnd,
          estimateFactor,
        };
        kind = 'join';
      } else if (sortMatch) {
        sorts += 1;
        signal = {
          id: `sort-${entry.index}`,
          severity: heatSeverity === 'high' ? 'high' : 'medium',
          title: 'Sort',
          detail: entry.line,
          costStart,
          costEnd,
          rowsEstimate,
          actualRows: resolvedActualRows,
          loops,
          actualTimeEnd,
          estimateFactor,
        };
        kind = 'operation';
      } else if (aggregateMatch) {
        signal = {
          id: `agg-${entry.index}`,
          severity: heatSeverity === 'high' ? 'high' : heatSeverity === 'medium' ? 'medium' : 'low',
          title: aggregateMatch[1],
          detail: entry.line,
          costStart,
          costEnd,
          rowsEstimate,
          actualRows: resolvedActualRows,
          loops,
          actualTimeEnd,
          estimateFactor,
        };
        kind = 'operation';
      } else if (/\b(Filter|Predicate)\b/i.test(entry.line)) {
        signal = {
          id: `filter-${entry.index}`,
          severity: heatSeverity === 'high' ? 'high' : 'medium',
          title: 'Filter',
          detail: entry.line,
          relationName: relationName ?? undefined,
          relationAliases: relationName ? getAliasesForRelation(relationName) : [],
          costStart,
          costEnd,
          rowsEstimate,
          actualRows: resolvedActualRows,
          loops,
          actualTimeEnd,
          estimateFactor,
        };
        kind = 'operation';
      } else if (filterMatch) {
        signal = {
          id: `filter-${entry.index}`,
          severity: heatSeverity === 'high' ? 'high' : 'medium',
          title: 'Rows Removed by Filter',
          detail: entry.line,
          costStart,
          costEnd,
          rowsEstimate,
          actualRows: resolvedActualRows,
          loops,
          actualTimeEnd,
          estimateFactor,
        };
        kind = 'operation';
      }

    if (getEstimateSeverity(estimateFactor) && getEstimateSeverity(estimateFactor) !== 'low') {
      estimateWarnings += 1;
    }

    if (costEnd !== undefined) {
      maxCost = Math.max(maxCost, costEnd);
    }

    if (rowsEstimate !== undefined) {
      maxRows = Math.max(maxRows, rowsEstimate);
    }

    const nextNode = {
      id: `plan-${entry.index}`,
      line: entry.line,
      indent: entry.indent,
      kind,
      signal,
      relationAliases,
      childIndices: [] as number[],
      descendantAliases: new Set<string>(relationAliases),
    };

    while (stack.length > 0 && nextNode.indent <= nodes[stack[stack.length - 1]].indent) {
      stack.pop();
    }

    if (stack.length > 0) {
      nodes[stack[stack.length - 1]].childIndices.push(nodes.length);
    }

    nodes.push(nextNode);
    stack.push(nodes.length - 1);
  });

  for (let index = nodes.length - 1; index >= 0; index -= 1) {
    nodes[index].childIndices.forEach((childIndex) => {
      nodes[childIndex].descendantAliases.forEach((alias) => {
        nodes[index].descendantAliases.add(alias);
      });
    });
  }

  nodes.forEach((node, nodeIndex) => {
    if (!node.signal) {
      return;
    }

    items.push(node.signal);

    node.relationAliases.forEach((alias) => {
      pushSignal(relationSignals, alias, node.signal as ExplainSignal);
    });

    if (node.kind !== 'join') {
      return;
    }

    const childAliasGroups = node.childIndices
      .map((childIndex) => nodes[childIndex].descendantAliases)
      .filter((aliases) => aliases.size > 0);

    let matchedJoinIds: string[] = [];

    if (childAliasGroups.length >= 2) {
      const leftAliases = childAliasGroups[0];
      const rightAliases = new Set<string>();

      childAliasGroups.slice(1).forEach((aliases) => {
        aliases.forEach((alias) => rightAliases.add(alias));
      });

      matchedJoinIds = joins
        .filter(
          (join) =>
            (leftAliases.has(join.sourceAlias) && rightAliases.has(join.targetAlias)) ||
            (leftAliases.has(join.targetAlias) && rightAliases.has(join.sourceAlias)),
        )
        .map((join) => join.id);
    }

    if (matchedJoinIds.length === 0 && node.descendantAliases.size >= 2) {
      matchedJoinIds = joins
        .filter(
          (join) =>
            node.descendantAliases.has(join.sourceAlias) &&
            node.descendantAliases.has(join.targetAlias),
        )
        .map((join) => join.id);
    }

    if (matchedJoinIds.length === 0) {
      const nearbyAliases = new Set<string>();

      nodes
        .slice(Math.max(0, nodeIndex - 2), Math.min(nodes.length, nodeIndex + 5))
        .forEach((candidate) => {
          if (candidate.kind !== 'scan') {
            return;
          }

          candidate.relationAliases.forEach((alias) => nearbyAliases.add(alias));
        });

      if (nearbyAliases.size >= 2) {
        matchedJoinIds = joins
          .filter(
            (join) =>
              nearbyAliases.has(join.sourceAlias) &&
              nearbyAliases.has(join.targetAlias),
          )
          .map((join) => join.id);
      }
    }

    const joinSignal: ExplainSignal = {
      ...node.signal,
      joinIds: matchedJoinIds,
      relationAliases: Array.from(node.descendantAliases),
    };

    matchedJoinIds.forEach((joinId) => {
      pushSignal(joinSignals, joinId, joinSignal);
    });
  });

  return {
    items,
    relationSignals,
    joinSignals,
    summary: {
      seqScans,
      indexedReads,
      joinNodes,
      sorts,
      mappedJoins: Object.keys(joinSignals).length,
      maxCost,
      maxRows,
      estimateWarnings,
    },
  };
};

const getStrongestExplainSignal = (signals: ExplainSignal[] | undefined) => {
  if (!signals || signals.length === 0) {
    return null;
  }

  const severityRank: Record<ExplainSignal['severity'], number> = {
    high: 3,
    medium: 2,
    low: 1,
  };

  return [...signals].sort((left, right) => {
    const severityDelta = severityRank[right.severity] - severityRank[left.severity];
    if (severityDelta !== 0) {
      return severityDelta;
    }

    const costDelta = (right.costEnd ?? 0) - (left.costEnd ?? 0);
    if (costDelta !== 0) {
      return costDelta;
    }

    return (right.rowsEstimate ?? 0) - (left.rowsEstimate ?? 0);
  })[0];
};

const getNodeNoteKey = (alias: string) => `node:${alias}`;

const getJoinNoteKey = (joinId: string) => `join:${joinId}`;

const formatSignedDelta = (value: number) => (value > 0 ? `+${value}` : `${value}`);

const buildJoinComparisonLabel = (join: JoinRef) =>
  `${formatJoinTypeLabel(join.type)} ${join.sourceAlias} -> ${join.targetAlias} / ${normalizeSpaces(`ON ${join.condition}`)}`;

const buildCompareSummary = (
  currentAnalysis: ReturnType<typeof analyzeSql>,
  currentPlan: ExplainSummary,
  baselineAnalysis: ReturnType<typeof analyzeSql>,
  baselinePlan: ExplainSummary,
): CompareSummary => {
  const currentTables = Array.from(new Set(currentAnalysis.tables.map((table) => `${table.alias} (${table.name})`))).sort();
  const baselineTables = Array.from(new Set(baselineAnalysis.tables.map((table) => `${table.alias} (${table.name})`))).sort();
  const currentJoins = Array.from(new Set(currentAnalysis.joins.map((join) => buildJoinComparisonLabel(join)))).sort();
  const baselineJoins = Array.from(new Set(baselineAnalysis.joins.map((join) => buildJoinComparisonLabel(join)))).sort();
  const currentFlags = Array.from(new Set(currentAnalysis.flags.map((flag) => getFlagContent(flag.title, flag.description).title))).sort();
  const baselineFlags = Array.from(new Set(baselineAnalysis.flags.map((flag) => getFlagContent(flag.title, flag.description).title))).sort();

  const addedTables = currentTables.filter((item) => !baselineTables.includes(item));
  const removedTables = baselineTables.filter((item) => !currentTables.includes(item));
  const addedJoins = currentJoins.filter((item) => !baselineJoins.includes(item));
  const removedJoins = baselineJoins.filter((item) => !currentJoins.includes(item));
  const addedFlags = currentFlags.filter((item) => !baselineFlags.includes(item));
  const removedFlags = baselineFlags.filter((item) => !currentFlags.includes(item));
  const hasPlanComparison = baselinePlan.items.length > 0 || currentPlan.items.length > 0;

  return {
    tableDelta: currentAnalysis.tables.length - baselineAnalysis.tables.length,
    joinDelta: currentAnalysis.joins.length - baselineAnalysis.joins.length,
    flagDelta: currentAnalysis.flags.length - baselineAnalysis.flags.length,
    complexityDelta: currentAnalysis.complexityScore - baselineAnalysis.complexityScore,
    planSignalDelta: currentPlan.items.length - baselinePlan.items.length,
    seqScanDelta: currentPlan.summary.seqScans - baselinePlan.summary.seqScans,
    joinNodeDelta: currentPlan.summary.joinNodes - baselinePlan.summary.joinNodes,
    maxCostDelta:
      hasPlanComparison ? currentPlan.summary.maxCost - baselinePlan.summary.maxCost : null,
    maxRowsDelta:
      hasPlanComparison ? currentPlan.summary.maxRows - baselinePlan.summary.maxRows : null,
    addedTables,
    removedTables,
    addedJoins,
    removedJoins,
    addedFlags,
    removedFlags,
    hasPlanComparison,
  };
};

const buildFanoutState = (joins: JoinRef[], joinInsights: Record<string, JoinInsight>) => {
  const aliasImpacts: Record<string, FanoutImpact> = {};
  const joinImpacts: Record<string, FanoutImpact> = {};

  joins.forEach((join) => {
    const inherited = aliasImpacts[join.sourceAlias] ?? null;
    const insight = joinInsights[join.id];
    const direct =
      insight?.fanoutSeverity === 'high'
        ? {
            severity: 'high' as const,
            viaJoinId: join.id,
            reason: insight.fanoutSummary,
          }
        : insight?.fanoutSeverity === 'caution'
          ? {
              severity: 'caution' as const,
              viaJoinId: join.id,
              reason: insight.fanoutSummary,
            }
          : null;

    const next =
      direct && inherited
        ? direct.severity === 'high' || inherited.severity === 'caution'
          ? direct
          : inherited
        : direct ?? inherited;

    if (!next) {
      return;
    }

    aliasImpacts[join.targetAlias] = next;
    joinImpacts[join.id] = next;
  });

  return {
    aliasImpacts,
    joinImpacts,
    impactedAliases: new Set(Object.keys(aliasImpacts)),
    impactedJoinIds: new Set(Object.keys(joinImpacts)),
  };
};

function App() {
  const sharedWorkspace = readSharedWorkspace();
  const [sql, setSql] = useState(sharedWorkspace?.sql ?? POSTGRES_SAMPLE_SQL);
  const [selectedStatementIndex, setSelectedStatementIndex] = useState(sharedWorkspace?.selectedStatementIndex ?? 0);
  const [savedQueries, setSavedQueries] = useState<SavedQuery[]>(() => readSavedQueries());
  const [dialect, setDialect] = useState<DialectMode>(
    sharedWorkspace?.dialect ?? detectSqlDialect(sharedWorkspace?.sql ?? POSTGRES_SAMPLE_SQL).dialect,
  );
  const [schemaSql, setSchemaSql] = useState('');
  const [layoutMode, setLayoutMode] = useState<LayoutMode>('horizontal');
  const [expandedDerivedIds, setExpandedDerivedIds] = useState<string[]>([]);
  const [searchQuery, setSearchQuery] = useState('');
  const [explainInput, setExplainInput] = useState('');
  const [zoom, setZoom] = useState(1);
  const [pan, setPan] = useState({ x: 0, y: 0 });
  const [shellSize, setShellSize] = useState({ width: 0, height: 0 });
  const [draggingNode, setDraggingNode] = useState<string | null>(null);
  const [selectedAlias, setSelectedAlias] = useState<string | null>(null);
  const [selectedJoinId, setSelectedJoinId] = useState<string | null>(null);
  const [selectedLineageId, setSelectedLineageId] = useState<string | null>(null);
  const [detailTab, setDetailTab] = useState<DetailTab>('joins');
  const [nodeOffsets, setNodeOffsets] = useState<Record<string, { x: number; y: number }>>({});
  const [entityNotes, setEntityNotes] = useState<Record<string, string>>({});
  const [compareSql, setCompareSql] = useState('');
  const [compareExplainInput, setCompareExplainInput] = useState('');
  const [showShortcutHelp, setShowShortcutHelp] = useState(false);
  const [sqlScrollTop, setSqlScrollTop] = useState(0);
  const [lastPoint, setLastPoint] = useState<{ x: number; y: number } | null>(null);
  const [isPanning, setIsPanning] = useState(false);
  const [statusMessage, setStatusMessage] = useState<string | null>(null);
  const fileInputRef = useRef<HTMLInputElement | null>(null);
  const schemaFileInputRef = useRef<HTMLInputElement | null>(null);
  const sqlInputRef = useRef<HTMLTextAreaElement | null>(null);
  const graphShellRef = useRef<HTMLDivElement | null>(null);
  const graphViewportRef = useRef<HTMLDivElement | null>(null);
  const deferredSearchQuery = useDeferredValue(searchQuery);
  const deferredCompareSql = useDeferredValue(compareSql);
  const deferredCompareExplainInput = useDeferredValue(compareExplainInput);
  const dialectDetection = useMemo(() => detectSqlDialect(sql, dialect), [dialect, sql]);
  const activeDialect = dialectDetection.dialect;

  const statements = useMemo(() => extractStatements(sql, activeDialect), [activeDialect, sql]);
  const diagnostics = useMemo<DiagnosticSummary>(() => {
    const items = diagnoseSqlInput(sql, activeDialect);
    const blocking = items.find((item) => item.severity === 'error') ?? null;
    return { blocking, items };
  }, [activeDialect, sql]);
  const primaryDiagnostic = diagnostics.blocking ?? diagnostics.items[0] ?? null;
  const safeSelectedStatementIndex = statements.length === 0
    ? 0
    : Math.min(selectedStatementIndex, statements.length - 1);
  const workspaceStateKey = useMemo(
    () => createWorkspaceStateKey(sql, safeSelectedStatementIndex, activeDialect),
    [activeDialect, safeSelectedStatementIndex, sql],
  );

  const analysis = useMemo(
    () => analyzeSql(sql, safeSelectedStatementIndex, activeDialect),
    [activeDialect, safeSelectedStatementIndex, sql],
  );
  const layout = useMemo(
    () => createNodeLayout(analysis.tables, analysis.joins, layoutMode),
    [analysis.tables, analysis.joins, layoutMode],
  );

  const positionedTables = analysis.tables.map((table) => {
    const base = layout.positions[table.alias] ?? { x: 220, y: 220 };
    const offset = nodeOffsets[table.alias] ?? { x: 0, y: 0 };
    return {
      ...table,
      x: base.x + offset.x,
      y: base.y + offset.y,
    };
  });

  const edgePortMap = useMemo(
    () => createEdgePortMap(analysis.joins, layout.positions, nodeOffsets),
    [analysis.joins, layout.positions, nodeOffsets],
  );
  const parsedSchema = useMemo(() => parseSchemaInput(schemaSql), [schemaSql]);
  const derivedRelationMap = useMemo(
    () =>
      Object.fromEntries(
        analysis.derivedRelations.map((relation) => [relation.id, relation]),
      ) as Record<string, DerivedRelation>,
    [analysis.derivedRelations],
  );
  const filterContext = useMemo(() => collectAliasFilterContext(analysis.filters), [analysis.filters]);
  const tableNameLookup = useMemo(
    () =>
      Object.fromEntries(
        analysis.tables.map((table) => [table.alias, table.name]),
      ) as Record<string, string>,
    [analysis.tables],
  );
  const schemaTableLookup = useMemo(
    () =>
      Object.fromEntries(
        analysis.tables.map((table) => [table.alias, findSchemaTable(parsedSchema.tables, table.name)]),
      ) as Record<string, SchemaTableMetadata | null>,
    [analysis.tables, parsedSchema.tables],
  );
  const joinInsights = useMemo(
    () =>
      Object.fromEntries(
        analysis.joins.map((join) => [
          join.id,
          inferJoinInsight(
            join,
            tableNameLookup[join.sourceAlias],
            tableNameLookup[join.targetAlias],
            filterContext,
            activeDialect,
            schemaTableLookup[join.sourceAlias],
            schemaTableLookup[join.targetAlias],
          ),
        ]),
      ) as Record<string, JoinInsight>,
    [activeDialect, analysis.joins, filterContext, schemaTableLookup, tableNameLookup],
  );
  const verifiedJoinCount = useMemo(
    () => Object.values(joinInsights).filter((insight) => insight.confidence === 'verified').length,
    [joinInsights],
  );
  const fanoutState = useMemo(
    () => buildFanoutState(analysis.joins, joinInsights),
    [analysis.joins, joinInsights],
  );
  const lineageColumns = useMemo(
    () => buildColumnLineage(analysis.columns, analysis.joins, analysis.tables[0]?.alias ?? ''),
    [analysis.columns, analysis.joins, analysis.tables],
  );
  const selectedLineage = useMemo(
    () => lineageColumns.find((column) => column.id === selectedLineageId) ?? null,
    [lineageColumns, selectedLineageId],
  );
  const selectedTable = useMemo(
    () => analysis.tables.find((table) => table.alias === selectedAlias) ?? null,
    [analysis.tables, selectedAlias],
  );
  const selectedJoin = useMemo(
    () => analysis.joins.find((join) => join.id === selectedJoinId) ?? null,
    [analysis.joins, selectedJoinId],
  );
  const focusState = useMemo(() => {
    if (selectedJoin) {
      return {
        active: true,
        relatedAliases: new Set<string>([selectedJoin.sourceAlias, selectedJoin.targetAlias]),
        relatedJoinIds: new Set<string>([selectedJoin.id]),
      };
    }

    if (!selectedAlias) {
      if (selectedLineage) {
        return {
          active: true,
          relatedAliases: new Set<string>(selectedLineage.relatedAliases),
          relatedJoinIds: new Set<string>(selectedLineage.relatedJoinIds),
        };
      }

      return {
        active: false,
        relatedAliases: new Set<string>(),
        relatedJoinIds: new Set<string>(),
      };
    }

    const relatedAliases = new Set<string>([selectedAlias]);
    const relatedJoinIds = new Set<string>();

    analysis.joins.forEach((join) => {
      if (join.sourceAlias === selectedAlias || join.targetAlias === selectedAlias) {
        relatedAliases.add(join.sourceAlias);
        relatedAliases.add(join.targetAlias);
        relatedJoinIds.add(join.id);
      }
    });

    return {
      active: true,
      relatedAliases,
      relatedJoinIds,
    };
  }, [analysis.joins, selectedAlias, selectedJoin, selectedLineage]);
  const searchState = useMemo<SearchState>(() => {
    const query = normalizeSpaces(deferredSearchQuery).toLowerCase();
    if (!query) {
      return {
        active: false,
        query: '',
        matchCount: 0,
        matchedAliases: new Set<string>(),
        matchedJoinIds: new Set<string>(),
      };
    }

    const matchedAliases = new Set<string>();
    const matchedJoinIds = new Set<string>();

    analysis.tables.forEach((table) => {
      if (
        table.alias.toLowerCase().includes(query) ||
        table.name.toLowerCase().includes(query)
      ) {
        matchedAliases.add(table.alias);
      }
    });

    analysis.joins.forEach((join) => {
      if (
        join.alias.toLowerCase().includes(query) ||
        join.tableName.toLowerCase().includes(query) ||
        join.condition.toLowerCase().includes(query) ||
        join.type.toLowerCase().includes(query) ||
        join.sourceAlias.toLowerCase().includes(query) ||
        join.targetAlias.toLowerCase().includes(query)
      ) {
        matchedJoinIds.add(join.id);
        matchedAliases.add(join.sourceAlias);
        matchedAliases.add(join.targetAlias);
      }
    });

    return {
      active: true,
      query,
      matchCount: matchedAliases.size + matchedJoinIds.size,
      matchedAliases,
      matchedJoinIds,
    };
  }, [analysis.joins, analysis.tables, deferredSearchQuery]);
  const explainSummary = useMemo(
    () => parseExplainInput(explainInput, analysis.tables, analysis.joins),
    [analysis.joins, analysis.tables, explainInput],
  );
  const selectedTableSignals = useMemo(
    () => (selectedTable ? explainSummary.relationSignals[selectedTable.alias] ?? [] : []),
    [explainSummary.relationSignals, selectedTable],
  );
  const selectedDerivedRelation = useMemo(
    () => (selectedTable?.derivedId ? derivedRelationMap[selectedTable.derivedId] ?? null : null),
    [derivedRelationMap, selectedTable],
  );
  const compareStatements = useMemo(
    () => extractStatements(deferredCompareSql, activeDialect),
    [activeDialect, deferredCompareSql],
  );
  const compareStatementIndex = compareStatements.length === 0
    ? 0
    : Math.min(safeSelectedStatementIndex, compareStatements.length - 1);
  const compareAnalysis = useMemo(
    () => (normalizeSpaces(deferredCompareSql) ? analyzeSql(deferredCompareSql, compareStatementIndex, activeDialect) : null),
    [activeDialect, compareStatementIndex, deferredCompareSql],
  );
  const compareExplainSummary = useMemo(
    () =>
      compareAnalysis
        ? parseExplainInput(deferredCompareExplainInput, compareAnalysis.tables, compareAnalysis.joins)
        : createEmptyExplainSummary(),
    [compareAnalysis, deferredCompareExplainInput],
  );
  const compareSummary = useMemo(
    () =>
      compareAnalysis
        ? buildCompareSummary(analysis, explainSummary, compareAnalysis, compareExplainSummary)
        : null,
    [analysis, compareAnalysis, compareExplainSummary, explainSummary],
  );
  const compareLayout = useMemo(
    () => (compareAnalysis ? createNodeLayout(compareAnalysis.tables, compareAnalysis.joins, layoutMode) : null),
    [compareAnalysis, layoutMode],
  );
  const compareEdgePortMap = useMemo(
    () => (compareAnalysis && compareLayout ? createEdgePortMap(compareAnalysis.joins, compareLayout.positions, {}) : null),
    [compareAnalysis, compareLayout],
  );
  const compareOverlay = useMemo(() => {
    if (!compareAnalysis) {
      return null;
    }

    const currentTableKeys = new Set(analysis.tables.map((table) => `${table.alias}|${table.name}`));
    const baselineTableKeys = new Set(compareAnalysis.tables.map((table) => `${table.alias}|${table.name}`));
    const currentJoinKeys = new Set(analysis.joins.map((join) => buildJoinComparisonLabel(join)));
    const baselineJoinKeys = new Set(compareAnalysis.joins.map((join) => buildJoinComparisonLabel(join)));

    return {
      addedAliases: new Set(
        analysis.tables
          .filter((table) => !baselineTableKeys.has(`${table.alias}|${table.name}`))
          .map((table) => table.alias),
      ),
      removedTables: compareAnalysis.tables.filter((table) => !currentTableKeys.has(`${table.alias}|${table.name}`)),
      addedJoinIds: new Set(
        analysis.joins
          .filter((join) => !baselineJoinKeys.has(buildJoinComparisonLabel(join)))
          .map((join) => join.id),
      ),
      removedJoins: compareAnalysis.joins.filter((join) => !currentJoinKeys.has(buildJoinComparisonLabel(join))),
    };
  }, [analysis.joins, analysis.tables, compareAnalysis]);
  const selectedJoinInsight = useMemo(
    () => (selectedJoin ? joinInsights[selectedJoin.id] : null),
    [joinInsights, selectedJoin],
  );
  const selectedJoinSignals = useMemo(() => {
    if (!selectedJoin) {
      return [];
    }

    const signalMap = new Map<string, ExplainSignal>();

    [
      ...(explainSummary.joinSignals[selectedJoin.id] ?? []),
      ...(explainSummary.relationSignals[selectedJoin.sourceAlias] ?? []),
      ...(explainSummary.relationSignals[selectedJoin.targetAlias] ?? []),
    ].forEach((signal) => {
      signalMap.set(signal.id, signal);
    });

    return Array.from(signalMap.values());
  }, [explainSummary.joinSignals, explainSummary.relationSignals, selectedJoin]);
  const selectedLineageSignals = useMemo(() => {
    if (!selectedLineage) {
      return [];
    }

    const signalMap = new Map<string, ExplainSignal>();

    selectedLineage.relatedAliases.forEach((alias) => {
      (explainSummary.relationSignals[alias] ?? []).forEach((signal) => {
        signalMap.set(signal.id, signal);
      });
    });

    selectedLineage.relatedJoinIds.forEach((joinId) => {
      (explainSummary.joinSignals[joinId] ?? []).forEach((signal) => {
        signalMap.set(signal.id, signal);
      });
    });

    return Array.from(signalMap.values());
  }, [explainSummary.joinSignals, explainSummary.relationSignals, selectedLineage]);
  const minimap = useMemo(() => {
    const graphWidth = Math.max(layout.width, compareLayout?.width ?? 0);
    const graphHeight = Math.max(layout.height, compareLayout?.height ?? 0);
    const scale = Math.min(196 / graphWidth, 140 / graphHeight, 1);
    const width = graphWidth * scale;
    const height = graphHeight * scale;
    const viewportWidth = shellSize.width > 0 ? Math.min(graphWidth, shellSize.width / zoom) : graphWidth;
    const viewportHeight = shellSize.height > 0 ? Math.min(graphHeight, shellSize.height / zoom) : graphHeight;
    const viewportX = clamp(-pan.x / zoom, 0, Math.max(0, graphWidth - viewportWidth));
    const viewportY = clamp(-pan.y / zoom, 0, Math.max(0, graphHeight - viewportHeight));

    return {
      scale,
      width,
      height,
      viewportWidth: viewportWidth * scale,
      viewportHeight: viewportHeight * scale,
      viewportX: viewportX * scale,
      viewportY: viewportY * scale,
    };
  }, [compareLayout?.height, compareLayout?.width, layout.height, layout.width, pan.x, pan.y, shellSize.height, shellSize.width, zoom]);
  const graphSize = useMemo(
    () => ({
      width: Math.max(layout.width, compareLayout?.width ?? 0),
      height: Math.max(layout.height, compareLayout?.height ?? 0),
    }),
    [compareLayout?.height, compareLayout?.width, layout.height, layout.width],
  );
  const reviewReport = useMemo(() => {
    const noteEntries = Object.entries(entityNotes).filter((entry) => entry[1].trim().length > 0);
    const lines: string[] = [
      '# Queryviz Review Report',
      '',
      `- Statement: #${analysis.analyzedStatementIndex + 1}`,
      `- Statement type: ${formatStatementTypeLabel(analysis.statementType)}`,
      `- Dialect: ${dialectLabel[activeDialect]}`,
      `- Layout: ${layoutModeLabel[layoutMode]}`,
      `- Complexity score: ${analysis.complexityScore}`,
      `- Tables: ${analysis.tables.length}`,
      `- Joins: ${analysis.joins.length}`,
      `- Filters: ${analysis.filters.length}`,
      `- Fanout paths: ${fanoutState.impactedAliases.size}`,
    ];

    if (parsedSchema.summary.tableCount > 0) {
      lines.push(`- Imported schema tables: ${parsedSchema.summary.tableCount}`);
      lines.push(`- Verified joins: ${verifiedJoinCount}`);
    }

    if (analysis.writeTarget) {
      lines.push(`- Write target: ${analysis.writeTarget}`);
    }

    if (analysis.derivedRelations.length > 0) {
      lines.push(`- Derived relations: ${analysis.derivedRelations.length}`);
    }

    if (explainSummary.items.length > 0) {
      lines.push(`- Plan overlay: ${formatPlural(explainSummary.items.length, 'signal')}`);
      if (explainSummary.summary.estimateWarnings > 0) {
        lines.push(`- Plan misestimates: ${explainSummary.summary.estimateWarnings}`);
      }
    }

    lines.push('', '## Joins', '');

    if (analysis.joins.length > 0) {
      lines.push('| Source | Join | Target | ON | Cardinality | Guidance |');
      lines.push('| --- | --- | --- | --- | --- | --- |');

      analysis.joins.forEach((join) => {
        const insight = joinInsights[join.id];
        lines.push(
          `| ${escapeMarkdownCell(join.sourceAlias)} | ${escapeMarkdownCell(insight?.joinLabel ?? formatJoinTypeLabel(join.type))} | ${escapeMarkdownCell(join.targetAlias)} | ${escapeMarkdownCell(insight?.fullConditionLabel ?? join.condition)} | ${escapeMarkdownCell(`${insight?.badgeLabel ?? 'M:N'} (${insight?.confidenceLabel ?? 'Heuristic'})`)} | ${escapeMarkdownCell(insight?.summary ?? '')} |`,
        );
      });
    } else {
      lines.push('No joins detected.');
    }

    lines.push('', '## Flags', '');

    if (analysis.flags.length > 0) {
      analysis.flags.forEach((flag) => {
        const content = getFlagContent(flag.title, flag.description);
        lines.push(`- [${severityLabel[flag.severity]}] ${content.title} — ${content.description}`);
      });
    } else {
      lines.push('- No major warnings.');
    }

    if (analysis.derivedRelations.length > 0) {
      lines.push('', '## Derived Relations', '');
      analysis.derivedRelations.forEach((relation) => {
        lines.push(`### ${relation.kind.toUpperCase()}: ${relation.alias}`);
        lines.push(`- Sources: ${relation.sourceCount}`);
        lines.push(`- Joins: ${relation.joinCount}`);
        lines.push(`- Subqueries: ${relation.subqueryCount}`);
        lines.push(`- Aggregation: ${relation.hasAggregation ? 'Yes' : 'No'}`);
        if (relation.dependencies.length > 0) {
          lines.push(`- Dependencies: ${relation.dependencies.join(', ')}`);
        }
        if (relation.flags.length > 0) {
          lines.push(`- Flags: ${relation.flags.join(', ')}`);
        }
        lines.push('');
      });
    }

    if (explainSummary.items.length > 0) {
      lines.push('## Plan Overlay', '');
      lines.push(`- Seq scans: ${explainSummary.summary.seqScans}`);
      lines.push(`- Indexed reads: ${explainSummary.summary.indexedReads}`);
      lines.push(`- Join nodes: ${explainSummary.summary.joinNodes}`);
      lines.push(`- Sorts: ${explainSummary.summary.sorts}`);
      lines.push(`- Mapped joins: ${explainSummary.summary.mappedJoins}`);
      if (explainSummary.summary.maxCost > 0) {
        lines.push(`- Max cost: ${formatCompactNumber(explainSummary.summary.maxCost)}`);
      }
      if (explainSummary.summary.maxRows > 0) {
        lines.push(`- Max rows: ${formatCompactNumber(explainSummary.summary.maxRows)}`);
      }
      if (explainSummary.summary.estimateWarnings > 0) {
        lines.push(`- Misestimates: ${explainSummary.summary.estimateWarnings}`);
      }
      lines.push('');
      explainSummary.items.slice(0, 12).forEach((signal) => {
        const metrics = formatPlanMetrics(signal, 'full');
        lines.push(`- ${signal.title}${metrics ? ` (${metrics})` : ''} — ${signal.detail}`);
      });
    }

    if (noteEntries.length > 0) {
      lines.push('', '## Notes', '');

      noteEntries.forEach(([entityKey, note]) => {
        const [kind, value] = entityKey.split(':');
        const join = kind === 'join' ? analysis.joins.find((item) => item.id === value) : null;
        const table = kind === 'node' ? analysis.tables.find((item) => item.alias === value) : null;
        const label =
          join
            ? `Join ${join.sourceAlias} -> ${join.targetAlias}`
            : table
              ? `Node ${table.alias} (${table.name})`
              : entityKey;

        lines.push(`- ${escapeMarkdownCell(label)} — ${escapeMarkdownCell(normalizeSpaces(note))}`);
      });
    }

    if (compareSummary && compareAnalysis) {
      lines.push('', '## Compare Mode', '');
      lines.push(`- Baseline statement: #${compareAnalysis.analyzedStatementIndex + 1}`);
      lines.push(`- Complexity delta: ${formatSignedDelta(compareSummary.complexityDelta)}`);
      lines.push(`- Table delta: ${formatSignedDelta(compareSummary.tableDelta)}`);
      lines.push(`- Join delta: ${formatSignedDelta(compareSummary.joinDelta)}`);
      lines.push(`- Flag delta: ${formatSignedDelta(compareSummary.flagDelta)}`);

      if (compareSummary.hasPlanComparison) {
        lines.push(`- Plan signal delta: ${formatSignedDelta(compareSummary.planSignalDelta)}`);
        lines.push(`- Seq scan delta: ${formatSignedDelta(compareSummary.seqScanDelta)}`);
        lines.push(`- Join node delta: ${formatSignedDelta(compareSummary.joinNodeDelta)}`);
        if (compareSummary.maxCostDelta !== null) {
          lines.push(`- Max cost delta: ${formatSignedDelta(Number(compareSummary.maxCostDelta.toFixed(2)))}`);
        }
        if (compareSummary.maxRowsDelta !== null) {
          lines.push(`- Max rows delta: ${formatSignedDelta(compareSummary.maxRowsDelta)}`);
        }
      }

      if (compareSummary.addedJoins.length > 0) {
        lines.push('', '### Added joins');
        compareSummary.addedJoins.slice(0, 8).forEach((item) => lines.push(`- ${escapeMarkdownCell(item)}`));
      }

      if (compareSummary.removedJoins.length > 0) {
        lines.push('', '### Removed joins');
        compareSummary.removedJoins.slice(0, 8).forEach((item) => lines.push(`- ${escapeMarkdownCell(item)}`));
      }

      if (compareSummary.addedFlags.length > 0 || compareSummary.removedFlags.length > 0) {
        lines.push('', '### Flag changes');
        compareSummary.addedFlags.forEach((item) => lines.push(`- Added flag: ${escapeMarkdownCell(item)}`));
        compareSummary.removedFlags.forEach((item) => lines.push(`- Removed flag: ${escapeMarkdownCell(item)}`));
      }
    }

    return lines.join('\n');
  }, [activeDialect, analysis, compareAnalysis, compareSummary, entityNotes, explainSummary, fanoutState.impactedAliases.size, joinInsights, layoutMode, parsedSchema.summary.tableCount, verifiedJoinCount]);
  const executionReport = useMemo(() => {
    const lines: string[] = [
      '# Queryviz Execution Report',
      '',
      `- Statement: #${analysis.analyzedStatementIndex + 1}`,
      `- Statement type: ${formatStatementTypeLabel(analysis.statementType)}`,
      `- Dialect: ${dialectLabel[activeDialect]}`,
      `- Complexity score: ${analysis.complexityScore}`,
      `- Tables: ${analysis.tables.length}`,
      `- Joins: ${analysis.joins.length}`,
    ];

    if (parsedSchema.summary.tableCount > 0) {
      lines.push(`- Imported schema tables: ${parsedSchema.summary.tableCount}`);
      lines.push(`- Verified joins: ${verifiedJoinCount}`);
    }

    if (analysis.writeTarget) {
      lines.push(`- Write target: ${analysis.writeTarget}`);
    }

    if (analysis.flags.length > 0) {
      lines.push(`- Flags: ${analysis.flags.length}`);
    }

    lines.push('', '## Plan Summary', '');

    if (explainSummary.items.length === 0) {
      lines.push('- No EXPLAIN data pasted yet.');
    } else {
      lines.push(`- Seq scans: ${explainSummary.summary.seqScans}`);
      lines.push(`- Indexed reads: ${explainSummary.summary.indexedReads}`);
      lines.push(`- Join nodes: ${explainSummary.summary.joinNodes}`);
      lines.push(`- Sorts: ${explainSummary.summary.sorts}`);
      lines.push(`- Mapped joins: ${explainSummary.summary.mappedJoins}`);
      if (explainSummary.summary.maxCost > 0) {
        lines.push(`- Max cost: ${formatCompactNumber(explainSummary.summary.maxCost)}`);
      }
      if (explainSummary.summary.maxRows > 0) {
        lines.push(`- Max estimated rows: ${formatCompactNumber(explainSummary.summary.maxRows)}`);
      }
      if (explainSummary.summary.estimateWarnings > 0) {
        lines.push(`- Misestimates: ${explainSummary.summary.estimateWarnings}`);
      }
    }

    lines.push('', '## Join Focus', '');

    if (analysis.joins.length === 0) {
      lines.push('- No joins detected.');
    } else {
      analysis.joins.forEach((join) => {
        const insight = joinInsights[join.id];
        const signal = getStrongestExplainSignal(explainSummary.joinSignals[join.id]);
        const metrics = signal ? formatPlanMetrics(signal, 'full') : '';
        lines.push(
          `- ${insight?.joinLabel ?? formatJoinTypeLabel(join.type)} ${join.sourceAlias} -> ${join.targetAlias} | ${insight?.badgeLabel ?? 'M:N'} | ${insight?.confidenceLabel ?? 'Heuristic'} | ${
            insight?.fullConditionLabel ?? join.condition
          }${signal ? ` | ${signal.title}${metrics ? ` (${metrics})` : ''}` : ''}`,
        );
      });
    }

    lines.push('', '## Relation Signals', '');

    if (explainSummary.items.length === 0) {
      lines.push('- Paste EXPLAIN / EXPLAIN ANALYZE output to populate engine signals.');
    } else {
      explainSummary.items.slice(0, 15).forEach((signal) => {
        const metrics = formatPlanMetrics(signal, 'full');
        lines.push(`- ${signal.title}${metrics ? ` (${metrics})` : ''} — ${signal.detail}`);
      });
    }

    if (analysis.flags.length > 0) {
      lines.push('', '## Flags', '');
      analysis.flags.forEach((flag) => {
        const content = getFlagContent(flag.title, flag.description);
        lines.push(`- [${severityLabel[flag.severity]}] ${content.title} — ${content.description}`);
      });
    }

    return lines.join('\n');
  }, [activeDialect, analysis, explainSummary, joinInsights, parsedSchema.summary.tableCount, verifiedJoinCount]);

  useEffect(() => {
    const element = graphShellRef.current;
    if (!element) {
      return undefined;
    }

    const handleWheel = (event: WheelEvent) => {
      event.preventDefault();
      event.stopPropagation();

      setZoom((current) => {
        const nextZoom = Math.min(2.2, Math.max(0.45, current - event.deltaY * 0.001));
        return Number(nextZoom.toFixed(2));
      });
    };

    element.addEventListener('wheel', handleWheel, { passive: false });

    return () => {
      element.removeEventListener('wheel', handleWheel);
    };
  }, []);

  useEffect(() => {
    const element = graphShellRef.current;
    if (!element || typeof ResizeObserver === 'undefined') {
      return undefined;
    }

    const updateSize = () => {
      const rect = element.getBoundingClientRect();
      setShellSize({ width: rect.width, height: rect.height });
    };

    updateSize();

    const observer = new ResizeObserver(() => {
      updateSize();
    });

    observer.observe(element);

    return () => {
      observer.disconnect();
    };
  }, []);

  useEffect(() => {
    persistSavedQueries(savedQueries);
  }, [savedQueries]);

  useLayoutEffect(() => {
    const savedState = readWorkspaceViewState(workspaceStateKey);
    if (!savedState) {
      return;
    }

    /* eslint-disable react-hooks/set-state-in-effect */
    setDialect(savedState.dialect);
    setSchemaSql(savedState.schemaSql);
    setLayoutMode(savedState.layoutMode);
    setExpandedDerivedIds(savedState.expandedDerivedIds);
    setNodeOffsets(savedState.nodeOffsets);
    setPan(savedState.pan);
    setZoom(savedState.zoom);
    setEntityNotes(savedState.entityNotes);
    setCompareSql(savedState.compareSql);
    setCompareExplainInput(savedState.compareExplainInput);
    /* eslint-enable react-hooks/set-state-in-effect */
  }, [workspaceStateKey]);

  useEffect(() => {
    if (!workspaceStateKey) {
      return;
    }

    saveWorkspaceViewState(workspaceStateKey, {
      dialect: activeDialect,
      schemaSql,
      layoutMode,
      expandedDerivedIds,
      nodeOffsets,
      pan,
      zoom,
      entityNotes,
      compareSql,
      compareExplainInput,
    });
  }, [
    compareExplainInput,
    compareSql,
    activeDialect,
    entityNotes,
    expandedDerivedIds,
    layoutMode,
    nodeOffsets,
    pan,
    schemaSql,
    workspaceStateKey,
    zoom,
  ]);

  useEffect(() => {
    if (!statusMessage) {
      return undefined;
    }

    const timeout = window.setTimeout(() => {
      setStatusMessage(null);
    }, 2400);

    return () => {
      window.clearTimeout(timeout);
    };
  }, [statusMessage]);

  const diagnosticHighlights = useMemo(
    () =>
      diagnostics.items.map((diagnostic, index) => ({
        ...diagnostic,
        top: SQL_EDITOR_PADDING + (diagnostic.line - 1) * SQL_EDITOR_LINE_HEIGHT - sqlScrollTop,
        isPrimary: index === 0 || diagnostic.index === primaryDiagnostic?.index,
      })),
    [diagnostics.items, primaryDiagnostic?.index, sqlScrollTop],
  );

  const planPlaceholder = useMemo(() => {
    if (activeDialect === 'bigquery') {
      return 'Paste BigQuery EXPLAIN text or JSON here to overlay scans and join signals.';
    }

    if (activeDialect === 'mysql' || activeDialect === 'mariadb') {
      return 'Paste MySQL or MariaDB EXPLAIN FORMAT=JSON or text plan here to overlay scans and join signals.';
    }

    if (activeDialect === 'sqlite') {
      return 'Paste SQLite EXPLAIN QUERY PLAN output here. JSON plans are also supported if you have them.';
    }

    if (activeDialect === 'sqlserver') {
      return 'Paste SQL Server SHOWPLAN text or XML here to overlay scans, joins, and lookups.';
    }

    if (activeDialect === 'oracle') {
      return 'Paste Oracle EXPLAIN PLAN text here to overlay scans and join signals.';
    }

    if (activeDialect === 'snowflake') {
      return 'Paste Snowflake EXPLAIN text or JSON here to overlay scans and join signals.';
    }

    if (activeDialect === 'duckdb') {
      return 'Paste DuckDB EXPLAIN output here to overlay scans and join signals.';
    }

    if (activeDialect === 'redshift') {
      return 'Paste Redshift EXPLAIN output here to overlay scans and join signals.';
    }

    if (activeDialect === 'trino') {
      return 'Paste Trino or Presto EXPLAIN output here to overlay scans and join signals.';
    }

    return 'Paste EXPLAIN / EXPLAIN ANALYZE text or FORMAT JSON here to overlay scans and join signals.';
  }, [activeDialect]);

  const loadWorkspace = (nextSql: string, nextStatementIndex = 0, nextDialect?: DialectMode) => {
    setSql(nextSql);
    setSelectedStatementIndex(nextStatementIndex);
    setDialect(nextDialect ?? detectSqlDialect(nextSql, dialect).dialect);
    setSelectedAlias(null);
    setSelectedJoinId(null);
    setSelectedLineageId(null);
    setExpandedDerivedIds([]);
    setDetailTab('joins');
    setExplainInput('');
    setNodeOffsets({});
    setEntityNotes({});
    setCompareSql('');
    setCompareExplainInput('');
    setSqlScrollTop(0);
    setPan({ x: 0, y: 0 });
    setZoom(1);
  };

  const beginCanvasPan = (event: ReactPointerEvent<HTMLDivElement>) => {
    if ((event.target as HTMLElement).closest('.graph-node')) {
      return;
    }

    setSelectedAlias(null);
    setSelectedJoinId(null);
    setSelectedLineageId(null);
    setIsPanning(true);
    setLastPoint({ x: event.clientX, y: event.clientY });
  };

  const handlePointerMove = (event: ReactPointerEvent<HTMLDivElement>) => {
    if (draggingNode && lastPoint) {
      const dx = (event.clientX - lastPoint.x) / zoom;
      const dy = (event.clientY - lastPoint.y) / zoom;
      setNodeOffsets((current) => {
        const previous = current[draggingNode] ?? { x: 0, y: 0 };
        return {
          ...current,
          [draggingNode]: { x: previous.x + dx, y: previous.y + dy },
        };
      });
      setLastPoint({ x: event.clientX, y: event.clientY });
      return;
    }

    if (isPanning && lastPoint) {
      const dx = event.clientX - lastPoint.x;
      const dy = event.clientY - lastPoint.y;
      setPan((current) => ({ x: current.x + dx, y: current.y + dy }));
      setLastPoint({ x: event.clientX, y: event.clientY });
    }
  };

  const endPointerAction = () => {
    setDraggingNode(null);
    setIsPanning(false);
    setLastPoint(null);
  };

  const beginNodeDrag = (alias: string, event: ReactPointerEvent<HTMLDivElement>) => {
    event.stopPropagation();
    setSelectedAlias(alias);
    setSelectedJoinId(null);
    setSelectedLineageId(null);
    setDraggingNode(alias);
    setLastPoint({ x: event.clientX, y: event.clientY });
  };

  const resetView = () => {
    setZoom(1);
    setPan({ x: 0, y: 0 });
    setNodeOffsets({});
  };

  const handleLayoutModeChange = (nextMode: LayoutMode) => {
    if (nextMode === layoutMode) {
      return;
    }

    setLayoutMode(nextMode);
    resetView();
  };

  const handleSqlChange = (event: ChangeEvent<HTMLTextAreaElement>) => {
    const nextSql = event.target.value;
    setSql(nextSql);
    setDialect(detectSqlDialect(nextSql, dialect).dialect);
    setSelectedAlias(null);
    setSelectedJoinId(null);
    setSelectedLineageId(null);
    setExpandedDerivedIds([]);
    setNodeOffsets({});
    setSqlScrollTop(event.target.scrollTop);
    setPan({ x: 0, y: 0 });
    setZoom(1);
  };

  const handleSchemaChange = (event: ChangeEvent<HTMLTextAreaElement>) => {
    setSchemaSql(event.target.value);
  };

  const handleFileChange = async (event: ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (!file) {
      return;
    }

    const nextSql = await file.text();
    loadWorkspace(nextSql, 0);
    setStatusMessage('SQL file loaded.');
    event.target.value = '';
  };

  const handleSchemaFileChange = async (event: ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (!file) {
      return;
    }

    const nextSchemaSql = await file.text();
    setSchemaSql(nextSchemaSql);
    setStatusMessage('Schema file loaded.');
    event.target.value = '';
  };

  const handleExportGraphviz = () => {
    if (!analysis.normalizedSql.trim()) {
      return;
    }

    const dot = buildGraphvizDot(analysis);
    const blob = new Blob([dot], { type: 'text/vnd.graphviz;charset=utf-8' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = createDotFileName(analysis.analyzedStatementIndex);
    link.click();
    URL.revokeObjectURL(url);
  };

  const handleExportPng = async () => {
    if (!analysis.normalizedSql.trim() || !graphViewportRef.current) {
      return;
    }

    try {
      const dataUrl = await toPng(graphViewportRef.current, {
        cacheBust: true,
        pixelRatio: 2,
        width: graphSize.width,
        height: graphSize.height,
        backgroundColor: '#030303',
        style: {
          transform: 'none',
          transformOrigin: '0 0',
          background:
            'linear-gradient(rgba(255, 255, 255, 0.045) 1px, transparent 1px), linear-gradient(90deg, rgba(255, 255, 255, 0.045) 1px, transparent 1px), #030303',
          backgroundSize: '40px 40px, 40px 40px, auto',
        },
      });

      const link = document.createElement('a');
      link.href = dataUrl;
      link.download = createPngFileName(analysis.analyzedStatementIndex);
      link.click();
      setStatusMessage('PNG exported.');
    } catch {
      setStatusMessage('PNG export failed.');
    }
  };

  const handleExportSvg = async () => {
    if (!analysis.normalizedSql.trim() || !graphViewportRef.current) {
      return;
    }

    try {
      const dataUrl = await toSvg(graphViewportRef.current, {
        cacheBust: true,
        width: graphSize.width,
        height: graphSize.height,
        backgroundColor: '#030303',
        style: {
          transform: 'none',
          transformOrigin: '0 0',
          background:
            'linear-gradient(rgba(255, 255, 255, 0.045) 1px, transparent 1px), linear-gradient(90deg, rgba(255, 255, 255, 0.045) 1px, transparent 1px), #030303',
          backgroundSize: '40px 40px, 40px 40px, auto',
        },
      });

      const link = document.createElement('a');
      link.href = dataUrl;
      link.download = createSvgFileName(analysis.analyzedStatementIndex);
      link.click();
      setStatusMessage('SVG exported.');
    } catch {
      setStatusMessage('SVG export failed.');
    }
  };

  const handleSaveQuery = () => {
    if (!analysis.normalizedSql.trim()) {
      return;
    }

    const nextEntry: SavedQuery = {
      id: createSavedQueryId(),
      title: createQueryTitle(sql),
      sql,
      updatedAt: Date.now(),
      selectedStatementIndex: safeSelectedStatementIndex,
      dialect: activeDialect,
    };

    setSavedQueries((current) => {
      const existing = current.find((item) => item.sql === sql);
      const nextList = existing
        ? current.map((item) =>
            item.id === existing.id
              ? { ...item, title: nextEntry.title, updatedAt: nextEntry.updatedAt, selectedStatementIndex: nextEntry.selectedStatementIndex, dialect: nextEntry.dialect }
              : item,
          )
        : [nextEntry, ...current];

      return nextList
        .sort((left, right) => right.updatedAt - left.updatedAt)
        .slice(0, MAX_SAVED_QUERIES);
    });
    setStatusMessage('Saved locally.');
  };

  const handleLoadSavedQuery = (query: SavedQuery) => {
    loadWorkspace(query.sql, query.selectedStatementIndex, query.dialect);
    setStatusMessage(`Loaded "${query.title}".`);
  };

  const handleSelectJoin = (joinId: string) => {
    setSelectedJoinId(joinId);
    setSelectedAlias(null);
    setSelectedLineageId(null);
    setDetailTab('joins');
  };

  const handleFocusSearchResult = () => {
    const firstAlias = Array.from(searchState.matchedAliases)[0];
    const firstJoinId = Array.from(searchState.matchedJoinIds)[0];

    if (firstJoinId) {
      handleSelectJoin(firstJoinId);
      return;
    }

    if (firstAlias) {
      setSelectedAlias(firstAlias);
      setSelectedJoinId(null);
      setSelectedLineageId(null);
      setDetailTab('joins');
    }
  };

  const handleSelectLineage = (lineageId: string) => {
    setSelectedLineageId(lineageId);
    setSelectedAlias(null);
    setSelectedJoinId(null);
    setDetailTab('lineage');
  };

  const handleDeleteSavedQuery = (queryId: string) => {
    setSavedQueries((current) => current.filter((item) => item.id !== queryId));
    setStatusMessage('Removed from local history.');
  };

  const handleFocusDiagnostic = (diagnostic: SqlDiagnostic) => {
    if (!sqlInputRef.current) {
      return;
    }

    const nextTop = Math.max(0, (diagnostic.line - 1) * SQL_EDITOR_LINE_HEIGHT - SQL_EDITOR_LINE_HEIGHT * 1.5);
    sqlInputRef.current.focus();
    sqlInputRef.current.scrollTop = nextTop;
    setSqlScrollTop(nextTop);

    try {
      sqlInputRef.current.setSelectionRange(diagnostic.index, Math.min(sql.length, diagnostic.index + Math.max(1, diagnostic.excerpt.length)));
    } catch {
      // Ignore selection issues in browsers that reject ranges for some IME states.
    }
  };

  const handleEntityNoteChange = (entityKey: string, nextValue: string) => {
    setEntityNotes((current) => {
      const normalized = nextValue.trim();
      if (!normalized) {
        if (!(entityKey in current)) {
          return current;
        }

        const rest = { ...current };
        delete rest[entityKey];
        return rest;
      }

      return {
        ...current,
        [entityKey]: nextValue,
      };
    });
  };

  const handleToggleDerivedNode = (derivedId: string) => {
    setExpandedDerivedIds((current) =>
      current.includes(derivedId) ? current.filter((item) => item !== derivedId) : [...current, derivedId],
    );
  };

  const handleMinimapNavigate = (event: ReactPointerEvent<HTMLDivElement>) => {
    event.stopPropagation();

    if (!graphShellRef.current || minimap.scale === 0) {
      return;
    }

    const rect = event.currentTarget.getBoundingClientRect();
    const graphX = (event.clientX - rect.left) / minimap.scale;
    const graphY = (event.clientY - rect.top) / minimap.scale;

    setPan({
      x: shellSize.width / 2 - graphX * zoom,
      y: shellSize.height / 2 - graphY * zoom,
    });
  };

  const handleCopyReviewReport = async () => {
    if (!analysis.normalizedSql.trim()) {
      return;
    }

    try {
      await navigator.clipboard.writeText(reviewReport);
      setStatusMessage('Review report copied.');
    } catch {
      window.prompt('Copy this review report:', reviewReport);
    }
  };

  const handleCopyExecutionReport = async () => {
    if (!analysis.normalizedSql.trim()) {
      return;
    }

    try {
      await navigator.clipboard.writeText(executionReport);
      setStatusMessage('Execution report copied.');
    } catch {
      window.prompt('Copy this execution report:', executionReport);
    }
  };

  const handleCopyShareLink = async () => {
    if (!analysis.normalizedSql.trim()) {
      return;
    }

    const shareUrl = createShareUrl(sql, safeSelectedStatementIndex, activeDialect);

    try {
      await navigator.clipboard.writeText(shareUrl);
      setStatusMessage('Share link copied.');
    } catch {
      window.prompt('Copy this share link:', shareUrl);
    }
  };

  const handleGlobalKeyDown = useEffectEvent((event: KeyboardEvent) => {
    const target = event.target as HTMLElement | null;
    const isEditing =
      target instanceof HTMLTextAreaElement ||
      target instanceof HTMLInputElement ||
      target?.isContentEditable;

    if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === 's') {
      event.preventDefault();
      handleSaveQuery();
      return;
    }

    if ((event.metaKey || event.ctrlKey) && event.shiftKey && event.key.toLowerCase() === 'c') {
      event.preventDefault();
      void handleCopyShareLink();
      return;
    }

    if (isEditing) {
      return;
    }

    if (event.key === 'Escape') {
      if (showShortcutHelp) {
        setShowShortcutHelp(false);
        return;
      }
      setSelectedAlias(null);
      setSelectedJoinId(null);
      setSelectedLineageId(null);
      setSearchQuery('');
    }

    if (event.key.toLowerCase() === 'f') {
      event.preventDefault();
      resetView();
    }

    if ((event.key === '?' || (event.shiftKey && event.key === '/')) && !isEditing) {
      event.preventDefault();
      setShowShortcutHelp((current) => !current);
    }
  });

  useEffect(() => {
    window.addEventListener('keydown', handleGlobalKeyDown);

    return () => {
      window.removeEventListener('keydown', handleGlobalKeyDown);
    };
  }, []);

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="logo-panel logo-panel--textonly">
          <strong className="brand-wordmark">QUERYVIZ</strong>
        </div>

        <div className="editor-panel">
          <div className="panel-head">
            <span>SQL input</span>
            <strong>{sql.length} chars · {dialectLabel[activeDialect]} · Auto</strong>
          </div>
          <div className="dialect-meta" aria-live="polite">
            <strong>Detected {dialectLabel[activeDialect]}</strong>
            <small>
              {dialectDetection.confident
                ? `Auto-detected from ${formatDialectEvidence(dialectDetection)}`
                : 'Auto-detecting from the SQL shape. Add a little more vendor-specific syntax if needed.'}
            </small>
          </div>
          <div className="sql-editor-shell">
            <div className="sql-editor__highlights" aria-hidden="true">
              {diagnosticHighlights.map((diagnostic) => (
                <div
                  key={`${diagnostic.index}-${diagnostic.title}`}
                  className={`sql-editor__highlight sql-editor__highlight--${diagnostic.severity}${diagnostic.isPrimary ? ' is-primary' : ''}`}
                  style={{ transform: `translateY(${diagnostic.top}px)` }}
                />
              ))}
            </div>
            <textarea
              ref={sqlInputRef}
              className="sql-input"
              value={sql}
              onChange={handleSqlChange}
              onScroll={(event) => setSqlScrollTop(event.currentTarget.scrollTop)}
              spellCheck={false}
              placeholder="Paste a SELECT query here..."
            />
          </div>
          <div className="editor-actions">
            <button
              type="button"
              onClick={() => {
                loadWorkspace(getDialectSampleSql(activeDialect), 0, activeDialect);
                setStatusMessage(`Loaded ${dialectLabel[activeDialect]} sample.`);
              }}
            >
              Load sample
            </button>
            <button type="button" onClick={() => fileInputRef.current?.click()}>
              Open .sql
            </button>
            <button type="button" onClick={() => loadWorkspace('')}>
              Clear
            </button>
            <button type="button" onClick={handleSaveQuery} disabled={!analysis.normalizedSql.trim()}>
              Save query
            </button>
            <button type="button" onClick={() => void handleCopyShareLink()} disabled={!analysis.normalizedSql.trim()}>
              Copy share link
            </button>
            <button type="button" onClick={() => void handleCopyReviewReport()} disabled={!analysis.normalizedSql.trim()}>
              Copy review report
            </button>
            <button type="button" onClick={() => void handleCopyExecutionReport()} disabled={!analysis.normalizedSql.trim()}>
              Copy execution report
            </button>
            <button type="button" onClick={() => setShowShortcutHelp(true)}>
              Shortcuts
            </button>
          </div>
          <input ref={fileInputRef} className="hidden-file-input" type="file" accept=".sql,.txt" onChange={handleFileChange} />
          {statements.length > 1 ? (
            <p className="editor-note">Detected {statements.length} SQL statements. Pick one below to graph it.</p>
          ) : null}
          {statusMessage ? <p className="editor-note editor-note--status">{statusMessage}</p> : null}
        </div>

        <section className="statement-panel">
          <div className="panel-head">
            <span>Schema import</span>
            <strong>{parsedSchema.summary.tableCount > 0 ? `${parsedSchema.summary.tableCount} tables` : 'Optional'}</strong>
          </div>
          <textarea
            className="plan-input schema-input"
            value={schemaSql}
            onChange={handleSchemaChange}
            spellCheck={false}
            placeholder="Paste CREATE TABLE / CREATE INDEX DDL here to verify PK/FK joins and index coverage."
          />
          <div className="editor-actions editor-actions--compact">
            <button
              type="button"
              onClick={() => {
                setSchemaSql(SAMPLE_SCHEMA_SQL);
                setStatusMessage('Sample schema loaded.');
              }}
            >
              Load sample schema
            </button>
            <button type="button" onClick={() => schemaFileInputRef.current?.click()}>
              Open DDL
            </button>
            <button
              type="button"
              onClick={() => {
                setSchemaSql('');
                setStatusMessage('Schema cleared.');
              }}
              disabled={!schemaSql.trim()}
            >
              Clear schema
            </button>
          </div>
          <input
            ref={schemaFileInputRef}
            className="hidden-file-input"
            type="file"
            accept=".sql,.ddl,.txt"
            onChange={handleSchemaFileChange}
          />
          {schemaSql.trim() ? (
            parsedSchema.summary.tableCount > 0 ? (
              <>
                <div className="plan-summary">
                  <span className="plan-summary__chip">Tables: {parsedSchema.summary.tableCount}</span>
                  <span className="plan-summary__chip">Foreign keys: {parsedSchema.summary.foreignKeyCount}</span>
                  <span className="plan-summary__chip">Indexed groups: {parsedSchema.summary.indexedGroupCount}</span>
                  <span className={`plan-summary__chip${verifiedJoinCount > 0 ? ' plan-summary__chip--good' : ''}`}>
                    Verified joins: {verifiedJoinCount}{analysis.joins.length > 0 ? ` / ${analysis.joins.length}` : ''}
                  </span>
                </div>
                <p className="editor-note">
                  Matching joins will switch from <strong>Heuristic</strong> to <strong>Verified</strong> when the imported schema confirms FK or key coverage.
                </p>
              </>
            ) : (
              <p className="editor-note">No `CREATE TABLE` or `CREATE INDEX` statements were recognized yet. Paste a little more DDL.</p>
            )
          ) : (
            <p className="editor-note">Keep this local and optional. Queryviz uses it only to improve confidence on joins and index hints.</p>
          )}
        </section>

        {diagnostics.items.length > 0 ? (
          <section className="statement-panel">
            <div className="panel-head">
              <span>Diagnostics</span>
              <strong>{diagnostics.items.length}</strong>
            </div>
            <div className="diagnostic-list">
              {diagnostics.items.map((diagnostic) => (
                <article
                  key={`${diagnostic.title}-${diagnostic.index}`}
                  className={`diagnostic-card diagnostic-card--${diagnostic.severity}`}
                  onClick={() => handleFocusDiagnostic(diagnostic)}
                >
                  <div className="diagnostic-card__head">
                    <strong>{diagnostic.title}</strong>
                    <span>
                      Ln {diagnostic.line}, Col {diagnostic.column}
                    </span>
                  </div>
                  <p>{diagnostic.message}</p>
                  <code>{diagnostic.excerpt}</code>
                  <small>{diagnostic.hint}</small>
                </article>
              ))}
            </div>
          </section>
        ) : null}

        {statements.length > 1 ? (
          <section className="statement-panel">
            <div className="panel-head">
              <span>Statements</span>
              <strong>{statements.length}</strong>
            </div>
            <div className="statement-list">
              {statements.map((statement, index) => (
                <button
                  key={`${index}-${statement.slice(0, 24)}`}
                  type="button"
                  className={`statement-item${safeSelectedStatementIndex === index ? ' statement-item--active' : ''}`}
                  onClick={() => {
                    setSelectedStatementIndex(index);
                    setSelectedAlias(null);
                    setSelectedJoinId(null);
                    setSelectedLineageId(null);
                    setExpandedDerivedIds([]);
                    setNodeOffsets({});
                    setPan({ x: 0, y: 0 });
                    setZoom(1);
                  }}
                >
                  {summarizeStatement(statement, index)}
                </button>
              ))}
            </div>
          </section>
        ) : null}

        <section className="statement-panel">
          <div className="panel-head">
            <span>Saved queries</span>
            <strong>{savedQueries.length}</strong>
          </div>
          <div className="saved-query-list">
            {savedQueries.length > 0 ? (
              savedQueries.map((query) => (
                <article key={query.id} className="saved-query-card">
                  <button type="button" className="saved-query-card__open" onClick={() => handleLoadSavedQuery(query)}>
                    <strong>{query.title}</strong>
                    <span>{new Date(query.updatedAt).toLocaleString()}</span>
                  </button>
                  <button
                    type="button"
                    className="saved-query-card__delete"
                    onClick={() => handleDeleteSavedQuery(query.id)}
                    aria-label={`Delete ${query.title}`}
                  >
                    Delete
                  </button>
                </article>
              ))
            ) : (
              <p className="editor-note">Save a query locally to reopen it later.</p>
            )}
          </div>
        </section>

        <section className="statement-panel">
          <div className="panel-head">
            <span>Plan overlay</span>
            <strong>{explainSummary.items.length}</strong>
          </div>
          <textarea
            className="plan-input"
            value={explainInput}
            onChange={(event) => setExplainInput(event.target.value)}
            spellCheck={false}
            placeholder={planPlaceholder}
          />
          {explainSummary.items.length > 0 ? (
            <>
              <div className="plan-summary">
                <span className="plan-summary__chip">Seq scans: {explainSummary.summary.seqScans}</span>
                <span className="plan-summary__chip">Indexed reads: {explainSummary.summary.indexedReads}</span>
                <span className="plan-summary__chip">Join nodes: {explainSummary.summary.joinNodes}</span>
                <span className="plan-summary__chip">Sorts: {explainSummary.summary.sorts}</span>
                <span className="plan-summary__chip">
                  Mapped joins: {explainSummary.summary.mappedJoins}
                  {analysis.joins.length > 0 ? ` / ${analysis.joins.length}` : ''}
                </span>
                {explainSummary.summary.maxCost > 0 ? (
                  <span className="plan-summary__chip">Max cost: {formatCompactNumber(explainSummary.summary.maxCost)}</span>
                ) : null}
                {explainSummary.summary.maxRows > 0 ? (
                  <span className="plan-summary__chip">Max rows: {formatCompactNumber(explainSummary.summary.maxRows)}</span>
                ) : null}
                {explainSummary.summary.estimateWarnings > 0 ? (
                  <span className="plan-summary__chip plan-summary__chip--warning">Misestimates: {explainSummary.summary.estimateWarnings}</span>
                ) : null}
              </div>
              <div className="plan-legend">
                <span className="plan-legend__item plan-legend__item--high">High signal</span>
                <span className="plan-legend__item plan-legend__item--medium">Medium signal</span>
                <span className="plan-legend__item plan-legend__item--low">Low signal</span>
                <span className="plan-legend__item plan-legend__item--estimate">Est 4x high / low = planner misestimate</span>
              </div>
              {analysis.joins.length > 0 && explainSummary.summary.mappedJoins < analysis.joins.length ? (
                <p className="editor-note">
                  Some joins are still unmatched. Paste a fuller plan or engine-native JSON/XML output for better edge mapping.
                </p>
              ) : null}
              <div className="plan-signal-list">
                {explainSummary.items.slice(0, 6).map((signal) => (
                  <article key={signal.id} className={`plan-signal plan-signal--${signal.severity}`}>
                    <strong>{signal.title}</strong>
                    {formatPlanMetrics(signal, 'full') ? <small>{formatPlanMetrics(signal, 'full')}</small> : null}
                    {getEstimateBadgeLabel(signal.estimateFactor) ? (
                      <span className={`plan-signal__estimate plan-signal__estimate--${getEstimateSeverity(signal.estimateFactor)}`}>
                        {getEstimateBadgeLabel(signal.estimateFactor)}
                      </span>
                    ) : null}
                    <span>{signal.detail}</span>
                  </article>
                ))}
              </div>
            </>
          ) : (
            <p className="editor-note">Paste a text or JSON plan and Queryviz will flag seq scans, joins, sorts, and indexed reads on the graph.</p>
          )}
        </section>

        <section className="statement-panel">
          <div className="panel-head">
            <span>Compare mode</span>
            <strong>{compareSummary && compareAnalysis ? `vs #${compareAnalysis.analyzedStatementIndex + 1}` : 'Optional'}</strong>
          </div>
          <textarea
            className="plan-input compare-input"
            value={compareSql}
            onChange={(event) => setCompareSql(event.target.value)}
            spellCheck={false}
            placeholder="Paste a baseline query to compare the current graph against it."
          />
          <textarea
            className="plan-input compare-input compare-input--secondary"
            value={compareExplainInput}
            onChange={(event) => setCompareExplainInput(event.target.value)}
            spellCheck={false}
            placeholder="Optional: paste a baseline EXPLAIN plan (text or JSON) for plan-level deltas."
          />
          <div className="editor-actions editor-actions--compact">
            <button type="button" onClick={() => {
              setCompareSql(sql);
              setCompareExplainInput(explainInput);
            }} disabled={!analysis.normalizedSql.trim()}>
              Use current as baseline
            </button>
            <button type="button" onClick={() => {
              setCompareSql('');
              setCompareExplainInput('');
            }} disabled={!compareSql.trim() && !compareExplainInput.trim()}>
              Clear compare
            </button>
          </div>
          {compareSummary && compareAnalysis ? (
            <div className="compare-panel">
              <div className="compare-summary">
                <span className={`compare-chip ${compareSummary.complexityDelta > 0 ? 'is-worse' : compareSummary.complexityDelta < 0 ? 'is-better' : ''}`}>
                  Complexity {formatSignedDelta(compareSummary.complexityDelta)}
                </span>
                <span className="compare-chip">Tables {formatSignedDelta(compareSummary.tableDelta)}</span>
                <span className="compare-chip">Joins {formatSignedDelta(compareSummary.joinDelta)}</span>
                <span className={`compare-chip ${compareSummary.flagDelta > 0 ? 'is-worse' : compareSummary.flagDelta < 0 ? 'is-better' : ''}`}>
                  Flags {formatSignedDelta(compareSummary.flagDelta)}
                </span>
                {compareSummary.hasPlanComparison ? (
                  <>
                    <span className={`compare-chip ${compareSummary.seqScanDelta > 0 ? 'is-worse' : compareSummary.seqScanDelta < 0 ? 'is-better' : ''}`}>
                      Seq scans {formatSignedDelta(compareSummary.seqScanDelta)}
                    </span>
                    <span className={`compare-chip ${compareSummary.joinNodeDelta > 0 ? 'is-worse' : compareSummary.joinNodeDelta < 0 ? 'is-better' : ''}`}>
                      Join nodes {formatSignedDelta(compareSummary.joinNodeDelta)}
                    </span>
                    {compareSummary.maxCostDelta !== null ? (
                      <span className={`compare-chip ${compareSummary.maxCostDelta > 0 ? 'is-worse' : compareSummary.maxCostDelta < 0 ? 'is-better' : ''}`}>
                        Max cost {formatSignedDelta(Number(compareSummary.maxCostDelta.toFixed(2)))}
                      </span>
                    ) : null}
                    {compareSummary.maxRowsDelta !== null ? (
                      <span className={`compare-chip ${compareSummary.maxRowsDelta > 0 ? 'is-worse' : compareSummary.maxRowsDelta < 0 ? 'is-better' : ''}`}>
                        Max rows {formatSignedDelta(compareSummary.maxRowsDelta)}
                      </span>
                    ) : null}
                  </>
                ) : null}
              </div>
              <div className="compare-groups">
                <article className="compare-group">
                  <strong>Table changes</strong>
                  {compareSummary.addedTables.length > 0 || compareSummary.removedTables.length > 0 ? (
                    <ul className="compare-list">
                      {compareSummary.addedTables.map((item) => (
                        <li key={`table-added-${item}`} className="compare-list__item compare-list__item--added">Added: {item}</li>
                      ))}
                      {compareSummary.removedTables.map((item) => (
                        <li key={`table-removed-${item}`} className="compare-list__item compare-list__item--removed">Removed: {item}</li>
                      ))}
                    </ul>
                  ) : (
                    <p className="editor-note">No table changes.</p>
                  )}
                </article>
                <article className="compare-group">
                  <strong>Added joins</strong>
                  {compareSummary.addedJoins.length > 0 ? (
                    <ul className="compare-list compare-list--added">
                      {compareSummary.addedJoins.slice(0, 6).map((item) => (
                        <li key={item}>{item}</li>
                      ))}
                    </ul>
                  ) : (
                    <p className="editor-note">No added joins.</p>
                  )}
                </article>
                <article className="compare-group">
                  <strong>Removed joins</strong>
                  {compareSummary.removedJoins.length > 0 ? (
                    <ul className="compare-list compare-list--removed">
                      {compareSummary.removedJoins.slice(0, 6).map((item) => (
                        <li key={item}>{item}</li>
                      ))}
                    </ul>
                  ) : (
                    <p className="editor-note">No removed joins.</p>
                  )}
                </article>
                <article className="compare-group">
                  <strong>Flag changes</strong>
                  {compareSummary.addedFlags.length > 0 || compareSummary.removedFlags.length > 0 ? (
                    <ul className="compare-list">
                      {compareSummary.addedFlags.map((item) => (
                        <li key={`added-${item}`} className="compare-list__item compare-list__item--added">Added: {item}</li>
                      ))}
                      {compareSummary.removedFlags.map((item) => (
                        <li key={`removed-${item}`} className="compare-list__item compare-list__item--removed">Removed: {item}</li>
                      ))}
                    </ul>
                  ) : (
                    <p className="editor-note">No flag changes.</p>
                  )}
                </article>
              </div>
            </div>
          ) : (
            <p className="editor-note">
              Paste a baseline query to diff joins, flags, complexity, and optional plan metrics against the current workspace.
            </p>
          )}
        </section>

        <div className="sidebar-grid">
          <section className="info-card">
            <div className="panel-head">
              <span>Metrics</span>
              <strong>{analysis.complexityScore}</strong>
            </div>
            <ul>
              <li>Dialect: {dialectLabel[activeDialect]}</li>
              <li>Statement: {formatStatementTypeLabel(analysis.statementType)}</li>
              <li>Schema mode: {parsedSchema.summary.tableCount > 0 ? 'Imported metadata' : 'Heuristic only'}</li>
              {analysis.writeTarget ? <li>Write target: {analysis.writeTarget}</li> : null}
              <li>Tables: {analysis.tables.length}</li>
              <li>Joins: {analysis.joins.length}</li>
              <li>Verified joins: {verifiedJoinCount}</li>
              <li>Filters: {analysis.filters.length}</li>
              <li>Columns: {analysis.columns.length}</li>
              <li>Fanout paths: {fanoutState.impactedAliases.size}</li>
            </ul>
          </section>

          <section className="info-card info-card--flags-left">
            <div className="panel-head">
              <span>Flags</span>
              <strong>{analysis.flags.length}</strong>
            </div>
            <div className="flag-list compact">
              {analysis.flags.length > 0 ? (
                analysis.flags.slice(0, 5).map((flag) => {
                  const content = getFlagContent(flag.title, flag.description);

                  return (
                    <article key={flag.title} className={`flag flag--${flag.severity}`}>
                      <div className="flag__head">
                        <strong>{content.title}</strong>
                        <span className="flag__severity">{severityLabel[flag.severity]}</span>
                      </div>
                      <p className="flag__description">{content.description}</p>
                    </article>
                  );
                })
              ) : (
                <article className="flag flag--clear">
                  <strong>No major warnings</strong>
                  <span>Nothing obvious stood out from the current SQL shape</span>
                </article>
              )}
            </div>
            {analysis.flags.length > 5 ? (
              <p className="editor-note">Showing top 5 flags. Copy the review report to capture the full list.</p>
            ) : null}
          </section>
        </div>
      </aside>

      <main className="canvas-panel">
        <section className="workspace-grid">
          <div className="graph-card">
            <div className="canvas-toolbar">
              <div className="canvas-toolbar__title-group">
                <span className="canvas-toolbar__label">Graph</span>
                <strong>{analysis.joins.length > 0 ? `Statement #${analysis.analyzedStatementIndex + 1}` : 'Waiting for joins'}</strong>
              </div>
              <div className="canvas-toolbar__controls">
                <div className="canvas-toolbar__search">
                  <input
                    type="search"
                    value={searchQuery}
                    onChange={(event) => setSearchQuery(event.target.value)}
                    placeholder="Search tables, aliases, joins..."
                    aria-label="Search graph"
                  />
                  <span className="canvas-toolbar__search-meta">
                    {searchState.active ? formatPlural(searchState.matchCount, 'match', 'matches') : 'Search graph'}
                  </span>
                  <button type="button" onClick={handleFocusSearchResult} disabled={!searchState.active || searchState.matchCount === 0}>
                    Focus first
                  </button>
                </div>
                <div className="canvas-toolbar__modes" role="tablist" aria-label="Layout mode">
                  {(['horizontal', 'vertical', 'radial'] as LayoutMode[]).map((mode) => (
                    <button
                      key={mode}
                      type="button"
                      className={layoutMode === mode ? 'is-active' : ''}
                      onClick={() => handleLayoutModeChange(mode)}
                      aria-pressed={layoutMode === mode}
                    >
                      {layoutModeLabel[mode]}
                    </button>
                  ))}
                </div>
                <button type="button" onClick={() => setZoom((value) => Math.max(0.45, Number((value - 0.1).toFixed(2))))}>
                  -
                </button>
                <span>{Math.round(zoom * 100)}%</span>
                <button type="button" onClick={() => setZoom((value) => Math.min(2.2, Number((value + 0.1).toFixed(2))))}>
                  +
                </button>
                <button type="button" onClick={resetView}>
                  Reset view
                </button>
                <div className={`export-menu${!analysis.normalizedSql.trim() ? ' export-menu--disabled' : ''}`}>
                  <button type="button" className="export-menu__trigger" disabled={!analysis.normalizedSql.trim()}>
                    Export
                  </button>
                  <div className="export-menu__dropdown">
                    <button type="button" onClick={handleExportGraphviz} disabled={!analysis.normalizedSql.trim()}>
                      Export DOT
                    </button>
                    <button type="button" onClick={() => void handleExportSvg()} disabled={!analysis.normalizedSql.trim()}>
                      Export as SVG
                    </button>
                    <button type="button" onClick={() => void handleExportPng()} disabled={!analysis.normalizedSql.trim()}>
                      Export as PNG
                    </button>
                  </div>
                </div>
              </div>
            </div>

            <div
              ref={graphShellRef}
              className="graph-shell"
              onPointerDown={beginCanvasPan}
              onPointerMove={handlePointerMove}
              onPointerUp={endPointerAction}
              onPointerLeave={endPointerAction}
            >
              <div
                ref={graphViewportRef}
                className="graph-viewport"
                style={{
                  width: `${graphSize.width}px`,
                  height: `${graphSize.height}px`,
                  transform: `translate(${pan.x}px, ${pan.y}px) scale(${zoom})`,
                }}
              >
                <svg className="graph-edges" width={graphSize.width} height={graphSize.height} viewBox={`0 0 ${graphSize.width} ${graphSize.height}`}>
                  {compareOverlay && compareEdgePortMap
                    ? compareOverlay.removedJoins.map((join) => {
                        const edge = compareEdgePortMap.get(join.id);
                        if (!edge) {
                          return null;
                        }

                        const path =
                          `M ${edge.startX} ${edge.startY} ` +
                          `C ${edge.controlOneX} ${edge.controlOneY}, ${edge.controlTwoX} ${edge.controlTwoY}, ${edge.endX} ${edge.endY}`;
                        const label = buildJoinComparisonLabel(join);

                        return (
                          <g key={`removed-${join.id}`} className="graph-edge graph-edge--compare-removed">
                            <title>{`Removed in current query\n${label}`}</title>
                            <path d={path} className="graph-edge-path" />
                          </g>
                        );
                      })
                    : null}
                  {analysis.joins.map((join) => {
                    const edge = edgePortMap.get(join.id);
                    const insight = joinInsights[join.id];
                    if (!edge || !insight) {
                      return null;
                    }

                    const isFocusRelated = !focusState.active || focusState.relatedJoinIds.has(join.id);
                    const isSearchMatched = searchState.matchedJoinIds.has(join.id);
                    const isSearchVisible =
                      !searchState.active ||
                      isSearchMatched ||
                      searchState.matchedAliases.has(join.sourceAlias) ||
                      searchState.matchedAliases.has(join.targetAlias);
                    const planSignal = getStrongestExplainSignal([
                      ...(explainSummary.joinSignals[join.id] ?? []),
                      ...(explainSummary.relationSignals[join.sourceAlias] ?? []),
                      ...(explainSummary.relationSignals[join.targetAlias] ?? []),
                    ]);
                    const estimateSeverity = getEstimateSeverity(planSignal?.estimateFactor);
                    const estimateLabel = getEstimateBadgeLabel(planSignal?.estimateFactor);
                    const isDimmed =
                      (focusState.active && !focusState.relatedJoinIds.has(join.id)) ||
                      (searchState.active && !isSearchVisible);
                    const path =
                      `M ${edge.startX} ${edge.startY} ` +
                      `C ${edge.controlOneX} ${edge.controlOneY}, ${edge.controlTwoX} ${edge.controlTwoY}, ${edge.endX} ${edge.endY}`;
                    const labelBoxX = edge.labelX - insight.labelWidth / 2;
                    const labelBoxY = edge.labelY - 26;
                    const badgeX = labelBoxX + insight.labelWidth + 10;
                    const badgeY = edge.labelY - 14;
                    const planMetrics = planSignal ? formatPlanMetrics(planSignal, 'compact') : '';
                    const planBadgeWidth = planMetrics ? Math.max(58, Math.min(104, planMetrics.length * 5.9 + 18)) : 0;
                    const joinNote = entityNotes[getJoinNoteKey(join.id)]?.trim() ?? '';
                    const estimateBadgeWidth = estimateLabel ? Math.max(72, Math.min(108, estimateLabel.length * 6.4 + 18)) : 0;
                    const noteBadgeX =
                      badgeX +
                      50 +
                      (planSignal && planMetrics ? planBadgeWidth + 6 : 0) +
                      (estimateLabel ? estimateBadgeWidth + 6 : 0);
                    const fanoutImpact = fanoutState.joinImpacts[join.id] ?? null;

                    return (
                      <g
                        key={join.id}
                        className={`graph-edge${
                          selectedJoinId === join.id ? ' graph-edge--selected' : ''
                        }${
                          focusState.active && isFocusRelated ? ' graph-edge--related' : ''
                        }${
                          isSearchMatched ? ' graph-edge--matched' : ''
                        }${
                          planSignal ? ` graph-edge--plan-${planSignal.severity}` : ''
                        }${
                          estimateSeverity && estimateSeverity !== 'low' ? ` graph-edge--estimate-${estimateSeverity}` : ''
                        }${
                          fanoutImpact ? ` graph-edge--fanout-${fanoutImpact.severity}` : ''
                        }${
                          compareOverlay?.addedJoinIds.has(join.id) ? ' graph-edge--compare-added' : ''
                        }${
                          isDimmed ? ' graph-edge--dimmed' : ''
                        }`}
                        onPointerDown={(event) => {
                          event.stopPropagation();
                          handleSelectJoin(join.id);
                        }}
                      >
                        <title>
                          {`${insight.joinLabel} / ${insight.fullConditionLabel}\n${insight.badgeLabel}: ${insight.badgeHint}${
                            planSignal ? `\nPlan: ${planSignal.title} — ${planSignal.detail}` : ''
                          }${fanoutImpact ? `\nFanout: ${fanoutImpact.reason}` : ''}${joinNote ? `\nNote: ${joinNote}` : ''}`}
                        </title>
                        <path d={path} className="graph-edge-hitbox" />
                        <path d={path} className="graph-edge-path" />
                        <rect x={labelBoxX} y={labelBoxY} width={insight.labelWidth} height={42} rx={10} className="graph-edge-label-box" />
                        <text x={edge.labelX} y={edge.labelY - 6} className="graph-edge-label graph-edge-label--primary">
                          {insight.joinLabel}
                        </text>
                        <text x={edge.labelX} y={edge.labelY + 10} className="graph-edge-label graph-edge-label--secondary">
                          {insight.compactCondition}
                        </text>
                        <rect x={badgeX} y={badgeY} width={44} height={22} rx={11} className={`graph-edge-badge graph-edge-badge--${insight.tone}`} />
                        <text x={badgeX + 22} y={badgeY + 15} className="graph-edge-badge__text">
                          {insight.badgeLabel}
                        </text>
                        {planSignal && planMetrics ? (
                          <>
                            <rect
                              x={badgeX + 50}
                              y={badgeY}
                              width={planBadgeWidth}
                              height={22}
                              rx={11}
                              className={`graph-edge-badge graph-edge-badge--plan graph-edge-badge--plan-${planSignal.severity}`}
                            />
                            <text x={badgeX + 50 + planBadgeWidth / 2} y={badgeY + 15} className="graph-edge-badge__text graph-edge-badge__text--plan">
                              {planMetrics}
                            </text>
                          </>
                        ) : null}
                        {estimateLabel ? (
                          <>
                            <rect
                              x={badgeX + 50 + (planSignal && planMetrics ? planBadgeWidth + 6 : 0)}
                              y={badgeY}
                              width={estimateBadgeWidth}
                              height={22}
                              rx={11}
                              className={`graph-edge-badge graph-edge-badge--estimate graph-edge-badge--estimate-${estimateSeverity}`}
                            />
                            <text
                              x={badgeX + 50 + (planSignal && planMetrics ? planBadgeWidth + 6 : 0) + estimateBadgeWidth / 2}
                              y={badgeY + 15}
                              className="graph-edge-badge__text graph-edge-badge__text--estimate"
                            >
                              {estimateLabel}
                            </text>
                          </>
                        ) : null}
                        {joinNote ? (
                          <>
                            <rect
                              x={noteBadgeX}
                              y={badgeY}
                              width={28}
                              height={22}
                              rx={11}
                              className="graph-edge-badge graph-edge-badge--note"
                            />
                            <text x={noteBadgeX + 14} y={badgeY + 15} className="graph-edge-badge__text graph-edge-badge__text--note">
                              N
                            </text>
                          </>
                        ) : null}
                      </g>
                    );
                  })}
                </svg>

                {positionedTables.map((table) => (
                  (() => {
                    const isSelected = selectedAlias === table.alias && !selectedJoinId;
                    const isFocusRelated = !focusState.active || focusState.relatedAliases.has(table.alias);
                    const isSearchMatched = searchState.matchedAliases.has(table.alias);
                    const isDimmed =
                      (focusState.active && !focusState.relatedAliases.has(table.alias)) ||
                      (searchState.active && !isSearchMatched);
                    const planSignal = getStrongestExplainSignal(explainSummary.relationSignals[table.alias]);
                    const planMetrics = planSignal ? formatPlanMetrics(planSignal, 'compact') : '';
                    const estimateSeverity = getEstimateSeverity(planSignal?.estimateFactor);
                    const estimateLabel = getEstimateBadgeLabel(planSignal?.estimateFactor);
                    const derived = table.derivedId ? derivedRelationMap[table.derivedId] : null;
                    const specialLabel = getSpecialNodeLabel(table);
                    const isExpanded = Boolean(table.derivedId && expandedDerivedIds.includes(table.derivedId));
                    const nodeNote = entityNotes[getNodeNoteKey(table.alias)]?.trim() ?? '';
                    const fanoutImpact = fanoutState.aliasImpacts[table.alias] ?? null;

                    return (
                      <div
                        key={table.alias}
                        className={`graph-node ${table.role === 'source' ? 'graph-node--source' : ''}${
                          table.role === 'target' ? ' graph-node--target' : ''
                        }${
                          derived ? ' graph-node--derived' : ''
                        }${
                          specialLabel ? ' graph-node--special' : ''
                        }${
                          derived ? ` graph-node--derived-${derived.kind}` : ''
                        }${
                          isSelected ? ' graph-node--selected' : ''
                        }${
                          focusState.active && !isSelected && isFocusRelated ? ' graph-node--related' : ''
                        }${
                          isSearchMatched ? ' graph-node--matched' : ''
                        }${
                          planSignal ? ` graph-node--plan-${planSignal.severity}` : ''
                        }${
                          estimateSeverity && estimateSeverity !== 'low' ? ` graph-node--estimate-${estimateSeverity}` : ''
                        }${
                          fanoutImpact ? ` graph-node--fanout-${fanoutImpact.severity}` : ''
                        }${
                          compareOverlay?.addedAliases.has(table.alias) ? ' graph-node--compare-added' : ''
                        }${
                          isDimmed ? ' graph-node--dimmed' : ''
                        }`}
                        style={{ transform: `translate(${table.x}px, ${table.y}px)` }}
                        onPointerDown={(event) => beginNodeDrag(table.alias, event)}
                      >
                        <span className="graph-node__role">
                          {table.role === 'source' ? 'source' : table.role === 'target' ? 'target' : 'join'}
                        </span>
                        {nodeNote ? (
                          <span className="graph-node__note-badge" title={nodeNote}>
                            Note
                          </span>
                        ) : null}
                        {planSignal ? (
                          <span className={`graph-node__plan-badge graph-node__plan-badge--${planSignal.severity}`}>
                            {planSignal.title}
                            {planMetrics ? <small>{planMetrics}</small> : null}
                          </span>
                        ) : null}
                        {estimateLabel ? (
                          <span className={`graph-node__estimate-badge graph-node__estimate-badge--${estimateSeverity}`}>
                            {estimateLabel}
                          </span>
                        ) : null}
                        {fanoutImpact ? (
                          <span className={`graph-node__fanout-badge graph-node__fanout-badge--${fanoutImpact.severity}`} title={fanoutImpact.reason}>
                            Fanout
                          </span>
                        ) : null}
                        {derived ? (
                          <button
                            type="button"
                            className={`graph-node__toggle${isExpanded ? ' is-open' : ''}`}
                            onPointerDown={(event) => {
                              event.stopPropagation();
                            }}
                            onClick={(event) => {
                              event.stopPropagation();
                              handleToggleDerivedNode(derived.id);
                            }}
                          >
                            {isExpanded ? 'Hide' : 'Expand'}
                          </button>
                        ) : null}
                        <strong>{table.name}</strong>
                        <p>alias: {table.alias}</p>
                        <div className="graph-node__meta">
                          {derived ? <span className="graph-node__kind">{derived.kind.toUpperCase()}</span> : null}
                          {specialLabel ? <span className="graph-node__special-badge">{specialLabel}</span> : null}
                        </div>
                        {derived && isExpanded ? (
                          <div className="graph-node__detail" onPointerDown={(event) => event.stopPropagation()}>
                            <div className="graph-node__detail-head">
                              <strong>{derived.kind === 'cte' ? 'CTE details' : 'Subquery details'}</strong>
                              <span>{formatPlural(derived.joinCount, 'join')}</span>
                            </div>
                            <p>
                              Sources: {derived.sourceCount} · Subqueries: {derived.subqueryCount} · Aggregation:{' '}
                              {derived.hasAggregation ? 'Yes' : 'No'}
                            </p>
                            {derived.dependencies.length > 0 ? (
                              <div className="graph-node__detail-section">
                                <span>Dependencies</span>
                                <code>{derived.dependencies.join(', ')}</code>
                              </div>
                            ) : null}
                            {derived.flags.length > 0 ? (
                              <div className="graph-node__detail-section">
                                <span>Flags</span>
                                <code>{derived.flags.join(', ')}</code>
                              </div>
                            ) : null}
                            <div className="graph-node__detail-section">
                              <span>SQL preview</span>
                              <code>{truncateText(normalizeSpaces(derived.body), 180)}</code>
                            </div>
                          </div>
                        ) : null}
                      </div>
                    );
                  })()
                ))}
                {compareOverlay && compareLayout
                  ? compareOverlay.removedTables.map((table) => {
                      const position = compareLayout.positions[table.alias];
                      if (!position) {
                        return null;
                      }

                      return (
                        <div
                          key={`removed-node-${table.alias}`}
                          className="graph-node graph-node--compare-removed"
                          style={{ transform: `translate(${position.x}px, ${position.y}px)` }}
                        >
                          <span className="graph-node__role">removed</span>
                          <strong>{table.name}</strong>
                          <p>alias: {table.alias}</p>
                          <div className="graph-node__meta">
                            {table.specialType ? <span className="graph-node__special-badge">{getSpecialNodeLabel(table)}</span> : null}
                          </div>
                        </div>
                      );
                    })
                  : null}
              </div>

              {primaryDiagnostic && analysis.tables.length === 0 ? (
                <div className={`graph-empty-state graph-empty-state--${primaryDiagnostic.severity}`}>
                  <strong>{primaryDiagnostic.title}</strong>
                  <p>{primaryDiagnostic.message}</p>
                  <span>
                    Line {primaryDiagnostic.line}, column {primaryDiagnostic.column}
                  </span>
                  <code>{primaryDiagnostic.excerpt}</code>
                  <small>{primaryDiagnostic.hint}</small>
                </div>
              ) : null}

              {analysis.tables.length > 0 ? (
                <div className="minimap">
                  <div className="minimap__head">
                    <span>Minimap</span>
                    <strong>{layoutModeLabel[layoutMode]}</strong>
                  </div>
                  <div
                    className="minimap__frame"
                    style={{ width: `${minimap.width}px`, height: `${minimap.height}px` }}
                    onPointerDown={handleMinimapNavigate}
                  >
                    <svg className="minimap__canvas" width={minimap.width} height={minimap.height} viewBox={`0 0 ${minimap.width} ${minimap.height}`}>
                      {compareOverlay && compareLayout
                        ? compareOverlay.removedJoins.map((join) => {
                            const source = compareAnalysis?.tables.find((table) => table.alias === join.sourceAlias);
                            const target = compareAnalysis?.tables.find((table) => table.alias === join.targetAlias);
                            if (!source || !target) {
                              return null;
                            }

                            const sourcePosition = compareLayout.positions[source.alias];
                            const targetPosition = compareLayout.positions[target.alias];
                            if (!sourcePosition || !targetPosition) {
                              return null;
                            }

                            return (
                              <line
                                key={`removed-${join.id}`}
                                x1={(sourcePosition.x + NODE_WIDTH / 2) * minimap.scale}
                                y1={(sourcePosition.y + NODE_HEIGHT / 2) * minimap.scale}
                                x2={(targetPosition.x + NODE_WIDTH / 2) * minimap.scale}
                                y2={(targetPosition.y + NODE_HEIGHT / 2) * minimap.scale}
                                className="minimap__edge minimap__edge--removed"
                              />
                            );
                          })
                        : null}
                      {analysis.joins.map((join) => {
                        const source = positionedTables.find((table) => table.alias === join.sourceAlias);
                        const target = positionedTables.find((table) => table.alias === join.targetAlias);
                        if (!source || !target) {
                          return null;
                        }

                        return (
                          <line
                            key={join.id}
                            x1={(source.x + NODE_WIDTH / 2) * minimap.scale}
                            y1={(source.y + NODE_HEIGHT / 2) * minimap.scale}
                            x2={(target.x + NODE_WIDTH / 2) * minimap.scale}
                            y2={(target.y + NODE_HEIGHT / 2) * minimap.scale}
                            className={`minimap__edge${compareOverlay?.addedJoinIds.has(join.id) ? ' minimap__edge--added' : ''}`}
                          />
                        );
                      })}
                      {compareOverlay && compareLayout
                        ? compareOverlay.removedTables.map((table) => {
                            const position = compareLayout.positions[table.alias];
                            if (!position) {
                              return null;
                            }

                            return (
                              <rect
                                key={`removed-${table.alias}`}
                                x={position.x * minimap.scale}
                                y={position.y * minimap.scale}
                                width={Math.max(12, NODE_WIDTH * minimap.scale)}
                                height={Math.max(8, NODE_HEIGHT * minimap.scale)}
                                rx={3}
                                className="minimap__node minimap__node--removed"
                              />
                            );
                          })
                        : null}
                      {positionedTables.map((table) => {
                        const planSignal = getStrongestExplainSignal(explainSummary.relationSignals[table.alias]);

                        return (
                          <rect
                            key={table.alias}
                            x={table.x * minimap.scale}
                            y={table.y * minimap.scale}
                            width={Math.max(12, NODE_WIDTH * minimap.scale)}
                            height={Math.max(8, NODE_HEIGHT * minimap.scale)}
                            rx={3}
                            className={`minimap__node${
                              selectedAlias === table.alias ? ' minimap__node--selected' : ''
                            }${
                              planSignal ? ` minimap__node--plan-${planSignal.severity}` : ''
                            }${
                              compareOverlay?.addedAliases.has(table.alias) ? ' minimap__node--added' : ''
                            }`}
                          />
                        );
                      })}
                      <rect
                        x={minimap.viewportX}
                        y={minimap.viewportY}
                        width={Math.max(18, minimap.viewportWidth)}
                        height={Math.max(14, minimap.viewportHeight)}
                        rx={4}
                        className="minimap__viewport"
                      />
                    </svg>
                  </div>
                </div>
              ) : null}

              <div className="graph-hint">Click a node, edge, or lineage item to inspect it. Drag empty space to pan. Scroll to zoom. Paste text or JSON EXPLAIN to map runtime signals. Press ? for shortcuts.</div>
            </div>
          </div>

          <aside className="detail-rail">
            <article className="rail-card inspector-panel">
              <div className="panel-head">
                <span>Inspector</span>
                <strong>
                  {selectedJoinInsight
                    ? selectedJoinInsight.badgeLabel
                    : selectedTable
                      ? selectedTable.alias
                      : selectedLineage
                        ? selectedLineage.label
                      : 'Select entity'}
                </strong>
              </div>
              <div className="bottom-card__content">
                {selectedJoin && selectedJoinInsight ? (
                  <div className="inspector-content">
                    <div className="inspector-route">
                      <strong>{selectedJoin.sourceAlias}</strong>
                      <span>{selectedJoinInsight.joinLabel}</span>
                      <strong>{selectedJoin.targetAlias}</strong>
                    </div>
                    <p className="inspector-summary">{selectedJoinInsight.summary}</p>
                    <div className="inspector-badges">
                      <span className={`inspector-badge inspector-badge--${selectedJoinInsight.tone}`}>{selectedJoinInsight.badgeLabel}</span>
                      <span className={`inspector-badge inspector-badge--${selectedJoinInsight.confidence === 'verified' ? 'good' : 'neutral'}`}>
                        {selectedJoinInsight.confidenceLabel}
                      </span>
                    </div>
                    <p className="inspector-note inspector-note--inline">
                      {selectedJoinInsight.confidence === 'verified'
                        ? 'Cardinality and index guidance are backed by imported schema metadata for PK, FK, or key coverage.'
                        : 'Cardinality is inferred from naming patterns like '}
                      {selectedJoinInsight.confidence === 'verified' ? null : (
                        <>
                          <code>id</code> and <code>_id</code>. Treat it as a review hint, not schema truth.
                        </>
                      )}
                    </p>

                    <section className="inspector-section">
                      <span className="canvas-toolbar__label">ON condition</span>
                      <code className="inspector-code">{selectedJoinInsight.fullConditionLabel}</code>
                    </section>

                    <section className="inspector-section">
                      <span className="canvas-toolbar__label">Key pairs</span>
                      {selectedJoinInsight.pairs.length > 0 ? (
                        <ul className="inspector-list">
                          {selectedJoinInsight.pairs.map((pair) => (
                            <li key={`${pair.sourceAlias}.${pair.sourceColumn}-${pair.targetAlias}.${pair.targetColumn}`}>
                              {pair.sourceAlias}.{pair.sourceColumn} = {pair.targetAlias}.{pair.targetColumn}
                            </li>
                          ))}
                        </ul>
                      ) : (
                        <p className="inspector-empty">No simple equality pairs found in this ON condition.</p>
                      )}
                    </section>

                    <section className="inspector-section">
                      <span className="canvas-toolbar__label">Index hints</span>
                      <ul className="inspector-list">
                        {selectedJoinInsight.indexHints.map((hint) => (
                          <li key={hint}>{hint}</li>
                        ))}
                      </ul>
                    </section>

                    <section className="inspector-section">
                      <span className="canvas-toolbar__label">Fanout risk</span>
                      <p className="inspector-summary">{selectedJoinInsight.fanoutSummary}</p>
                    </section>

                    <section className="inspector-section">
                      <span className="canvas-toolbar__label">Plan signals</span>
                      {selectedJoinSignals.length > 0 ? (
                        <div className="plan-signal-list">
                          {selectedJoinSignals.slice(0, 4).map((signal) => (
                            <article key={signal.id} className={`plan-signal plan-signal--${signal.severity}`}>
                              <strong>{signal.title}</strong>
                              {formatPlanMetrics(signal, 'full') ? <small>{formatPlanMetrics(signal, 'full')}</small> : null}
                              {getEstimateBadgeLabel(signal.estimateFactor) ? (
                                <span className={`plan-signal__estimate plan-signal__estimate--${getEstimateSeverity(signal.estimateFactor)}`}>
                                  {getEstimateBadgeLabel(signal.estimateFactor)}
                                </span>
                              ) : null}
                              <span>{signal.detail}</span>
                            </article>
                          ))}
                        </div>
                      ) : (
                        <p className="inspector-empty">
                          {explainInput.trim()
                            ? 'No relation-specific plan signals were mapped to this join yet.'
                            : 'Paste EXPLAIN output on the left to correlate scans and join nodes.'}
                        </p>
                      )}
                      {selectedJoinSignals.length > 0 && selectedJoinSignals.some((signal) => getEstimateSeverity(signal.estimateFactor) && getEstimateSeverity(signal.estimateFactor) !== 'low') ? (
                        <small className="inspector-note">Estimate badges highlight where the planner expected far fewer or far more rows than actually showed up.</small>
                      ) : null}
                    </section>

                    <section className="inspector-section">
                      <span className="canvas-toolbar__label">Notes</span>
                      <textarea
                        className="note-input"
                        value={entityNotes[getJoinNoteKey(selectedJoin.id)] ?? ''}
                        onChange={(event) => handleEntityNoteChange(getJoinNoteKey(selectedJoin.id), event.target.value)}
                        spellCheck={false}
                        placeholder="Add a note about this join, fanout risk, or follow-up."
                      />
                      <small className="inspector-note">Notes are saved locally per query workspace.</small>
                    </section>
                  </div>
                ) : selectedTable ? (
                  <div className="inspector-content">
                    <div className="inspector-route">
                      <strong>{selectedTable.alias}</strong>
                      <span>
                        {selectedTable.role === 'source'
                          ? 'Source node'
                          : selectedTable.role === 'target'
                            ? 'Write target'
                            : 'Join node'}
                      </span>
                      <strong>{selectedTable.name}</strong>
                    </div>
                    <p className="inspector-summary">
                      {selectedDerivedRelation
                        ? `${selectedDerivedRelation.kind.toUpperCase()} relation with ${formatPlural(selectedDerivedRelation.joinCount, 'join')}, ${formatPlural(selectedDerivedRelation.sourceCount, 'source')} and ${selectedDerivedRelation.hasAggregation ? 'aggregation' : 'no aggregation'}.`
                        : selectedTable.role === 'target'
                          ? 'Write target for the current DML statement. Inspect incoming joins and filters before running it.'
                          : 'Base relation in the current statement graph.'}
                    </p>
                    <div className="inspector-badges">
                      <span className="inspector-badge inspector-badge--neutral">{selectedTable.role.toUpperCase()}</span>
                      {selectedDerivedRelation ? (
                        <span className="inspector-badge inspector-badge--neutral">{selectedDerivedRelation.kind.toUpperCase()}</span>
                      ) : null}
                      {selectedTable.specialType ? (
                        <span className="inspector-badge inspector-badge--neutral">{getSpecialNodeLabel(selectedTable)}</span>
                      ) : null}
                    </div>

                    {selectedDerivedRelation ? (
                      <section className="inspector-section">
                        <span className="canvas-toolbar__label">Derived relation</span>
                        <ul className="inspector-list">
                          <li>Dependencies: {selectedDerivedRelation.dependencies.length > 0 ? selectedDerivedRelation.dependencies.join(', ') : 'None detected'}</li>
                          <li>Subqueries: {selectedDerivedRelation.subqueryCount}</li>
                          <li>Aggregation: {selectedDerivedRelation.hasAggregation ? 'Yes' : 'No'}</li>
                        </ul>
                      </section>
                    ) : null}

                    {selectedTable.specialType ? (
                      <section className="inspector-section">
                        <span className="canvas-toolbar__label">Relation type</span>
                        <p className="inspector-summary">
                          {selectedTable.specialType === 'unnest'
                            ? 'This node expands nested array data. Watch fanout and downstream aggregate accuracy.'
                            : selectedTable.specialType === 'flatten'
                              ? 'This node flattens semi-structured values. Filter early to avoid row explosion.'
                              : selectedTable.specialType === 'external'
                                ? 'This node reads from external files or object storage. Predicate pushdown matters more here.'
                                : selectedTable.specialType === 'temp'
                                  ? 'This node points to a temp table or table variable. Double-check row counts and missing stats.'
                                  : 'This node comes from a table-valued function or relation function.'}
                        </p>
                      </section>
                    ) : null}

                    <section className="inspector-section">
                      <span className="canvas-toolbar__label">Plan signals</span>
                      {selectedTableSignals.length > 0 ? (
                        <div className="plan-signal-list">
                          {selectedTableSignals.slice(0, 4).map((signal) => (
                            <article key={signal.id} className={`plan-signal plan-signal--${signal.severity}`}>
                              <strong>{signal.title}</strong>
                              {formatPlanMetrics(signal, 'full') ? <small>{formatPlanMetrics(signal, 'full')}</small> : null}
                              {getEstimateBadgeLabel(signal.estimateFactor) ? (
                                <span className={`plan-signal__estimate plan-signal__estimate--${getEstimateSeverity(signal.estimateFactor)}`}>
                                  {getEstimateBadgeLabel(signal.estimateFactor)}
                                </span>
                              ) : null}
                              <span>{signal.detail}</span>
                            </article>
                          ))}
                        </div>
                      ) : (
                        <p className="inspector-empty">
                          {explainInput.trim()
                            ? 'No plan signals were mapped to this node yet.'
                            : 'Paste EXPLAIN output on the left to correlate scans and costs with this node.'}
                        </p>
                      )}
                      {selectedTableSignals.length > 0 && selectedTableSignals.some((signal) => getEstimateSeverity(signal.estimateFactor) && getEstimateSeverity(signal.estimateFactor) !== 'low') ? (
                        <small className="inspector-note">Large estimate gaps usually mean stale stats, skew, or fanout the planner did not predict well.</small>
                      ) : null}
                    </section>

                    {fanoutState.aliasImpacts[selectedTable.alias] ? (
                      <section className="inspector-section">
                        <span className="canvas-toolbar__label">Fanout propagation</span>
                        <p className="inspector-summary">{fanoutState.aliasImpacts[selectedTable.alias].reason}</p>
                      </section>
                    ) : null}

                    <section className="inspector-section">
                      <span className="canvas-toolbar__label">Notes</span>
                      <textarea
                        className="note-input"
                        value={entityNotes[getNodeNoteKey(selectedTable.alias)] ?? ''}
                        onChange={(event) => handleEntityNoteChange(getNodeNoteKey(selectedTable.alias), event.target.value)}
                        spellCheck={false}
                        placeholder="Add a note about this table, CTE, or subquery."
                      />
                      <small className="inspector-note">Notes are saved locally per query workspace.</small>
                    </section>
                  </div>
                ) : selectedLineage ? (
                  <div className="inspector-content">
                    <div className="inspector-route">
                      <strong>{selectedLineage.label}</strong>
                      <span>Column lineage</span>
                      <strong>{selectedLineage.alias ?? 'expression'}</strong>
                    </div>
                    <p className="inspector-summary">{selectedLineage.expression}</p>

                    <section className="inspector-section">
                      <span className="canvas-toolbar__label">Source references</span>
                      {selectedLineage.references.length > 0 ? (
                        <ul className="inspector-list">
                          {selectedLineage.references.map((reference) => (
                            <li key={`${reference.alias}.${reference.column}`}>{reference.alias}.{reference.column}</li>
                          ))}
                        </ul>
                      ) : (
                        <p className="inspector-empty">No explicit alias.column references were detected in this expression.</p>
                      )}
                    </section>

                    <section className="inspector-section">
                      <span className="canvas-toolbar__label">Lineage path</span>
                      <p className="inspector-summary">
                        {selectedLineage.relatedAliases.length > 0
                          ? selectedLineage.relatedAliases.join(' -> ')
                          : 'This expression does not map cleanly to a join path yet.'}
                      </p>
                    </section>

                    {selectedLineage.functionNames.length > 0 ? (
                      <section className="inspector-section">
                        <span className="canvas-toolbar__label">Functions</span>
                        <ul className="inspector-list">
                          {selectedLineage.functionNames.map((name) => (
                            <li key={name}>{name}</li>
                          ))}
                        </ul>
                      </section>
                    ) : null}

                    <section className="inspector-section">
                      <span className="canvas-toolbar__label">Plan signals</span>
                      {selectedLineageSignals.length > 0 ? (
                        <div className="plan-signal-list">
                          {selectedLineageSignals.slice(0, 4).map((signal) => (
                            <article key={signal.id} className={`plan-signal plan-signal--${signal.severity}`}>
                              <strong>{signal.title}</strong>
                              {formatPlanMetrics(signal, 'full') ? <small>{formatPlanMetrics(signal, 'full')}</small> : null}
                              {getEstimateBadgeLabel(signal.estimateFactor) ? (
                                <span className={`plan-signal__estimate plan-signal__estimate--${getEstimateSeverity(signal.estimateFactor)}`}>
                                  {getEstimateBadgeLabel(signal.estimateFactor)}
                                </span>
                              ) : null}
                              <span>{signal.detail}</span>
                            </article>
                          ))}
                        </div>
                      ) : (
                        <p className="inspector-empty">
                          {explainInput.trim()
                            ? 'No plan signals were mapped to this lineage path yet.'
                            : 'Paste EXPLAIN output on the left to correlate this lineage path with runtime signals.'}
                        </p>
                      )}
                    </section>
                  </div>
                ) : (
                  <p className="inspector-empty">
                    Select a node, edge, or column lineage item to inspect cardinality, mapped EXPLAIN signals, fanout, and local notes.
                  </p>
                )}
              </div>
            </article>

            <article className="bottom-card rail-card">
              <div className="panel-head">
                <span>Details</span>
                <strong>
                  {detailTab === 'joins'
                    ? analysis.joins.length
                    : detailTab === 'lineage'
                      ? lineageColumns.length
                      : analysis.clauses.filter((clause) => clause.present).length}
                </strong>
              </div>
              <div className="rail-tabs" role="tablist" aria-label="Detail tabs">
                <button
                  type="button"
                  className={detailTab === 'joins' ? 'is-active' : ''}
                  onClick={() => setDetailTab('joins')}
                >
                  Joins
                </button>
                <button
                  type="button"
                  className={detailTab === 'clauses' ? 'is-active' : ''}
                  onClick={() => setDetailTab('clauses')}
                >
                  Clauses
                </button>
                <button
                  type="button"
                  className={detailTab === 'lineage' ? 'is-active' : ''}
                  onClick={() => setDetailTab('lineage')}
                >
                  Lineage
                </button>
              </div>
              <div className={`bottom-card__content ${detailTab === 'joins' || detailTab === 'lineage' ? 'mono-list' : 'clause-list compact'}`}>
                {detailTab === 'joins' ? (
                  analysis.joins.length > 0 ? (
                    <>
                      <p className="rail-note">Cardinality badges use `id` and `_id` naming heuristics. They are helpful, not guaranteed.</p>
                      {analysis.joins.map((join) => {
                        const insight = joinInsights[join.id];
                        const isSearchMatched = searchState.matchedJoinIds.has(join.id);
                        const joinNote = entityNotes[getJoinNoteKey(join.id)]?.trim() ?? '';
                        const fanoutImpact = fanoutState.joinImpacts[join.id] ?? null;
                        const isDimmed =
                          (focusState.active && !focusState.relatedJoinIds.has(join.id)) ||
                          (searchState.active &&
                            !isSearchMatched &&
                            !searchState.matchedAliases.has(join.sourceAlias) &&
                            !searchState.matchedAliases.has(join.targetAlias));

                        return (
                          <button
                            key={join.id}
                            type="button"
                            className={`join-list-item${
                              selectedJoinId === join.id ? ' join-list-item--selected' : ''
                            }${
                              focusState.active && focusState.relatedJoinIds.has(join.id) ? ' join-list-item--related' : ''
                            }${
                              isSearchMatched ? ' join-list-item--matched' : ''
                            }${
                              isDimmed ? ' join-list-item--dimmed' : ''
                            }`}
                            onClick={() => handleSelectJoin(join.id)}
                          >
                            <strong>{insight?.joinLabel ?? formatJoinTypeLabel(join.type)}</strong>
                            <span>{join.sourceAlias} {'->'} {join.targetAlias}</span>
                            <code>{insight?.compactCondition ?? join.condition}</code>
                            {fanoutImpact ? <small className="join-list-item__note">Fanout: {insight?.fanoutSummary}</small> : null}
                            {joinNote ? <small className="join-list-item__note">Note: {truncateText(normalizeSpaces(joinNote), 96)}</small> : null}
                          </button>
                        );
                      })}
                    </>
                  ) : (
                    <div>No joins detected.</div>
                  )
                ) : detailTab === 'lineage' ? (
                  lineageColumns.length > 0 ? (
                    <>
                      <p className="rail-note">Select a projected column to highlight the aliases and joins it depends on.</p>
                      {lineageColumns.map((lineage) => (
                        <button
                          key={lineage.id}
                          type="button"
                          className={`join-list-item${selectedLineageId === lineage.id ? ' join-list-item--selected' : ''}`}
                          onClick={() => handleSelectLineage(lineage.id)}
                        >
                          <strong>{lineage.label}</strong>
                          <span>{lineage.references.length > 0 ? lineage.references.map((reference) => `${reference.alias}.${reference.column}`).join(', ') : 'Expression only'}</span>
                          <code>{truncateText(normalizeSpaces(lineage.expression), 120)}</code>
                        </button>
                      ))}
                    </>
                  ) : (
                    <div>No projected columns detected.</div>
                  )
                ) : (
                  analysis.clauses.map((clause) => (
                    <div key={clause.label} className={`clause-chip ${clause.present ? 'is-on' : 'is-off'}`}>
                      <strong>{clause.label}</strong>
                      <span>{clause.detail}</span>
                    </div>
                  ))
                )}
              </div>
            </article>
          </aside>
        </section>
      </main>

      {showShortcutHelp ? (
        <div className="shortcut-overlay" role="dialog" aria-modal="true" aria-label="Keyboard shortcuts">
          <div className="shortcut-card">
            <div className="panel-head">
              <span>Shortcuts</span>
              <button type="button" onClick={() => setShowShortcutHelp(false)}>Close</button>
            </div>
            <div className="shortcut-list">
              <div className="shortcut-item"><kbd>Cmd/Ctrl + S</kbd><span>Save query locally</span></div>
              <div className="shortcut-item"><kbd>Cmd/Ctrl + Shift + C</kbd><span>Copy share link</span></div>
              <div className="shortcut-item"><kbd>F</kbd><span>Reset graph view</span></div>
              <div className="shortcut-item"><kbd>Esc</kbd><span>Clear current focus or close this dialog</span></div>
              <div className="shortcut-item"><kbd>?</kbd><span>Toggle shortcut help</span></div>
            </div>
          </div>
        </div>
      ) : null}
    </div>
  );
}

export default App;
