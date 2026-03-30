import {
  useEffect,
  useDeferredValue,
  useEffectEvent,
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
  diagnoseSqlInput,
  extractStatements,
  type JoinRef,
  type Severity,
  type SqlDiagnostic,
  type TableRef,
} from './lib/analyzeSql';

const SAMPLE_SQL = `SELECT
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

const NODE_WIDTH = 188;
const NODE_HEIGHT = 92;

interface GraphLayout {
  width: number;
  height: number;
  positions: Record<string, { x: number; y: number }>;
}

type LayoutMode = 'horizontal' | 'vertical' | 'radial';
type NodeSide = 'left' | 'right' | 'top' | 'bottom';

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
  labelWidth: number;
  joinLabel: string;
  compactCondition: string;
  fullConditionLabel: string;
  summary: string;
  pairs: JoinConditionPair[];
  indexHints: string[];
}

interface SavedQuery {
  id: string;
  title: string;
  sql: string;
  updatedAt: number;
  selectedStatementIndex: number;
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
  };
}

type DetailTab = 'joins' | 'clauses';

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

const EDGE_CONDITION_MAX = 30;
const SAVED_QUERIES_KEY = 'queryviz.savedQueries.v1';
const MAX_SAVED_QUERIES = 10;
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
};

const createDotFileName = (statementIndex: number) => `queryviz-statement-${statementIndex + 1}.dot`;
const createPngFileName = (statementIndex: number) => `queryviz-statement-${statementIndex + 1}.png`;
const createSvgFileName = (statementIndex: number) => `queryviz-statement-${statementIndex + 1}.svg`;

const summarizeStatement = (statement: string, index: number) => {
  const normalized = statement.replace(/\s+/g, ' ').trim();
  return `#${index + 1} ${normalized.slice(0, 78)}${normalized.length > 78 ? '...' : ''}`;
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

const formatJoinTypeLabel = (joinType: string) => {
  const normalized = normalizeSpaces(joinType);
  if (!normalized) {
    return 'JOIN';
  }

  return /join$/i.test(normalized) ? normalized.toUpperCase() : `${normalized.toUpperCase()} JOIN`;
};

const cleanColumnName = (value: string) => value.replace(/[`"'[\]]/g, '').trim();

const inferColumnRole = (columnName: string) => {
  const normalized = cleanColumnName(columnName).toLowerCase();

  if (normalized === 'id') {
    return 'pk';
  }

  if (normalized.endsWith('_id')) {
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

const buildIndexHints = (pairs: JoinConditionPair[], cardinality: JoinCardinality) => {
  if (pairs.length === 0) {
    return ['Consider indexes on the columns used in the ON condition.'];
  }

  const hints = new Set<string>();

  pairs.forEach((pair) => {
    if (cardinality === 'N:1') {
      hints.add(`Index likely helpful on ${pair.sourceAlias}(${pair.sourceColumn}).`);
      return;
    }

    if (cardinality === '1:N') {
      hints.add(`Index likely helpful on ${pair.targetAlias}(${pair.targetColumn}).`);
      return;
    }

    hints.add(`Review indexes on ${pair.sourceAlias}(${pair.sourceColumn}) and ${pair.targetAlias}(${pair.targetColumn}).`);
  });

  return Array.from(hints);
};

const classifyColumnPair = (sourceColumn: string, targetColumn: string): JoinCardinality => {
  const sourceRole = inferColumnRole(sourceColumn);
  const targetRole = inferColumnRole(targetColumn);

  if (sourceRole === 'fk' && targetRole === 'pk') {
    return 'N:1';
  }

  if (sourceRole === 'pk' && targetRole === 'fk') {
    return '1:N';
  }

  return 'M:N';
};

const inferJoinInsight = (join: JoinRef): JoinInsight => {
  const pairs = extractJoinConditionPairs(join);
  const pairClassifications = pairs.map((pair) => classifyColumnPair(pair.sourceColumn, pair.targetColumn));

  const cardinality =
    pairClassifications.length > 0 && pairClassifications.every((value) => value === pairClassifications[0])
      ? pairClassifications[0]
      : 'M:N';
  const joinLabel = formatJoinTypeLabel(join.type);
  const compactCondition = truncateText(normalizeSpaces(`ON ${join.condition}`), EDGE_CONDITION_MAX);
  const fullConditionLabel = normalizeSpaces(`ON ${join.condition}`);
  const labelWidth = Math.max(110, Math.min(268, Math.max(joinLabel.length * 8.2, compactCondition.length * 6.4) + 24));
  const indexHints = buildIndexHints(pairs, cardinality);

  if (cardinality === 'N:1') {
    return {
      cardinality,
      tone: 'good',
      badgeLabel: cardinality,
      badgeHint: 'Heuristic: source looks like FK -> target PK. Preferred join shape.',
      labelWidth,
      joinLabel,
      compactCondition,
      fullConditionLabel,
      summary: 'FK -> PK join. This is usually the healthiest join direction and tends to preserve row counts.',
      pairs,
      indexHints,
    };
  }

  if (cardinality === '1:N') {
    return {
      cardinality,
      tone: 'caution',
      badgeLabel: cardinality,
      badgeHint: 'Heuristic: source looks like PK -> target FK. This can multiply rows downstream.',
      labelWidth,
      joinLabel,
      compactCondition,
      fullConditionLabel,
      summary: 'PK -> FK join. This can multiply rows downstream, especially before aggregates.',
      pairs,
      indexHints,
    };
  }

  return {
    cardinality,
    tone: 'review',
    badgeLabel: cardinality,
    badgeHint: 'Heuristic: non-key columns or mixed predicates. Worth reviewing for fanout risk.',
    labelWidth,
    joinLabel,
    compactCondition,
    fullConditionLabel,
    summary: 'Non-key or mixed join predicates. Treat the fanout as ambiguous until you inspect it.',
    pairs,
    indexHints,
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

    return parsed.filter((item): item is SavedQuery =>
      typeof item?.id === 'string' &&
      typeof item?.title === 'string' &&
      typeof item?.sql === 'string' &&
      typeof item?.updatedAt === 'number' &&
      typeof item?.selectedStatementIndex === 'number',
    );
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

const createShareUrl = (sql: string, statementIndex: number) => {
  if (typeof window === 'undefined') {
    return '';
  }

  const params = new URLSearchParams();
  params.set('sql', encodeBase64Url(sql));
  params.set('statement', String(statementIndex));

  return `${window.location.origin}${window.location.pathname}#${params.toString()}`;
};

const readSharedWorkspace = () => {
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

    return { sql, selectedStatementIndex };
  } catch {
    return null;
  }
};

const getTableLookupKeys = (table: TableRef) => [
  table.alias.toLowerCase(),
  table.name.toLowerCase(),
  table.name.split('.').pop()?.toLowerCase() ?? table.name.toLowerCase(),
];

const parseExplainInput = (input: string, tables: TableRef[], joins: JoinRef[]): ExplainSummary => {
  const trimmed = input.trim();
  if (!trimmed) {
    return {
      items: [],
      relationSignals: {},
      joinSignals: {},
      summary: {
        seqScans: 0,
        indexedReads: 0,
        joinNodes: 0,
        sorts: 0,
        mappedJoins: 0,
      },
    };
  }

  const relationSignals: Record<string, ExplainSignal[]> = {};
  const joinSignals: Record<string, ExplainSignal[]> = {};
  const items: ExplainSignal[] = [];
  let seqScans = 0;
  let indexedReads = 0;
  let joinNodes = 0;
  let sorts = 0;

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

  const getAliasesForRelation = (relationName: string) => {
    const normalized = cleanColumnName(relationName).toLowerCase();
    const shortName = normalized.split('.').pop() ?? normalized;
    const aliases = new Set<string>();

    (tableIndex.get(normalized) ?? new Set()).forEach((alias) => aliases.add(alias));
    (tableIndex.get(shortName) ?? new Set()).forEach((alias) => aliases.add(alias));

    return Array.from(aliases);
  };

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
      indent: rawLine.match(/^\s*/)?.[0].length ?? 0,
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
    const seqMatch = entry.line.match(/\bSeq Scan on ([a-zA-Z0-9_."`[\]]+)/i);
    const indexMatch = entry.line.match(/\b(Index Scan|Index Only Scan|Bitmap Heap Scan|Bitmap Index Scan) (?:using [^\s]+ )?on ([a-zA-Z0-9_."`[\]]+)/i);
    const nestedLoopMatch = entry.line.match(/\b(Nested Loop|Hash Join|Merge Join|Hash Left Join|Hash Right Join|Hash Full Join)\b/i);
    const sortMatch = entry.line.match(/\bSort\b/i);
    const aggregateMatch = entry.line.match(/\b(Aggregate|GroupAggregate|HashAggregate)\b/i);
    const filterMatch = entry.line.match(/\bRows Removed by Filter:\s*([\d,]+)/i);
    let signal: ExplainSignal | null = null;
    let kind: 'scan' | 'join' | 'operation' | 'other' = 'other';
    let relationAliases: string[] = [];

    if (seqMatch) {
      seqScans += 1;
      relationAliases = getAliasesForRelation(seqMatch[1]);
      signal = {
        id: `seq-${entry.index}`,
        severity: 'high',
        title: 'Seq Scan',
        detail: entry.line,
        relationName: seqMatch[1],
        relationAliases,
      };
      kind = 'scan';
    } else if (indexMatch) {
      indexedReads += 1;
      relationAliases = getAliasesForRelation(indexMatch[2]);
      signal = {
        id: `index-${entry.index}`,
        severity: 'low',
        title: indexMatch[1],
        detail: entry.line,
        relationName: indexMatch[2],
        relationAliases,
      };
      kind = 'scan';
    } else if (nestedLoopMatch) {
      joinNodes += 1;
      signal = {
        id: `join-${entry.index}`,
        severity: nestedLoopMatch[1].toLowerCase() === 'nested loop' ? 'high' : 'medium',
        title: nestedLoopMatch[1],
        detail: entry.line,
      };
      kind = 'join';
    } else if (sortMatch) {
      sorts += 1;
      signal = {
        id: `sort-${entry.index}`,
        severity: 'medium',
        title: 'Sort',
        detail: entry.line,
      };
      kind = 'operation';
    } else if (aggregateMatch) {
      signal = {
        id: `agg-${entry.index}`,
        severity: 'low',
        title: aggregateMatch[1],
        detail: entry.line,
      };
      kind = 'operation';
    } else if (filterMatch) {
      signal = {
        id: `filter-${entry.index}`,
        severity: 'medium',
        title: 'Rows Removed by Filter',
        detail: entry.line,
      };
      kind = 'operation';
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

  return [...signals].sort((left, right) => severityRank[right.severity] - severityRank[left.severity])[0];
};

function App() {
  const sharedWorkspace = readSharedWorkspace();
  const [sql, setSql] = useState(sharedWorkspace?.sql ?? SAMPLE_SQL);
  const [selectedStatementIndex, setSelectedStatementIndex] = useState(sharedWorkspace?.selectedStatementIndex ?? 0);
  const [savedQueries, setSavedQueries] = useState<SavedQuery[]>(() => readSavedQueries());
  const [layoutMode, setLayoutMode] = useState<LayoutMode>('horizontal');
  const [searchQuery, setSearchQuery] = useState('');
  const [explainInput, setExplainInput] = useState('');
  const [zoom, setZoom] = useState(1);
  const [pan, setPan] = useState({ x: 0, y: 0 });
  const [shellSize, setShellSize] = useState({ width: 0, height: 0 });
  const [draggingNode, setDraggingNode] = useState<string | null>(null);
  const [selectedAlias, setSelectedAlias] = useState<string | null>(null);
  const [selectedJoinId, setSelectedJoinId] = useState<string | null>(null);
  const [detailTab, setDetailTab] = useState<DetailTab>('joins');
  const [nodeOffsets, setNodeOffsets] = useState<Record<string, { x: number; y: number }>>({});
  const [lastPoint, setLastPoint] = useState<{ x: number; y: number } | null>(null);
  const [isPanning, setIsPanning] = useState(false);
  const [statusMessage, setStatusMessage] = useState<string | null>(null);
  const fileInputRef = useRef<HTMLInputElement | null>(null);
  const graphShellRef = useRef<HTMLDivElement | null>(null);
  const graphViewportRef = useRef<HTMLDivElement | null>(null);
  const deferredSearchQuery = useDeferredValue(searchQuery);

  const statements = useMemo(() => extractStatements(sql), [sql]);
  const diagnostics = useMemo<DiagnosticSummary>(() => {
    const items = diagnoseSqlInput(sql);
    const blocking = items.find((item) => item.severity === 'error') ?? null;
    return { blocking, items };
  }, [sql]);
  const primaryDiagnostic = diagnostics.blocking ?? diagnostics.items[0] ?? null;
  const safeSelectedStatementIndex = statements.length === 0
    ? 0
    : Math.min(selectedStatementIndex, statements.length - 1);

  const analysis = useMemo(() => analyzeSql(sql, safeSelectedStatementIndex), [sql, safeSelectedStatementIndex]);
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
  const joinInsights = useMemo(
    () =>
      Object.fromEntries(
        analysis.joins.map((join) => [join.id, inferJoinInsight(join)]),
      ) as Record<string, JoinInsight>,
    [analysis.joins],
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
  }, [analysis.joins, selectedAlias, selectedJoin]);
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
  const minimap = useMemo(() => {
    const scale = Math.min(196 / layout.width, 140 / layout.height, 1);
    const width = layout.width * scale;
    const height = layout.height * scale;
    const viewportWidth = shellSize.width > 0 ? Math.min(layout.width, shellSize.width / zoom) : layout.width;
    const viewportHeight = shellSize.height > 0 ? Math.min(layout.height, shellSize.height / zoom) : layout.height;
    const viewportX = clamp(-pan.x / zoom, 0, Math.max(0, layout.width - viewportWidth));
    const viewportY = clamp(-pan.y / zoom, 0, Math.max(0, layout.height - viewportHeight));

    return {
      scale,
      width,
      height,
      viewportWidth: viewportWidth * scale,
      viewportHeight: viewportHeight * scale,
      viewportX: viewportX * scale,
      viewportY: viewportY * scale,
    };
  }, [layout.height, layout.width, pan.x, pan.y, shellSize.height, shellSize.width, zoom]);

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

  const loadWorkspace = (nextSql: string, nextStatementIndex = 0) => {
    setSql(nextSql);
    setSelectedStatementIndex(nextStatementIndex);
    setSelectedAlias(null);
    setSelectedJoinId(null);
    setDetailTab('joins');
    setExplainInput('');
    setNodeOffsets({});
    setPan({ x: 0, y: 0 });
    setZoom(1);
  };

  const beginCanvasPan = (event: ReactPointerEvent<HTMLDivElement>) => {
    if ((event.target as HTMLElement).closest('.graph-node')) {
      return;
    }

    setSelectedAlias(null);
    setSelectedJoinId(null);
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
    setSql(event.target.value);
    setSelectedAlias(null);
    setSelectedJoinId(null);
    setNodeOffsets({});
    setPan({ x: 0, y: 0 });
    setZoom(1);
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
        width: layout.width,
        height: layout.height,
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
        width: layout.width,
        height: layout.height,
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
    };

    setSavedQueries((current) => {
      const existing = current.find((item) => item.sql === sql);
      const nextList = existing
        ? current.map((item) =>
            item.id === existing.id
              ? { ...item, title: nextEntry.title, updatedAt: nextEntry.updatedAt, selectedStatementIndex: nextEntry.selectedStatementIndex }
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
    loadWorkspace(query.sql, query.selectedStatementIndex);
    setStatusMessage(`Loaded "${query.title}".`);
  };

  const handleSelectJoin = (joinId: string) => {
    setSelectedJoinId(joinId);
    setSelectedAlias(null);
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
      setDetailTab('joins');
    }
  };

  const handleDeleteSavedQuery = (queryId: string) => {
    setSavedQueries((current) => current.filter((item) => item.id !== queryId));
    setStatusMessage('Removed from local history.');
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

  const handleCopyShareLink = async () => {
    if (!analysis.normalizedSql.trim()) {
      return;
    }

    const shareUrl = createShareUrl(sql, safeSelectedStatementIndex);

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
      setSelectedAlias(null);
      setSelectedJoinId(null);
      setSearchQuery('');
    }

    if (event.key.toLowerCase() === 'f') {
      event.preventDefault();
      resetView();
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
            <strong>{sql.length} chars</strong>
          </div>
          <textarea
            className="sql-input"
            value={sql}
            onChange={handleSqlChange}
            spellCheck={false}
            placeholder="Paste a SELECT query here..."
          />
          <div className="editor-actions">
            <button type="button" onClick={() => loadWorkspace(SAMPLE_SQL)}>
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
          </div>
          <input ref={fileInputRef} className="hidden-file-input" type="file" accept=".sql,.txt" onChange={handleFileChange} />
          {statements.length > 1 ? (
            <p className="editor-note">Detected {statements.length} SQL statements. Pick one below to graph it.</p>
          ) : null}
          {statusMessage ? <p className="editor-note editor-note--status">{statusMessage}</p> : null}
        </div>

        {diagnostics.items.length > 0 ? (
          <section className="statement-panel">
            <div className="panel-head">
              <span>Diagnostics</span>
              <strong>{diagnostics.items.length}</strong>
            </div>
            <div className="diagnostic-list">
              {diagnostics.items.map((diagnostic) => (
                <article key={`${diagnostic.title}-${diagnostic.index}`} className={`diagnostic-card diagnostic-card--${diagnostic.severity}`}>
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
            placeholder="Paste EXPLAIN / EXPLAIN ANALYZE output here to overlay scans and join signals."
          />
          {explainSummary.items.length > 0 ? (
            <>
              <div className="plan-summary">
                <span className="plan-summary__chip">Seq scans: {explainSummary.summary.seqScans}</span>
                <span className="plan-summary__chip">Indexed reads: {explainSummary.summary.indexedReads}</span>
                <span className="plan-summary__chip">Join nodes: {explainSummary.summary.joinNodes}</span>
                <span className="plan-summary__chip">Sorts: {explainSummary.summary.sorts}</span>
                <span className="plan-summary__chip">Mapped joins: {explainSummary.summary.mappedJoins}</span>
              </div>
              <div className="plan-signal-list">
                {explainSummary.items.slice(0, 6).map((signal) => (
                  <article key={signal.id} className={`plan-signal plan-signal--${signal.severity}`}>
                    <strong>{signal.title}</strong>
                    <span>{signal.detail}</span>
                  </article>
                ))}
              </div>
            </>
          ) : (
            <p className="editor-note">Paste a text plan and Queryviz will flag seq scans, joins, sorts, and indexed reads on the graph.</p>
          )}
        </section>

        <div className="sidebar-grid">
          <section className="info-card">
            <div className="panel-head">
              <span>Metrics</span>
              <strong>{analysis.complexityScore}</strong>
            </div>
            <ul>
              <li>Tables: {analysis.tables.length}</li>
              <li>Joins: {analysis.joins.length}</li>
              <li>Filters: {analysis.filters.length}</li>
              <li>Columns: {analysis.columns.length}</li>
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
                  <span>Parser looks happy</span>
                </article>
              )}
            </div>
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
                  width: `${layout.width}px`,
                  height: `${layout.height}px`,
                  transform: `translate(${pan.x}px, ${pan.y}px) scale(${zoom})`,
                }}
              >
                <svg className="graph-edges" width={layout.width} height={layout.height} viewBox={`0 0 ${layout.width} ${layout.height}`}>
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
                          }`}
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

                    return (
                      <div
                        key={table.alias}
                        className={`graph-node ${table.role === 'source' ? 'graph-node--source' : ''}${
                          isSelected ? ' graph-node--selected' : ''
                        }${
                          focusState.active && !isSelected && isFocusRelated ? ' graph-node--related' : ''
                        }${
                          isSearchMatched ? ' graph-node--matched' : ''
                        }${
                          planSignal ? ` graph-node--plan-${planSignal.severity}` : ''
                        }${
                          isDimmed ? ' graph-node--dimmed' : ''
                        }`}
                        style={{ transform: `translate(${table.x}px, ${table.y}px)` }}
                        onPointerDown={(event) => beginNodeDrag(table.alias, event)}
                      >
                        <span className="graph-node__role">{table.role === 'source' ? 'source' : 'join'}</span>
                        {planSignal ? <span className={`graph-node__plan-badge graph-node__plan-badge--${planSignal.severity}`}>{planSignal.title}</span> : null}
                        <strong>{table.name}</strong>
                        <p>alias: {table.alias}</p>
                      </div>
                    );
                  })()
                ))}
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
                            className="minimap__edge"
                          />
                        );
                      })}
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

              <div className="graph-hint">Click a node or edge to inspect it. Drag empty space to pan. Scroll to zoom. Use the minimap to jump across large queries.</div>
            </div>
          </div>

          <aside className="detail-rail">
            <article className="rail-card inspector-panel">
              <div className="panel-head">
                <span>Join inspector</span>
                <strong>{selectedJoinInsight ? selectedJoinInsight.badgeLabel : 'Select edge'}</strong>
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
                      <span className="inspector-badge inspector-badge--neutral">Heuristic</span>
                    </div>

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
                      <span className="canvas-toolbar__label">Plan signals</span>
                      {selectedJoinSignals.length > 0 ? (
                        <div className="plan-signal-list">
                          {selectedJoinSignals.slice(0, 4).map((signal) => (
                            <article key={signal.id} className={`plan-signal plan-signal--${signal.severity}`}>
                              <strong>{signal.title}</strong>
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
                    </section>
                  </div>
                ) : (
                  <p className="inspector-empty">
                    Select an edge to inspect its full ON clause, heuristic cardinality, index hints, and any mapped EXPLAIN signals.
                  </p>
                )}
              </div>
            </article>

            <article className="bottom-card rail-card">
              <div className="panel-head">
                <span>Details</span>
                <strong>{detailTab === 'joins' ? analysis.joins.length : analysis.clauses.filter((clause) => clause.present).length}</strong>
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
              </div>
              <div className={`bottom-card__content ${detailTab === 'joins' ? 'mono-list' : 'clause-list compact'}`}>
                {detailTab === 'joins' ? (
                  analysis.joins.length > 0 ? (
                    <>
                      <p className="rail-note">Cardinality badges use `id` and `_id` naming heuristics. They are helpful, not guaranteed.</p>
                      {analysis.joins.map((join) => {
                        const insight = joinInsights[join.id];
                        const isSearchMatched = searchState.matchedJoinIds.has(join.id);
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
                          </button>
                        );
                      })}
                    </>
                  ) : (
                    <div>No joins detected.</div>
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
    </div>
  );
}

export default App;
