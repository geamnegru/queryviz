import type { LayoutMode, ReviewStatus, SchemaSourceKind, Severity, SqlAnalysis, SqlDialect, SqlDialectDetection } from './types';

export const severityLabel: Record<Severity, string> = {
  high: 'High',
  medium: 'Medium',
  low: 'Low',
};

export const layoutModeLabel: Record<LayoutMode, string> = {
  horizontal: 'Horizontal',
  vertical: 'Vertical',
  radial: 'Radial',
};

export const reviewStatusLabel: Record<ReviewStatus, string> = {
  draft: 'Draft',
  needs_changes: 'Needs changes',
  approved: 'Approved',
};

export const reviewStatusTone: Record<ReviewStatus, 'neutral' | 'review' | 'good'> = {
  draft: 'neutral',
  needs_changes: 'review',
  approved: 'good',
};

export const dialectLabel: Record<SqlDialect, string> = {
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

export const schemaSourceLabel: Record<SchemaSourceKind, string> = {
  empty: 'No metadata',
  ddl: 'DDL',
  'dbt-manifest': 'dbt manifest',
  'dbt-schema-yml': 'dbt schema.yml',
};

export const FLAG_COPY: Record<string, { title: string; description: string }> = {
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

export const formatDialectEvidence = (detection: SqlDialectDetection) =>
  detection.evidence.length > 0 ? detection.evidence.join(' · ') : 'No strong dialect markers yet';

export const formatStatementTypeLabel = (statementType: string) =>
  statementType
    .split('-')
    .map((part) => part.toUpperCase())
    .join(' ');

export const normalizeSpaces = (value: string) => value.replace(/\s+/g, ' ').trim();

export const truncateText = (value: string, maxLength: number) => {
  if (value.length <= maxLength) {
    return value;
  }

  return `${value.slice(0, Math.max(0, maxLength - 1)).trimEnd()}…`;
};

export const formatPlural = (count: number, singular: string, plural = `${singular}s`) =>
  `${count} ${count === 1 ? singular : plural}`;

export const formatCompactNumber = (value: number) => {
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

export const escapeMarkdownCell = (value: string) => value.replace(/\|/g, '\\|').replace(/\n/g, '<br />');

export const formatJoinTypeLabel = (joinType: string) => {
  const normalized = normalizeSpaces(joinType);
  if (!normalized) {
    return 'JOIN';
  }

  if (/^(merge|update target)$/i.test(normalized)) {
    return normalized.toUpperCase();
  }

  return /join$/i.test(normalized) ? normalized.toUpperCase() : `${normalized.toUpperCase()} JOIN`;
};

export const getFlagContent = (title: string, fallbackDescription: string) => {
  const mapped = FLAG_COPY[title.toLowerCase()];
  return mapped ?? { title, description: fallbackDescription };
};

export const formatSignedDelta = (value: number) => (value > 0 ? `+${value}` : `${value}`);

export const formatEntityNoteLabel = (entityKey: string, analysis: SqlAnalysis) => {
  const [kind, ...rest] = entityKey.split(':');
  const value = rest.join(':');
  const join = kind === 'join' ? analysis.joins.find((item) => item.id === value) : null;
  const table = kind === 'node' ? analysis.tables.find((item) => item.alias === value) : null;

  if (join) {
    return `Join ${join.sourceAlias} -> ${join.targetAlias}`;
  }

  if (table) {
    return `Node ${table.alias} (${table.name})`;
  }

  if (kind === 'flag') {
    return `Flag ${value}`;
  }

  return entityKey;
};
