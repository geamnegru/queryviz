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
} from './lib/analyzeSql';
import {
  findSchemaTable,
  findForeignKeyPairMatch,
  getColumnSetCoverage,
  hasForeignKeyMatch,
  parseSchemaInput,
} from './lib/schemaMetadata';
import {
  NODE_HEIGHT,
  NODE_WIDTH,
  createEdgePortMap,
  createNodeLayout,
} from './lib/graphLayout';
import {
  createQueryTitle,
  createReviewShareUrl,
  createSavedQueryId,
  createShareUrl,
  createWorkspaceStateKey,
  persistSavedQueries,
  readSavedQueries,
  readSharedWorkspace,
  readWorkspaceViewState,
  saveWorkspaceViewState,
  upsertSavedQuery,
} from './lib/workspaceState';
import {
  createEmptyExplainSummary,
  formatExplainMapping,
  formatPlanMetrics,
  getEstimateBadgeLabel,
  getEstimateSeverity,
  getStrongestExplainSignal,
  parseExplainInput,
} from './lib/explainAnalysis';
import { SAMPLE_SCHEMA_SQL, getDialectSampleSql } from './lib/sqlSamples';
import {
  dialectLabel,
  formatCompactNumber,
  formatDialectEvidence,
  formatEntityNoteLabel,
  formatJoinTypeLabel,
  formatPlural,
  formatSignedDelta,
  formatStatementTypeLabel,
  getFlagContent,
  layoutModeLabel,
  normalizeSpaces,
  reviewStatusLabel,
  reviewStatusTone,
  schemaSourceLabel,
  severityLabel,
  truncateText,
} from './lib/presentation';
import {
  buildCompareSummary,
  buildExecutionReport,
  buildJoinComparisonLabel,
  buildReviewReport,
  buildRewriteGuidance,
} from './lib/reviewArtifacts';
import type {
  AliasFilterContext,
  ColumnLineage,
  DerivedRelation,
  DetailTab,
  DiagnosticSummary,
  DialectMode,
  ExplainSignal,
  FanoutImpact,
  JoinCardinality,
  JoinConditionPair,
  JoinInsight,
  JoinRef,
  LayoutMode,
  ReviewStatus,
  SavedQuery,
  SchemaTableMetadata,
  SearchState,
  SqlDiagnostic,
  TableRef,
} from './lib/types';

const SQL_EDITOR_LINE_HEIGHT = 24;
const SQL_EDITOR_PADDING = 14;

const EDGE_CONDITION_MAX = 30;

const createDotFileName = (statementIndex: number) => `queryviz-statement-${statementIndex + 1}.dot`;
const createPngFileName = (statementIndex: number) => `queryviz-statement-${statementIndex + 1}.png`;
const createSvgFileName = (statementIndex: number) => `queryviz-statement-${statementIndex + 1}.svg`;

const summarizeStatement = (statement: string, index: number) => {
  const normalized = statement.replace(/\s+/g, ' ').trim();
  return `#${index + 1} ${normalized.slice(0, 78)}${normalized.length > 78 ? '...' : ''}`;
};
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

const cleanColumnName = (value: string) => value.replace(/[`"'[\]]/g, '').trim();

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

const formatVerificationColumns = (alias: string, columns: string[]) => `${alias}(${columns.join(', ')})`;

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
  const sourceToTargetPairs = pairs.map((pair) => ({
    column: pair.sourceColumn,
    referenceColumn: pair.targetColumn,
  }));
  const targetToSourcePairs = pairs.map((pair) => ({
    column: pair.targetColumn,
    referenceColumn: pair.sourceColumn,
  }));
  const sourceToTargetMatch = findForeignKeyPairMatch(sourceSchema, sourceToTargetPairs, targetSchema);
  const targetToSourceMatch = findForeignKeyPairMatch(targetSchema, targetToSourcePairs, sourceSchema);
  const verifiedSourceToTarget = Boolean(sourceToTargetMatch);
  const verifiedTargetToSource = Boolean(targetToSourceMatch);
  const pairClassifications = pairs.map((pair) =>
    classifyColumnPair(pair.sourceColumn, pair.targetColumn, sourceTableName, targetTableName, sourceSchema, targetSchema),
  );
  const hasVerifiedPair =
    verifiedSourceToTarget ||
    verifiedTargetToSource ||
    pairs.some(
      (pair) =>
        hasForeignKeyMatch(sourceSchema, [pair.sourceColumn], targetSchema, [pair.targetColumn]) ||
        hasForeignKeyMatch(targetSchema, [pair.targetColumn], sourceSchema, [pair.sourceColumn]),
    );

  const cardinality =
    verifiedSourceToTarget
      ? 'N:1'
      : verifiedTargetToSource
        ? '1:N'
        : pairClassifications.length > 0 && pairClassifications.every((value) => value === pairClassifications[0])
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
  const verificationDetails =
    sourceToTargetMatch
      ? [
          `Matched FOREIGN KEY ${formatVerificationColumns(join.sourceAlias, sourceToTargetMatch.foreignKey.columns)} -> ${formatVerificationColumns(join.targetAlias, sourceToTargetMatch.foreignKey.referencesColumns)}.`,
          sourceToTargetMatch.referenceCoverage
            ? `Target key coverage is backed by ${describeCoverage(sourceToTargetMatch.referenceCoverage)} on ${formatVerificationColumns(join.targetAlias, sourceToTargetMatch.foreignKey.referencesColumns)}.`
            : `Target key coverage is schema-backed but does not advertise PRIMARY KEY / UNIQUE / INDEX metadata explicitly.`,
          sourceToTargetMatch.pairCount > 1
            ? `Composite verification matched ${sourceToTargetMatch.pairCount} join columns.`
            : 'Single-column verification matched the imported FK exactly.',
        ]
      : targetToSourceMatch
        ? [
            `Matched FOREIGN KEY ${formatVerificationColumns(join.targetAlias, targetToSourceMatch.foreignKey.columns)} -> ${formatVerificationColumns(join.sourceAlias, targetToSourceMatch.foreignKey.referencesColumns)}.`,
            targetToSourceMatch.referenceCoverage
              ? `Source key coverage is backed by ${describeCoverage(targetToSourceMatch.referenceCoverage)} on ${formatVerificationColumns(join.sourceAlias, targetToSourceMatch.foreignKey.referencesColumns)}.`
              : `Source key coverage is schema-backed but does not advertise PRIMARY KEY / UNIQUE / INDEX metadata explicitly.`,
            targetToSourceMatch.pairCount > 1
              ? `Composite verification matched ${targetToSourceMatch.pairCount} join columns.`
              : 'Single-column verification matched the imported FK exactly.',
          ]
        : [];

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
      verificationDetails,
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
      verificationDetails,
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
    verificationDetails,
    fanoutSeverity: 'high',
    fanoutSummary: 'Ambiguous join keys can create fanout quickly. Validate row counts before aggregating.',
  };
};

const getNodeNoteKey = (alias: string) => `node:${alias}`;

const getJoinNoteKey = (joinId: string) => `join:${joinId}`;

const getFlagNoteKey = (title: string) => `flag:${title}`;

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
  const defaultSql = getDialectSampleSql('postgres');
  const sharedWorkspace = readSharedWorkspace();
  const [sql, setSql] = useState(sharedWorkspace?.sql ?? defaultSql);
  const [selectedStatementIndex, setSelectedStatementIndex] = useState(sharedWorkspace?.selectedStatementIndex ?? 0);
  const [savedQueries, setSavedQueries] = useState<SavedQuery[]>(() => readSavedQueries());
  const [dialect, setDialect] = useState<DialectMode>(
    sharedWorkspace?.dialect ?? detectSqlDialect(sharedWorkspace?.sql ?? defaultSql).dialect,
  );
  const [schemaSql, setSchemaSql] = useState(sharedWorkspace?.schemaSql ?? '');
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
  const [detailTab, setDetailTab] = useState<DetailTab>(sharedWorkspace?.mode === 'review' ? 'review' : 'joins');
  const [nodeOffsets, setNodeOffsets] = useState<Record<string, { x: number; y: number }>>({});
  const [entityNotes, setEntityNotes] = useState<Record<string, string>>(sharedWorkspace?.entityNotes ?? {});
  const [reviewStatus, setReviewStatus] = useState<ReviewStatus>(sharedWorkspace?.reviewStatus ?? 'draft');
  const [reviewSummary, setReviewSummary] = useState(sharedWorkspace?.reviewSummary ?? '');
  const [isReadOnlyReview, setIsReadOnlyReview] = useState(sharedWorkspace?.mode === 'review');
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
  const skipInitialWorkspaceRestoreRef = useRef(Boolean(sharedWorkspace));
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
  const reviewCommentEntries = useMemo(
    () => Object.entries(entityNotes).filter((entry) => entry[1].trim().length > 0),
    [entityNotes],
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
  const reviewGuidance = useMemo(
    () => buildRewriteGuidance(analysis, joinInsights, explainSummary, activeDialect),
    [activeDialect, analysis, explainSummary, joinInsights],
  );
  const selectedJoinGuidance = useMemo(
    () => (selectedJoin ? reviewGuidance.filter((item) => item.relatedJoinId === selectedJoin.id) : []),
    [reviewGuidance, selectedJoin],
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
  const reviewReport = useMemo(
    () =>
      buildReviewReport({
        activeDialect,
        analysis,
        compareAnalysis,
        compareSummary,
        entityNotes,
        explainSummary,
        fanoutPathCount: fanoutState.impactedAliases.size,
        joinInsights,
        layoutMode,
        parsedSchema,
        reviewGuidance,
        reviewStatus,
        reviewSummary,
        verifiedJoinCount,
      }),
    [activeDialect, analysis, compareAnalysis, compareSummary, entityNotes, explainSummary, fanoutState.impactedAliases.size, joinInsights, layoutMode, parsedSchema, reviewGuidance, reviewStatus, reviewSummary, verifiedJoinCount],
  );
  const executionReport = useMemo(
    () =>
      buildExecutionReport({
        activeDialect,
        analysis,
        explainSummary,
        joinInsights,
        parsedSchema,
        verifiedJoinCount,
      }),
    [activeDialect, analysis, explainSummary, joinInsights, parsedSchema, verifiedJoinCount],
  );

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
    if (skipInitialWorkspaceRestoreRef.current) {
      skipInitialWorkspaceRestoreRef.current = false;
      return;
    }

    const savedState = readWorkspaceViewState(workspaceStateKey);
    if (!savedState) {
      return;
    }

    /* eslint-disable react-hooks/set-state-in-effect */
    setDialect(savedState.dialect);
    setSchemaSql(savedState.schemaSql);
    setReviewStatus(savedState.reviewStatus);
    setReviewSummary(savedState.reviewSummary);
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
      reviewStatus,
      reviewSummary,
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
    reviewStatus,
    reviewSummary,
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
    setSchemaSql('');
    setIsReadOnlyReview(false);
    setReviewStatus('draft');
    setReviewSummary('');
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
      return upsertSavedQuery(current, nextEntry);
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

  const handleCopyReviewLink = async () => {
    if (!analysis.normalizedSql.trim()) {
      return;
    }

    const shareUrl = createReviewShareUrl({
      sql,
      selectedStatementIndex: safeSelectedStatementIndex,
      dialect: activeDialect,
      schemaSql,
      mode: 'review',
      reviewStatus,
      reviewSummary,
      entityNotes,
    });

    try {
      await navigator.clipboard.writeText(shareUrl);
      setStatusMessage('Review link copied.');
    } catch {
      window.prompt('Copy this review link:', shareUrl);
    }
  };

  const handleExitReadOnlyReview = () => {
    if (typeof window !== 'undefined') {
      window.location.hash = createShareUrl(sql, safeSelectedStatementIndex, activeDialect).split('#')[1] ?? '';
    }
    setIsReadOnlyReview(false);
    setStatusMessage('Switched to editable workspace.');
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
          {isReadOnlyReview ? (
            <div className="review-banner">
              <strong>Read-only review page</strong>
              <span>This link opened Queryviz in review mode. You can inspect the graph, comments, and guidance, but edits are locked.</span>
              <button type="button" onClick={handleExitReadOnlyReview}>
                Open editable workspace
              </button>
            </div>
          ) : null}
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
              readOnly={isReadOnlyReview}
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
              disabled={isReadOnlyReview}
            >
              Load sample
            </button>
            <button type="button" onClick={() => fileInputRef.current?.click()} disabled={isReadOnlyReview}>
              Open .sql
            </button>
            <button type="button" onClick={() => loadWorkspace('')} disabled={isReadOnlyReview}>
              Clear
            </button>
            <button type="button" onClick={handleSaveQuery} disabled={isReadOnlyReview || !analysis.normalizedSql.trim()}>
              Save query
            </button>
            <button type="button" onClick={() => void handleCopyShareLink()} disabled={!analysis.normalizedSql.trim()}>
              Copy share link
            </button>
            <button type="button" onClick={() => void handleCopyReviewLink()} disabled={!analysis.normalizedSql.trim()}>
              Copy review link
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
            <strong>
              {parsedSchema.summary.tableCount > 0
                ? `${parsedSchema.summary.tableCount} tables · ${schemaSourceLabel[parsedSchema.sourceKind]}`
                : 'Optional'}
            </strong>
          </div>
          <textarea
            className="plan-input schema-input"
            value={schemaSql}
            onChange={handleSchemaChange}
            readOnly={isReadOnlyReview}
            spellCheck={false}
            placeholder="Paste CREATE TABLE / CREATE INDEX DDL, a dbt manifest JSON, or dbt schema.yml metadata here."
          />
          <div className="editor-actions editor-actions--compact">
            <button
              type="button"
              onClick={() => {
                setSchemaSql(SAMPLE_SCHEMA_SQL);
                setStatusMessage('Sample schema loaded.');
              }}
              disabled={isReadOnlyReview}
            >
              Load sample schema
            </button>
            <button type="button" onClick={() => schemaFileInputRef.current?.click()} disabled={isReadOnlyReview}>
              Open metadata
            </button>
            <button
              type="button"
              onClick={() => {
                setSchemaSql('');
                setStatusMessage('Schema cleared.');
              }}
              disabled={isReadOnlyReview || !schemaSql.trim()}
            >
              Clear schema
            </button>
          </div>
          <input
            ref={schemaFileInputRef}
            className="hidden-file-input"
            type="file"
            accept=".sql,.ddl,.txt,.json,.yml,.yaml"
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
                  Matching joins will switch from <strong>Heuristic</strong> to <strong>Verified</strong> when imported DDL or dbt metadata confirms FK or key coverage.
                </p>
              </>
            ) : (
              <p className="editor-note">No DDL or dbt metadata was recognized yet. Paste a little more schema context.</p>
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
            readOnly={isReadOnlyReview}
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
                {explainSummary.summary.unmappedJoinNodes > 0 ? (
                  <span className="plan-summary__chip plan-summary__chip--warning">
                    Unmapped join nodes: {explainSummary.summary.unmappedJoinNodes}
                  </span>
                ) : null}
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
                  Some joins are still unmatched. Paste a fuller plan or engine-native JSON/XML output for better edge mapping. Unmapped join nodes are counted separately above.
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

                    {selectedJoinInsight.verificationDetails.length > 0 ? (
                      <section className="inspector-section">
                        <span className="canvas-toolbar__label">Verification</span>
                        <ul className="inspector-list">
                          {selectedJoinInsight.verificationDetails.map((detail) => (
                            <li key={detail}>{detail}</li>
                          ))}
                        </ul>
                      </section>
                    ) : null}

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

                    {selectedJoinGuidance.length > 0 ? (
                      <section className="inspector-section">
                        <span className="canvas-toolbar__label">Rewrite guidance</span>
                        <div className="guidance-list">
                          {selectedJoinGuidance.slice(0, 3).map((item) => (
                            <article key={item.id} className={`guidance-card guidance-card--${item.priority}`}>
                              <strong>{item.title}</strong>
                              <p>{item.summary}</p>
                              <small>{item.action}</small>
                            </article>
                          ))}
                        </div>
                      </section>
                    ) : null}

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
                              {formatExplainMapping(signal) ? <small>{formatExplainMapping(signal)}</small> : null}
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
                      <span className="canvas-toolbar__label">Review comments</span>
                      <textarea
                        className="note-input"
                        value={entityNotes[getJoinNoteKey(selectedJoin.id)] ?? ''}
                        onChange={(event) => handleEntityNoteChange(getJoinNoteKey(selectedJoin.id), event.target.value)}
                        readOnly={isReadOnlyReview}
                        spellCheck={false}
                        placeholder="Add a comment about this join, fanout risk, or follow-up."
                      />
                      <small className="inspector-note">Comments are saved locally per query workspace and can be included in review links.</small>
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
                              {formatExplainMapping(signal) ? <small>{formatExplainMapping(signal)}</small> : null}
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
                      <span className="canvas-toolbar__label">Review comments</span>
                      <textarea
                        className="note-input"
                        value={entityNotes[getNodeNoteKey(selectedTable.alias)] ?? ''}
                        onChange={(event) => handleEntityNoteChange(getNodeNoteKey(selectedTable.alias), event.target.value)}
                        readOnly={isReadOnlyReview}
                        spellCheck={false}
                        placeholder="Add a comment about this table, CTE, or subquery."
                      />
                      <small className="inspector-note">Comments are saved locally per query workspace and can be included in review links.</small>
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
                    : detailTab === 'review'
                      ? reviewGuidance.length + reviewCommentEntries.length + (reviewSummary.trim() ? 1 : 0)
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
                <button
                  type="button"
                  className={detailTab === 'review' ? 'is-active' : ''}
                  onClick={() => setDetailTab('review')}
                >
                  Review
                </button>
              </div>
              <div className={`bottom-card__content ${detailTab === 'joins' || detailTab === 'lineage' ? 'mono-list' : detailTab === 'clauses' ? 'clause-list compact' : ''}`}>
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
                ) : detailTab === 'review' ? (
                  <div className="review-panel">
                    {isReadOnlyReview ? (
                      <p className="rail-note">
                        This review link is read-only. You can inspect comments, verification details, and rewrite guidance, but any edits stay locked.
                      </p>
                    ) : null}
                    <section className="review-section">
                      <span className="canvas-toolbar__label">Review status</span>
                      <div className="review-statuses">
                        {(['draft', 'needs_changes', 'approved'] as const).map((status) => (
                          <button
                            key={status}
                            type="button"
                            className={`review-status review-status--${reviewStatusTone[status]}${reviewStatus === status ? ' is-active' : ''}`}
                            onClick={() => setReviewStatus(status)}
                            disabled={isReadOnlyReview}
                          >
                            {reviewStatusLabel[status]}
                          </button>
                        ))}
                      </div>
                      <p className="rail-note">Use review mode to leave a quick verdict, capture follow-up, and share the exact workspace state with teammates.</p>
                    </section>

                    <section className="review-section">
                      <div className="panel-head">
                        <span>Summary</span>
                        <strong>{reviewCommentEntries.length} comments</strong>
                      </div>
                      <textarea
                        className="note-input note-input--summary"
                        value={reviewSummary}
                        onChange={(event) => setReviewSummary(event.target.value)}
                        readOnly={isReadOnlyReview}
                        spellCheck={false}
                        placeholder="Summarize the main review outcome, risk, or next step."
                      />
                      <div className="editor-actions editor-actions--compact">
                        <button type="button" onClick={() => void handleCopyReviewReport()} disabled={!analysis.normalizedSql.trim()}>
                          Copy review report
                        </button>
                        <button type="button" onClick={() => void handleCopyReviewLink()} disabled={!analysis.normalizedSql.trim()}>
                          Copy review link
                        </button>
                      </div>
                    </section>

                    <section className="review-section">
                      <div className="panel-head">
                        <span>Rewrite guidance</span>
                        <strong>{reviewGuidance.length}</strong>
                      </div>
                      {reviewGuidance.length > 0 ? (
                        <div className="guidance-list">
                          {reviewGuidance.map((item) => (
                            <article key={item.id} className={`guidance-card guidance-card--${item.priority}`}>
                              <div className="guidance-card__head">
                                <strong>{item.title}</strong>
                                <span>{item.priority.toUpperCase()} · {item.confidence === 'structural' ? 'Structural' : item.confidence === 'verified' ? 'Verified' : 'Heuristic'}</span>
                              </div>
                              <p>{item.summary}</p>
                              <small>{item.action}</small>
                            </article>
                          ))}
                        </div>
                      ) : (
                        <p className="inspector-empty">No extra rewrite guidance yet. Paste EXPLAIN, import schema, or inspect flags to surface more specific fixes.</p>
                      )}
                    </section>

                    <section className="review-section">
                      <div className="panel-head">
                        <span>Flag comments</span>
                        <strong>{analysis.flags.length}</strong>
                      </div>
                      {analysis.flags.length > 0 ? (
                        <div className="flag-review-list">
                          {analysis.flags.map((flag) => {
                            const content = getFlagContent(flag.title, flag.description);
                            return (
                              <article key={content.title} className={`flag flag--${flag.severity}`}>
                                <div className="flag__head">
                                  <strong>{content.title}</strong>
                                  <span className="flag__severity">{severityLabel[flag.severity]}</span>
                                </div>
                                <p className="flag__description">{content.description}</p>
                                <textarea
                                  className="note-input note-input--compact"
                                  value={entityNotes[getFlagNoteKey(content.title)] ?? ''}
                                  onChange={(event) => handleEntityNoteChange(getFlagNoteKey(content.title), event.target.value)}
                                  readOnly={isReadOnlyReview}
                                  spellCheck={false}
                                  placeholder="Leave a review comment or follow-up for this flag."
                                />
                              </article>
                            );
                          })}
                        </div>
                      ) : (
                        <p className="inspector-empty">No flags on this statement. You can still leave summary comments above if you reviewed it manually.</p>
                      )}
                    </section>

                    {reviewCommentEntries.length > 0 ? (
                      <section className="review-section">
                        <div className="panel-head">
                          <span>Comments in workspace</span>
                          <strong>{reviewCommentEntries.length}</strong>
                        </div>
                        <div className="comment-list">
                          {reviewCommentEntries.map(([entityKey, note]) => (
                            <article key={entityKey} className="comment-card">
                              <strong>{formatEntityNoteLabel(entityKey, analysis)}</strong>
                              <p>{normalizeSpaces(note)}</p>
                            </article>
                          ))}
                        </div>
                      </section>
                    ) : null}
                  </div>
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
