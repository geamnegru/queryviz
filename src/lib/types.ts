export type Severity = 'high' | 'medium' | 'low';

export type SqlDialect =
  | 'postgres'
  | 'mysql'
  | 'mariadb'
  | 'sqlite'
  | 'bigquery'
  | 'sqlserver'
  | 'oracle'
  | 'snowflake'
  | 'duckdb'
  | 'redshift'
  | 'trino';

export type SqlStatementType =
  | 'select'
  | 'insert-select'
  | 'create-view'
  | 'create-table-as'
  | 'update-from'
  | 'merge'
  | 'unknown';

export interface ColumnRef {
  expression: string;
  alias?: string;
}

export interface TableRef {
  id: string;
  name: string;
  alias: string;
  role: 'source' | 'join' | 'target';
  kind?: 'table' | 'cte' | 'subquery';
  derivedId?: string;
  specialType?: 'unnest' | 'flatten' | 'function' | 'external' | 'temp';
}

export interface JoinRef {
  id: string;
  type: string;
  tableName: string;
  alias: string;
  condition: string;
  sourceAlias: string;
  targetAlias: string;
}

export interface QueryFlag {
  severity: Severity;
  title: string;
  description: string;
}

export interface ClauseStatus {
  label: string;
  present: boolean;
  detail: string;
}

export interface DerivedRelation {
  id: string;
  name: string;
  alias: string;
  kind: 'cte' | 'subquery';
  body: string;
  sourceCount: number;
  joinCount: number;
  subqueryCount: number;
  hasAggregation: boolean;
  dependencies: string[];
  flags: string[];
}

export type SqlDiagnosticSeverity = 'error' | 'warning';

export interface SqlDiagnostic {
  severity: SqlDiagnosticSeverity;
  title: string;
  message: string;
  hint: string;
  line: number;
  column: number;
  index: number;
  excerpt: string;
}

export interface SqlAnalysis {
  statementCount: number;
  analyzedStatementIndex: number;
  analyzedStatement: string;
  normalizedSql: string;
  statementType: SqlStatementType;
  statementLabel: string;
  writeTarget?: string;
  columns: ColumnRef[];
  tables: TableRef[];
  joins: JoinRef[];
  filters: string[];
  groupBy: string[];
  orderBy: string[];
  limit?: string;
  clauses: ClauseStatus[];
  flags: QueryFlag[];
  derivedRelations: DerivedRelation[];
  subqueryCount: number;
  hasAggregation: boolean;
  complexityScore: number;
}

export interface SqlDialectDetection {
  dialect: SqlDialect;
  confident: boolean;
  evidence: string[];
  score: number;
}

export interface ClauseRanges {
  selectIndex: number;
  fromIndex: number;
  whereIndex: number;
  groupByIndex: number;
  havingIndex: number;
  qualifyIndex: number;
  windowIndex: number;
  orderByIndex: number;
  limitIndex: number;
  offsetIndex: number;
  fetchIndex: number;
}

export interface StatementEnvelope {
  statementType: SqlStatementType;
  statementLabel: string;
  writeTarget?: string;
  writeTargetAlias?: string;
  graphSql: string;
  mode: 'select' | 'update-from' | 'merge';
  updateSetClause?: string;
  mergeUsingClause?: string;
  mergeOnClause?: string;
}

export interface SchemaForeignKey {
  columns: string[];
  referencesTable: string;
  referencesColumns: string[];
}

export interface SchemaForeignKeyPairMatch {
  column: string;
  referenceColumn: string;
}

export interface SchemaVerificationMatch {
  foreignKey: SchemaForeignKey;
  referenceCoverage: 'primary-key' | 'unique' | 'index' | null;
  pairCount: number;
}

export type SchemaSourceKind = 'empty' | 'ddl' | 'dbt-manifest' | 'dbt-schema-yml';

export interface SchemaTableMetadata {
  name: string;
  normalizedName: string;
  shortName: string;
  columns: string[];
  primaryKey: string[];
  uniqueKeys: string[][];
  indexes: string[][];
  foreignKeys: SchemaForeignKey[];
}

export interface ParsedSchemaMetadata {
  tables: SchemaTableMetadata[];
  sourceKind: SchemaSourceKind;
  summary: {
    tableCount: number;
    foreignKeyCount: number;
    indexedGroupCount: number;
  };
}

export interface GraphLayout {
  width: number;
  height: number;
  positions: Record<string, { x: number; y: number }>;
}

export type LayoutMode = 'horizontal' | 'vertical' | 'radial';

export type NodeSide = 'left' | 'right' | 'top' | 'bottom';

export interface EdgePort {
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

export type ReviewStatus = 'draft' | 'needs_changes' | 'approved';

export type WorkspaceMode = 'workspace' | 'review';

export interface SavedQuery {
  id: string;
  title: string;
  sql: string;
  updatedAt: number;
  selectedStatementIndex: number;
  dialect: SqlDialect;
}

export interface WorkspaceViewState {
  layoutMode: LayoutMode;
  dialect: SqlDialect;
  schemaSql: string;
  reviewStatus: ReviewStatus;
  reviewSummary: string;
  expandedDerivedIds: string[];
  nodeOffsets: Record<string, { x: number; y: number }>;
  pan: { x: number; y: number };
  zoom: number;
  entityNotes: Record<string, string>;
  compareSql: string;
  compareExplainInput: string;
  updatedAt: number;
}

export interface SharedWorkspaceState {
  sql: string;
  selectedStatementIndex: number;
  dialect: SqlDialect;
  schemaSql: string;
  mode: WorkspaceMode;
  reviewStatus: ReviewStatus;
  reviewSummary: string;
  entityNotes: Record<string, string>;
}

export interface ExplainSignal {
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
  mappingMethod?: 'explicit' | 'relation-signals' | 'child-branches' | 'descendant-aliases' | 'nearby-scans';
  mappingConfidence?: 'high' | 'medium' | 'low';
}

export interface ExplainSummary {
  items: ExplainSignal[];
  relationSignals: Record<string, ExplainSignal[]>;
  joinSignals: Record<string, ExplainSignal[]>;
  summary: {
    seqScans: number;
    indexedReads: number;
    joinNodes: number;
    sorts: number;
    mappedJoins: number;
    unmappedJoinNodes: number;
    maxCost: number;
    maxRows: number;
    estimateWarnings: number;
  };
}

export interface CompareSummary {
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

export interface ReviewGuidanceItem {
  id: string;
  title: string;
  summary: string;
  action: string;
  priority: 'high' | 'medium' | 'low';
  confidence: 'structural' | 'heuristic' | 'verified';
  relatedJoinId?: string;
  relatedFlagTitle?: string;
}

export interface ReportJoinInsight {
  cardinality: 'N:1' | '1:N' | 'M:N';
  badgeLabel: string;
  confidence: 'heuristic' | 'verified';
  confidenceLabel: 'Heuristic' | 'Verified';
  joinLabel: string;
  fullConditionLabel: string;
  summary: string;
  indexHints: string[];
  verificationDetails: string[];
}

export type ParsedSchemaSummary = Pick<ParsedSchemaMetadata, 'sourceKind' | 'summary'>;

export interface BuildReviewReportOptions {
  activeDialect: SqlDialect;
  analysis: SqlAnalysis;
  compareAnalysis: SqlAnalysis | null;
  compareSummary: CompareSummary | null;
  entityNotes: Record<string, string>;
  explainSummary: ExplainSummary;
  fanoutPathCount: number;
  joinInsights: Record<string, ReportJoinInsight>;
  layoutMode: LayoutMode;
  parsedSchema: ParsedSchemaSummary;
  reviewGuidance: ReviewGuidanceItem[];
  reviewStatus: ReviewStatus;
  reviewSummary: string;
  verifiedJoinCount: number;
}

export interface BuildExecutionReportOptions {
  activeDialect: SqlDialect;
  analysis: SqlAnalysis;
  explainSummary: ExplainSummary;
  joinInsights: Record<string, ReportJoinInsight>;
  parsedSchema: ParsedSchemaSummary;
  verifiedJoinCount: number;
}

export type DialectMode = SqlDialect;

export type JoinCardinality = 'N:1' | '1:N' | 'M:N';

export interface JoinConditionPair {
  sourceAlias: string;
  sourceColumn: string;
  targetAlias: string;
  targetColumn: string;
}

export interface JoinInsight {
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
  verificationDetails: string[];
  fanoutSeverity: 'none' | 'caution' | 'high';
  fanoutSummary: string;
}

export interface DiagnosticSummary {
  blocking: SqlDiagnostic | null;
  items: SqlDiagnostic[];
}

export interface AliasFilterContext {
  plainColumns: string[];
  wrappedColumns: string[];
  expressions: string[];
}

export interface SearchState {
  active: boolean;
  query: string;
  matchCount: number;
  matchedAliases: Set<string>;
  matchedJoinIds: Set<string>;
}

export interface ColumnLineage {
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

export interface FanoutImpact {
  severity: 'caution' | 'high';
  viaJoinId: string;
  reason: string;
}

export type DetailTab = 'joins' | 'clauses' | 'lineage' | 'review';
