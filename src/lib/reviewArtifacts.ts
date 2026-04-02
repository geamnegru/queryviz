import {
  dialectLabel,
  escapeMarkdownCell,
  formatCompactNumber,
  formatEntityNoteLabel,
  formatJoinTypeLabel,
  formatPlural,
  formatSignedDelta,
  formatStatementTypeLabel,
  getFlagContent,
  layoutModeLabel,
  normalizeSpaces,
  reviewStatusLabel,
  schemaSourceLabel,
  severityLabel,
} from './presentation';
import {
  formatExplainMapping,
  formatPlanMetrics,
  getEstimateSeverity,
  getStrongestExplainSignal,
} from './explainAnalysis';
import type {
  BuildExecutionReportOptions,
  BuildReviewReportOptions,
  CompareSummary,
  ExplainSummary,
  ReportJoinInsight,
  ReviewGuidanceItem,
  SqlAnalysis,
  SqlDialect,
} from './types';

export const buildRewriteGuidance = (
  analysis: SqlAnalysis,
  joinInsights: Record<string, ReportJoinInsight>,
  explainSummary: ExplainSummary,
  dialect: SqlDialect,
) => {
  const items = new Map<string, ReviewGuidanceItem>();
  const priorityRank: Record<ReviewGuidanceItem['priority'], number> = {
    high: 3,
    medium: 2,
    low: 1,
  };
  const addItem = (item: ReviewGuidanceItem) => {
    if (!items.has(item.id)) {
      items.set(item.id, item);
    }
  };
  const flagByTitle = new Map<string, { title: string; description: string }>();

  analysis.flags.forEach((flag) => {
    const content = getFlagContent(flag.title, flag.description);
    flagByTitle.set(content.title.toLowerCase(), content);
  });

  if (flagByTitle.has('function in where')) {
    addItem({
      id: 'rewrite:function-where',
      title: 'Rewrite function-wrapped filters',
      summary: 'A function in WHERE usually blocks normal index access and turns a cheap seek into a scan.',
      action: 'Rewrite DATE_TRUNC/LOWER predicates into range predicates or compare against a computed column so the base column stays sargable.',
      priority: 'high',
      confidence: 'structural',
      relatedFlagTitle: 'Function in WHERE',
    });
  }

  if (flagByTitle.has('wildcard select')) {
    addItem({
      id: 'rewrite:wildcard-select',
      title: 'Trim the SELECT list to the columns you actually need',
      summary: 'Wide projections increase I/O, memory pressure, and schema drift risk for downstream code.',
      action: 'Replace SELECT * with an explicit column list, then verify whether the plan can stay index-only or at least read fewer pages.',
      priority: 'medium',
      confidence: 'structural',
      relatedFlagTitle: 'Wildcard SELECT',
    });
  }

  if (flagByTitle.has('correlated subquery')) {
    addItem({
      id: 'rewrite:correlated-subquery',
      title: 'Pre-aggregate the correlated subquery before joining it back',
      summary: 'Correlated subqueries often execute once per row and can quietly turn into repeated nested-loop work.',
      action: 'Move the subquery into a derived table or CTE grouped at the needed grain, then LEFT JOIN that aggregate back to the driving relation.',
      priority: 'high',
      confidence: 'structural',
      relatedFlagTitle: 'Correlated subquery',
    });
  }

  if (flagByTitle.has('join-heavy query')) {
    addItem({
      id: 'rewrite:join-heavy',
      title: 'Use EXPLAIN to confirm join order and scan shape before rewriting blindly',
      summary: 'Join-heavy queries are not automatically bad, but they magnify the cost of one bad join order or missing index.',
      action: 'Run EXPLAIN ANALYZE, inspect the highest-cost joins first, and isolate wide subgraphs into CTEs only when that makes the plan easier to reason about.',
      priority: 'medium',
      confidence: 'structural',
      relatedFlagTitle: 'Join-heavy query',
    });
  }

  if (flagByTitle.has('repeated unnest') || flagByTitle.has('cross join unnest')) {
    addItem({
      id: 'rewrite:unnest',
      title: 'Filter and re-aggregate around UNNEST operations',
      summary: 'Repeated UNNEST and CROSS JOIN UNNEST can explode row counts before you notice it in aggregates.',
      action: 'Push filters into the array source first, then aggregate back to the parent grain before joining the expanded rows to the rest of the query.',
      priority: 'high',
      confidence: 'structural',
      relatedFlagTitle: flagByTitle.has('repeated unnest') ? 'Repeated UNNEST' : 'CROSS JOIN UNNEST',
    });
  }

  if (flagByTitle.has('flatten relation')) {
    addItem({
      id: 'rewrite:flatten',
      title: 'Constrain FLATTEN inputs before they hit the main join graph',
      summary: 'FLATTEN expands semi-structured arrays quickly and can dominate the rest of the plan.',
      action: 'Filter the source JSON first, project only needed fields, and aggregate or distinct the flattened rows before rejoining them.',
      priority: 'high',
      confidence: 'structural',
      relatedFlagTitle: 'FLATTEN relation',
    });
  }

  if (flagByTitle.has('qualify filter') || flagByTitle.has('windowed qualify')) {
    addItem({
      id: 'rewrite:qualify',
      title: `Reduce ${dialectLabel[dialect]} QUALIFY work before the window stage`,
      summary: 'QUALIFY usually means a window sort, and large partitions get expensive fast.',
      action: 'Filter earlier if possible, partition on a smaller key, and consider pre-aggregating before ROW_NUMBER or other ranking logic.',
      priority: 'medium',
      confidence: 'structural',
      relatedFlagTitle: flagByTitle.has('windowed qualify') ? 'Windowed QUALIFY' : 'QUALIFY filter',
    });
  }

  if (flagByTitle.has('apply operator')) {
    addItem({
      id: 'rewrite:apply',
      title: 'Replace APPLY with a set-based derived table when possible',
      summary: 'APPLY often behaves like row-by-row work, especially when SHOWPLAN shows nested loops or repeated scans.',
      action: 'Try rewriting APPLY into a LEFT JOIN against a pre-aggregated subquery or a window-function result set that can be evaluated once per group.',
      priority: 'high',
      confidence: 'structural',
      relatedFlagTitle: 'APPLY operator',
    });
  }

  if (flagByTitle.has('top without order by')) {
    addItem({
      id: 'rewrite:top-order',
      title: 'Add deterministic ordering to TOP/FETCH queries',
      summary: 'Row-capped queries without ORDER BY return arbitrary rows and make performance investigations noisy.',
      action: 'Add a stable ORDER BY on the intended sort key before trusting the sample or exporting it to callers.',
      priority: 'medium',
      confidence: 'structural',
      relatedFlagTitle: 'TOP without ORDER BY',
    });
  }

  if (flagByTitle.has('nolock hint')) {
    addItem({
      id: 'rewrite:nolock',
      title: 'Remove NOLOCK unless inconsistent reads are acceptable',
      summary: 'NOLOCK can return dirty, duplicated, or skipped rows, which makes debugging fanout and aggregates much harder.',
      action: 'Prefer normal read committed semantics first, then add targeted indexing or query rewrites instead of masking blocking with NOLOCK.',
      priority: 'medium',
      confidence: 'structural',
      relatedFlagTitle: 'NOLOCK hint',
    });
  }

  if (flagByTitle.has('external file scan') || flagByTitle.has('wildcard table scan')) {
    addItem({
      id: 'rewrite:external-scan',
      title: 'Prune external scans early',
      summary: 'External file and wildcard table scans get expensive mainly because they read too much data before filters land.',
      action: 'Project fewer columns, push partition filters down as early as possible, and narrow file or shard patterns before joining.',
      priority: 'medium',
      confidence: 'structural',
      relatedFlagTitle: flagByTitle.has('external file scan') ? 'External file scan' : 'Wildcard table scan',
    });
  }

  analysis.joins.forEach((join) => {
    const insight = joinInsights[join.id];
    if (!insight) {
      return;
    }

    if (insight.cardinality === '1:N') {
      addItem({
        id: `rewrite:fanout:${join.id}`,
        title: `Aggregate ${join.targetAlias} before joining if the parent grain matters`,
        summary: `${join.sourceAlias} -> ${join.targetAlias} is ${insight.badgeLabel} (${insight.confidenceLabel.toLowerCase()}). That is the classic place where row multiplication starts.`,
        action: `If downstream metrics should stay at ${join.sourceAlias} grain, pre-aggregate ${join.targetAlias} by the join key and join that smaller result back instead of raw detail rows.`,
        priority: insight.confidence === 'verified' ? 'high' : 'medium',
        confidence: insight.confidence,
        relatedJoinId: join.id,
      });
    }

    if (insight.cardinality === 'M:N') {
      addItem({
        id: `rewrite:ambiguous:${join.id}`,
        title: `Review ${join.sourceAlias} ↔ ${join.targetAlias} for bridge-table or duplicate-key behavior`,
        summary: 'Mixed or non-key join predicates are the easiest way to create accidental many-to-many fanout.',
        action: 'Check both sides for duplicate keys, consider pre-aggregating each side to the intended grain, and validate row counts before and after this join.',
        priority: 'high',
        confidence: insight.confidence,
        relatedJoinId: join.id,
      });
    }

    const missingIndexHint = insight.indexHints.find(
      (hint) => hint.startsWith('Index likely helpful') || hint.startsWith('Imported schema does not currently show index coverage'),
    );
    if (missingIndexHint) {
      addItem({
        id: `rewrite:index:${join.id}`,
        title: `Test a supporting index for ${join.sourceAlias} -> ${join.targetAlias}`,
        summary: `This join does not yet show clear index coverage on the predicate columns used by ${insight.joinLabel}.`,
        action: missingIndexHint,
        priority: insight.confidence === 'verified' ? 'high' : 'medium',
        confidence: insight.confidence,
        relatedJoinId: join.id,
      });
    }
  });

  explainSummary.items
    .filter((signal) => {
      const severity = getEstimateSeverity(signal.estimateFactor);
      return Boolean(severity && severity !== 'low');
    })
    .sort((left, right) => Math.abs(right.estimateFactor ?? 1) - Math.abs(left.estimateFactor ?? 1))
    .slice(0, 3)
    .forEach((signal, index) => {
      addItem({
        id: `rewrite:misestimate:${signal.id}:${index}`,
        title: 'Investigate planner misestimates before trusting the plan',
        summary: `${signal.title} is showing a large estimated-vs-actual row gap, which usually means stale stats, skew, or hidden fanout.`,
        action: 'Check row counts at the join inputs, refresh statistics if needed, and test whether pre-aggregation or a different predicate shape makes the planner estimate closer to reality.',
        priority: 'high',
        confidence: 'structural',
      });
    });

  if (analysis.statementType !== 'select' && analysis.writeTarget) {
    addItem({
      id: 'rewrite:write-target',
      title: `Dry-run the source side before writing into ${analysis.writeTarget}`,
      summary: 'Write statements are harder to recover from when the source query fans out or scans more data than expected.',
      action: 'Run the SELECT portion first, validate row counts and join keys, then execute the write only after the source plan looks stable.',
      priority: 'high',
      confidence: 'structural',
    });
  }

  return Array.from(items.values()).sort((left, right) => {
    const priorityDelta = priorityRank[right.priority] - priorityRank[left.priority];
    if (priorityDelta !== 0) {
      return priorityDelta;
    }

    return left.title.localeCompare(right.title);
  });
};

export const buildJoinComparisonLabel = (join: SqlAnalysis['joins'][number]) =>
  `${formatJoinTypeLabel(join.type)} ${join.sourceAlias} -> ${join.targetAlias} / ${normalizeSpaces(`ON ${join.condition}`)}`;

export const buildCompareSummary = (
  currentAnalysis: SqlAnalysis,
  currentPlan: ExplainSummary,
  baselineAnalysis: SqlAnalysis,
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
    maxCostDelta: hasPlanComparison ? currentPlan.summary.maxCost - baselinePlan.summary.maxCost : null,
    maxRowsDelta: hasPlanComparison ? currentPlan.summary.maxRows - baselinePlan.summary.maxRows : null,
    addedTables,
    removedTables,
    addedJoins,
    removedJoins,
    addedFlags,
    removedFlags,
    hasPlanComparison,
  };
};

export const buildReviewReport = ({
  activeDialect,
  analysis,
  compareAnalysis,
  compareSummary,
  entityNotes,
  explainSummary,
  fanoutPathCount,
  joinInsights,
  layoutMode,
  parsedSchema,
  reviewGuidance,
  reviewStatus,
  reviewSummary,
  verifiedJoinCount,
}: BuildReviewReportOptions) => {
  const noteEntries = Object.entries(entityNotes).filter((entry) => entry[1].trim().length > 0);
  const verifiedJoinInsights = analysis.joins
    .map((join) => ({ join, insight: joinInsights[join.id] }))
    .filter((entry) => entry.insight?.confidence === 'verified' && entry.insight.verificationDetails.length > 0);
  const lines: string[] = [
    '# Queryviz Review Report',
    '',
    `- Review status: ${reviewStatusLabel[reviewStatus]}`,
    `- Statement: #${analysis.analyzedStatementIndex + 1}`,
    `- Statement type: ${formatStatementTypeLabel(analysis.statementType)}`,
    `- Dialect: ${dialectLabel[activeDialect]}`,
    `- Layout: ${layoutModeLabel[layoutMode]}`,
    `- Complexity score: ${analysis.complexityScore}`,
    `- Tables: ${analysis.tables.length}`,
    `- Joins: ${analysis.joins.length}`,
    `- Filters: ${analysis.filters.length}`,
    `- Fanout paths: ${fanoutPathCount}`,
  ];

  if (parsedSchema.summary.tableCount > 0) {
    lines.push(`- Imported metadata source: ${schemaSourceLabel[parsedSchema.sourceKind]}`);
    lines.push(`- Imported schema tables: ${parsedSchema.summary.tableCount}`);
    lines.push(`- Verified joins: ${verifiedJoinCount}`);
  }

  if (analysis.writeTarget) {
    lines.push(`- Write target: ${analysis.writeTarget}`);
  }

  if (reviewSummary.trim()) {
    lines.push(`- Review summary: ${normalizeSpaces(reviewSummary)}`);
  }

  if (analysis.derivedRelations.length > 0) {
    lines.push(`- Derived relations: ${analysis.derivedRelations.length}`);
  }

  if (explainSummary.items.length > 0) {
    lines.push(`- Plan overlay: ${formatPlural(explainSummary.items.length, 'signal')}`);
    lines.push(`- Mapped joins: ${explainSummary.summary.mappedJoins}`);
    lines.push(`- Unmapped join nodes: ${explainSummary.summary.unmappedJoinNodes}`);
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

  if (reviewGuidance.length > 0) {
    lines.push('', '## Rewrite Guidance', '');
    reviewGuidance.slice(0, 12).forEach((item) => {
      lines.push(`- [${item.priority.toUpperCase()}] ${item.title} — ${item.summary} Action: ${item.action}`);
    });
  }

  if (verifiedJoinInsights.length > 0) {
    lines.push('', '## Verified Join Matches', '');
    verifiedJoinInsights.forEach(({ join, insight }) => {
      lines.push(`### ${insight.joinLabel} ${join.sourceAlias} -> ${join.targetAlias}`);
      insight.verificationDetails.forEach((detail) => {
        lines.push(`- ${detail}`);
      });
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
    lines.push(`- Unmapped join nodes: ${explainSummary.summary.unmappedJoinNodes}`);
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
    lines.push('', '## Review Comments', '');

    noteEntries.forEach(([entityKey, note]) => {
      lines.push(`- ${escapeMarkdownCell(formatEntityNoteLabel(entityKey, analysis))} — ${escapeMarkdownCell(normalizeSpaces(note))}`);
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
};

export const buildExecutionReport = ({
  activeDialect,
  analysis,
  explainSummary,
  joinInsights,
  parsedSchema,
  verifiedJoinCount,
}: BuildExecutionReportOptions) => {
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
    lines.push(`- Imported metadata source: ${schemaSourceLabel[parsedSchema.sourceKind]}`);
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
    lines.push(`- Unmapped join nodes: ${explainSummary.summary.unmappedJoinNodes}`);
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
      const mapping = signal ? formatExplainMapping(signal) : '';
      lines.push(
        `- ${insight?.joinLabel ?? formatJoinTypeLabel(join.type)} ${join.sourceAlias} -> ${join.targetAlias} | ${insight?.badgeLabel ?? 'M:N'} | ${insight?.confidenceLabel ?? 'Heuristic'} | ${
          insight?.fullConditionLabel ?? join.condition
        }${signal ? ` | ${signal.title}${metrics ? ` (${metrics})` : ''}${mapping ? ` | ${mapping}` : ''}` : ''}`,
      );
      if (insight?.verificationDetails.length) {
        insight.verificationDetails.forEach((detail) => {
          lines.push(`  - ${detail}`);
        });
      }
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
};
