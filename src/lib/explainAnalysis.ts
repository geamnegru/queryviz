import type { ExplainSignal, ExplainSummary, JoinRef, TableRef } from './types';

const explainMappingLabel: Record<NonNullable<ExplainSignal['mappingMethod']>, string> = {
  explicit: 'Explicit join id',
  'relation-signals': 'Relation alias match',
  'child-branches': 'Child branch match',
  'descendant-aliases': 'Descendant alias match',
  'nearby-scans': 'Nearby scan fallback',
};

const normalizeSpaces = (value: string) => value.replace(/\s+/g, ' ').trim();
const cleanIdentifier = (value: string) => value.replace(/[`"'[\]]/g, '').trim();
const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === 'object' && value !== null && !Array.isArray(value);

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

export const formatPlanMetrics = (signal: ExplainSignal, variant: 'compact' | 'full' = 'compact') => {
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

const cleanExplainRelationName = (value: string) => {
  const bracketParts = Array.from(value.matchAll(/\[([^\]]+)\]/g)).map((match) => match[1]);
  if (bracketParts.length > 0) {
    const objectParts =
      bracketParts.length >= 4 && /^(?:PK|IX|AK|UQ|FK|IDX)[_A-Z0-9-]*/i.test(bracketParts[bracketParts.length - 1])
        ? bracketParts.slice(0, -1)
        : bracketParts;
    return cleanIdentifier(objectParts.join('.').replace(/:/g, '.'));
  }

  return cleanIdentifier(
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

export const getEstimateSeverity = (factor?: number) => {
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

export const getEstimateBadgeLabel = (factor?: number) => {
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
    unmappedJoinNodes: 0,
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

const createMappedJoinSignal = (
  signal: ExplainSignal,
  joinIds: string[],
  mappingMethod: ExplainSignal['mappingMethod'],
  relationAliases: string[],
) => {
  const mappingConfidence: ExplainSignal['mappingConfidence'] =
    mappingMethod === 'explicit' || mappingMethod === 'child-branches'
      ? 'high'
      : mappingMethod === 'relation-signals' || mappingMethod === 'descendant-aliases'
        ? 'medium'
        : 'low';

  return {
    ...signal,
    joinIds,
    mappingMethod,
    mappingConfidence,
    relationAliases,
  };
};

const getTableLookupKeys = (table: TableRef) => [
  table.alias.toLowerCase(),
  table.name.toLowerCase(),
  table.name.split('.').pop()?.toLowerCase() ?? table.name.toLowerCase(),
];

const createExplainContext = (tables: TableRef[]) => {
  const tableIndex = new Map<string, Set<string>>();

  tables.forEach((table) => {
    getTableLookupKeys(table)
      .map((key) => cleanIdentifier(key).toLowerCase())
      .filter(Boolean)
      .forEach((key) => {
        const aliases = tableIndex.get(key) ?? new Set<string>();
        aliases.add(table.alias);
        tableIndex.set(key, aliases);
      });
  });

  return {
    getAliasesForRelation: (relationName: string) => {
      const normalized = cleanIdentifier(relationName).toLowerCase();
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
  let unmappedJoinNodes = 0;
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
    let mappingMethod: ExplainSignal['mappingMethod'] = matchedJoinIds.length > 0 ? 'explicit' : undefined;

    if (matchedJoinIds.length === 0 && signal.relationAliases && signal.relationAliases.length >= 2) {
      const aliasSet = new Set(signal.relationAliases);
      matchedJoinIds = joins
        .filter((join) => aliasSet.has(join.sourceAlias) && aliasSet.has(join.targetAlias))
        .map((join) => join.id);
      if (matchedJoinIds.length > 0) {
        mappingMethod = 'relation-signals';
      }
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
        if (matchedJoinIds.length > 0) {
          mappingMethod = 'relation-signals';
        }
      }
    }

    if (matchedJoinIds.length === 0) {
      unmappedJoinNodes += 1;
      return;
    }

    const mappedSignal = createMappedJoinSignal(
      signal,
      matchedJoinIds,
      mappingMethod ?? 'relation-signals',
      signal.relationAliases ?? [],
    );

    matchedJoinIds.forEach((joinId) => {
      pushSignal(joinId, mappedSignal);
    });
  });

  return {
    joinSignals,
    unmappedJoinNodes,
  };
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
      Array.from(descendantAliases).forEach((alias) => pushRelationSignal(alias, signal));
    }
  });

  const { joinSignals, unmappedJoinNodes } = mapJoinSignals(items, relationSignals, joins);

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
      unmappedJoinNodes,
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
        pushRelationSignal(alias, signal);
      });
    }

    return descendantAliases;
  };

  roots.forEach((root) => {
    visit(root);
  });
  const { joinSignals, unmappedJoinNodes } = mapJoinSignals(items, relationSignals, joins);

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
      unmappedJoinNodes,
      maxCost,
      maxRows,
      estimateWarnings,
    },
  };
};

export const parseExplainInput = (input: string, tables: TableRef[], joins: JoinRef[]): ExplainSummary => {
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
  let unmappedJoinNodes = 0;

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
      pushSignal(relationSignals, alias, node.signal!);
    });

    if (node.kind !== 'join') {
      return;
    }

    const childAliasGroups = node.childIndices
      .map((childIndex) => nodes[childIndex].descendantAliases)
      .filter((aliases) => aliases.size > 0);

    let matchedJoinIds: string[] = [];
    let mappingMethod: ExplainSignal['mappingMethod'] | undefined;

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
      if (matchedJoinIds.length > 0) {
        mappingMethod = 'child-branches';
      }
    }

    if (matchedJoinIds.length === 0 && node.descendantAliases.size >= 2) {
      matchedJoinIds = joins
        .filter(
          (join) =>
            node.descendantAliases.has(join.sourceAlias) &&
            node.descendantAliases.has(join.targetAlias),
        )
        .map((join) => join.id);
      if (matchedJoinIds.length > 0) {
        mappingMethod = 'descendant-aliases';
      }
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
        if (matchedJoinIds.length > 0) {
          mappingMethod = 'nearby-scans';
        }
      }
    }

    if (matchedJoinIds.length === 0) {
      unmappedJoinNodes += 1;
      return;
    }

    const joinSignal = createMappedJoinSignal(
      node.signal,
      matchedJoinIds,
      mappingMethod ?? 'descendant-aliases',
      Array.from(node.descendantAliases),
    );

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
      unmappedJoinNodes,
      maxCost,
      maxRows,
      estimateWarnings,
    },
  };
};

export const getStrongestExplainSignal = (signals: ExplainSignal[] | undefined) => {
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

export const formatExplainMapping = (signal: ExplainSignal) =>
  signal.mappingMethod
    ? `${explainMappingLabel[signal.mappingMethod]}${signal.mappingConfidence ? ` · ${signal.mappingConfidence} confidence` : ''}`
    : '';

export { createEmptyExplainSummary };
