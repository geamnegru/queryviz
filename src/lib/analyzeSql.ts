export type Severity = 'high' | 'medium' | 'low';

export interface ColumnRef {
  expression: string;
  alias?: string;
}

export interface TableRef {
  id: string;
  name: string;
  alias: string;
  role: 'source' | 'join';
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

export interface SqlAnalysis {
  statementCount: number;
  analyzedStatementIndex: number;
  analyzedStatement: string;
  normalizedSql: string;
  columns: ColumnRef[];
  tables: TableRef[];
  joins: JoinRef[];
  filters: string[];
  groupBy: string[];
  orderBy: string[];
  limit?: string;
  clauses: ClauseStatus[];
  flags: QueryFlag[];
  subqueryCount: number;
  hasAggregation: boolean;
  complexityScore: number;
}

const stripComments = (input: string) =>
  input
    .replace(/\/\*[\s\S]*?\*\//g, ' ')
    .replace(/^\s*--.*$/gm, ' ')
    .trim();

const splitTopLevel = (input: string, delimiter = ',') => {
  const parts: string[] = [];
  let current = '';
  let depth = 0;
  let singleQuote = false;
  let doubleQuote = false;

  for (let index = 0; index < input.length; index += 1) {
    const char = input[index];
    const prev = input[index - 1];

    if (char === "'" && prev !== '\\' && !doubleQuote) {
      singleQuote = !singleQuote;
    } else if (char === '"' && prev !== '\\' && !singleQuote) {
      doubleQuote = !doubleQuote;
    }

    if (!singleQuote && !doubleQuote) {
      if (char === '(') {
        depth += 1;
      } else if (char === ')') {
        depth = Math.max(0, depth - 1);
      }
    }

    if (char === delimiter && depth === 0 && !singleQuote && !doubleQuote) {
      if (current.trim()) {
        parts.push(current.trim());
      }
      current = '';
      continue;
    }

    current += char;
  }

  if (current.trim()) {
    parts.push(current.trim());
  }

  return parts;
};

export const extractStatements = (sqlInput: string) => {
  const cleaned = stripComments(sqlInput);
  return splitTopLevel(cleaned, ';')
    .map((statement) => statement.trim())
    .filter(Boolean)
    .filter((statement) => /\bselect\b/i.test(statement));
};

const cleanIdentifier = (value: string) => value.replace(/[`"'[\]]/g, '').trim();

const isBoundary = (char?: string) => !char || !/[a-zA-Z0-9_]/.test(char);

const findTopLevelKeyword = (sql: string, keyword: string, startIndex = 0) => {
  const needle = keyword.toLowerCase();
  let depth = 0;
  let singleQuote = false;
  let doubleQuote = false;

  for (let index = startIndex; index < sql.length; index += 1) {
    const char = sql[index];
    const prev = sql[index - 1];

    if (char === "'" && prev !== '\\' && !doubleQuote) {
      singleQuote = !singleQuote;
      continue;
    }

    if (char === '"' && prev !== '\\' && !singleQuote) {
      doubleQuote = !doubleQuote;
      continue;
    }

    if (singleQuote || doubleQuote) {
      continue;
    }

    if (char === '(') {
      depth += 1;
      continue;
    }

    if (char === ')') {
      depth = Math.max(0, depth - 1);
      continue;
    }

    if (depth !== 0) {
      continue;
    }

    if (sql.slice(index, index + needle.length).toLowerCase() === needle) {
      const before = sql[index - 1];
      const after = sql[index + needle.length];
      if (isBoundary(before) && isBoundary(after)) {
        return index;
      }
    }
  }

  return -1;
};

const getClauseRanges = (sql: string) => {
  const selectIndex = findTopLevelKeyword(sql, 'select');
  if (selectIndex === -1) {
    return null;
  }

  const fromIndex = findTopLevelKeyword(sql, 'from', selectIndex + 6);
  const whereIndex = findTopLevelKeyword(sql, 'where', fromIndex + 4);
  const groupByIndex = findTopLevelKeyword(sql, 'group by', fromIndex + 4);
  const orderByIndex = findTopLevelKeyword(sql, 'order by', fromIndex + 4);
  const limitIndex = findTopLevelKeyword(sql, 'limit', fromIndex + 4);

  return {
    selectIndex,
    fromIndex,
    whereIndex,
    groupByIndex,
    orderByIndex,
    limitIndex,
  };
};

const sliceClause = (sql: string, start: number, keyword: string, endCandidates: number[]) => {
  if (start === -1) {
    return '';
  }

  const clauseStart = start + keyword.length;
  const clauseEnd = endCandidates.filter((value) => value > start).sort((a, b) => a - b)[0] ?? sql.length;
  return sql.slice(clauseStart, clauseEnd).trim();
};

const createColumns = (selectClause: string): ColumnRef[] =>
  splitTopLevel(selectClause).map((raw) => {
    const aliasMatch = raw.match(/\s+as\s+([a-zA-Z_][\w$]*)$/i);
    const inlineAliasMatch = raw.match(/(.+?)\s+([a-zA-Z_][\w$]*)$/);
    let alias: string | undefined;

    if (aliasMatch) {
      alias = aliasMatch[1];
    } else if (
      inlineAliasMatch &&
      !/[().]/.test(inlineAliasMatch[2]) &&
      !/\b(case|when|then|else|end)\b/i.test(raw)
    ) {
      alias = inlineAliasMatch[2];
    }

    return {
      expression: raw,
      alias,
    };
  });

const createFilters = (whereClause: string) => {
  if (!whereClause) {
    return [];
  }

  return whereClause
    .split(/\s+(?:and|or)\s+/i)
    .map((part) => part.trim())
    .filter(Boolean);
};

const dedupeByAlias = (tables: TableRef[]) => {
  const seen = new Set<string>();
  return tables.filter((table) => {
    const key = table.alias.toLowerCase();
    if (seen.has(key)) {
      return false;
    }
    seen.add(key);
    return true;
  });
};

const buildFlags = (
  sql: string,
  columns: ColumnRef[],
  joins: JoinRef[],
  filters: string[],
  orderBy: string[],
  limit?: string,
) => {
  const flags: QueryFlag[] = [];

  if (columns.some((column) => /\*/.test(column.expression))) {
    flags.push({
      severity: 'high',
      title: 'Wildcard select',
      description: 'Using SELECT * can pull extra columns, increase payload size, and hide expensive scans.',
    });
  }

  if (joins.length >= 4) {
    flags.push({
      severity: 'medium',
      title: 'Join-heavy query',
      description: 'Multiple joins increase planner complexity and make missing indexes much more painful.',
    });
  }

  if (/\bwhere\b[\s\S]*\b(lower|upper|date|cast|coalesce|substring|trim)\s*\(/i.test(sql)) {
    flags.push({
      severity: 'medium',
      title: 'Function inside WHERE',
      description: 'Wrapping filter columns in functions can prevent index usage and slow down lookups.',
    });
  }

  if (/like\s+['"]%/i.test(sql)) {
    flags.push({
      severity: 'medium',
      title: 'Leading wildcard search',
      description: 'LIKE patterns starting with % usually skip normal indexes and trigger broad scans.',
    });
  }

  if (orderBy.length > 0 && !limit) {
    flags.push({
      severity: 'low',
      title: 'ORDER BY without LIMIT',
      description: 'Sorting a large result set without a row cap can become expensive fast.',
    });
  }

  if (filters.length === 0 && !limit) {
    flags.push({
      severity: 'low',
      title: 'Wide-open result set',
      description: 'No filters or LIMIT often means the query will read more rows than intended.',
    });
  }

  if ((sql.match(/\(\s*select\b/gi) ?? []).length > 0) {
    flags.push({
      severity: 'medium',
      title: 'Subquery detected',
      description: 'Nested SELECT blocks are valid, but they usually deserve a second look for simplification.',
    });
  }

  return flags;
};

export const analyzeSql = (sqlInput: string, preferredStatementIndex?: number): SqlAnalysis => {
  const statements = extractStatements(sqlInput);
  const fallbackStatement = stripComments(sqlInput);
  const safeIndex = statements.length > 0
    ? Math.min(Math.max(preferredStatementIndex ?? statements.length - 1, 0), statements.length - 1)
    : 0;
  const analyzedStatement = statements[safeIndex] ?? fallbackStatement;
  const normalizedSql = analyzedStatement.replace(/\s+/g, ' ').trim();
  const ranges = getClauseRanges(normalizedSql);

  const selectClause = ranges
    ? sliceClause(normalizedSql, ranges.selectIndex, 'select', [ranges.fromIndex])
    : '';
  const fromClause = ranges
    ? sliceClause(normalizedSql, ranges.fromIndex, 'from', [ranges.whereIndex, ranges.groupByIndex, ranges.orderByIndex, ranges.limitIndex])
    : '';
  const whereClause = ranges
    ? sliceClause(normalizedSql, ranges.whereIndex, 'where', [ranges.groupByIndex, ranges.orderByIndex, ranges.limitIndex])
    : '';
  const groupByClause = ranges
    ? sliceClause(normalizedSql, ranges.groupByIndex, 'group by', [ranges.orderByIndex, ranges.limitIndex])
    : '';
  const orderByClause = ranges
    ? sliceClause(normalizedSql, ranges.orderByIndex, 'order by', [ranges.limitIndex])
    : '';
  const limitClause = ranges ? sliceClause(normalizedSql, ranges.limitIndex, 'limit', []) : '';

  const columns = createColumns(selectClause);
  const tables: TableRef[] = [];
  const joins: JoinRef[] = [];

  const sourceMatch = fromClause.match(/^([a-zA-Z0-9_."`[\]]+)(?:\s+(?:as\s+)?([a-zA-Z_][\w$]*))?/i);
  if (sourceMatch) {
    const name = cleanIdentifier(sourceMatch[1]);
    const alias = cleanIdentifier(sourceMatch[2] ?? sourceMatch[1].split('.').pop() ?? sourceMatch[1]);
    tables.push({
      id: alias,
      name,
      alias,
      role: 'source',
    });
  }

  const joinRegex =
    /\b(left|right|inner|full outer|full|cross)?\s*join\s+([a-zA-Z0-9_."`[\]]+)(?:\s+(?:as\s+)?([a-zA-Z_][\w$]*))?\s+on\s+([\s\S]+?)(?=\b(?:left|right|inner|full outer|full|cross)?\s*join\b|$)/gi;

  let joinMatch = joinRegex.exec(fromClause);
  while (joinMatch) {
    const tableName = cleanIdentifier(joinMatch[2]);
    const alias = cleanIdentifier(joinMatch[3] ?? joinMatch[2].split('.').pop() ?? joinMatch[2]);
    const condition = joinMatch[4].trim();
    const aliasesInCondition = Array.from(condition.matchAll(/([a-zA-Z_][\w$]*)\./g)).map((match) => match[1]);
    const sourceAlias =
      aliasesInCondition.find((candidate) => candidate !== alias) ?? tables[tables.length - 1]?.alias ?? alias;

    if (!tables.some((table) => table.alias.toLowerCase() === alias.toLowerCase())) {
      tables.push({
        id: alias,
        name: tableName,
        alias,
        role: 'join',
      });
    }

    if (!joins.some((join) => join.alias.toLowerCase() === alias.toLowerCase() && join.condition === condition)) {
      joins.push({
        id: `${sourceAlias}-${alias}-${joins.length}`,
        type: (joinMatch[1] ?? 'join').toUpperCase(),
        tableName,
        alias,
        condition,
        sourceAlias,
        targetAlias: alias,
      });
    }

    joinMatch = joinRegex.exec(fromClause);
  }

  const uniqueTables = dedupeByAlias(tables);
  const filters = createFilters(whereClause);
  const groupBy = splitTopLevel(groupByClause);
  const orderBy = splitTopLevel(orderByClause);
  const hasAggregation = /\b(count|sum|avg|min|max)\s*\(/i.test(selectClause);
  const subqueryCount = (normalizedSql.match(/\(\s*select\b/gi) ?? []).length;
  const flags = buildFlags(normalizedSql, columns, joins, filters, orderBy, limitClause || undefined);

  const clauses: ClauseStatus[] = [
    { label: 'SELECT', present: Boolean(selectClause), detail: selectClause || 'No select list detected' },
    { label: 'FROM', present: Boolean(fromClause), detail: fromClause || 'No source table detected' },
    { label: 'WHERE', present: Boolean(whereClause), detail: whereClause || 'No filters' },
    { label: 'GROUP BY', present: Boolean(groupByClause), detail: groupByClause || 'No grouping' },
    { label: 'ORDER BY', present: Boolean(orderByClause), detail: orderByClause || 'No sorting' },
    { label: 'LIMIT', present: Boolean(limitClause), detail: limitClause || 'No row cap' },
  ];

  const complexityScore = Math.min(
    100,
    24 +
      uniqueTables.length * 11 +
      joins.length * 8 +
      filters.length * 5 +
      groupBy.length * 5 +
      orderBy.length * 4 +
      subqueryCount * 10 +
      (hasAggregation ? 7 : 0),
  );

  return {
    statementCount: statements.length || (normalizedSql ? 1 : 0),
    analyzedStatementIndex: safeIndex,
    analyzedStatement,
    normalizedSql,
    columns,
    tables: uniqueTables,
    joins,
    filters,
    groupBy,
    orderBy,
    limit: limitClause || undefined,
    clauses,
    flags,
    subqueryCount,
    hasAggregation,
    complexityScore,
  };
};
