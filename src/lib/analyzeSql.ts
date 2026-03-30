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

const escapeDotLabel = (value: string) =>
  value
    .replace(/\\/g, '\\\\')
    .replace(/"/g, '\\"')
    .replace(/\r?\n/g, '\\n');

const wrapDotLabel = (value: string, width = 46) => {
  const normalized = value.replace(/\s+/g, ' ').trim();
  if (normalized.length <= width) {
    return normalized;
  }

  const chunks: string[] = [];
  let current = '';

  normalized.split(' ').forEach((word) => {
    const next = current ? `${current} ${word}` : word;
    if (next.length > width && current) {
      chunks.push(current);
      current = word;
      return;
    }
    current = next;
  });

  if (current) {
    chunks.push(current);
  }

  return chunks.join('\\n');
};

const stripComments = (input: string) =>
  input
    .replace(/\/\*[\s\S]*?\*\//g, ' ')
    .replace(/^\s*--.*$/gm, ' ')
    .trim();

const maskComments = (input: string) =>
  input
    .replace(/\/\*[\s\S]*?\*\//g, (match) => match.replace(/[^\n]/g, ' '))
    .replace(/--.*$/gm, (match) => match.replace(/[^\n]/g, ' '));

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

const getLineAndColumn = (input: string, index: number) => {
  const safeIndex = Math.max(0, Math.min(index, input.length));
  let line = 1;
  let column = 1;

  for (let cursor = 0; cursor < safeIndex; cursor += 1) {
    if (input[cursor] === '\n') {
      line += 1;
      column = 1;
    } else {
      column += 1;
    }
  }

  return { line, column };
};

const getDiagnosticExcerpt = (input: string, lineNumber: number) => {
  const lines = input.split(/\r?\n/);
  const line = lines[lineNumber - 1] ?? '';
  return line.trim() || ' ';
};

const createDiagnostic = (
  input: string,
  index: number,
  title: string,
  message: string,
  hint: string,
  severity: SqlDiagnosticSeverity = 'error',
): SqlDiagnostic => {
  const location = getLineAndColumn(input, index);

  return {
    severity,
    title,
    message,
    hint,
    index,
    line: location.line,
    column: location.column,
    excerpt: getDiagnosticExcerpt(input, location.line),
  };
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

const findBlockingSyntaxDiagnostic = (sourceInput: string, input: string): SqlDiagnostic | null => {
  let depth = 0;
  let singleQuote = false;
  let doubleQuote = false;

  for (let index = 0; index < input.length; index += 1) {
    const char = input[index];
    const prev = input[index - 1];

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
      if (depth === 0) {
        return createDiagnostic(
          sourceInput,
          index,
          'Unexpected closing parenthesis',
          'This query closes a parenthesis that was never opened.',
          'Remove the extra `)` or add the missing opening `(` earlier in the statement.',
        );
      }

      depth -= 1;
    }
  }

  if (singleQuote) {
    return createDiagnostic(
      sourceInput,
      input.length - 1,
      'Unclosed string literal',
      'A single-quoted string was opened but never closed.',
      'Add the missing closing quote or escape embedded quotes inside the string.',
    );
  }

  if (doubleQuote) {
    return createDiagnostic(
      sourceInput,
      input.length - 1,
      'Unclosed quoted identifier',
      'A double-quoted identifier was opened but never closed.',
      'Add the missing closing double quote around the identifier.',
    );
  }

  if (depth > 0) {
    return createDiagnostic(
      sourceInput,
      input.length - 1,
      'Missing closing parenthesis',
      'One or more opening parentheses were not closed before the end of the query.',
      'Close the pending subquery or function call with `)` before continuing.',
    );
  }

  return null;
};

const findTrailingClauseDiagnostic = (sourceInput: string, input: string): SqlDiagnostic | null => {
  const trimmed = input.trimEnd();
  if (!trimmed) {
    return null;
  }

  const clausePatterns = [
    {
      regex: /\bselect\s*$/i,
      title: 'Incomplete SELECT clause',
      message: 'The query ends right after SELECT, so there is no column list to parse.',
      hint: 'Add one or more columns or expressions after SELECT.',
    },
    {
      regex: /\bfrom\s*$/i,
      title: 'Incomplete FROM clause',
      message: 'The query ends right after FROM, so there is no source table to graph.',
      hint: 'Add a table, view, or subquery after FROM.',
    },
    {
      regex: /\bjoin\s*$/i,
      title: 'Incomplete JOIN clause',
      message: 'The query ends right after JOIN, so the joined table is missing.',
      hint: 'Add the table name after JOIN, then include the ON condition if needed.',
    },
    {
      regex: /\bon\s*$/i,
      title: 'Incomplete ON condition',
      message: 'The query ends right after ON, so the join predicate is missing.',
      hint: 'Add the join condition, for example `ON child.parent_id = parent.id`.',
    },
    {
      regex: /\bwhere\s*$/i,
      title: 'Incomplete WHERE clause',
      message: 'The query ends right after WHERE, so there is no filter expression to evaluate.',
      hint: "Add a predicate after WHERE, such as `status = 'paid'`.",
    },
    {
      regex: /\bgroup\s+by\s*$/i,
      title: 'Incomplete GROUP BY clause',
      message: 'The query ends right after GROUP BY, so there are no grouping columns listed.',
      hint: 'Add one or more grouping expressions after GROUP BY.',
    },
    {
      regex: /\border\s+by\s*$/i,
      title: 'Incomplete ORDER BY clause',
      message: 'The query ends right after ORDER BY, so there is no sort expression to apply.',
      hint: 'Add one or more columns or expressions after ORDER BY.',
    },
    {
      regex: /\blimit\s*$/i,
      title: 'Incomplete LIMIT clause',
      message: 'The query ends right after LIMIT, so the row cap is missing.',
      hint: 'Add a numeric row limit after LIMIT.',
    },
    {
      regex: /\b(?:and|or)\s*$/i,
      title: 'Dangling boolean operator',
      message: 'The query ends with AND/OR, which means the predicate after it is missing.',
      hint: 'Remove the trailing operator or add the remaining condition.',
    },
    {
      regex: /(?:=|<>|!=|<=|>=|<|>)\s*$/i,
      title: 'Incomplete comparison',
      message: 'The query ends with a comparison operator but no value on the right-hand side.',
      hint: 'Add the missing literal, parameter, or column reference after the operator.',
    },
  ] as const;

  for (const pattern of clausePatterns) {
    const match = pattern.regex.exec(trimmed);
    if (match && match.index !== undefined) {
      return createDiagnostic(sourceInput, match.index, pattern.title, pattern.message, pattern.hint);
    }
  }

  return null;
};

const findJoinDiagnostic = (sourceInput: string, input: string, fromIndex: number, nextClauseIndexes: number[]) => {
  const clauseEnd = nextClauseIndexes.filter((value) => value > fromIndex).sort((a, b) => a - b)[0] ?? input.length;
  const fromBody = input.slice(fromIndex + 4, clauseEnd);
  const visibleOffset = fromBody.search(/\S/);
  const normalizedFromBody = visibleOffset === -1 ? '' : fromBody.slice(visibleOffset);

  if (!normalizedFromBody) {
    return null;
  }

  const joinRegex = /\b(left|right|inner|full outer|full|cross)?\s*join\b/gi;
  const clauseBoundaries = /\b(?:left|right|inner|full outer|full|cross)?\s*join\b|\bwhere\b|\bgroup\s+by\b|\border\s+by\b|\blimit\b|;/gi;
  let match = joinRegex.exec(normalizedFromBody);

  while (match) {
    const segmentStart = match.index;
    clauseBoundaries.lastIndex = segmentStart + match[0].length;
    const nextBoundary = clauseBoundaries.exec(normalizedFromBody);
    const segmentEnd = nextBoundary?.index ?? normalizedFromBody.length;
    const segment = normalizedFromBody.slice(segmentStart, segmentEnd);
    const absoluteIndex = fromIndex + 4 + visibleOffset + segmentStart;

    if (!/\bjoin\s+\S+/i.test(segment)) {
      return createDiagnostic(
        sourceInput,
        absoluteIndex,
        'Missing joined table',
        'A JOIN keyword is present, but the table or subquery name after it is missing.',
        'Add the table you want to join before writing the ON condition.',
      );
    }

    if (!/\bcross\s+join\b/i.test(segment)) {
      const onMatch = /\bon\b/i.exec(segment);
      if (!onMatch) {
        return createDiagnostic(
          sourceInput,
          absoluteIndex,
          'JOIN is missing an ON condition',
          'A non-CROSS JOIN appears without an ON predicate, so the relationship is ambiguous.',
          'Add an ON clause, for example `JOIN customers c ON c.id = orders.customer_id`.',
        );
      }

      const condition = segment.slice(onMatch.index + onMatch[0].length).trim();
      if (!condition) {
        return createDiagnostic(
          sourceInput,
          absoluteIndex + onMatch.index,
          'JOIN condition is incomplete',
          'The JOIN includes ON, but the predicate after it is empty.',
          'Finish the ON expression with the two sides of the join.',
        );
      }
    }

    match = joinRegex.exec(normalizedFromBody);
  }

  return null;
};

const sliceClause = (sql: string, start: number, keyword: string, endCandidates: readonly number[]) => {
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

export const diagnoseSqlInput = (sqlInput: string): SqlDiagnostic[] => {
  const maskedInput = maskComments(sqlInput);
  const trimmedInput = maskedInput.trim();

  if (!trimmedInput) {
    return [];
  }

  const blockingSyntaxDiagnostic = findBlockingSyntaxDiagnostic(sqlInput, maskedInput);
  if (blockingSyntaxDiagnostic) {
    return [blockingSyntaxDiagnostic];
  }

  const selectIndex = findTopLevelKeyword(maskedInput, 'select');
  if (selectIndex === -1) {
    const firstTokenIndex = maskedInput.search(/\S/);
    return [
      createDiagnostic(
        sqlInput,
        firstTokenIndex === -1 ? 0 : firstTokenIndex,
        'No SELECT statement found',
        'Queryviz currently visualizes SELECT-based SQL, but no top-level SELECT was detected here.',
        'Paste a SELECT query or finish the statement before trying to graph it.',
        'warning',
      ),
    ];
  }

  const ranges = getClauseRanges(maskedInput);
  if (!ranges) {
    return [];
  }

  if (ranges.fromIndex !== -1) {
    const selectBody = sliceClause(maskedInput, ranges.selectIndex, 'select', [ranges.fromIndex]);
    if (!selectBody.trim()) {
      return [
        createDiagnostic(
          sqlInput,
          ranges.selectIndex,
          'SELECT list is empty',
          'The query reaches FROM immediately after SELECT, so no columns or expressions were provided.',
          'Add the columns or expressions you want to project before FROM.',
        ),
      ];
    }
  }

  if (ranges.fromIndex !== -1) {
    const fromBody = sliceClause(maskedInput, ranges.fromIndex, 'from', [ranges.whereIndex, ranges.groupByIndex, ranges.orderByIndex, ranges.limitIndex]);
    if (!fromBody.trim()) {
      return [
        createDiagnostic(
          sqlInput,
          ranges.fromIndex,
          'FROM clause is empty',
          'The query has a FROM keyword, but there is no source table or subquery after it.',
          'Add a table, view, or subquery after FROM.',
        ),
      ];
    }
  }

  const clauseChecks = [
    {
      start: ranges.whereIndex,
      keyword: 'where',
      title: 'WHERE clause is empty',
      message: 'The query has a WHERE keyword, but no predicate follows it.',
      hint: 'Add a filter expression after WHERE.',
      endCandidates: [ranges.groupByIndex, ranges.orderByIndex, ranges.limitIndex],
    },
    {
      start: ranges.groupByIndex,
      keyword: 'group by',
      title: 'GROUP BY clause is empty',
      message: 'The query has a GROUP BY keyword, but no grouping columns follow it.',
      hint: 'Add the grouping expressions after GROUP BY.',
      endCandidates: [ranges.orderByIndex, ranges.limitIndex],
    },
    {
      start: ranges.orderByIndex,
      keyword: 'order by',
      title: 'ORDER BY clause is empty',
      message: 'The query has an ORDER BY keyword, but no sort expression follows it.',
      hint: 'Add one or more columns or expressions after ORDER BY.',
      endCandidates: [ranges.limitIndex],
    },
    {
      start: ranges.limitIndex,
      keyword: 'limit',
      title: 'LIMIT clause is empty',
      message: 'The query has a LIMIT keyword, but the row limit itself is missing.',
      hint: 'Add a numeric value after LIMIT.',
      endCandidates: [],
    },
  ] as const;

  for (const clause of clauseChecks) {
    if (clause.start !== -1 && !sliceClause(maskedInput, clause.start, clause.keyword, clause.endCandidates).trim()) {
      return [createDiagnostic(sqlInput, clause.start, clause.title, clause.message, clause.hint)];
    }
  }

  if (ranges.fromIndex !== -1) {
    const joinDiagnostic = findJoinDiagnostic(sqlInput, maskedInput, ranges.fromIndex, [
      ranges.whereIndex,
      ranges.groupByIndex,
      ranges.orderByIndex,
      ranges.limitIndex,
    ]);
    if (joinDiagnostic) {
      return [joinDiagnostic];
    }
  }

  const trailingClauseDiagnostic = findTrailingClauseDiagnostic(sqlInput, maskedInput);
  if (trailingClauseDiagnostic) {
    return [trailingClauseDiagnostic];
  }

  return [];
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

export const buildGraphvizDot = (analysis: SqlAnalysis) => {
  const lines: string[] = [
    'digraph Queryviz {',
    '  rankdir=LR;',
    '  graph [pad="0.3", nodesep="0.6", ranksep="1.0"];',
    '  node [shape=box, style="rounded", fontname="Helvetica"];',
    '  edge [fontname="Helvetica"];',
    '',
  ];

  analysis.tables.forEach((table) => {
    const role = table.role === 'source' ? 'SOURCE' : 'JOIN';
    const label = escapeDotLabel(`${table.name}\\nalias: ${table.alias}\\n${role}`);
    lines.push(`  "${table.alias}" [label="${label}"];`);
  });

  if (analysis.tables.length > 0 && analysis.joins.length === 0) {
    lines.push('');
    lines.push('  // No joins detected in the selected statement.');
  }

  analysis.joins.forEach((join) => {
    const label = escapeDotLabel(`${join.type}\\n${wrapDotLabel(join.condition)}`);
    lines.push(`  "${join.sourceAlias}" -> "${join.targetAlias}" [label="${label}"];`);
  });

  lines.push('}');
  return lines.join('\n');
};
