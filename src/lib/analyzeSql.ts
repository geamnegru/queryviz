import type {
  ClauseRanges,
  ClauseStatus,
  ColumnRef,
  DerivedRelation,
  JoinRef,
  QueryFlag,
  SqlAnalysis,
  SqlDiagnostic,
  SqlDiagnosticSeverity,
  SqlDialect,
  SqlDialectDetection,
  StatementEnvelope,
  TableRef,
} from './types';

export const SUPPORTED_SQL_DIALECTS = [
  'postgres',
  'mysql',
  'mariadb',
  'sqlite',
  'bigquery',
  'sqlserver',
  'oracle',
  'snowflake',
  'duckdb',
  'redshift',
  'trino',
] as const satisfies readonly SqlDialect[];

const DIALECTS_WITH_QUALIFY = new Set<SqlDialect>(['bigquery', 'snowflake', 'redshift']);
const DIALECTS_WITH_WINDOW_CLAUSE = new Set<SqlDialect>([
  'postgres',
  'mysql',
  'mariadb',
  'sqlite',
  'bigquery',
  'snowflake',
  'duckdb',
  'redshift',
  'trino',
]);
const DIALECTS_WITH_FETCH = new Set<SqlDialect>([
  'postgres',
  'sqlserver',
  'oracle',
  'snowflake',
  'duckdb',
  'redshift',
  'trino',
]);
const DIALECTS_WITH_TOP = new Set<SqlDialect>(['sqlserver']);

const normalizeSpaces = (value: string) => value.replace(/\s+/g, ' ').trim();

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

const getClauseKeywordMap = (dialect: SqlDialect) => ({
  qualify: DIALECTS_WITH_QUALIFY.has(dialect) ? 'qualify' : '',
  window: DIALECTS_WITH_WINDOW_CLAUSE.has(dialect) ? 'window' : '',
  fetch: DIALECTS_WITH_FETCH.has(dialect) ? 'fetch' : '',
});

const DIALECT_DETECTION_RULES: Array<{
  dialect: SqlDialect;
  label: string;
  score: number;
  test: RegExp;
}> = [
  { dialect: 'sqlserver', label: 'TOP', score: 3, test: /\btop\s*(?:\(\s*\d+\s*\)|\d+)/i },
  { dialect: 'sqlserver', label: 'WITH (NOLOCK)', score: 4, test: /\bwith\s*\(\s*nolock\b/i },
  { dialect: 'sqlserver', label: 'APPLY', score: 4, test: /\b(?:cross|outer)\s+apply\b/i },
  { dialect: 'sqlserver', label: '[] identifiers', score: 3, test: /\[[^\]]+\]/ },
  { dialect: 'sqlserver', label: 'GETDATE', score: 2, test: /\bgetdate\s*\(/i },
  { dialect: 'sqlserver', label: 'DATEADD', score: 2, test: /\bdateadd\s*\(/i },
  { dialect: 'oracle', label: 'ROWNUM', score: 4, test: /\brownum\b/i },
  { dialect: 'oracle', label: 'CONNECT BY', score: 5, test: /\bconnect\s+by\b/i },
  { dialect: 'oracle', label: 'START WITH', score: 4, test: /\bstart\s+with\b/i },
  { dialect: 'oracle', label: 'NVL', score: 3, test: /\bnvl\s*\(/i },
  { dialect: 'oracle', label: 'SYSDATE', score: 3, test: /\bsysdate\b/i },
  { dialect: 'oracle', label: 'DUAL', score: 3, test: /\bfrom\s+dual\b/i },
  { dialect: 'bigquery', label: 'project.dataset backticks', score: 5, test: /`[^`]+\.[^`]+\.[^`]+`/ },
  { dialect: 'bigquery', label: 'UNNEST', score: 3, test: /\bunnest\s*\(/i },
  { dialect: 'bigquery', label: 'SAFE_CAST', score: 3, test: /\bsafe_cast\s*\(/i },
  { dialect: 'bigquery', label: 'STRUCT', score: 2, test: /\bstruct\s*</i },
  { dialect: 'snowflake', label: 'IFF', score: 3, test: /\biff\s*\(/i },
  { dialect: 'snowflake', label: 'LATERAL FLATTEN', score: 5, test: /\blateral\s+flatten\s*\(/i },
  { dialect: 'snowflake', label: 'QUALIFY', score: 2, test: /\bqualify\b/i },
  { dialect: 'snowflake', label: 'SAMPLE BERNOULLI', score: 4, test: /\bsample\s+bernoulli\b/i },
  { dialect: 'redshift', label: 'DISTKEY', score: 5, test: /\bdistkey\b/i },
  { dialect: 'redshift', label: 'SORTKEY', score: 5, test: /\bsortkey\b/i },
  { dialect: 'redshift', label: 'DISTSTYLE', score: 5, test: /\bdiststyle\b/i },
  { dialect: 'redshift', label: 'QUALIFY', score: 2, test: /\bqualify\b/i },
  { dialect: 'duckdb', label: 'READ_PARQUET', score: 5, test: /\bread_parquet\s*\(/i },
  { dialect: 'duckdb', label: 'READ_CSV', score: 5, test: /\bread_csv(?:_auto)?\s*\(/i },
  { dialect: 'duckdb', label: 'READ_JSON', score: 5, test: /\bread_json(?:_auto)?\s*\(/i },
  { dialect: 'duckdb', label: 'LIST_VALUE', score: 3, test: /\blist_value\s*\(/i },
  { dialect: 'trino', label: 'WITH ORDINALITY', score: 5, test: /\bwith\s+ordinality\b/i },
  { dialect: 'trino', label: 'APPROX_DISTINCT', score: 3, test: /\bapprox_distinct\s*\(/i },
  { dialect: 'trino', label: 'CROSS JOIN UNNEST', score: 3, test: /\bcross\s+join\s+unnest\s*\(/i },
  { dialect: 'trino', label: 'DATE_PARSE', score: 3, test: /\bdate_parse\s*\(/i },
  { dialect: 'mariadb', label: 'STRAIGHT_JOIN', score: 3, test: /\bstraight_join\b/i },
  { dialect: 'mysql', label: 'DATE_SUB', score: 2, test: /\bdate_sub\s*\(/i },
  { dialect: 'mysql', label: 'CURDATE', score: 2, test: /\bcurdate\s*\(/i },
  { dialect: 'mysql', label: 'backticks', score: 1, test: /`[^`]+`/ },
  { dialect: 'mariadb', label: 'backticks', score: 1, test: /`[^`]+`/ },
  { dialect: 'sqlite', label: 'strftime', score: 4, test: /\bstrftime\s*\(/i },
  { dialect: 'sqlite', label: "date('now')", score: 3, test: /\bdate\s*\(\s*'now'/i },
  { dialect: 'sqlite', label: "datetime('now')", score: 3, test: /\bdatetime\s*\(\s*'now'/i },
  { dialect: 'postgres', label: 'ILIKE', score: 4, test: /\bilike\b/i },
  { dialect: 'postgres', label: ':: cast', score: 4, test: /::\s*[a-zA-Z_][\w.]*/ },
  { dialect: 'postgres', label: 'DISTINCT ON', score: 4, test: /\bdistinct\s+on\b/i },
  { dialect: 'postgres', label: 'DATE_TRUNC', score: 2, test: /\bdate_trunc\s*\(/i },
];

const readJoinToken = (input: string, startIndex: number) => {
  const slice = input.slice(startIndex);
  const decorated =
    /^(left outer|left|right outer|right|inner|full outer|full|cross|outer)\s+(join|apply)\b/i.exec(slice);
  if (decorated) {
    return {
      text: decorated[0],
      type: `${decorated[1].toUpperCase()} ${decorated[2].toUpperCase()}`,
      endIndex: startIndex + decorated[0].length,
    };
  }

  const straightJoin = /^straight_join\b/i.exec(slice);
  if (straightJoin) {
    return {
      text: straightJoin[0],
      type: 'STRAIGHT_JOIN',
      endIndex: startIndex + straightJoin[0].length,
    };
  }

  const simple = /^(join|apply)\b/i.exec(slice);
  if (simple) {
    return {
      text: simple[0],
      type: simple[1].toUpperCase(),
      endIndex: startIndex + simple[0].length,
    };
  }

  return null;
};

export const detectSqlDialect = (
  sqlInput: string,
  fallback: SqlDialect = 'postgres',
): SqlDialectDetection => {
  const maskedInput = maskComments(sqlInput);
  const trimmedInput = maskedInput.trim();

  if (!trimmedInput) {
    return {
      dialect: fallback,
      confident: false,
      evidence: [],
      score: 0,
    };
  }

  const scores = new Map<SqlDialect, number>();
  const evidenceMap = new Map<SqlDialect, string[]>();

  DIALECT_DETECTION_RULES.forEach((rule) => {
    if (!rule.test.test(maskedInput)) {
      return;
    }

    scores.set(rule.dialect, (scores.get(rule.dialect) ?? 0) + rule.score);
    const nextEvidence = evidenceMap.get(rule.dialect) ?? [];
    if (!nextEvidence.includes(rule.label)) {
      nextEvidence.push(rule.label);
      evidenceMap.set(rule.dialect, nextEvidence);
    }
  });

  const ranked = Array.from(scores.entries()).sort((left, right) => {
    if (right[1] !== left[1]) {
      return right[1] - left[1];
    }

    if (left[0] === fallback) {
      return -1;
    }

    if (right[0] === fallback) {
      return 1;
    }

    return 0;
  });

  const [bestDialect, bestScore] = ranked[0] ?? [fallback, 0];
  const secondScore = ranked[1]?.[1] ?? 0;
  const confident = bestScore >= 4 || (bestScore >= 2 && bestScore > secondScore);

  return {
    dialect: bestScore > 0 ? bestDialect : fallback,
    confident,
    evidence: evidenceMap.get(bestDialect)?.slice(0, 3) ?? [],
    score: bestScore,
  };
};

const joinAllowsImplicitCondition = (joinType: string) =>
  /\b(cross join|cross apply|outer apply|apply)\b/i.test(joinType);

const skipRelationDecorators = (input: string, startIndex: number) => {
  let index = skipWhitespace(input, startIndex);

  while (index < input.length) {
    if (input.slice(index, index + 4).toLowerCase() === 'with' && /\s*\(/.test(input.slice(index + 4, index + 10))) {
      const parenIndex = input.indexOf('(', index + 4);
      if (parenIndex !== -1) {
        const balanced = readBalancedParenthesis(input, parenIndex);
        if (balanced) {
          index = skipWhitespace(input, balanced.endIndex + 1);
          continue;
        }
      }
    }

    if (input.slice(index, index + 11).toLowerCase() === 'tablesample' && /\s*\(/.test(input.slice(index + 11, index + 17))) {
      const parenIndex = input.indexOf('(', index + 11);
      if (parenIndex !== -1) {
        const balanced = readBalancedParenthesis(input, parenIndex);
        if (balanced) {
          index = skipWhitespace(input, balanced.endIndex + 1);
          continue;
        }
      }
    }

    break;
  }

  return index;
};

const splitTopLevel = (input: string, delimiter = ',') => {
  const parts: string[] = [];
  let current = '';
  let depth = 0;
  let bracketDepth = 0;
  let singleQuote = false;
  let doubleQuote = false;
  let backtickQuote = false;

  for (let index = 0; index < input.length; index += 1) {
    const char = input[index];
    const prev = input[index - 1];

    if (char === "'" && prev !== '\\' && !doubleQuote && !backtickQuote) {
      singleQuote = !singleQuote;
    } else if (char === '"' && prev !== '\\' && !singleQuote && !backtickQuote) {
      doubleQuote = !doubleQuote;
    } else if (char === '`' && prev !== '\\' && !singleQuote && !doubleQuote) {
      backtickQuote = !backtickQuote;
    }

    if (!singleQuote && !doubleQuote && !backtickQuote) {
      if (char === '(') {
        depth += 1;
      } else if (char === ')') {
        depth = Math.max(0, depth - 1);
      } else if (char === '[') {
        bracketDepth += 1;
      } else if (char === ']') {
        bracketDepth = Math.max(0, bracketDepth - 1);
      }
    }

    if (char === delimiter && depth === 0 && bracketDepth === 0 && !singleQuote && !doubleQuote && !backtickQuote) {
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

export const extractStatements = (sqlInput: string, dialect: SqlDialect = 'postgres') => {
  void dialect;
  const cleaned = stripComments(sqlInput);
  return splitTopLevel(cleaned, ';')
    .map((statement) => statement.trim())
    .filter(Boolean);
};

const cleanIdentifier = (value: string) => value.replace(/[`"'[\]]/g, '').trim();

const normalizeRelationName = (value: string) => normalizeSpaces(value).replace(/\s+/g, ' ').trim();

const inferSpecialRelationType = (relationName: string) => {
  const normalized = relationName.toLowerCase();

  if (normalized.startsWith('unnest(')) {
    return 'unnest' as const;
  }

  if (normalized.startsWith('flatten(') || normalized.startsWith('lateral flatten(') || normalized.includes(' lateral flatten(')) {
    return 'flatten' as const;
  }

  if (normalized.startsWith('read_parquet(') || normalized.startsWith('read_csv(') || normalized.startsWith('read_csv_auto(') || normalized.startsWith('read_json(') || normalized.startsWith('read_json_auto(')) {
    return 'external' as const;
  }

  if (/^(#|@)/.test(normalized)) {
    return 'temp' as const;
  }

  if (/\w+\s*\(/.test(normalized)) {
    return 'function' as const;
  }

  return undefined;
};

const skipWhitespace = (input: string, startIndex: number) => {
  let index = startIndex;
  while (index < input.length && /\s/.test(input[index])) {
    index += 1;
  }
  return index;
};

const readBalancedParenthesis = (input: string, startIndex: number) => {
  if (input[startIndex] !== '(') {
    return null;
  }

  let depth = 0;
  let singleQuote = false;
  let doubleQuote = false;
  let backtickQuote = false;
  let bracketDepth = 0;

  for (let index = startIndex; index < input.length; index += 1) {
    const char = input[index];
    const prev = input[index - 1];

    if (char === "'" && prev !== '\\' && !doubleQuote && !backtickQuote) {
      singleQuote = !singleQuote;
      continue;
    }

    if (char === '"' && prev !== '\\' && !singleQuote && !backtickQuote) {
      doubleQuote = !doubleQuote;
      continue;
    }

    if (char === '`' && prev !== '\\' && !singleQuote && !doubleQuote) {
      backtickQuote = !backtickQuote;
      continue;
    }

    if (!singleQuote && !doubleQuote && !backtickQuote) {
      if (char === '[') {
        bracketDepth += 1;
        continue;
      }

      if (char === ']' && bracketDepth > 0) {
        bracketDepth -= 1;
        continue;
      }
    }

    if (singleQuote || doubleQuote || backtickQuote || bracketDepth > 0) {
      continue;
    }

    if (char === '(') {
      depth += 1;
      continue;
    }

    if (char === ')') {
      depth -= 1;
      if (depth === 0) {
        return {
          content: input.slice(startIndex + 1, index),
          endIndex: index,
        };
      }
    }
  }

  return null;
};

const readRelationIdentifier = (input: string, startIndex: number) => {
  let index = startIndex;
  let value = '';

  while (index < input.length) {
    const char = input[index];

    if (/[a-zA-Z0-9_$#@.-]/.test(char)) {
      value += char;
      index += 1;
      continue;
    }

    if (char === '"' || char === '`' || char === '[') {
      const closing = char === '[' ? ']' : char;
      let endIndex = index + 1;

      while (endIndex < input.length && input[endIndex] !== closing) {
        endIndex += 1;
      }

      value += input.slice(index, Math.min(endIndex + 1, input.length));
      index = Math.min(endIndex + 1, input.length);
      continue;
    }

    break;
  }

  if (!value.trim()) {
    return null;
  }

  return {
    value: cleanIdentifier(value),
    endIndex: index,
  };
};

const readAliasToken = (input: string, startIndex: number) => {
  let index = skipWhitespace(input, startIndex);

  if (input.slice(index, index + 2).toLowerCase() === 'as' && isBoundary(input[index + 2])) {
    index = skipWhitespace(input, index + 2);
  }

  const match = /^([a-zA-Z_][\w$]*)/.exec(input.slice(index));
  if (!match) {
    return {
      alias: '',
      endIndex: startIndex,
    };
  }

  const aliasCandidate = cleanIdentifier(match[1]);
  if (
    /^(select|from|where|group|having|qualify|window|order|limit|offset|fetch|join|inner|left|right|full|cross|outer|straight_join|on|using|set|when|into|update|merge|values)$/i.test(
      aliasCandidate,
    )
  ) {
    return {
      alias: '',
      endIndex: startIndex,
    };
  }

  return {
    alias: aliasCandidate,
    endIndex: index + match[0].length,
  };
};

const isBoundary = (char?: string) => !char || !/[a-zA-Z0-9_]/.test(char);

const findTopLevelKeyword = (sql: string, keyword: string, startIndex = 0) => {
  const needle = keyword.toLowerCase();
  let depth = 0;
  let bracketDepth = 0;
  let singleQuote = false;
  let doubleQuote = false;
  let backtickQuote = false;

  for (let index = startIndex; index < sql.length; index += 1) {
    const char = sql[index];
    const prev = sql[index - 1];

    if (char === "'" && prev !== '\\' && !doubleQuote && !backtickQuote) {
      singleQuote = !singleQuote;
      continue;
    }

    if (char === '"' && prev !== '\\' && !singleQuote && !backtickQuote) {
      doubleQuote = !doubleQuote;
      continue;
    }

    if (char === '`' && prev !== '\\' && !singleQuote && !doubleQuote) {
      backtickQuote = !backtickQuote;
      continue;
    }

    if (!singleQuote && !doubleQuote && !backtickQuote) {
      if (char === '[') {
        bracketDepth += 1;
        continue;
      }

      if (char === ']' && bracketDepth > 0) {
        bracketDepth -= 1;
        continue;
      }
    }

    if (singleQuote || doubleQuote || backtickQuote || bracketDepth > 0) {
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

const findTopLevelJoinBoundary = (sql: string, startIndex: number) => {
  let depth = 0;
  let bracketDepth = 0;
  let singleQuote = false;
  let doubleQuote = false;
  let backtickQuote = false;

  for (let index = startIndex; index < sql.length; index += 1) {
    const char = sql[index];
    const prev = sql[index - 1];

    if (char === "'" && prev !== '\\' && !doubleQuote && !backtickQuote) {
      singleQuote = !singleQuote;
      continue;
    }

    if (char === '"' && prev !== '\\' && !singleQuote && !backtickQuote) {
      doubleQuote = !doubleQuote;
      continue;
    }

    if (char === '`' && prev !== '\\' && !singleQuote && !doubleQuote) {
      backtickQuote = !backtickQuote;
      continue;
    }

    if (!singleQuote && !doubleQuote && !backtickQuote) {
      if (char === '[') {
        bracketDepth += 1;
        continue;
      }

      if (char === ']' && bracketDepth > 0) {
        bracketDepth -= 1;
        continue;
      }
    }

    if (singleQuote || doubleQuote || backtickQuote || bracketDepth > 0) {
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

    const joinMatch = readJoinToken(sql, index);
    if (joinMatch && isBoundary(sql[index - 1])) {
      return index;
    }
  }

  return sql.length;
};

const parseCteDefinitions = (sql: string) => {
  const initial = skipWhitespace(sql, 0);
  if (sql.slice(initial, initial + 4).toLowerCase() !== 'with' || !isBoundary(sql[initial + 4])) {
    return [];
  }

  let index = skipWhitespace(sql, initial + 4);
  if (sql.slice(index, index + 9).toLowerCase() === 'recursive' && isBoundary(sql[index + 9])) {
    index = skipWhitespace(sql, index + 9);
  }

  const definitions: Array<{ id: string; name: string; body: string }> = [];

  while (index < sql.length) {
    const nameMatch = /^([a-zA-Z_][\w$]*)/.exec(sql.slice(index));
    if (!nameMatch) {
      break;
    }

    const name = cleanIdentifier(nameMatch[1]);
    index = skipWhitespace(sql, index + nameMatch[0].length);

    if (sql[index] === '(') {
      const columnList = readBalancedParenthesis(sql, index);
      if (!columnList) {
        break;
      }
      index = skipWhitespace(sql, columnList.endIndex + 1);
    }

    if (sql.slice(index, index + 2).toLowerCase() !== 'as' || !isBoundary(sql[index + 2])) {
      break;
    }

    index = skipWhitespace(sql, index + 2);
    const bodyRange = readBalancedParenthesis(sql, index);
    if (!bodyRange) {
      break;
    }

    definitions.push({
      id: `cte:${name.toLowerCase()}`,
      name,
      body: bodyRange.content.trim(),
    });

    index = skipWhitespace(sql, bodyRange.endIndex + 1);
    if (sql[index] === ',') {
      index = skipWhitespace(sql, index + 1);
      continue;
    }

    break;
  }

  return definitions;
};

const findSelectEnvelope = (sql: string): StatementEnvelope => ({
  statementType: 'select',
  statementLabel: 'SELECT',
  graphSql: sql,
  mode: 'select',
});

const findUnknownEnvelope = (sql: string): StatementEnvelope => ({
  statementType: 'unknown',
  statementLabel: 'STATEMENT',
  graphSql: sql,
  mode: 'select',
});

const detectStatementEnvelope = (sql: string): StatementEnvelope => {
  const trimmed = sql.trim();
  if (!trimmed) {
    return findSelectEnvelope(sql);
  }

  const selectIndex = findTopLevelKeyword(trimmed, 'select');
  const mergeIndex = findTopLevelKeyword(trimmed, 'merge');
  const updateIndex = findTopLevelKeyword(trimmed, 'update');

  if (selectIndex === 0) {
    return findSelectEnvelope(trimmed);
  }

  const insertMatch = /^\s*insert\s+into\s+/i.exec(trimmed);
  if (insertMatch) {
    const targetStart = insertMatch[0].length;
    const targetRelation = readRelationIdentifier(trimmed, targetStart);
    if (targetRelation && selectIndex !== -1 && selectIndex > targetRelation.endIndex) {
      return {
        statementType: 'insert-select',
        statementLabel: 'INSERT INTO',
        writeTarget: normalizeRelationName(targetRelation.value),
        graphSql: trimmed.slice(selectIndex).trim(),
        mode: 'select',
      };
    }
  }

  const createViewMatch = /^\s*create(?:\s+or\s+replace)?\s+view\s+/i.exec(trimmed);
  if (createViewMatch) {
    const targetStart = createViewMatch[0].length;
    const targetRelation = readRelationIdentifier(trimmed, targetStart);
    const asIndex = findTopLevelKeyword(trimmed, 'as', targetRelation?.endIndex ?? targetStart);
    const nestedSelectIndex = asIndex === -1 ? -1 : findTopLevelKeyword(trimmed, 'select', asIndex + 2);
    if (targetRelation && nestedSelectIndex !== -1) {
      return {
        statementType: 'create-view',
        statementLabel: 'CREATE VIEW',
        writeTarget: normalizeRelationName(targetRelation.value),
        graphSql: trimmed.slice(nestedSelectIndex).trim(),
        mode: 'select',
      };
    }
  }

  const createTableMatch = /^\s*create(?:\s+(?:or\s+replace)\s+)?(?:temporary\s+|temp\s+)?table\s+/i.exec(trimmed);
  if (createTableMatch) {
    const targetStart = createTableMatch[0].length;
    const targetRelation = readRelationIdentifier(trimmed, targetStart);
    const asIndex = findTopLevelKeyword(trimmed, 'as', targetRelation?.endIndex ?? targetStart);
    const nestedSelectIndex = asIndex === -1 ? -1 : findTopLevelKeyword(trimmed, 'select', asIndex + 2);
    if (targetRelation && nestedSelectIndex !== -1) {
      return {
        statementType: 'create-table-as',
        statementLabel: 'CREATE TABLE AS',
        writeTarget: normalizeRelationName(targetRelation.value),
        graphSql: trimmed.slice(nestedSelectIndex).trim(),
        mode: 'select',
      };
    }
  }

  if (updateIndex === 0) {
    const targetStart = skipWhitespace(trimmed, updateIndex + 6);
    const targetRelation = readRelationIdentifier(trimmed, targetStart);
    if (targetRelation) {
      const targetAliasResult = readAliasToken(trimmed, targetRelation.endIndex);
      const setIndex = findTopLevelKeyword(trimmed, 'set', targetRelation.endIndex);
      const fromIndex = findTopLevelKeyword(trimmed, 'from', Math.max(targetRelation.endIndex, setIndex));
      const whereIndex = findTopLevelKeyword(trimmed, 'where', Math.max(targetRelation.endIndex, setIndex));
      const updateSetClause =
        setIndex === -1
          ? ''
          : trimmed
              .slice(
                setIndex + 3,
                [fromIndex, whereIndex].filter((value) => value > setIndex).sort((left, right) => left - right)[0] ?? trimmed.length,
              )
              .trim();

      return {
        statementType: 'update-from',
        statementLabel: 'UPDATE',
        writeTarget: normalizeRelationName(targetRelation.value),
        writeTargetAlias: targetAliasResult.alias || cleanIdentifier(targetRelation.value.split('.').pop() ?? targetRelation.value),
        graphSql: trimmed,
        mode: 'update-from',
        updateSetClause,
      };
    }
  }

  if (mergeIndex === 0) {
    const intoIndex = findTopLevelKeyword(trimmed, 'into', mergeIndex + 5);
    if (intoIndex !== -1) {
      const targetStart = skipWhitespace(trimmed, intoIndex + 4);
      const targetRelation = readRelationIdentifier(trimmed, targetStart);
      if (targetRelation) {
        const targetAliasResult = readAliasToken(trimmed, targetRelation.endIndex);
        const usingIndex = findTopLevelKeyword(trimmed, 'using', targetRelation.endIndex);
        const onIndex = findTopLevelKeyword(trimmed, 'on', usingIndex + 5);
        const mergeUsingClause =
          usingIndex === -1
            ? ''
            : trimmed.slice(usingIndex + 5, onIndex === -1 ? trimmed.length : onIndex).trim();
        const mergeOnClause =
          onIndex === -1
            ? ''
            : trimmed
                .slice(
                  onIndex + 2,
                  [
                    findTopLevelKeyword(trimmed, 'when matched', onIndex + 2),
                    findTopLevelKeyword(trimmed, 'when not matched', onIndex + 2),
                  ]
                    .filter((value) => value > onIndex)
                    .sort((left, right) => left - right)[0] ?? trimmed.length,
                )
                .trim();

        return {
          statementType: 'merge',
          statementLabel: 'MERGE',
          writeTarget: normalizeRelationName(targetRelation.value),
          writeTargetAlias: targetAliasResult.alias || cleanIdentifier(targetRelation.value.split('.').pop() ?? targetRelation.value),
          graphSql: trimmed,
          mode: 'merge',
          mergeUsingClause,
          mergeOnClause,
        };
      }
    }
  }

  return selectIndex !== -1 ? findSelectEnvelope(trimmed.slice(selectIndex)) : findUnknownEnvelope(trimmed);
};

const getClauseRanges = (sql: string, dialect: SqlDialect = 'postgres'): ClauseRanges | null => {
  const keywords = getClauseKeywordMap(dialect);
  const selectIndex = findTopLevelKeyword(sql, 'select');
  if (selectIndex === -1) {
    return null;
  }

  const fromIndex = findTopLevelKeyword(sql, 'from', selectIndex + 6);
  const whereIndex = findTopLevelKeyword(sql, 'where', fromIndex + 4);
  const groupByIndex = findTopLevelKeyword(sql, 'group by', fromIndex + 4);
  const havingIndex = findTopLevelKeyword(sql, 'having', fromIndex + 4);
  const qualifyIndex = keywords.qualify ? findTopLevelKeyword(sql, keywords.qualify, fromIndex + 4) : -1;
  const windowIndex = keywords.window ? findTopLevelKeyword(sql, keywords.window, fromIndex + 4) : -1;
  const orderByIndex = findTopLevelKeyword(sql, 'order by', fromIndex + 4);
  const limitIndex = findTopLevelKeyword(sql, 'limit', fromIndex + 4);
  const offsetIndex = findTopLevelKeyword(sql, 'offset', fromIndex + 4);
  const fetchIndex = keywords.fetch ? findTopLevelKeyword(sql, keywords.fetch, fromIndex + 4) : -1;

  return {
    selectIndex,
    fromIndex,
    whereIndex,
    groupByIndex,
    havingIndex,
    qualifyIndex,
    windowIndex,
    orderByIndex,
    limitIndex,
    offsetIndex,
    fetchIndex,
  };
};

const findBlockingSyntaxDiagnostic = (sourceInput: string, input: string): SqlDiagnostic | null => {
  let depth = 0;
  let bracketDepth = 0;
  let singleQuote = false;
  let doubleQuote = false;
  let backtickQuote = false;

  for (let index = 0; index < input.length; index += 1) {
    const char = input[index];
    const prev = input[index - 1];

    if (char === "'" && prev !== '\\' && !doubleQuote && !backtickQuote) {
      singleQuote = !singleQuote;
      continue;
    }

    if (char === '"' && prev !== '\\' && !singleQuote && !backtickQuote) {
      doubleQuote = !doubleQuote;
      continue;
    }

    if (char === '`' && prev !== '\\' && !singleQuote && !doubleQuote) {
      backtickQuote = !backtickQuote;
      continue;
    }

    if (!singleQuote && !doubleQuote && !backtickQuote) {
      if (char === '[') {
        bracketDepth += 1;
        continue;
      }

      if (char === ']' && bracketDepth > 0) {
        bracketDepth -= 1;
        continue;
      }
    }

    if (singleQuote || doubleQuote || backtickQuote || bracketDepth > 0) {
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

  if (backtickQuote) {
    return createDiagnostic(
      sourceInput,
      input.length - 1,
      'Unclosed backtick identifier',
      'A backtick-quoted identifier was opened but never closed.',
      'Add the missing closing backtick around the identifier.',
    );
  }

  if (bracketDepth > 0) {
    return createDiagnostic(
      sourceInput,
      input.length - 1,
      'Unclosed bracketed identifier or array',
      'A square bracket expression was opened but never closed.',
      'Close the pending bracket expression with `]` before continuing.',
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
      regex: /\btop(?:\s*\(\s*|\s+)\s*$/i,
      title: 'Incomplete TOP clause',
      message: 'The query starts a TOP clause but does not include the row cap or select list after it.',
      hint: 'Finish the TOP clause, for example `TOP 10` or `TOP (10)`, then list the projected columns.',
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
      regex: /\bhaving\s*$/i,
      title: 'Incomplete HAVING clause',
      message: 'The query ends right after HAVING, so there is no aggregate filter to evaluate.',
      hint: 'Add a HAVING predicate such as `HAVING COUNT(*) > 1`.',
    },
    {
      regex: /\bqualify\s*$/i,
      title: 'Incomplete QUALIFY clause',
      message: 'The query ends right after QUALIFY, so there is no window filter to apply.',
      hint: 'Add a QUALIFY predicate such as `QUALIFY ROW_NUMBER() = 1`.',
    },
    {
      regex: /\bwindow\s*$/i,
      title: 'Incomplete WINDOW clause',
      message: 'The query ends right after WINDOW, so there is no named window definition to parse.',
      hint: 'Add a named window definition after WINDOW or remove the clause.',
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
      regex: /\boffset\s*$/i,
      title: 'Incomplete OFFSET clause',
      message: 'The query ends right after OFFSET, so the starting row position is missing.',
      hint: 'Add a numeric offset after OFFSET.',
    },
    {
      regex: /\bfetch\s*(?:first|next)?\s*$/i,
      title: 'Incomplete FETCH clause',
      message: 'The query starts a FETCH clause but does not include the row count.',
      hint: 'Finish the FETCH clause, for example `FETCH FIRST 20 ROWS ONLY`.',
    },
    {
      regex: /\busing\s*$/i,
      title: 'Incomplete USING clause',
      message: 'The query ends right after USING, so the join key list is missing.',
      hint: 'Add the shared join column list, for example `USING (order_id)`.',
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

  const clauseBoundaryPattern = /\bwhere\b|\bgroup\s+by\b|\bhaving\b|\bqualify\b|\bwindow\b|\border\s+by\b|\blimit\b|\boffset\b|\bfetch\b|;/gi;
  let scanIndex = 0;

  while (scanIndex < normalizedFromBody.length) {
    const nextJoinStart = findTopLevelJoinBoundary(normalizedFromBody, scanIndex);
    if (nextJoinStart >= normalizedFromBody.length) {
      break;
    }

    const joinToken = readJoinToken(normalizedFromBody, nextJoinStart);
    if (!joinToken) {
      break;
    }

    clauseBoundaryPattern.lastIndex = joinToken.endIndex;
    const nextClauseBoundary = clauseBoundaryPattern.exec(normalizedFromBody);
    const nextJoinBoundary = findTopLevelJoinBoundary(normalizedFromBody, joinToken.endIndex);
    const segmentEnd = Math.min(nextJoinBoundary, nextClauseBoundary?.index ?? normalizedFromBody.length);
    const segment = normalizedFromBody.slice(nextJoinStart, segmentEnd);
    const absoluteIndex = fromIndex + 4 + visibleOffset + nextJoinStart;

    if (!/\b(?:join|apply|straight_join)\s+\S+/i.test(segment)) {
      return createDiagnostic(
        sourceInput,
        absoluteIndex,
        'Missing joined table',
        'A JOIN keyword is present, but the table or subquery name after it is missing.',
        'Add the table you want to join before writing the ON condition.',
      );
    }

    if (!joinAllowsImplicitCondition(joinToken.type)) {
      const onMatch = /\bon\b/i.exec(segment);
      const usingMatch = /\busing\b/i.exec(segment);
      if (!onMatch && !usingMatch) {
        return createDiagnostic(
          sourceInput,
          absoluteIndex,
          'JOIN is missing an ON condition',
          'A non-CROSS JOIN appears without an ON or USING predicate, so the relationship is ambiguous.',
          'Add an ON clause such as `JOIN customers c ON c.id = orders.customer_id` or a USING list like `USING (customer_id)`.',
        );
      }

      const conditionStart = onMatch ?? usingMatch;
      const keyword = onMatch ? 'ON' : 'USING';
      const condition = segment.slice((conditionStart?.index ?? 0) + keyword.length).trim();
      if (!condition) {
        return createDiagnostic(
          sourceInput,
          absoluteIndex + (conditionStart?.index ?? 0),
          `${keyword} condition is incomplete`,
          `The JOIN includes ${keyword}, but the predicate after it is empty.`,
          keyword === 'ON'
            ? 'Finish the ON expression with the two sides of the join.'
            : 'Finish the USING column list with one or more shared keys.',
        );
      }
    }

    scanIndex = joinToken.endIndex;
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

const extractSelectMetadata = (selectClause: string, dialect: SqlDialect) => {
  if (!DIALECTS_WITH_TOP.has(dialect)) {
    return { selectClause, topClause: '' };
  }

  const trimmed = selectClause.trim();
  const prefixMatch = /^(all|distinct)\b\s*/i.exec(trimmed);
  const prefix = prefixMatch?.[0] ?? '';
  const remainder = prefix ? trimmed.slice(prefix.length) : trimmed;
  const topMatch = /^top\s*(\(\s*[^)]+\s*\)|\d+)(?:\s+percent)?(?:\s+with\s+ties)?\s+/i.exec(remainder);

  if (!topMatch) {
    return { selectClause, topClause: '' };
  }

  return {
    selectClause: `${prefix}${remainder.slice(topMatch[0].length)}`.trim(),
    topClause: normalizeSpaces(topMatch[0].trim()),
  };
};

const extractOracleRowCap = (whereClause: string) => {
  const rownumMatch = /\brownum\s*(<=|<|=)\s*(\d+)/i.exec(whereClause);
  if (!rownumMatch) {
    return '';
  }

  return `ROWNUM ${rownumMatch[1]} ${rownumMatch[2]}`;
};

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
  rowCap?: string,
  dialect: SqlDialect = 'postgres',
  statementType: SqlAnalysis['statementType'] = 'select',
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

  if (/\bwhere\b[\s\S]*\b(lower|upper|date|datetime|date_trunc|date_format|strftime|cast|coalesce|ifnull|nvl|substring|substr|trim|extract|regexp_contains|trunc|to_char|to_date|convert|dateadd|datediff)\s*\(/i.test(sql)) {
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

  if (orderBy.length > 0 && !rowCap) {
    flags.push({
      severity: 'low',
      title: 'ORDER BY without row cap',
      description: 'Sorting a large result set without LIMIT, TOP, FETCH, or an equivalent row cap can become expensive fast.',
    });
  }

  if (filters.length === 0 && !rowCap) {
    flags.push({
      severity: 'low',
      title: 'Wide-open result set',
      description: 'No filters or row cap often means the query will read more rows than intended.',
    });
  }

  if ((sql.match(/\(\s*select\b/gi) ?? []).length > 0) {
    flags.push({
      severity: 'medium',
      title: 'Subquery detected',
      description: 'Nested SELECT blocks are valid, but they usually deserve a second look for simplification.',
    });
  }

  if ((dialect === 'mysql' || dialect === 'mariadb') && /\bstraight_join\b/i.test(sql)) {
    flags.push({
      severity: 'low',
      title: 'STRAIGHT_JOIN hint',
      description: 'STRAIGHT_JOIN forces join order. Re-check the access path with EXPLAIN before keeping it.',
    });
  }

  if (dialect === 'sqlserver' && /\bwith\s*\(\s*nolock\b/i.test(sql)) {
    flags.push({
      severity: 'medium',
      title: 'NOLOCK hint',
      description: 'NOLOCK can read uncommitted rows and duplicates. Validate whether dirty reads are acceptable here.',
    });
  }

  if (dialect === 'sqlserver' && /\b(?:cross|outer)\s+apply\b/i.test(sql)) {
    flags.push({
      severity: 'medium',
      title: 'APPLY operator',
      description: 'APPLY can execute like a row-by-row nested loop. Check row counts and look for repeated scans in SHOWPLAN.',
    });
  }

  if (dialect === 'sqlserver' && /\btop\s*\(/i.test(sql) && !/\border\s+by\b/i.test(sql)) {
    flags.push({
      severity: 'low',
      title: 'TOP without ORDER BY',
      description: 'TOP without ORDER BY is nondeterministic. Add an order if downstream code expects stable rows.',
    });
  }

  if (dialect === 'oracle' && /\brownum\b/i.test(sql)) {
    flags.push({
      severity: 'low',
      title: 'ROWNUM filter',
      description: 'ROWNUM is applied before the final ORDER BY in many Oracle plans. Double-check that the row cap matches intent.',
    });
  }

  if (dialect === 'oracle' && /\bconnect\s+by\b/i.test(sql)) {
    flags.push({
      severity: 'medium',
      title: 'CONNECT BY recursion',
      description: 'CONNECT BY trees can fan out quickly. Inspect row growth and sort cost before assuming the hierarchy is cheap.',
    });
  }

  if (DIALECTS_WITH_QUALIFY.has(dialect) && /\bqualify\b/i.test(sql)) {
    flags.push({
      severity: 'low',
      title: 'QUALIFY filter',
      description: 'QUALIFY can be perfect for windowed dedupe, but it is worth checking the sort and partition cost in the plan.',
    });
  }

  if ((dialect === 'bigquery' || dialect === 'trino') && (sql.match(/\bunnest\s*\(/gi) ?? []).length > 1) {
    flags.push({
      severity: 'medium',
      title: 'Repeated UNNEST',
      description: 'Multiple UNNEST operations can explode row counts quickly. Validate fanout before downstream aggregation.',
    });
  }

  if (/`[^`]*\*[^`]*`/.test(sql)) {
    flags.push({
      severity: 'high',
      title: 'Wildcard table scan',
      description: 'Wildcard tables can scan many shards at once. Prune with _TABLE_SUFFIX or a tighter source pattern before running it broadly.',
    });
  }

  if (dialect === 'bigquery' && /\bqualify\b/i.test(sql) && /\bover\s*\([^)]*\border\s+by\b/i.test(sql)) {
    flags.push({
      severity: 'low',
      title: 'Windowed QUALIFY',
      description: 'QUALIFY with ORDER BY usually introduces a window sort. Check bytes processed and partition width in the plan.',
    });
  }

  if (dialect === 'snowflake' && /\bflatten\s*\(/i.test(sql)) {
    flags.push({
      severity: 'medium',
      title: 'FLATTEN relation',
      description: 'FLATTEN can multiply rows heavily. Check the lateral join shape and prune nested arrays early if possible.',
    });
  }

  if (dialect === 'snowflake' && /\bqualify\b/i.test(sql) && /\bover\s*\([^)]*\border\s+by\b/i.test(sql)) {
    flags.push({
      severity: 'low',
      title: 'Windowed QUALIFY',
      description: 'QUALIFY with ORDER BY usually triggers a window sort. Inspect repartitioning and sort width before calling it cheap.',
    });
  }

  if (dialect === 'duckdb' && /\bread_(?:parquet|csv|json)(?:_auto)?\s*\(/i.test(sql)) {
    flags.push({
      severity: 'low',
      title: 'External file scan',
      description: 'File-backed scans are great for exploration, but push filters down early to avoid reading unnecessary data.',
    });
  }

  if (dialect === 'redshift' && /\bjoin\b/i.test(sql) && !/\bwhere\b/i.test(sql)) {
    flags.push({
      severity: 'low',
      title: 'Unfiltered warehouse join',
      description: 'Redshift joins without selective filters can trigger distribution-heavy scans. Check DS_BCAST / DS_DIST steps in EXPLAIN.',
    });
  }

  if (dialect === 'trino' && /\bcross\s+join\s+unnest\b/i.test(sql)) {
    flags.push({
      severity: 'medium',
      title: 'CROSS JOIN UNNEST',
      description: 'CROSS JOIN UNNEST can expand rows aggressively. Confirm downstream aggregates still match business grain.',
    });
  }

  if (statementType !== 'select' && statementType !== 'unknown') {
    flags.push({
      severity: 'low',
      title: 'Write statement',
      description: 'This statement writes data. Review the read-side graph and predicates before running it in production.',
    });
  }

  return flags;
};

function summarizeDerivedRelation(
  body: string,
  depth = 0,
  dialect: SqlDialect = 'postgres',
): Omit<DerivedRelation, 'id' | 'name' | 'alias' | 'kind' | 'body'> {
  if (depth >= 2) {
    const normalizedBody = body.replace(/\s+/g, ' ').trim();
    const dependencyMatches = Array.from(
      normalizedBody.matchAll(/\b(?:from|join)\s+([a-zA-Z0-9_."`[\]]+)/gi),
    ).map((match) => cleanIdentifier(match[1]));

    return {
      sourceCount: dependencyMatches.length > 0 ? 1 : 0,
      joinCount: (normalizedBody.match(/\bjoin\b/gi) ?? []).length,
      subqueryCount: (normalizedBody.match(/\(\s*select\b/gi) ?? []).length,
      hasAggregation: /\b(count|sum|avg|min|max)\s*\(/i.test(normalizedBody),
      dependencies: Array.from(new Set(dependencyMatches)),
      flags: [],
    };
  }

  const nested = analyzeSql(body, 0, dialect);
  return {
    sourceCount: nested.tables.filter((table) => table.role === 'source').length || (nested.tables.length > 0 ? 1 : 0),
    joinCount: nested.joins.length,
    subqueryCount: nested.subqueryCount,
    hasAggregation: nested.hasAggregation,
    dependencies: Array.from(new Set(nested.tables.map((table) => table.name))),
    flags: nested.flags.map((flag) => flag.title),
  };
}

const parseRelationTerm = (
  input: string,
  startIndex: number,
  role: 'source' | 'join',
  cteDefinitions: Map<string, { id: string; name: string; body: string }>,
  derivedRelations: DerivedRelation[],
  dialect: SqlDialect = 'postgres',
  depth = 0,
) => {
  let index = skipWhitespace(input, startIndex);

  if (input[index] === '(') {
    const bodyRange = readBalancedParenthesis(input, index);
    if (!bodyRange) {
      return null;
    }

    index = skipWhitespace(input, bodyRange.endIndex + 1);
    const aliasResult = readAliasToken(input, index);
    const alias = aliasResult.alias || `subquery_${derivedRelations.filter((item) => item.kind === 'subquery').length + 1}`;
    const derivedId = `subquery:${alias.toLowerCase()}`;

    if (!derivedRelations.some((item) => item.id === derivedId)) {
      derivedRelations.push({
        id: derivedId,
        name: 'Subquery',
        alias,
        kind: 'subquery',
        body: bodyRange.content.trim(),
        ...summarizeDerivedRelation(bodyRange.content.trim(), depth + 1, dialect),
      });
    }

    return {
      table: {
        id: alias,
        name: 'Subquery',
        alias,
        role,
        kind: 'subquery' as const,
        derivedId,
      },
      nextIndex: aliasResult.endIndex,
    };
  }

  const identifier = readRelationIdentifier(input, index);
  if (!identifier) {
    return null;
  }

  let baseIdentifier = identifier;
  let relationPrefix = '';
  if (identifier.value.toLowerCase() === 'lateral') {
    const lateralIndex = skipWhitespace(input, identifier.endIndex);
    const lateralIdentifier = readRelationIdentifier(input, lateralIndex);
    if (lateralIdentifier) {
      baseIdentifier = lateralIdentifier;
      relationPrefix = 'LATERAL ';
    }
  }

  let relationName = `${relationPrefix}${baseIdentifier.value}`.trim();
  let relationEndIndex = baseIdentifier.endIndex;
  const functionStartIndex = skipWhitespace(input, relationEndIndex);

  if (input[functionStartIndex] === '(') {
    const functionArgs = readBalancedParenthesis(input, functionStartIndex);
    if (functionArgs) {
      relationName = `${relationPrefix}${baseIdentifier.value}(${functionArgs.content.trim()})`.trim();
      relationEndIndex = functionArgs.endIndex + 1;
    }
  }

  let aliasResult = readAliasToken(input, relationEndIndex);
  let nextIndex = relationEndIndex;

  if (aliasResult.alias) {
    nextIndex = skipRelationDecorators(input, aliasResult.endIndex);
  } else {
    const decoratedIndex = skipRelationDecorators(input, relationEndIndex);
    if (decoratedIndex !== relationEndIndex) {
      const aliasAfterDecorators = readAliasToken(input, decoratedIndex);
      if (aliasAfterDecorators.alias) {
        aliasResult = aliasAfterDecorators;
        nextIndex = skipRelationDecorators(input, aliasAfterDecorators.endIndex);
      } else {
        nextIndex = decoratedIndex;
      }
    }
  }

  const fallbackAlias = baseIdentifier.value.split('.').pop() ?? baseIdentifier.value;
  const alias = aliasResult.alias || cleanIdentifier(fallbackAlias);
  const cteDefinition =
    relationName === baseIdentifier.value ? cteDefinitions.get(baseIdentifier.value.toLowerCase()) : undefined;

  return {
    table: {
      id: alias,
      name: relationName,
      alias,
      role,
      kind: cteDefinition ? ('cte' as const) : ('table' as const),
      derivedId: cteDefinition?.id,
      specialType: inferSpecialRelationType(relationName),
    },
    nextIndex: nextIndex || relationEndIndex,
  };
};

const createWarningDiagnostic = (
  input: string,
  index: number,
  title: string,
  message: string,
  hint: string,
) => createDiagnostic(input, index, title, message, hint, 'warning');

const inferDialectHintDiagnostics = (
  sqlInput: string,
  maskedInput: string,
  dialect: SqlDialect,
) => {
  const firstTokenIndex = Math.max(0, maskedInput.search(/\S/));
  const hints: SqlDiagnostic[] = [];
  const pushHint = (title: string, message: string, hint: string) => {
    if (hints.some((item) => item.title === title)) {
      return;
    }

    hints.push(createWarningDiagnostic(sqlInput, firstTokenIndex, title, message, hint));
  };

  if (
    dialect !== 'sqlserver' &&
    (/\[[^\]]+\]/.test(maskedInput) ||
      /\btop\s*(?:\(\s*\d+\s*\)|\d+)/i.test(maskedInput) ||
      /\bwith\s*\(\s*nolock\b/i.test(maskedInput) ||
      /\b(?:cross|outer)\s+apply\b/i.test(maskedInput) ||
      /(^|\W)(?:#|@)[a-zA-Z_][\w$#@]*/.test(maskedInput))
  ) {
    pushHint(
      'This looks like SQL Server syntax',
      'The query uses T-SQL patterns such as bracketed identifiers, TOP, NOLOCK, APPLY, or temp/table variables.',
      'Queryviz can auto-detect SQL Server here. If the result still looks off, paste a little more of the query.',
    );
  }

  if (
    dialect !== 'oracle' &&
    (/\brownum\b/i.test(maskedInput) ||
      /\bconnect\s+by\b/i.test(maskedInput) ||
      /\bstart\s+with\b/i.test(maskedInput) ||
      /\bnvl\s*\(/i.test(maskedInput) ||
      /\bdecode\s*\(/i.test(maskedInput))
  ) {
    pushHint(
      'This looks like Oracle syntax',
      'The query uses Oracle-style constructs such as ROWNUM, CONNECT BY, START WITH, NVL, or DECODE.',
      'Queryviz can auto-detect Oracle here. If the result still looks off, paste a little more of the query.',
    );
  }

  if (dialect !== 'duckdb' && /\bread_(?:parquet|csv|json)\s*\(/i.test(maskedInput)) {
    pushHint(
      'This looks like DuckDB syntax',
      'The query reads files through table functions such as READ_PARQUET / READ_CSV / READ_JSON.',
      'Queryviz can auto-detect DuckDB here. If the result still looks off, paste a little more of the query.',
    );
  }

  if (
    dialect !== 'redshift' &&
    (/\bdistkey\b/i.test(maskedInput) ||
      /\bsortkey\b/i.test(maskedInput) ||
      /\bdiststyle\b/i.test(maskedInput) ||
      /\bencode\s+zstd\b/i.test(maskedInput))
  ) {
    pushHint(
      'This looks like Redshift syntax',
      'The query includes Redshift-specific warehouse hints such as DISTKEY, SORTKEY, DISTSTYLE, or ENCODE.',
      'Queryviz can auto-detect Redshift here. If the result still looks off, paste a little more of the query.',
    );
  }

  if (dialect !== 'trino' && /\bcross\s+join\s+unnest\s*\(/i.test(maskedInput)) {
    pushHint(
      'This looks like Trino syntax',
      'The query uses a CROSS JOIN UNNEST pattern that is common in Trino / Presto query plans and SQL.',
      'Queryviz can auto-detect Trino here. If the result still looks off, paste a little more of the query.',
    );
  }

  if (dialect !== 'mariadb' && dialect !== 'mysql' && /\bstraight_join\b/i.test(maskedInput)) {
    pushHint(
      'This looks like MySQL or MariaDB syntax',
      'The query uses STRAIGHT_JOIN, which is specific to MySQL-family dialects.',
      'Queryviz can auto-detect MySQL-family SQL here. If the result still looks off, paste a little more of the query.',
    );
  }

  if (!DIALECTS_WITH_QUALIFY.has(dialect) && /\bqualify\b/i.test(maskedInput)) {
    pushHint(
      'This query uses QUALIFY',
      'QUALIFY is more typical in BigQuery, Snowflake, and Redshift than in the currently selected dialect.',
      'Queryviz will usually auto-detect warehouse dialects from QUALIFY. Paste more of the query if detection is still ambiguous.',
    );
  }

  if (dialect !== 'bigquery' && /`[^`]+`/.test(maskedInput) && (/\bunnest\s*\(/i.test(maskedInput) || /\bqualify\b/i.test(maskedInput))) {
    pushHint(
      'This looks like BigQuery syntax',
      'The query mixes backtick identifiers with UNNEST or QUALIFY, which strongly suggests BigQuery.',
      'Queryviz can auto-detect BigQuery here. If the result still looks off, paste a little more of the query.',
    );
  } else if (
    dialect !== 'mysql' &&
    dialect !== 'mariadb' &&
    dialect !== 'bigquery' &&
    /`[^`]+`/.test(maskedInput)
  ) {
    pushHint(
      'This query uses backtick identifiers',
      'Backtick-quoted identifiers are more common in MySQL, MariaDB, and BigQuery than in the current dialect.',
      'Queryviz will usually auto-detect that from the rest of the query. Paste more of the query if detection is still ambiguous.',
    );
  }

  return hints.slice(0, 2);
};

export const diagnoseSqlInput = (sqlInput: string, dialect: SqlDialect = 'postgres'): SqlDiagnostic[] => {
  const maskedInput = maskComments(sqlInput);
  const trimmedInput = maskedInput.trim();

  if (!trimmedInput) {
    return [];
  }

  const dialectHints = inferDialectHintDiagnostics(sqlInput, maskedInput, dialect);
  const statementEnvelope = detectStatementEnvelope(trimmedInput);

  const blockingSyntaxDiagnostic = findBlockingSyntaxDiagnostic(sqlInput, maskedInput);
  if (blockingSyntaxDiagnostic) {
    return [blockingSyntaxDiagnostic, ...dialectHints];
  }

  const selectIndex =
    statementEnvelope.mode === 'select'
      ? findTopLevelKeyword(maskedInput, 'select')
      : statementEnvelope.statementType === 'update-from'
        ? findTopLevelKeyword(maskedInput, 'update')
        : statementEnvelope.statementType === 'merge'
          ? findTopLevelKeyword(maskedInput, 'merge')
          : findTopLevelKeyword(maskedInput, 'select');
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
      ...dialectHints,
    ];
  }

  if (statementEnvelope.statementType === 'update-from') {
    const setIndex = findTopLevelKeyword(maskedInput, 'set');
    if (setIndex === -1) {
      return [
        createDiagnostic(
          sqlInput,
          selectIndex,
          'UPDATE is missing SET',
          'The statement starts with UPDATE but never reaches a SET clause.',
          'Add the SET assignments before the FROM / WHERE predicates.',
        ),
        ...dialectHints,
      ];
    }

    if (!statementEnvelope.updateSetClause?.trim()) {
      return [
        createDiagnostic(
          sqlInput,
          setIndex,
          'SET clause is empty',
          'The UPDATE statement has a SET keyword but no assignments after it.',
          "Add one or more assignments such as `SET status = 'paid'` in SQL syntax.",
        ),
        ...dialectHints,
      ];
    }

    return dialectHints;
  }

  if (statementEnvelope.statementType === 'merge') {
    const usingIndex = findTopLevelKeyword(maskedInput, 'using');
    if (usingIndex === -1) {
      return [
        createDiagnostic(
          sqlInput,
          selectIndex,
          'MERGE is missing USING',
          'The MERGE statement needs a USING source before the ON predicate can be evaluated.',
          'Add a source relation after USING, such as a staging table or subquery.',
        ),
        ...dialectHints,
      ];
    }

    if (!statementEnvelope.mergeUsingClause?.trim()) {
      return [
        createDiagnostic(
          sqlInput,
          usingIndex,
          'USING source is empty',
          'The MERGE statement reaches USING but the source relation is missing.',
          'Add the source table or subquery after USING.',
        ),
        ...dialectHints,
      ];
    }

    const onIndex = findTopLevelKeyword(maskedInput, 'on', usingIndex + 5);
    if (onIndex === -1 || !statementEnvelope.mergeOnClause?.trim()) {
      return [
        createDiagnostic(
          sqlInput,
          onIndex === -1 ? usingIndex : onIndex,
          'MERGE is missing ON',
          'The MERGE statement needs an ON condition to match source rows to the target.',
          'Add an ON predicate such as `ON target.id = source.id`.',
        ),
        ...dialectHints,
      ];
    }

    return dialectHints;
  }

  if (statementEnvelope.statementType !== 'select') {
    return dialectHints;
  }

  const ranges = getClauseRanges(statementEnvelope.graphSql, dialect);
  if (!ranges) {
    return [];
  }

  if (ranges.fromIndex !== -1) {
    const selectBody = sliceClause(maskedInput, ranges.selectIndex, 'select', [ranges.fromIndex]);
    const selectMetadata = extractSelectMetadata(selectBody, dialect);
    if (!selectMetadata.selectClause.trim()) {
      return [
        createDiagnostic(
          sqlInput,
          ranges.selectIndex,
          'SELECT list is empty',
          'The query reaches FROM immediately after SELECT, so no columns or expressions were provided.',
          'Add the columns or expressions you want to project before FROM.',
        ),
        ...dialectHints,
      ];
    }
  }

  if (ranges.fromIndex !== -1) {
    const fromBody = sliceClause(maskedInput, ranges.fromIndex, 'from', [
      ranges.whereIndex,
      ranges.groupByIndex,
      ranges.havingIndex,
      ranges.qualifyIndex,
      ranges.windowIndex,
      ranges.orderByIndex,
      ranges.limitIndex,
      ranges.offsetIndex,
      ranges.fetchIndex,
    ]);
    if (!fromBody.trim()) {
      return [
        createDiagnostic(
          sqlInput,
          ranges.fromIndex,
          'FROM clause is empty',
          'The query has a FROM keyword, but there is no source table or subquery after it.',
          'Add a table, view, or subquery after FROM.',
        ),
        ...dialectHints,
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
      endCandidates: [ranges.groupByIndex, ranges.havingIndex, ranges.qualifyIndex, ranges.windowIndex, ranges.orderByIndex, ranges.limitIndex, ranges.offsetIndex, ranges.fetchIndex],
    },
    {
      start: ranges.groupByIndex,
      keyword: 'group by',
      title: 'GROUP BY clause is empty',
      message: 'The query has a GROUP BY keyword, but no grouping columns follow it.',
      hint: 'Add the grouping expressions after GROUP BY.',
      endCandidates: [ranges.havingIndex, ranges.qualifyIndex, ranges.windowIndex, ranges.orderByIndex, ranges.limitIndex, ranges.offsetIndex, ranges.fetchIndex],
    },
    {
      start: ranges.havingIndex,
      keyword: 'having',
      title: 'HAVING clause is empty',
      message: 'The query has a HAVING keyword, but no aggregate predicate follows it.',
      hint: 'Add a HAVING filter such as `HAVING COUNT(*) > 1`.',
      endCandidates: [ranges.qualifyIndex, ranges.windowIndex, ranges.orderByIndex, ranges.limitIndex, ranges.offsetIndex, ranges.fetchIndex],
    },
    {
      start: ranges.qualifyIndex,
      keyword: 'qualify',
      title: 'QUALIFY clause is empty',
      message: 'The query has a QUALIFY keyword, but no window predicate follows it.',
      hint: 'Add a QUALIFY predicate such as `QUALIFY ROW_NUMBER() = 1`.',
      endCandidates: [ranges.windowIndex, ranges.orderByIndex, ranges.limitIndex, ranges.offsetIndex, ranges.fetchIndex],
    },
    {
      start: ranges.windowIndex,
      keyword: 'window',
      title: 'WINDOW clause is empty',
      message: 'The query has a WINDOW keyword, but no named window definition follows it.',
      hint: 'Add one or more named windows after WINDOW.',
      endCandidates: [ranges.orderByIndex, ranges.limitIndex, ranges.offsetIndex, ranges.fetchIndex],
    },
    {
      start: ranges.orderByIndex,
      keyword: 'order by',
      title: 'ORDER BY clause is empty',
      message: 'The query has an ORDER BY keyword, but no sort expression follows it.',
      hint: 'Add one or more columns or expressions after ORDER BY.',
      endCandidates: [ranges.limitIndex, ranges.offsetIndex, ranges.fetchIndex],
    },
    {
      start: ranges.limitIndex,
      keyword: 'limit',
      title: 'LIMIT clause is empty',
      message: 'The query has a LIMIT keyword, but the row limit itself is missing.',
      hint: 'Add a numeric value after LIMIT.',
      endCandidates: [ranges.offsetIndex, ranges.fetchIndex],
    },
    {
      start: ranges.offsetIndex,
      keyword: 'offset',
      title: 'OFFSET clause is empty',
      message: 'The query has an OFFSET keyword, but the number of skipped rows is missing.',
      hint: 'Add a numeric value after OFFSET.',
      endCandidates: [ranges.fetchIndex],
    },
    {
      start: ranges.fetchIndex,
      keyword: 'fetch',
      title: 'FETCH clause is empty',
      message: 'The query has a FETCH keyword, but the number of rows to fetch is missing.',
      hint: 'Add a FETCH clause such as `FETCH FIRST 20 ROWS ONLY`.',
      endCandidates: [],
    },
  ] as const;

  for (const clause of clauseChecks) {
    if (clause.start !== -1 && !sliceClause(maskedInput, clause.start, clause.keyword, clause.endCandidates).trim()) {
      return [createDiagnostic(sqlInput, clause.start, clause.title, clause.message, clause.hint), ...dialectHints];
    }
  }

  if (ranges.fromIndex !== -1) {
    const joinDiagnostic = findJoinDiagnostic(sqlInput, maskedInput, ranges.fromIndex, [
      ranges.whereIndex,
      ranges.groupByIndex,
      ranges.havingIndex,
      ranges.qualifyIndex,
      ranges.windowIndex,
      ranges.orderByIndex,
      ranges.limitIndex,
      ranges.offsetIndex,
      ranges.fetchIndex,
    ]);
    if (joinDiagnostic) {
      return [joinDiagnostic, ...dialectHints];
    }
  }

  const trailingClauseDiagnostic = findTrailingClauseDiagnostic(sqlInput, maskedInput);
  if (trailingClauseDiagnostic) {
    return [trailingClauseDiagnostic, ...dialectHints];
  }

  return dialectHints;
};

export const analyzeSql = (
  sqlInput: string,
  preferredStatementIndex?: number,
  dialect: SqlDialect = 'postgres',
): SqlAnalysis => {
  const statements = extractStatements(sqlInput, dialect);
  const fallbackStatement = stripComments(sqlInput);
  const safeIndex = statements.length > 0
    ? Math.min(Math.max(preferredStatementIndex ?? statements.length - 1, 0), statements.length - 1)
    : 0;
  const analyzedStatement = statements[safeIndex] ?? fallbackStatement;
  const statementEnvelope = detectStatementEnvelope(analyzedStatement);
  let resolvedWriteTarget = statementEnvelope.writeTarget;
  const statementSql = analyzedStatement.replace(/\s+/g, ' ').trim();
  const normalizedSql = statementEnvelope.mode === 'select' ? statementEnvelope.graphSql.replace(/\s+/g, ' ').trim() : statementSql;
  const ranges = statementEnvelope.mode === 'select' ? getClauseRanges(normalizedSql, dialect) : null;

  let selectClause = '';
  let fromClause = '';
  let whereClause = '';
  let groupByClause = '';
  let havingClause = '';
  let qualifyClause = '';
  let windowClause = '';
  let orderByClause = '';
  let limitClause = '';
  let offsetClause = '';
  let fetchClause = '';

  if (ranges) {
    selectClause = sliceClause(normalizedSql, ranges.selectIndex, 'select', [ranges.fromIndex]);
    fromClause = sliceClause(normalizedSql, ranges.fromIndex, 'from', [
      ranges.whereIndex,
      ranges.groupByIndex,
      ranges.havingIndex,
      ranges.qualifyIndex,
      ranges.windowIndex,
      ranges.orderByIndex,
      ranges.limitIndex,
      ranges.offsetIndex,
      ranges.fetchIndex,
    ]);
    whereClause = sliceClause(normalizedSql, ranges.whereIndex, 'where', [
      ranges.groupByIndex,
      ranges.havingIndex,
      ranges.qualifyIndex,
      ranges.windowIndex,
      ranges.orderByIndex,
      ranges.limitIndex,
      ranges.offsetIndex,
      ranges.fetchIndex,
    ]);
    groupByClause = sliceClause(normalizedSql, ranges.groupByIndex, 'group by', [
      ranges.havingIndex,
      ranges.qualifyIndex,
      ranges.windowIndex,
      ranges.orderByIndex,
      ranges.limitIndex,
      ranges.offsetIndex,
      ranges.fetchIndex,
    ]);
    havingClause = sliceClause(normalizedSql, ranges.havingIndex, 'having', [
      ranges.qualifyIndex,
      ranges.windowIndex,
      ranges.orderByIndex,
      ranges.limitIndex,
      ranges.offsetIndex,
      ranges.fetchIndex,
    ]);
    qualifyClause = sliceClause(normalizedSql, ranges.qualifyIndex, 'qualify', [
      ranges.windowIndex,
      ranges.orderByIndex,
      ranges.limitIndex,
      ranges.offsetIndex,
      ranges.fetchIndex,
    ]);
    windowClause = sliceClause(normalizedSql, ranges.windowIndex, 'window', [
      ranges.orderByIndex,
      ranges.limitIndex,
      ranges.offsetIndex,
      ranges.fetchIndex,
    ]);
    orderByClause = sliceClause(normalizedSql, ranges.orderByIndex, 'order by', [ranges.limitIndex, ranges.offsetIndex, ranges.fetchIndex]);
    limitClause = sliceClause(normalizedSql, ranges.limitIndex, 'limit', [ranges.offsetIndex, ranges.fetchIndex]);
    offsetClause = sliceClause(normalizedSql, ranges.offsetIndex, 'offset', [ranges.fetchIndex]);
    fetchClause = sliceClause(normalizedSql, ranges.fetchIndex, 'fetch', []);
  } else if (statementEnvelope.mode === 'update-from') {
    const setIndex = findTopLevelKeyword(statementSql, 'set');
    const fromIndex = findTopLevelKeyword(statementSql, 'from', Math.max(0, setIndex));
    const whereIndex = findTopLevelKeyword(statementSql, 'where', Math.max(fromIndex, setIndex));
    fromClause =
      fromIndex === -1
        ? ''
        : statementSql.slice(fromIndex + 4, [whereIndex].filter((value) => value > fromIndex).sort((left, right) => left - right)[0] ?? statementSql.length).trim();
    whereClause = whereIndex === -1 ? '' : statementSql.slice(whereIndex + 5).trim();
  } else if (statementEnvelope.mode === 'merge') {
    fromClause = statementEnvelope.mergeUsingClause ?? '';
  }

  const selectMetadata = extractSelectMetadata(selectClause, dialect);
  const rownumClause = dialect === 'oracle' ? extractOracleRowCap(whereClause) : '';
  const rowCapClause = selectMetadata.topClause || fetchClause || limitClause || rownumClause;

  const columns =
    statementEnvelope.mode === 'select'
      ? createColumns(selectMetadata.selectClause)
      : [];
  const tables: TableRef[] = [];
  const joins: JoinRef[] = [];
  const cteDefinitions = parseCteDefinitions(analyzedStatement);
  const cteMap = new Map(cteDefinitions.map((definition) => [definition.name.toLowerCase(), definition]));
  const derivedRelations: DerivedRelation[] = cteDefinitions.map((definition) => ({
    id: definition.id,
    name: definition.name,
    alias: definition.name,
    kind: 'cte',
    body: definition.body,
    ...summarizeDerivedRelation(definition.body, 1, dialect),
  }));

  const parseFromRelations = (body: string) => {
    const sourceRelation = parseRelationTerm(body, 0, 'source', cteMap, derivedRelations, dialect);
    if (sourceRelation?.table) {
      tables.push(sourceRelation.table);
    }

    let scanIndex = sourceRelation?.nextIndex ?? 0;
    while (scanIndex < body.length) {
      scanIndex = skipWhitespace(body, scanIndex);
      if (scanIndex >= body.length) {
        break;
      }

      const joinMatch = readJoinToken(body, scanIndex);
      if (!joinMatch) {
        break;
      }

      const joinType = joinMatch.type;
      scanIndex = joinMatch.endIndex;

      const relation = parseRelationTerm(body, scanIndex, 'join', cteMap, derivedRelations, dialect);
      if (!relation?.table) {
        break;
      }

      const tableName = relation.table.name;
      const alias = relation.table.alias;

      if (!tables.some((table) => table.alias.toLowerCase() === alias.toLowerCase())) {
        tables.push(relation.table);
      }

      const nextJoinIndex = findTopLevelJoinBoundary(body, relation.nextIndex);
      let condition = '';

      if (!joinAllowsImplicitCondition(joinType)) {
        const onIndex = findTopLevelKeyword(body, 'on', relation.nextIndex);
        const usingIndex = findTopLevelKeyword(body, 'using', relation.nextIndex);
        const hasOn = onIndex !== -1 && onIndex < nextJoinIndex;
        const hasUsing = usingIndex !== -1 && usingIndex < nextJoinIndex;

        if (hasOn && (!hasUsing || onIndex <= usingIndex)) {
          condition = body.slice(onIndex + 2, nextJoinIndex).trim();
        } else if (hasUsing) {
          condition = `USING ${body.slice(usingIndex + 5, nextJoinIndex).trim()}`;
        }
      }

      const aliasesInCondition = Array.from(condition.matchAll(/([a-zA-Z_][\w$]*)\./g)).map((match) => match[1]);
      const sourceAlias =
        aliasesInCondition.find((candidate) => candidate !== alias) ?? tables[tables.length - 2]?.alias ?? alias;

      if (!joins.some((join) => join.alias.toLowerCase() === alias.toLowerCase() && join.condition === condition)) {
        joins.push({
          id: `${sourceAlias}-${alias}-${joins.length}`,
          type: joinType,
          tableName,
          alias,
          condition: condition || 'cross join',
          sourceAlias,
          targetAlias: alias,
        });
      }

      scanIndex = nextJoinIndex;
    }
  };

  if (statementEnvelope.mode === 'select') {
    parseFromRelations(fromClause);
  } else if (statementEnvelope.mode === 'update-from') {
    const updateStart = skipWhitespace(analyzedStatement, findTopLevelKeyword(analyzedStatement, 'update') + 6);
    const targetIdentifier = readRelationIdentifier(analyzedStatement, updateStart);
    const targetAliasToken = readAliasToken(analyzedStatement, targetIdentifier?.endIndex ?? updateStart);
    const fallbackTargetAlias = cleanIdentifier(
      statementEnvelope.writeTarget?.split('.').pop() ?? statementEnvelope.writeTarget ?? 'target',
    );
    const targetAlias = statementEnvelope.writeTargetAlias || targetAliasToken.alias || fallbackTargetAlias;

    if (resolvedWriteTarget) {
      tables.push({
        id: targetAlias,
        name: resolvedWriteTarget,
        alias: targetAlias,
        role: 'target',
        kind: 'table',
        specialType: inferSpecialRelationType(resolvedWriteTarget),
      });
    }

    parseFromRelations(fromClause);

    const mirroredSource = tables.find(
      (table) => table.role !== 'target' && table.alias.toLowerCase() === targetAlias.toLowerCase() && table.name !== targetAlias,
    );
    const targetTable = tables.find(
      (table) => table.role === 'target' && table.alias.toLowerCase() === targetAlias.toLowerCase(),
    );

    if (targetTable && mirroredSource) {
      targetTable.name = mirroredSource.name;
      targetTable.specialType = mirroredSource.specialType;
      resolvedWriteTarget = mirroredSource.name;
    }

    const linkPredicates = createFilters(whereClause).filter((filter) =>
      new RegExp(`\\b${targetAlias.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\.`, 'i').test(filter),
    );
    const seenTargetLinks = new Set<string>();

    linkPredicates.forEach((predicate) => {
      const aliases = Array.from(predicate.matchAll(/([a-zA-Z_][\w$]*)\./g)).map((match) => match[1]);
      aliases
        .filter((alias) => alias.toLowerCase() !== targetAlias.toLowerCase())
        .forEach((alias) => {
          const joinKey = `${targetAlias}->${alias}:${predicate}`;
          const relatedTable = tables.find((table) => table.alias.toLowerCase() === alias.toLowerCase());
          if (!relatedTable || seenTargetLinks.has(joinKey)) {
            return;
          }

          seenTargetLinks.add(joinKey);
          joins.push({
            id: `${targetAlias}-${alias}-${joins.length}`,
            type: 'UPDATE TARGET',
            tableName: relatedTable.name,
            alias,
            condition: predicate,
            sourceAlias: targetAlias,
            targetAlias: alias,
          });
        });
    });
  } else if (statementEnvelope.mode === 'merge') {
    const mergeIndex = findTopLevelKeyword(analyzedStatement, 'merge');
    const intoIndex = findTopLevelKeyword(analyzedStatement, 'into', mergeIndex + 5);
    const targetStart = skipWhitespace(analyzedStatement, intoIndex + 4);
    const targetIdentifier = readRelationIdentifier(analyzedStatement, targetStart);
    const targetAliasToken = readAliasToken(analyzedStatement, targetIdentifier?.endIndex ?? targetStart);
    const fallbackTargetAlias = cleanIdentifier(
      statementEnvelope.writeTarget?.split('.').pop() ?? statementEnvelope.writeTarget ?? 'target',
    );
    const targetAlias = statementEnvelope.writeTargetAlias || targetAliasToken.alias || fallbackTargetAlias;

    if (resolvedWriteTarget) {
      tables.push({
        id: targetAlias,
        name: resolvedWriteTarget,
        alias: targetAlias,
        role: 'target',
        kind: 'table',
        specialType: inferSpecialRelationType(resolvedWriteTarget),
      });
    }

    const mergeSource = parseRelationTerm(
      statementEnvelope.mergeUsingClause ?? '',
      0,
      'source',
      cteMap,
      derivedRelations,
      dialect,
    );
    if (mergeSource?.table && !tables.some((table) => table.alias.toLowerCase() === mergeSource.table.alias.toLowerCase())) {
      tables.push(mergeSource.table);
    }

    if (mergeSource?.table) {
      joins.push({
        id: `${targetAlias}-${mergeSource.table.alias}-${joins.length}`,
        type: 'MERGE',
        tableName: mergeSource.table.name,
        alias: mergeSource.table.alias,
        condition: statementEnvelope.mergeOnClause || 'merge match',
        sourceAlias: targetAlias,
        targetAlias: mergeSource.table.alias,
      });
    }
  }

  const uniqueTables = dedupeByAlias(tables);
  const filters = createFilters(whereClause);
  const groupBy = splitTopLevel(groupByClause);
  const orderBy = splitTopLevel(orderByClause);
  const hasAggregation = /\b(count|sum|avg|min|max)\s*\(/i.test(selectMetadata.selectClause);
  const subqueryCount = (normalizedSql.match(/\(\s*select\b/gi) ?? []).length;
  const flags = buildFlags(
    statementEnvelope.mode === 'select' ? normalizedSql : analyzedStatement.replace(/\s+/g, ' ').trim(),
    columns,
    joins,
    filters,
    orderBy,
    rowCapClause || undefined,
    dialect,
    statementEnvelope.statementType,
  );

  const clauses: ClauseStatus[] = [
    {
      label: 'STATEMENT',
      present: Boolean(statementEnvelope.statementLabel),
      detail:
        resolvedWriteTarget
          ? `${statementEnvelope.statementLabel} ${resolvedWriteTarget}`
          : statementEnvelope.statementLabel,
    },
    { label: 'SELECT', present: Boolean(selectMetadata.selectClause), detail: selectMetadata.selectClause || 'No select list detected' },
    { label: 'FROM', present: Boolean(fromClause), detail: fromClause || 'No source table detected' },
    ...(statementEnvelope.mode === 'update-from'
      ? [{ label: 'SET', present: Boolean(statementEnvelope.updateSetClause), detail: statementEnvelope.updateSetClause || 'No assignments' }]
      : []),
    ...(statementEnvelope.mode === 'merge'
      ? [
          { label: 'USING', present: Boolean(statementEnvelope.mergeUsingClause), detail: statementEnvelope.mergeUsingClause || 'No merge source' },
          { label: 'ON', present: Boolean(statementEnvelope.mergeOnClause), detail: statementEnvelope.mergeOnClause || 'No merge predicate' },
        ]
      : []),
    { label: 'WHERE', present: Boolean(whereClause), detail: whereClause || 'No filters' },
    { label: 'GROUP BY', present: Boolean(groupByClause), detail: groupByClause || 'No grouping' },
    { label: 'HAVING', present: Boolean(havingClause), detail: havingClause || 'No aggregate filters' },
    ...(DIALECTS_WITH_QUALIFY.has(dialect)
      ? [
          { label: 'QUALIFY', present: Boolean(qualifyClause), detail: qualifyClause || 'No window filters' },
        ]
      : []),
    ...(DIALECTS_WITH_WINDOW_CLAUSE.has(dialect)
      ? [
          { label: 'WINDOW', present: Boolean(windowClause), detail: windowClause || 'No named windows' },
        ]
      : []),
    { label: 'ORDER BY', present: Boolean(orderByClause), detail: orderByClause || 'No sorting' },
    ...(selectMetadata.topClause ? [{ label: 'TOP', present: true, detail: selectMetadata.topClause }] : []),
    { label: 'LIMIT', present: Boolean(limitClause), detail: limitClause || 'No row cap' },
    { label: 'OFFSET', present: Boolean(offsetClause), detail: offsetClause || 'No offset' },
    ...(fetchClause ? [{ label: 'FETCH', present: true, detail: fetchClause }] : []),
    ...(rownumClause ? [{ label: 'ROWNUM', present: true, detail: rownumClause }] : []),
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
    statementType: statementEnvelope.statementType,
    statementLabel: statementEnvelope.statementLabel,
    writeTarget: resolvedWriteTarget,
    columns,
    tables: uniqueTables,
    joins,
    filters,
    groupBy,
    orderBy,
    limit: rowCapClause || undefined,
    clauses,
    flags,
    derivedRelations,
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
    const role = table.role === 'source' ? 'SOURCE' : table.role === 'target' ? 'TARGET' : 'JOIN';
    const kind = table.kind && table.kind !== 'table' ? `\\n${table.kind.toUpperCase()}` : '';
    const special = table.specialType ? `\\n${table.specialType.toUpperCase()}` : '';
    const label = escapeDotLabel(`${table.name}\\nalias: ${table.alias}\\n${role}${kind}${special}`);
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
