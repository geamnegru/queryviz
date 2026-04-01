export interface SchemaForeignKey {
  columns: string[];
  referencesTable: string;
  referencesColumns: string[];
}

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
  summary: {
    tableCount: number;
    foreignKeyCount: number;
    indexedGroupCount: number;
  };
}

const stripComments = (input: string) =>
  input
    .replace(/\/\*[\s\S]*?\*\//g, ' ')
    .replace(/^\s*--.*$/gm, ' ')
    .trim();

const normalizeSpaces = (value: string) => value.replace(/\s+/g, ' ').trim();

const unwrapIdentifier = (value: string) => {
  const trimmed = value.trim();
  if (
    (trimmed.startsWith('"') && trimmed.endsWith('"')) ||
    (trimmed.startsWith('`') && trimmed.endsWith('`')) ||
    (trimmed.startsWith('[') && trimmed.endsWith(']'))
  ) {
    return trimmed.slice(1, -1);
  }

  return trimmed;
};

const normalizeIdentifierPart = (value: string) => unwrapIdentifier(value).replace(/\s+/g, '').toLowerCase();

export const normalizeSchemaName = (value: string) =>
  value
    .split('.')
    .map((part) => normalizeIdentifierPart(part))
    .filter(Boolean)
    .join('.');

export const normalizeSchemaColumnName = (value: string) => {
  const normalized = normalizeSchemaName(value);
  return normalized.split('.').pop() ?? normalized;
};

const readIdentifier = (input: string, start: number, allowQualified = true) => {
  let cursor = start;
  const parts: string[] = [];

  const skipWhitespace = () => {
    while (cursor < input.length && /\s/.test(input[cursor])) {
      cursor += 1;
    }
  };

  skipWhitespace();

  while (cursor < input.length) {
    const char = input[cursor];
    let token = '';

    if (char === '"' || char === '`') {
      const quote = char;
      const nextQuote = input.indexOf(quote, cursor + 1);
      if (nextQuote === -1) {
        return null;
      }
      token = input.slice(cursor, nextQuote + 1);
      cursor = nextQuote + 1;
    } else if (char === '[') {
      const nextBracket = input.indexOf(']', cursor + 1);
      if (nextBracket === -1) {
        return null;
      }
      token = input.slice(cursor, nextBracket + 1);
      cursor = nextBracket + 1;
    } else {
      const match = input.slice(cursor).match(/^[a-zA-Z_#][\w$#]*/);
      if (!match) {
        break;
      }
      token = match[0];
      cursor += token.length;
    }

    parts.push(token);
    skipWhitespace();

    if (!allowQualified || input[cursor] !== '.') {
      break;
    }

    cursor += 1;
    skipWhitespace();
  }

  if (parts.length === 0) {
    return null;
  }

  return {
    raw: parts.join('.'),
    normalized: normalizeSchemaName(parts.join('.')),
    end: cursor,
  };
};

const findMatchingParen = (input: string, openIndex: number) => {
  let depth = 0;
  let quote: '"' | "'" | '`' | null = null;

  for (let index = openIndex; index < input.length; index += 1) {
    const char = input[index];

    if (quote) {
      if (char === quote && input[index - 1] !== '\\') {
        quote = null;
      }
      continue;
    }

    if (char === '"' || char === "'" || char === '`') {
      quote = char;
      continue;
    }

    if (char === '(') {
      depth += 1;
      continue;
    }

    if (char === ')') {
      depth -= 1;
      if (depth === 0) {
        return index;
      }
    }
  }

  return -1;
};

const splitTopLevel = (input: string, separator: ',' | ';') => {
  const parts: string[] = [];
  let depth = 0;
  let quote: '"' | "'" | '`' | null = null;
  let start = 0;

  for (let index = 0; index < input.length; index += 1) {
    const char = input[index];

    if (quote) {
      if (char === quote && input[index - 1] !== '\\') {
        quote = null;
      }
      continue;
    }

    if (char === '"' || char === "'" || char === '`') {
      quote = char;
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

    if (depth === 0 && char === separator) {
      const part = input.slice(start, index).trim();
      if (part) {
        parts.push(part);
      }
      start = index + 1;
    }
  }

  const tail = input.slice(start).trim();
  if (tail) {
    parts.push(tail);
  }

  return parts;
};

const parseColumnList = (input: string) =>
  splitTopLevel(input, ',')
    .map((item) => normalizeSchemaColumnName(item))
    .filter(Boolean);

const hasPrefixCoverage = (candidate: string[], requested: string[]) =>
  requested.length <= candidate.length &&
  requested.every((column, index) => candidate[index] === column);

const parseCreateTableStatement = (statement: string): SchemaTableMetadata | null => {
  const headerMatch = statement.match(/^create\s+(?:or\s+replace\s+)?(?:temporary\s+|temp\s+)?table\s+(?:if\s+not\s+exists\s+)?/i);
  if (!headerMatch) {
    return null;
  }

  const rest = statement.slice(headerMatch[0].length).trim();
  const identifier = readIdentifier(rest, 0, true);
  if (!identifier) {
    return null;
  }

  const openIndex = rest.indexOf('(', identifier.end);
  if (openIndex === -1) {
    return null;
  }

  const closeIndex = findMatchingParen(rest, openIndex);
  if (closeIndex === -1) {
    return null;
  }

  const body = rest.slice(openIndex + 1, closeIndex);
  const parts = splitTopLevel(body, ',');
  const columns = new Set<string>();
  const primaryKey = new Set<string>();
  const uniqueKeys: string[][] = [];
  const indexes: string[][] = [];
  const foreignKeys: SchemaForeignKey[] = [];

  parts.forEach((part) => {
    const normalizedPart = normalizeSpaces(part);
    const constraintBody = normalizedPart.replace(/^constraint\s+[^\s]+\s+/i, '');

    const tablePrimaryKey = constraintBody.match(/^primary\s+key\s*\(([^)]+)\)/i);
    if (tablePrimaryKey) {
      const keyColumns = parseColumnList(tablePrimaryKey[1]);
      keyColumns.forEach((column) => primaryKey.add(column));
      if (keyColumns.length > 0) {
        uniqueKeys.push(keyColumns);
      }
      return;
    }

    const tableUnique = constraintBody.match(/^unique\s*\(([^)]+)\)/i);
    if (tableUnique) {
      const keyColumns = parseColumnList(tableUnique[1]);
      if (keyColumns.length > 0) {
        uniqueKeys.push(keyColumns);
      }
      return;
    }

    const tableForeignKey = constraintBody.match(/^foreign\s+key\s*\(([^)]+)\)\s+references\s+([^\s(]+)\s*\(([^)]+)\)/i);
    if (tableForeignKey) {
      foreignKeys.push({
        columns: parseColumnList(tableForeignKey[1]),
        referencesTable: normalizeSchemaName(tableForeignKey[2]),
        referencesColumns: parseColumnList(tableForeignKey[3]),
      });
      return;
    }

    const columnIdentifier = readIdentifier(part, 0, false);
    if (!columnIdentifier) {
      return;
    }

    const columnName = normalizeSchemaColumnName(columnIdentifier.raw);
    if (!columnName) {
      return;
    }

    columns.add(columnName);
    const columnRest = part.slice(columnIdentifier.end);

    if (/\bprimary\s+key\b/i.test(columnRest)) {
      primaryKey.add(columnName);
      uniqueKeys.push([columnName]);
    }

    if (/\bunique\b/i.test(columnRest)) {
      uniqueKeys.push([columnName]);
    }

    const inlineReference = columnRest.match(/\breferences\s+([^\s(]+)\s*\(([^)]+)\)/i);
    if (inlineReference) {
      foreignKeys.push({
        columns: [columnName],
        referencesTable: normalizeSchemaName(inlineReference[1]),
        referencesColumns: parseColumnList(inlineReference[2]),
      });
    }
  });

  const normalizedName = identifier.normalized;
  return {
    name: identifier.raw,
    normalizedName,
    shortName: normalizedName.split('.').pop() ?? normalizedName,
    columns: Array.from(columns),
    primaryKey: Array.from(primaryKey),
    uniqueKeys,
    indexes,
    foreignKeys,
  };
};

const parseCreateIndexStatement = (statement: string) => {
  const match = statement.match(/^create\s+(?:unique\s+)?index\s+(?:if\s+not\s+exists\s+)?[^\s]+\s+on\s+([^\s(]+)\s*\(([^)]+)\)/i);
  if (!match) {
    return null;
  }

  return {
    tableName: normalizeSchemaName(match[1]),
    columns: parseColumnList(match[2]),
  };
};

export const parseSchemaInput = (input: string): ParsedSchemaMetadata => {
  const sanitized = stripComments(input);
  if (!sanitized) {
    return {
      tables: [],
      summary: {
        tableCount: 0,
        foreignKeyCount: 0,
        indexedGroupCount: 0,
      },
    };
  }

  const statements = splitTopLevel(sanitized, ';');
  const tableMap = new Map<string, SchemaTableMetadata>();

  statements.forEach((statement) => {
    const normalized = normalizeSpaces(statement);
    if (!normalized) {
      return;
    }

    const parsedTable = parseCreateTableStatement(statement);
    if (parsedTable) {
      tableMap.set(parsedTable.normalizedName, parsedTable);
      return;
    }

    const parsedIndex = parseCreateIndexStatement(normalized);
    if (!parsedIndex) {
      return;
    }

    const table = tableMap.get(parsedIndex.tableName);
    if (!table || parsedIndex.columns.length === 0) {
      return;
    }

    table.indexes.push(parsedIndex.columns);
  });

  const tables = Array.from(tableMap.values());
  return {
    tables,
    summary: {
      tableCount: tables.length,
      foreignKeyCount: tables.reduce((total, table) => total + table.foreignKeys.length, 0),
      indexedGroupCount: tables.reduce(
        (total, table) => total + table.indexes.length + table.uniqueKeys.length + (table.primaryKey.length > 0 ? 1 : 0),
        0,
      ),
    },
  };
};

export const findSchemaTable = (tables: SchemaTableMetadata[], relationName?: string | null) => {
  if (!relationName) {
    return null;
  }

  const normalized = normalizeSchemaName(relationName);
  const shortName = normalized.split('.').pop() ?? normalized;
  return (
    tables.find((table) => table.normalizedName === normalized) ??
    tables.find((table) => table.shortName === shortName) ??
    null
  );
};

export const getColumnSetCoverage = (table: SchemaTableMetadata | null | undefined, columns: string[]) => {
  if (!table || columns.length === 0) {
    return null;
  }

  const normalizedColumns = columns.map((column) => normalizeSchemaColumnName(column));
  if (table.primaryKey.length > 0 && hasPrefixCoverage(table.primaryKey, normalizedColumns)) {
    return 'primary-key';
  }

  if (table.uniqueKeys.some((keyColumns) => hasPrefixCoverage(keyColumns, normalizedColumns))) {
    return 'unique';
  }

  if (table.indexes.some((indexColumns) => hasPrefixCoverage(indexColumns, normalizedColumns))) {
    return 'index';
  }

  return null;
};

export const hasForeignKeyMatch = (
  table: SchemaTableMetadata | null | undefined,
  columns: string[],
  referencesTable: SchemaTableMetadata | null | undefined,
  referenceColumns: string[],
) => {
  if (!table || !referencesTable || columns.length === 0 || referenceColumns.length === 0) {
    return false;
  }

  const normalizedColumns = columns.map((column) => normalizeSchemaColumnName(column));
  const normalizedReferenceColumns = referenceColumns.map((column) => normalizeSchemaColumnName(column));

  return table.foreignKeys.some(
    (foreignKey) =>
      foreignKey.referencesTable === referencesTable.normalizedName &&
      foreignKey.columns.length === normalizedColumns.length &&
      foreignKey.referencesColumns.length === normalizedReferenceColumns.length &&
      foreignKey.columns.every((column, index) => column === normalizedColumns[index]) &&
      foreignKey.referencesColumns.every((column, index) => column === normalizedReferenceColumns[index]),
  );
};
