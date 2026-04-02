import type {
  ParsedSchemaMetadata,
  SchemaForeignKey,
  SchemaForeignKeyPairMatch,
  SchemaTableMetadata,
  SchemaVerificationMatch,
} from './types';

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

const createEmptyMetadata = (sourceKind: ParsedSchemaMetadata['sourceKind']): ParsedSchemaMetadata => ({
  tables: [],
  sourceKind,
  summary: {
    tableCount: 0,
    foreignKeyCount: 0,
    indexedGroupCount: 0,
  },
});

const ensureSchemaTable = (tableMap: Map<string, SchemaTableMetadata>, relationName: string) => {
  const normalizedName = normalizeSchemaName(relationName);
  if (!normalizedName) {
    return null;
  }

  const existing = tableMap.get(normalizedName);
  if (existing) {
    return existing;
  }

  const nextTable: SchemaTableMetadata = {
    name: relationName,
    normalizedName,
    shortName: normalizedName.split('.').pop() ?? normalizedName,
    columns: [],
    primaryKey: [],
    uniqueKeys: [],
    indexes: [],
    foreignKeys: [],
  };
  tableMap.set(normalizedName, nextTable);
  return nextTable;
};

const pushUniqueString = (items: string[], nextItem: string) => {
  if (!items.includes(nextItem)) {
    items.push(nextItem);
  }
};

const pushUniqueColumnGroup = (groups: string[][], nextGroup: string[]) => {
  if (
    nextGroup.length === 0 ||
    groups.some(
      (group) =>
        group.length === nextGroup.length &&
        group.every((column, index) => column === nextGroup[index]),
    )
  ) {
    return;
  }

  groups.push(nextGroup);
};

const hasPrefixCoverage = (candidate: string[], requested: string[]) =>
  requested.length <= candidate.length &&
  requested.every((column, index) => candidate[index] === column);

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

const relationSummary = (tables: SchemaTableMetadata[], sourceKind: ParsedSchemaMetadata['sourceKind']): ParsedSchemaMetadata => ({
  tables,
  sourceKind,
  summary: {
    tableCount: tables.length,
    foreignKeyCount: tables.reduce((total, table) => total + table.foreignKeys.length, 0),
    indexedGroupCount: tables.reduce(
      (total, table) => total + table.indexes.length + table.uniqueKeys.length + (table.primaryKey.length > 0 ? 1 : 0),
      0,
    ),
  },
});

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
        pushUniqueColumnGroup(uniqueKeys, keyColumns);
      }
      return;
    }

    const tableUnique = constraintBody.match(/^unique\s*\(([^)]+)\)/i);
    if (tableUnique) {
      const keyColumns = parseColumnList(tableUnique[1]);
      if (keyColumns.length > 0) {
        pushUniqueColumnGroup(uniqueKeys, keyColumns);
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
      pushUniqueColumnGroup(uniqueKeys, [columnName]);
    }

    if (/\bunique\b/i.test(columnRest)) {
      pushUniqueColumnGroup(uniqueKeys, [columnName]);
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

const parseAlterTableStatement = (statement: string) => {
  const headerMatch = statement.match(/^alter\s+table\s+(?:if\s+exists\s+)?(?:only\s+)?/i);
  if (!headerMatch) {
    return null;
  }

  const rest = statement.slice(headerMatch[0].length).trim();
  const identifier = readIdentifier(rest, 0, true);
  if (!identifier) {
    return null;
  }

  const operation = normalizeSpaces(rest.slice(identifier.end))
    .replace(/^add\s+/i, '')
    .replace(/^constraint\s+[^\s]+\s+/i, '');
  if (!operation) {
    return null;
  }

  const primaryKey = operation.match(/^primary\s+key\s*\(([^)]+)\)/i);
  if (primaryKey) {
    return {
      tableName: identifier.normalized,
      primaryKey: parseColumnList(primaryKey[1]),
      uniqueKeys: [] as string[][],
      foreignKeys: [] as SchemaForeignKey[],
      indexes: [] as string[][],
    };
  }

  const unique = operation.match(/^unique\s*(?:key|index)?\s*\(([^)]+)\)/i);
  if (unique) {
    return {
      tableName: identifier.normalized,
      primaryKey: [] as string[],
      uniqueKeys: [parseColumnList(unique[1])],
      foreignKeys: [] as SchemaForeignKey[],
      indexes: [] as string[][],
    };
  }

  const foreignKey = operation.match(/^foreign\s+key\s*\(([^)]+)\)\s+references\s+([^\s(]+)\s*\(([^)]+)\)/i);
  if (foreignKey) {
    return {
      tableName: identifier.normalized,
      primaryKey: [] as string[],
      uniqueKeys: [] as string[][],
      foreignKeys: [
        {
          columns: parseColumnList(foreignKey[1]),
          referencesTable: normalizeSchemaName(foreignKey[2]),
          referencesColumns: parseColumnList(foreignKey[3]),
        },
      ],
      indexes: [] as string[][],
    };
  }

  return null;
};

const parseDbtRelationReference = (input: string) => {
  const trimmed = input.trim().replace(/^['"]|['"]$/g, '');
  const sourceMatch = trimmed.match(/^source\(\s*['"]([^'"]+)['"]\s*,\s*['"]([^'"]+)['"]\s*\)$/i);
  if (sourceMatch) {
    return normalizeSchemaName(`${sourceMatch[1]}.${sourceMatch[2]}`);
  }

  const refMatch = trimmed.match(/^ref\(\s*['"]([^'"]+)['"]\s*\)$/i);
  if (refMatch) {
    return normalizeSchemaName(refMatch[1]);
  }

  return normalizeSchemaName(trimmed);
};

const applyDbtColumnTests = (table: SchemaTableMetadata, columnName: string, tests: unknown[]) => {
  const normalizedColumn = normalizeSchemaColumnName(columnName);
  pushUniqueString(table.columns, normalizedColumn);

  tests.forEach((testEntry) => {
    if (typeof testEntry === 'string') {
      if (testEntry.toLowerCase() === 'unique') {
        pushUniqueColumnGroup(table.uniqueKeys, [normalizedColumn]);
      }
      return;
    }

    if (typeof testEntry !== 'object' || !testEntry || Array.isArray(testEntry)) {
      return;
    }

    const [testName, rawConfig] = Object.entries(testEntry)[0] ?? [];
    if (!testName) {
      return;
    }

    if (testName.toLowerCase() === 'unique') {
      pushUniqueColumnGroup(table.uniqueKeys, [normalizedColumn]);
      return;
    }

    if (testName.toLowerCase() !== 'relationships' || typeof rawConfig !== 'object' || !rawConfig || Array.isArray(rawConfig)) {
      return;
    }

    const config = rawConfig as Record<string, unknown>;
    const targetRelation = typeof config.to === 'string' ? parseDbtRelationReference(config.to) : '';
    const targetField = typeof config.field === 'string' ? normalizeSchemaColumnName(config.field) : 'id';
    if (!targetRelation) {
      return;
    }

    table.foreignKeys.push({
      columns: [normalizedColumn],
      referencesTable: targetRelation,
      referencesColumns: [targetField],
    });
  });
};

const parseDbtManifestInput = (input: string): ParsedSchemaMetadata | null => {
  let parsed: unknown;

  try {
    parsed = JSON.parse(input);
  } catch {
    return null;
  }

  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
    return null;
  }

  const manifest = parsed as { nodes?: Record<string, unknown>; sources?: Record<string, unknown> };
  if (!manifest.nodes && !manifest.sources) {
    return null;
  }

  const tableMap = new Map<string, SchemaTableMetadata>();
  const uniqueIdToRelation = new Map<string, string>();

  const registerRelationNode = (node: Record<string, unknown>) => {
    const resourceType = typeof node.resource_type === 'string' ? node.resource_type : '';
    if (!['model', 'seed', 'snapshot', 'source'].includes(resourceType)) {
      return;
    }

    const rawName =
      (typeof node.alias === 'string' && node.alias) ||
      (typeof node.identifier === 'string' && node.identifier) ||
      (typeof node.name === 'string' && node.name) ||
      '';
    if (!rawName) {
      return;
    }

    const relationName = [node.database, node.schema, rawName]
      .filter((part): part is string => typeof part === 'string' && part.length > 0)
      .join('.');
    const table = ensureSchemaTable(tableMap, relationName || rawName);
    if (!table) {
      return;
    }

    if (typeof node.unique_id === 'string') {
      uniqueIdToRelation.set(node.unique_id, table.normalizedName);
    }

    const columns = typeof node.columns === 'object' && node.columns && !Array.isArray(node.columns)
      ? (node.columns as Record<string, unknown>)
      : {};

    Object.entries(columns).forEach(([columnName, columnValue]) => {
      pushUniqueString(table.columns, normalizeSchemaColumnName(columnName));

      if (typeof columnValue !== 'object' || !columnValue || Array.isArray(columnValue)) {
        return;
      }

      const columnRecord = columnValue as Record<string, unknown>;
      const tests = Array.isArray(columnRecord.tests) ? columnRecord.tests : [];
      applyDbtColumnTests(table, columnName, tests);

      const constraints = Array.isArray(columnRecord.constraints) ? columnRecord.constraints : [];
      constraints.forEach((constraint) => {
        if (typeof constraint !== 'object' || !constraint || Array.isArray(constraint)) {
          return;
        }

        const type = typeof (constraint as Record<string, unknown>).type === 'string'
          ? ((constraint as Record<string, unknown>).type as string).toLowerCase()
          : '';
        if (type === 'primary_key') {
          pushUniqueString(table.primaryKey, normalizeSchemaColumnName(columnName));
          pushUniqueColumnGroup(table.uniqueKeys, [normalizeSchemaColumnName(columnName)]);
        } else if (type === 'unique') {
          pushUniqueColumnGroup(table.uniqueKeys, [normalizeSchemaColumnName(columnName)]);
        }
      });
    });
  };

  [...Object.values(manifest.nodes ?? {}), ...Object.values(manifest.sources ?? {})].forEach((item) => {
    if (item && typeof item === 'object' && !Array.isArray(item)) {
      registerRelationNode(item as Record<string, unknown>);
    }
  });

  Object.values(manifest.nodes ?? {}).forEach((item) => {
    if (typeof item !== 'object' || !item || Array.isArray(item)) {
      return;
    }

    const node = item as Record<string, unknown>;
    if (node.resource_type !== 'test' || typeof node.test_metadata !== 'object' || !node.test_metadata || Array.isArray(node.test_metadata)) {
      return;
    }

    const testMetadata = node.test_metadata as Record<string, unknown>;
    const testName = typeof testMetadata.name === 'string' ? testMetadata.name.toLowerCase() : '';
    const kwargs =
      typeof testMetadata.kwargs === 'object' && testMetadata.kwargs && !Array.isArray(testMetadata.kwargs)
        ? (testMetadata.kwargs as Record<string, unknown>)
        : {};
    const columnName =
      typeof node.column_name === 'string'
        ? normalizeSchemaColumnName(node.column_name)
        : typeof kwargs.column_name === 'string'
          ? normalizeSchemaColumnName(kwargs.column_name)
          : '';
    const dependsOn =
      typeof node.depends_on === 'object' && node.depends_on && !Array.isArray(node.depends_on)
        ? (node.depends_on as Record<string, unknown>)
        : {};
    const dependencyIds = Array.isArray(dependsOn.nodes)
      ? dependsOn.nodes.filter((value): value is string => typeof value === 'string')
      : [];
    const sourceTableName = dependencyIds
      .map((dependencyId) => uniqueIdToRelation.get(dependencyId) ?? '')
      .find(Boolean);
    const sourceTable = sourceTableName ? ensureSchemaTable(tableMap, sourceTableName) : null;

    if (!sourceTable || !columnName) {
      return;
    }

    pushUniqueString(sourceTable.columns, columnName);

    if (testName === 'unique') {
      pushUniqueColumnGroup(sourceTable.uniqueKeys, [columnName]);
      return;
    }

    if (testName !== 'relationships') {
      return;
    }

    const targetRelation =
      typeof kwargs.to === 'string'
        ? parseDbtRelationReference(kwargs.to)
        : dependencyIds
            .slice(1)
            .map((dependencyId) => uniqueIdToRelation.get(dependencyId) ?? '')
            .find(Boolean) ?? '';
    const targetField = typeof kwargs.field === 'string' ? normalizeSchemaColumnName(kwargs.field) : 'id';
    if (!targetRelation) {
      return;
    }

    sourceTable.foreignKeys.push({
      columns: [columnName],
      referencesTable: targetRelation,
      referencesColumns: [targetField],
    });
  });

  const tables = Array.from(tableMap.values()).filter((table) => table.columns.length > 0 || table.foreignKeys.length > 0);
  return tables.length > 0 ? relationSummary(tables, 'dbt-manifest') : null;
};

const parseDbtSchemaYamlInput = (input: string): ParsedSchemaMetadata | null => {
  if (!/\bversion\s*:/i.test(input) || !/\b(?:models|sources)\s*:/i.test(input)) {
    return null;
  }

  const lines = input
    .replace(/\r/g, '')
    .split('\n')
    .map((line) => line.replace(/\s+#.*$/, ''));
  const tableMap = new Map<string, SchemaTableMetadata>();
  let section: 'models' | 'sources' | null = null;
  let sourceGroup = '';
  let currentTable: SchemaTableMetadata | null = null;
  let currentColumn = '';
  let inColumns = false;
  let inTests = false;
  let pendingRelationship: { column: string; target: string; field: string } | null = null;

  const flushRelationship = () => {
    if (!currentTable || !pendingRelationship || !pendingRelationship.target) {
      pendingRelationship = null;
      return;
    }

    currentTable.foreignKeys.push({
      columns: [normalizeSchemaColumnName(pendingRelationship.column)],
      referencesTable: pendingRelationship.target,
      referencesColumns: [normalizeSchemaColumnName(pendingRelationship.field || 'id')],
    });
    pendingRelationship = null;
  };

  lines.forEach((rawLine) => {
    const trimmed = rawLine.trim();
    if (!trimmed) {
      return;
    }

    const indent = rawLine.match(/^ */)?.[0].length ?? 0;

    if (indent === 0) {
      flushRelationship();
      inColumns = false;
      inTests = false;
      currentColumn = '';

      if (/^models\s*:/i.test(trimmed)) {
        section = 'models';
        sourceGroup = '';
      } else if (/^sources\s*:/i.test(trimmed)) {
        section = 'sources';
        sourceGroup = '';
      }
      return;
    }

    if (section === 'models' && indent === 2 && /^-\s+name\s*:/i.test(trimmed)) {
      flushRelationship();
      inColumns = false;
      inTests = false;
      currentColumn = '';
      const modelName = trimmed.replace(/^-\s+name\s*:\s*/i, '').replace(/^['"]|['"]$/g, '');
      currentTable = ensureSchemaTable(tableMap, modelName);
      return;
    }

    if (section === 'sources' && indent === 2 && /^-\s+name\s*:/i.test(trimmed)) {
      flushRelationship();
      sourceGroup = trimmed.replace(/^-\s+name\s*:\s*/i, '').replace(/^['"]|['"]$/g, '');
      currentTable = null;
      inColumns = false;
      inTests = false;
      currentColumn = '';
      return;
    }

    if (section === 'sources' && indent === 4 && /^-\s+name\s*:/i.test(trimmed)) {
      flushRelationship();
      inColumns = false;
      inTests = false;
      currentColumn = '';
      const tableName = trimmed.replace(/^-\s+name\s*:\s*/i, '').replace(/^['"]|['"]$/g, '');
      currentTable = ensureSchemaTable(tableMap, sourceGroup ? `${sourceGroup}.${tableName}` : tableName);
      return;
    }

    if (/^columns\s*:/i.test(trimmed)) {
      flushRelationship();
      inColumns = true;
      inTests = false;
      currentColumn = '';
      return;
    }

    if (inColumns && indent >= 6 && /^-\s+name\s*:/i.test(trimmed)) {
      flushRelationship();
      currentColumn = trimmed.replace(/^-\s+name\s*:\s*/i, '').replace(/^['"]|['"]$/g, '');
      if (currentTable) {
        pushUniqueString(currentTable.columns, normalizeSchemaColumnName(currentColumn));
      }
      inTests = false;
      return;
    }

    if (currentTable && currentColumn && /^tests\s*:/i.test(trimmed)) {
      flushRelationship();
      inTests = true;
      return;
    }

    if (!currentTable || !currentColumn || !inTests) {
      return;
    }

    if (/^-\s+unique$/i.test(trimmed)) {
      pushUniqueColumnGroup(currentTable.uniqueKeys, [normalizeSchemaColumnName(currentColumn)]);
      return;
    }

    if (/^-\s+relationships\s*:/i.test(trimmed)) {
      pendingRelationship = {
        column: currentColumn,
        target: '',
        field: 'id',
      };
      return;
    }

    if (!pendingRelationship) {
      return;
    }

    if (/^to\s*:/i.test(trimmed)) {
      pendingRelationship.target = parseDbtRelationReference(trimmed.replace(/^to\s*:\s*/i, ''));
      flushRelationship();
      return;
    }

    if (/^field\s*:/i.test(trimmed)) {
      pendingRelationship.field = trimmed.replace(/^field\s*:\s*/i, '').replace(/^['"]|['"]$/g, '');
    }
  });

  flushRelationship();

  const tables = Array.from(tableMap.values()).filter((table) => table.columns.length > 0 || table.foreignKeys.length > 0);
  return tables.length > 0 ? relationSummary(tables, 'dbt-schema-yml') : null;
};

const parseDdlInput = (input: string): ParsedSchemaMetadata => {
  const sanitized = stripComments(input);
  if (!sanitized) {
    return createEmptyMetadata('empty');
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
    if (parsedIndex) {
      const table = tableMap.get(parsedIndex.tableName);
      if (!table || parsedIndex.columns.length === 0) {
        return;
      }

      pushUniqueColumnGroup(table.indexes, parsedIndex.columns);
      return;
    }

    const parsedAlter = parseAlterTableStatement(normalized);
    if (!parsedAlter) {
      return;
    }

    const table = tableMap.get(parsedAlter.tableName);
    if (!table) {
      return;
    }

    parsedAlter.primaryKey.forEach((column) => {
      if (!table.primaryKey.includes(column)) {
        table.primaryKey.push(column);
      }
    });

    if (parsedAlter.primaryKey.length > 0) {
      pushUniqueColumnGroup(table.uniqueKeys, parsedAlter.primaryKey);
    }

    parsedAlter.uniqueKeys.forEach((group) => pushUniqueColumnGroup(table.uniqueKeys, group));
    parsedAlter.indexes.forEach((group) => pushUniqueColumnGroup(table.indexes, group));

    parsedAlter.foreignKeys.forEach((foreignKey) => {
      const exists = table.foreignKeys.some(
        (candidate) =>
          candidate.referencesTable === foreignKey.referencesTable &&
          candidate.columns.length === foreignKey.columns.length &&
          candidate.referencesColumns.length === foreignKey.referencesColumns.length &&
          candidate.columns.every((column, index) => column === foreignKey.columns[index]) &&
          candidate.referencesColumns.every((column, index) => column === foreignKey.referencesColumns[index]),
      );
      if (!exists) {
        table.foreignKeys.push(foreignKey);
      }
    });
  });

  return relationSummary(Array.from(tableMap.values()), 'ddl');
};

export const parseSchemaInput = (input: string): ParsedSchemaMetadata => {
  const trimmed = input.trim();
  if (!trimmed) {
    return createEmptyMetadata('empty');
  }

  const manifest = parseDbtManifestInput(trimmed);
  if (manifest) {
    return manifest;
  }

  const dbtSchema = parseDbtSchemaYamlInput(trimmed);
  if (dbtSchema) {
    return dbtSchema;
  }

  return parseDdlInput(trimmed);
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

export const findForeignKeyPairMatch = (
  table: SchemaTableMetadata | null | undefined,
  pairs: SchemaForeignKeyPairMatch[],
  referencesTable: SchemaTableMetadata | null | undefined,
): SchemaVerificationMatch | null => {
  if (!table || !referencesTable || pairs.length === 0) {
    return null;
  }

  const normalizedPairs = pairs.map((pair) => ({
    column: normalizeSchemaColumnName(pair.column),
    referenceColumn: normalizeSchemaColumnName(pair.referenceColumn),
  }));

  const foreignKey = table.foreignKeys.find((candidate) => {
    if (
      candidate.referencesTable !== referencesTable.normalizedName ||
      candidate.columns.length !== normalizedPairs.length ||
      candidate.referencesColumns.length !== normalizedPairs.length
    ) {
      return false;
    }

    return candidate.columns.every((column, index) => {
      const pair = normalizedPairs.find((entry) => entry.column === column);
      return pair?.referenceColumn === candidate.referencesColumns[index];
    });
  });

  if (!foreignKey) {
    return null;
  }

  return {
    foreignKey,
    referenceCoverage: getColumnSetCoverage(referencesTable, foreignKey.referencesColumns),
    pairCount: foreignKey.columns.length,
  };
};

export const hasForeignKeyPairMatch = (
  table: SchemaTableMetadata | null | undefined,
  pairs: SchemaForeignKeyPairMatch[],
  referencesTable: SchemaTableMetadata | null | undefined,
) => Boolean(findForeignKeyPairMatch(table, pairs, referencesTable));
