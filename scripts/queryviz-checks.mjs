#!/usr/bin/env node

import { readFile, readdir, stat } from 'node:fs/promises';
import path from 'node:path';
import process from 'node:process';
import {
  analyzeSql,
  detectSqlDialect,
  diagnoseSqlInput,
  extractStatements,
  SUPPORTED_SQL_DIALECTS,
} from '../src/lib/analyzeSql.ts';
import { parseSchemaInput } from '../src/lib/schemaMetadata.ts';

const SQL_EXTENSIONS = new Set(['.sql', '.ddl', '.txt']);
const severityRank = {
  none: 0,
  low: 1,
  medium: 2,
  high: 3,
  error: 4,
};

const FLAG_RECOMMENDATIONS = {
  'join-heavy query': 'Run EXPLAIN ANALYZE and inspect the highest-cost joins before rewriting.',
  'function inside where': 'Rewrite function-wrapped predicates into sargable range checks or computed-column comparisons.',
  'subquery detected': 'Pre-aggregate the subquery and LEFT JOIN it back instead of evaluating it row by row.',
  'wildcard select': 'Replace SELECT * with an explicit column list to reduce I/O and schema drift.',
  'nolock hint': 'Confirm that dirty or duplicated reads are acceptable before keeping NOLOCK.',
  'apply operator': 'Test a set-based rewrite with a derived table or window-function result.',
  'top without order by': 'Add ORDER BY if callers expect deterministic rows.',
  'repeated unnest': 'Filter before UNNEST and aggregate back to the parent grain early.',
  'flatten relation': 'Project fewer nested fields and reduce rows before rejoining them.',
  'wildcard table scan': 'Use tighter shard pruning so the query reads fewer physical sources.',
  'external file scan': 'Push filters down early and avoid scanning files you do not need.',
};

const printUsage = () => {
  console.log(`queryviz-checks

Usage:
  npm run check:sql -- <file-or-dir> [...more paths]
  npm run check:sql -- - < query.sql

Options:
  --dialect <name>      Force a dialect (${SUPPORTED_SQL_DIALECTS.join(', ')})
  --format <text|json|sarif>
                        Output format (default: text)
  --fail-on <level>     Exit non-zero on low|medium|high findings (default: high)
  --statement <n|all>   Analyze one statement number (1-based) or all statements
  --schema <path>       Parse a schema DDL file and include its summary in the report
`);
};

const normalizeFlagTitle = (title) => title.trim().toLowerCase();
const slugify = (value) => value.trim().toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/(^-|-$)/g, '');

const recommendationForFlag = (flag) =>
  FLAG_RECOMMENDATIONS[normalizeFlagTitle(flag.title)] ?? flag.description;

const toArtifactUri = (inputPath) => {
  if (inputPath === '-') {
    return 'stdin.sql';
  }

  const relative = path.relative(process.cwd(), inputPath);
  return relative && !relative.startsWith('..') ? relative : inputPath;
};

const diagnosticSarifLevel = (severity) => (severity === 'error' ? 'error' : 'warning');
const flagSarifLevel = (severity) => {
  if (severity === 'high') {
    return 'warning';
  }

  if (severity === 'medium') {
    return 'warning';
  }

  return 'note';
};

const parseArguments = (argv) => {
  const options = {
    dialect: null,
    format: 'text',
    failOn: 'high',
    statement: 'all',
    schemaPath: null,
    inputs: [],
  };

  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];

    if (token === '--help' || token === '-h') {
      options.help = true;
      continue;
    }

    if (token === '--dialect') {
      options.dialect = argv[index + 1] ?? null;
      index += 1;
      continue;
    }

    if (token === '--format') {
      options.format = argv[index + 1] ?? 'text';
      index += 1;
      continue;
    }

    if (token === '--fail-on') {
      options.failOn = argv[index + 1] ?? 'high';
      index += 1;
      continue;
    }

    if (token === '--statement') {
      options.statement = argv[index + 1] ?? 'all';
      index += 1;
      continue;
    }

    if (token === '--schema') {
      options.schemaPath = argv[index + 1] ?? null;
      index += 1;
      continue;
    }

    options.inputs.push(token);
  }

  return options;
};

const isSupportedDialect = (value) => typeof value === 'string' && SUPPORTED_SQL_DIALECTS.includes(value);

const expandInputPath = async (inputPath) => {
  if (inputPath === '-') {
    return ['-'];
  }

  const stats = await stat(inputPath);
  if (stats.isFile()) {
    return [inputPath];
  }

  if (!stats.isDirectory()) {
    return [];
  }

  const entries = await readdir(inputPath, { withFileTypes: true });
  const discovered = [];

  for (const entry of entries) {
    const nextPath = path.join(inputPath, entry.name);
    if (entry.isDirectory()) {
      discovered.push(...(await expandInputPath(nextPath)));
      continue;
    }

    if (entry.isFile() && SQL_EXTENSIONS.has(path.extname(entry.name).toLowerCase())) {
      discovered.push(nextPath);
    }
  }

  return discovered;
};

const readInputFile = async (inputPath) => {
  if (inputPath === '-') {
    return new Promise((resolve, reject) => {
      let buffer = '';
      process.stdin.setEncoding('utf8');
      process.stdin.on('data', (chunk) => {
        buffer += chunk;
      });
      process.stdin.on('end', () => resolve(buffer));
      process.stdin.on('error', reject);
    });
  }

  return readFile(inputPath, 'utf8');
};

const getStatementIndexes = (statementOption, statementCount) => {
  if (statementCount === 0) {
    return [0];
  }

  if (statementOption === 'all') {
    return Array.from({ length: statementCount }, (_, index) => index);
  }

  const parsed = Number(statementOption);
  if (!Number.isFinite(parsed) || parsed < 1) {
    throw new Error(`Invalid --statement value "${statementOption}". Use a 1-based number or "all".`);
  }

  return [Math.min(statementCount - 1, parsed - 1)];
};

const rankDiagnostic = (diagnostic) => (diagnostic.severity === 'error' ? severityRank.error : severityRank.medium);
const rankFlag = (flag) => severityRank[flag.severity] ?? severityRank.low;

const summarizeInput = async (inputPath, options, schemaSummary) => {
  const sql = await readInputFile(inputPath);
  const detected = isSupportedDialect(options.dialect)
    ? { dialect: options.dialect, confident: true, evidence: ['forced by CLI'] }
    : detectSqlDialect(sql);
  const dialect = detected.dialect;
  const diagnostics = diagnoseSqlInput(sql, dialect);
  const statements = extractStatements(sql, dialect);
  const statementIndexes = getStatementIndexes(options.statement, statements.length);
  const analyses = statementIndexes.map((statementIndex) => analyzeSql(sql, statementIndex, dialect));
  const highestSeverity = Math.max(
    0,
    ...diagnostics.map(rankDiagnostic),
    ...analyses.flatMap((analysis) => analysis.flags.map(rankFlag)),
  );

  return {
    input: inputPath,
    dialect,
    detected,
    schema: schemaSummary,
    diagnostics: diagnostics.map((diagnostic) => ({
      severity: diagnostic.severity,
      title: diagnostic.title,
      message: diagnostic.message,
      line: diagnostic.line,
      column: diagnostic.column,
      hint: diagnostic.hint,
    })),
    statements: analyses.map((analysis) => ({
      index: analysis.analyzedStatementIndex + 1,
      statementType: analysis.statementType,
      complexityScore: analysis.complexityScore,
      writeTarget: analysis.writeTarget ?? null,
      tables: analysis.tables.length,
      joins: analysis.joins.length,
      filters: analysis.filters.length,
      derivedRelations: analysis.derivedRelations.length,
      flags: analysis.flags.map((flag) => ({
        severity: flag.severity,
        title: flag.title,
        description: flag.description,
        recommendation: recommendationForFlag(flag),
      })),
    })),
    highestSeverity,
  };
};

const printTextReport = (results, failOn) => {
  results.forEach((result, index) => {
    if (index > 0) {
      console.log('');
    }

    console.log(result.input === '-' ? 'stdin' : result.input);
    console.log(`Dialect: ${result.dialect}${result.detected.confident ? ` (${result.detected.evidence.join(', ')})` : ' (auto)'}`);
    if (result.schema) {
      console.log(
        `Schema metadata: ${result.schema.tableCount} tables, ${result.schema.foreignKeyCount} foreign keys, ${result.schema.indexedGroupCount} indexed groups`,
      );
    }

    if (result.diagnostics.length > 0) {
      console.log('Diagnostics:');
      result.diagnostics.forEach((diagnostic) => {
        console.log(
          `  - [${diagnostic.severity.toUpperCase()}] ${diagnostic.title} at ${diagnostic.line}:${diagnostic.column} — ${diagnostic.message}`,
        );
        console.log(`    Hint: ${diagnostic.hint}`);
      });
    }

    result.statements.forEach((statement) => {
      console.log(`Statement #${statement.index} · ${statement.statementType.toUpperCase()}`);
      console.log(
        `  Complexity ${statement.complexityScore} · ${statement.tables} tables · ${statement.joins} joins · ${statement.filters} filters · ${statement.derivedRelations} derived relations`,
      );
      if (statement.writeTarget) {
        console.log(`  Write target: ${statement.writeTarget}`);
      }

      if (statement.flags.length === 0) {
        console.log('  Flags: none');
        return;
      }

      console.log('  Flags:');
      statement.flags.forEach((flag) => {
        console.log(`    - [${flag.severity.toUpperCase()}] ${flag.title}`);
        console.log(`      ${flag.recommendation}`);
      });
    });
  });

  console.log('');
  console.log(`Fail threshold: ${failOn.toUpperCase()}`);
};

const buildSarifReport = (results, failOn) => {
  const rules = new Map();
  const sarifResults = [];

  const ensureRule = (id, name, description) => {
    if (rules.has(id)) {
      return;
    }

    rules.set(id, {
      id,
      name,
      shortDescription: {
        text: name,
      },
      fullDescription: {
        text: description,
      },
    });
  };

  results.forEach((result) => {
    const artifactUri = toArtifactUri(result.input);

    result.diagnostics.forEach((diagnostic) => {
      const ruleId = `diagnostic/${slugify(diagnostic.title) || 'diagnostic'}`;
      ensureRule(ruleId, diagnostic.title, diagnostic.hint || diagnostic.message);
      sarifResults.push({
        ruleId,
        level: diagnosticSarifLevel(diagnostic.severity),
        message: {
          text: `${diagnostic.message}${diagnostic.hint ? ` Hint: ${diagnostic.hint}` : ''}`,
        },
        locations: [
          {
            physicalLocation: {
              artifactLocation: {
                uri: artifactUri,
              },
              region: diagnostic.line
                ? {
                    startLine: diagnostic.line,
                    startColumn: diagnostic.column || 1,
                  }
                : undefined,
            },
          },
        ],
        properties: {
          category: 'diagnostic',
          dialect: result.dialect,
          severity: diagnostic.severity,
        },
      });
    });

    result.statements.forEach((statement) => {
      statement.flags.forEach((flag) => {
        const ruleId = `flag/${slugify(flag.title) || 'flag'}`;
        ensureRule(ruleId, flag.title, flag.recommendation);
        sarifResults.push({
          ruleId,
          level: flagSarifLevel(flag.severity),
          message: {
            text: `${flag.description} Recommendation: ${flag.recommendation}`,
          },
          locations: [
            {
              physicalLocation: {
                artifactLocation: {
                  uri: artifactUri,
                },
              },
            },
          ],
          properties: {
            category: 'flag',
            dialect: result.dialect,
            severity: flag.severity,
            statementIndex: statement.index,
            statementType: statement.statementType,
          },
        });
      });
    });
  });

  return {
    version: '2.1.0',
    $schema: 'https://json.schemastore.org/sarif-2.1.0.json',
    runs: [
      {
        tool: {
          driver: {
            name: 'queryviz-checks',
            informationUri: 'https://github.com/geamnegru/queryviz',
            rules: Array.from(rules.values()),
          },
        },
        invocations: [
          {
            executionSuccessful: true,
            properties: {
              failOn,
            },
          },
        ],
        results: sarifResults,
      },
    ],
  };
};

const main = async () => {
  const options = parseArguments(process.argv.slice(2));

  if (options.help) {
    printUsage();
    return;
  }

  if (!['text', 'json', 'sarif'].includes(options.format)) {
    throw new Error(`Unsupported --format "${options.format}". Use "text", "json", or "sarif".`);
  }

  if (!Object.hasOwn(severityRank, options.failOn)) {
    throw new Error(`Unsupported --fail-on value "${options.failOn}". Use none, low, medium, or high.`);
  }

  if (options.dialect && !isSupportedDialect(options.dialect)) {
    throw new Error(`Unsupported --dialect "${options.dialect}". Use one of: ${SUPPORTED_SQL_DIALECTS.join(', ')}`);
  }

  if (options.inputs.length === 0) {
    printUsage();
    throw new Error('Provide at least one SQL file, directory, or "-" for stdin.');
  }

  const inputFiles = Array.from(
    new Set(
      (
        await Promise.all(
          options.inputs.map((inputPath) =>
            expandInputPath(inputPath === '-' ? inputPath : path.resolve(process.cwd(), inputPath)),
          ),
        )
      ).flat(),
    ),
  );

  if (inputFiles.length === 0) {
    throw new Error('No SQL files found for the provided inputs.');
  }

  let schemaSummary = null;
  if (options.schemaPath) {
    const schemaSql = await readFile(path.resolve(process.cwd(), options.schemaPath), 'utf8');
    schemaSummary = parseSchemaInput(schemaSql).summary;
  }

  const results = await Promise.all(
    inputFiles.map((inputPath) => summarizeInput(inputPath, options, schemaSummary)),
  );

  if (options.format === 'json') {
    console.log(JSON.stringify({ results, failOn: options.failOn }, null, 2));
  } else if (options.format === 'sarif') {
    console.log(JSON.stringify(buildSarifReport(results, options.failOn), null, 2));
  } else {
    printTextReport(results, options.failOn);
  }

  const failThreshold = severityRank[options.failOn];
  if (failThreshold > 0 && results.some((result) => result.highestSeverity >= failThreshold)) {
    process.exitCode = 1;
  }
};

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
