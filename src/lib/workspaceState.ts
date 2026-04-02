import { SUPPORTED_SQL_DIALECTS } from './analyzeSql';
import type { ReviewStatus, SavedQuery, SharedWorkspaceState, SqlDialect, WorkspaceViewState } from './types';

const SAVED_QUERIES_KEY = 'queryviz.savedQueries.v1';
const WORKSPACE_VIEW_STATE_KEY = 'queryviz.workspaceState.v1';
const MAX_SAVED_QUERIES = 10;
const MAX_WORKSPACE_VIEW_STATES = 18;
const DIALECT_OPTIONS = SUPPORTED_SQL_DIALECTS as readonly SqlDialect[];

const normalizeSpaces = (value: string) => value.replace(/\s+/g, ' ').trim();

const isSupportedDialect = (value: unknown): value is SqlDialect =>
  typeof value === 'string' && DIALECT_OPTIONS.includes(value as SqlDialect);

const encodeBase64Url = (value: string) => {
  const bytes = new TextEncoder().encode(value);
  let binary = '';

  bytes.forEach((byte) => {
    binary += String.fromCharCode(byte);
  });

  return btoa(binary).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/g, '');
};

const decodeBase64Url = (value: string) => {
  const normalized = value.replace(/-/g, '+').replace(/_/g, '/');
  const padding = '='.repeat((4 - (normalized.length % 4 || 4)) % 4);
  const binary = atob(`${normalized}${padding}`);
  const bytes = Uint8Array.from(binary, (char) => char.charCodeAt(0));
  return new TextDecoder().decode(bytes);
};

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === 'object' && value !== null && !Array.isArray(value);

const isPoint = (value: unknown): value is { x: number; y: number } =>
  isRecord(value) && typeof value.x === 'number' && typeof value.y === 'number';

const sanitizeNoteRecord = (value: unknown) => {
  if (!isRecord(value)) {
    return {};
  }

  return Object.fromEntries(
    Object.entries(value).filter((entry): entry is [string, string] => typeof entry[1] === 'string'),
  );
};

const sanitizeOffsets = (value: unknown) => {
  if (!isRecord(value)) {
    return {};
  }

  return Object.fromEntries(
    Object.entries(value).filter((entry): entry is [string, { x: number; y: number }] => isPoint(entry[1])),
  );
};

export const sanitizeReviewStatus = (value: unknown): ReviewStatus =>
  value === 'needs_changes' || value === 'approved' ? value : 'draft';

export const readSavedQueries = (): SavedQuery[] => {
  if (typeof window === 'undefined') {
    return [];
  }

  try {
    const stored = window.localStorage.getItem(SAVED_QUERIES_KEY);
    if (!stored) {
      return [];
    }

    const parsed = JSON.parse(stored);
    if (!Array.isArray(parsed)) {
      return [];
    }

    return parsed
      .filter((item): item is SavedQuery | (Omit<SavedQuery, 'dialect'> & { dialect?: SqlDialect }) =>
        typeof item?.id === 'string' &&
        typeof item?.title === 'string' &&
        typeof item?.sql === 'string' &&
        typeof item?.updatedAt === 'number' &&
        typeof item?.selectedStatementIndex === 'number' &&
        (item?.dialect === undefined || isSupportedDialect(item?.dialect)),
      )
      .map((item) => ({
        ...item,
        dialect: item.dialect ?? 'postgres',
      }));
  } catch {
    return [];
  }
};

export const persistSavedQueries = (queries: SavedQuery[]) => {
  if (typeof window === 'undefined') {
    return;
  }

  window.localStorage.setItem(SAVED_QUERIES_KEY, JSON.stringify(queries));
};

export const createQueryTitle = (sql: string) => {
  const normalized = normalizeSpaces(sql);

  if (!normalized) {
    return 'Untitled query';
  }

  return normalized.length > 56 ? `${normalized.slice(0, 56)}...` : normalized;
};

export const createSavedQueryId = () =>
  globalThis.crypto?.randomUUID?.() ?? `query-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;

export const upsertSavedQuery = (queries: SavedQuery[], nextEntry: SavedQuery) => {
  const existing = queries.find((item) => item.sql === nextEntry.sql);
  const nextList = existing
    ? queries.map((item) =>
        item.id === existing.id
          ? {
              ...item,
              title: nextEntry.title,
              updatedAt: nextEntry.updatedAt,
              selectedStatementIndex: nextEntry.selectedStatementIndex,
              dialect: nextEntry.dialect,
            }
          : item,
      )
    : [nextEntry, ...queries];

  return nextList
    .sort((left, right) => right.updatedAt - left.updatedAt)
    .slice(0, MAX_SAVED_QUERIES);
};

export const createShareUrl = (sql: string, statementIndex: number, dialect: SqlDialect) => {
  if (typeof window === 'undefined') {
    return '';
  }

  const params = new URLSearchParams();
  params.set('sql', encodeBase64Url(sql));
  params.set('statement', String(statementIndex));
  params.set('dialect', dialect);

  return `${window.location.origin}${window.location.pathname}#${params.toString()}`;
};

export const createReviewShareUrl = (workspace: SharedWorkspaceState) => {
  if (typeof window === 'undefined') {
    return '';
  }

  const params = new URLSearchParams();
  params.set('sql', encodeBase64Url(workspace.sql));
  params.set('statement', String(workspace.selectedStatementIndex));
  params.set('dialect', workspace.dialect);
  params.set('mode', workspace.mode);

  if (workspace.schemaSql.trim()) {
    params.set('schema', encodeBase64Url(workspace.schemaSql));
  }

  const noteEntries = Object.fromEntries(
    Object.entries(workspace.entityNotes).filter((entry) => entry[1].trim().length > 0),
  );

  if (workspace.reviewStatus !== 'draft' || workspace.reviewSummary.trim() || Object.keys(noteEntries).length > 0) {
    params.set(
      'review',
      encodeBase64Url(
        JSON.stringify({
          status: workspace.reviewStatus,
          summary: workspace.reviewSummary,
          notes: noteEntries,
        }),
      ),
    );
  }

  return `${window.location.origin}${window.location.pathname}#${params.toString()}`;
};

export const readSharedWorkspace = (): SharedWorkspaceState | null => {
  if (typeof window === 'undefined' || !window.location.hash) {
    return null;
  }

  const params = new URLSearchParams(window.location.hash.slice(1));
  const encodedSql = params.get('sql');
  if (!encodedSql) {
    return null;
  }

  try {
    const sql = decodeBase64Url(encodedSql);
    const selectedStatementIndex = Math.max(0, Number(params.get('statement') ?? '0') || 0);
    const rawDialect = params.get('dialect');
    const dialect = isSupportedDialect(rawDialect) ? rawDialect : 'postgres';
    const mode = params.get('mode') === 'review' ? 'review' : 'workspace';
    const schemaSql = params.get('schema') ? decodeBase64Url(params.get('schema') ?? '') : '';
    const rawReview = params.get('review');
    let reviewStatus: ReviewStatus = 'draft';
    let reviewSummary = '';
    let entityNotes: Record<string, string> = {};

    if (rawReview) {
      try {
        const parsedReview = JSON.parse(decodeBase64Url(rawReview));
        reviewStatus = sanitizeReviewStatus(parsedReview?.status);
        reviewSummary = typeof parsedReview?.summary === 'string' ? parsedReview.summary : '';
        entityNotes = sanitizeNoteRecord(parsedReview?.notes);
      } catch {
        reviewStatus = 'draft';
        reviewSummary = '';
        entityNotes = {};
      }
    }

    return { sql, selectedStatementIndex, dialect, schemaSql, mode, reviewStatus, reviewSummary, entityNotes };
  } catch {
    return null;
  }
};

const readWorkspaceViewStates = (): Record<string, WorkspaceViewState> => {
  if (typeof window === 'undefined') {
    return {};
  }

  try {
    const stored = window.localStorage.getItem(WORKSPACE_VIEW_STATE_KEY);
    if (!stored) {
      return {};
    }

    const parsed = JSON.parse(stored);
    if (!isRecord(parsed)) {
      return {};
    }

    return Object.fromEntries(
      Object.entries(parsed)
        .map(([key, rawValue]) => {
          if (!isRecord(rawValue)) {
            return null;
          }

          const layoutMode =
            rawValue.layoutMode === 'horizontal' ||
            rawValue.layoutMode === 'vertical' ||
            rawValue.layoutMode === 'radial'
              ? rawValue.layoutMode
              : 'horizontal';
          const dialect = isSupportedDialect(rawValue.dialect) ? rawValue.dialect : 'postgres';
          const zoom = typeof rawValue.zoom === 'number' ? rawValue.zoom : 1;
          const updatedAt = typeof rawValue.updatedAt === 'number' ? rawValue.updatedAt : 0;

          return [
            key,
            {
              layoutMode,
              dialect,
              schemaSql: typeof rawValue.schemaSql === 'string' ? rawValue.schemaSql : '',
              reviewStatus: sanitizeReviewStatus(rawValue.reviewStatus),
              reviewSummary: typeof rawValue.reviewSummary === 'string' ? rawValue.reviewSummary : '',
              expandedDerivedIds: Array.isArray(rawValue.expandedDerivedIds)
                ? rawValue.expandedDerivedIds.filter((item): item is string => typeof item === 'string')
                : [],
              nodeOffsets: sanitizeOffsets(rawValue.nodeOffsets),
              pan: isPoint(rawValue.pan) ? rawValue.pan : { x: 0, y: 0 },
              zoom,
              entityNotes: sanitizeNoteRecord(rawValue.entityNotes),
              compareSql: typeof rawValue.compareSql === 'string' ? rawValue.compareSql : '',
              compareExplainInput: typeof rawValue.compareExplainInput === 'string' ? rawValue.compareExplainInput : '',
              updatedAt,
            } satisfies WorkspaceViewState,
          ] as const;
        })
        .filter((entry): entry is readonly [string, WorkspaceViewState] => entry !== null),
    );
  } catch {
    return {};
  }
};

const persistWorkspaceViewStates = (states: Record<string, WorkspaceViewState>) => {
  if (typeof window === 'undefined') {
    return;
  }

  window.localStorage.setItem(WORKSPACE_VIEW_STATE_KEY, JSON.stringify(states));
};

export const createWorkspaceStateKey = (sql: string, statementIndex: number, dialect: SqlDialect) => {
  const normalized = normalizeSpaces(sql);
  if (!normalized) {
    return '';
  }

  const encoded = encodeBase64Url(`${dialect}:${statementIndex}:${normalized}`);
  return `ws-${encoded.slice(0, 72)}-${normalized.length}`;
};

export const saveWorkspaceViewState = (
  key: string,
  state: Omit<WorkspaceViewState, 'updatedAt'>,
) => {
  if (!key || typeof window === 'undefined') {
    return;
  }

  const current = readWorkspaceViewStates();
  current[key] = {
    ...state,
    updatedAt: Date.now(),
  };

  const trimmedEntries = Object.entries(current)
    .sort((left, right) => right[1].updatedAt - left[1].updatedAt)
    .slice(0, MAX_WORKSPACE_VIEW_STATES);

  persistWorkspaceViewStates(Object.fromEntries(trimmedEntries));
};

export const readWorkspaceViewState = (key: string) => {
  if (!key) {
    return null;
  }

  return readWorkspaceViewStates()[key] ?? null;
};
