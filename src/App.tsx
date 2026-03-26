import {
  useEffect,
  useMemo,
  useRef,
  useState,
  type ChangeEvent,
  type PointerEvent as ReactPointerEvent,
} from 'react';
import './App.css';
import { analyzeSql, extractStatements, type JoinRef, type Severity, type TableRef } from './lib/analyzeSql';

const SAMPLE_SQL = `SELECT
  o.id,
  o.created_at,
  c.name AS customer_name,
  SUM(oi.quantity * oi.unit_price) AS total_revenue,
  COUNT(DISTINCT p.id) AS product_count
FROM orders o
INNER JOIN customers c ON c.id = o.customer_id
LEFT JOIN order_items oi ON oi.order_id = o.id
LEFT JOIN products p ON p.id = oi.product_id
WHERE o.created_at >= DATE_TRUNC('month', CURRENT_DATE)
  AND LOWER(c.country) = 'romania'
GROUP BY o.id, o.created_at, c.name
ORDER BY total_revenue DESC
LIMIT 50;`;

const NODE_WIDTH = 188;
const NODE_HEIGHT = 92;

interface GraphLayout {
  width: number;
  height: number;
  positions: Record<string, { x: number; y: number }>;
}

interface EdgePort {
  startX: number;
  startY: number;
  endX: number;
  endY: number;
  controlOneX: number;
  controlTwoX: number;
  labelX: number;
  labelY: number;
}

const severityLabel: Record<Severity, string> = {
  high: 'Needs attention',
  medium: 'Worth reviewing',
  low: 'Nice to double-check',
};

const summarizeJoin = (join: JoinRef) => `${join.type} ${join.alias} on ${join.condition}`;

const summarizeStatement = (statement: string, index: number) => {
  const normalized = statement.replace(/\s+/g, ' ').trim();
  return `#${index + 1} ${normalized.slice(0, 78)}${normalized.length > 78 ? '...' : ''}`;
};

const buildDepthMap = (tables: TableRef[], joins: JoinRef[]) => {
  const depthMap = new Map<string, number>();
  const source = tables[0];

  if (source) {
    depthMap.set(source.alias, 0);
  }

  for (let iteration = 0; iteration < tables.length + joins.length; iteration += 1) {
    let changed = false;

    joins.forEach((join) => {
      const sourceDepth = depthMap.get(join.sourceAlias);
      const targetDepth = depthMap.get(join.targetAlias);
      const nextDepth = (sourceDepth ?? 0) + 1;

      if (targetDepth === undefined || nextDepth > targetDepth) {
        depthMap.set(join.targetAlias, nextDepth);
        changed = true;
      }
    });

    if (!changed) {
      break;
    }
  }

  tables.forEach((table, index) => {
    if (!depthMap.has(table.alias)) {
      depthMap.set(table.alias, index === 0 ? 0 : 1);
    }
  });

  return depthMap;
};

const createNodeLayout = (tables: TableRef[], joins: JoinRef[]): GraphLayout => {
  const depthMap = buildDepthMap(tables, joins);
  const columns = new Map<number, TableRef[]>();

  tables.forEach((table) => {
    const depth = depthMap.get(table.alias) ?? 0;
    const bucket = columns.get(depth) ?? [];
    bucket.push(table);
    columns.set(depth, bucket);
  });

  const sortedDepths = Array.from(columns.keys()).sort((left, right) => left - right);
  const maxColumnSize = Math.max(...Array.from(columns.values()).map((bucket) => bucket.length), 1);
  const joinCount = Math.max(0, tables.length - 1);
  const columnGap = joinCount <= 4 ? 320 : joinCount <= 9 ? 360 : 410;
  const rowGap = maxColumnSize <= 3 ? 170 : maxColumnSize <= 6 ? 195 : 225;
  const leftPadding = 110;
  const topPadding = 100;
  const width = Math.max(1500, leftPadding + sortedDepths.length * columnGap + NODE_WIDTH + 200);
  const height = Math.max(960, topPadding * 2 + (maxColumnSize - 1) * rowGap + NODE_HEIGHT);
  const positions: Record<string, { x: number; y: number }> = {};

  sortedDepths.forEach((depth, columnIndex) => {
    const bucket = columns.get(depth) ?? [];
    const bucketHeight = NODE_HEIGHT + rowGap * Math.max(bucket.length - 1, 0);
    const startY = Math.max(topPadding, (height - bucketHeight) / 2);

    bucket.forEach((table, rowIndex) => {
      positions[table.alias] = {
        x: leftPadding + columnIndex * columnGap,
        y: startY + rowIndex * rowGap,
      };
    });
  });

  return { width, height, positions };
};

const createEdgePortMap = (
  joins: JoinRef[],
  positions: Record<string, { x: number; y: number }>,
  nodeOffsets: Record<string, { x: number; y: number }>,
) => {
  const outgoing = new Map<string, JoinRef[]>();
  const incoming = new Map<string, JoinRef[]>();

  joins.forEach((join) => {
    outgoing.set(join.sourceAlias, [...(outgoing.get(join.sourceAlias) ?? []), join]);
    incoming.set(join.targetAlias, [...(incoming.get(join.targetAlias) ?? []), join]);
  });

  outgoing.forEach((list, alias) => {
    list.sort((left, right) => {
      const leftNode = positions[left.targetAlias] ?? { x: 0, y: 0 };
      const rightNode = positions[right.targetAlias] ?? { x: 0, y: 0 };
      return leftNode.y - rightNode.y;
    });
    outgoing.set(alias, list);
  });

  incoming.forEach((list, alias) => {
    list.sort((left, right) => {
      const leftNode = positions[left.sourceAlias] ?? { x: 0, y: 0 };
      const rightNode = positions[right.sourceAlias] ?? { x: 0, y: 0 };
      return leftNode.y - rightNode.y;
    });
    incoming.set(alias, list);
  });

  const portMap = new Map<string, EdgePort>();

  joins.forEach((join) => {
    const sourceBase = positions[join.sourceAlias] ?? { x: 0, y: 0 };
    const targetBase = positions[join.targetAlias] ?? { x: 0, y: 0 };
    const sourceOffset = nodeOffsets[join.sourceAlias] ?? { x: 0, y: 0 };
    const targetOffset = nodeOffsets[join.targetAlias] ?? { x: 0, y: 0 };
    const source = { x: sourceBase.x + sourceOffset.x, y: sourceBase.y + sourceOffset.y };
    const target = { x: targetBase.x + targetOffset.x, y: targetBase.y + targetOffset.y };
    const outgoingEdges = outgoing.get(join.sourceAlias) ?? [join];
    const incomingEdges = incoming.get(join.targetAlias) ?? [join];
    const outgoingIndex = Math.max(0, outgoingEdges.findIndex((item) => item.id === join.id));
    const incomingIndex = Math.max(0, incomingEdges.findIndex((item) => item.id === join.id));
    const startStep = NODE_HEIGHT / (outgoingEdges.length + 1);
    const endStep = NODE_HEIGHT / (incomingEdges.length + 1);
    const startX = source.x + NODE_WIDTH;
    const startY = source.y + startStep * (outgoingIndex + 1);
    const endX = target.x;
    const endY = target.y + endStep * (incomingIndex + 1);
    const span = Math.max(120, (endX - startX) * 0.42);

    portMap.set(join.id, {
      startX,
      startY,
      endX,
      endY,
      controlOneX: startX + span,
      controlTwoX: endX - span,
      labelX: (startX + endX) / 2,
      labelY: (startY + endY) / 2 - 10,
    });
  });

  return portMap;
};

function App() {
  const [sql, setSql] = useState(SAMPLE_SQL);
  const [selectedStatementIndex, setSelectedStatementIndex] = useState(0);
  const [zoom, setZoom] = useState(1);
  const [pan, setPan] = useState({ x: 0, y: 0 });
  const [draggingNode, setDraggingNode] = useState<string | null>(null);
  const [nodeOffsets, setNodeOffsets] = useState<Record<string, { x: number; y: number }>>({});
  const [lastPoint, setLastPoint] = useState<{ x: number; y: number } | null>(null);
  const [isPanning, setIsPanning] = useState(false);
  const fileInputRef = useRef<HTMLInputElement | null>(null);
  const graphShellRef = useRef<HTMLDivElement | null>(null);

  const statements = useMemo(() => extractStatements(sql), [sql]);
  const safeSelectedStatementIndex = statements.length === 0
    ? 0
    : Math.min(selectedStatementIndex, statements.length - 1);

  const analysis = useMemo(() => analyzeSql(sql, safeSelectedStatementIndex), [sql, safeSelectedStatementIndex]);
  const layout = useMemo(() => createNodeLayout(analysis.tables, analysis.joins), [analysis.tables, analysis.joins]);

  const positionedTables = analysis.tables.map((table) => {
    const base = layout.positions[table.alias] ?? { x: 220, y: 220 };
    const offset = nodeOffsets[table.alias] ?? { x: 0, y: 0 };
    return {
      ...table,
      x: base.x + offset.x,
      y: base.y + offset.y,
    };
  });

  const edgePortMap = useMemo(
    () => createEdgePortMap(analysis.joins, layout.positions, nodeOffsets),
    [analysis.joins, layout.positions, nodeOffsets],
  );

  useEffect(() => {
    const element = graphShellRef.current;
    if (!element) {
      return undefined;
    }

    const handleWheel = (event: WheelEvent) => {
      event.preventDefault();
      event.stopPropagation();

      setZoom((current) => {
        const nextZoom = Math.min(2.2, Math.max(0.45, current - event.deltaY * 0.001));
        return Number(nextZoom.toFixed(2));
      });
    };

    element.addEventListener('wheel', handleWheel, { passive: false });

    return () => {
      element.removeEventListener('wheel', handleWheel);
    };
  }, []);

  const beginCanvasPan = (event: ReactPointerEvent<HTMLDivElement>) => {
    if ((event.target as HTMLElement).closest('.graph-node')) {
      return;
    }

    setIsPanning(true);
    setLastPoint({ x: event.clientX, y: event.clientY });
  };

  const handlePointerMove = (event: ReactPointerEvent<HTMLDivElement>) => {
    if (draggingNode && lastPoint) {
      const dx = (event.clientX - lastPoint.x) / zoom;
      const dy = (event.clientY - lastPoint.y) / zoom;
      setNodeOffsets((current) => {
        const previous = current[draggingNode] ?? { x: 0, y: 0 };
        return {
          ...current,
          [draggingNode]: { x: previous.x + dx, y: previous.y + dy },
        };
      });
      setLastPoint({ x: event.clientX, y: event.clientY });
      return;
    }

    if (isPanning && lastPoint) {
      const dx = event.clientX - lastPoint.x;
      const dy = event.clientY - lastPoint.y;
      setPan((current) => ({ x: current.x + dx, y: current.y + dy }));
      setLastPoint({ x: event.clientX, y: event.clientY });
    }
  };

  const endPointerAction = () => {
    setDraggingNode(null);
    setIsPanning(false);
    setLastPoint(null);
  };

  const beginNodeDrag = (alias: string, event: ReactPointerEvent<HTMLDivElement>) => {
    event.stopPropagation();
    setDraggingNode(alias);
    setLastPoint({ x: event.clientX, y: event.clientY });
  };

  const resetView = () => {
    setZoom(1);
    setPan({ x: 0, y: 0 });
    setNodeOffsets({});
  };

  const handleSqlChange = (event: ChangeEvent<HTMLTextAreaElement>) => {
    setSql(event.target.value);
    setNodeOffsets({});
    setPan({ x: 0, y: 0 });
    setZoom(1);
  };

  const handleFileChange = async (event: ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (!file) {
      return;
    }

    const nextSql = await file.text();
    setSql(nextSql);
    setSelectedStatementIndex(0);
    setNodeOffsets({});
    setPan({ x: 0, y: 0 });
    setZoom(1);
    event.target.value = '';
  };

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="logo-panel logo-panel--textonly">
          <strong className="brand-wordmark">QUERYVIZ</strong>
        </div>

        <div className="editor-panel">
          <div className="panel-head">
            <span>SQL input</span>
            <strong>{sql.length} chars</strong>
          </div>
          <textarea
            className="sql-input"
            value={sql}
            onChange={handleSqlChange}
            spellCheck={false}
            placeholder="Paste a SELECT query here..."
          />
          <div className="editor-actions">
            <button type="button" onClick={() => setSql(SAMPLE_SQL)}>
              Load sample
            </button>
            <button type="button" onClick={() => fileInputRef.current?.click()}>
              Open .sql
            </button>
            <button type="button" onClick={() => setSql('')}>
              Clear
            </button>
          </div>
          <input ref={fileInputRef} className="hidden-file-input" type="file" accept=".sql,.txt" onChange={handleFileChange} />
          {statements.length > 1 ? (
            <p className="editor-note">Detected {statements.length} SQL statements. Pick one below to graph it.</p>
          ) : null}
        </div>

        {statements.length > 1 ? (
          <section className="statement-panel">
            <div className="panel-head">
              <span>Statements</span>
              <strong>{statements.length}</strong>
            </div>
            <div className="statement-list">
              {statements.map((statement, index) => (
                <button
                  key={`${index}-${statement.slice(0, 24)}`}
                  type="button"
                  className={`statement-item${safeSelectedStatementIndex === index ? ' statement-item--active' : ''}`}
                  onClick={() => {
                    setSelectedStatementIndex(index);
                    setNodeOffsets({});
                    setPan({ x: 0, y: 0 });
                    setZoom(1);
                  }}
                >
                  {summarizeStatement(statement, index)}
                </button>
              ))}
            </div>
          </section>
        ) : null}

        <div className="sidebar-grid">
          <section className="info-card">
            <div className="panel-head">
              <span>Metrics</span>
              <strong>{analysis.complexityScore}</strong>
            </div>
            <ul>
              <li>Tables: {analysis.tables.length}</li>
              <li>Joins: {analysis.joins.length}</li>
              <li>Filters: {analysis.filters.length}</li>
              <li>Columns: {analysis.columns.length}</li>
            </ul>
          </section>

          <section className="info-card info-card--flags-left">
            <div className="panel-head">
              <span>Flags</span>
              <strong>{analysis.flags.length}</strong>
            </div>
            <div className="flag-list compact">
              {analysis.flags.length > 0 ? (
                analysis.flags.slice(0, 5).map((flag) => (
                  <article key={flag.title} className={`flag flag--${flag.severity}`}>
                    <strong>{flag.title}</strong>
                    <span>{severityLabel[flag.severity]}</span>
                  </article>
                ))
              ) : (
                <article className="flag flag--clear">
                  <strong>No major warnings</strong>
                  <span>Parser looks happy</span>
                </article>
              )}
            </div>
          </section>
        </div>
      </aside>

      <main className="canvas-panel">
        <section className="workspace-grid">
          <div className="graph-card">
            <div className="canvas-toolbar">
              <div className="canvas-toolbar__title-group">
                <span className="canvas-toolbar__label">Graph</span>
                <strong>{analysis.joins.length > 0 ? `Statement #${analysis.analyzedStatementIndex + 1}` : 'Waiting for joins'}</strong>
              </div>
              <div className="canvas-toolbar__controls">
                <button type="button" onClick={() => setZoom((value) => Math.max(0.45, Number((value - 0.1).toFixed(2))))}>
                  -
                </button>
                <span>{Math.round(zoom * 100)}%</span>
                <button type="button" onClick={() => setZoom((value) => Math.min(2.2, Number((value + 0.1).toFixed(2))))}>
                  +
                </button>
                <button type="button" onClick={resetView}>
                  Reset view
                </button>
              </div>
            </div>

            <div
              ref={graphShellRef}
              className="graph-shell"
              onPointerDown={beginCanvasPan}
              onPointerMove={handlePointerMove}
              onPointerUp={endPointerAction}
              onPointerLeave={endPointerAction}
            >
              <div
                className="graph-viewport"
                style={{
                  width: `${layout.width}px`,
                  height: `${layout.height}px`,
                  transform: `translate(${pan.x}px, ${pan.y}px) scale(${zoom})`,
                }}
              >
                <svg className="graph-edges" width={layout.width} height={layout.height} viewBox={`0 0 ${layout.width} ${layout.height}`}>
                  {analysis.joins.map((join) => {
                    const edge = edgePortMap.get(join.id);
                    if (!edge) {
                      return null;
                    }

                    const path = `M ${edge.startX} ${edge.startY} C ${edge.controlOneX} ${edge.startY}, ${edge.controlTwoX} ${edge.endY}, ${edge.endX} ${edge.endY}`;

                    return (
                      <g key={join.id}>
                        <path d={path} className="graph-edge-path" />
                        <text x={edge.labelX} y={edge.labelY} className="graph-edge-label">
                          {join.type}
                        </text>
                      </g>
                    );
                  })}
                </svg>

                {positionedTables.map((table) => (
                  <div
                    key={table.alias}
                    className={`graph-node ${table.role === 'source' ? 'graph-node--source' : ''}`}
                    style={{ transform: `translate(${table.x}px, ${table.y}px)` }}
                    onPointerDown={(event) => beginNodeDrag(table.alias, event)}
                  >
                    <span className="graph-node__role">{table.role === 'source' ? 'source' : 'join'}</span>
                    <strong>{table.name}</strong>
                    <p>alias: {table.alias}</p>
                  </div>
                ))}
              </div>

              <div className="graph-hint">Drag empty space to pan. Scroll to zoom. Drag boxes to rearrange.</div>
            </div>
          </div>

          <aside className="detail-rail">
            <article className="bottom-card rail-card">
              <div className="panel-head">
                <span>Join list</span>
                <strong>{analysis.joins.length}</strong>
              </div>
              <div className="bottom-card__content mono-list">
                {analysis.joins.length > 0 ? (
                  analysis.joins.map((join) => <div key={join.id}>{summarizeJoin(join)}</div>)
                ) : (
                  <div>No joins detected.</div>
                )}
              </div>
            </article>

            <article className="bottom-card rail-card">
              <div className="panel-head">
                <span>Clause scan</span>
                <strong>{analysis.clauses.filter((clause) => clause.present).length}</strong>
              </div>
              <div className="bottom-card__content clause-list compact">
                {analysis.clauses.map((clause) => (
                  <div key={clause.label} className={`clause-chip ${clause.present ? 'is-on' : 'is-off'}`}>
                    <strong>{clause.label}</strong>
                    <span>{clause.detail}</span>
                  </div>
                ))}
              </div>
            </article>
          </aside>
        </section>
      </main>
    </div>
  );
}

export default App;
