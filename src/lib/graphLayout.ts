import type { EdgePort, GraphLayout, JoinRef, LayoutMode, NodeSide, TableRef } from './types';

export const NODE_WIDTH = 188;
export const NODE_HEIGHT = 92;

const clamp = (value: number, min: number, max: number) => Math.min(max, Math.max(min, value));

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

const createHorizontalLayout = (tables: TableRef[], joins: JoinRef[]): GraphLayout => {
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

const createVerticalLayout = (tables: TableRef[], joins: JoinRef[]): GraphLayout => {
  const depthMap = buildDepthMap(tables, joins);
  const rows = new Map<number, TableRef[]>();

  tables.forEach((table) => {
    const depth = depthMap.get(table.alias) ?? 0;
    const bucket = rows.get(depth) ?? [];
    bucket.push(table);
    rows.set(depth, bucket);
  });

  const sortedDepths = Array.from(rows.keys()).sort((left, right) => left - right);
  const maxRowSize = Math.max(...Array.from(rows.values()).map((bucket) => bucket.length), 1);
  const joinCount = Math.max(0, tables.length - 1);
  const columnGap = maxRowSize <= 3 ? 260 : maxRowSize <= 6 ? 228 : 204;
  const rowGap = joinCount <= 4 ? 210 : joinCount <= 9 ? 235 : 260;
  const leftPadding = 120;
  const topPadding = 100;
  const width = Math.max(1500, leftPadding * 2 + NODE_WIDTH + Math.max(0, maxRowSize - 1) * columnGap);
  const height = Math.max(960, topPadding * 2 + NODE_HEIGHT + Math.max(0, sortedDepths.length - 1) * rowGap);
  const positions: Record<string, { x: number; y: number }> = {};

  sortedDepths.forEach((depth, rowIndex) => {
    const bucket = rows.get(depth) ?? [];
    const bucketWidth = NODE_WIDTH + columnGap * Math.max(bucket.length - 1, 0);
    const startX = Math.max(leftPadding, (width - bucketWidth) / 2);

    bucket.forEach((table, columnIndex) => {
      positions[table.alias] = {
        x: startX + columnIndex * columnGap,
        y: topPadding + rowIndex * rowGap,
      };
    });
  });

  return { width, height, positions };
};

const createRadialLayout = (tables: TableRef[], joins: JoinRef[]): GraphLayout => {
  const depthMap = buildDepthMap(tables, joins);
  const rings = new Map<number, TableRef[]>();

  tables.forEach((table) => {
    const depth = depthMap.get(table.alias) ?? 0;
    const bucket = rings.get(depth) ?? [];
    bucket.push(table);
    rings.set(depth, bucket);
  });

  const sortedDepths = Array.from(rings.keys()).sort((left, right) => left - right);
  const maxDepth = Math.max(...sortedDepths, 0);
  const ringGap = maxDepth <= 1 ? 220 : maxDepth <= 3 ? 196 : 172;
  const padding = 240;
  const width = Math.max(1480, padding * 2 + NODE_WIDTH + maxDepth * ringGap * 2);
  const height = Math.max(1120, padding * 2 + NODE_HEIGHT + maxDepth * ringGap * 2);
  const centerX = width / 2;
  const centerY = height / 2;
  const positions: Record<string, { x: number; y: number }> = {};

  sortedDepths.forEach((depth) => {
    const bucket = rings.get(depth) ?? [];

    if (depth === 0) {
      bucket.forEach((table) => {
        positions[table.alias] = {
          x: centerX - NODE_WIDTH / 2,
          y: centerY - NODE_HEIGHT / 2,
        };
      });
      return;
    }

    const radius = depth * ringGap;
    const step = bucket.length <= 1 ? 0 : (Math.PI * 2) / bucket.length;
    const startAngle = bucket.length <= 1 ? -Math.PI / 2 : -Math.PI / 2 + (depth % 2 === 0 ? 0 : step / 2);

    bucket.forEach((table, index) => {
      const angle = startAngle + step * index;

      positions[table.alias] = {
        x: centerX + Math.cos(angle) * radius - NODE_WIDTH / 2,
        y: centerY + Math.sin(angle) * radius - NODE_HEIGHT / 2,
      };
    });
  });

  return { width, height, positions };
};

export const createNodeLayout = (tables: TableRef[], joins: JoinRef[], layoutMode: LayoutMode): GraphLayout => {
  if (layoutMode === 'vertical') {
    return createVerticalLayout(tables, joins);
  }

  if (layoutMode === 'radial') {
    return createRadialLayout(tables, joins);
  }

  return createHorizontalLayout(tables, joins);
};

const getPreferredSide = (source: { x: number; y: number }, target: { x: number; y: number }): NodeSide => {
  const dx = target.x + NODE_WIDTH / 2 - (source.x + NODE_WIDTH / 2);
  const dy = target.y + NODE_HEIGHT / 2 - (source.y + NODE_HEIGHT / 2);

  if (Math.abs(dx) >= Math.abs(dy)) {
    return dx >= 0 ? 'right' : 'left';
  }

  return dy >= 0 ? 'bottom' : 'top';
};

const getSideVector = (side: NodeSide) => {
  if (side === 'left') {
    return { x: -1, y: 0 };
  }

  if (side === 'right') {
    return { x: 1, y: 0 };
  }

  if (side === 'top') {
    return { x: 0, y: -1 };
  }

  return { x: 0, y: 1 };
};

const getAnchorPoint = (
  position: { x: number; y: number },
  side: NodeSide,
  index: number,
  count: number,
) => {
  if (side === 'left' || side === 'right') {
    const step = NODE_HEIGHT / (count + 1);

    return {
      x: side === 'right' ? position.x + NODE_WIDTH : position.x,
      y: position.y + step * (index + 1),
    };
  }

  const step = NODE_WIDTH / (count + 1);

  return {
    x: position.x + step * (index + 1),
    y: side === 'bottom' ? position.y + NODE_HEIGHT : position.y,
  };
};

const getBezierMidpoint = (
  start: { x: number; y: number },
  controlOne: { x: number; y: number },
  controlTwo: { x: number; y: number },
  end: { x: number; y: number },
) => {
  const t = 0.5;
  const inverse = 1 - t;

  return {
    x:
      inverse ** 3 * start.x +
      3 * inverse ** 2 * t * controlOne.x +
      3 * inverse * t ** 2 * controlTwo.x +
      t ** 3 * end.x,
    y:
      inverse ** 3 * start.y +
      3 * inverse ** 2 * t * controlOne.y +
      3 * inverse * t ** 2 * controlTwo.y +
      t ** 3 * end.y,
  };
};

export const createEdgePortMap = (
  joins: JoinRef[],
  positions: Record<string, { x: number; y: number }>,
  nodeOffsets: Record<string, { x: number; y: number }>,
) => {
  const sides = new Map<string, { sourceSide: NodeSide; targetSide: NodeSide }>();
  const sourceBuckets = new Map<string, JoinRef[]>();
  const targetBuckets = new Map<string, JoinRef[]>();

  joins.forEach((join) => {
    const sourceBase = positions[join.sourceAlias] ?? { x: 0, y: 0 };
    const targetBase = positions[join.targetAlias] ?? { x: 0, y: 0 };
    const sourceOffset = nodeOffsets[join.sourceAlias] ?? { x: 0, y: 0 };
    const targetOffset = nodeOffsets[join.targetAlias] ?? { x: 0, y: 0 };
    const source = { x: sourceBase.x + sourceOffset.x, y: sourceBase.y + sourceOffset.y };
    const target = { x: targetBase.x + targetOffset.x, y: targetBase.y + targetOffset.y };
    const sourceSide = getPreferredSide(source, target);
    const targetSide = getPreferredSide(target, source);

    sides.set(join.id, { sourceSide, targetSide });

    const sourceKey = `${join.sourceAlias}:${sourceSide}`;
    const targetKey = `${join.targetAlias}:${targetSide}`;
    sourceBuckets.set(sourceKey, [...(sourceBuckets.get(sourceKey) ?? []), join]);
    targetBuckets.set(targetKey, [...(targetBuckets.get(targetKey) ?? []), join]);
  });

  const sortBucket = (
    bucket: JoinRef[],
    getSide: (join: JoinRef) => NodeSide,
    getCounterpart: (join: JoinRef) => { x: number; y: number },
  ) => {
    bucket.sort((left, right) => {
      const side = getSide(left);
      const leftPoint = getCounterpart(left);
      const rightPoint = getCounterpart(right);

      return side === 'left' || side === 'right'
        ? leftPoint.y - rightPoint.y
        : leftPoint.x - rightPoint.x;
    });
  };

  sourceBuckets.forEach((bucket) => {
    sortBucket(
      bucket,
      (join) => sides.get(join.id)?.sourceSide ?? 'right',
      (join) => {
        const base = positions[join.targetAlias] ?? { x: 0, y: 0 };
        const offset = nodeOffsets[join.targetAlias] ?? { x: 0, y: 0 };
        return { x: base.x + offset.x, y: base.y + offset.y };
      },
    );
  });

  targetBuckets.forEach((bucket) => {
    sortBucket(
      bucket,
      (join) => sides.get(join.id)?.targetSide ?? 'left',
      (join) => {
        const base = positions[join.sourceAlias] ?? { x: 0, y: 0 };
        const offset = nodeOffsets[join.sourceAlias] ?? { x: 0, y: 0 };
        return { x: base.x + offset.x, y: base.y + offset.y };
      },
    );
  });

  const portMap = new Map<string, EdgePort>();

  joins.forEach((join) => {
    const sourceBase = positions[join.sourceAlias] ?? { x: 0, y: 0 };
    const targetBase = positions[join.targetAlias] ?? { x: 0, y: 0 };
    const sourceOffset = nodeOffsets[join.sourceAlias] ?? { x: 0, y: 0 };
    const targetOffset = nodeOffsets[join.targetAlias] ?? { x: 0, y: 0 };
    const source = { x: sourceBase.x + sourceOffset.x, y: sourceBase.y + sourceOffset.y };
    const target = { x: targetBase.x + targetOffset.x, y: targetBase.y + targetOffset.y };
    const currentSides = sides.get(join.id) ?? { sourceSide: 'right' as const, targetSide: 'left' as const };
    const sourceKey = `${join.sourceAlias}:${currentSides.sourceSide}`;
    const targetKey = `${join.targetAlias}:${currentSides.targetSide}`;
    const outgoingEdges = sourceBuckets.get(sourceKey) ?? [join];
    const incomingEdges = targetBuckets.get(targetKey) ?? [join];
    const outgoingIndex = Math.max(0, outgoingEdges.findIndex((item) => item.id === join.id));
    const incomingIndex = Math.max(0, incomingEdges.findIndex((item) => item.id === join.id));
    const start = getAnchorPoint(source, currentSides.sourceSide, outgoingIndex, outgoingEdges.length);
    const end = getAnchorPoint(target, currentSides.targetSide, incomingIndex, incomingEdges.length);
    const distance = Math.hypot(end.x - start.x, end.y - start.y);
    const span = clamp(distance * 0.34, 84, 220);
    const sourceVector = getSideVector(currentSides.sourceSide);
    const targetVector = getSideVector(currentSides.targetSide);
    const controlOne = {
      x: start.x + sourceVector.x * span,
      y: start.y + sourceVector.y * span,
    };
    const controlTwo = {
      x: end.x + targetVector.x * span,
      y: end.y + targetVector.y * span,
    };
    const labelPoint = getBezierMidpoint(start, controlOne, controlTwo, end);

    portMap.set(join.id, {
      startX: start.x,
      startY: start.y,
      endX: end.x,
      endY: end.y,
      controlOneX: controlOne.x,
      controlOneY: controlOne.y,
      controlTwoX: controlTwo.x,
      controlTwoY: controlTwo.y,
      labelX: labelPoint.x,
      labelY: labelPoint.y - 10,
    });
  });

  return portMap;
};
