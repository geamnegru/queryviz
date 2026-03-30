# Queryviz

> A fast, private, client-side SQL visualizer that turns pasted queries into graphs, clause scans, and query health hints.

**Runs 100% in your browser** · **No backend** · **No uploads**

---

## Why Queryviz?

Most SQL tools either execute the query, depend on a live database, or bury the structure inside walls of text.

Queryviz does the opposite: paste a query, and it instantly turns it into something you can actually inspect.

With Queryviz, you can:

- see how tables connect before reading every clause
- scan joins and filters faster
- spot suspicious patterns like `SELECT *` or function-wrapped filters
- explain complex SQL visually in demos, docs, or posts
- explore query shape without connecting to any database

---

## What it shows

Paste a SQL `SELECT` query and Queryviz generates:

- a visual relationship graph for tables and joins
- a join list for quick inspection
- a clause scan for `SELECT`, `FROM`, `JOIN`, `WHERE`, `GROUP BY`, `ORDER BY`, and `LIMIT`
- lightweight warnings based on query shape
- a complexity score based on joins, filters, grouping, aggregation, and subqueries

Everything happens locally in the browser.

| Queryviz | Typical SQL tooling |
| --- | --- |
| No database required | Usually requires one |
| No query execution | Often executes SQL |
| No backend | Often server-backed |
| Safe for demos and mock data | Can expose real environments |
| Instant visual structure | Usually text-first |

---

## Privacy, by design

Queryviz has no backend. There is no server, no database, no analytics pipeline, and no query execution layer.

- SQL stays in the browser
- No pasted query is uploaded anywhere
- Closing the tab clears the session
- The app works as a static parser and visualizer

This is not a policy page. It is how the app is built.

---

## Current MVP

The current version supports:

- top-level `SELECT` statement analysis
- multi-statement SQL files with statement selection
- table and alias detection
- selected column extraction
- join graph rendering directly in the browser
- Graphviz / DOT export for the selected statement
- pan, zoom, and draggable graph nodes
- heuristic flags for patterns like `SELECT *`, leading wildcard `LIKE`, and function-wrapped filters
- large local test files in `public/demo`

### Known limits

Queryviz currently does **not**:

- execute SQL
- validate schemas against a real database
- support every SQL dialect perfectly
- fully understand every edge case involving deeply nested SQL

---

## Tech stack

```text
Vite            - dev/build tooling
React           - interface layer
TypeScript      - parser and UI logic
Plain CSS       - custom layout and graph styling
```

---

## Getting started

```bash
git clone https://github.com/<your-username>/queryviz.git
cd queryviz
npm install
npm run dev
```

Open `http://localhost:5173` and paste a query.

### Build

```bash
npm run build
npm run preview
```

---

## Project structure

```text
src/
├── App.tsx
├── App.css
├── index.css
└── lib/
    └── analyzeSql.ts

public/
└── demo/
    ├── stress-test.sql
    └── wide-single-statement.sql
```
---

## Next steps

Good features to push this from MVP to star-worthy:

- SVG or PNG export for graph snapshots
- better CTE and nested subquery support
- smarter edge routing for dense graphs
- dialect toggles for Postgres / MySQL / SQLite
- EXPLAIN-style hints without needing a database
- copyable query summary cards for docs and social posts

---

## Contributing

Pull requests are welcome. If you want to improve parsing, graph layout, or SQL coverage, open an issue or ship a focused PR.

If Queryviz made a messy query easier to understand, leave a star.
