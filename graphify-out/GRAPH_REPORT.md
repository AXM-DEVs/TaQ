# Graph Report - TaQ  (2026-09-30)

## Corpus Check
- 19 files · ~12,943 words
- Verdict: corpus is large enough that graph structure adds value.
- Unclassified: 5 file(s) not represented in the graph (top: (none) 3, .example 1, .toml 1)

## Summary
- 115 nodes · 121 edges · 16 communities (11 shown, 5 thin omitted)
- Extraction: 100% EXTRACTED · 0% INFERRED · 0% AMBIGUOUS
- Token cost: 0 input · 0 output

## Community Hubs (Navigation)
- main.rs
- lib.rs
- What You Must Do When Invoked
- /graphify
- graphify reference: extra exports and benchmark
- ADR 0001: Arquitectura y stack de la Fase 0
- graphify reference: query, path, explain
- TaQ
- graphify reference: add a URL and watch a folder
- graphify reference: commit hook and native CLAUDE.md integration
- graphify reference: incremental update and cluster-only
- graphify reference: GitHub clone and cross-repo merge
- graphify reference: transcribe video and audio
- AGENTS.md
- extraction-spec.md
- taq-core

## God Nodes (most connected - your core abstractions)
1. `What You Must Do When Invoked` - 12 edges
2. `/graphify` - 10 edges
3. `graphify reference: extra exports and benchmark` - 8 edges
4. `account_result()` - 7 edges
5. `collect_one()` - 7 edges
6. `main()` - 7 edges
7. `ADR 0001: Arquitectura y stack de la Fase 0` - 6 edges
8. `SignalKind` - 5 edges
9. `Signal` - 5 edges
10. `graphify reference: query, path, explain` - 5 edges

## Surprising Connections (you probably didn't know these)
- `taq-loadgen` --depends_on--> `taq-core`  [EXTRACTED]
  crates/taq-loadgen/Cargo.toml → crates/taq-core/Cargo.toml

## Import Cycles
- None detected.

## Communities (16 total, 5 thin omitted)

### Community 0 - "main.rs"
Cohesion: 0.16
Nodes (8): account_result(), collect_one(), Config, event_id(), main(), MAX_IN_FLIGHT, now_ms(), parse_args()

### Community 1 - "lib.rs"
Cohesion: 0.15
Nodes (7): Action, Event, Signal, SignalKind, Anomaly, Burst, Forecast

### Community 2 - "What You Must Do When Invoked"
Cohesion: 0.13
Nodes (15): Part A - Structural extraction for code files, Part B - Semantic extraction (parallel subagents), Part C - Merge AST + semantic into final extraction, Step 0 - GitHub repos and multi-path merge (only if a URL or several paths), Step 1 - Ensure graphify is installed, Step 2.5 - Video and audio (only if video files detected), Step 2 - Detect files, Step 3 - Extract entities and relationships (+7 more)

### Community 3 - "/graphify"
Cohesion: 0.20
Nodes (9): For /graphify add and --watch, For /graphify query, For the commit hook and native CLAUDE.md integration, For --update and --cluster-only, /graphify, Honesty Rules, Interpreter guard for subcommands, Usage (+1 more)

### Community 4 - "graphify reference: extra exports and benchmark"
Cohesion: 0.22
Nodes (8): graphify reference: extra exports and benchmark, Step 6b - Wiki (only if --wiki flag), Step 7 - Neo4j export (only if --neo4j or --neo4j-push flag), Step 7a - FalkorDB export (only if --falkordb or --falkordb-push flag), Step 7b - SVG export (only if --svg flag), Step 7c - GraphML export (only if --graphml flag), Step 7d - MCP server (only if --mcp flag), Step 8 - Token reduction benchmark (only if total_words > 5000)

### Community 5 - "ADR 0001: Arquitectura y stack de la Fase 0"
Cohesion: 0.29
Nodes (6): ADR 0001: Arquitectura y stack de la Fase 0, Consecuencias, Contexto, Decisión, Decisión abierta: deduplicación, Imágenes verificadas

### Community 6 - "graphify reference: query, path, explain"
Cohesion: 0.33
Nodes (5): For /graphify explain, For /graphify path, graphify reference: query, path, explain, Step 0 — Constrained query expansion (REQUIRED before traversal), Step 1 — Traversal

### Community 7 - "TaQ"
Cohesion: 0.40
Nodes (4): Arranque, Estructura, Requisitos, TaQ

### Community 8 - "graphify reference: add a URL and watch a folder"
Cohesion: 0.50
Nodes (3): For /graphify add, For --watch, graphify reference: add a URL and watch a folder

### Community 9 - "graphify reference: commit hook and native CLAUDE.md integration"
Cohesion: 0.50
Nodes (3): For git commit hook, For native CLAUDE.md integration, graphify reference: commit hook and native CLAUDE.md integration

### Community 10 - "graphify reference: incremental update and cluster-only"
Cohesion: 0.50
Nodes (3): For --cluster-only, For --update (incremental re-extraction), graphify reference: incremental update and cluster-only

## Knowledge Gaps
- **56 isolated node(s):** `taq-core`, `Burst`, `Anomaly`, `Forecast`, `taq-loadgen` (+51 more)
  These have ≤1 connection - possible missing edges or undocumented components. (Counts symbols only; 83 node(s) total have ≤1 connection when file, concept and rationale nodes are included.)
- **5 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `What You Must Do When Invoked` connect `What You Must Do When Invoked` to `/graphify`?**
  _High betweenness centrality (0.035) - this node is a cross-community bridge._
- **Why does `/graphify` connect `/graphify` to `What You Must Do When Invoked`?**
  _High betweenness centrality (0.027) - this node is a cross-community bridge._
- **What connects `taq-core`, `Burst`, `Anomaly` to the rest of the system?**
  _56 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `What You Must Do When Invoked` be split into smaller, more focused modules?**
  _Cohesion score 0.13333333333333333 - nodes in this community are weakly interconnected._