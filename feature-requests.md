# Feature Requests

This document tracks feature work that is still needed, items that need confirmation, and a prioritized set of work packages that can be implemented independently with minimal collision.

## Notes And Corrections

- `Store position of nodes in local storage` does not appear to work reliably in practice and should remain on the list.
- `Keeping the materialize tab selectable without prior output` is still not fully solved. After the page finishes loading and inspect calls settle, the materialize tab can still become unavailable unless a materialize result exists.
- `Inspect is shown when 0 rows are returned` works for the inspect pane, but not yet for the node preview table.
- `Configure connections and environments in a connection environment editor panel` is already done.
- `Prevent creating a new asset that overlaps with another one` still matters for duplicate name/path prevention.
- `Prevent creating a new asset that overlaps with another one` for duplicate SQL/output target prevention is already implemented.
- `Change the query type selector (duckdb, postgres, etc.) from free-text input to an explicit selector` is already done.
- `Integrated terminal with AI/MCP` should stay in a maybe section for now.

## Highest Priority Fixes

- Show the current pipeline name in the editor header instead of the static `Asset Editor` title, with inline rename support:
  - default state shows pipeline name plus edit button
  - edit state shows input plus save button
  - save on button click and `Enter`
- Keep existing inspect/preview table data visible while refresh/load-more requests are in flight and show an inline spinner instead of replacing the table.
- Add shared load-more support to both inspect pane and node preview table.
- Keep the materialize tab selectable even before any output exists and show a placeholder state.
- Fix node-position persistence so canvas positions are actually restored from local storage.
- Ensure node preview also handles 0-row inspect results cleanly instead of behaving like “no data”.
- Prevent duplicate asset creation by name/path.

## Product / UX Features

- Add drag/drop and paste support for files or paths (`parquet`, `csv`, `s3://...`) to auto-create assets.
- Add smooth camera transition when a newly selected canvas node becomes active.
- Show a friendly already-saved message on `Ctrl+S`.
- Add rename functionality directly inside the SQL editor.
- Add `Ctrl+Enter` in the SQL editor to inspect the selected asset.
- Add a search feature for asset name and asset content.
- Add SQL auto-formatting.
- Add different highlighting for schema name, table name, and column name.
- Add a connection delete confirmation dialog.
- Require at least one connection during onboarding before the rest of the page is usable.

## Inspect, Preview, Materialize, And Visualization

- Batch the initial preview/inspect requests on first load instead of firing many individual `/inspect` calls.
- Make the preview table a proper data table and share a common table component between inspect and preview.
- Reuse inspect-table value color coding in node previews, especially for booleans and possibly dates.
- Make the inspect view horizontally scrollable.
- Mark assets as failed when materialization fails.
- Add a loading animation to an asset while it is materializing.
- Add a materialize button to each asset card/node, visible on hover.
- Ensure the visible-columns visualization setting affects the rendered visualization.
- Ensure the preview-table load-more control remains visible when horizontally scrolled on wide tables, while still only appearing near the bottom.
- Improve canvas drag performance when very wide preview tables are visible.

## Asset, Query, And Metadata Semantics

- Properly rename assets with SQL refactoring support.
- Clean up asset dependency metadata when queries change.
- Clean up column metadata when asset queries change.
- Ensure inspect respects the effective query limit when the query itself contains a smaller `LIMIT` than the inspect request limit.

## SQL, Paths, And Query Intelligence

- Autocomplete S3 paths and local paths inside DuckDB queries.
- Add SQL autocomplete for column values, at least for DuckDB, via distinct-value queries.
- Add a proper SQL language server or deeper SQL parsing to improve intellisense.

## Canvas, Graph, And Node Presentation

- Show ingestr assets as `source_icon -> target_icon`.
- Redesign ingestr nodes so they use two large icons with an arrow between them instead of the current generic asset-node treatment.
- Remove row/materialization info from ingestr nodes because it does not make sense there.
- Increase the minimum zoom-out level.
- Remove or hide the `React Flow` attribution/footer text if licensing/configuration allows.
- Fix vertical alignment issues between nodes with different widths.

## Connections, Variables, Templates, Incrementals

- Automatically create an S3 connection from local S3 configuration.
- Improve handling of Jinja templates.
- Improve handling of variables.
- Improve handling of incremental assets.

## Server, Packaging, Platform, And Quality

- Upgrade to the latest Vite version.
- Refactor server code to reuse more Bruin internal functions and align more closely with an MVC-style structure.
- Ensure frontend assets from `web/dist` are embedded into the resulting Bruin binary rather than depending on a runtime `--static-dir` directory.
- Add end-to-end tests.
- Run a security audit.
- Replace or standardize forms around shadcn forms plus TanStack Forms.

## Maybe

- Add an integrated interactive terminal that launches the MCP server so Claude Code or similar tools can run from inside the UI.

## Initial Concepts

### Lineage Tab With Column-Level Statistics

Possible first version:

- add a `Lineage` tab next to inspect/materialize for the selected asset
- compute stats from the selected asset plus immediate upstreams/downstreams only
- first implementation can be metadata-driven and cheap:
  - column names
  - inferred lineage links when SQL parser can resolve them
  - nullability / primary key / type if known
  - simple profile info from inspect rows when available: null count, distinct-ish count, boolean distribution, min/max for numeric/date-like values
- avoid full warehouse-wide lineage or expensive background computation initially
- degrade gracefully when SQL parsing cannot resolve columns

### Variables Handling

Possible first version:

- introduce a dedicated variables model for the web UI, separate from raw text editing concerns
- show resolved variable sources for the selected environment:
  - default/global
  - environment override
  - runtime override if applicable
- provide validation and preview of rendered values in context
- surface unresolved/missing variables directly in editor diagnostics and asset detail panes

### Incremental Assets Handling

Possible first version:

- detect incremental assets explicitly in asset metadata and editor UI
- show incremental-specific controls in the configuration panel:
  - strategy
  - key / merge condition
  - update columns
  - last materialized status if known
- add validation that incremental configuration is internally consistent
- add dedicated materialize/inspect messaging so users understand when they are seeing partial vs rebuilt state

## Prioritized Independent Work Packages

These are grouped so they can be worked on mostly independently without colliding.

### Package A: Results And Table UX

Scope:

- keep data visible during refresh/load-more
- shared load-more support in inspect + preview
- make materialize tab always selectable
- fix 0-row preview behavior
- unify inspect/preview table component

Touches mostly:

- results panel
- preview table components
- inspect hooks/state
- node preview rendering

Avoids direct collision with:

- server MVC work
- settings/forms work
- node visual redesign

### Package B: Pipeline Header And Editor Shortcuts

Scope:

- pipeline name header with inline rename
- save on `Enter`
- `Ctrl+S` saved message
- `Ctrl+Enter` inspect shortcut
- editor-side rename entry point

Touches mostly:

- editor header
- editor pane
- keyboard handling
- pipeline rename actions

Avoids direct collision with:

- inspect batching
- API restructuring
- graph performance work

### Package C: Graph Persistence And Motion

Scope:

- fix local-storage node position persistence
- smooth transition to selected node
- zoom-out tuning
- node vertical alignment improvements

Touches mostly:

- graph controller
- persisted positions hook
- viewport focus hook
- layout math

Avoids direct collision with:

- settings
- server
- SQL intelligence

### Package D: Ingestr Node Redesign

Scope:

- source icon -> target icon presentation
- custom ingestr node layout
- remove irrelevant materialization/row info for ingestr nodes

Touches mostly:

- asset node rendering
- asset type icon mapping
- graph node data shape

Avoids direct collision with:

- inspect batching
- editor/header work
- server work

### Package E: Inspect / Preview Backend Efficiency

Scope:

- batch initial inspect calls
- fix inspect-limit semantics vs query limit
- add better support for progressive preview loading

Touches mostly:

- web inspect APIs
- server inspect endpoints
- inspect state hooks

Avoids direct collision with:

- settings/forms
- ingestr node redesign

### Package F: Asset Integrity And Refactoring

Scope:

- duplicate asset name/path prevention
- proper asset rename with SQL refactoring
- dependency cleanup when queries change
- column metadata cleanup when queries change

Touches mostly:

- asset creation/update flows
- SQL parser/refactor logic
- dependency metadata sync

Avoids direct collision with:

- graph persistence
- settings
- packaging

### Package G: SQL Intelligence And Path UX

Scope:

- S3/local path autocomplete in DuckDB
- column-value autocomplete
- deeper SQL parsing / language-server work
- schema/table/column highlighting
- SQL formatting

Touches mostly:

- Monaco providers
- SQL parser integration
- SQL discovery APIs

Avoids direct collision with:

- results/materialize UI
- onboarding/settings

### Package H: Onboarding And Connection Quality

Scope:

- require at least one connection during onboarding
- auto-create S3 connection from local config
- connection delete confirmation dialog

Touches mostly:

- onboarding flow
- config/settings UI
- connection APIs

Avoids direct collision with:

- graph/node work
- SQL language tooling

### Package I: Binary Packaging And Server Architecture

Scope:

- embed `web/dist` into the Bruin binary
- stop relying on runtime static-dir for packaged builds
- refactor web server code toward better internal-function reuse / MVC shape

Touches mostly:

- `cmd/web.go`
- static asset serving
- server package structure

Avoids direct collision with:

- most frontend-only work packages

### Package J: Testing, Audit, And Platform Hardening

Scope:

- end-to-end tests
- security audit
- Vite upgrade
- forms standardization

Touches mostly:

- test harness
- CI/build tooling
- shared form infrastructure

Avoids direct collision with:

- most feature-specific UI work if scheduled afterward
