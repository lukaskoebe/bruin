# Bruin Web Onboarding Implementation Plan

## Goal

Implement a skippable, full-page onboarding flow in Bruin Web that guides a first-time user through:

1. choosing a warehouse / connection type
2. entering and validating connection details
3. importing assets into the current workspace using Bruin's import capability

The onboarding must respect the current Bruin Web architecture:

- the filesystem remains the source of truth
- all writes go through the Go backend
- normal SSE/workspace refresh behavior reconciles the final UI state
- the implementation should reuse existing connection/config infrastructure where possible

## Existing Building Blocks

The current codebase already provides several pieces we should reuse directly.

### Frontend

- `web/components/workspace-connection-pane.tsx`
- `web/components/workspace-connection-form-fields.tsx`
- `web/hooks/use-workspace-connection-form.ts`
- `web/lib/api-config.ts`
- `web/components/workspace-main-content.tsx`
- `web/components/workspace-page.tsx`

Relevant existing capabilities:

- create/update/delete/test workspace connections
- provider-specific connection field rendering
- settings/config state already fetched from the backend
- mobile sheet / desktop split-view patterns already exist

### Backend

- `internal/web/httpapi/config.go`
- `internal/web/service/config.go`
- `internal/web/httpapi/run.go`
- `internal/web/service/run.go`
- existing Bruin CLI import command support in the broader codebase

Relevant existing capabilities:

- `/api/config`
- `/api/config/connections`
- `/api/config/connections/test`
- ad hoc command execution seam already exists via `/api/run`

## Product Scope For V1

The first version should stay intentionally narrow.

### Included

- full-page onboarding experience for first-time/empty-workspace users
- skippable flow
- provider selection step with a curated list of common warehouses
- connection form step using existing connection field definitions
- required successful connection test before continuing to import
- import flow that uses Bruin's existing import functionality through the backend
- success handoff back into the normal workspace UI

### Excluded From V1

- multi-connection onboarding in one pass
- advanced environment management during onboarding
- non-database onboarding paths
- a custom importer implementation separate from Bruin import
- background import job orchestration beyond the current request/stream model

## Recommended User Flow

### Step 0: Entry Decision

Show onboarding when all of the following are true:

- the user has not explicitly skipped onboarding for the current browser/session state
- the workspace has no configured connections, or the workspace is effectively empty and we want to bias toward setup

Initial heuristic for V1:

- show onboarding if there are zero configured connections across environments

This should be driven by backend config/workspace state, not by a separate frontend-only truth.

### Step 1: Choose Connection Type

Show large, opinionated cards for common warehouse types:

- Postgres
- BigQuery
- Snowflake
- DuckDB
- Redshift
- Databricks

Also include:

- a secondary "More connection types" list if needed later
- `Skip for now`
- `Continue`

Output of this step:

- selected connection type

### Step 2: Configure Connection

Reuse the current connection field rendering and save mechanics.

Required UI elements:

- connection name
- provider-specific fields
- `Test connection`
- validation status area
- `Back`
- `Continue`
- `Skip for now`

Behavior:

- save the connection through the existing config endpoints
- test using `/api/config/connections/test`
- gate forward progress on a successful test in V1

Output of this step:

- saved connection name
- saved environment name, probably default for V1

### Step 3: Import Assets

This step should explain what import does in clear terms.

Required UI elements:

- selected connection summary
- target pipeline choice:
  - create a new pipeline, or
  - import into an existing pipeline if one exists
- import scope fields depending on what the backend can support cleanly in V1
- `Import assets`
- `Back`
- `Skip for now`

V1 simplification recommendation:

- keep import inputs small and aligned with Bruin import's existing flags
- prefer a form that captures the minimal viable import intent instead of building a complex database object browser immediately

Possible V1 inputs:

- connection name
- target pipeline name/path
- schema / dataset / database
- optional table filter pattern

Output of this step:

- backend runs Bruin import
- workspace refresh/SSE updates surface newly created pipeline/assets
- onboarding transitions to success state

### Step 4: Success State

Show a simple success page:

- connection created
- import completed summary
- `Open workspace`

Then hand control back to the normal workspace UI.

## Implementation Plan

## Phase 1: Backend Onboarding API Surface

Add a narrow onboarding-specific backend surface instead of forcing the frontend to compose everything itself.

Recommended endpoints:

### `GET /api/onboarding/state`

Purpose:

- tell the frontend whether onboarding should be shown
- provide curated connection options and current config summary

Suggested response:

```json
{
  "should_show": true,
  "has_connections": false,
  "connection_types": [
    { "type": "postgres", "label": "Postgres" },
    { "type": "bigquery", "label": "BigQuery" }
  ],
  "default_environment": "default"
}
```

Implementation notes:

- build from existing config service data
- do not create a separate persistence model just for onboarding

### `POST /api/onboarding/import`

Purpose:

- wrap Bruin import through a backend-owned request shape

Suggested request shape:

```json
{
  "connection_name": "warehouse",
  "environment_name": "default",
  "pipeline_name": "analytics",
  "schema": "public",
  "pattern": "customer_*"
}
```

Suggested response shape:

```json
{
  "status": "ok",
  "pipeline_path": "analytics",
  "created_assets": 12,
  "message": "Imported 12 assets into analytics"
}
```

Implementation notes:

- use `internal/web/service/run.go` or a dedicated onboarding/import service seam
- prefer a dedicated service wrapper over calling `/api/run` directly from the frontend
- keep command construction server-side

## Phase 2: Frontend Onboarding Shell

Add a dedicated onboarding component rather than embedding the flow inside the current settings pane.

Suggested new files:

- `web/components/workspace-onboarding.tsx`
- `web/components/workspace-onboarding-step-choose-connection.tsx`
- `web/components/workspace-onboarding-step-configure-connection.tsx`
- `web/components/workspace-onboarding-step-import.tsx`
- `web/hooks/use-workspace-onboarding.ts`
- `web/lib/api-onboarding.ts`

Responsibilities:

- load onboarding state
- manage current onboarding step
- manage skip/resume state
- bridge to existing config APIs

Placement:

- gate the normal workspace main content in `workspace-page.tsx`
- if onboarding is active, render the full-page onboarding instead of the usual main workspace content

## Phase 3: Reuse Connection Form Infrastructure

Do not build a second connection form system.

Recommended approach:

- extract the reusable save/test logic from the current connection pane if needed
- reuse `WorkspaceConnectionFormFields` directly or create a thin onboarding-specific wrapper around it
- keep request payloads identical to existing config APIs

Possible small extraction if needed:

- move connection save/test orchestration into a shared hook usable by both settings and onboarding

## Phase 4: Import Step

Build the import step around a minimal server-owned request format.

Recommended first version UX:

- input for target pipeline name
- input for schema/dataset
- optional filter/pattern input
- import button with loading state

This avoids building an expensive schema browser before we have the core onboarding working.

If later needed, object browsing can become a second-phase enhancement.

## Phase 5: Skip / Resume Semantics

V1 persistence recommendation:

- store skip state in local storage
- compute whether onboarding is still relevant from backend state on every load

Behavior:

- if user skips and still has zero connections, do not force the full-page flow again immediately in the same browser
- show a lightweight CTA in the empty state or settings to restart onboarding

Future enhancement:

- optionally persist onboarding completion in workspace config metadata if workspace-level memory becomes important

## Phase 6: Validation And E2E

Add focused live coverage for:

- onboarding appears when there are zero connections
- connection type selection advances correctly
- connection form save/test flow works
- skip path returns user to workspace
- import request calls backend and transitions to success

Mobile coverage should be included because onboarding will likely rely on the same sheet/popup primitives and full-page responsive layout rules.

## Architecture Notes

### Why not use `/api/run` directly from the frontend?

Because onboarding should remain product-owned and stable even if CLI command details change.

The frontend should not know:

- exact import command construction
- argument ordering
- output parsing details

That belongs in the backend.

### Why full-page instead of modal?

Because this flow is a primary first-run task, not a secondary detail edit. A full-page layout allows:

- clearer step progression
- larger provider cards
- more room for provider-specific forms
- easier mobile support

## Proposed Delivery Order

1. add onboarding plan doc
2. add backend onboarding state + import endpoints
3. add frontend onboarding shell and entry gating
4. wire connection step using existing config APIs
5. wire import step using new onboarding import endpoint
6. add skip/resume behavior
7. add focused live Playwright coverage

## Open Questions

These do not block the first implementation pass, but they should be kept explicit.

1. Should onboarding appear only when there are zero connections, or also for empty workspaces with existing connections?
2. Should the import step create a new pipeline by default, or prefer importing into the currently selected pipeline when one exists?
3. Which connection types should be promoted as first-class cards in V1?
4. Should successful connection creation immediately persist even if the user skips before import?
5. Should import run as a normal request/response flow in V1, or stream output like materialize does?

## Recommended V1 Defaults

Unless product requirements change, use these defaults:

- show onboarding when there are zero configured connections
- use `default` environment automatically
- save the connection during step 2
- require a successful connection test before step 3
- create a new pipeline during import by default
- use a normal request/response import endpoint for the first version
- persist skip state locally in the browser
