# Frontend Cleanup Audit

This document captures concrete cleanup, consolidation, and refactor opportunities in `web/`, with the constraints from `web/AGENTS.md` in mind:

- keep the filesystem/backend as the source of truth
- preserve SSE-driven reconciliation
- avoid adding parallel state systems
- prefer composition over giant view-switch components
- preserve current canvas/editor/results interactions

Additional preferences:

- keep optimistic behavior where Monaco/editor content would otherwise produce cursor jumps or editing artifacts
- prefer representing durable UI/page state as route state whenever practical, especially selected settings entities and editor modes
- prefer directory-style route files over dot-style route files, e.g. `_workspace/settings/connections.tsx` over `_workspace.settings.connections.tsx`

The goal is not a rewrite. The goal is to reduce overlap, tighten state ownership, and make future frontend work safer.

## Highest-Priority Cleanup Opportunities

### 1. Unify inspect state and fetching

Files:

- `web/hooks/use-asset-results.ts`
- `web/hooks/use-asset-inspect.ts`
- `web/hooks/use-asset-previews.ts`
- `web/components/workspace-results-panel.tsx`

Current issue:

- selected-asset inspect and node-preview inspect still behave like two systems
- both paths deal with the same backend response shape, loading states, error handling, and column registration
- `useAssetInspect` is already moving toward shared state, but `useAssetResults` still owns selected inspect result presentation and action flow separately

Why this matters:

- inspect behavior is easy to drift between preview and full results
- fixes to caching, retries, errors, limits, or refresh behavior have to be repeated

Recommended cleanup:

- make one shared inspect source of truth own:
  - fetch/dedupe
  - cache entries
  - refresh semantics
  - loading/error normalization
  - limit expansion rules
- let preview and results panel only differ in rendering and selection behavior

### 2. Simplify selection ownership; reduce route + atom duplication

Files:

- `web/hooks/use-workspace-selection.ts`
- `web/lib/atoms/selection.ts`
- `web/components/workspace-layout.tsx`

Current issue:

- route search params and Jotai atoms both represent pipeline/asset selection
- selection is mirrored into atoms, then re-derived again from workspace contents
- fallback logic lives in multiple places

Why this matters:

- selection bugs become hard to reason about
- route state, local state, and workspace reconciliation can disagree temporarily

Recommended cleanup:

- keep URL search as the requested selection
- derive resolved selection from workspace in one direction only
- avoid route -> atom -> derived atom -> route loops

### 3. Remove or centralize ad hoc optimistic workspace mutations

Files:

- `web/hooks/use-editor-actions.ts`
- `web/hooks/use-workspace-sync.ts`

Current issue:

- some mutations rely purely on backend/SSE reconciliation
- some now patch `workspaceAtom` optimistically in-place
- this is inconsistent with the filesystem-first model

Why this matters:

- scattered optimism becomes a parallel truth path
- different mutations get different UX/state semantics

Recommended cleanup:

- preserve optimistic editor behavior where it prevents Monaco cursor jumping or content flicker
- centralize optimism rules instead of adding mutation-specific patches opportunistically
- distinguish clearly between:
  - content-edit optimism that is required for editor UX
  - metadata/config optimism that should usually defer to backend/SSE reconciliation

### 4. Split `WorkspacePage` into smaller orchestration units

File:

- `web/components/workspace-page.tsx`

Current issue:

- it coordinates graph state, inspect preview wiring, onboarding, dialogs, mobile editor behavior, form reset, pipeline run actions, recompute layout, and editor plumbing

Why this matters:

- any change to canvas, editor, preview, or mobile layout touches the same file
- the component is now the main coupling point for otherwise independent concerns

Recommended cleanup:

- extract composition helpers such as:
  - `useWorkspaceGraphController`
  - `useWorkspacePageActions`
  - `WorkspaceMobileEditorSheet`
  - `WorkspacePageResultsController`
- keep `WorkspacePage` as the orchestration shell, not the implementation body

### 5. Split `WorkspaceEditorPane` by responsibility

File:

- `web/components/workspace-editor-pane.tsx`

Current issue:

- Monaco lifecycle, SQL context wiring, form fields, visualization settings, connection warnings, debug tools, and side-effectful inference logic all live together

Why this matters:

- layout changes risk breaking data effects
- editor-specific side effects are hard to test and reason about

Recommended cleanup:

- move non-visual logic into hooks
- split sections into focused components, for example:
  - `AssetEditorHeader`
  - `AssetEditorActions`
  - `AssetConfigurationTab`
  - `AssetVisualizationTab`
  - `AssetDebugTab`
- keep Monaco/editor shell separate from metadata/config concerns

### 6. Consolidate API response/error/stream handling

File:

- `web/lib/api.ts`

Current issue:

- API helpers currently combine CRUD, config, inspect normalization, stream parsing, and ad hoc error extraction
- inspect/materialize/config errors are normalized in slightly different ways

Why this matters:

- UI-facing error handling keeps getting patched case by case
- stream endpoints and normal JSON endpoints do not follow one obvious pattern

Recommended cleanup:

- introduce small internal helpers for:
  - structured error extraction
  - JSON fetch with typed API error parsing
  - stream response handling
  - inspect output normalization
- keep endpoint exports thin

### 7. Decouple sidebar selection from expansion state

File:

- `web/components/workspace-sidebar.tsx`

Current issue:

- pipeline row click currently mixes navigation and expansion behavior
- chevron handling has separate stop-propagation logic

Why this matters:

- makes sidebar interaction fragile on desktop and mobile
- harder to reason about “selected pipeline” vs “expanded pipeline”

Recommended cleanup:

- make expansion an explicit disclosure action
- keep navigation/select behavior independent
- context menu and disclosure behavior should not rely on active-route side effects

### 8. Align settings architecture with workspace patterns

Files:

- `web/components/workspace-settings-split-view.tsx`
- `web/components/workspace-config-pane-layout.tsx`
- `web/components/workspace-connection-pane.tsx`
- `web/components/workspace-environment-pane.tsx`
- `web/src/routes/_workspace.settings.connections.tsx`
- `web/src/routes/_workspace.settings.environments.tsx`

Current issue:

- settings pages are developing their own layout/state conventions
- environment and connection pages already diverge in selection/mode/search ownership

Why this matters:

- settings can accidentally become a second frontend architecture
- mobile fixes and route behavior will continue to fork

Recommended cleanup:

- align settings with the same principles as the workspace:
  - route state for requested selection/mode
  - focused form hooks
  - shared pane shell
  - backend/SSE/config as authority
- push more settings state into route state where it represents page identity, such as:
  - selected environment
  - selected connection
  - editor mode
  - requested connection type

## Medium-Priority Cleanup Opportunities

### Global results state is not asset-scoped

Files:

- `web/lib/atoms/results.ts`
- `web/hooks/use-asset-results.ts`

Issue:

- inspect/materialize results are effectively global UI state
- selection changes can leave stale result context around

Cleanup direction:

- decide explicitly whether results are:
  - global console-like output
  - per-selected-asset output
- then encode that model in state instead of leaving it implicit

### Environment/config state is assembled from overlapping sources

Files:

- `web/hooks/use-workspace-settings-data.ts`
- `web/components/workspace-editor-pane.tsx`
- `web/lib/atoms/workspace.ts`

Issue:

- editor warnings and settings panes combine workspace-selected environment, config-selected environment, and route-selected environment

Cleanup direction:

- define one helper for “effective config environment” and reuse it everywhere

### Atom organization is uneven

Files:

- `web/lib/atoms/graph.ts`
- `web/lib/atoms/selection.ts`
- `web/lib/atoms/materialization.ts`
- `web/lib/atoms/results.ts`
- `web/lib/atoms/inspect.ts`

Issue:

- some atoms are grouped by domain, some by legacy usage
- e.g. `changedAssetIdsAtom` lives under graph even though it really drives inspect refresh

Cleanup direction:

- reorganize atoms by domain ownership:
  - workspace/selection
  - inspect/results
  - graph/layout
  - editor/form
  - settings/config

### Settings route/search state can be standardized further

Files:

- `web/src/routes/_workspace.settings.connections.tsx`
- `web/src/routes/_workspace.settings.environments.tsx`
- `web/hooks/use-workspace-config-selection.ts`

Issue:

- connections uses route search more heavily than environments
- mode/environment selection behavior is not fully symmetric

Cleanup direction:

- make settings pages follow one route-state contract
- use local state only for transient input, not for page identity
- when touching route files, prefer migrating toward directory-style route file organization instead of extending the dot-style layout

### Mobile layout strategy is inconsistent across app sections

Files:

- `web/components/workspace-page.tsx`
- `web/components/workspace-settings-split-view.tsx`

Issue:

- workspace uses sheet-style editor behavior on mobile
- settings currently uses stacked resizable panels

Cleanup direction:

- choose a shared mobile pattern for secondary editors/panes
- either stacked panels or sheets, but intentionally

## Files/Components That Are Too Large or Overloaded

### `web/components/workspace-page.tsx`

- main orchestration hotspot
- should become composition-only over time

### `web/components/workspace-editor-pane.tsx`

- too many UI and side-effect responsibilities mixed together

### `web/hooks/use-asset-inspect.ts`

- does a lot well, but owns too many concerns at once:
  - cache
  - request dedupe
  - refresh-on-change
  - pagination/limit policy
  - column registration

### `web/lib/api.ts`

- single file for nearly all API concerns
- strong candidate for internal helper extraction

### `web/components/workspace-connection-pane.tsx`

- form rendering, validation, field-type coercion, test-connection behavior, and secret handling all live together

### `web/lib/atoms/suggestion-catalog.ts`

- large derived-state pipeline that likely deserves decomposition into smaller reducers/builders

## Safe Incremental Refactor Plan

### Phase 1: Low-risk infrastructure cleanup

1. extract internal API helpers from `web/lib/api.ts`
2. centralize inspect error parsing and validation error parsing
3. normalize settings route search state contracts

### Phase 2: Inspect/state cleanup

4. unify inspect fetching/caching between previews and results
5. remove duplicated inspect-related column registration paths
6. move `changedAssetIdsAtom` and related inspect refresh logic into a clearer domain

### Phase 3: State ownership cleanup

7. simplify selection ownership so route state is authoritative
8. remove one-off optimistic workspace mutations or centralize them explicitly
9. define one helper for effective config environment resolution

### Phase 4: Component decomposition

10. split `WorkspacePage` into orchestration helpers
11. split `WorkspaceEditorPane` into focused subcomponents + hooks
12. split connection/environment panes into field rendering vs action shell

### Phase 5: Interaction consistency cleanup

13. decouple sidebar expansion from navigation
14. standardize mobile secondary-pane behavior across workspace/settings

## Notable Fragile Areas to Avoid Making Worse

### Do not add more parallel state layers

- avoid introducing new local mirrors of workspace state
- avoid route + atom + local state representing the same page identity

### Do not remove required Monaco optimism blindly

- optimistic editor content can be necessary to avoid cursor jumps and editing artifacts
- any cleanup in save/editor flows should preserve that UX, even if other metadata mutations become less optimistic

### Do not expand ad hoc optimistic behavior

- if optimism is needed, centralize it
- do not add more mutation-specific workspace patching

### Do not fork inspect behavior again

- preview inspect and full inspect should continue converging, not diverging

### Be very careful around flex/panel shrink behavior

Files:

- `web/components/workspace-editor-pane.tsx`
- `web/components/workspace-layout.tsx`
- `web/components/workspace-settings-split-view.tsx`
- `web/components/workspace-config-pane-layout.tsx`

Notes:

- keep `min-w-0` / `min-h-0` discipline
- validate both desktop and mobile pane behavior after any layout cleanup

### Do not let settings become a separate architecture

- keep settings aligned with workspace conventions for route state, backend authority, and focused hooks

## Good Existing Directions Worth Continuing

- the move toward shared inspect state in `web/hooks/use-asset-inspect.ts`
- keeping filesystem/backend as authority via SSE reconciliation
- route-based settings flows instead of hidden global settings state
- using focused hooks like `use-workspace-sync.ts`, `use-editor-actions.ts`, and settings form hooks

## Summary

The frontend does not need a large rewrite. The biggest wins come from:

1. unifying inspect state/fetching
2. simplifying selection ownership
3. reducing one-off optimistic mutations
4. splitting overloaded orchestration/components
5. standardizing settings/mobile patterns

If cleaned up in that order, the codebase should get easier to extend without breaking the SSE-driven, filesystem-first model described in `web/AGENTS.md`.
