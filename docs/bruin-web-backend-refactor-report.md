# Bruin Web Backend Refactor Report

## Purpose

This document started as an architecture analysis for `cmd/web.go` and `internal/web`, and now also tracks the concrete refactor progress. It still describes the target architecture and migration priorities, but it additionally records what has already been moved out of `cmd/web.go` and what remains.

## Progress Snapshot

The refactor is active and materially underway. `cmd/web.go` is still the composition root and still contains some transport DTOs and Bruin Web-specific HTTP handlers, but a substantial amount of backend behavior has already moved into canonical `internal/web` packages.

Completed extractions so far:

- workspace refresh and asset resolution now go through `internal/web/service/workspace.go`
- config and settings endpoints now use `internal/web/service/config.go` and `internal/web/httpapi/config.go`
- workspace and event endpoints now use `internal/web/httpapi/workspace.go`
- pipeline CRUD now uses `internal/web/service/pipeline.go` and `internal/web/httpapi/pipeline.go`
- execution transport now uses `internal/web/httpapi/execution.go`
- asset CRUD and SQL format flows now use `internal/web/service/asset.go` and `internal/web/httpapi/assets.go`
- pipeline execution and materialization endpoints now use `internal/web/httpapi/pipeline_execution.go`
- asset column management now uses `internal/web/service/asset_columns.go` and `internal/web/httpapi/asset_columns.go`
- execution business logic now lives in `internal/web/service/execution.go`
- asset rename dependency refactor and deferred SQL patch scheduling now live in `internal/web/service/asset.go`
- materialization inspection, freshness evaluation, query JSON parsing, and connection-query helpers now live in `internal/web/service/execution.go`
- SQL database discovery, table discovery, table-column introspection, and column-values execution now use `internal/web/service/sql.go` and `internal/web/httpapi/sql.go`
- ingestr suggestions and SQL path suggestions now use `internal/web/service/suggestions.go` and `internal/web/httpapi/suggestions.go`
- SQL parse-context now uses `internal/web/service/parse_context.go` and `internal/web/httpapi/parse_context.go`
- ad hoc command execution via `/api/run` now uses `internal/web/service/run.go` and `internal/web/httpapi/run.go`
- workspace snapshot storage, revisioning, watcher suppression, change fan-out, and changed-asset ID calculation now center on `internal/web/service/workspace_coordinator.go`

Current canonical ownership:

- HTTP transport: `internal/web/httpapi`
- workspace/config/pipeline/asset/execution services: `internal/web/service`
- SSE hub: `internal/web/events`
- freshness tracking: `internal/web/freshness`
- static asset serving: `internal/web/static`
- file watching: `internal/web/watch`

What still remains in `cmd/web.go`:

- process bootstrap and dependency wiring
- route registration and adapter methods used by `internal/web/httpapi`
- Bruin Web-specific DTO conversions between internal service/model types and the existing web response shapes
- a small compatibility helper layer retained mainly for `cmd/web_test.go`

Recent cleanup notes:

- dead SQL discovery / suggestion response DTOs were removed from `cmd/web.go`
- dead duplicate dialect/wrapper helpers were removed from `cmd/web.go`
- dead downstream-expansion runtime helpers were removed from `cmd/web.go` once workspace coordination moved to `internal/web/service/workspace_coordinator.go`
- remaining compatibility shims used mainly by `cmd/web_test.go` were moved into `cmd/web_compat.go`, leaving `cmd/web.go` more focused on composition and active adapters
- the remaining top-level DTOs in `cmd/web.go` are still part of the active web-state conversion path, so moving them further right now would add churn without materially shrinking responsibilities
- workspace snapshot storage and publish/suppression coordination owned by `webServer`

Verification status for the refactor batches to date:

- targeted backend tests have been kept green
- `go test ./internal/web/...` has been kept green
- live Playwright E2E has been used as the main regression check after each substantial batch

## Executive Summary

The Bruin Web backend already contains the beginnings of a clean internal package structure, but the real architecture still lives inside `cmd/web.go`. Today, `cmd/web.go` acts simultaneously as:

- CLI entrypoint and process bootstrap
- dependency wiring and composition root
- HTTP router and transport layer
- request/response DTO layer
- workspace state store
- workspace parsing service
- config editing service
- asset/pipeline CRUD service
- command execution gateway
- SQL discovery/intelligence adapter
- SSE publisher and change fan-out coordinator
- watcher suppression and debounce coordinator
- materialization freshness tracker integration

That concentration of concerns creates the main design problem: the server is difficult to reason about, difficult to test in isolation, and prone to drift because some of the same responsibilities also already exist under `internal/web`.

The highest-value architectural move is not local cleanup inside `cmd/web.go`; it is to finish the extraction that has already started and make `cmd/web.go` a thin composition root while `internal/web` becomes the canonical application boundary.

## Scope

Primary focus:

- `cmd/web.go`
- `internal/web/api`
- `internal/web/events`
- `internal/web/freshness`
- `internal/web/model`
- `internal/web/service`
- `internal/web/sqlintelligence`
- `internal/web/static`
- `internal/web/watch`

Secondary context:

- integration points with `pkg/config`, `pkg/pipeline`, `pkg/git`, `pkg/query`, `pkg/sqlparser`, and CLI subprocess execution

## Current Architecture

## What Exists Today

### `cmd/web.go` is the de facto application

`cmd/web.go` defines the primary server state object, bootstraps dependencies, registers routes, computes workspace state, owns request handlers, publishes SSE updates, and coordinates internal mutable state. The concentration is visible immediately in the `webServer` definition at `cmd/web.go:357` and the route registration block at `cmd/web.go:526`.

The server struct currently owns unrelated concerns in one place:

- workspace configuration (`workspaceRoot`, `staticDir`, watcher settings)
- mutable workspace snapshot (`stateMu`, `state`, `revision`) at `cmd/web.go:364`
- patch/debounce state (`patchMu`, `patchTimers`) at `cmd/web.go:368`
- SSE hub, runner, freshness tracker at `cmd/web.go:371`
- DuckDB concurrency coordination at `cmd/web.go:375`
- watcher suppression for self-generated writes at `cmd/web.go:378`

This is a strong sign of an application object that has accumulated multiple subsystem responsibilities instead of orchestrating narrower services.

### `internal/web` already contains pieces of a better architecture

There are several promising packages under `internal/web`:

- `internal/web/events/hub.go`: SSE pub/sub with debounce
- `internal/web/watch/watcher.go`: filesystem watching abstraction
- `internal/web/freshness/tracker.go`: in-memory materialization/content freshness tracking
- `internal/web/static/handler.go`: static asset serving abstraction
- `internal/web/service/runner.go`: CLI command execution abstraction
- `internal/web/service/workspace.go`: workspace state computation and asset resolution service
- `internal/web/model/dto.go`: DTOs for workspace, assets, pipelines, commands, and inspect responses
- `internal/web/api/response.go`: response envelope helpers

These packages are the right kind of building blocks, but they are only partially adopted.

## The Central Architectural Problem: Split-Brain Design

The current codebase has two parallel architectures:

1. the monolithic implementation in `cmd/web.go`
2. the partially extracted implementation in `internal/web`

This is the most important structural issue in the backend.

### Example: workspace state computation is duplicated

- `cmd/web.go:599` implements `computeWorkspaceState`
- `internal/web/service/workspace.go:94` implements `ComputeState`

The implementations are materially similar: both load `.bruin.yml`, both scan pipeline paths, both build pipeline summaries, both convert asset metadata, and both accumulate parse errors.

### Example: asset resolution is duplicated

- `cmd/web.go:1987` implements `resolveAssetByID`
- `internal/web/service/workspace.go:196` implements `ResolveAssetByID`

### Example: DTOs are duplicated

- request and response structs are defined throughout `cmd/web.go`
- overlapping canonical DTOs already exist in `internal/web/model/dto.go:6`

### Example: API response helpers are duplicated

- local helpers `writeJSON` and `writeAPIError` exist in `cmd/web.go:709` and `cmd/web.go:715`
- standardized helpers exist in `internal/web/api/response.go:24`

This is more than cosmetic duplication. It creates four concrete risks:

- logic divergence over time
- inconsistent API behavior
- duplicated tests or untested drift
- slower refactors because authors must first discover which implementation is actually canonical

## Main Anti-Patterns

## 1. God File / God Object

`cmd/web.go` is too large and too broad in responsibility. The file mixes transport code, domain logic, filesystem logic, config mutation logic, orchestration logic, and eventing logic.

The practical symptoms are:

- very large handler methods with substantial business logic
- difficulty following a request path end-to-end
- high cognitive load for changes
- low unit-testability because dependencies are not isolated
- no clean service seams for business use cases

This is the dominant maintainability issue.

## 2. Transport and Domain Logic Are Interleaved

Many handlers do all of the following inline:

- decode HTTP input
- validate request fields
- resolve assets/pipelines from disk
- mutate files directly
- parse workspace state again
- invoke subprocesses
- publish SSE updates
- build final JSON responses

For example:

- asset CRUD and asset mutation logic in `cmd/web.go:1288`, `cmd/web.go:1451`, `cmd/web.go:1967`
- SQL parse-context and SQL column-value endpoints in `cmd/web.go:2588` and `cmd/web.go:2651`
- workspace config editing endpoints in `cmd/web.go:753` onward

This produces thick handlers. In idiomatic Go, handlers should primarily:

- translate HTTP to application input
- call a service/use-case boundary
- map result and errors back to HTTP

They should not also own the use-case implementation.

## 3. Incomplete Layering and Incomplete Extraction

The repository already suggests a layered design, but `cmd/web.go` bypasses it frequently. The result is an architecture that looks layered in package layout but behaves monolithically in practice.

That is worse than either extreme alone:

- a pure monolith would at least have one obvious source of truth
- a real layered design would have clear ownership

Right now the code has both shapes at once.

## 4. Inconsistent Error Semantics

The backend currently mixes several error-response styles.

### Local response helpers and shared response helpers coexist

- local helpers in `cmd/web.go:709`
- shared helpers in `internal/web/api/response.go:24`

### Some endpoints return HTTP errors

Examples:

- `webapi.WriteBadRequest(...)` in many handlers such as `cmd/web.go:2591`

### Some endpoints return `200 OK` with `{status:"error"}`

Examples:

- parse-context errors in `cmd/web.go:2627`
- column-values errors in `cmd/web.go:2672`

Returning transport success for application failure is sometimes acceptable for deliberate UX reasons, but here it is inconsistent rather than principled. The result is an API whose semantics vary by endpoint instead of by policy.

### Errors are often silently dropped

Examples:

- JSON encoding errors ignored in `cmd/web.go:712`
- workspace refresh errors discarded in `cmd/web.go:3955`
- watcher errors mostly ignored in `internal/web/watch/watcher.go:156`
- broadcast marshal failures ignored in `internal/web/events/hub.go:62`

The backend needs a single error model and a consistent rule for when the transport status code reflects failure versus when the endpoint intentionally reports a soft application error inside a success envelope.

## 5. Global Mutable In-Memory Coordination State

The backend owns a lot of mutable state in memory:

- workspace snapshot and revision counters in `cmd/web.go:364`
- patch timers in `cmd/web.go:368`
- DuckDB lock map in `cmd/web.go:375`
- recent write suppression map in `cmd/web.go:381`
- event hub client state in `internal/web/events/hub.go:14`
- freshness timestamps in `internal/web/freshness/tracker.go:24`

None of this is automatically wrong, but several issues follow from the current shape:

- state ownership boundaries are unclear
- related state is grouped by convenience rather than subsystem
- invariants are implicit rather than enforced by smaller types
- concurrency reasoning is spread across the file instead of encapsulated inside dedicated services

The `webServer` struct has effectively become the shared mutable context object for the whole system.

## 6. File Watcher and API Writes Are Manually Reconciled

The watcher model is compensating for insufficiently separated write and event pipelines.

`cmd/web.go:378` describes `recentServerWrites`, which suppresses filesystem watcher events for files recently changed by API handlers so the server does not emit duplicate notifications.

That mechanism is understandable, but it indicates that the system currently has two overlapping sources of truth for change propagation:

- direct handler-driven updates
- filesystem watcher-driven updates

This overlap creates complexity:

- duplicate suppression logic
- patch timer coordination
- debounce + immediate publish rules
- uncertainty around when refreshes should happen and which event should be authoritative

The watcher should ideally be one piece of a broader domain-event model rather than an externally competing event source that must be suppressed after writes.

## 7. Watcher Event Fidelity Is Weak

The watcher abstraction is useful, but its event model is currently too lossy for long-term architectural cleanliness.

Notable issues in `internal/web/watch/watcher.go`:

- fsnotify path changes are collapsed into generic `workspace.updated` events at `internal/web/watch/watcher.go:152`
- polling only emits the first changed path at `internal/web/watch/watcher.go:192`
- watcher callbacks use `context.Background()` instead of the caller context at `internal/web/watch/watcher.go:154` and `internal/web/watch/watcher.go:196`

This makes watcher output hard to treat as a first-class, typed source of domain change events.

## 8. Freshness Tracking Keys Are Too Weak

`internal/web/freshness/tracker.go:24` stores freshness data keyed by asset name.

That is convenient, but it is not a robust identity key. Asset names may be globally unique in practice today, but the architecture should not rely on that forever. Better identity candidates would be:

- encoded asset ID
- workspace-relative asset path
- a dedicated server-side stable asset key

Using names as identity also makes renames more fragile because freshness state semantics become tied to display-facing identifiers.

## 9. CLI Subprocess Coupling Is Too Strong

`internal/web/service/runner.go:15` is a useful abstraction, but it still represents a major architectural coupling: much of the web backend talks to the CLI by spawning the current binary (`os.Args[0]`) and parsing command output.

This has several consequences:

- the web backend is coupled to CLI command-line surface area
- output schemas become hidden contracts between layers of the same application
- testability is reduced because the happy path is external process execution
- latency and resource use are higher than direct library calls
- transport logic sometimes needs to know too much about command retry semantics and output parsing

Some subprocess use may remain necessary, but it should be isolated behind a narrow gateway and not spread through request handlers.

## 10. SQL Intelligence Path Is Expensive and Request-Scoped

`internal/web/sqlintelligence/parser.go:76` starts an embedded Python process on each parse request, extracts embedded files into temp directories, writes a request over stdin, reads a single-line response, and tears the process down.

This design is simple, but it is expensive for a request path that may be called frequently while typing.

Even if the parser remains Python-backed, the architecture should treat this as a subsystem with explicit performance expectations, lifecycle management, and fallback behavior rather than a thin helper embedded into request handling.

## 11. Domain Concepts Are Under-Modeled

The backend currently has many concrete operations but relatively few explicit domain services or use-case types.

Examples of domain concepts that deserve first-class ownership:

- workspace snapshot lifecycle
- asset identity and resolution
- config mutation transactions
- inspect/materialize execution orchestration
- SQL discovery and SQL intelligence
- event publication policy
- freshness and staleness semantics

Without those boundaries, the code models endpoints more strongly than it models the underlying backend capabilities.

## Areas That Are Already Good Foundations

Not everything needs redesign. Several parts are solid and worth preserving.

## 1. Static asset serving is already nicely encapsulated

`internal/web/static/handler.go` is small, cohesive, and easy to reason about. It is a good example of a narrow infrastructure component.

## 2. The SSE hub is a good subsystem boundary

`internal/web/events/hub.go` contains event buffering, subscriptions, and broadcast mechanics in a focused type. It should stay as a dedicated subsystem, though it should eventually participate in a cleaner event model.

## 3. The watcher abstraction is conceptually right

`internal/web/watch/watcher.go` is the correct idea: isolate file watching behind a package and configuration object. The internals need refinement, but the boundary is valuable.

## 4. The runner abstraction is directionally correct

Even though it is still subprocess-centric, `internal/web/service/runner.go` is the right kind of seam. The problem is not that the abstraction exists; the problem is that it is not yet part of a more complete application boundary.

## 5. Internal DTO and API utility packages are useful if made canonical

`internal/web/model/dto.go` and `internal/web/api/response.go` should not be removed; they should become the single source of truth, or they should be superseded by a different single source of truth. The current issue is duplication, not the existence of those packages.

## Recommended Target Architecture

The target should be a standard Go service architecture with explicit boundaries:

- `cmd` contains process startup and composition only
- `internal/web` contains the actual web application
- HTTP handlers are thin adapters
- use-case/application services own business logic
- infrastructure packages own external concerns such as filesystem, subprocesses, SQL parser process lifecycle, watchers, and SSE

## Recommended Layering

### 1. Composition Layer

Suggested package:

- `internal/web/app`

Responsibilities:

- create dependencies
- wire handlers, services, repositories, gateways, and infrastructure adapters
- register routes
- expose an `http.Handler` or server object

`cmd/web.go` should reduce to:

- parse CLI flags
- resolve workspace root and runtime configuration
- construct `app.Server`
- start HTTP server
- manage shutdown lifecycle

### 2. Transport Layer

Suggested package:

- `internal/web/httpapi`

Responsibilities:

- route registration
- request decoding
- validation of transport-level fields
- response writing
- HTTP error mapping
- SSE stream endpoint handling

Suggested internal breakdown:

- `workspace_handler.go`
- `config_handler.go`
- `pipeline_handler.go`
- `asset_handler.go`
- `execution_handler.go`
- `sql_handler.go`
- `events_handler.go`

Handlers should depend on interfaces or concrete application services, not on filesystem details.

### 3. Application / Use-Case Layer

Suggested packages:

- `internal/web/workspace`
- `internal/web/configedit`
- `internal/web/assets`
- `internal/web/execution`
- `internal/web/discovery`
- `internal/web/intelligence`

Responsibilities:

- orchestrate operations
- enforce business rules
- coordinate repositories and gateways
- emit domain events
- remain independent of HTTP specifics

Examples:

- `workspace.Service` computes and refreshes workspace snapshots
- `assets.Service` owns create/update/delete/rename flows
- `configedit.Service` owns environment and connection mutations
- `execution.Service` owns inspect/materialize/run orchestration and lock policy
- `intelligence.Service` owns parse-context requests and SQL metadata resolution

### 4. Infrastructure Layer

Suggested packages:

- `internal/web/fsrepo`
- `internal/web/cligateway`
- `internal/web/watch`
- `internal/web/events`
- `internal/web/sqlintelligence`
- `internal/web/static`

Responsibilities:

- filesystem reads/writes
- subprocess execution
- external parser lifecycle
- file watching
- static asset serving
- low-level transport-agnostic plumbing

## Recommended Package Reorganization

One possible target layout:

```text
internal/web/
  app/
    server.go
    routes.go
    dependencies.go
  httpapi/
    response.go
    errors.go
    workspace_handler.go
    config_handler.go
    pipeline_handler.go
    asset_handler.go
    execution_handler.go
    sql_handler.go
    events_handler.go
  workspace/
    service.go
    snapshot.go
    resolver.go
    events.go
  assets/
    service.go
    rename.go
    columns.go
  pipelines/
    service.go
  configedit/
    service.go
    environments.go
    connections.go
  execution/
    service.go
    inspect.go
    materialize.go
    run.go
    duckdb.go
  discovery/
    service.go
    sql.go
    ingestr.go
  intelligence/
    service.go
  model/
    workspace.go
    config.go
    asset.go
    execution.go
  events/
    hub.go
  watch/
    watcher.go
  freshness/
    tracker.go
  sqlintelligence/
    parser.go
  static/
    handler.go
```

Important note: the exact package split can vary, but the key requirement is that `cmd/web.go` stops being the home of application logic.

## Architectural Principles for the Refactor

## 1. Single Source of Truth Per Concern

For each concern, there should be exactly one canonical implementation:

- one workspace-state builder
- one asset resolver
- one DTO definition package
- one API response/error contract
- one place for config-edit use cases

Duplication should be treated as an architectural bug, not just code smell.

## 2. Thin Handlers, Thick Services

Handlers should be dumb. Services should own behavior.

This improves:

- testability
- readability
- consistency across endpoints
- future portability if another transport is ever added

## 3. Explicit Interfaces at External Boundaries Only

Do not introduce interfaces for every internal type. Introduce them where they buy decoupling and testability.

Good interface candidates:

- workspace repository/parser
- config repository/editor
- CLI gateway / command runner
- SQL parser gateway
- event publisher
- clock/time source for deterministic tests

Less useful:

- interfaces for simple pure services used only in one package

## 4. Domain Events Instead of Ad-Hoc Cross-Subsystem Calls

Instead of handlers directly mutating state, calling refresh, updating freshness, and publishing SSE in sequence, use an internal event model.

Examples of useful internal events:

- `WorkspaceChanged`
- `AssetUpdated`
- `AssetMaterialized`
- `PipelineDeleted`
- `ConfigChanged`

The watcher can publish the same event family as direct write paths. That removes the need for loosely coordinated suppression logic spread across the server.

## 5. Clear Separation Between HTTP Errors and Application Errors

The backend should define a small error taxonomy, for example:

- validation errors
- not found errors
- conflict errors
- unauthorized/forbidden errors if ever needed
- infrastructure errors
- upstream execution errors

Then map them consistently in the transport layer.

If some endpoints intentionally need `200 OK` envelopes with embedded status because the frontend treats them as soft failures while typing, that should be a documented exception rather than a handler-local choice.

## 6. Stable Identifiers Over Display Names

State trackers and cross-request references should prefer stable IDs or canonical workspace-relative paths over display names.

This especially applies to:

- freshness tracking
- change detection
- event payloads
- asset rename behavior

## Specific Refactoring Recommendations

## A. Reduce `cmd/web.go` to a Real Composition Root

Target responsibilities for `cmd/web.go`:

- CLI flag parsing
- path resolution
- server dependency construction
- HTTP server start/shutdown

Everything else should move out.

Expected end state:

- no request DTOs in `cmd/web.go`
- no business logic in `cmd/web.go`
- no file mutation helpers in `cmd/web.go`
- no route-local validation helpers in `cmd/web.go`

## B. Make `internal/web/service/workspace.go` Canonical or Remove It

Right now it is neither fully used nor obsolete. That is the worst state.

Recommended action:

- promote it into the canonical workspace service and move all workspace state computation and asset resolution there
- remove the duplicated implementations from `cmd/web.go`

If additional behavior is needed, expand the service rather than cloning logic back into handlers.

## C. Consolidate DTOs Into One Place

All web API DTOs should live in one canonical package. `internal/web/model` is the obvious candidate if the team wants DTO-style structs. Another acceptable approach is transport-specific DTOs under `internal/web/httpapi`, but not both.

Recommendation:

- use `internal/web/httpapi` for transport DTOs if you want strong separation between API shape and internal model
- use `internal/web/model` for internal resource models only

The main thing to avoid is mirrored structs in both `cmd/web.go` and `internal/web/model`.

## D. Standardize Response Writing and Error Mapping

Choose a single response-writing package and make every handler use it.

Suggested improvements:

- central `WriteJSON`
- central error mapping from domain/infrastructure error types to HTTP status codes
- central request decoding helper for JSON bodies
- central SSE writing helper

Then remove `cmd/web.go` local helpers like `writeAPIError`.

## E. Introduce a Workspace State Service and Snapshot Store

The current `state` plus `stateMu` in `webServer` should become a dedicated service, for example:

- `workspace.Store`
- `workspace.SnapshotService`

Responsibilities:

- compute snapshots
- atomically update current snapshot
- expose current read-only snapshot
- derive changed asset IDs
- coordinate revision increments

This isolates the concurrency and snapshot lifecycle from HTTP logic.

## F. Isolate Config Mutation Into a Dedicated Service

The environment/connection editing endpoints should call a focused config-edit service.

That service should own:

- loading/parsing `.bruin.yml`
- environment CRUD
- connection CRUD
- validation/test connection orchestration
- persistence semantics
- optimistic refresh/event emission hooks

This keeps configuration semantics out of handlers and away from unrelated asset logic.

## G. Create a Dedicated Execution Service

Inspect, materialize, and run-related flows should be gathered into one subsystem.

That subsystem should own:

- command construction policy
- lock/retry policy for DuckDB
- stdout/stderr streaming behavior
- mapping command results into internal execution result structs
- materialization success/failure event publication

The `Runner` stays useful here, but the rest of the code should not build CLI subprocess behavior in many places.

## H. Revisit the SQL Intelligence Runtime Model

The parser subsystem is valuable, but the current implementation is too expensive for high-frequency editor interactions.

Possible options:

1. keep the current subprocess-per-request model temporarily, but isolate it behind a service with strict metrics and timeouts
2. add a warm long-lived worker process with request multiplexing
3. expose a pooled parser worker abstraction behind the same interface

The report does not recommend which one to implement first, but it strongly recommends that parser process lifecycle become an explicit subsystem concern rather than a hidden detail of a request helper.

## I. Improve the Watcher Event Model

Recommended changes:

- preserve caller context instead of using `context.Background()` in watcher callbacks
- emit typed events with richer metadata
- report multiple changed paths when known
- distinguish create/update/delete when feasible
- keep the dedup logic internal to the watcher/event pipeline, not spread across server code

The watcher should publish events that application services can reason about without additional guesswork.

## J. Replace Freshness Name Keys With Stable Asset Identity

Recommended change:

- use asset ID or workspace-relative asset path as the tracker key

Benefits:

- safer renames
- fewer accidental collisions
- better alignment with API payloads and server identity rules

## K. Introduce Focused Tests at Service Boundaries

The refactor should not just move code; it should increase the testable surface.

Recommended test categories:

- workspace snapshot computation tests
- asset resolver tests
- config mutation service tests
- execution service tests with mocked runner
- HTTP handler tests using test doubles for services
- watcher tests for event fidelity and dedup behavior
- SSE tests for publication semantics
- freshness tests keyed by stable asset identity

## Migration Strategy

This should be an incremental refactor, not a rewrite.

## Phase 1: Establish Canonical Utilities and Models

- choose one response utility package
- choose one DTO package strategy
- remove duplicated helper functions from `cmd/web.go`
- add tests around current behavior before moving logic

## Phase 2: Extract Workspace State and Asset Resolution

- make `internal/web/service/workspace.go` or successor package canonical
- route all workspace state refresh and asset resolution through it
- remove duplicate implementations in `cmd/web.go`

## Phase 3: Extract Config Editing Use Cases

- move environment and connection handlers onto a config-edit service
- centralize `.bruin.yml` mutation logic

## Phase 4: Extract Execution Use Cases

- consolidate inspect/materialize/run flows
- centralize DuckDB retry and lock semantics
- standardize result/error mapping

## Phase 5: Introduce a Real HTTP API Layer

- move route handlers into `internal/web/httpapi`
- keep them thin and service-driven
- make `cmd/web.go` only wire dependencies and start the server

## Phase 6: Improve Eventing and Watcher Coordination

- introduce a clearer internal event model
- reduce reliance on suppression maps and ad-hoc refresh/publish sequencing

## Phase 7: Optimize SQL Intelligence Runtime

- evaluate parser process pooling or warm worker lifecycle
- instrument latency and failure behavior

## Prioritized Refactor Backlog

If this work must be prioritized, the recommended order is:

1. finish architectural extraction from `cmd/web.go`
2. remove duplicated workspace/asset-resolution logic
3. standardize responses and error handling
4. extract config editing services
5. extract execution services
6. stabilize event/watcher/freshness semantics
7. optimize SQL intelligence runtime model

## Risks and Things to Preserve

The refactor should preserve these strengths and operational realities:

- Bruin Web is intentionally local-first and filesystem-backed
- the backend should remain close to existing Bruin domain packages rather than reinventing them
- embedded static asset serving already works and should stay simple
- SSE-based live updates are a core product behavior and should not regress
- some CLI subprocess bridging may remain necessary in the short term

Main risks during refactor:

- introducing abstractions without deleting old paths, causing more split-brain code
- over-abstracting simple helpers behind unnecessary interfaces
- changing API semantics unintentionally while normalizing responses
- moving logic without adding service-level tests first

## Recommended End State

After the refactor, the backend should look like this conceptually:

- `cmd/web.go` starts the app and nothing more
- `internal/web/app` wires the system
- `internal/web/httpapi` handles HTTP only
- application services own workspace, config, asset, execution, discovery, and intelligence use cases
- infrastructure packages encapsulate watchers, SSE, static assets, subprocess execution, and parser runtime
- each concern has one canonical home
- error handling is consistent
- mutable state is encapsulated by subsystem, not concentrated in a god object
- the server is easy to test without spawning the full binary for most backend unit tests

## Final Recommendation

Do not treat the current backend problems as isolated code-style issues. The central issue is architectural ownership.

The best path forward is to finish the move from a monolithic `cmd/web.go` implementation to a layered `internal/web` application, with thin handlers, canonical services, explicit infrastructure boundaries, and a single source of truth for DTOs, error handling, workspace state, and asset resolution.

That approach will make the codebase more idiomatic Go, easier to test, easier to evolve, and much less likely to accumulate new backend-only UI behavior directly inside the CLI command file.
