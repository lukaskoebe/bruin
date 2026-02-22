### Project Context: Bruin Web

**Project Overview**
We are building "Bruin Web," a local-first web interface for the Bruin CLI data pipeline tool. Bruin is a CLI tool written in Go that allows users to ingest data, transform it using SQL/Python, run quality checks, and manage glossaries. Bruin Web aims to take the developer experience of Bruin (and its VS Code extension) and translate it into a highly interactive, browser-based visual workspace.

The application will be compiled into a single executable. The frontend is a statically exported React app embedded directly into a Go-based HTTP server using `go:embed`.

**The Tech Stack**

- **Backend:** Go (Native Bruin core) wrapped in a custom HTTP server.
- **Frontend Framework:** React (Tanstack Router, exported as static HTML/JS/CSS).
- **UI Component Library:** shadcn/ui (Tailwind CSS, Radix UI primitives).
- **Visual Canvas:** React Flow (for interactive DAG/lineage visualization).
- **Code Editor:** Monaco Editor (`@monaco-editor/react`) for SQL, Python, and YAML.
- **Client State Management:** Jotai (for complex canvas and editor state).
- **Real-time Communication:** Server-Sent Events (SSE).

**Core Architecture & Data Flow**

1. **The Go Reactive Bridge:** The backend is not a traditional CRUD server; it is a file-system bridge. The Go server uses `fsnotify` to recursively watch the user's Bruin workspace (e.g., `pipeline.yml`, `glossary.yml`, and the `assets/` directory).
2. **Server-Sent Events (SSE):** When a file is created, modified, or deleted (whether by the web UI or an external CLI command), the Go server immediately parses the changes and pushes the updated pipeline state to the React frontend via an SSE endpoint (`/api/events`).
3. **Automated UI Synchronization:** The frontend listens to the SSE stream. The UI must update automatically and instantly without requiring the user to refresh the page or click a "sync" button.
4. **Debounced Writes:** When a user types in the Monaco editor or tweaks settings in the UI, Jotai updates the local state immediately for a snappy UX. However, file system writes back to the Go server (`PUT /api/assets`) must be debounced (e.g., 400-500ms) to prevent locking the Go server or triggering infinite SSE loops.

**User Experience (UX) Requirements**

- **Visual-First Canvas:** The primary view is a React Flow drawing board. Users can right-click or drag-and-drop to create new data assets (nodes). The DAG connects nodes based on their Bruin `depends_on` relationships.
- **Split-Pane Editing:** Clicking a node on the canvas opens a sliding side-panel or resizable pane. This pane contains the Monaco Editor for the raw code (SQL/Python) alongside shadcn/ui forms to configure asset metadata (policies, quality checks, variables).
- **AI Integration:** The UI should include a chat panel that connects to the Bruin MCP (Model Context Protocol) server. Users can ask the AI to query data, compare tables, or build pipelines, and the resulting code should integrate directly into the visual workspace.

**Strict Constraints & Rules for the Agent**

- **No Node.js Backend:** Do not write any Node.js API routes (`app/api/...`) that rely on server-side execution for the final build. The React app will be exported using `output: 'export'`. All `/api/...` calls made by the frontend must assume they are hitting the Go HTTP server running on the same host.
- **Single Source of Truth:** The file system is the ultimate source of truth. Jotai is only used for immediate UI feedback and managing the React Flow canvas state.
- **Do Not Over-fetch:** Rely on the SSE connection for state updates. Avoid writing polling mechanisms (`setInterval`) to check for pipeline changes.
