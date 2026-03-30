package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	webapi "github.com/bruin-data/bruin/internal/web/api"
	"github.com/bruin-data/bruin/internal/web/service"
	"github.com/go-chi/chi/v5"
)

type RunHandlers interface {
	Run(ctx context.Context, req service.RunRequest) service.RunResult
}

type RunAPI struct {
	Service RunHandlers
}

func RegisterRunRoutes(router chi.Router, handlers *RunAPI) {
	router.Post("/api/run", handlers.HandleRun)
}

func (h *RunAPI) HandleRun(w http.ResponseWriter, r *http.Request) {
	var req service.RunRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		webapi.WriteBadRequest(w, "invalid_request_body", err.Error())
		return
	}

	command := req.Command
	if command == "" {
		command = "run"
	}
	if !service.IsCommandAllowed(command) {
		webapi.WriteBadRequest(w, "command_not_allowed", fmt.Sprintf("command %q is not allowed; permitted commands: run, query, patch, lint", command))
		return
	}

	result := h.Service.Run(r.Context(), req)
	webapi.WriteJSON(w, result.HTTPCode, map[string]any{
		"status":    result.Status,
		"command":   result.Command,
		"output":    result.Output,
		"error":     result.Error,
		"exit_code": result.ExitCode,
	})
}
