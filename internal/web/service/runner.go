// Package service provides the business logic layer for the Bruin web server.
package service

import (
	"bytes"
	"context"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"
)

// Runner provides an interface for executing Bruin CLI commands.
type Runner interface {
	// Run executes a command and returns its output.
	Run(ctx context.Context, args []string) ([]byte, error)

	// Stream executes a command and streams incremental output chunks.
	Stream(ctx context.Context, args []string, onChunk func([]byte)) ([]byte, error)

	// RunWithRetry executes a command with DuckDB-aware retry logic.
	RunWithRetry(ctx context.Context, args []string, retries int, initialDelay time.Duration) ([]byte, error, int)
}

// DefaultRunner implements Runner using subprocess execution.
type DefaultRunner struct {
	WorkspaceRoot string
	BinaryPath    string
}

// NewRunner creates a new DefaultRunner.
// If binaryPath is empty, it uses the current executable.
func NewRunner(workspaceRoot string) *DefaultRunner {
	return &DefaultRunner{
		WorkspaceRoot: workspaceRoot,
		BinaryPath:    os.Args[0],
	}
}

// Run executes a command and returns its output.
func (r *DefaultRunner) Run(ctx context.Context, args []string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, r.BinaryPath, args...)
	cmd.Dir = r.WorkspaceRoot
	return cmd.CombinedOutput()
}

// Stream executes a command and streams stdout/stderr chunks as they are produced.
func (r *DefaultRunner) Stream(ctx context.Context, args []string, onChunk func([]byte)) ([]byte, error) {
	cmd := exec.CommandContext(ctx, r.BinaryPath, args...)
	cmd.Dir = r.WorkspaceRoot

	buffer := bytes.NewBuffer(nil)
	writer := &streamCaptureWriter{
		onChunk: onChunk,
		buffer:  buffer,
	}
	cmd.Stdout = writer
	cmd.Stderr = writer

	err := cmd.Run()
	return buffer.Bytes(), err
}

// RunWithRetry executes a command with DuckDB-aware retry logic.
// Returns the output, error (if any), and the number of attempts made.
func (r *DefaultRunner) RunWithRetry(
	ctx context.Context,
	args []string,
	retries int,
	initialDelay time.Duration,
) ([]byte, error, int) {
	attempt := 0
	delay := initialDelay
	for {
		attempt++
		output, err := r.Run(ctx, args)
		if err == nil {
			return output, nil, attempt
		}

		if !IsDuckDBLockError(err, output) || attempt > retries {
			return output, err, attempt
		}

		select {
		case <-ctx.Done():
			return output, ctx.Err(), attempt
		case <-time.After(delay):
		}

		delay *= 2
	}
}

// IsDuckDBLockError checks if an error is a DuckDB file lock conflict.
func IsDuckDBLockError(err error, output []byte) bool {
	if err == nil {
		return false
	}

	message := strings.ToLower(err.Error() + "\n" + string(output))
	return strings.Contains(message, "could not set lock on file") ||
		strings.Contains(message, "conflicting lock is held")
}

// AppendDuckDBReadOnlyMode adds read-only mode to a DuckDB path if not already present.
func AppendDuckDBReadOnlyMode(path string) string {
	if path == "" {
		return path
	}

	lower := strings.ToLower(path)
	if strings.Contains(lower, "access_mode=read_only") || strings.HasPrefix(lower, "md:") {
		return path
	}

	separator := "?"
	if strings.Contains(path, "?") {
		separator = "&"
	}

	return path + separator + "access_mode=read_only"
}

// AllowedCommands is the list of commands that can be executed via the API.
var AllowedCommands = map[string]bool{
	"run":   true,
	"query": true,
	"patch": true,
	"lint":  true,
}

// IsCommandAllowed checks if a command is in the allowlist.
func IsCommandAllowed(command string) bool {
	return AllowedCommands[strings.ToLower(command)]
}

type streamCaptureWriter struct {
	mu      sync.Mutex
	buffer  *bytes.Buffer
	onChunk func([]byte)
}

func (w *streamCaptureWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.buffer.Write(p); err != nil {
		return 0, err
	}

	if w.onChunk != nil {
		chunk := append([]byte(nil), p...)
		w.onChunk(chunk)
	}

	return len(p), nil
}

var _ io.Writer = (*streamCaptureWriter)(nil)
