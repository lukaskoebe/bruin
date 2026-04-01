package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type stubRunRunner struct {
	args   []string
	output []byte
	err    error
}

func (s *stubRunRunner) Run(_ context.Context, args []string) ([]byte, error) {
	s.args = append([]string(nil), args...)
	return s.output, s.err
}

func (s *stubRunRunner) Stream(_ context.Context, args []string, _ func([]byte)) ([]byte, error) {
	s.args = append([]string(nil), args...)
	return s.output, s.err
}

func (s *stubRunRunner) RunWithRetry(_ context.Context, args []string, _ int, _ time.Duration) ([]byte, error, int) {
	s.args = append([]string(nil), args...)
	return s.output, s.err, 1
}

func TestRunServiceExecute_DefaultsToRunCommand(t *testing.T) {
	t.Parallel()

	runner := &stubRunRunner{output: []byte("ok")}
	svc := NewRunService(RunDependencies{Runner: runner})

	result := svc.Execute(context.Background(), RunRequest{})

	require.Equal(t, []string{"run", "."}, runner.args)
	assert.Equal(t, "ok", result.Status)
	assert.Equal(t, 200, result.HTTPCode)
	assert.Equal(t, 0, result.ExitCode)
	assert.Equal(t, []string{"run", "."}, result.Command)
	assert.Equal(t, "ok", result.Output)
}

func TestRunServiceExecute_UsesPipelineTarget(t *testing.T) {
	t.Parallel()

	runner := &stubRunRunner{output: []byte("done")}
	svc := NewRunService(RunDependencies{Runner: runner})

	result := svc.Execute(context.Background(), RunRequest{
		Command:    "lint",
		PipelineID: EncodeID("pipelines/orders/pipeline.yml"),
		Args:       []string{"--debug"},
	})

	require.Equal(t, []string{"lint", "pipelines/orders", "--debug"}, runner.args)
	assert.Equal(t, "ok", result.Status)
	assert.Equal(t, []string{"lint", "pipelines/orders", "--debug"}, result.Command)
	assert.Equal(t, "done", result.Output)
}

func TestRunServiceExecute_AssetPathOverridesPipelineID(t *testing.T) {
	t.Parallel()

	runner := &stubRunRunner{output: []byte("asset")}
	svc := NewRunService(RunDependencies{Runner: runner})

	result := svc.Execute(context.Background(), RunRequest{
		Command:    "query",
		PipelineID: EncodeID("pipelines/orders/pipeline.yml"),
		AssetPath:  "pipelines/orders/assets/order_items.sql",
	})

	require.Equal(t, []string{"query", "pipelines/orders/assets/order_items.sql"}, runner.args)
	assert.Equal(t, "ok", result.Status)
	assert.Equal(t, []string{"query", "pipelines/orders/assets/order_items.sql"}, result.Command)
}

func TestRunServiceExecute_InvalidPipelineID(t *testing.T) {
	t.Parallel()

	runner := &stubRunRunner{}
	svc := NewRunService(RunDependencies{Runner: runner})

	result := svc.Execute(context.Background(), RunRequest{PipelineID: "%%%"})

	assert.Equal(t, "error", result.Status)
	assert.Equal(t, 400, result.HTTPCode)
	assert.Equal(t, 1, result.ExitCode)
	assert.Equal(t, "invalid pipeline id", result.Error)
	assert.Nil(t, runner.args)
}

func TestRunServiceExecute_PropagatesRunnerFailure(t *testing.T) {
	t.Parallel()

	runner := &stubRunRunner{output: []byte("bad output"), err: errors.New("boom")}
	svc := NewRunService(RunDependencies{Runner: runner})

	result := svc.Execute(context.Background(), RunRequest{Command: "patch", AssetPath: "assets/foo.sql"})

	require.Equal(t, []string{"patch", "assets/foo.sql"}, runner.args)
	assert.Equal(t, "error", result.Status)
	assert.Equal(t, 400, result.HTTPCode)
	assert.Equal(t, 1, result.ExitCode)
	assert.Equal(t, []string{"patch", "assets/foo.sql"}, result.Command)
	assert.Equal(t, "bad output", result.Output)
	assert.Equal(t, "boom", result.Error)
}
