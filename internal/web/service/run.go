package service

import "context"

type RunRequest struct {
	Command    string   `json:"command"`
	PipelineID string   `json:"pipeline_id"`
	AssetPath  string   `json:"asset_path"`
	Args       []string `json:"args"`
}

type RunResult struct {
	Status   string
	Command  []string
	Output   string
	Error    string
	ExitCode int
	HTTPCode int
}

type RunDependencies struct {
	Runner Runner
}

type RunService struct {
	deps RunDependencies
}

func NewRunService(deps RunDependencies) *RunService {
	return &RunService{deps: deps}
}

func (s *RunService) Execute(ctx context.Context, req RunRequest) RunResult {
	command := req.Command
	if command == "" {
		command = "run"
	}

	target := "."
	if req.PipelineID != "" {
		relPath, err := ResolvePipelineRunTarget(req.PipelineID)
		if err != nil {
			return RunResult{
				Status:   "error",
				Error:    "invalid pipeline id",
				ExitCode: 1,
				HTTPCode: 400,
			}
		}
		target = relPath
	}

	if req.AssetPath != "" {
		target = req.AssetPath
	}

	cmdArgs := append([]string{command, target}, req.Args...)
	output, err := s.deps.Runner.Run(ctx, cmdArgs)
	if err != nil {
		return RunResult{
			Status:   "error",
			Command:  cmdArgs,
			Output:   string(output),
			Error:    err.Error(),
			ExitCode: 1,
			HTTPCode: 400,
		}
	}

	return RunResult{
		Status:   "ok",
		Command:  cmdArgs,
		Output:   string(output),
		ExitCode: 0,
		HTTPCode: 200,
	}
}
