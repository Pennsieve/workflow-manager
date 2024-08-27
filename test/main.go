package main

import (
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/exec"
	"strings"
)

func main() {
	programLevel := new(slog.LevelVar)
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: programLevel}))
	slog.SetDefault(logger)

	log.Println("Welcome to the WorkflowManager")
	log.Println("Starting pipeline")
	integrationID := os.Getenv("INTEGRATION_ID")
	apiKey := os.Getenv("PENNSIEVE_API_KEY")
	apiSecret := os.Getenv("PENNSIEVE_API_SECRET")

	// run pipeline
	workspaceDir := "/service/workflows"
	cmd := exec.Command("nextflow",
		"-log", "/service/workflows/nextflow.log",
		"run", "./workflows/pennsieve.aws.nf",
		"-w", workspaceDir,
		"-ansi-log", "false",
		"--integrationID", integrationID,
		"--apiKey", apiKey,
		"--apiSecret", apiSecret,
		"--workspaceDir", workspaceDir)
	cmd.Dir = "/service"
	var stdout strings.Builder
	var stderr strings.Builder
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		logger.Error(err.Error(),
			slog.String("error", stderr.String()))
	}
	fmt.Println(stdout.String())
}
