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

	logger.Info("Initializing workspace ...")
	baseDir := os.Getenv("BASE_DIR")
	if baseDir == "" {
		baseDir = "/mnt/efs"
	}

	// create workspace sub-directories
	err := os.Chdir(baseDir)
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}

	// inputDir
	inputDir := fmt.Sprintf("%s/input/%s", baseDir, integrationID)
	err = os.MkdirAll(inputDir, 0755)
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}

	// outputDir
	outputDir := fmt.Sprintf("%s/output/%s", baseDir, integrationID)
	err = os.MkdirAll(outputDir, 0777)
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}

	// workspaceDir
	workspaceDir := fmt.Sprintf("%s/workspace/%s", baseDir, integrationID)
	err = os.MkdirAll(outputDir, 0777)
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}

	// resourcesDir
	resourcesDir := fmt.Sprintf("%s/resources", baseDir)
	err = os.MkdirAll(resourcesDir, 0777)
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}

	// run analysis pipeline
	nextflowLogPath := fmt.Sprintf("%s/nextflow.log", workspaceDir)
	logger.Info("Starting analysis pipeline")
	logger.Info("Starting debugging")
	cmd := exec.Command("nextflow",
		"-log", nextflowLogPath,
		"run", "./workflows/pennsieve.aws.nf", "-ansi-log", "false",
		"-w", workspaceDir,
		"--integrationID", integrationID,
		"--apiKey", apiKey,
		"--apiSecret", apiSecret,
		"--workspaceDir", workspaceDir,
		"--resourcesDir", resourcesDir)
	cmd.Dir = "/service"
	var stdout strings.Builder
	var stderr strings.Builder
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	logger.Info("running actual command")
	if err := cmd.Run(); err != nil {
		var nextflowLog string
		if nextflowLogBytes, logErr := os.ReadFile(nextflowLogPath); logErr != nil {
			nextflowLog = fmt.Sprintf("unable to read nextflow log: %s", logErr)
		} else {
			nextflowLog = string(nextflowLogBytes)
		}
		logger.Error(err.Error(),
			slog.String("stderr", stderr.String()),
			slog.String("stdout", stdout.String()),
			slog.String("nextflowLog", nextflowLog))
	}

	logger.Info("stdout", slog.String("message", stdout.String()))
	logger.Info("after nextflow command run")

	logger.Info("starting cleanup")
	// cleanup files
	err = os.RemoveAll(inputDir)
	if err != nil {
		logger.Error("error deleting files",
			slog.String("error", err.Error()))
	}
	log.Printf("dir %s deleted", inputDir)
	err = os.RemoveAll(outputDir)
	if err != nil {
		logger.Error("error deleting files",
			slog.String("error", err.Error()))
	}
	log.Printf("Dir %s deleted", outputDir)
}
