//package main
//
//import (
//	"fmt"
//	"log"
//	"log/slog"
//	"os"
//	"os/exec"
//	"strings"
//)
//
//func main() {
//	programLevel := new(slog.LevelVar)
//	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: programLevel}))
//	slog.SetDefault(logger)
//
//	log.Println("Welcome to the WorkflowManager")
//	log.Println("Starting pipeline")
//	integrationID := os.Getenv("INTEGRATION_ID")
//	apiKey := os.Getenv("PENNSIEVE_API_KEY")
//	apiSecret := os.Getenv("PENNSIEVE_API_SECRET")
//
//	// run pipeline
//	workspaceDir := "/service/workflows"
//	cmd := exec.Command("nextflow",
//		"-log", "/service/workflows/nextflow.log",
//		"run", "./workflows/pennsieve.aws.nf",
//		"-w", workspaceDir,
//		"-ansi-log", "false",
//		"--integrationID", integrationID,
//		"--apiKey", apiKey,
//		"--apiSecret", apiSecret,
//		"--workspaceDir", workspaceDir)
//	cmd.Dir = "/service"
//	var stdout strings.Builder
//	var stderr strings.Builder
//	cmd.Stdout = &stdout
//	cmd.Stderr = &stderr
//	if err := cmd.Run(); err != nil {
//		logger.Error(err.Error(),
//			slog.String("error", stderr.String()))
//	}
//	fmt.Println(stdout.String())
//}

package main

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	"log"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"github.com/aws/aws-sdk-go-v2/config"
)

const (
	visibilityTimeout = 4320 * 10
	waitingTimeout    = 20
)

type CommandStatusInfo struct {
	IntegrationID string
	PID           int
	InputDir      string
	OutputDir     string
}

func main() {
	programLevel := new(slog.LevelVar)
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: programLevel}))
	slog.SetDefault(logger)

	logger.Info("Welcome to the WorkflowManager")

	baseDir := os.Getenv("BASE_DIR")
	if baseDir == "" {
		baseDir = "/service"
	}

	// create output directory and set permissions
	err := os.Chdir(baseDir)
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}

	err = os.MkdirAll("output", 0777)
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}
	err = os.Chown("output", 1000, 1000)
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}

	err = os.MkdirAll("workspace", 0777)
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}
	err = os.Chown("workspace", 1000, 1000)
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}

	_, err = config.LoadDefaultConfig(context.Background())
	if err != nil {
		log.Fatalf("LoadDefaultConfig: %v\n", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)

	queueUrl := os.Getenv("SQS_URL")
	log.Printf("QUEUE_URL: %s", queueUrl)

	// Tacking file for PID, IntegrationID
	csvFile, err := os.Create(filepath.Join(baseDir, "pid_tracking.csv"))
	if err != nil {
		logger.Info("Error creating CSV file:", err)
	}

	defer csvFile.Close()

	csvWriter := csv.NewWriter(csvFile)
	defer csvWriter.Flush()

	csvWriter.Write([]string{"IntegrationID", "PID"})

	testMessage := []int{1}

	var wg sync.WaitGroup

loop:
	for {
		select {
		case <-signalChan: // if get SIGTERM
			log.Println("got SIGTERM signal, cancelling the context")
			cancel() // cancel context

		default:
			_, err := processSQS(ctx, testMessage, logger, &wg)

			if err != nil {
				if errors.Is(err, context.Canceled) {
					log.Printf("stop processing, context is cancelled %v", err)
					break loop
				}

				log.Fatalf("error processing SQS %v", err)
			}
		}
	}
	log.Println("service is safely stopped")

}

type MsgType struct {
	IntegrationID string `json:"integrationId"`
	ApiKey        string `json:"api_key"`
	ApiSecret     string `json:"api_secret"`
	Cancel        bool   `json:"cancel"`
}

func processSQS(ctx context.Context, messages []int, logger *slog.Logger, wg *sync.WaitGroup) (bool, error) {

	// Track completed commands
	doneChannel := make(chan *CommandStatusInfo)

	for _, _ = range messages {
		wg.Add(1)

		var newMsg MsgType

		if newMsg.Cancel == true {
			var fileMutex sync.Mutex
			killProcess(ctx, newMsg.IntegrationID, &fileMutex, logger)
		}

		go func() {
			defer wg.Done()
			logger.Info("Initializing workspace ...")

			integrationID := os.Getenv("INTEGRATION_ID")
			apiKey := os.Getenv("PENNSIEVE_API_KEY")
			apiSecret := os.Getenv("PENNSIEVE_API_SECRET")

			baseDir := os.Getenv("BASE_DIR")
			if baseDir == "" {
				baseDir = "/service"
			}

			// create workspace subdirectories
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
			workspaceDir := "/service/workflows/"
			err = os.MkdirAll(outputDir, 0777)
			if err != nil {
				logger.Error(err.Error())
				os.Exit(1)
			}

			// run analysis pipeline
			logger.Info("Starting analysis pipeline")
			logger.Info(workspaceDir)
			logger.Info(integrationID)
			logger.Info(apiKey)
			logger.Info(workspaceDir)
			cmd := exec.Command("nextflow",
				"-log", fmt.Sprintf("%s/nextflow.log", workspaceDir),
				"run", "./workflows/pennsieve.aws.nf", "-ansi-log", "false",
				"-w", workspaceDir,
				"--integrationID", integrationID,
				"--apiKey", apiKey,
				"--apiSecret", apiSecret,
				"--workspaceDir", workspaceDir)

			//cmd = exec.Command("nextflow",
			//	"-log", fmt.Sprintf("%s/nextflow.log", workspaceDir),
			//	"run", "./workflows/pennsieve.aws.nf", "-ansi-log", "false",
			//)
			cmd.Dir = "/service"
			var stdout, stderr strings.Builder
			// cmd.Start() to stop blocking and not wait compared to cmd.Run()
			if err := cmd.Start(); err != nil {
				logger.Error(err.Error(),
					slog.String("error", stderr.String()))
			}

			pid := cmd.Process.Pid
			csvFile, err := os.OpenFile(filepath.Join(baseDir, "pid_tracking.csv"), os.O_APPEND|os.O_WRONLY, os.ModeAppend)
			csvWriter := csv.NewWriter(csvFile)

			err = csvWriter.Write([]string{integrationID, fmt.Sprintf("%d", pid)})
			if err != nil {
				logger.Info("Could not write to PID file")
			}
			csvWriter.Flush()

			if err := cmd.Wait(); err != nil {
				logger.Error("Failed to wait for command", slog.String("error", err.Error()))
			}

			cmd.Stdout = &stdout
			cmd.Stderr = &stderr
			logger.Info("Command output", slog.String("stdout", stdout.String()), slog.String("stderr", stderr.String()))

			logger.Info(fmt.Sprintf("Command finished waiting with status %v", err))
			if err != nil {
				logger.Info(fmt.Sprintf("Error waiting for nextflow command: %v. PID: %d", err, pid))
			}

			logger.Info(fmt.Sprintf("Command finished waiting with status %v", err))
			doneChannel <- &CommandStatusInfo{
				IntegrationID: integrationID,
				PID:           pid,
				InputDir:      inputDir,
				OutputDir:     outputDir,
			}
		}()

		// Clean up go func, waits for command to complete then cleans up
		go func() {
			for cmdInfo := range doneChannel {
				// Delete input and output directories after the command completes
				fmt.Printf("Cleaning up for PID %d (IntegrationID: %s)\n", cmdInfo.PID, cmdInfo.IntegrationID)
				os.RemoveAll(cmdInfo.InputDir)
				os.RemoveAll(cmdInfo.OutputDir)

				// WAIT
				// Split out into go routine
				// cleanup files
				err := os.RemoveAll(cmdInfo.InputDir)
				if err != nil {
					logger.Error("error deleting files",
						slog.String("error", err.Error()))
				}
				log.Printf("Input Dir %s deleted", cmdInfo.InputDir)

				err = os.RemoveAll(cmdInfo.OutputDir)
				if err != nil {
					logger.Error("error deleting files",
						slog.String("error", err.Error()))
				}
				log.Printf("Output Dir %s deleted", cmdInfo.OutputDir)

				if err != nil {
					logger.Error("error deleting message from SQS",
						slog.String("error", err.Error()))
				}
			}
		}()
	}
	wg.Wait()
	close(doneChannel)
	return true, nil
}

func killProcess(ctx context.Context, integrationID string, lock *sync.Mutex, logger *slog.Logger) {
	lock.Lock()
	defer lock.Unlock()
	baseDir := getBaseDir()

	file, err := os.Open(filepath.Join(baseDir, "pid_tracking.csv"))
	if err != nil {
		fmt.Printf("Error opening file: %v\n", err)
		return
	}
	defer file.Close()

	reader := csv.NewReader(file)
	rows, err := reader.ReadAll()
	if err != nil {
		fmt.Printf("Error reading CSV: %v\n", err)
		return
	}

	dataMap := make(map[string]string)
	for i, row := range rows {
		if i == 0 {
			// skip header row
			continue
		}
		if len(row) >= 2 {
			dataMap[row[0]] = row[1]
		}

		if pidString, exists := dataMap[integrationID]; exists {
			fmt.Printf("Killing integration %s with PID %s\n", integrationID, pidString)
			pid, err := strconv.Atoi(pidString)
			if err != nil {
				fmt.Printf("Error converting string to int: %v\n", err)
				return
			}
			// kill -9 [PID]
			err = syscall.Kill(pid, syscall.SIGKILL)
			if err != nil {
				fmt.Printf("Failed to kill process %d: %v\n", pid, err)
				return
			}
			// Kill running ECS task
			killECS(ctx, logger)
			updateIntegration()

		} else {
			fmt.Printf("IntegrationID %s not found\n", integrationID)
		}
	}
}

func killECS(ctx context.Context, logger *slog.Logger) []string {
	var cancelledTasks []string
	var integrationId = 0
	var TaskArn = 7
	var ClusterName = 8

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		logger.Error("LoadDefaultConfig:" + err.Error())
		return []string{}
	}

	client := ecs.NewFromConfig(cfg)

	// Get taskArn and ClusterName
	baseDir := getBaseDir()
	file, err := os.Open(filepath.Join(baseDir, "pid_tracking.csv"))
	if err != nil {
		logger.Info("Error opening file: %v\n", err)
		return cancelledTasks
	}
	defer file.Close()

	reader := csv.NewReader(file)
	rows, err := reader.ReadAll()

	// Loop through and stop each task
	for _, row := range rows {
		// Call StopTask API
		input := &ecs.StopTaskInput{
			Cluster: aws.String(row[ClusterName]),
			Task:    aws.String(row[TaskArn]),
		}

		_, err = client.StopTask(context.TODO(), input)
		if err != nil {
			logger.Error("failed to stop task, %v", err)
			return cancelledTasks
		}
		cancelledTasks = append(cancelledTasks, row[integrationId])
	}
	return cancelledTasks

}

func updateIntegration() {

}
func getBaseDir() string {
	baseDir := os.Getenv("BASE_DIR")
	if baseDir == "" {
		baseDir = "/mnt/efs"
	}
	return baseDir
}
