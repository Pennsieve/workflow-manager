package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/aws/aws-sdk-go-v2/service/ecs"
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
		baseDir = "/mnt/efs"
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

	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		log.Fatalf("LoadDefaultConfig: %v\n", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)

	queueUrl := os.Getenv("SQS_URL")
	log.Printf("QUEUE_URL: %s", queueUrl)

	sqsSvc := sqs.NewFromConfig(cfg)

	// Tacking file for PID, IntegrationID
	csvFile, err := os.Create(filepath.Join(baseDir, "pid_tracking.csv"))
	if err != nil {
		logger.Info("Error creating CSV file:", err)
	}

	defer csvFile.Close()

	csvWriter := csv.NewWriter(csvFile)
	defer csvWriter.Flush()

	csvWriter.Write([]string{"IntegrationID", "PID"})

	var wg sync.WaitGroup

loop:
	for {
		select {
		case <-signalChan: // if get SIGTERM
			log.Println("got SIGTERM signal, cancelling the context")
			cancel() // cancel context

		default:
			_, err := processSQS(ctx, sqsSvc, queueUrl, logger, &wg)

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

func processSQS(ctx context.Context, sqsSvc *sqs.Client, queueUrl string, logger *slog.Logger, wg *sync.WaitGroup) (bool, error) {
	input := &sqs.ReceiveMessageInput{
		QueueUrl:            &queueUrl,
		MaxNumberOfMessages: 1,
		VisibilityTimeout:   visibilityTimeout,
		WaitTimeSeconds:     waitingTimeout, // use long polling
	}

	resp, err := sqsSvc.ReceiveMessage(ctx, input)

	if err != nil {
		return false, fmt.Errorf("error receiving message %w", err)
	}

	log.Printf("received messages: %v", len(resp.Messages))
	if len(resp.Messages) == 0 {
		return false, nil
	}

	// Track completed nextflow tasks
	doneChannel := make(chan *CommandStatusInfo)

	for _, msg := range resp.Messages {
		wg.Add(1)

		var newMsg MsgType
		id := *msg.MessageId

		err := json.Unmarshal([]byte(*msg.Body), &newMsg)
		if err != nil {
			return false, fmt.Errorf("error unmarshalling %w", err)
		}

		log.Printf("message id %s is received from SQS: %#v", id, newMsg.IntegrationID)

		// If a message comes in with new key Cancel, find the process and kill it
		if newMsg.Cancel == true {
			// TODO: Set cancelling status immediately first
			var fileMutex sync.Mutex
			go killProcess(ctx, newMsg.IntegrationID, &fileMutex, logger)
			wg.Done()
			continue
		}

		go func(msg types.Message) {
			defer wg.Done()
			logger.Info("Initializing workspace ...")

			integrationID := newMsg.IntegrationID
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

			// run analysis pipeline
			logger.Info("Starting analysis pipeline")
			cmd := exec.Command("nextflow",
				"-log", fmt.Sprintf("%s/nextflow.log", workspaceDir),
				"run", "./workflows/pennsieve.aws.nf", "-ansi-log", "false",
				"-w", workspaceDir,
				"--integrationID", integrationID,
				"--apiKey", newMsg.ApiKey,
				"--apiSecret", newMsg.ApiSecret,
				"--workspaceDir", workspaceDir)
			cmd.Dir = "/service"
			var stdout strings.Builder
			var stderr strings.Builder
			cmd.Stdout = &stdout
			cmd.Stderr = &stderr

			// cmd.Start() to stop blocking and not wait compared to cmd.Run()
			if err := cmd.Start(); err != nil {
				logger.Error(err.Error(),
					slog.String("error", stderr.String()))
			}

			// Save PID to file
			pid := cmd.Process.Pid
			csvFile, err := os.OpenFile(filepath.Join(baseDir, "pid_tracking.csv"), os.O_APPEND|os.O_WRONLY, os.ModeAppend)
			csvWriter := csv.NewWriter(csvFile)

			err = csvWriter.Write([]string{integrationID, fmt.Sprintf("%d", pid)})
			if err != nil {
				logger.Info("Could not write to PID file")
			}
			csvWriter.Flush()

			err = cmd.Wait()
			if err != nil {
				logger.Info(fmt.Sprintf("Error waiting for nextflow command %v. PID: %d", err, pid))
			}

			// Report work done
			doneChannel <- &CommandStatusInfo{
				IntegrationID: integrationID,
				PID:           pid,
				InputDir:      inputDir,
				OutputDir:     outputDir,
			}
		}(msg)

		// Receive complete messages when nextflow task is done
		// Then clean up input / output files
		go func(msg types.Message) {
			for cmdInfo := range doneChannel {
				// Delete input and output directories after the command completes
				logger.Info("Clean up files for (IntegrationID: %s)\n", cmdInfo.IntegrationID)

				err = os.RemoveAll(cmdInfo.InputDir)
				if err != nil {
					logger.Error("error deleting files",
						slog.String("error", err.Error()))
				}
				logger.Info("dir %s deleted", cmdInfo.InputDir)

				err = os.RemoveAll(cmdInfo.OutputDir)
				if err != nil {
					logger.Error("error deleting files",
						slog.String("error", err.Error()))
				}
				logger.Info("Dir %s deleted", cmdInfo.OutputDir)

				// delete message
				_, err = sqsSvc.DeleteMessage(ctx, &sqs.DeleteMessageInput{
					QueueUrl:      &queueUrl,
					ReceiptHandle: msg.ReceiptHandle,
				})

				if err != nil {
					logger.Error("error deleting message from SQS",
						slog.String("error", err.Error()))
				}
				logger.Info("message id %s is deleted from queue", id)
			}
		}(msg)

		close(doneChannel)

	}
	wg.Wait()
	return true, nil
}

func killProcess(ctx context.Context, integrationID string, lock *sync.Mutex, logger *slog.Logger) {
	lock.Lock()
	defer lock.Unlock()
	baseDir := getBaseDir()

	file, err := os.Open(filepath.Join(baseDir, "pid_tracking.csv"))
	if err != nil {
		logger.Info("Error opening file: %v\n", err)
		return
	}
	defer file.Close()

	reader := csv.NewReader(file)
	rows, err := reader.ReadAll()
	if err != nil {
		logger.Info("Error reading CSV: %v\n", err)
		return
	}

	// Creat dict to have e asy access to PIDs
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
			logger.Info("Killing integration %s with PID %s\n", integrationID, pidString)
			pid, err := strconv.Atoi(pidString)
			if err != nil {
				logger.Error("Error converting string to int: %v\n", err)
				return
			}
			// kill -9 [PID]
			err = syscall.Kill(pid, syscall.SIGKILL)
			if err != nil {
				logger.Error("Failed to kill process %d: %v\n", pid, err)
				return
			}
			// Kill running ECS task
			cancelledAllTasks := killECS(ctx, logger, integrationID)
			if cancelledAllTasks {
				updateIntegration("CANCELED", integrationID)
			}

			//cleanPidFile()

		} else {
			fmt.Printf("IntegrationID %s not found\n", integrationID)
		}
	}
}

func killECS(ctx context.Context, logger *slog.Logger, integrationID string) bool {
	//var cancelledTasks []string
	//var integrationId = os.Getenv("INTEGRATION_ID")
	var TaskArn = 7
	var ClusterName = 8

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		logger.Error("LoadDefaultConfig:" + err.Error())
		return false
	}

	client := ecs.NewFromConfig(cfg)

	// Get taskArn and ClusterName
	baseDir := getBaseDir()
	file, err := os.Open(filepath.Join(baseDir, "processors.csv"))
	if err != nil {
		logger.Info("Error opening file: %v\n", err)
		return false
	}
	defer file.Close()

	reader := csv.NewReader(file)
	rows, err := reader.ReadAll()

	// Loop through and stop each task
	for _, row := range rows {
		if row[0] == integrationID {
			// Call StopTask API
			input := &ecs.StopTaskInput{
				Cluster: aws.String(row[ClusterName]),
				Task:    aws.String(row[TaskArn]),
			}

			_, err = client.StopTask(context.TODO(), input)
			if err != nil {
				logger.Error("failed to stop task, %v", err)
				return false
			}
		}
	}
	return true
}

func updateIntegration(status string, integrationID string) {
	url := fmt.Sprintf("https://app.pennsieve.io/workflows/instances/%s/status", integrationID)

	jsonData := fmt.Sprintf(`{
		"uuid": %s,
		"status": %s,
		"timestamp": %s
	}`, integrationID, status, time.Now().String())

	req, err := http.NewRequest("POST", url, bytes.NewBuffer([]byte(jsonData)))
	if err != nil {
		fmt.Println("Error creating request:", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Println("Error making request:", err)
		return
	}
	defer resp.Body.Close()
}

func cleanPidFile() {
	// TODO: House keeping, clean up PID file
}

func getBaseDir() string {
	baseDir := os.Getenv("BASE_DIR")
	if baseDir == "" {
		baseDir = "/mnt/efs"
	}
	return baseDir
}
