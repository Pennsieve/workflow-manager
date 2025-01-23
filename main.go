package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
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
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
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
			go killProcess(newMsg.IntegrationID)
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
		go func() {
			for cmdInfo := range doneChannel {
				// Delete input and output directories after the command completes
				fmt.Printf("Clean up files for (IntegrationID: %s)\n", cmdInfo.IntegrationID)

				err = os.RemoveAll(cmdInfo.InputDir)
				if err != nil {
					logger.Error("error deleting files",
						slog.String("error", err.Error()))
				}
				log.Printf("dir %s deleted", cmdInfo.InputDir)

				err = os.RemoveAll(cmdInfo.OutputDir)
				if err != nil {
					logger.Error("error deleting files",
						slog.String("error", err.Error()))
				}
				log.Printf("Dir %s deleted", cmdInfo.OutputDir)

				// delete message
				_, err = sqsSvc.DeleteMessage(ctx, &sqs.DeleteMessageInput{
					QueueUrl:      &queueUrl,
					ReceiptHandle: msg.ReceiptHandle,
				})

				if err != nil {
					logger.Error("error deleting message from SQS",
						slog.String("error", err.Error()))
				}
				log.Printf("message id %s is deleted from queue", id)
			}
		}()

		close(doneChannel)

	}
	wg.Wait()
	return true, nil
}

func killProcess(integrationID string) {
	baseDir := os.Getenv("BASE_DIR")
	if baseDir == "" {
		baseDir = "/mnt/efs"
	}

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
			killECS()
			updateIntegration()
			cleanPidFile()

		} else {
			fmt.Printf("IntegrationID %s not found\n", integrationID)
		}
	}
}

func killECS() {
	// TODO: Kill ECS task based on containerID from processors.csv
}

func updateIntegration() {
	// TODO: Send back Cancelled status on successful kill of container
}

func cleanPidFile() {
	// TODO: House keeping, clean up PID file
}
