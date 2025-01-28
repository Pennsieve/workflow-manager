package helpers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"log"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

const (
	visibilityTimeout              = 4320 * 10
	waitingTimeout                 = 20
	WorkflowInstanceStatusCanceled = "CANCELED"
)

type CommandStatusInfo struct {
	IntegrationID string
	PID           int
	InputDir      string
	OutputDir     string
}

type MsgType struct {
	IntegrationID string `json:"integrationId"`
	ApiKey        string `json:"api_key"`
	ApiSecret     string `json:"api_secret"`
	Cancel        bool   `json:"cancel"`
}

type SQSService interface {
	ReceiveMessage(ctx context.Context, input *sqs.ReceiveMessageInput, opts ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessage(ctx context.Context, input *sqs.DeleteMessageInput, opts ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
}

func ProcessSQS(ctx context.Context, client SQSService, queueUrl string, logger *slog.Logger) (bool, error) {
	var wg sync.WaitGroup
	input := &sqs.ReceiveMessageInput{
		QueueUrl:            &queueUrl,
		MaxNumberOfMessages: 1,
		VisibilityTimeout:   visibilityTimeout,
		WaitTimeSeconds:     waitingTimeout, // use long polling
	}

	resp, err := client.ReceiveMessage(ctx, input)
	if err != nil {
		return false, fmt.Errorf("error receiving message %w", err)
	}

	log.Printf("received messages: %v", len(resp.Messages))
	if len(resp.Messages) == 0 {
		return false, nil
	}

	// Track completed nextflow tasks
	doneChannel := make(chan CommandStatusInfo)

	for _, msg := range resp.Messages {
		wg.Add(1)

		var newMsg MsgType
		id := *msg.MessageId

		err := json.Unmarshal([]byte(*msg.Body), &newMsg)
		if err != nil {
			return false, fmt.Errorf("error unmarshalling %w", err)
		}

		log.Printf("message id %s is received from SQS: %#v", id, newMsg.IntegrationID)

		var fileMutex sync.Mutex

		if newMsg.Cancel == true {
			logger.Info("Got kill signal")

			killProcess(ctx, newMsg.IntegrationID, &fileMutex, logger, newMsg)
			// Report work done
			doneChannel <- CommandStatusInfo{
				IntegrationID: newMsg.IntegrationID,
				PID:           0,
				InputDir:      "inputDir",
				OutputDir:     "outputDir",
			}
			continue
		}
		go func(msg types.Message) {
			logger.Info("Initializing workspace ...")

			integrationID := newMsg.IntegrationID
			baseDir := GetBaseDir()

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
			logger.Info("Started Nextflow command for", "integration", integrationID)

			// Save PID to file
			pid := cmd.Process.Pid

			fileMutex.Lock()
			defer fileMutex.Unlock()
			pidFile, err := os.OpenFile(filepath.Join(baseDir, "pids", integrationID), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0777)
			defer pidFile.Close()

			_, err = pidFile.WriteString(strconv.Itoa(pid))
			if err != nil {
				fmt.Printf("Error writing to file: %v\n", err)
				return
			}

			err = cmd.Wait()
			if err != nil {
				var exiterr *exec.ExitError
				if errors.As(err, &exiterr) {
					if exiterr.ExitCode() == -1 {
						logger.Info("Likely kill signal received:", "status code", exiterr.ExitCode())
					} else {
						logger.Info("Abnormal Exit", "status code", exiterr.ExitCode())
					}

				}
			}

			// Report work done
			doneChannel <- CommandStatusInfo{
				IntegrationID: integrationID,
				PID:           pid,
				InputDir:      inputDir,
				OutputDir:     outputDir,
			}
		}(msg)

		// Receive complete messages when nextflow task is done
		// Then clean up input / output files
		go func(msg types.Message) {
			for _ = range doneChannel {
				wg.Done()
				// delete message
				_, err = client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
					QueueUrl:      &queueUrl,
					ReceiptHandle: msg.ReceiptHandle,
				})

				if err != nil {
					logger.Error("error deleting message from SQS",
						slog.String("error", err.Error()))
				}
				logger.Info("message id deleted from queue", "id", id)
			}
		}(msg)
	}
	wg.Wait()
	//close(doneChannel)
	return true, nil
}
