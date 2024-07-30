package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

const (
	visibilityTimeout = 4320 * 10
	waitingTimeout    = 20
)

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

loop:
	for {
		select {
		case <-signalChan: // if get SIGTERM
			log.Println("got SIGTERM signal, cancelling the context")
			cancel() // cancel context

		default:
			_, err := processSQS(ctx, sqsSvc, queueUrl, logger)

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
}

func processSQS(ctx context.Context, sqsSvc *sqs.Client, queueUrl string, logger *slog.Logger) (bool, error) {
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

	for _, msg := range resp.Messages {
		var newMsg MsgType
		id := *msg.MessageId

		err := json.Unmarshal([]byte(*msg.Body), &newMsg)
		if err != nil {
			return false, fmt.Errorf("error unmarshalling %w", err)
		}

		log.Printf("message id %s is received from SQS: %#v", id, newMsg.IntegrationID)

		go func(msg types.Message) {
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
				"--apiSecret", newMsg.ApiSecret)
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

			// list workspace files
			logger.Info("Listing workspace files")
			cmd2 := exec.Command("ls", "-alhR", workspaceDir)
			cmd2.Dir = "/service"
			var stdout2 strings.Builder
			var stderr2 strings.Builder
			cmd2.Stdout = &stdout2
			cmd2.Stderr = &stderr2
			if err := cmd2.Run(); err != nil {
				logger.Error(err.Error(),
					slog.String("error", stderr2.String()))
			}
			fmt.Println(stdout2.String())

			// cleanup files
			err = os.RemoveAll(inputDir)
			if err != nil {
				logger.Error("error deleting files",
					slog.String("error", err.Error()))
			}
			log.Printf("Dir %s deleted", inputDir)

			err = os.RemoveAll(outputDir)
			if err != nil {
				logger.Error("error deleting files",
					slog.String("error", err.Error()))
			}
			log.Printf("Dir %s deleted", outputDir)

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
		}(msg)

	}
	return true, nil
}
