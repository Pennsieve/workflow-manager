package main

import (
	"context"
	"errors"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/pennsieve/workflow-manager/helpers"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
)

type SQSProcessor interface {
	Process(ctx context.Context, queueUrl string) (bool, error)
}

type AwsSQSProcessor struct {
	Client *sqs.Client
	Logger *slog.Logger
}

func (r *AwsSQSProcessor) Process(ctx context.Context, queueUrl string) (bool, error) {
	return helpers.ProcessSQS(ctx, r.Client, queueUrl, r.Logger)
}

func main() {
	programLevel := new(slog.LevelVar)
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: programLevel}))
	slog.SetDefault(logger)

	logger.Info("Welcome to the WorkflowManager")

	baseDir := helpers.GetBaseDir()

	err := helpers.SetupFolders(baseDir, logger)
	if err != nil {
		logger.Info("Failed to setup folders", "error", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)

	queueUrl := os.Getenv("SQS_URL")
	log.Printf("QUEUE_URL: %s", queueUrl)

	sqsSvc, err := initializeAWS(context.Background())
	if err != nil {
		log.Fatalf("Failed to initialize AWS: %v", err)
	}

	processor := &AwsSQSProcessor{
		Client: sqsSvc,
		Logger: logger,
	}

	// Call run with the real processor
	if err := run(ctx, processor, queueUrl, logger); err != nil {
		logger.Error("Service stopped with error", slog.Any("error", err))
		os.Exit(1)
	}
	log.Println("service is safely stopped")
}

// program while loop
func run(ctx context.Context, processor SQSProcessor, queueUrl string, logger *slog.Logger) error {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)

loop:
	for {
		select {
		case <-signalChan:
			logger.Info("Received SIGTERM, stopping service")
			return nil

		default:
			success, err := processor.Process(ctx, queueUrl)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					logger.Info("Context canceled, stopping processing")
					break loop
				}
				logger.Error("Error processing SQS", slog.Any("error", err))
				return err
			}
			if success {
				logger.Info("Processed a message successfully")
			}
		}
	}
	return nil
}

func initializeAWS(ctx context.Context) (*sqs.Client, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}
	return sqs.NewFromConfig(cfg), nil
}
