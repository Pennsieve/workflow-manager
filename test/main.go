package main

import (
	"context"
	"encoding/csv"
	"errors"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/pennsieve/workflow-manager/helpers"
	"github.com/stretchr/testify/mock"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
)

const (
	visibilityTimeout = 4320 * 10
	waitingTimeout    = 20
)

// Mock SQS Client
type MockSQSClient struct {
	mock.Mock
}
type MockSQSProcessor struct {
	Client *MockSQSClient
	Logger *slog.Logger
}

func (m *MockSQSClient) ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, opts ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	args := m.Called(ctx, params)
	return args.Get(0).(*sqs.ReceiveMessageOutput), args.Error(1)
}

func (m *MockSQSClient) DeleteMessage(ctx context.Context, params *sqs.DeleteMessageInput, opts ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	args := m.Called(ctx, params)
	return args.Get(0).(*sqs.DeleteMessageOutput), args.Error(1)
}

func (m *MockSQSProcessor) Process(ctx context.Context, queueUrl string) (bool, error) {

	messageBody1 := `{"integrationId": "493f7f5f-ca6e-4b4c-8ba9-78932e4e4f36","api_key": "","api_secret": ""}` // Needs to be an actual ID, key and secret for local test to work
	messageBody2 := `{"integrationId": "mgs2","api_key": "": ""}`
	messageBody3 := `{"integrationId": "mgs3","api_key": "","api_secret": ""}`
	messageBody4 := `{"integrationId": "mgs4","api_key": "","api_secret": ""}`
	messageBody5 := `{"integrationId": "mgs5","api_key": "","api_secret": ""}`
	messageBody6 := `{"integrationId": "mgs6","api_key": "","api_secret": "}`
	messageBody7 := `{"integrationId": "493f7f5f-ca6e-4b4c-8ba9-78932e4e4f36","api_key": "","api_secret": "","cancel": true}`

	mockMessages := []types.Message{
		{
			MessageId:     aws.String("1"),
			Body:          aws.String(messageBody1),
			ReceiptHandle: aws.String("handle123"),
		},
		{
			MessageId:     aws.String("2"),
			Body:          aws.String(messageBody2),
			ReceiptHandle: aws.String("handle124"),
		},
		{
			MessageId:     aws.String("3"),
			Body:          aws.String(messageBody3),
			ReceiptHandle: aws.String("handle125"),
		},
		{
			MessageId:     aws.String("4"),
			Body:          aws.String(messageBody4),
			ReceiptHandle: aws.String("handle125"),
		},
		{
			MessageId:     aws.String("5"),
			Body:          aws.String(messageBody5),
			ReceiptHandle: aws.String("handle125"),
		},
		{
			MessageId:     aws.String("6"),
			Body:          aws.String(messageBody6),
			ReceiptHandle: aws.String("handle125"),
		},
		{
			MessageId:     aws.String("7"),
			Body:          aws.String(messageBody7),
			ReceiptHandle: aws.String("handle125"),
		},
	}
	m.Client.On("ReceiveMessage", ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            &queueUrl,
		MaxNumberOfMessages: 1,
		VisibilityTimeout:   visibilityTimeout,
		WaitTimeSeconds:     waitingTimeout,
	}).Return(&sqs.ReceiveMessageOutput{
		Messages: mockMessages,
	}, nil)

	m.Client.On("DeleteMessage", mock.Anything, &sqs.DeleteMessageInput{
		QueueUrl:      &queueUrl,
		ReceiptHandle: aws.String("handle123"),
	}).Return(&sqs.DeleteMessageOutput{}, nil)

	m.Client.On("DeleteMessage", mock.Anything, &sqs.DeleteMessageInput{
		QueueUrl:      &queueUrl,
		ReceiptHandle: aws.String("handle124"),
	}).Return(&sqs.DeleteMessageOutput{}, nil)

	m.Client.On("DeleteMessage", mock.Anything, &sqs.DeleteMessageInput{
		QueueUrl:      &queueUrl,
		ReceiptHandle: aws.String("handle125"),
	}).Return(&sqs.DeleteMessageOutput{}, nil)

	// Process the message
	success, err := helpers.ProcessSQS(ctx, m.Client, queueUrl, m.Logger)
	if err != nil {
		return false, err
	}

	// Mock DeleteMessage behavior if message was processed
	if success {
		m.Client.On("DeleteMessage", ctx, &sqs.DeleteMessageInput{
			QueueUrl:      &queueUrl,
			ReceiptHandle: aws.String("handle123"),
		}).Return(&sqs.DeleteMessageOutput{}, nil).Once()
	}

	return success, nil
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

	// Write file for mgs1 in test since it will fire too quickly for nextflow to write it
	filePath := "/service/workspace/493f7f5f-ca6e-4b4c-8ba9-78932e4e4f36/processors.csv"
	dirPath := "/service/workspace/493f7f5f-ca6e-4b4c-8ba9-78932e4e4f36"

	if err := os.MkdirAll(dirPath, os.ModePerm); err != nil {
		log.Fatalf("Failed to create directories: %s", err)
	}

	file, err := os.Create(filePath)
	if err != nil {
		log.Fatalf("Failed to create file: %s", err)
	}
	defer file.Close()

	log.Println("File created successfully:", filePath)

	writer := csv.NewWriter(file)
	defer writer.Flush()

	header := []string{"integration_id", "log_group_name", "log_stream_name", "application_id", "container_name", "application_type"}
	if err := writer.Write(header); err != nil {
		log.Fatalf("Failed to write header: %s", err)
	}

	rows := [][]string{
		{"someId", "someLogGroupName", "someLogStreamName", "4083a1ce-0ce4-47ff-9a0c-d750c92b0566", "738082483-dev", "preprocessor"},
		{"someId", "someLogGroupName", "someLogStreamName", "04bb67f2-67b1-4817-b490-6ef9981fc8eb", "python-application-template-537996532276-dev", "processor"},
		{"someId", "someLogGroupName", "someLogStreamName", "5ea21208-f1b2-43c7-83e5-680d10ebe574", "1717977142-dev", "postprocessor"},
	}
	for _, row := range rows {
		if err := writer.Write(row); err != nil {
			log.Fatalf("Failed to write row: %s", err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)

	// Mock queue URL for testing
	queueUrl := "http://mock-queue"

	// Create a mock SQS client
	mockSQSClient := &MockSQSClient{}

	// Use MockSQSProcessor with the mock client
	processor := &MockSQSProcessor{
		Client: mockSQSClient,
		Logger: logger,
	}

	// Call run with the mock processor
	if err := run(ctx, processor, queueUrl, logger); err != nil {
		logger.Error("Service stopped with error", slog.Any("error", err))
		os.Exit(1)
	}
	log.Println("service is safely stopped")
}

// Program while loop
func run(ctx context.Context, processor *MockSQSProcessor, queueUrl string, logger *slog.Logger) error {
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
