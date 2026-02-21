package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider"
	providerTypes "github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

const (
	visibilityTimeout = 4320 * 10 // 12 hours
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

	err = os.MkdirAll("workdir", 0777)
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}
	err = os.Chown("workdir", 1000, 1000)
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
	SessionToken  string `json:"session_token"`
	RefreshToken  string `json:"refresh_token"`
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

		// check workflow instance status in DB
		// if running, skip processing this message
		// if not running, continue processing
		environment := os.Getenv("ENVIRONMENT")
		var apiHost2 string

		if environment == "local" || environment == "dev" {
			apiHost2 = "https://api2.pennsieve.net"

		} else {
			apiHost2 = "https://api2.pennsieve.io"
		}

		workflowInstanceResponse, err := getRun(apiHost2, newMsg.IntegrationID, newMsg.SessionToken)
		if err != nil {
			logger.Error(err.Error())
		}

		var executionRun ExecutionRun
		if err := json.Unmarshal(workflowInstanceResponse, &executionRun); err != nil {
			logger.Error(err.Error())
		}
		fmt.Println(executionRun)

		if executionRun.Status == "STARTED" || executionRun.Status == "SUCCEEDED" {
			// This is a retry after 12 hours, but job already processed
			logger.Info("job already processed, deleting message",
				slog.String("workflowInstanceId", newMsg.IntegrationID),
				slog.String("status", executionRun.Status))

			_, err = sqsSvc.DeleteMessage(ctx, &sqs.DeleteMessageInput{
				QueueUrl:      &queueUrl,
				ReceiptHandle: msg.ReceiptHandle,
			})
			if err != nil {
				logger.Error("error deleting message",
					slog.String("error", err.Error()))
			}
			continue
		}

		if executionRun.Status == "NOT_STARTED" {
			logger.Info("job not started yet, processing message and setting status to STARTED",
				slog.String("workflowInstanceId", newMsg.IntegrationID),
				slog.String("status", executionRun.Status))

			// Update workflow status to "started"
			_, err = putRunStatus(
				apiHost2,
				newMsg.IntegrationID,
				"STARTED",
				time.Now().Unix(),
				newMsg.SessionToken,
			)
			if err != nil {
				logger.Error("failed to update workflow status to started",
					slog.String("error", err.Error()))
			}
		}

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

			// resourcesDir
			resourcesDir := fmt.Sprintf("%s/resources", baseDir)
			err = os.MkdirAll(resourcesDir, 0777)
			if err != nil {
				logger.Error(err.Error())
				os.Exit(1)
			}

			// workDir
			workDir := fmt.Sprintf("%s/workDir/%s", baseDir, integrationID)
			err = os.MkdirAll(outputDir, 0777)
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
				"run", "./workflows/test.ecr.nf", "-ansi-log", "false",
				"-w", workspaceDir,
				"--integrationID", integrationID,
				"--sessionToken", newMsg.SessionToken,
				"--refreshToken", newMsg.RefreshToken,
				"--workspaceDir", workspaceDir,
				"--resourcesDir", resourcesDir,
				"--workDir", workDir)
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

			err = os.RemoveAll(workDir)
			if err != nil {
				logger.Error("error deleting files",
					slog.String("error", err.Error()))
			}
			log.Printf("Dir %s deleted", workDir)

			logger.Info("starting message deletion")
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

type ExecutionRun struct {
	Uuid   string `json:"uuid"`
	Status string `json:"status"`
}

func getRun(apiHost string, workflowInstanceId string, sessionToken string) ([]byte, error) {
	url := fmt.Sprintf("%s/compute/workflows/runs/%s", apiHost, workflowInstanceId)

	req, _ := http.NewRequest("GET", url, nil)

	req.Header.Add("accept", "application/json")
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", sessionToken))

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)

	return body, nil
}

func putRunStatus(apiHost string, workflowInstanceId string, status string, timestamp int64, sessionToken string) ([]byte, error) {
	url := fmt.Sprintf("%s/compute/workflows/runs/%s/status", apiHost, workflowInstanceId)

	requestBody := map[string]interface{}{
		"status":    status,
		"timestamp": timestamp,
	}

	jsonBody, _ := json.Marshal(requestBody)

	req, _ := http.NewRequest("PUT", url, bytes.NewBuffer(jsonBody))

	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", sessionToken))

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)

	return body, nil
}

type Client struct {
	apiHost string
	// other fields...
}

func (c *Client) Authenticate(apiKey, apiSecret string) (string, error) {
	url := fmt.Sprintf("%s/authentication/cognito-config", c.apiHost)

	// Get cognito config
	resp, err := http.Get(url)
	if err != nil {
		return "", fmt.Errorf("failed to reach authentication server: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("failed to reach authentication server with status: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read response body: %w", err)
	}

	// Parse JSON response
	var data struct {
		TokenPool struct {
			AppClientID string `json:"appClientId"`
		} `json:"tokenPool"`
		Region string `json:"region"`
	}

	if err := json.Unmarshal(body, &data); err != nil {
		return "", fmt.Errorf("failed to decode authentication response: %w", err)
	}

	cognitoAppClientID := data.TokenPool.AppClientID
	cognitoRegion := data.Region

	// Create Cognito IDP client
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(cognitoRegion),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("", "", "")),
	)
	if err != nil {
		return "", fmt.Errorf("failed to load AWS config: %w", err)
	}

	cognitoClient := cognitoidentityprovider.NewFromConfig(cfg)

	// Initiate authentication
	authFlow := providerTypes.AuthFlowTypeUserPasswordAuth
	loginResponse, err := cognitoClient.InitiateAuth(context.Background(), &cognitoidentityprovider.InitiateAuthInput{
		AuthFlow: authFlow,
		AuthParameters: map[string]string{
			"USERNAME": apiKey,
			"PASSWORD": apiSecret,
		},
		ClientId: &cognitoAppClientID,
	})

	if err != nil {
		return "", fmt.Errorf("failed to authenticate: %w", err)
	}

	if loginResponse.AuthenticationResult == nil || loginResponse.AuthenticationResult.AccessToken == nil {
		return "", fmt.Errorf("authentication result is nil")
	}

	accessToken := *loginResponse.AuthenticationResult.AccessToken
	return accessToken, nil
}
