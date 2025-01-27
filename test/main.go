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
	"github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/aws/aws-sdk-go-v2/service/ecs"
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

func main() {
	programLevel := new(slog.LevelVar)
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: programLevel}))
	slog.SetDefault(logger)

	logger.Info("Welcome to the WorkflowManager")

	baseDir := getBaseDir()

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

	err = os.MkdirAll("pids", 0777)
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}
	err = os.Chown("pids", 1000, 1000)
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}

	//cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		log.Fatalf("LoadDefaultConfig: %v\n", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)

	queueUrl := os.Getenv("SQS_URL")
	log.Printf("QUEUE_URL: %s", queueUrl)

	//sqsSvc := sqs.NewFromConfig(cfg)

	var wg sync.WaitGroup

loop:
	for {
		select {
		case <-signalChan: // if get SIGTERM
			log.Println("got SIGTERM signal, cancelling the context")
			cancel() // cancel context

		default:
			_, err := processSQS(ctx, queueUrl, logger, &wg)

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

func processSQS(ctx context.Context, queueUrl string, logger *slog.Logger, wg *sync.WaitGroup) (bool, error) {
	//input := &sqs.ReceiveMessageInput{
	//	QueueUrl:            &queueUrl,
	//	MaxNumberOfMessages: 1,
	//	VisibilityTimeout:   visibilityTimeout,
	//	WaitTimeSeconds:     waitingTimeout, // use long polling
	//}

	//resp, err := sqsSvc.ReceiveMessage(ctx, input)
	regularMsg := MsgType{
		IntegrationID: "54321",
		ApiKey:        "api-key-xyz",
		ApiSecret:     "api-secret-abc",
		Cancel:        false,
	}
	regularMsg1 := MsgType{
		IntegrationID: "101010",
		ApiKey:        "api-key-xyz",
		ApiSecret:     "api-secret-abc",
		Cancel:        false,
	}
	regularMsg2 := MsgType{
		IntegrationID: "2468",
		ApiKey:        "api-key-xyz",
		ApiSecret:     "api-secret-abc",
		Cancel:        false,
	}
	regularMsg3 := MsgType{
		IntegrationID: "102030",
		ApiKey:        "api-key-xyz",
		ApiSecret:     "api-secret-abc",
		Cancel:        false,
	}
	regularMsg4 := MsgType{
		IntegrationID: "9876",
		ApiKey:        "api-key-xyz",
		ApiSecret:     "api-secret-abc",
		Cancel:        false,
	}
	regularMsg5 := MsgType{
		IntegrationID: "1234",
		ApiKey:        "api-key-xyz",
		ApiSecret:     "api-secret-abc",
		Cancel:        false,
	}
	cancelMsg := MsgType{
		IntegrationID: "54321",
		ApiKey:        "api-key-xyz",
		ApiSecret:     "api-secret-abc",
		Cancel:        true,
	}
	regularMsgJson, _ := json.Marshal(regularMsg)
	regularMsgJson1, _ := json.Marshal(regularMsg1)
	regularMsgJson2, _ := json.Marshal(regularMsg2)
	regularMsgJson3, _ := json.Marshal(regularMsg3)
	regularMsgJson4, _ := json.Marshal(regularMsg4)
	regularMsgJson5, _ := json.Marshal(regularMsg5)
	cancelMsgJson, _ := json.Marshal(cancelMsg)
	resp := &sqs.ReceiveMessageOutput{
		Messages: []types.Message{
			{
				MessageId:     aws.String("2b3c4d5e-6789-01ab-cdef-EXAMPLE22222"),
				ReceiptHandle: aws.String("AQEBzJnKyrHigUMZj6rYigCgxlaS3SLy1b..."),
				Body:          aws.String(string(regularMsgJson)),
				Attributes: map[string]string{
					"SenderId": "987654321098",
				},
			},
			{
				MessageId:     aws.String("2b3c4d5e-6789-01ab-cdef-EXAMPLE22222"),
				ReceiptHandle: aws.String("AQEBzJnKyrHigUMZj6rYigCgxlaS3SLy1b..."),
				Body:          aws.String(string(regularMsgJson1)),
				Attributes: map[string]string{
					"SenderId": "987654321098",
				},
			},
			{
				MessageId:     aws.String("2b3c4d5e-6789-01ab-cdef-EXAMPLE22222"),
				ReceiptHandle: aws.String("AQEBzJnKyrHigUMZj6rYigCgxlaS3SLy1b..."),
				Body:          aws.String(string(regularMsgJson2)),
				Attributes: map[string]string{
					"SenderId": "987654321098",
				},
			},
			{
				MessageId:     aws.String("2b3c4d5e-6789-01ab-cdef-EXAMPLE22222"),
				ReceiptHandle: aws.String("AQEBzJnKyrHigUMZj6rYigCgxlaS3SLy1b..."),
				Body:          aws.String(string(regularMsgJson3)),
				Attributes: map[string]string{
					"SenderId": "987654321098",
				},
			},
			{
				MessageId:     aws.String("2b3c4d5e-6789-01ab-cdef-EXAMPLE22222"),
				ReceiptHandle: aws.String("AQEBzJnKyrHigUMZj6rYigCgxlaS3SLy1b..."),
				Body:          aws.String(string(regularMsgJson4)),
				Attributes: map[string]string{
					"SenderId": "987654321098",
				},
			},
			{
				MessageId:     aws.String("2b3c4d5e-6789-01ab-cdef-EXAMPLE22222"),
				ReceiptHandle: aws.String("AQEBzJnKyrHigUMZj6rYigCgxlaS3SLy1b..."),
				Body:          aws.String(string(regularMsgJson5)),
				Attributes: map[string]string{
					"SenderId": "987654321098",
				},
			},
			{
				MessageId:     aws.String("3c4d5e6f-7890-12ab-cdef-EXAMPLE33333"),
				ReceiptHandle: aws.String("AQEBxKnKyrHigUMZj6rYigCgxlaS3SLy2c..."),
				Body:          aws.String(string(cancelMsgJson)),
				Attributes: map[string]string{
					"SenderId": "112233445566",
				},
			},
		},
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
		log.Printf("message cancel status %b", newMsg.Cancel)

		var fileMutex sync.Mutex
		if newMsg.Cancel == true {
			logger.Info("Received Kill signal")

			go killProcess(ctx, newMsg.IntegrationID, &fileMutex, logger, newMsg)
			wg.Done()
			continue
		}

		go func(msg types.Message) {
			defer wg.Done()
			logger.Info("Initializing workspace ...")

			integrationID := newMsg.IntegrationID
			baseDir := getBaseDir()

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

			logger.Info("Nextflow command output", "stdout", stdout.String())
			logger.Info("Nextflow command output", "stdout", stderr.String())
			// Save PID to file
			pid := cmd.Process.Pid
			fileMutex.Lock()
			defer fileMutex.Unlock()
			pidFile, err := os.OpenFile(filepath.Join(baseDir, "pids", integrationID), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0777)
			defer pidFile.Close()

			if err != nil {
				fmt.Printf("Error locking file: %v\n", err)
				return
			}

			_, err = pidFile.WriteString(strconv.Itoa(pid))
			if err != nil {
				fmt.Printf("Error writing to file: %v\n", err)
				return
			}

			logger.Info("Saved pid to file", "file", filepath.Join(baseDir, "pids", integrationID))

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
				logger.Info("Clean up files for IntegrationID", "IntegrationID", cmdInfo.IntegrationID)

				err = os.RemoveAll(cmdInfo.InputDir)
				if err != nil {
					logger.Error("error deleting files",
						slog.String("error", err.Error()))
				}
				logger.Info("dir deleted", "InputDir", cmdInfo.InputDir)

				err = os.RemoveAll(cmdInfo.OutputDir)
				if err != nil {
					logger.Error("error deleting files",
						slog.String("error", err.Error()))
				}
				logger.Info("Dir deleted", "OutputDir", cmdInfo.OutputDir)

				// delete message
				//_, err = sqsSvc.DeleteMessage(ctx, &sqs.DeleteMessageInput{
				//	QueueUrl:      &queueUrl,
				//	ReceiptHandle: msg.ReceiptHandle,
				//})

				if err != nil {
					logger.Error("error deleting message from SQS",
						slog.String("error", err.Error()))
				}
				logger.Info("message id deleted from queue", "id", id)
			}
		}(msg)

	}
	wg.Wait()
	return true, nil
}

func killProcess(ctx context.Context, integrationID string, lock *sync.Mutex, logger *slog.Logger, newMsg MsgType) {
	lock.Lock()
	defer lock.Unlock()
	baseDir := getBaseDir()
	logger.Info("Attempt to kill process for integration", "integration", integrationID)

	//pidFile, err := os.OpenFile(filepath.Join(baseDir, "pids", integrationID), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0777)
	//defer pidFile.Close()
	//
	//err = syscall.Flock(int(pidFile.Fd()), syscall.LOCK_EX)
	//if err != nil {
	//	fmt.Printf("Error locking file: %v\n", err)
	//	return
	//}
	//defer syscall.Flock(int(pidFile.Fd()), syscall.LOCK_UN)

	content, err := readFile(baseDir, integrationID)
	if err != nil {
		fmt.Println("Error reading file:", err)
		return
	}
	pidString := strings.TrimSpace(string(content))

	logger.Info("Killing integration", "IntegrationID", integrationID, "PID", pidString)
	pid, err := strconv.Atoi(pidString)
	if err != nil {
		logger.Error("Error converting string to int: %v\n", err)
		return
	}
	// kill -9 [PID]
	err = syscall.Kill(pid, syscall.SIGKILL)
	if err != nil {
		logger.Error("Failed to kill process", "PID", pid, "error", err)
		return
	}
	logger.Info("Killed process", "pid", pid, "integration", integrationID)
	// Stop running ECS task
	cancelledAllTasks := stopECSTasks(ctx, logger, integrationID)
	if cancelledAllTasks {
		err = updateIntegration(WorkflowInstanceStatusCanceled, integrationID, logger, newMsg)
		if err != nil {
			logger.Info("Failed to update integration", "error", err)
			return
		}
	}
}

func stopECSTasks(ctx context.Context, logger *slog.Logger, integrationID string) bool {
	var TaskArn = 7
	var ClusterName = 8

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		logger.Error("LoadDefaultConfigError", "error", err.Error())
		return false
	}

	client := ecs.NewFromConfig(cfg)

	// Get taskArn and ClusterName
	baseDir := getBaseDir()
	file, err := os.Open(filepath.Join(baseDir, "processors.csv"))
	if err != nil {
		logger.Info("Error opening file", "error", err)
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
				logger.Error("failed to stop task", "integrationID", integrationID, "error", err)
				return false
			}
		}
	}
	return true
}

func updateIntegration(status string, integrationID string, logger *slog.Logger, newMsg MsgType) error {

	env := strings.ToLower(os.Getenv("ENVIRONMENT"))
	var pennsieveHost string
	if env == "dev" || env == "local" {
		pennsieveHost = "https://api2.pennsieve.net"
	} else {
		pennsieveHost = "https://api2.pennsieve.io"
	}
	accessToken, err := getAccessToken(pennsieveHost, newMsg.ApiKey, newMsg.ApiSecret)
	if err != nil {
		logger.Info("Could not access Session token", "error", err)
	}

	url := fmt.Sprintf("%s/workflows/instances/%s/status", pennsieveHost, integrationID)

	jsonData := fmt.Sprintf(`{
		"uuid": %s,
		"status": %s,
		"timestamp": %s
	}`, integrationID, status, time.Now().String())

	req, err := http.NewRequest("POST", url, bytes.NewBuffer([]byte(jsonData)))
	if err != nil {
		logger.Info("Error creating request", "error", err)
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", accessToken))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		logger.Info("Error making request", "error", err)
		return err
	}
	if resp.StatusCode != 200 {
		logger.Info("Request failed with status code", "status code", resp.StatusCode)
	} else {
		logger.Info("Updated integration")
	}
	defer resp.Body.Close()
	return nil
}

func getBaseDir() string {
	baseDir := os.Getenv("BASE_DIR")
	if baseDir == "" {
		baseDir = "/mnt/efs"
	}
	return baseDir
}

func getAccessToken(pennsieveHost string, apiKey string, apiSecret string) (string, error) {

	var sessionToken string

	resp, err := http.Get(fmt.Sprintf("%s/authentication/cognito-config", pennsieveHost))
	if err != nil {
		return sessionToken, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return sessionToken, errors.New(fmt.Sprintf("invalid status code: %s", resp.StatusCode))
	}

	var cognitoConfig struct {
		TokenPool struct {
			AppClientId string `json:"appClientId"`
		} `json:"tokenPool"`
		Region string `json:"region"`
	}
	err = json.NewDecoder(resp.Body).Decode(&cognitoConfig)
	if err != nil {
		return sessionToken, err
	}

	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(cognitoConfig.Region))
	if err != nil {
		return sessionToken, err
	}

	cognitoClient := cognitoidentityprovider.NewFromConfig(cfg)

	input := &cognitoidentityprovider.InitiateAuthInput{
		AuthFlow: "USER_PASSWORD_AUTH",
		AuthParameters: map[string]string{
			"USERNAME": apiKey,
			"PASSWORD": apiSecret,
		},
		ClientId: aws.String(cognitoConfig.TokenPool.AppClientId),
	}

	authResp, err := cognitoClient.InitiateAuth(context.TODO(), input)
	if err != nil {
		return sessionToken, err
	}

	sessionToken = aws.ToString(authResp.AuthenticationResult.AccessToken)
	fmt.Println("Session Token:", sessionToken)

	return sessionToken, nil
}

func readFile(baseDir, integrationID string) ([]byte, error) {
	filePath := filepath.Join(baseDir, "pids", integrationID)
	for {
		content, err := os.ReadFile(filePath)
		if err != nil {
			if !os.IsNotExist(err) {
				return nil, fmt.Errorf("error reading file: %w", err)
			}
		} else if len(content) > 0 {
			// Return content if the file is not empty
			return content, nil
		}

		time.Sleep(100 * time.Millisecond)
	}
}
