package helpers

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	"io"
	"log"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

func killProcess(ctx context.Context, integrationID string, lock *sync.Mutex, logger *slog.Logger, newMsg MsgType) {
	lock.Lock()
	defer lock.Unlock()
	baseDir := GetBaseDir()

	logger.Info("Attempt to kill process for integration", "integration", integrationID)
	content, err := readFile(baseDir, integrationID)
	if err != nil {
		fmt.Println("Error reading file:", err)
		return
	}
	pidString := strings.TrimSpace(string(content))

	logger.Info("Killing nextflow", "IntegrationID", integrationID, "PID", pidString)
	pid, err := strconv.Atoi(pidString)
	if err != nil {
		logger.Error("Error converting string to int: %v\n", err)
		return
	}
	//kill - 9[PID]
	err = syscall.Kill(pid, syscall.SIGKILL)
	if err != nil {
		logger.Error("Failed to kill process. Did process already exit?", "PID", pid, "error", err)
	}
	logger.Info("Killed process", "pid", pid, "integration", integrationID)

	err = os.Remove(filepath.Join(baseDir, "pids", integrationID))
	if err != nil {
		logger.Error("Could not delete integration PID file")
	}

	// Stop running ECS task
	ecsCancelled := stopECSTasks(ctx, logger, integrationID)
	logger.Info("API", "secret", newMsg.ApiSecret, "key", newMsg.ApiKey)
	if ecsCancelled {
		err = updateIntegration(WorkflowInstanceStatusCanceled, integrationID, logger, newMsg)
		if err != nil {
			logger.Info("Failed to update integration", "error", err)
			return
		}
	}

	//Delete input and output directories after the command completes
	logger.Info("Clean up files for IntegrationID", "IntegrationID", integrationID)

	err = os.RemoveAll("service/input/" + integrationID)
	if err != nil {
		logger.Error("error deleting files",
			slog.String("error", err.Error()))
	}
	logger.Info("dir deleted", "InputDir", "service/input/"+integrationID)

	err = os.RemoveAll("service/output/" + integrationID)
	if err != nil {
		logger.Error("error deleting files",
			slog.String("error", err.Error()))
	}
	logger.Info("Dir deleted", "OutputDir", "service/output/"+integrationID)
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
	baseDir := GetBaseDir()
	logger.Info("Open Processor.csv")
	file, err := os.Open(filepath.Join(baseDir, "workspace", integrationID, "processors.csv"))
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
			}
			logger.Info("Stop ECS message sent")
		}
	}
	return true
}

func updateIntegration(status string, integrationID string, logger *slog.Logger, newMsg MsgType) error {

	env := strings.ToLower(os.Getenv("ENVIRONMENT"))
	var pennsieveHost string
	var pennsieveHost2 string
	if env == "dev" || env == "local" {
		pennsieveHost = "https://api.pennsieve.net"
		pennsieveHost2 = "https://api2.pennsieve.net"
	} else {
		pennsieveHost = "https://api.pennsieve.io"
		pennsieveHost2 = "https://api2.pennsieve.io"
	}

	accessToken, err := getAccessToken(pennsieveHost, newMsg.ApiKey, newMsg.ApiSecret)
	if err != nil {
		logger.Info("Could not access Session token", "error", err)
	}

	url := fmt.Sprintf("%s/workflows/instances/%s/status", pennsieveHost2, integrationID)

	data := map[string]interface{}{
		"uuid":      integrationID,
		"status":    status,
		"timestamp": time.Now().Unix(),
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		log.Fatalf("Error marshalling JSON: %v", err)
	}
	logger.Info("json data", "data", string(jsonData))
	logger.Info("data", "data", data)

	// Create the PUT request
	req, err := http.NewRequest("PUT", url, bytes.NewBuffer(jsonData))
	if err != nil {
		log.Fatalf("Error creating request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", accessToken))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		logger.Info("Error making request", "error", err)
		return err
	}
	logger.Info("URL", "url", url)
	logger.Info("HTTP status code", "status code", resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		logger.Info("Error reading response body", "error", err)
		return err
	}
	logger.Info("body", "body", string(body))
	if resp.StatusCode != http.StatusOK {
		logger.Info("Request failed with status code", "status code", resp.StatusCode)
	} else {
		logger.Info("Updated integration")
	}
	defer resp.Body.Close()
	return nil
}
