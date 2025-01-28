package helpers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

func SetupFolders(baseDir string, logger *slog.Logger) error {
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
	return err
}

func GetBaseDir() string {
	baseDir := os.Getenv("BASE_DIR")
	if baseDir == "" {
		baseDir = "/mnt/efs"
	}
	return baseDir
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

func getAccessToken(pennsieveHost string, apiKey string, apiSecret string) (string, error) {

	var sessionToken string

	url := fmt.Sprintf("%s/authentication/cognito-config", pennsieveHost)
	resp, err := http.Get(url)
	if err != nil {
		return sessionToken, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return sessionToken, errors.New(fmt.Sprintf("invalid status code: %d", resp.StatusCode))
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

	return sessionToken, nil
}
