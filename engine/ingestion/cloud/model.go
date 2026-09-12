// Package cloudingestion contains the AWS Lambda control and worker handlers
// for durable Massive market-data ingestion.
package cloudingestion

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

const (
	defaultAWSRegion = "us-east-1"
	defaultMaxUnits  = 100000
	maxSymbols       = 100
	maxAttempts      = 5
)

// Config is the non-secret runtime configuration for both Lambda handlers.
// The provider key is deliberately not represented here.
type Config struct {
	Region           string
	Bucket           string
	TableName        string
	QueueURL         string
	WorkerFunction   string
	MassiveSecretARN string
	MaxUnits         int
}

func ConfigFromEnv(getenv func(string) string) (Config, error) {
	if getenv == nil {
		getenv = os.Getenv
	}
	cfg := Config{
		Region:           strings.TrimSpace(getenv("BACKTEST_AWS_REGION")),
		Bucket:           strings.TrimSpace(getenv("INGESTION_S3_BUCKET")),
		TableName:        strings.TrimSpace(getenv("INGESTION_TABLE_NAME")),
		QueueURL:         strings.TrimSpace(getenv("INGESTION_QUEUE_URL")),
		WorkerFunction:   strings.TrimSpace(getenv("INGESTION_WORKER_FUNCTION")),
		MassiveSecretARN: strings.TrimSpace(getenv("MASSIVE_SECRET_ARN")),
		MaxUnits:         defaultMaxUnits,
	}
	if cfg.Region == "" {
		cfg.Region = defaultAWSRegion
	}
	if value := strings.TrimSpace(getenv("INGESTION_MAX_UNITS")); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 1 {
			return Config{}, fmt.Errorf("INGESTION_MAX_UNITS must be a positive integer")
		}
		cfg.MaxUnits = parsed
	}
	return cfg, nil
}

func (c Config) validateControl() error {
	if c.Bucket == "" {
		return errors.New("INGESTION_S3_BUCKET is required")
	}
	if c.TableName == "" {
		return errors.New("INGESTION_TABLE_NAME is required")
	}
	if c.QueueURL == "" {
		return errors.New("INGESTION_QUEUE_URL is required")
	}
	if c.WorkerFunction == "" {
		return errors.New("INGESTION_WORKER_FUNCTION is required")
	}
	return nil
}

func (c Config) validateWorker() error {
	if c.Bucket == "" {
		return errors.New("INGESTION_S3_BUCKET is required")
	}
	if c.TableName == "" {
		return errors.New("INGESTION_TABLE_NAME is required")
	}
	if c.MassiveSecretARN == "" && strings.TrimSpace(os.Getenv("MASSIVE_API_KEY")) == "" {
		return errors.New("MASSIVE_SECRET_ARN is required")
	}
	return nil
}

// Command is the payload sent by the desktop control boundary.
type Command struct {
	Action string          `json:"action"`
	Input  json.RawMessage `json:"input"`
}

// UnitMessage is the durable queue payload for one symbol/date unit.
type UnitMessage struct {
	JobID  string `json:"job_id"`
	Symbol string `json:"symbol"`
	Date   string `json:"date"`
}

// QueueEvent is intentionally compatible with the Lambda SQS event shape.
type QueueEvent struct {
	Records []QueueRecord `json:"Records"`
}

type QueueRecord struct {
	MessageID string `json:"messageId"`
	Body      string `json:"body"`
}

type unitState struct {
	JobID       string
	Symbol      string
	Date        string
	Status      string
	Attempts    int
	Rows        int64
	InvalidRows int64
	Error       string
}

type schedule struct {
	ID      string
	ListID  string
	Symbols []string
	Enabled bool
}

type controlResult struct {
	Value any
}
