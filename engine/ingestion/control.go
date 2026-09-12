package ingestion

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
)

var ErrNotConfigured = errors.New("ingestion AWS control Lambda is not configured")

type ControlInvoker interface {
	Invoke(ctx context.Context, action string, input any, output any) error
}

type LambdaControl struct {
	region  string
	profile string
	arn     string
	once    sync.Once
	client  *lambda.Client
	loadErr error
}

func NewLambdaControl(cfg Config) *LambdaControl {
	return &LambdaControl{region: cfg.Region, profile: cfg.Profile, arn: cfg.ControlARN}
}

func (c *LambdaControl) Invoke(ctx context.Context, action string, input any, output any) error {
	if c.arn == "" {
		return ErrNotConfigured
	}
	c.once.Do(func() {
		options := []func(*config.LoadOptions) error{config.WithRegion(c.region)}
		if c.profile != "" {
			options = append(options, config.WithSharedConfigProfile(c.profile))
		}
		awsCfg, err := config.LoadDefaultConfig(ctx, options...)
		if err != nil {
			c.loadErr = fmt.Errorf("load AWS credentials: %w", err)
			return
		}
		c.client = lambda.NewFromConfig(awsCfg)
	})
	if c.loadErr != nil {
		return c.loadErr
	}
	payload, err := json.Marshal(struct {
		Action string `json:"action"`
		Input  any    `json:"input"`
	}{Action: action, Input: input})
	if err != nil {
		return fmt.Errorf("encode ingestion command: %w", err)
	}
	result, err := c.client.Invoke(ctx, &lambda.InvokeInput{
		FunctionName:   &c.arn,
		InvocationType: "RequestResponse",
		Payload:        payload,
	})
	if err != nil {
		return fmt.Errorf("invoke ingestion control Lambda: %w", err)
	}
	if result.FunctionError != nil && *result.FunctionError != "" {
		return fmt.Errorf("ingestion control Lambda failed: %s", *result.FunctionError)
	}
	if output == nil || len(result.Payload) == 0 {
		return nil
	}
	if err := json.Unmarshal(result.Payload, output); err != nil {
		return fmt.Errorf("decode ingestion control response: %w", err)
	}
	return nil
}
