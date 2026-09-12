package main

import (
	"context"
	"log"

	cloud "backtest_engine/engine/ingestion/cloud"
	"github.com/aws/aws-lambda-go/lambda"
)

func main() {
	handler, err := cloud.NewControlHandler(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	lambda.Start(handler.Handle)
}
