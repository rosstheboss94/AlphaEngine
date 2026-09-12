# Massive ingestion deployment

This stack deploys the two Go Lambdas used by the AlphaEngine Data page:

- `control` validates commands, stores jobs and symbol lists in DynamoDB, and
  publishes one durable SQS message per symbol/date unit.
- `worker` reads `MASSIVE_API_KEY` from Secrets Manager, fetches one complete
  Massive unit, and writes Snappy Parquet or gzip JSON Lines quarantine objects
  to the existing S3 bucket.

The stack also creates the DynamoDB table, SQS unit queue and New York-time 06:00
EventBridge Scheduler trigger. The existing `AlphaEngineServiceRole` is used by
both Lambdas. A separate scheduler role is created because EventBridge Scheduler
and Lambda require different trust policies.

From the repository root, put the provider key in the existing untracked `.env`
file and run:

```powershell
./iac/massive-ingestion/deploy.ps1
```

The script reads `MASSIVE_API_KEY` only to create or rotate the named Secrets
Manager secret. It does not put the key in Lambda environment variables or the
CloudFormation template. It uploads versioned Lambda zip files under the
`_deploy/ingestion/` prefix in the supplied bucket.

After deployment, set the control Lambda ARN from the stack outputs before
starting the desktop app:

```powershell
$env:BACKTEST_AWS_REGION = "us-east-1"
$env:BACKTEST_INGESTION_CONTROL_ARN = "arn:aws:lambda:...:function:alphaengine-ingestion-control"
wails dev
```

The AWS identity running the script needs access to the bucket, IAM role,
Secrets Manager, CloudFormation, Lambda, DynamoDB, SQS and Scheduler. The
existing role must trust `lambda.amazonaws.com`; the deployment also attaches
the least-privilege ingestion policy used by the handlers.

This stack does not alter the existing bucket policy, versioning, or encryption
settings. Keep the bucket private and encrypted before publishing market data.
