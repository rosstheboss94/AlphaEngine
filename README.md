# Backtest Engine

Backtest Engine is a Wails desktop app with a React UI and a Go backtesting
engine. The repository is organized as a small monorepo so the future ML,
infrastructure, and delivery code have clear homes.

See [PROJECT_STRUCTURE.md](PROJECT_STRUCTURE.md) for the directory map and
ownership rules.

## Run the desktop app

Run these commands from the project root:

```powershell
wails dev
```

Build the Windows executable:

```powershell
wails build
.\build\bin\backtest-desktop.exe
```

## Run the app with Docker

Docker serves the browser version of the React app. It does not launch the
native Wails window, which requires a desktop environment. From the project
root, run:

```powershell
docker compose up --build
```

Open `http://localhost:5173`. Stop the container with:

```powershell
docker compose down
```

The Docker image contains the built frontend only. Browser-only behavior, such
as CSV download, remains available; Wails native integrations are not present.

For browser-only development:

```powershell
cd app
npm ci
npm run dev
```

Open `http://127.0.0.1:5173`. Window controls are available only in Wails.
The browser and Wails app both support CSV export. Wails opens a native save
dialog; cancellation preserves the current screen.

The current UI uses illustrative sample results. Model training, Strategies,
data loading, calendar filtering, and engine calls are not connected to the
frontend yet.

The Data page provides the ingestion control surface. Browser preview can
validate and preview a range locally, but it never downloads data. The desktop
uses the Wails Go boundary and reports AWS setup status. To connect it to the
cloud control function, configure these variables before launching:

```powershell
$env:BACKTEST_AWS_REGION = "us-east-1"
$env:BACKTEST_AWS_PROFILE = "your-aws-profile"
$env:BACKTEST_INGESTION_CONTROL_ARN = "arn:aws:lambda:..."
wails dev
```

The profile must be allowed to invoke the deployed control Lambda. The Massive
key belongs in AWS Secrets Manager and is read by the cloud worker. The desktop
does not accept or log provider keys. The control Lambda receives commands with
an `action` and `input` JSON object, as defined by the ingestion contracts. The
CloudFormation template and deployment script live in
[`iac/massive-ingestion`](iac/massive-ingestion/README.md).

## Checks

```powershell
cd app
npm test
npm run build
cd ..
go test ./...
go vet ./...
```

Build the app assets before running Go checks on a fresh checkout because the
Wails entry point embeds `app/dist`. Node dependencies and generated assets are
ignored by Git.
