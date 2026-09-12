# Project structure

```text
/
├── app/       React, Vite, and Wails frontend assets
├── engine/    Go backtesting packages, CLI, and Go tests
│   ├── ingestion/       Desktop-facing AWS ingestion boundary
│   ├── internal/        Engine-owned market data, simulation, strategy, and results
│   ├── cmd/              Go command-line entry points
│   └── tests/            Engine tests and fixtures
├── ml/        Future Python machine-learning code
├── iac/       Future Terraform infrastructure
├── cicd/      Future build, test, and deployment workflows
├── specs/     Product requirements, design, decisions, and verification
├── images/    Product reference images
└── build/     Wails build configuration and local outputs
```

The root keeps the Wails bootstrap files (`main.go`, `app.go`, `go.mod`, and
`wails.json`) because Wails builds the desktop host from the Go module root.
The root host imports the public package at `engine/ingestion`; engine-only
packages remain under `engine/internal`.

The Wails project configuration points its frontend directory and generated
bindings to `app/`. The frontend keeps its feature-oriented layout under
`app/src`, while engine tests stay beside the engine code under `engine/tests`.

The `ml`, `iac`, and `cicd` directories intentionally contain no runtime code
yet. Add code there when those workstreams begin.
