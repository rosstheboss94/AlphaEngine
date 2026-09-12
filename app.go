package main

import (
	"context"
	"fmt"
	"os"

	"backtest_engine/engine/ingestion"
	"github.com/wailsapp/wails/v2/pkg/runtime"
)

// App exposes native export and the typed ingestion control boundary.
type App struct {
	ctx       context.Context
	ingestion *ingestion.Service
}

func (a *App) startup(ctx context.Context) {
	a.ctx = ctx
	a.ingestion = ingestion.NewService()
}

func (a *App) requestContext() context.Context {
	if a.ctx == nil {
		return context.Background()
	}
	return a.ctx
}

func (a *App) ingestionService() *ingestion.Service {
	if a.ingestion == nil {
		a.ingestion = ingestion.NewService()
	}
	return a.ingestion
}

// ExportCSV asks the user for a destination. Cancellation returns false.
func (a *App) ExportCSV(content string) (bool, error) {
	if len(content) > 1024*1024 {
		return false, fmt.Errorf("sample export exceeds 1 MiB")
	}
	path, err := runtime.SaveFileDialog(a.ctx, runtime.SaveDialogOptions{
		Title: "Export illustrative backtests", DefaultFilename: "illustrative-backtests.csv",
		Filters: []runtime.FileFilter{{DisplayName: "CSV files", Pattern: "*.csv"}},
	})
	if err != nil {
		return false, fmt.Errorf("choose export destination: %w", err)
	}
	if path == "" {
		return false, nil
	}
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		return false, fmt.Errorf("write CSV: %w", err)
	}
	return true, nil
}

func (a *App) IngestionStatus() ingestion.Status {
	return a.ingestionService().Status()
}

func (a *App) IngestionLists() ([]ingestion.SymbolList, error) {
	return a.ingestionService().Lists(a.requestContext())
}

func (a *App) IngestionSaveList(list ingestion.SymbolList) (ingestion.SymbolList, error) {
	return a.ingestionService().SaveList(a.requestContext(), list)
}

func (a *App) IngestionDeleteList(id string) error {
	return a.ingestionService().DeleteList(a.requestContext(), id)
}

func (a *App) IngestionPreview(request ingestion.PreviewRequest) (ingestion.Preview, error) {
	return a.ingestionService().Preview(a.requestContext(), request)
}

func (a *App) IngestionStartBackfill(request ingestion.BackfillRequest) (ingestion.Job, error) {
	return a.ingestionService().StartBackfill(a.requestContext(), request)
}

func (a *App) IngestionJobs() ([]ingestion.Job, error) {
	return a.ingestionService().Jobs(a.requestContext())
}

func (a *App) IngestionCancelJob(id string) error {
	return a.ingestionService().CancelJob(a.requestContext(), id)
}

func (a *App) IngestionRetryJob(id string) error {
	return a.ingestionService().RetryJob(a.requestContext(), id)
}

func (a *App) IngestionSetSchedule(request ingestion.ScheduleRequest) error {
	return a.ingestionService().SetSchedule(a.requestContext(), request)
}

func (a *App) IngestionReviewDiscrepancy(request ingestion.ReviewRequest) error {
	return a.ingestionService().ReviewDiscrepancy(a.requestContext(), request)
}

func (a *App) IngestionDeleteData(request ingestion.DeleteRequest) (ingestion.DeleteResult, error) {
	return a.ingestionService().DeleteData(a.requestContext(), request)
}
