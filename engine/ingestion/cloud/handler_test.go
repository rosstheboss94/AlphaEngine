package cloudingestion

import (
	"bytes"
	"io"
	"strings"
	"testing"
	"time"

	"backtest_engine/engine/ingestion"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/parquet-go/parquet-go"
)

func TestConfigFromEnvUsesSafeDefaultsAndRuntimeInputs(t *testing.T) {
	values := map[string]string{
		"INGESTION_S3_BUCKET":       "alphaengine-bucket",
		"INGESTION_TABLE_NAME":      "ingestion",
		"INGESTION_QUEUE_URL":       "https://sqs.us-east-1.amazonaws.com/queue.fifo",
		"INGESTION_WORKER_FUNCTION": "worker",
		"MASSIVE_SECRET_ARN":        "arn:aws:secretsmanager:us-east-1:123:secret:massive",
		"INGESTION_MAX_UNITS":       "42",
	}
	cfg, err := ConfigFromEnv(func(key string) string { return values[key] })
	if err != nil {
		t.Fatalf("ConfigFromEnv() error = %v", err)
	}
	if cfg.Region != defaultAWSRegion || cfg.MaxUnits != 42 || cfg.MassiveSecretARN == "" {
		t.Fatalf("unexpected config: %+v", cfg)
	}
	if err := (Config{Bucket: "bucket", TableName: "table", QueueURL: "queue", WorkerFunction: "worker"}).validateControl(); err != nil {
		t.Fatalf("validateControl() error = %v", err)
	}
}

func TestDatesAndScheduleUseInclusiveNewYorkCalendarDays(t *testing.T) {
	dates, err := datesBetween("2024-03-09", "2024-03-11")
	if err != nil {
		t.Fatalf("datesBetween() error = %v", err)
	}
	if strings.Join(dates, ",") != "2024-03-09,2024-03-10,2024-03-11" {
		t.Fatalf("unexpected date range: %v", dates)
	}
	dates = recentCompletedDates(time.Date(2024, 3, 11, 10, 0, 0, 0, time.UTC), 2)
	if strings.Join(dates, ",") != "2024-03-10,2024-03-09" {
		t.Fatalf("unexpected recent dates: %v", dates)
	}
}

func TestEncodeParquetUsesNormalizedSchemaAndSnappy(t *testing.T) {
	timestamp := time.Date(2024, 1, 2, 14, 30, 0, 0, time.UTC)
	data, err := encodeParquet([]ingestion.NormalizedBar{{
		Provider: "Massive", Symbol: "AAL", IntervalStart: timestamp,
		Open: 10, High: 11, Low: 9, Close: 10.5, Volume: 100,
	}})
	if err != nil {
		t.Fatalf("encodeParquet() error = %v", err)
	}
	reader := parquet.NewReader(bytes.NewReader(data))
	for _, column := range []string{"provider", "symbol", "open", "high", "low", "close", "volume", "ts_event"} {
		if _, ok := reader.Schema().Lookup(column); !ok {
			t.Fatalf("schema is missing %q: %s", column, reader.Schema())
		}
	}
	if !strings.Contains(strings.ToUpper(reader.File().Metadata().RowGroups[0].Columns[0].MetaData.Codec.String()), "SNAPPY") {
		t.Fatalf("expected Snappy compression, got %s", reader.File().Metadata().RowGroups[0].Columns[0].MetaData.Codec)
	}
	generic := parquet.NewGenericReader[parquetBar](bytes.NewReader(data))
	defer generic.Close()
	rows := make([]parquetBar, 1)
	count, err := generic.Read(rows)
	if err != nil && err != io.EOF {
		t.Fatalf("read encoded row error = %v", err)
	}
	if count != 1 || rows[0].Symbol == nil || *rows[0].Symbol != "AAL" || rows[0].TsEvent == nil || !rows[0].TsEvent.Equal(timestamp) {
		t.Fatalf("unexpected encoded row: %+v", rows[0])
	}
}

func TestUnitStatusDeltasAndRetryBudget(t *testing.T) {
	delta := statusDelta("queued", "completed")
	if delta["finished_units"] != 1 || delta["clean_units"] != 1 {
		t.Fatalf("unexpected completion delta: %+v", delta)
	}
	delta = statusDelta("incomplete", "waiting")
	if delta["finished_units"] != -1 || delta["incomplete_units"] != -1 {
		t.Fatalf("unexpected retry delta: %+v", delta)
	}
	state := jobState{Job: ingestion.Job{Units: 2}, FinishedUnits: 1, CleanUnits: 1}
	if got := statusForCounters(state, map[string]int64{"finished_units": 1, "clean_units": 1}); got != "completed" {
		t.Fatalf("statusForCounters() = %q, want completed", got)
	}
	if got := statusForCounters(state, map[string]int64{"finished_units": 1, "failed_units": 1}); got != "failed" {
		t.Fatalf("statusForCounters() = %q, want failed", got)
	}
}

func TestDeletionFenceOnlyAllowsNewerManualBackfill(t *testing.T) {
	item := map[string]dynamodbtypes.AttributeValue{
		"status":     stringValue("deleted"),
		"updated_at": stringValue("2024-01-02T10:00:00Z"),
	}
	if !deletionPredatesJob(item, "2024-01-02T10:00:01Z") {
		t.Fatal("a backfill created after deletion should clear the exclusion")
	}
	if deletionPredatesJob(item, "2024-01-02T09:59:59Z") {
		t.Fatal("a stale backfill should not clear the exclusion")
	}
}
