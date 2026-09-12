package cloudingestion

import (
	"bytes"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	"backtest_engine/engine/ingestion"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/parquet-go/parquet-go"
)

func TestConfigFromEnvUsesSafeDefaultsAndRuntimeInputs(t *testing.T) {
	values := map[string]string{
		"INGESTION_S3_BUCKET":           "alphaengine-bucket",
		"INGESTION_TABLE_NAME":          "ingestion",
		"INGESTION_QUEUE_URL":           "https://sqs.us-east-1.amazonaws.com/queue.fifo",
		"INGESTION_SCHEDULED_QUEUE_URL": "https://sqs.us-east-1.amazonaws.com/scheduled",
		"INGESTION_WORKER_FUNCTION":     "worker",
		"INGESTION_STATE_MACHINE_ARN":   "arn:aws:states:us-east-1:123:stateMachine:ingestion",
		"MASSIVE_SECRET_ARN":            "arn:aws:secretsmanager:us-east-1:123:secret:massive",
		"INGESTION_MAX_UNITS":           "42",
	}
	cfg, err := ConfigFromEnv(func(key string) string { return values[key] })
	if err != nil {
		t.Fatalf("ConfigFromEnv() error = %v", err)
	}
	if cfg.Region != defaultAWSRegion || cfg.MaxUnits != 42 || cfg.MassiveSecretARN == "" || cfg.StateMachineARN == "" {
		t.Fatalf("unexpected config: %+v", cfg)
	}
	if err := (Config{Bucket: "bucket", TableName: "table", QueueURL: "queue", ScheduledQueueURL: "scheduled", WorkerFunction: "worker", StateMachineARN: "state-machine"}).validateControl(); err != nil {
		t.Fatalf("validateControl() error = %v", err)
	}
	if err := (Config{Bucket: "bucket", TableName: "table", QueueURL: "queue", MassiveSecretARN: "secret"}).validateWorker(); err != nil {
		t.Fatalf("validateWorker() error = %v", err)
	}
	if err := (Config{Bucket: "bucket", TableName: "table", MassiveSecretARN: "secret"}).validateWorker(); err == nil {
		t.Fatal("worker queue URL should be required")
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

func TestExecutionObjectKeysAreIsolatedByJob(t *testing.T) {
	first := publishedObjectKey("job-a", "AAL", "2024-01-02", "lease-a")
	second := publishedObjectKey("job-b", "AAL", "2024-01-02", "lease-b")
	if first == second || !strings.Contains(first, "execution=job-a") || !strings.Contains(first, "lease=lease-a") || !strings.Contains(second, "execution=job-b") || first == publishedObjectKey("job-a", "AAL", "2024-01-02", "lease-b") {
		t.Fatalf("execution keys are not isolated: %q, %q", first, second)
	}
	if !strings.Contains(stagePageKey("job-a", "AAL", "2024-01-02", 3, "lease-a"), "page=000003") {
		t.Fatal("staged page key should include a zero-padded page number")
	}
}

func TestUnitItemRoundTripsDurablePageAndLeaseState(t *testing.T) {
	want := unitState{
		JobID: "job-a", Symbol: "AAL", Date: "2024-01-02", Status: "waiting", Attempts: 2,
		Rows: 10, InvalidRows: 1, Cursor: "https://api.massive.com/next", PageNumber: 3,
		StageKeys: []string{"staging/page-1.json.gz", "staging/page-2.json.gz"}, NotBefore: 1704202300000,
		QueueURL:   "https://sqs.us-east-1.amazonaws.com/queue",
		PreviousTS: 1704202200000000000, LeaseToken: "lease-a", LeaseExpiresAt: 1704202400000,
		ExpectedGeneration: "generation-a",
	}
	got := unitFromItem(unitItem(want))
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unit state round trip = %+v, want %+v", got, want)
	}
}

func TestRetryDelayUsesBoundedExponentialBackoff(t *testing.T) {
	if retryDelay(1) != 15*time.Second || retryDelay(2) != 30*time.Second || retryDelay(5) != 4*time.Minute || retryDelay(7) != 15*time.Minute {
		t.Fatalf("unexpected retry delays: %s, %s, %s, %s", retryDelay(1), retryDelay(2), retryDelay(5), retryDelay(7))
	}
}

func TestPageContinuationDoesNotConsumeUnitRetryBudget(t *testing.T) {
	if claimAttempt(unitState{}) != 1 {
		t.Fatal("initial unit claim should count as the first attempt")
	}
	if claimAttempt(unitState{Attempts: 1, Cursor: "https://api.massive.com/next"}) != 1 {
		t.Fatal("successful page continuation should retain the attempt count")
	}
	if claimAttempt(unitState{Attempts: 1, NotBefore: 1704202300000}) != 2 {
		t.Fatal("a delayed retry should consume the next attempt")
	}
}

func TestRandomIDsDoNotReuseExecutionIdentity(t *testing.T) {
	first, err := randomID("request")
	if err != nil {
		t.Fatalf("randomID() error = %v", err)
	}
	second, err := randomID("request")
	if err != nil {
		t.Fatalf("randomID() error = %v", err)
	}
	if first == second || !strings.HasPrefix(first, "request-") || !strings.HasPrefix(second, "request-") {
		t.Fatalf("unexpected random IDs: %q, %q", first, second)
	}
}
