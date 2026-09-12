package ingestion_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"backtest_engine/engine/ingestion"
)

func TestMassiveFetchUnitNormalizesPagesAndQuarantinesRows(t *testing.T) {
	var requests int
	nextURL := ""
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.URL.Query().Get("apiKey") != "secret" {
			t.Fatalf("api key missing from request")
		}
		if r.URL.Query().Get("adjusted") != "false" || r.URL.Query().Get("sort") != "asc" || r.URL.Query().Get("limit") != "50000" {
			t.Fatalf("unexpected request options: %s", r.URL.RawQuery)
		}
		w.Header().Set("Content-Type", "application/json")
		if requests == 1 {
			_, _ = w.Write([]byte(`{"results":[{"t":1704115800000,"o":10,"h":11,"l":9,"c":10.5,"v":100},{"t":1704115860000,"o":10,"h":9,"l":9.5,"c":10,"v":2}],"next_url":"` + nextURL + `"}`))
			return
		}
		_, _ = w.Write([]byte(`{"results":[{"t":1704115920000,"o":10.5,"h":11,"l":10,"c":10.8,"v":3}]}`))
	}))
	defer server.Close()

	// The handler is intentionally kept small so the test exercises pagination
	// and the same-origin check rather than a fixture file.
	nextURL = server.URL + "/next"
	client := ingestion.NewMassiveClient("secret", server.Client())
	client.BaseURL = server.URL
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	result, err := client.FetchUnit(context.Background(), ingestion.FetchRequest{
		Symbol: "AAL", StartDate: start, EndDate: start, Adjusted: false,
	})
	if err != nil {
		t.Fatalf("FetchUnit() error = %v", err)
	}
	if len(result.Bars) != 2 || len(result.Issues) != 1 || result.Pages != 2 || !result.Complete {
		t.Fatalf("unexpected result: %+v", result)
	}
	if result.CheckedEmpty || result.Bars[0].IntervalStart.UnixMilli() != 1704115800000 {
		t.Fatalf("unexpected normalized bars: %+v", result.Bars)
	}
	if result.Issues[0].Column != "ohlc" {
		t.Fatalf("unexpected issue: %+v", result.Issues[0])
	}
	if strings.Contains(result.Issues[0].SourcePage, "secret") {
		t.Fatal("source page leaked API key")
	}
}

func TestMassiveFetchUnitRejectsFractionalVolumeAndWrongDate(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"results":[{"t":1704115800123,"o":10,"h":11,"l":9,"c":10.5,"v":1.5},{"t":1704202200000,"o":10,"h":11,"l":9,"c":10.5,"v":1}]}`))
	}))
	defer server.Close()
	client := ingestion.NewMassiveClient("secret", server.Client())
	client.BaseURL = server.URL
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	result, err := client.FetchUnit(context.Background(), ingestion.FetchRequest{Symbol: "AAL", StartDate: start, EndDate: start})
	if err != nil {
		t.Fatalf("FetchUnit() error = %v", err)
	}
	if len(result.Bars) != 0 || len(result.Issues) != 2 {
		t.Fatalf("expected two quarantined rows, got %+v", result)
	}
	joined := ""
	for _, issue := range result.Issues {
		joined += issue.Reason + "\n"
	}
	if !strings.Contains(joined, "volume") || !strings.Contains(joined, "outside requested dates") {
		t.Fatalf("unexpected reasons: %s", joined)
	}
}

func TestMassiveFetchPagePersistsOnlyCredentialFreeCursor(t *testing.T) {
	nextURL := ""
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("apiKey") != "secret" {
			t.Fatal("page request did not include the API key")
		}
		_, _ = w.Write([]byte(`{"results":[{"t":1704115800000,"o":10,"h":11,"l":9,"c":10.5,"v":100}],"next_url":"` + nextURL + `"}`))
	}))
	defer server.Close()
	nextURL = server.URL + "/next?apiKey=should-not-persist"
	client := ingestion.NewMassiveClient("secret", server.Client())
	client.BaseURL = server.URL
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	result, err := client.FetchPage(context.Background(), ingestion.PageRequest{
		FetchRequest: ingestion.FetchRequest{Symbol: "AAL", StartDate: start, EndDate: start}, PageNumber: 1,
	})
	if err != nil {
		t.Fatalf("FetchPage() error = %v", err)
	}
	if len(result.Bars) != 1 || result.NextCursor == "" || strings.Contains(result.NextCursor, "apiKey") {
		t.Fatalf("unexpected page result: %+v", result)
	}
}

func TestServicePreviewValidatesAndReportsSetup(t *testing.T) {
	service := ingestion.NewConfiguredService(ingestion.Config{Region: "us-east-1"}, nil)
	status := service.Status()
	if status.Configured || status.Mode != "setup-required" {
		t.Fatalf("unexpected status: %+v", status)
	}
	preview, err := service.Preview(context.Background(), ingestion.PreviewRequest{Symbols: []string{"AAL", "MSFT"}, Start: "2024-01-01", End: "2024-01-02"})
	if err != nil || preview.Units != 4 || preview.Adjustment != "unadjusted" {
		t.Fatalf("unexpected preview: %+v, %v", preview, err)
	}
	if _, err := service.Preview(context.Background(), ingestion.PreviewRequest{Symbols: []string{"bad symbol"}, Start: "2024-01-01", End: "2024-01-02"}); err == nil {
		t.Fatal("invalid symbol should fail")
	}
}

func TestProviderErrorRetryClassification(t *testing.T) {
	for _, tc := range []struct {
		kind  string
		retry bool
	}{
		{kind: "throttle", retry: true}, {kind: "server", retry: true},
		{kind: "transport", retry: true}, {kind: "authentication", retry: false},
	} {
		err := &ingestion.ProviderError{Kind: tc.kind, Message: "test"}
		if err.Retryable() != tc.retry {
			t.Fatalf("kind %q retryable = %v, want %v", tc.kind, err.Retryable(), tc.retry)
		}
	}
}

func TestMassiveFetchUnitRejectsAdjustedRequests(t *testing.T) {
	client := ingestion.NewMassiveClient("secret", nil)
	_, err := client.FetchUnit(context.Background(), ingestion.FetchRequest{
		Symbol: "AAL", StartDate: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		EndDate: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), Adjusted: true,
	})
	if err == nil || !strings.Contains(err.Error(), "unadjusted") {
		t.Fatalf("expected adjusted request rejection, got %v", err)
	}
}
