// Package ingestion contains provider normalization and the desktop control
// boundary for cloud-backed market-data jobs.
package ingestion

type Status struct {
	Provider   string `json:"provider"`
	Mode       string `json:"mode"`
	Configured bool   `json:"configured"`
	Region     string `json:"region"`
	Message    string `json:"message"`
}

type SymbolList struct {
	ID        string   `json:"id"`
	Name      string   `json:"name"`
	Symbols   []string `json:"symbols"`
	UpdatedAt string   `json:"updated_at"`
}

type PreviewRequest struct {
	Symbols []string `json:"symbols"`
	Start   string   `json:"start"`
	End     string   `json:"end"`
}

type Preview struct {
	Symbols      []string `json:"symbols"`
	Start        string   `json:"start"`
	End          string   `json:"end"`
	Units        int      `json:"units"`
	Provider     string   `json:"provider"`
	Resolution   string   `json:"resolution"`
	Adjustment   string   `json:"adjustment"`
	LimitMessage string   `json:"limit_message"`
}

type BackfillRequest struct {
	ListID  string   `json:"list_id"`
	Symbols []string `json:"symbols,omitempty"`
	Start   string   `json:"start"`
	End     string   `json:"end"`
}

type Job struct {
	ID             string        `json:"id"`
	Status         string        `json:"status"`
	Provider       string        `json:"provider"`
	ListID         string        `json:"list_id"`
	Symbols        int           `json:"symbols"`
	Units          int           `json:"units"`
	CompletedUnits int           `json:"completed_units"`
	Rows           int64         `json:"rows"`
	InvalidRows    int64         `json:"invalid_rows"`
	LastAttempt    string        `json:"last_attempt,omitempty"`
	LastSuccessful string        `json:"last_successful,omitempty"`
	Error          string        `json:"error,omitempty"`
	NeedsAttention bool          `json:"needs_attention"`
	Discrepancies  []Discrepancy `json:"discrepancies,omitempty"`
}

type Discrepancy struct {
	ID                string   `json:"id"`
	Symbol            string   `json:"symbol"`
	Date              string   `json:"date"`
	OmittedTimestamps []string `json:"omitted_timestamps"`
}

type ScheduleRequest struct {
	ListID  string   `json:"list_id"`
	Symbols []string `json:"symbols,omitempty"`
	Enabled bool     `json:"enabled"`
}

type DeleteRequest struct {
	Symbols []string `json:"symbols"`
	Start   string   `json:"start"`
	End     string   `json:"end"`
}

type DeleteResult struct {
	Status       string `json:"status"`
	DeletedUnits int    `json:"deleted_units"`
	Message      string `json:"message"`
}

type ReviewRequest struct {
	CandidateID    string `json:"candidate_id"`
	AcceptRemovals bool   `json:"accept_removals"`
	Note           string `json:"note"`
}

type Config struct {
	Region     string
	Profile    string
	ControlARN string
}

func ConfigFromEnv(getenv func(string) string) Config {
	region := getenv("BACKTEST_AWS_REGION")
	if region == "" {
		region = "us-east-1"
	}
	return Config{
		Region:     region,
		Profile:    getenv("BACKTEST_AWS_PROFILE"),
		ControlARN: getenv("BACKTEST_INGESTION_CONTROL_ARN"),
	}
}
