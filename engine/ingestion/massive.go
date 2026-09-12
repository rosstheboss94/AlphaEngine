package ingestion

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const defaultMassiveBaseURL = "https://api.massive.com"

type FetchRequest struct {
	Symbol    string
	StartDate time.Time
	EndDate   time.Time
	Adjusted  bool
}

// PageRequest identifies one provider page. Cursor is an opaque absolute
// provider URL returned by a previous page and Previous is the last accepted
// timestamp from the prior page in the same unit.
type PageRequest struct {
	FetchRequest
	Cursor     string
	Previous   time.Time
	PageNumber int
}

// PageResult is one complete provider response page. The cursor is safe to
// persist because provider credentials have been removed.
type PageResult struct {
	Bars         []NormalizedBar
	Issues       []RowIssue
	NextCursor   string
	Complete     bool
	CheckedEmpty bool
}

type NormalizedBar struct {
	Provider      string    `json:"provider"`
	Symbol        string    `json:"symbol"`
	IntervalStart time.Time `json:"ts_event"`
	Open          float64   `json:"open"`
	High          float64   `json:"high"`
	Low           float64   `json:"low"`
	Close         float64   `json:"close"`
	Volume        uint64    `json:"volume"`
	SourcePage    string    `json:"source_page"`
	SourceRow     int64     `json:"source_row"`
}

type RowIssue struct {
	SourcePage string `json:"source_page"`
	SourceRow  int64  `json:"source_row"`
	Column     string `json:"column,omitempty"`
	Reason     string `json:"reason"`
}

type UnitResult struct {
	Bars         []NormalizedBar `json:"bars"`
	Issues       []RowIssue      `json:"issues"`
	Pages        int             `json:"pages"`
	Complete     bool            `json:"complete"`
	CheckedEmpty bool            `json:"checked_empty"`
}

type ProviderError struct {
	Kind       string
	StatusCode int
	RetryAfter time.Duration
	Message    string
}

func (e *ProviderError) Error() string {
	if e.StatusCode == 0 {
		return e.Message
	}
	return fmt.Sprintf("massive %s (%d): %s", e.Kind, e.StatusCode, e.Message)
}

func (e *ProviderError) Retryable() bool {
	return e.Kind == "transport" || e.Kind == "throttle" || e.Kind == "server"
}

type MassiveClient struct {
	APIKey     string
	BaseURL    string
	HTTPClient *http.Client
}

func NewMassiveClient(apiKey string, client *http.Client) *MassiveClient {
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	return &MassiveClient{APIKey: strings.TrimSpace(apiKey), BaseURL: defaultMassiveBaseURL, HTTPClient: client}
}

func (c *MassiveClient) FetchUnit(ctx context.Context, request FetchRequest) (UnitResult, error) {
	request.Symbol = strings.ToUpper(strings.TrimSpace(request.Symbol))
	if c == nil || strings.TrimSpace(c.APIKey) == "" {
		return UnitResult{}, &ProviderError{Kind: "authentication", Message: "Massive API key is not configured"}
	}
	if err := validateFetchRequest(request); err != nil {
		return UnitResult{}, err
	}
	base := strings.TrimRight(c.BaseURL, "/")
	if base == "" {
		base = defaultMassiveBaseURL
	}
	path := fmt.Sprintf("%s/v2/aggs/ticker/%s/range/1/minute/%s/%s", base,
		url.PathEscape(request.Symbol), request.StartDate.Format("2006-01-02"), request.EndDate.Format("2006-01-02"))
	pageURL, err := c.withAPIKey(path)
	if err != nil {
		return UnitResult{}, err
	}
	var result UnitResult
	var previous time.Time
	seenPages := make(map[string]struct{})
	for pageURL != "" {
		if _, seen := seenPages[pageURL]; seen {
			return UnitResult{}, &ProviderError{Kind: "schema", Message: "pagination cursor repeated"}
		}
		seenPages[pageURL] = struct{}{}
		result.Pages++
		page, next, err := c.fetchPage(ctx, pageURL, request, result.Pages, previous)
		if err != nil {
			return UnitResult{}, err
		}
		result.Bars = append(result.Bars, page.Bars...)
		result.Issues = append(result.Issues, page.Issues...)
		if len(page.Bars) != 0 {
			previous = page.Bars[len(page.Bars)-1].IntervalStart
		}
		if next == "" {
			break
		}
		pageURL, err = c.withAPIKey(next)
		if err != nil {
			return UnitResult{}, err
		}
	}
	result.Complete = true
	result.CheckedEmpty = len(result.Bars) == 0 && len(result.Issues) == 0
	return result, nil
}

// FetchPage fetches exactly one Massive response page. It performs no hidden
// retries, making it suitable for a worker that checkpoints pagination in S3
// and DynamoDB between Lambda invocations.
func (c *MassiveClient) FetchPage(ctx context.Context, request PageRequest) (PageResult, error) {
	request.Symbol = strings.ToUpper(strings.TrimSpace(request.Symbol))
	if c == nil || strings.TrimSpace(c.APIKey) == "" {
		return PageResult{}, &ProviderError{Kind: "authentication", Message: "Massive API key is not configured"}
	}
	if err := validateFetchRequest(request.FetchRequest); err != nil {
		return PageResult{}, err
	}
	pageNumber := request.PageNumber
	if pageNumber < 1 {
		pageNumber = 1
	}
	pageURL := strings.TrimSpace(request.Cursor)
	if pageURL == "" {
		base := strings.TrimRight(c.BaseURL, "/")
		if base == "" {
			base = defaultMassiveBaseURL
		}
		path := fmt.Sprintf("%s/v2/aggs/ticker/%s/range/1/minute/%s/%s", base,
			url.PathEscape(request.Symbol), request.StartDate.Format("2006-01-02"), request.EndDate.Format("2006-01-02"))
		pageURL = path
	}
	pageURL, err := c.withAPIKey(pageURL)
	if err != nil {
		return PageResult{}, err
	}
	page, next, err := c.fetchPage(ctx, pageURL, request.FetchRequest, pageNumber, request.Previous)
	if err != nil {
		return PageResult{}, err
	}
	return PageResult{
		Bars: page.Bars, Issues: page.Issues, NextCursor: safeSourcePage(next),
		Complete: next == "", CheckedEmpty: next == "" && len(page.Bars) == 0 && len(page.Issues) == 0,
	}, nil
}

type pageResult struct {
	Bars   []NormalizedBar
	Issues []RowIssue
}

func (c *MassiveClient) fetchPage(ctx context.Context, pageURL string, request FetchRequest, pageNumber int, previous time.Time) (pageResult, string, error) {
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodGet, pageURL, nil)
	if err != nil {
		return pageResult{}, "", &ProviderError{Kind: "transport", Message: "build Massive request: " + err.Error()}
	}
	response, err := c.HTTPClient.Do(httpRequest)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return pageResult{}, "", err
		}
		return pageResult{}, "", &ProviderError{Kind: "transport", Message: err.Error()}
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		kind := "provider"
		switch {
		case response.StatusCode == http.StatusUnauthorized:
			kind = "authentication"
		case response.StatusCode == http.StatusForbidden:
			kind = "entitlement"
		case response.StatusCode == http.StatusTooManyRequests:
			kind = "throttle"
		case response.StatusCode >= 500:
			kind = "server"
		}
		return pageResult{}, "", &ProviderError{Kind: kind, StatusCode: response.StatusCode, Message: strings.TrimSpace(string(body)), RetryAfter: retryAfter(response.Header.Get("Retry-After"))}
	}
	var payload struct {
		Results []json.RawMessage `json:"results"`
		NextURL string            `json:"next_url"`
	}
	decoder := json.NewDecoder(response.Body)
	decoder.UseNumber()
	if err := decoder.Decode(&payload); err != nil {
		return pageResult{}, "", &ProviderError{Kind: "schema", StatusCode: response.StatusCode, Message: "decode response: " + err.Error()}
	}
	if payload.Results == nil {
		return pageResult{}, "", &ProviderError{Kind: "schema", StatusCode: response.StatusCode, Message: "response is missing results"}
	}
	result := pageResult{Bars: make([]NormalizedBar, 0, len(payload.Results)), Issues: make([]RowIssue, 0)}
	sourcePage := safeSourcePage(pageURL)
	for index, raw := range payload.Results {
		bar, issue := normalizeBar(raw, request, sourcePage, int64(index), previous)
		if issue != nil {
			result.Issues = append(result.Issues, *issue)
			continue
		}
		result.Bars = append(result.Bars, bar)
		previous = bar.IntervalStart
	}
	return result, payload.NextURL, nil
}

func normalizeBar(raw json.RawMessage, request FetchRequest, page string, row int64, previous time.Time) (NormalizedBar, *RowIssue) {
	issue := func(column, reason string) *RowIssue {
		return &RowIssue{SourcePage: page, SourceRow: row, Column: column, Reason: reason}
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return NormalizedBar{}, issue("row", "row is not an object")
	}
	read := func(name string) (json.Number, bool) {
		value, ok := fields[name]
		if !ok || string(value) == "null" {
			return "", false
		}
		var number json.Number
		if err := json.Unmarshal(value, &number); err != nil {
			return "", false
		}
		return number, true
	}
	open, ok := read("o")
	if !ok {
		return NormalizedBar{}, issue("open", "missing or non-numeric value")
	}
	high, ok := read("h")
	if !ok {
		return NormalizedBar{}, issue("high", "missing or non-numeric value")
	}
	low, ok := read("l")
	if !ok {
		return NormalizedBar{}, issue("low", "missing or non-numeric value")
	}
	closePrice, ok := read("c")
	if !ok {
		return NormalizedBar{}, issue("close", "missing or non-numeric value")
	}
	volume, ok := read("v")
	if !ok {
		return NormalizedBar{}, issue("volume", "missing or non-numeric value")
	}
	timestamp, ok := read("t")
	if !ok {
		return NormalizedBar{}, issue("t", "missing or non-numeric value")
	}
	parsedOpen, err := parsePositiveFloat(open)
	if err != nil {
		return NormalizedBar{}, issue("open", err.Error())
	}
	parsedHigh, err := parsePositiveFloat(high)
	if err != nil {
		return NormalizedBar{}, issue("high", err.Error())
	}
	parsedLow, err := parsePositiveFloat(low)
	if err != nil {
		return NormalizedBar{}, issue("low", err.Error())
	}
	parsedClose, err := parsePositiveFloat(closePrice)
	if err != nil {
		return NormalizedBar{}, issue("close", err.Error())
	}
	parsedVolume, err := parseUint64(volume)
	if err != nil {
		return NormalizedBar{}, issue("volume", err.Error())
	}
	ms, err := parseInt64(timestamp)
	if err != nil {
		return NormalizedBar{}, issue("t", err.Error())
	}
	ts := time.UnixMilli(ms).UTC()
	if ts.UnixNano()%int64(time.Minute) != 0 {
		return NormalizedBar{}, issue("t", "timestamp is not minute-aligned")
	}
	location, _ := time.LoadLocation("America/New_York")
	local := ts.In(location)
	startDate := request.StartDate.Format("2006-01-02")
	endDate := request.EndDate.Format("2006-01-02")
	if local.Format("2006-01-02") < startDate || local.Format("2006-01-02") > endDate {
		return NormalizedBar{}, issue("t", "timestamp is outside requested dates")
	}
	if !previous.IsZero() && !ts.After(previous) {
		return NormalizedBar{}, issue("t", "timestamps must be strictly increasing")
	}
	if parsedLow > parsedOpen || parsedLow > parsedClose || parsedHigh < parsedOpen || parsedHigh < parsedClose || parsedLow > parsedHigh {
		return NormalizedBar{}, issue("ohlc", "low/high bounds are inconsistent")
	}
	return NormalizedBar{Provider: "Massive", Symbol: request.Symbol, IntervalStart: ts, Open: parsedOpen, High: parsedHigh, Low: parsedLow, Close: parsedClose, Volume: parsedVolume, SourcePage: page, SourceRow: row}, nil
}

func parsePositiveFloat(value json.Number) (float64, error) {
	parsed, err := strconv.ParseFloat(value.String(), 64)
	if err != nil || math.IsNaN(parsed) || math.IsInf(parsed, 0) || parsed <= 0 {
		return 0, errors.New("price must be finite and positive")
	}
	return parsed, nil
}

func parseUint64(value json.Number) (uint64, error) {
	rational, ok := new(big.Rat).SetString(value.String())
	if !ok || !rational.IsInt() || rational.Sign() < 0 || !rational.Num().IsUint64() {
		return 0, errors.New("volume must be a nonnegative uint64")
	}
	return rational.Num().Uint64(), nil
}

func parseInt64(value json.Number) (int64, error) {
	rational, ok := new(big.Rat).SetString(value.String())
	if !ok || !rational.IsInt() || !rational.Num().IsInt64() {
		return 0, errors.New("timestamp must be an integer millisecond")
	}
	return rational.Num().Int64(), nil
}

func (c *MassiveClient) withAPIKey(raw string) (string, error) {
	parsed, err := url.Parse(raw)
	if err != nil {
		return "", &ProviderError{Kind: "schema", Message: "invalid pagination URL"}
	}
	base, err := url.Parse(strings.TrimRight(c.BaseURL, "/"))
	if err != nil {
		return "", err
	}
	if parsed.Host != "" && base.Host != "" && !strings.EqualFold(parsed.Host, base.Host) {
		return "", &ProviderError{Kind: "schema", Message: "pagination URL changed provider host"}
	}
	if parsed.Host == "" {
		return "", &ProviderError{Kind: "schema", Message: "pagination URL is not absolute"}
	}
	query := parsed.Query()
	query.Set("apiKey", c.APIKey)
	query.Set("adjusted", "false")
	query.Set("sort", "asc")
	query.Set("limit", "50000")
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

func safeSourcePage(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil {
		return "massive"
	}
	query := parsed.Query()
	query.Del("apiKey")
	query.Del("api_key")
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

func retryAfter(value string) time.Duration {
	seconds, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil || seconds < 0 {
		return 0
	}
	return time.Duration(seconds) * time.Second
}

func validateFetchRequest(request FetchRequest) error {
	if !symbolPattern.MatchString(strings.ToUpper(strings.TrimSpace(request.Symbol))) {
		return errors.New("invalid Massive symbol")
	}
	if request.StartDate.IsZero() || request.EndDate.IsZero() || request.EndDate.Before(request.StartDate) {
		return errors.New("invalid fetch date range")
	}
	if request.Adjusted {
		return errors.New("Massive ingestion only supports unadjusted bars")
	}
	return nil
}
