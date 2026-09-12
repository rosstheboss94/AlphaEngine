package ingestion

import (
	"context"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"
)

var symbolPattern = regexp.MustCompile(`^[A-Z][A-Z0-9.-]{0,11}$`)

type Service struct {
	config  Config
	control ControlInvoker
}

func NewService() *Service {
	cfg := ConfigFromEnv(func(key string) string { return strings.TrimSpace(os.Getenv(key)) })
	var control ControlInvoker
	if cfg.ControlARN != "" {
		control = NewLambdaControl(cfg)
	}
	return &Service{config: cfg, control: control}
}

func NewConfiguredService(cfg Config, control ControlInvoker) *Service {
	return &Service{config: cfg, control: control}
}

func (s *Service) Status() Status {
	if s.control == nil {
		return Status{Provider: "Massive", Mode: "setup-required", Configured: false, Region: s.config.Region, Message: "Configure BACKTEST_INGESTION_CONTROL_ARN and AWS credentials to connect."}
	}
	return Status{Provider: "Massive", Mode: "aws-control-lambda", Configured: true, Region: s.config.Region, Message: "AWS control Lambda configured. Provider credentials remain in Secrets Manager."}
}

func (s *Service) Lists(ctx context.Context) ([]SymbolList, error) {
	var result []SymbolList
	if err := s.invoke(ctx, "list_symbol_lists", nil, &result); err != nil {
		return nil, err
	}
	return result, nil
}

func (s *Service) SaveList(ctx context.Context, list SymbolList) (SymbolList, error) {
	if err := validateList(list); err != nil {
		return SymbolList{}, err
	}
	var result SymbolList
	if err := s.invoke(ctx, "save_symbol_list", list, &result); err != nil {
		return SymbolList{}, err
	}
	return result, nil
}

func (s *Service) DeleteList(ctx context.Context, id string) error {
	if strings.TrimSpace(id) == "" {
		return errors.New("symbol list id is required")
	}
	return s.invoke(ctx, "delete_symbol_list", map[string]string{"id": id}, nil)
}

func (s *Service) RetryJob(ctx context.Context, id string) error {
	if strings.TrimSpace(id) == "" {
		return errors.New("job id is required")
	}
	return s.invoke(ctx, "retry_job", map[string]string{"id": id}, nil)
}

func (s *Service) SetSchedule(ctx context.Context, request ScheduleRequest) error {
	if strings.TrimSpace(request.ListID) == "" && len(request.Symbols) == 0 {
		return errors.New("saved symbol list or symbols are required")
	}
	if len(request.Symbols) > 0 {
		if err := validateSymbols(request.Symbols); err != nil {
			return err
		}
	}
	return s.invoke(ctx, "set_schedule", request, nil)
}

func (s *Service) ReviewDiscrepancy(ctx context.Context, request ReviewRequest) error {
	if strings.TrimSpace(request.CandidateID) == "" {
		return errors.New("candidate id is required")
	}
	if !request.AcceptRemovals {
		return errors.New("explicit removal approval is required")
	}
	return s.invoke(ctx, "review_discrepancy", request, nil)
}

func (s *Service) Preview(ctx context.Context, request PreviewRequest) (Preview, error) {
	if err := validateRange(request.Symbols, request.Start, request.End); err != nil {
		return Preview{}, err
	}
	start, _ := parseDate(request.Start)
	end, _ := parseDate(request.End)
	days := int(end.Sub(start).Hours()/24) + 1
	preview := Preview{
		Symbols: append([]string(nil), request.Symbols...), Start: request.Start, End: request.End,
		Units: len(request.Symbols) * days, Provider: "Massive", Resolution: "1 minute",
		Adjustment: "unadjusted", LimitMessage: "Provider access and empty sessions are checked by the job.",
	}
	if s.control == nil {
		return preview, nil
	}
	var result Preview
	if err := s.invoke(ctx, "preview_backfill", request, &result); err != nil {
		return Preview{}, err
	}
	return result, nil
}

func (s *Service) StartBackfill(ctx context.Context, request BackfillRequest) (Job, error) {
	if len(request.Symbols) > 0 {
		if err := validateRange(request.Symbols, request.Start, request.End); err != nil {
			return Job{}, err
		}
	} else if err := validateDateRange(request.Start, request.End); err != nil {
		return Job{}, err
	}
	if request.ListID == "" && len(request.Symbols) == 0 {
		return Job{}, errors.New("saved symbol list or symbols are required")
	}
	var result Job
	if err := s.invoke(ctx, "start_backfill", request, &result); err != nil {
		return Job{}, err
	}
	return result, nil
}

func (s *Service) Jobs(ctx context.Context) ([]Job, error) {
	var result []Job
	if err := s.invoke(ctx, "list_jobs", nil, &result); err != nil {
		return nil, err
	}
	return result, nil
}

func (s *Service) CancelJob(ctx context.Context, id string) error {
	if strings.TrimSpace(id) == "" {
		return errors.New("job id is required")
	}
	return s.invoke(ctx, "cancel_job", map[string]string{"id": id}, nil)
}

func (s *Service) DeleteData(ctx context.Context, request DeleteRequest) (DeleteResult, error) {
	if err := validateRange(request.Symbols, request.Start, request.End); err != nil {
		return DeleteResult{}, err
	}
	var result DeleteResult
	if err := s.invoke(ctx, "delete_data", request, &result); err != nil {
		return DeleteResult{}, err
	}
	return result, nil
}

func (s *Service) invoke(ctx context.Context, action string, input any, output any) error {
	if s.control == nil {
		return ErrNotConfigured
	}
	return s.control.Invoke(ctx, action, input, output)
}

func validateList(list SymbolList) error {
	if strings.TrimSpace(list.Name) == "" {
		return errors.New("symbol list name is required")
	}
	if len(list.Symbols) == 0 || len(list.Symbols) > 100 {
		return errors.New("symbol lists must contain between 1 and 100 symbols")
	}
	return validateSymbols(list.Symbols)
}

func validateSymbols(symbols []string) error {
	seen := make(map[string]struct{}, len(symbols))
	for i, symbol := range symbols {
		normalized := strings.ToUpper(strings.TrimSpace(symbol))
		if !symbolPattern.MatchString(normalized) {
			return fmt.Errorf("symbol %d is invalid", i+1)
		}
		if _, ok := seen[normalized]; ok {
			return fmt.Errorf("symbol %q is duplicated", normalized)
		}
		seen[normalized] = struct{}{}
	}
	return nil
}

// ValidateSymbols checks the symbol rules shared by the desktop and cloud
// ingestion boundaries.
func ValidateSymbols(symbols []string) error {
	return validateSymbols(symbols)
}

// NormalizeSymbols returns the canonical uppercase representation while
// preserving the caller's order.
func NormalizeSymbols(symbols []string) []string {
	normalized := make([]string, 0, len(symbols))
	for _, symbol := range symbols {
		normalized = append(normalized, strings.ToUpper(strings.TrimSpace(symbol)))
	}
	return normalized
}

func validateRange(symbols []string, start, end string) error {
	if len(symbols) == 0 || len(symbols) > 100 {
		return errors.New("select between 1 and 100 symbols")
	}
	if err := validateSymbols(symbols); err != nil {
		return err
	}
	return validateDateRange(start, end)
}

func validateDateRange(start, end string) error {
	startDate, err := parseDate(start)
	if err != nil {
		return errors.New("start must be YYYY-MM-DD")
	}
	endDate, err := parseDate(end)
	if err != nil {
		return errors.New("end must be YYYY-MM-DD")
	}
	if endDate.Before(startDate) {
		return errors.New("end must be on or after start")
	}
	return nil
}

// ValidateDateRange checks an inclusive YYYY-MM-DD range.
func ValidateDateRange(start, end string) error {
	return validateDateRange(start, end)
}

func parseDate(value string) (time.Time, error) {
	return time.ParseInLocation("2006-01-02", value, time.UTC)
}
