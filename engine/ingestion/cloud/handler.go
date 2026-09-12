package cloudingestion

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"backtest_engine/engine/ingestion"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/snappy"
)

const (
	jobPartition            = "JOBS"
	listPartition           = "LISTS"
	schedulePartition       = "SCHEDULES"
	currentSortKey          = "CURRENT"
	dataPartitionBase       = "DATA#"
	unitPartitionBase       = "JOB#"
	unitSortPrefix          = "UNIT#"
	listSortPrefix          = "LIST#"
	jobSortPrefix           = "JOB#"
	scheduleSortPrefix      = "SCHEDULE#"
	providerRequestInterval = 15 * time.Second
)

// Handler owns one Lambda's AWS clients. A handler can be reused across warm
// invocations without reloading the AWS configuration.
type Handler struct {
	cfg     Config
	ddb     *dynamodb.Client
	s3      *s3.Client
	sqs     *sqs.Client
	secrets *secretsmanager.Client
	now     func() time.Time

	secretOnce sync.Once
	secret     string
	secretErr  error
}

// NewControlHandler creates the command Lambda handler.
func NewControlHandler(ctx context.Context) (*Handler, error) {
	cfg, err := ConfigFromEnv(os.Getenv)
	if err != nil {
		return nil, err
	}
	if err := cfg.validateControl(); err != nil {
		return nil, err
	}
	return newHandler(ctx, cfg)
}

// NewWorkerHandler creates the SQS worker Lambda handler.
func NewWorkerHandler(ctx context.Context) (*Handler, error) {
	cfg, err := ConfigFromEnv(os.Getenv)
	if err != nil {
		return nil, err
	}
	if err := cfg.validateWorker(); err != nil {
		return nil, err
	}
	return newHandler(ctx, cfg)
}

func newHandler(ctx context.Context, cfg Config) (*Handler, error) {
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(cfg.Region))
	if err != nil {
		return nil, fmt.Errorf("load AWS configuration: %w", err)
	}
	return &Handler{
		cfg:     cfg,
		ddb:     dynamodb.NewFromConfig(awsCfg),
		s3:      s3.NewFromConfig(awsCfg),
		sqs:     sqs.NewFromConfig(awsCfg),
		secrets: secretsmanager.NewFromConfig(awsCfg),
		now:     time.Now,
	}, nil
}

// Handle dispatches one desktop control command and returns the command's
// JSON-compatible result. The worker never receives this payload.
func (h *Handler) Handle(ctx context.Context, command Command) (any, error) {
	action := strings.TrimSpace(command.Action)
	switch action {
	case "preview_backfill":
		return h.preview(command.Input)
	case "list_symbol_lists":
		return h.listLists(ctx)
	case "save_symbol_list":
		return h.saveList(ctx, command.Input)
	case "delete_symbol_list":
		return nil, h.deleteList(ctx, command.Input)
	case "start_backfill":
		return h.startBackfill(ctx, command.Input)
	case "list_jobs":
		return h.listJobs(ctx)
	case "cancel_job":
		return nil, h.cancelJob(ctx, command.Input)
	case "retry_job":
		return h.retryJob(ctx, command.Input)
	case "set_schedule":
		return nil, h.setSchedule(ctx, command.Input)
	case "run_scheduled":
		return h.runScheduled(ctx)
	case "review_discrepancy":
		return nil, h.reviewDiscrepancy(ctx, command.Input)
	case "delete_data":
		return h.deleteData(ctx, command.Input)
	default:
		return nil, fmt.Errorf("unsupported ingestion action %q", action)
	}
}

// HandleQueue processes the SQS event used by the worker Lambda. The template
// configures a batch size of one so a returned error retries exactly one unit.
func (h *Handler) HandleQueue(ctx context.Context, event events.SQSEvent) error {
	for _, record := range event.Records {
		var message UnitMessage
		if err := json.Unmarshal([]byte(record.Body), &message); err != nil {
			return fmt.Errorf("decode queue message %q: %w", record.MessageId, err)
		}
		if err := h.handleUnit(ctx, message); err != nil {
			return err
		}
	}
	return nil
}

func (h *Handler) preview(raw json.RawMessage) (ingestion.Preview, error) {
	var request ingestion.PreviewRequest
	if err := decodeInput(raw, &request); err != nil {
		return ingestion.Preview{}, err
	}
	request.Symbols = ingestion.NormalizeSymbols(request.Symbols)
	if len(request.Symbols) == 0 || len(request.Symbols) > maxSymbols {
		return ingestion.Preview{}, errors.New("select between 1 and 100 symbols")
	}
	if err := ingestion.ValidateSymbols(request.Symbols); err != nil {
		return ingestion.Preview{}, err
	}
	if err := ingestion.ValidateDateRange(request.Start, request.End); err != nil {
		return ingestion.Preview{}, err
	}
	start, _ := parseDate(request.Start)
	end, _ := parseDate(request.End)
	days := int(end.Sub(start).Hours()/24) + 1
	return ingestion.Preview{
		Symbols:      request.Symbols,
		Start:        request.Start,
		End:          request.End,
		Units:        len(request.Symbols) * days,
		Provider:     "Massive",
		Resolution:   "1 minute",
		Adjustment:   "unadjusted",
		LimitMessage: "Provider access and empty sessions are checked by the worker.",
	}, nil
}

func (h *Handler) listLists(ctx context.Context) ([]ingestion.SymbolList, error) {
	items, err := h.query(ctx, listPartition, listSortPrefix)
	if err != nil {
		return nil, err
	}
	result := make([]ingestion.SymbolList, 0, len(items))
	for _, item := range items {
		result = append(result, listFromItem(item))
	}
	return result, nil
}

func (h *Handler) saveList(ctx context.Context, raw json.RawMessage) (ingestion.SymbolList, error) {
	var list ingestion.SymbolList
	if err := decodeInput(raw, &list); err != nil {
		return ingestion.SymbolList{}, err
	}
	list.Name = strings.TrimSpace(list.Name)
	list.Symbols = ingestion.NormalizeSymbols(list.Symbols)
	if list.Name == "" {
		return ingestion.SymbolList{}, errors.New("symbol list name is required")
	}
	if len(list.Symbols) == 0 || len(list.Symbols) > maxSymbols {
		return ingestion.SymbolList{}, errors.New("symbol lists must contain between 1 and 100 symbols")
	}
	if err := ingestion.ValidateSymbols(list.Symbols); err != nil {
		return ingestion.SymbolList{}, err
	}
	if list.ID == "" {
		list.ID = stableID("list", list.Name, strings.Join(list.Symbols, ","))
	}
	list.UpdatedAt = h.now().UTC().Format(time.RFC3339)
	item := map[string]dynamodbtypes.AttributeValue{
		"pk":         stringValue(listPartition),
		"sk":         stringValue(listSortPrefix + list.ID),
		"entity":     stringValue("symbol_list"),
		"id":         stringValue(list.ID),
		"name":       stringValue(list.Name),
		"symbols":    stringSet(list.Symbols),
		"updated_at": stringValue(list.UpdatedAt),
	}
	if _, err := h.ddb.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(h.cfg.TableName), Item: item,
	}); err != nil {
		return ingestion.SymbolList{}, fmt.Errorf("save symbol list: %w", err)
	}
	return list, nil
}

func (h *Handler) deleteList(ctx context.Context, raw json.RawMessage) error {
	var request struct {
		ID string `json:"id"`
	}
	if err := decodeInput(raw, &request); err != nil {
		return err
	}
	if strings.TrimSpace(request.ID) == "" {
		return errors.New("symbol list id is required")
	}
	if _, err := h.ddb.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String(h.cfg.TableName),
		Key:       key(listPartition, listSortPrefix+strings.TrimSpace(request.ID)),
	}); err != nil {
		return fmt.Errorf("delete symbol list: %w", err)
	}
	return nil
}

func (h *Handler) startBackfill(ctx context.Context, raw json.RawMessage) (ingestion.Job, error) {
	return h.startBackfillWithSource(ctx, raw, "manual")
}

func (h *Handler) startBackfillWithSource(ctx context.Context, raw json.RawMessage, source string) (ingestion.Job, error) {
	var request ingestion.BackfillRequest
	if err := decodeInput(raw, &request); err != nil {
		return ingestion.Job{}, err
	}
	request.ListID = strings.TrimSpace(request.ListID)
	symbols := ingestion.NormalizeSymbols(request.Symbols)
	if len(symbols) == 0 && request.ListID != "" {
		list, found, err := h.getList(ctx, request.ListID)
		if err != nil {
			return ingestion.Job{}, err
		}
		if !found {
			return ingestion.Job{}, fmt.Errorf("symbol list %q was not found", request.ListID)
		}
		symbols = ingestion.NormalizeSymbols(list.Symbols)
	}
	if len(symbols) == 0 {
		return ingestion.Job{}, errors.New("saved symbol list or symbols are required")
	}
	if len(symbols) > maxSymbols {
		return ingestion.Job{}, errors.New("select between 1 and 100 symbols")
	}
	if err := ingestion.ValidateSymbols(symbols); err != nil {
		return ingestion.Job{}, err
	}
	if err := ingestion.ValidateDateRange(request.Start, request.End); err != nil {
		return ingestion.Job{}, err
	}
	dates, err := datesBetween(request.Start, request.End)
	if err != nil {
		return ingestion.Job{}, err
	}
	if len(dates)*len(symbols) > h.cfg.MaxUnits {
		return ingestion.Job{}, fmt.Errorf("backfill contains %d units, maximum is %d", len(dates)*len(symbols), h.cfg.MaxUnits)
	}
	jobID := stableID("job", request.ListID, strings.Join(symbols, ","), request.Start, request.End)
	if existing, found, err := h.getJob(ctx, jobID); err != nil {
		return ingestion.Job{}, err
	} else if found {
		return existing.Job, nil
	}
	now := h.now().UTC().Format(time.RFC3339)
	job := ingestion.Job{
		ID:       jobID,
		Status:   "queued",
		Provider: "Massive",
		ListID:   request.ListID,
		Symbols:  len(symbols),
		Units:    len(dates) * len(symbols),
	}
	jobItem := map[string]dynamodbtypes.AttributeValue{
		"pk":               stringValue(jobPartition),
		"sk":               stringValue(jobSortPrefix + jobID),
		"entity":           stringValue("job"),
		"id":               stringValue(jobID),
		"status":           stringValue(job.Status),
		"provider":         stringValue(job.Provider),
		"list_id":          stringValue(request.ListID),
		"symbols":          numberValue(job.Symbols),
		"units":            numberValue(job.Units),
		"finished_units":   numberValue(0),
		"clean_units":      numberValue(0),
		"checked_empty":    numberValue(0),
		"incomplete_units": numberValue(0),
		"attention_units":  numberValue(0),
		"failed_units":     numberValue(0),
		"rows":             numberValue(0),
		"invalid_rows":     numberValue(0),
		"start":            stringValue(request.Start),
		"end":              stringValue(request.End),
		"source":           stringValue(source),
		"symbol_snapshot":  jsonValue(symbols),
		"created_at":       stringValue(now),
		"updated_at":       stringValue(now),
	}
	if _, err := h.ddb.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:           aws.String(h.cfg.TableName),
		Item:                jobItem,
		ConditionExpression: aws.String("attribute_not_exists(#pk)"),
		ExpressionAttributeNames: map[string]string{
			"#pk": "pk",
		},
	}); err != nil {
		if isConditionalFailure(err) {
			existing, found, getErr := h.getJob(ctx, jobID)
			if getErr != nil {
				return ingestion.Job{}, getErr
			}
			if found {
				return existing.Job, nil
			}
		}
		return ingestion.Job{}, fmt.Errorf("create ingestion job: %w", err)
	}
	units := make([]dynamodbtypes.WriteRequest, 0, job.Units)
	messages := make([]UnitMessage, 0, job.Units)
	for _, date := range dates {
		for _, symbol := range symbols {
			state := unitState{JobID: jobID, Symbol: symbol, Date: date, Status: "queued"}
			units = append(units, dynamodbtypes.WriteRequest{PutRequest: &dynamodbtypes.PutRequest{Item: unitItem(state)}})
			messages = append(messages, UnitMessage{JobID: jobID, Symbol: symbol, Date: date})
		}
	}
	if err := h.batchWrite(ctx, units); err != nil {
		_ = h.markJobFailed(ctx, jobID, err.Error())
		return ingestion.Job{}, err
	}
	if err := h.enqueue(ctx, messages); err != nil {
		_ = h.markJobFailed(ctx, jobID, err.Error())
		return ingestion.Job{}, err
	}
	return job, nil
}

func (h *Handler) listJobs(ctx context.Context) ([]ingestion.Job, error) {
	items, err := h.query(ctx, jobPartition, jobSortPrefix)
	if err != nil {
		return nil, err
	}
	result := make([]ingestion.Job, 0, len(items))
	for _, item := range items {
		state := jobFromItem(item)
		candidates, err := h.queryIndex(ctx, unitPartitionBase+state.Job.ID, "CANDIDATE#")
		if err != nil {
			return nil, err
		}
		for _, candidate := range candidates {
			if candidateState := candidateFromItem(candidate); candidateState.Status == "pending" {
				state.Job.Discrepancies = append(state.Job.Discrepancies, discrepancyFromItem(candidate))
			}
		}
		if len(state.Job.Discrepancies) > 0 {
			state.Job.NeedsAttention = true
		}
		result = append(result, state.Job)
	}
	return result, nil
}

func (h *Handler) cancelJob(ctx context.Context, raw json.RawMessage) error {
	id, err := requestedID(raw, "job id is required")
	if err != nil {
		return err
	}
	if _, err = h.ddb.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:           aws.String(h.cfg.TableName),
		Key:                 key(jobPartition, jobSortPrefix+id),
		UpdateExpression:    aws.String("SET #status = :cancelled, updated_at = :updated"),
		ConditionExpression: aws.String("#status IN (:queued, :running, :waiting, :incomplete, :attention)"),
		ExpressionAttributeNames: map[string]string{
			"#status": "status",
		},
		ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
			":cancelled":  stringValue("cancelled"),
			":updated":    stringValue(h.now().UTC().Format(time.RFC3339)),
			":queued":     stringValue("queued"),
			":running":    stringValue("running"),
			":waiting":    stringValue("waiting"),
			":incomplete": stringValue("incomplete"),
			":attention":  stringValue("needs-attention"),
		},
	}); err != nil && !isConditionalFailure(err) {
		return fmt.Errorf("cancel job: %w", err)
	}
	return nil
}

func (h *Handler) retryJob(ctx context.Context, raw json.RawMessage) (ingestion.Job, error) {
	id, err := requestedID(raw, "job id is required")
	if err != nil {
		return ingestion.Job{}, err
	}
	state, found, err := h.getJob(ctx, id)
	if err != nil {
		return ingestion.Job{}, err
	}
	if !found {
		return ingestion.Job{}, fmt.Errorf("job %q was not found", id)
	}
	items, err := h.query(ctx, unitPartitionBase+id, unitSortPrefix)
	if err != nil {
		return ingestion.Job{}, err
	}
	messages := make([]UnitMessage, 0)
	for _, item := range items {
		unit := unitFromItem(item)
		if unit.Status == "queued" || unit.Status == "waiting" {
			messages = append(messages, UnitMessage{JobID: id, Symbol: unit.Symbol, Date: unit.Date})
			continue
		}
		if !retryableUnitStatus(unit.Status) {
			continue
		}
		if err := h.resetUnit(ctx, unit); err != nil {
			return ingestion.Job{}, err
		}
		messages = append(messages, UnitMessage{JobID: id, Symbol: unit.Symbol, Date: unit.Date})
	}
	if len(messages) == 0 {
		return state.Job, nil
	}
	if err := h.enqueue(ctx, messages); err != nil {
		return ingestion.Job{}, err
	}
	if _, err := h.ddb.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:                aws.String(h.cfg.TableName),
		Key:                      key(jobPartition, jobSortPrefix+id),
		UpdateExpression:         aws.String("SET #status = :queued, updated_at = :updated, #error = :empty"),
		ExpressionAttributeNames: map[string]string{"#status": "status", "#error": "error"},
		ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
			":queued":  stringValue("queued"),
			":updated": stringValue(h.now().UTC().Format(time.RFC3339)),
			":empty":   stringValue(""),
		},
	}); err != nil {
		return ingestion.Job{}, fmt.Errorf("mark job for retry: %w", err)
	}
	state.Job.Status = "queued"
	return state.Job, nil
}

func (h *Handler) setSchedule(ctx context.Context, raw json.RawMessage) error {
	var request ingestion.ScheduleRequest
	if err := decodeInput(raw, &request); err != nil {
		return err
	}
	request.ListID = strings.TrimSpace(request.ListID)
	request.Symbols = ingestion.NormalizeSymbols(request.Symbols)
	if request.ListID == "" && len(request.Symbols) == 0 {
		return errors.New("saved symbol list or symbols are required")
	}
	if len(request.Symbols) > 0 {
		if err := ingestion.ValidateSymbols(request.Symbols); err != nil {
			return err
		}
	}
	if request.ListID != "" {
		if _, found, err := h.getList(ctx, request.ListID); err != nil {
			return err
		} else if !found {
			return fmt.Errorf("symbol list %q was not found", request.ListID)
		}
	}
	id := stableID("schedule", request.ListID, strings.Join(request.Symbols, ","))
	item := map[string]dynamodbtypes.AttributeValue{
		"pk":         stringValue(schedulePartition),
		"sk":         stringValue(scheduleSortPrefix + id),
		"entity":     stringValue("schedule"),
		"id":         stringValue(id),
		"list_id":    stringValue(request.ListID),
		"symbols":    jsonValue(request.Symbols),
		"enabled":    boolValue(request.Enabled),
		"updated_at": stringValue(h.now().UTC().Format(time.RFC3339)),
	}
	if _, err := h.ddb.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String(h.cfg.TableName), Item: item}); err != nil {
		return fmt.Errorf("save ingestion schedule: %w", err)
	}
	return nil
}

func (h *Handler) runScheduled(ctx context.Context) ([]ingestion.Job, error) {
	items, err := h.query(ctx, schedulePartition, scheduleSortPrefix)
	if err != nil {
		return nil, err
	}
	dates := recentCompletedDates(h.now(), 7)
	start := dates[len(dates)-1]
	end := dates[0]
	result := make([]ingestion.Job, 0, len(items))
	for _, item := range items {
		schedule := scheduleFromItem(item)
		if !schedule.Enabled {
			continue
		}
		request := ingestion.BackfillRequest{ListID: schedule.ListID, Symbols: schedule.Symbols, Start: start, End: end}
		raw, _ := json.Marshal(request)
		job, err := h.startBackfillWithSource(ctx, raw, "schedule")
		if err != nil {
			return nil, fmt.Errorf("run schedule %q: %w", schedule.ID, err)
		}
		result = append(result, job)
	}
	return result, nil
}

func (h *Handler) reviewDiscrepancy(ctx context.Context, raw json.RawMessage) error {
	var request ingestion.ReviewRequest
	if err := decodeInput(raw, &request); err != nil {
		return err
	}
	if strings.TrimSpace(request.CandidateID) == "" {
		return errors.New("candidate id is required")
	}
	if !request.AcceptRemovals {
		return errors.New("explicit removal approval is required")
	}
	output, err := h.ddb.GetItem(ctx, &dynamodb.GetItemInput{TableName: aws.String(h.cfg.TableName), Key: key("CANDIDATE#"+strings.TrimSpace(request.CandidateID), currentSortKey)})
	if err != nil {
		return fmt.Errorf("read discrepancy candidate: %w", err)
	}
	if len(output.Item) == 0 {
		return fmt.Errorf("discrepancy candidate %q was not found", request.CandidateID)
	}
	candidate := candidateFromItem(output.Item)
	if candidate.Status != "pending" {
		return fmt.Errorf("discrepancy candidate %q is already %s", candidate.ID, candidate.Status)
	}
	current, err := h.getDataItem(ctx, candidate.Symbol, candidate.Date)
	if err != nil {
		return err
	}
	if stringAttribute(current, "status") != "current" || stringAttribute(current, "generation") != candidate.CurrentGeneration {
		return errors.New("discrepancy candidate no longer matches current data")
	}
	objectKey := fmt.Sprintf("data/provider=massive/symbol=%s/date=%s/generation=%s.parquet", candidate.Symbol, candidate.Date, stableID("review", candidate.ID))
	if _, err := h.s3.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket: aws.String(h.cfg.Bucket), Key: aws.String(objectKey),
		CopySource: aws.String(url.PathEscape(h.cfg.Bucket + "/" + candidate.CandidateKey)),
	}); err != nil {
		return fmt.Errorf("publish reviewed candidate: %w", err)
	}
	newGeneration := stableID("generation", objectKey)
	if _, err := h.ddb.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: []dynamodbtypes.TransactWriteItem{
		{Update: &dynamodbtypes.Update{
			TableName: aws.String(h.cfg.TableName), Key: key(dataKey(candidate.Symbol, candidate.Date), currentSortKey),
			UpdateExpression:         aws.String("SET object_key = :object_key, generation = :new_generation, updated_at = :updated"),
			ConditionExpression:      aws.String("#status = :current AND generation = :old_generation"),
			ExpressionAttributeNames: map[string]string{"#status": "status"},
			ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
				":current": stringValue("current"), ":old_generation": stringValue(candidate.CurrentGeneration),
				":object_key": stringValue(objectKey), ":new_generation": stringValue(newGeneration),
				":updated": stringValue(h.now().UTC().Format(time.RFC3339)),
			},
		}},
		{Update: &dynamodbtypes.Update{
			TableName: aws.String(h.cfg.TableName), Key: key("CANDIDATE#"+candidate.ID, currentSortKey),
			UpdateExpression:         aws.String("SET #status = :approved, reviewed_at = :reviewed, note = :note"),
			ConditionExpression:      aws.String("#status = :pending"),
			ExpressionAttributeNames: map[string]string{"#status": "status"},
			ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
				":approved": stringValue("approved"), ":reviewed": stringValue(h.now().UTC().Format(time.RFC3339)),
				":note": stringValue(request.Note), ":pending": stringValue("pending"),
			},
		}},
	}}); err != nil {
		_, _ = h.s3.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(h.cfg.Bucket), Key: aws.String(objectKey)})
		return fmt.Errorf("commit reviewed data reference: %w", err)
	}
	unit, found, err := h.getUnit(ctx, candidate.JobID, candidate.Symbol, candidate.Date)
	if err != nil {
		return err
	}
	if found {
		updated := false
		if _, err := h.ddb.UpdateItem(ctx, &dynamodb.UpdateItemInput{
			TableName: aws.String(h.cfg.TableName), Key: key(unitPartitionBase+candidate.JobID, unitKey(candidate.Symbol, candidate.Date)),
			UpdateExpression:         aws.String("SET #status = :completed, #error = :empty, object_key = :object_key, candidate_key = :empty"),
			ConditionExpression:      aws.String("#status = :attention"),
			ExpressionAttributeNames: map[string]string{"#status": "status", "#error": "error"},
			ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
				":completed": stringValue("completed"), ":attention": stringValue("needs-attention"),
				":empty": stringValue(""), ":object_key": stringValue(objectKey),
			},
		}); err != nil {
			if !isConditionalFailure(err) {
				return fmt.Errorf("complete reviewed ingestion unit: %w", err)
			}
		} else {
			updated = true
		}
		if updated {
			if err := h.adjustJob(ctx, unit, "completed", unit.Rows, unit.InvalidRows, ""); err != nil {
				return err
			}
		}
	}
	return nil
}

func (h *Handler) deleteData(ctx context.Context, raw json.RawMessage) (ingestion.DeleteResult, error) {
	var request ingestion.DeleteRequest
	if err := decodeInput(raw, &request); err != nil {
		return ingestion.DeleteResult{}, err
	}
	request.Symbols = ingestion.NormalizeSymbols(request.Symbols)
	if len(request.Symbols) == 0 || len(request.Symbols) > maxSymbols {
		return ingestion.DeleteResult{}, errors.New("select between 1 and 100 symbols")
	}
	if err := ingestion.ValidateSymbols(request.Symbols); err != nil {
		return ingestion.DeleteResult{}, err
	}
	dates, err := datesBetween(request.Start, request.End)
	if err != nil {
		return ingestion.DeleteResult{}, err
	}
	deleted := 0
	for _, symbol := range request.Symbols {
		for _, date := range dates {
			if err := h.deleteDataUnit(ctx, symbol, date); err != nil {
				return ingestion.DeleteResult{}, err
			}
			deleted++
		}
	}
	return ingestion.DeleteResult{Status: "completed", DeletedUnits: deleted, Message: "Data deleted and excluded from scheduled catch-up."}, nil
}

func (h *Handler) deleteDataUnit(ctx context.Context, symbol, date string) error {
	item, err := h.getDataItem(ctx, symbol, date)
	if err != nil {
		return err
	}
	for _, name := range []string{"object_key", "candidate_key", "quarantine_key"} {
		if objectKey := stringAttribute(item, name); objectKey != "" {
			if _, err := h.s3.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(h.cfg.Bucket), Key: aws.String(objectKey)}); err != nil {
				return fmt.Errorf("delete S3 object %q: %w", objectKey, err)
			}
		}
	}
	for _, prefix := range []string{
		fmt.Sprintf("data/provider=massive/symbol=%s/date=%s/", symbol, date),
		fmt.Sprintf("candidates/provider=massive/symbol=%s/date=%s/", symbol, date),
		fmt.Sprintf("quarantine/provider=massive/symbol=%s/date=%s/", symbol, date),
	} {
		if err := h.deleteObjectsWithPrefix(ctx, prefix); err != nil {
			return err
		}
	}
	tombstone := map[string]dynamodbtypes.AttributeValue{
		"pk":         stringValue(dataKey(symbol, date)),
		"sk":         stringValue(currentSortKey),
		"entity":     stringValue("data_exclusion"),
		"status":     stringValue("deleted"),
		"symbol":     stringValue(symbol),
		"date":       stringValue(date),
		"updated_at": stringValue(h.now().UTC().Format(time.RFC3339)),
	}
	if _, err := h.ddb.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String(h.cfg.TableName), Item: tombstone}); err != nil {
		return fmt.Errorf("record deletion exclusion: %w", err)
	}
	return nil
}

func (h *Handler) deleteObjectsWithPrefix(ctx context.Context, prefix string) error {
	paginator := s3.NewListObjectsV2Paginator(h.s3, &s3.ListObjectsV2Input{Bucket: aws.String(h.cfg.Bucket), Prefix: aws.String(prefix)})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("list S3 objects under %q: %w", prefix, err)
		}
		identifiers := make([]s3types.ObjectIdentifier, 0, len(page.Contents))
		for _, object := range page.Contents {
			identifiers = append(identifiers, s3types.ObjectIdentifier{Key: object.Key})
		}
		if len(identifiers) == 0 {
			continue
		}
		_, err = h.s3.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(h.cfg.Bucket), Delete: &s3types.Delete{Objects: identifiers, Quiet: aws.Bool(true)},
		})
		if err != nil {
			return fmt.Errorf("delete S3 objects under %q: %w", prefix, err)
		}
	}
	return nil
}

func (h *Handler) handleUnit(ctx context.Context, message UnitMessage) error {
	message.Symbol = strings.ToUpper(strings.TrimSpace(message.Symbol))
	message.Date = strings.TrimSpace(message.Date)
	if message.JobID == "" || message.Symbol == "" || message.Date == "" {
		return errors.New("queue unit requires job_id, symbol and date")
	}
	if err := ingestion.ValidateSymbols([]string{message.Symbol}); err != nil {
		return err
	}
	if err := ingestion.ValidateDateRange(message.Date, message.Date); err != nil {
		return err
	}
	job, found, err := h.getJob(ctx, message.JobID)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("job %q was not found", message.JobID)
	}
	if job.Job.Status == "cancelled" {
		return nil
	}
	unit, found, err := h.getUnit(ctx, message.JobID, message.Symbol, message.Date)
	if err != nil {
		return err
	}
	if !found || terminalUnitStatus(unit.Status) {
		return nil
	}
	claimed := false
	unit, claimed, err = h.claimUnit(ctx, unit)
	if err != nil {
		return err
	}
	if !claimed {
		return nil
	}
	if job.Job.Status == "cancelled" || job.Job.Status == "failed" {
		return h.finishUnit(ctx, unit, "cancelled", 0, 0, "job is no longer active", "", "", "")
	}
	dataItem, err := h.getDataItem(ctx, message.Symbol, message.Date)
	if err != nil {
		return h.retryUnit(ctx, unit, err)
	}
	if stringAttribute(dataItem, "status") == "deleted" {
		if job.Source == "manual" && deletionPredatesJob(dataItem, job.CreatedAt) {
			if err := h.clearExclusion(ctx, message.Symbol, message.Date); err != nil {
				return h.retryUnit(ctx, unit, err)
			}
		} else {
			return h.finishUnit(ctx, unit, "cancelled", 0, 0, "unit is excluded by an explicit deletion", "", "", "")
		}
	}
	key, err := h.massiveAPIKey(ctx)
	if err != nil {
		return h.finishProviderError(ctx, unit, err)
	}
	if err := h.waitForProviderSlot(ctx); err != nil {
		return h.retryUnit(ctx, unit, err)
	}
	location, _ := time.LoadLocation("America/New_York")
	date, _ := time.ParseInLocation("2006-01-02", message.Date, location)
	result, err := ingestion.NewMassiveClient(key, nil).FetchUnit(ctx, ingestion.FetchRequest{
		Symbol: message.Symbol, StartDate: date, EndDate: date, Adjusted: false,
	})
	if err != nil {
		return h.finishProviderError(ctx, unit, err)
	}
	quarantineKey := ""
	if len(result.Issues) > 0 {
		quarantineKey = quarantineObjectKey(message.Symbol, message.Date, unit.Attempts)
		if err := h.putQuarantine(ctx, quarantineKey, result.Issues); err != nil {
			return h.retryUnit(ctx, unit, err)
		}
	}
	if len(result.Bars) == 0 {
		if result.CheckedEmpty {
			return h.finishUnit(ctx, unit, "checked-empty", 0, int64(len(result.Issues)), "", "", "", quarantineKey)
		}
		return h.finishUnit(ctx, unit, "needs-attention", 0, int64(len(result.Issues)), "all returned rows were invalid", "", "", quarantineKey)
	}
	if len(result.Issues) == 0 && job.Source == "schedule" && stringAttribute(dataItem, "status") == "current" {
		omitted, err := h.omittedTimestamps(ctx, stringAttribute(dataItem, "object_key"), result.Bars)
		if err != nil {
			return h.retryUnit(ctx, unit, err)
		}
		if len(omitted) > 0 {
			candidateKey := candidateObjectKey(message.Symbol, message.Date, unit.Attempts)
			if err := h.putParquet(ctx, candidateKey, result.Bars); err != nil {
				return h.retryUnit(ctx, unit, err)
			}
			candidateID := stableID("discrepancy", message.JobID, message.Symbol, message.Date, stringAttribute(dataItem, "generation"))
			if err := h.recordCandidate(ctx, candidateState{
				ID: candidateID, JobID: message.JobID, Symbol: message.Symbol, Date: message.Date,
				CurrentGeneration: stringAttribute(dataItem, "generation"), CurrentObjectKey: stringAttribute(dataItem, "object_key"),
				CandidateKey: candidateKey, Omitted: omitted, Rows: int64(len(result.Bars)),
			}); err != nil {
				return h.retryUnit(ctx, unit, err)
			}
			return h.finishUnit(ctx, unit, "needs-attention", int64(len(result.Bars)), 0, "refresh omitted timestamps; explicit review is required", "", candidateKey, "")
		}
	}
	if len(result.Issues) > 0 {
		candidateKey := candidateObjectKey(message.Symbol, message.Date, unit.Attempts)
		if err := h.putParquet(ctx, candidateKey, result.Bars); err != nil {
			return h.retryUnit(ctx, unit, err)
		}
		return h.finishUnit(ctx, unit, "incomplete", int64(len(result.Bars)), int64(len(result.Issues)), "invalid rows were quarantined; candidate was not published", "", candidateKey, quarantineKey)
	}
	objectKey := publishedObjectKey(message.Symbol, message.Date, unit.Attempts)
	if err := h.publish(ctx, message.Symbol, message.Date, objectKey, result.Bars); err != nil {
		return h.retryUnit(ctx, unit, err)
	}
	return h.finishUnit(ctx, unit, "completed", int64(len(result.Bars)), 0, "", objectKey, "", "")
}

func (h *Handler) finishProviderError(ctx context.Context, unit unitState, providerErr error) error {
	var typed *ingestion.ProviderError
	if errors.As(providerErr, &typed) && typed.Retryable() && unit.Attempts < maxAttempts {
		if err := h.finishUnit(ctx, unit, "waiting", 0, 0, providerErr.Error(), "", "", ""); err != nil {
			return err
		}
		return providerErr
	}
	status := "needs-attention"
	if errors.As(providerErr, &typed) && typed.Retryable() {
		status = "failed"
	}
	return h.finishUnit(ctx, unit, status, 0, 0, providerErr.Error(), "", "", "")
}

func (h *Handler) retryUnit(ctx context.Context, unit unitState, cause error) error {
	status := "waiting"
	if unit.Attempts >= maxAttempts {
		status = "failed"
	}
	if err := h.finishUnit(ctx, unit, status, 0, 0, cause.Error(), "", "", ""); err != nil {
		return err
	}
	if status == "failed" {
		return nil
	}
	return cause
}

func (h *Handler) massiveAPIKey(ctx context.Context) (string, error) {
	h.secretOnce.Do(func() {
		if h.cfg.MassiveSecretARN == "" {
			h.secret = strings.TrimSpace(os.Getenv("MASSIVE_API_KEY"))
			if h.secret == "" {
				h.secretErr = errors.New("Massive API key is not configured")
			}
			return
		}
		output, err := h.secrets.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{SecretId: aws.String(h.cfg.MassiveSecretARN)})
		if err != nil {
			h.secretErr = fmt.Errorf("read Massive secret: %w", err)
			return
		}
		secret := strings.TrimSpace(aws.ToString(output.SecretString))
		var payload struct {
			APIKey string `json:"MASSIVE_API_KEY"`
			Key    string `json:"api_key"`
		}
		if json.Unmarshal([]byte(secret), &payload) == nil {
			if payload.APIKey != "" {
				secret = payload.APIKey
			} else if payload.Key != "" {
				secret = payload.Key
			}
		}
		h.secret = strings.TrimSpace(secret)
		if h.secret == "" {
			h.secretErr = errors.New("Massive secret is empty")
		}
	})
	if h.secretErr != nil {
		return "", h.secretErr
	}
	return h.secret, nil
}

func (h *Handler) waitForProviderSlot(ctx context.Context) error {
	for {
		now := h.now().UTC()
		nowMillis := now.UnixMilli()
		nextMillis := now.Add(providerRequestInterval).UnixMilli()
		_, err := h.ddb.UpdateItem(ctx, &dynamodb.UpdateItemInput{
			TableName:           aws.String(h.cfg.TableName),
			Key:                 key("RATE#Massive", currentSortKey),
			UpdateExpression:    aws.String("SET next_allowed_at = :next, updated_at = :updated"),
			ConditionExpression: aws.String("attribute_not_exists(next_allowed_at) OR next_allowed_at <= :now"),
			ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
				":next":    numberValue(nextMillis),
				":now":     numberValue(nowMillis),
				":updated": stringValue(now.Format(time.RFC3339)),
			},
		})
		if err == nil {
			return nil
		}
		if !isConditionalFailure(err) {
			return fmt.Errorf("reserve Massive request slot: %w", err)
		}
		item, err := h.ddb.GetItem(ctx, &dynamodb.GetItemInput{TableName: aws.String(h.cfg.TableName), Key: key("RATE#Massive", currentSortKey), ProjectionExpression: aws.String("next_allowed_at")})
		if err != nil {
			return fmt.Errorf("read Massive request slot: %w", err)
		}
		next := numberAttribute(item.Item, "next_allowed_at")
		delay := time.Duration(next-nowMillis) * time.Millisecond
		if delay < 0 {
			delay = providerRequestInterval
		}
		if err := wait(ctx, delay); err != nil {
			return err
		}
	}
}

type parquetBar struct {
	Provider *string    `parquet:"provider,optional"`
	Symbol   *string    `parquet:"symbol,optional"`
	Open     *float64   `parquet:"open,optional"`
	High     *float64   `parquet:"high,optional"`
	Low      *float64   `parquet:"low,optional"`
	Close    *float64   `parquet:"close,optional"`
	Volume   *uint64    `parquet:"volume,optional,uint(64)"`
	TsEvent  *time.Time `parquet:"ts_event,optional,timestamp(nanosecond:utc)"`
}

func (h *Handler) putParquet(ctx context.Context, objectKey string, bars []ingestion.NormalizedBar) error {
	data, err := encodeParquet(bars)
	if err != nil {
		return err
	}
	if _, err := h.s3.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(h.cfg.Bucket),
		Key:         aws.String(objectKey),
		Body:        bytes.NewReader(data),
		ContentType: aws.String("application/vnd.apache.parquet"),
	}); err != nil {
		return fmt.Errorf("write S3 object %q: %w", objectKey, err)
	}
	return nil
}

func encodeParquet(bars []ingestion.NormalizedBar) ([]byte, error) {
	var buffer bytes.Buffer
	writer := parquet.NewGenericWriter[parquetBar](&buffer, parquet.Compression(&snappy.Codec{}))
	rows := make([]parquetBar, 0, len(bars))
	for _, bar := range bars {
		provider, symbol := bar.Provider, bar.Symbol
		open, high, low, closePrice := bar.Open, bar.High, bar.Low, bar.Close
		volume, timestamp := bar.Volume, bar.IntervalStart
		rows = append(rows, parquetBar{Provider: &provider, Symbol: &symbol, Open: &open, High: &high, Low: &low, Close: &closePrice, Volume: &volume, TsEvent: &timestamp})
	}
	if _, err := writer.Write(rows); err != nil {
		return nil, fmt.Errorf("encode Parquet: %w", err)
	}
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("close Parquet writer: %w", err)
	}
	return buffer.Bytes(), nil
}

func (h *Handler) putQuarantine(ctx context.Context, objectKey string, issues []ingestion.RowIssue) error {
	var buffer bytes.Buffer
	zipWriter := gzip.NewWriter(&buffer)
	encoder := json.NewEncoder(zipWriter)
	for _, issue := range issues {
		if err := encoder.Encode(issue); err != nil {
			_ = zipWriter.Close()
			return fmt.Errorf("encode quarantine row: %w", err)
		}
	}
	if err := zipWriter.Close(); err != nil {
		return fmt.Errorf("close quarantine object: %w", err)
	}
	if _, err := h.s3.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(h.cfg.Bucket), Key: aws.String(objectKey), Body: bytes.NewReader(buffer.Bytes()),
		ContentType: aws.String("application/x-ndjson"), ContentEncoding: aws.String("gzip"),
	}); err != nil {
		return fmt.Errorf("write quarantine object %q: %w", objectKey, err)
	}
	return nil
}

func (h *Handler) omittedTimestamps(ctx context.Context, objectKey string, bars []ingestion.NormalizedBar) ([]string, error) {
	if objectKey == "" {
		return nil, nil
	}
	output, err := h.s3.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(h.cfg.Bucket), Key: aws.String(objectKey)})
	if err != nil {
		return nil, fmt.Errorf("read current Parquet object: %w", err)
	}
	data, readErr := io.ReadAll(output.Body)
	closeErr := output.Body.Close()
	if readErr != nil {
		return nil, fmt.Errorf("read current Parquet bytes: %w", readErr)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("close current Parquet object: %w", closeErr)
	}
	reader := parquet.NewGenericReader[parquetBar](bytes.NewReader(data))
	defer reader.Close()
	previous := make(map[int64]time.Time)
	rows := make([]parquetBar, 256)
	for {
		count, err := reader.Read(rows)
		for index := 0; index < count; index++ {
			if rows[index].TsEvent != nil {
				previous[rows[index].TsEvent.UnixNano()] = *rows[index].TsEvent
			}
			rows[index] = parquetBar{}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("decode current Parquet object: %w", err)
		}
	}
	current := make(map[int64]struct{}, len(bars))
	for _, bar := range bars {
		current[bar.IntervalStart.UnixNano()] = struct{}{}
	}
	omitted := make([]time.Time, 0)
	for timestamp, value := range previous {
		if _, exists := current[timestamp]; !exists {
			omitted = append(omitted, value)
		}
	}
	sort.Slice(omitted, func(left, right int) bool { return omitted[left].Before(omitted[right]) })
	result := make([]string, 0, len(omitted))
	for _, timestamp := range omitted {
		result = append(result, timestamp.UTC().Format(time.RFC3339))
	}
	return result, nil
}

type candidateState struct {
	ID                string
	JobID             string
	Symbol            string
	Date              string
	CurrentGeneration string
	CurrentObjectKey  string
	CandidateKey      string
	Omitted           []string
	Rows              int64
	Status            string
}

func (h *Handler) recordCandidate(ctx context.Context, candidate candidateState) error {
	if _, err := h.ddb.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(h.cfg.TableName),
		Item: map[string]dynamodbtypes.AttributeValue{
			"pk":                 stringValue("CANDIDATE#" + candidate.ID),
			"sk":                 stringValue(currentSortKey),
			"gsi1pk":             stringValue(unitPartitionBase + candidate.JobID),
			"gsi1sk":             stringValue("CANDIDATE#" + candidate.ID),
			"entity":             stringValue("discrepancy_candidate"),
			"id":                 stringValue(candidate.ID),
			"job_id":             stringValue(candidate.JobID),
			"symbol":             stringValue(candidate.Symbol),
			"date":               stringValue(candidate.Date),
			"current_generation": stringValue(candidate.CurrentGeneration),
			"current_object_key": stringValue(candidate.CurrentObjectKey),
			"candidate_key":      stringValue(candidate.CandidateKey),
			"omitted":            jsonValue(candidate.Omitted),
			"rows":               numberValue(candidate.Rows),
			"status":             stringValue("pending"),
			"created_at":         stringValue(h.now().UTC().Format(time.RFC3339)),
		},
	}); err != nil {
		return fmt.Errorf("record discrepancy candidate: %w", err)
	}
	return nil
}

func (h *Handler) queryIndex(ctx context.Context, partition, sortPrefix string) ([]map[string]dynamodbtypes.AttributeValue, error) {
	output, err := h.ddb.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(h.cfg.TableName),
		IndexName:              aws.String("IngestionIndex"),
		KeyConditionExpression: aws.String("#pk = :pk AND begins_with(#sk, :prefix)"),
		ExpressionAttributeNames: map[string]string{
			"#pk": "gsi1pk",
			"#sk": "gsi1sk",
		},
		ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
			":pk": stringValue(partition), ":prefix": stringValue(sortPrefix),
		},
	})
	if err != nil {
		return nil, fmt.Errorf("query discrepancy candidates: %w", err)
	}
	return output.Items, nil
}

func (h *Handler) publish(ctx context.Context, symbol, date, objectKey string, bars []ingestion.NormalizedBar) error {
	if deleted, err := h.isDeleted(ctx, symbol, date); err != nil {
		return err
	} else if deleted {
		return errors.New("unit was deleted while it was being fetched")
	}
	if err := h.putParquet(ctx, objectKey, bars); err != nil {
		return err
	}
	item := map[string]dynamodbtypes.AttributeValue{
		"pk":         stringValue(dataKey(symbol, date)),
		"sk":         stringValue(currentSortKey),
		"entity":     stringValue("current_data"),
		"status":     stringValue("current"),
		"symbol":     stringValue(symbol),
		"date":       stringValue(date),
		"object_key": stringValue(objectKey),
		"generation": stringValue(stableID("generation", objectKey)),
		"updated_at": stringValue(h.now().UTC().Format(time.RFC3339)),
	}
	if _, err := h.ddb.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:                 aws.String(h.cfg.TableName),
		Item:                      item,
		ConditionExpression:       aws.String("attribute_not_exists(#status) OR #status <> :deleted"),
		ExpressionAttributeNames:  map[string]string{"#status": "status"},
		ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":deleted": stringValue("deleted")},
	}); err != nil {
		_, _ = h.s3.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(h.cfg.Bucket), Key: aws.String(objectKey)})
		return fmt.Errorf("publish current data reference: %w", err)
	}
	return nil
}

func (h *Handler) claimUnit(ctx context.Context, unit unitState) (unitState, bool, error) {
	attempt := unit.Attempts + 1
	if _, err := h.ddb.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:                aws.String(h.cfg.TableName),
		Key:                      key(unitPartitionBase+unit.JobID, unitKey(unit.Symbol, unit.Date)),
		UpdateExpression:         aws.String("SET #status = :running, attempts = :attempts, last_attempt = :last_attempt, updated_at = :updated"),
		ConditionExpression:      aws.String("#status IN (:queued, :waiting)"),
		ExpressionAttributeNames: map[string]string{"#status": "status"},
		ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
			":running":      stringValue("running"),
			":attempts":     numberValue(attempt),
			":last_attempt": stringValue(h.now().UTC().Format(time.RFC3339)),
			":updated":      stringValue(h.now().UTC().Format(time.RFC3339)),
			":queued":       stringValue("queued"),
			":waiting":      stringValue("waiting"),
		},
	}); err != nil {
		if isConditionalFailure(err) {
			return unit, false, nil
		}
		return unit, false, fmt.Errorf("claim ingestion unit: %w", err)
	}
	unit.Attempts = attempt
	return unit, true, nil
}

func (h *Handler) finishUnit(ctx context.Context, unit unitState, status string, rows, invalidRows int64, message, objectKey, candidateKey, quarantineKey string) error {
	values := map[string]dynamodbtypes.AttributeValue{
		":status":     stringValue(status),
		":rows":       numberValue(rows),
		":invalid":    numberValue(invalidRows),
		":error":      stringValue(message),
		":updated":    stringValue(h.now().UTC().Format(time.RFC3339)),
		":running":    stringValue("running"),
		":object_key": stringValue(objectKey),
		":candidate":  stringValue(candidateKey),
		":quarantine": stringValue(quarantineKey),
	}
	if _, err := h.ddb.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:                 aws.String(h.cfg.TableName),
		Key:                       key(unitPartitionBase+unit.JobID, unitKey(unit.Symbol, unit.Date)),
		UpdateExpression:          aws.String("SET #status = :status, rows = :rows, invalid_rows = :invalid, #error = :error, object_key = :object_key, candidate_key = :candidate, quarantine_key = :quarantine, updated_at = :updated"),
		ConditionExpression:       aws.String("#status = :running"),
		ExpressionAttributeNames:  map[string]string{"#status": "status", "#error": "error"},
		ExpressionAttributeValues: values,
	}); err != nil {
		if isConditionalFailure(err) {
			return nil
		}
		return fmt.Errorf("save ingestion unit result: %w", err)
	}
	return h.adjustJob(ctx, unit, status, rows, invalidRows, message)
}

func (h *Handler) adjustJob(ctx context.Context, previous unitState, nextStatus string, rows, invalidRows int64, message string) error {
	state, found, err := h.getJob(ctx, previous.JobID)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("job %q was not found while recording unit", previous.JobID)
	}
	delta := statusDelta(previous.Status, nextStatus)
	delta["rows"] = rows - previous.Rows
	delta["invalid_rows"] = invalidRows - previous.InvalidRows
	status := state.Job.Status
	if status != "cancelled" {
		status = statusForCounters(state, delta)
		if status == "" {
			status = "running"
		}
	}
	values := map[string]dynamodbtypes.AttributeValue{
		":finished":  numberValue(delta["finished_units"]),
		":clean":     numberValue(delta["clean_units"]),
		":empty":     numberValue(delta["checked_empty"]),
		":partial":   numberValue(delta["incomplete_units"]),
		":attention": numberValue(delta["attention_units"]),
		":failed":    numberValue(delta["failed_units"]),
		":rows":      numberValue(delta["rows"]),
		":invalid":   numberValue(delta["invalid_rows"]),
		":status":    stringValue(status),
		":updated":   stringValue(h.now().UTC().Format(time.RFC3339)),
		":running":   stringValue("running"),
		":cancelled": stringValue("cancelled"),
	}
	update := "SET #status = :status, last_attempt = :updated, updated_at = :updated"
	if message != "" {
		update += ", #error = :message"
		values[":message"] = stringValue(message)
	}
	if nextStatus == "completed" {
		update += ", last_successful = :updated"
	}
	update += " ADD finished_units :finished, clean_units :clean, checked_empty :empty, incomplete_units :partial, attention_units :attention, failed_units :failed, rows :rows, invalid_rows :invalid"
	_, err = h.ddb.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(h.cfg.TableName), Key: key(jobPartition, jobSortPrefix+previous.JobID),
		UpdateExpression:          aws.String(update),
		ConditionExpression:       aws.String("#status <> :cancelled"),
		ExpressionAttributeNames:  map[string]string{"#status": "status", "#error": "error"},
		ExpressionAttributeValues: values,
	})
	if err != nil && !isConditionalFailure(err) {
		return fmt.Errorf("update ingestion job: %w", err)
	}
	return nil
}

func (h *Handler) resetUnit(ctx context.Context, unit unitState) error {
	if _, err := h.ddb.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(h.cfg.TableName), Key: key(unitPartitionBase+unit.JobID, unitKey(unit.Symbol, unit.Date)),
		UpdateExpression:         aws.String("SET #status = :waiting, attempts = :zero, rows = :zero, invalid_rows = :zero, #error = :empty, object_key = :empty, candidate_key = :empty, quarantine_key = :empty, updated_at = :updated"),
		ConditionExpression:      aws.String("#status IN (:failed, :incomplete, :attention)"),
		ExpressionAttributeNames: map[string]string{"#status": "status", "#error": "error"},
		ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
			":waiting":    stringValue("waiting"),
			":zero":       numberValue(0),
			":empty":      stringValue(""),
			":updated":    stringValue(h.now().UTC().Format(time.RFC3339)),
			":failed":     stringValue("failed"),
			":incomplete": stringValue("incomplete"),
			":attention":  stringValue("needs-attention"),
		},
	}); err != nil {
		if isConditionalFailure(err) {
			return nil
		}
		return fmt.Errorf("reset ingestion unit: %w", err)
	}
	return h.adjustJob(ctx, unit, "waiting", 0, 0, "")
}

func (h *Handler) markJobFailed(ctx context.Context, id, message string) error {
	_, err := h.ddb.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(h.cfg.TableName), Key: key(jobPartition, jobSortPrefix+id),
		UpdateExpression:         aws.String("SET #status = :failed, #error = :error, updated_at = :updated"),
		ExpressionAttributeNames: map[string]string{"#status": "status", "#error": "error"},
		ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
			":failed":  stringValue("failed"),
			":error":   stringValue(message),
			":updated": stringValue(h.now().UTC().Format(time.RFC3339)),
		},
	})
	return err
}

func (h *Handler) getList(ctx context.Context, id string) (ingestion.SymbolList, bool, error) {
	output, err := h.ddb.GetItem(ctx, &dynamodb.GetItemInput{TableName: aws.String(h.cfg.TableName), Key: key(listPartition, listSortPrefix+id)})
	if err != nil {
		return ingestion.SymbolList{}, false, fmt.Errorf("read symbol list: %w", err)
	}
	if len(output.Item) == 0 {
		return ingestion.SymbolList{}, false, nil
	}
	return listFromItem(output.Item), true, nil
}

func (h *Handler) getJob(ctx context.Context, id string) (jobState, bool, error) {
	output, err := h.ddb.GetItem(ctx, &dynamodb.GetItemInput{TableName: aws.String(h.cfg.TableName), Key: key(jobPartition, jobSortPrefix+id)})
	if err != nil {
		return jobState{}, false, fmt.Errorf("read ingestion job: %w", err)
	}
	if len(output.Item) == 0 {
		return jobState{}, false, nil
	}
	return jobFromItem(output.Item), true, nil
}

func (h *Handler) getUnit(ctx context.Context, jobID, symbol, date string) (unitState, bool, error) {
	output, err := h.ddb.GetItem(ctx, &dynamodb.GetItemInput{TableName: aws.String(h.cfg.TableName), Key: key(unitPartitionBase+jobID, unitKey(symbol, date))})
	if err != nil {
		return unitState{}, false, fmt.Errorf("read ingestion unit: %w", err)
	}
	if len(output.Item) == 0 {
		return unitState{}, false, nil
	}
	return unitFromItem(output.Item), true, nil
}

func (h *Handler) getDataItem(ctx context.Context, symbol, date string) (map[string]dynamodbtypes.AttributeValue, error) {
	output, err := h.ddb.GetItem(ctx, &dynamodb.GetItemInput{TableName: aws.String(h.cfg.TableName), Key: key(dataKey(symbol, date), currentSortKey)})
	if err != nil {
		return nil, fmt.Errorf("read current data reference: %w", err)
	}
	return output.Item, nil
}

func (h *Handler) isDeleted(ctx context.Context, symbol, date string) (bool, error) {
	item, err := h.getDataItem(ctx, symbol, date)
	if err != nil {
		return false, err
	}
	return stringAttribute(item, "status") == "deleted", nil
}

func (h *Handler) clearExclusion(ctx context.Context, symbol, date string) error {
	item, err := h.getDataItem(ctx, symbol, date)
	if err != nil {
		return err
	}
	if stringAttribute(item, "status") != "deleted" {
		return nil
	}
	if _, err := h.ddb.DeleteItem(ctx, &dynamodb.DeleteItemInput{TableName: aws.String(h.cfg.TableName), Key: key(dataKey(symbol, date), currentSortKey)}); err != nil {
		return fmt.Errorf("clear data exclusion: %w", err)
	}
	return nil
}

func deletionPredatesJob(item map[string]dynamodbtypes.AttributeValue, createdAt string) bool {
	deletedAt, deletedErr := time.Parse(time.RFC3339, stringAttribute(item, "updated_at"))
	jobAt, jobErr := time.Parse(time.RFC3339, createdAt)
	return deletedErr == nil && jobErr == nil && deletedAt.Before(jobAt)
}

func (h *Handler) query(ctx context.Context, partition, sortPrefix string) ([]map[string]dynamodbtypes.AttributeValue, error) {
	output, err := h.ddb.Query(ctx, &dynamodb.QueryInput{
		TableName:                 aws.String(h.cfg.TableName),
		KeyConditionExpression:    aws.String("#pk = :pk AND begins_with(#sk, :prefix)"),
		ExpressionAttributeNames:  map[string]string{"#pk": "pk", "#sk": "sk"},
		ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":pk": stringValue(partition), ":prefix": stringValue(sortPrefix)},
	})
	if err != nil {
		return nil, fmt.Errorf("query ingestion records: %w", err)
	}
	return output.Items, nil
}

func (h *Handler) batchWrite(ctx context.Context, requests []dynamodbtypes.WriteRequest) error {
	for len(requests) > 0 {
		count := 25
		if len(requests) < count {
			count = len(requests)
		}
		pending := requests[:count]
		requests = requests[count:]
		for attempt := 0; ; attempt++ {
			output, err := h.ddb.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{RequestItems: map[string][]dynamodbtypes.WriteRequest{h.cfg.TableName: pending}})
			if err != nil {
				return fmt.Errorf("write ingestion records: %w", err)
			}
			pending = output.UnprocessedItems[h.cfg.TableName]
			if len(pending) == 0 {
				break
			}
			if attempt >= 4 {
				return errors.New("DynamoDB left ingestion records unprocessed after retries")
			}
			if err := wait(ctx, time.Duration(1<<attempt)*100*time.Millisecond); err != nil {
				return err
			}
		}
	}
	return nil
}

func (h *Handler) enqueue(ctx context.Context, messages []UnitMessage) error {
	for len(messages) > 0 {
		count := 10
		if len(messages) < count {
			count = len(messages)
		}
		batch := messages[:count]
		messages = messages[count:]
		entries := make([]sqstypes.SendMessageBatchRequestEntry, 0, len(batch))
		for index, message := range batch {
			body, _ := json.Marshal(message)
			entry := sqstypes.SendMessageBatchRequestEntry{Id: aws.String(strconv.Itoa(index)), MessageBody: aws.String(string(body))}
			if strings.HasSuffix(h.cfg.QueueURL, ".fifo") {
				entry.MessageGroupId = aws.String("massive-ingestion")
				entry.MessageDeduplicationId = aws.String(stableID("unit", message.JobID, message.Symbol, message.Date))
			}
			entries = append(entries, entry)
		}
		output, err := h.sqs.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{QueueUrl: aws.String(h.cfg.QueueURL), Entries: entries})
		if err != nil {
			return fmt.Errorf("enqueue ingestion units: %w", err)
		}
		if len(output.Failed) > 0 {
			return fmt.Errorf("enqueue ingestion units: %s", aws.ToString(output.Failed[0].Message))
		}
	}
	return nil
}

func (h *Handler) massiveKeyForTests(key string) {
	h.secretOnce.Do(func() { h.secret = key })
}

func decodeInput(raw json.RawMessage, target any) error {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	if err := json.Unmarshal(raw, target); err != nil {
		return fmt.Errorf("decode ingestion input: %w", err)
	}
	return nil
}

func requestedID(raw json.RawMessage, required string) (string, error) {
	var request struct {
		ID string `json:"id"`
	}
	if err := decodeInput(raw, &request); err != nil {
		return "", err
	}
	request.ID = strings.TrimSpace(request.ID)
	if request.ID == "" {
		return "", errors.New(required)
	}
	return request.ID, nil
}

func datesBetween(start, end string) ([]string, error) {
	if err := ingestion.ValidateDateRange(start, end); err != nil {
		return nil, err
	}
	first, _ := parseDate(start)
	last, _ := parseDate(end)
	result := make([]string, 0)
	for current := first; !current.After(last); current = current.AddDate(0, 0, 1) {
		result = append(result, current.Format("2006-01-02"))
	}
	return result, nil
}

func recentCompletedDates(now time.Time, count int) []string {
	location, _ := time.LoadLocation("America/New_York")
	today := now.In(location)
	result := make([]string, 0, count)
	for offset := 1; len(result) < count; offset++ {
		result = append(result, today.AddDate(0, 0, -offset).Format("2006-01-02"))
	}
	return result
}

func parseDate(value string) (time.Time, error) {
	return time.ParseInLocation("2006-01-02", value, time.UTC)
}

func stableID(prefix string, values ...string) string {
	digest := sha256.New()
	_, _ = io.WriteString(digest, prefix)
	for _, value := range values {
		_, _ = io.WriteString(digest, "\x00")
		_, _ = io.WriteString(digest, value)
	}
	return prefix + "-" + hex.EncodeToString(digest.Sum(nil)[:12])
}

func dataKey(symbol, date string) string { return dataPartitionBase + symbol + "#" + date }

func unitKey(symbol, date string) string { return unitSortPrefix + date + "#" + symbol }

func publishedObjectKey(symbol, date string, attempt int) string {
	return fmt.Sprintf("data/provider=massive/symbol=%s/date=%s/generation=%s.parquet", symbol, date, stableID("generation", symbol, date, strconv.Itoa(attempt)))
}

func candidateObjectKey(symbol, date string, attempt int) string {
	return fmt.Sprintf("candidates/provider=massive/symbol=%s/date=%s/candidate=%s.parquet", symbol, date, stableID("candidate", symbol, date, strconv.Itoa(attempt)))
}

func quarantineObjectKey(symbol, date string, attempt int) string {
	return fmt.Sprintf("quarantine/provider=massive/symbol=%s/date=%s/attempt=%d.jsonl.gz", symbol, date, attempt)
}

func wait(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func retryableUnitStatus(status string) bool {
	return status == "failed" || status == "incomplete" || status == "needs-attention"
}

func terminalUnitStatus(status string) bool {
	return status == "completed" || status == "checked-empty" || status == "incomplete" || status == "needs-attention" || status == "failed" || status == "cancelled"
}

func statusDelta(previous, next string) map[string]int64 {
	delta := map[string]int64{"finished_units": 0, "clean_units": 0, "checked_empty": 0, "incomplete_units": 0, "attention_units": 0, "failed_units": 0}
	deltaFor := func(status string, value int64) {
		if terminalUnitStatus(status) {
			delta["finished_units"] += value
		}
		switch status {
		case "completed":
			delta["clean_units"] += value
		case "checked-empty":
			delta["checked_empty"] += value
		case "incomplete":
			delta["incomplete_units"] += value
		case "needs-attention":
			delta["attention_units"] += value
		case "failed":
			delta["failed_units"] += value
		}
	}
	deltaFor(previous, -1)
	deltaFor(next, 1)
	return delta
}

func statusForCounters(state jobState, delta map[string]int64) string {
	finished := state.FinishedUnits + delta["finished_units"]
	if finished < int64(state.Job.Units) {
		return "running"
	}
	if state.FailedUnits+delta["failed_units"] > 0 {
		return "failed"
	}
	if state.AttentionUnits+delta["attention_units"] > 0 {
		return "needs-attention"
	}
	if state.IncompleteUnits+delta["incomplete_units"] > 0 {
		return "incomplete"
	}
	if state.CheckedEmptyUnits+delta["checked_empty"] == int64(state.Job.Units) {
		return "checked-empty"
	}
	return "completed"
}

func unitItem(unit unitState) map[string]dynamodbtypes.AttributeValue {
	return map[string]dynamodbtypes.AttributeValue{
		"pk":             stringValue(unitPartitionBase + unit.JobID),
		"sk":             stringValue(unitKey(unit.Symbol, unit.Date)),
		"entity":         stringValue("unit"),
		"job_id":         stringValue(unit.JobID),
		"symbol":         stringValue(unit.Symbol),
		"date":           stringValue(unit.Date),
		"status":         stringValue(unit.Status),
		"attempts":       numberValue(unit.Attempts),
		"rows":           numberValue(unit.Rows),
		"invalid_rows":   numberValue(unit.InvalidRows),
		"error":          stringValue(unit.Error),
		"object_key":     stringValue(""),
		"candidate_key":  stringValue(""),
		"quarantine_key": stringValue(""),
	}
}

type jobState struct {
	Job               ingestion.Job
	FinishedUnits     int64
	CleanUnits        int64
	CheckedEmptyUnits int64
	IncompleteUnits   int64
	AttentionUnits    int64
	FailedUnits       int64
	Start             string
	End               string
	Source            string
	CreatedAt         string
}

func jobFromItem(item map[string]dynamodbtypes.AttributeValue) jobState {
	state := jobState{Job: ingestion.Job{
		ID:             stringAttribute(item, "id"),
		Status:         stringAttribute(item, "status"),
		Provider:       stringAttribute(item, "provider"),
		ListID:         stringAttribute(item, "list_id"),
		Symbols:        int(numberAttribute(item, "symbols")),
		Units:          int(numberAttribute(item, "units")),
		CompletedUnits: int(numberAttribute(item, "finished_units")),
		Rows:           numberAttribute(item, "rows"),
		InvalidRows:    numberAttribute(item, "invalid_rows"),
		LastAttempt:    stringAttribute(item, "last_attempt"),
		LastSuccessful: stringAttribute(item, "last_successful"),
		Error:          stringAttribute(item, "error"),
		NeedsAttention: stringAttribute(item, "status") == "needs-attention",
	},
		FinishedUnits:     numberAttribute(item, "finished_units"),
		CleanUnits:        numberAttribute(item, "clean_units"),
		CheckedEmptyUnits: numberAttribute(item, "checked_empty"),
		IncompleteUnits:   numberAttribute(item, "incomplete_units"),
		AttentionUnits:    numberAttribute(item, "attention_units"),
		FailedUnits:       numberAttribute(item, "failed_units"),
		Start:             stringAttribute(item, "start"),
		End:               stringAttribute(item, "end"),
		Source:            stringAttribute(item, "source"),
		CreatedAt:         stringAttribute(item, "created_at"),
	}
	return state
}

func listFromItem(item map[string]dynamodbtypes.AttributeValue) ingestion.SymbolList {
	return ingestion.SymbolList{ID: stringAttribute(item, "id"), Name: stringAttribute(item, "name"), Symbols: stringSetAttribute(item, "symbols"), UpdatedAt: stringAttribute(item, "updated_at")}
}

func unitFromItem(item map[string]dynamodbtypes.AttributeValue) unitState {
	return unitState{JobID: stringAttribute(item, "job_id"), Symbol: stringAttribute(item, "symbol"), Date: stringAttribute(item, "date"), Status: stringAttribute(item, "status"), Attempts: int(numberAttribute(item, "attempts")), Rows: numberAttribute(item, "rows"), InvalidRows: numberAttribute(item, "invalid_rows"), Error: stringAttribute(item, "error")}
}

func scheduleFromItem(item map[string]dynamodbtypes.AttributeValue) schedule {
	var symbols []string
	_ = json.Unmarshal([]byte(stringAttribute(item, "symbols")), &symbols)
	return schedule{ID: stringAttribute(item, "id"), ListID: stringAttribute(item, "list_id"), Symbols: symbols, Enabled: boolAttribute(item, "enabled")}
}

func candidateFromItem(item map[string]dynamodbtypes.AttributeValue) candidateState {
	var omitted []string
	_ = json.Unmarshal([]byte(stringAttribute(item, "omitted")), &omitted)
	return candidateState{
		ID:                stringAttribute(item, "id"),
		JobID:             stringAttribute(item, "job_id"),
		Symbol:            stringAttribute(item, "symbol"),
		Date:              stringAttribute(item, "date"),
		CurrentGeneration: stringAttribute(item, "current_generation"),
		CurrentObjectKey:  stringAttribute(item, "current_object_key"),
		CandidateKey:      stringAttribute(item, "candidate_key"),
		Omitted:           omitted,
		Rows:              numberAttribute(item, "rows"),
		Status:            stringAttribute(item, "status"),
	}
}

func discrepancyFromItem(item map[string]dynamodbtypes.AttributeValue) ingestion.Discrepancy {
	candidate := candidateFromItem(item)
	return ingestion.Discrepancy{ID: candidate.ID, Symbol: candidate.Symbol, Date: candidate.Date, OmittedTimestamps: candidate.Omitted}
}

func key(partition, sort string) map[string]dynamodbtypes.AttributeValue {
	return map[string]dynamodbtypes.AttributeValue{"pk": stringValue(partition), "sk": stringValue(sort)}
}

func stringValue(value string) dynamodbtypes.AttributeValue {
	return &dynamodbtypes.AttributeValueMemberS{Value: value}
}

func stringSet(values []string) dynamodbtypes.AttributeValue {
	return &dynamodbtypes.AttributeValueMemberSS{Value: values}
}

func numberValue(value interface{}) dynamodbtypes.AttributeValue {
	return &dynamodbtypes.AttributeValueMemberN{Value: fmt.Sprint(value)}
}

func boolValue(value bool) dynamodbtypes.AttributeValue {
	return &dynamodbtypes.AttributeValueMemberBOOL{Value: value}
}

func jsonValue(value any) dynamodbtypes.AttributeValue {
	encoded, _ := json.Marshal(value)
	return stringValue(string(encoded))
}

func stringAttribute(item map[string]dynamodbtypes.AttributeValue, name string) string {
	value, ok := item[name].(*dynamodbtypes.AttributeValueMemberS)
	if !ok {
		return ""
	}
	return value.Value
}

func stringSetAttribute(item map[string]dynamodbtypes.AttributeValue, name string) []string {
	value, ok := item[name].(*dynamodbtypes.AttributeValueMemberSS)
	if !ok {
		return nil
	}
	return append([]string(nil), value.Value...)
}

func numberAttribute(item map[string]dynamodbtypes.AttributeValue, name string) int64 {
	value, ok := item[name].(*dynamodbtypes.AttributeValueMemberN)
	if !ok {
		return 0
	}
	parsed, _ := strconv.ParseInt(value.Value, 10, 64)
	return parsed
}

func boolAttribute(item map[string]dynamodbtypes.AttributeValue, name string) bool {
	value, ok := item[name].(*dynamodbtypes.AttributeValueMemberBOOL)
	return ok && value.Value
}

func isConditionalFailure(err error) bool {
	var target *dynamodbtypes.ConditionalCheckFailedException
	return errors.As(err, &target)
}
