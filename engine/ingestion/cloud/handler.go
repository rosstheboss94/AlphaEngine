package cloudingestion

import (
	"bytes"
	"compress/gzip"
	"context"
	cryptorand "crypto/rand"
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
	"github.com/aws/aws-sdk-go-v2/service/sfn"
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
	generationAbsent        = "__no_current_generation__"
	providerRequestInterval = 15 * time.Second
	leaseDuration           = 3 * time.Minute
)

// Handler owns one Lambda's AWS clients. A handler can be reused across warm
// invocations without reloading the AWS configuration.
type Handler struct {
	cfg     Config
	ddb     *dynamodb.Client
	s3      *s3.Client
	sqs     *sqs.Client
	sfn     *sfn.Client
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
		sfn:     sfn.NewFromConfig(awsCfg),
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
	requestKey := strings.TrimSpace(request.IdempotencyKey)
	if requestKey == "" {
		requestKey, err = randomID("request")
		if err != nil {
			return ingestion.Job{}, err
		}
	}
	if len(requestKey) > 256 {
		return ingestion.Job{}, errors.New("idempotency key is too long")
	}
	jobID := stableID("job", requestKey)
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
		"idempotency_key":  stringValue(requestKey),
		"manifest_key":     stringValue(manifestObjectKey(jobID)),
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
	if err := h.startUnitDispatch(ctx, jobID, messages); err != nil {
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
		request := ingestion.BackfillRequest{ListID: schedule.ListID, Symbols: schedule.Symbols, Start: start, End: end, IdempotencyKey: stableID("schedule-run", schedule.ID, start, end)}
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
		fmt.Sprintf("staging/provider=massive/symbol=%s/date=%s/", symbol, date),
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
		output, err := h.s3.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(h.cfg.Bucket), Delete: &s3types.Delete{Objects: identifiers, Quiet: aws.Bool(true)},
		})
		if err != nil {
			return fmt.Errorf("delete S3 objects under %q: %w", prefix, err)
		}
		if len(output.Errors) > 0 {
			failure := output.Errors[0]
			return fmt.Errorf("delete S3 objects under %q: object %q failed with %s: %s", prefix, aws.ToString(failure.Key), aws.ToString(failure.Code), aws.ToString(failure.Message))
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
	if job.Job.Status == "cancelled" || job.Job.Status == "failed" {
		return nil
	}
	unit, found, err := h.getUnit(ctx, message.JobID, message.Symbol, message.Date)
	if err != nil {
		return err
	}
	if !found || terminalUnitStatus(unit.Status) {
		return nil
	}
	if unit.NotBefore > h.now().UTC().UnixMilli() {
		delay := time.Duration(unit.NotBefore-h.now().UTC().UnixMilli()) * time.Millisecond
		return h.enqueueWithDelay(ctx, []UnitMessage{message}, delay)
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
		return h.finishUnit(ctx, unit, "cancelled", unit.Rows, unit.InvalidRows, "job is no longer active", "", "", "")
	}
	dataItem, err := h.getDataItem(ctx, message.Symbol, message.Date)
	if err != nil {
		return h.retryUnit(ctx, unit, err)
	}
	if stringAttribute(dataItem, "status") == "deleted" {
		if job.Source == "manual" && deletionPredatesJob(dataItem, job.CreatedAt) {
			cleared, err := h.clearExclusion(ctx, message.Symbol, message.Date, stringAttribute(dataItem, "updated_at"))
			if err != nil {
				return h.retryUnit(ctx, unit, err)
			}
			if !cleared {
				return h.finishUnit(ctx, unit, "cancelled", unit.Rows, unit.InvalidRows, "a newer deletion exclusion was recorded", "", "", "")
			}
			dataItem = nil
		} else {
			return h.finishUnit(ctx, unit, "cancelled", unit.Rows, unit.InvalidRows, "unit is excluded by an explicit deletion", "", "", "")
		}
	}
	currentGeneration := ""
	if stringAttribute(dataItem, "status") == "current" {
		currentGeneration = stringAttribute(dataItem, "generation")
	}
	if unit.ExpectedGeneration == generationAbsent && currentGeneration != "" {
		return h.finishUnit(ctx, unit, "needs-attention", unit.Rows, unit.InvalidRows, "current data appeared while this unit was in progress", "", "", "")
	}
	if unit.ExpectedGeneration != "" && unit.ExpectedGeneration != generationAbsent && currentGeneration != unit.ExpectedGeneration {
		return h.finishUnit(ctx, unit, "needs-attention", unit.Rows, unit.InvalidRows, "current data changed while this unit was in progress", "", "", "")
	}
	if unit.ExpectedGeneration == "" {
		if currentGeneration == "" {
			unit.ExpectedGeneration = generationAbsent
		} else {
			unit.ExpectedGeneration = currentGeneration
		}
	}
	key, err := h.massiveAPIKey(ctx)
	if err != nil {
		return h.finishProviderError(ctx, UnitMessage{JobID: message.JobID, Symbol: message.Symbol, Date: message.Date}, unit, err)
	}
	location, _ := time.LoadLocation("America/New_York")
	date, _ := time.ParseInLocation("2006-01-02", message.Date, location)
	previous := time.Time{}
	if unit.PreviousTS != 0 {
		previous = time.Unix(0, unit.PreviousTS).UTC()
	}
	pageNumber := unit.PageNumber + 1
	if err := h.waitForProviderSlot(ctx); err != nil {
		return h.retryUnit(ctx, unit, err)
	}
	result, err := ingestion.NewMassiveClient(key, nil).FetchPage(ctx, ingestion.PageRequest{
		FetchRequest: ingestion.FetchRequest{Symbol: message.Symbol, StartDate: date, EndDate: date, Adjusted: false},
		Cursor:       unit.Cursor,
		Previous:     previous,
		PageNumber:   pageNumber,
	})
	if err != nil {
		return h.finishProviderError(ctx, message, unit, err)
	}
	stageKey := stagePageKey(message.JobID, message.Symbol, message.Date, pageNumber, unit.LeaseToken)
	if err := h.putFencedStagedPage(ctx, unit, stageKey, stagedPage{Bars: result.Bars, Issues: result.Issues}); err != nil {
		return h.retryUnit(ctx, unit, err)
	}
	if result.NextCursor != "" && result.NextCursor == unit.Cursor {
		return h.finishProviderError(ctx, message, unit, &ingestion.ProviderError{Kind: "schema", Message: "pagination cursor repeated"})
	}
	if result.NextCursor != "" {
		if err := h.checkpointPage(ctx, unit, stageKey, result.NextCursor, pageNumber, result.Bars, result.Issues); err != nil {
			return err
		}
		return h.enqueue(ctx, []UnitMessage{message})
	}
	stageKeys := append(append([]string(nil), unit.StageKeys...), stageKey)
	allBars, allIssues, err := h.readStagedPages(ctx, stageKeys)
	if err != nil {
		return h.retryUnit(ctx, unit, err)
	}
	quarantineKey := ""
	if len(allIssues) > 0 {
		quarantineKey = quarantineObjectKey(message.JobID, message.Symbol, message.Date, unit.LeaseToken)
		if err := h.putFencedQuarantine(ctx, unit, quarantineKey, allIssues); err != nil {
			return h.retryUnit(ctx, unit, err)
		}
	}
	if len(allBars) == 0 {
		if len(allIssues) == 0 {
			return h.finishUnit(ctx, unit, "checked-empty", 0, 0, "", "", "", quarantineKey)
		}
		return h.finishUnit(ctx, unit, "needs-attention", 0, int64(len(allIssues)), "all returned rows were invalid", "", "", quarantineKey)
	}
	if len(allIssues) == 0 && job.Source == "schedule" && stringAttribute(dataItem, "status") == "current" {
		omitted, err := h.omittedTimestamps(ctx, stringAttribute(dataItem, "object_key"), allBars)
		if err != nil {
			return h.retryUnit(ctx, unit, err)
		}
		if len(omitted) > 0 {
			candidateKey := candidateObjectKey(message.JobID, message.Symbol, message.Date, unit.LeaseToken)
			if err := h.putFencedParquet(ctx, unit, candidateKey, allBars); err != nil {
				return h.retryUnit(ctx, unit, err)
			}
			candidateID := stableID("discrepancy", message.JobID, message.Symbol, message.Date, stringAttribute(dataItem, "generation"))
			if err := h.recordCandidate(ctx, unit, candidateState{
				ID: candidateID, JobID: message.JobID, Symbol: message.Symbol, Date: message.Date,
				CurrentGeneration: stringAttribute(dataItem, "generation"), CurrentObjectKey: stringAttribute(dataItem, "object_key"),
				CandidateKey: candidateKey, Omitted: omitted, Rows: int64(len(allBars)),
			}); err != nil {
				return h.retryUnit(ctx, unit, err)
			}
			return h.finishUnit(ctx, unit, "needs-attention", int64(len(allBars)), 0, "refresh omitted timestamps; explicit review is required", "", candidateKey, "")
		}
	}
	if len(allIssues) > 0 {
		candidateKey := candidateObjectKey(message.JobID, message.Symbol, message.Date, unit.LeaseToken)
		if err := h.putFencedParquet(ctx, unit, candidateKey, allBars); err != nil {
			return h.retryUnit(ctx, unit, err)
		}
		return h.finishUnit(ctx, unit, "incomplete", int64(len(allBars)), int64(len(allIssues)), "invalid rows were quarantined; candidate was not published", "", candidateKey, quarantineKey)
	}
	objectKey := publishedObjectKey(message.JobID, message.Symbol, message.Date, unit.LeaseToken)
	if err := h.publish(ctx, unit, message.Symbol, message.Date, objectKey, allBars); err != nil {
		if errors.Is(err, errPublicationFence) {
			latestJob, jobFound, jobErr := h.getJob(ctx, message.JobID)
			if jobErr != nil {
				return h.retryUnit(ctx, unit, jobErr)
			}
			if jobFound && latestJob.Job.Status == "cancelled" {
				return h.finishUnit(ctx, unit, "cancelled", unit.Rows, unit.InvalidRows, "job was cancelled before publication", "", "", "")
			}
			latestData, dataErr := h.getDataItem(ctx, message.Symbol, message.Date)
			if dataErr != nil {
				return h.retryUnit(ctx, unit, dataErr)
			}
			if stringAttribute(latestData, "status") == "deleted" || (unit.ExpectedGeneration == generationAbsent && stringAttribute(latestData, "status") == "current") || (unit.ExpectedGeneration != generationAbsent && stringAttribute(latestData, "generation") != "" && stringAttribute(latestData, "generation") != unit.ExpectedGeneration) {
				return h.finishUnit(ctx, unit, "needs-attention", int64(len(allBars)), 0, "current data changed before publication; explicit review is required", "", "", "")
			}
		}
		return h.retryUnit(ctx, unit, err)
	}
	return h.finishUnit(ctx, unit, "completed", int64(len(allBars)), 0, "", objectKey, "", "")
}

type stagedPage struct {
	Bars   []ingestion.NormalizedBar `json:"bars"`
	Issues []ingestion.RowIssue      `json:"issues"`
}

func (h *Handler) putStagedPage(ctx context.Context, objectKey string, page stagedPage) error {
	var buffer bytes.Buffer
	writer := gzip.NewWriter(&buffer)
	if err := json.NewEncoder(writer).Encode(page); err != nil {
		_ = writer.Close()
		return fmt.Errorf("encode staged page: %w", err)
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("close staged page: %w", err)
	}
	if _, err := h.s3.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(h.cfg.Bucket), Key: aws.String(objectKey), Body: bytes.NewReader(buffer.Bytes()),
		ContentType: aws.String("application/json"), ContentEncoding: aws.String("gzip"),
	}); err != nil {
		return fmt.Errorf("write staged page %q: %w", objectKey, err)
	}
	return nil
}

func (h *Handler) putFencedStagedPage(ctx context.Context, unit unitState, objectKey string, page stagedPage) error {
	if err := h.artifactFence(ctx, unit); err != nil {
		return err
	}
	if err := h.putStagedPage(ctx, objectKey, page); err != nil {
		return err
	}
	if err := h.artifactFence(ctx, unit); err != nil {
		_, _ = h.s3.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(h.cfg.Bucket), Key: aws.String(objectKey)})
		return err
	}
	return nil
}

func (h *Handler) readStagedPages(ctx context.Context, stageKeys []string) ([]ingestion.NormalizedBar, []ingestion.RowIssue, error) {
	if len(stageKeys) < 1 {
		return nil, nil, errors.New("staged page count must be positive")
	}
	bars := make([]ingestion.NormalizedBar, 0)
	issues := make([]ingestion.RowIssue, 0)
	for pageNumber, stageKey := range stageKeys {
		object, err := h.s3.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(h.cfg.Bucket), Key: aws.String(stageKey)})
		if err != nil {
			return nil, nil, fmt.Errorf("read staged page %d: %w", pageNumber, err)
		}
		reader, err := gzip.NewReader(object.Body)
		if err != nil {
			_ = object.Body.Close()
			return nil, nil, fmt.Errorf("open staged page %d: %w", pageNumber, err)
		}
		var page stagedPage
		decodeErr := json.NewDecoder(reader).Decode(&page)
		closeReaderErr := reader.Close()
		closeBodyErr := object.Body.Close()
		if decodeErr != nil {
			return nil, nil, fmt.Errorf("decode staged page %d: %w", pageNumber, decodeErr)
		}
		if closeReaderErr != nil || closeBodyErr != nil {
			return nil, nil, fmt.Errorf("close staged page %d: %v", pageNumber, firstError(closeReaderErr, closeBodyErr))
		}
		bars = append(bars, page.Bars...)
		issues = append(issues, page.Issues...)
	}
	return bars, issues, nil
}

func firstError(first, second error) error {
	if first != nil {
		return first
	}
	return second
}

func (h *Handler) checkpointPage(ctx context.Context, unit unitState, stageKey, cursor string, pageNumber int, bars []ingestion.NormalizedBar, issues []ingestion.RowIssue) error {
	rows := unit.Rows + int64(len(bars))
	invalidRows := unit.InvalidRows + int64(len(issues))
	previousTS := unit.PreviousTS
	if len(bars) > 0 {
		previousTS = bars[len(bars)-1].IntervalStart.UnixNano()
	}
	stageKeys := append(append([]string(nil), unit.StageKeys...), stageKey)
	now := h.now().UTC()
	_, err := h.ddb.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:                aws.String(h.cfg.TableName),
		Key:                      key(unitPartitionBase+unit.JobID, unitKey(unit.Symbol, unit.Date)),
		UpdateExpression:         aws.String("SET #status = :waiting, cursor = :cursor, page_number = :page, previous_ts = :previous, rows = :rows, invalid_rows = :invalid, staged_pages = :staged_pages, expected_generation = :expected_generation, not_before = :zero, lease_token = :empty, lease_expires_at = :zero, updated_at = :updated"),
		ConditionExpression:      aws.String("#status = :running AND lease_token = :lease"),
		ExpressionAttributeNames: map[string]string{"#status": "status"},
		ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
			":waiting": stringValue("waiting"), ":cursor": stringValue(cursor), ":page": numberValue(pageNumber),
			":previous": numberValue(previousTS), ":rows": numberValue(rows), ":invalid": numberValue(invalidRows),
			":staged_pages":        jsonValue(stageKeys),
			":expected_generation": stringValue(unit.ExpectedGeneration),
			":empty":               stringValue(""), ":zero": numberValue(0), ":updated": stringValue(now.Format(time.RFC3339)),
			":lease": stringValue(unit.LeaseToken),
		},
	})
	if err != nil {
		if isConditionalFailure(err) {
			return nil
		}
		return fmt.Errorf("checkpoint ingestion page: %w", err)
	}
	return nil
}

func (h *Handler) finishProviderError(ctx context.Context, message UnitMessage, unit unitState, providerErr error) error {
	var typed *ingestion.ProviderError
	if errors.As(providerErr, &typed) && typed.Retryable() && unit.Attempts < maxAttempts {
		return h.retryWithDelay(ctx, message, unit, providerErr, typed.RetryAfter)
	}
	status := "needs-attention"
	if errors.As(providerErr, &typed) && typed.Retryable() {
		status = "failed"
	}
	return h.finishUnit(ctx, unit, status, unit.Rows, unit.InvalidRows, providerErr.Error(), "", "", "")
}

func (h *Handler) retryUnit(ctx context.Context, unit unitState, cause error) error {
	if unit.Attempts >= maxAttempts {
		return h.finishUnit(ctx, unit, "failed", unit.Rows, unit.InvalidRows, cause.Error(), "", "", "")
	}
	return h.retryWithDelay(ctx, UnitMessage{JobID: unit.JobID, Symbol: unit.Symbol, Date: unit.Date}, unit, cause, 0)
}

func (h *Handler) retryWithDelay(ctx context.Context, message UnitMessage, unit unitState, cause error, retryAfter time.Duration) error {
	if unit.Attempts >= maxAttempts {
		return h.finishUnit(ctx, unit, "failed", unit.Rows, unit.InvalidRows, cause.Error(), "", "", "")
	}
	delay := jitteredRetryDelay(retryDelay(unit.Attempts))
	if retryAfter > delay {
		delay = retryAfter
	}
	notBefore := h.now().UTC().Add(delay).UnixMilli()
	if err := h.releaseUnit(ctx, unit, cause.Error(), notBefore); err != nil {
		return err
	}
	if err := h.enqueueWithDelay(ctx, []UnitMessage{message}, delay); err != nil {
		return err
	}
	return nil
}

func (h *Handler) releaseUnit(ctx context.Context, unit unitState, message string, notBefore int64) error {
	_, err := h.ddb.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(h.cfg.TableName), Key: key(unitPartitionBase+unit.JobID, unitKey(unit.Symbol, unit.Date)),
		UpdateExpression:         aws.String("SET #status = :waiting, #error = :error, not_before = :not_before, lease_token = :empty, lease_expires_at = :zero, updated_at = :updated"),
		ConditionExpression:      aws.String("#status = :running AND lease_token = :lease"),
		ExpressionAttributeNames: map[string]string{"#status": "status", "#error": "error"},
		ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
			":waiting": stringValue("waiting"), ":error": stringValue(message), ":empty": stringValue(""),
			":zero": numberValue(0), ":not_before": numberValue(notBefore), ":updated": stringValue(h.now().UTC().Format(time.RFC3339)), ":lease": stringValue(unit.LeaseToken),
		},
	})
	if err != nil && !isConditionalFailure(err) {
		return fmt.Errorf("release ingestion unit: %w", err)
	}
	return nil
}

func retryDelay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	delay := 15 * time.Second
	for index := 1; index < attempt; index++ {
		if delay >= 15*time.Minute/2 {
			return 15 * time.Minute
		}
		delay *= 2
	}
	if delay > 15*time.Minute {
		return 15 * time.Minute
	}
	return delay
}

func jitteredRetryDelay(base time.Duration) time.Duration {
	if base <= 0 {
		return 0
	}
	var sample [1]byte
	if _, err := cryptorand.Read(sample[:]); err != nil {
		return base
	}
	return base + time.Duration(sample[0]%26)*base/100
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

func (h *Handler) putFencedParquet(ctx context.Context, unit unitState, objectKey string, bars []ingestion.NormalizedBar) error {
	if err := h.artifactFence(ctx, unit); err != nil {
		return err
	}
	if err := h.putParquet(ctx, objectKey, bars); err != nil {
		return err
	}
	if err := h.artifactFence(ctx, unit); err != nil {
		_, _ = h.s3.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(h.cfg.Bucket), Key: aws.String(objectKey)})
		return err
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

func (h *Handler) putFencedQuarantine(ctx context.Context, unit unitState, objectKey string, issues []ingestion.RowIssue) error {
	if err := h.artifactFence(ctx, unit); err != nil {
		return err
	}
	if err := h.putQuarantine(ctx, objectKey, issues); err != nil {
		return err
	}
	if err := h.artifactFence(ctx, unit); err != nil {
		_, _ = h.s3.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(h.cfg.Bucket), Key: aws.String(objectKey)})
		return err
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

func (h *Handler) recordCandidate(ctx context.Context, unit unitState, candidate candidateState) error {
	item := map[string]dynamodbtypes.AttributeValue{
		"pk": stringValue("CANDIDATE#" + candidate.ID), "sk": stringValue(currentSortKey),
		"gsi1pk": stringValue(unitPartitionBase + candidate.JobID), "gsi1sk": stringValue("CANDIDATE#" + candidate.ID),
		"entity": stringValue("discrepancy_candidate"), "id": stringValue(candidate.ID), "job_id": stringValue(candidate.JobID),
		"symbol": stringValue(candidate.Symbol), "date": stringValue(candidate.Date),
		"current_generation": stringValue(candidate.CurrentGeneration), "current_object_key": stringValue(candidate.CurrentObjectKey),
		"candidate_key": stringValue(candidate.CandidateKey), "omitted": jsonValue(candidate.Omitted), "rows": numberValue(candidate.Rows),
		"status": stringValue("pending"), "created_at": stringValue(h.now().UTC().Format(time.RFC3339)),
	}
	transactItems := append(h.artifactFenceItems(unit), dynamodbtypes.TransactWriteItem{Put: &dynamodbtypes.Put{
		TableName: aws.String(h.cfg.TableName), Item: item,
	}})
	if _, err := h.ddb.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: transactItems}); err != nil {
		if isConditionalFailure(err) || isTransactionCanceled(err) {
			_, _ = h.s3.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(h.cfg.Bucket), Key: aws.String(candidate.CandidateKey)})
			return errPublicationFence
		}
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

var errPublicationFence = errors.New("publication fence rejected")

func (h *Handler) publish(ctx context.Context, unit unitState, symbol, date, objectKey string, bars []ingestion.NormalizedBar) error {
	if err := h.putParquet(ctx, objectKey, bars); err != nil {
		return err
	}
	now := h.now().UTC()
	item := map[string]dynamodbtypes.AttributeValue{
		"pk":         stringValue(dataKey(symbol, date)),
		"sk":         stringValue(currentSortKey),
		"entity":     stringValue("current_data"),
		"status":     stringValue("current"),
		"symbol":     stringValue(symbol),
		"date":       stringValue(date),
		"object_key": stringValue(objectKey),
		"generation": stringValue(stableID("generation", objectKey)),
		"updated_at": stringValue(now.Format(time.RFC3339)),
	}
	jobValues := map[string]dynamodbtypes.AttributeValue{
		":queued": stringValue("queued"), ":running": stringValue("running"), ":waiting": stringValue("waiting"),
	}
	unitValues := map[string]dynamodbtypes.AttributeValue{
		":running": stringValue("running"), ":lease": stringValue(unit.LeaseToken), ":now": numberValue(now.UnixMilli()),
	}
	dataValues := map[string]dynamodbtypes.AttributeValue{
		":entity": stringValue("current_data"), ":current": stringValue("current"), ":symbol": stringValue(symbol),
		":date": stringValue(date), ":object_key": stringValue(objectKey), ":new_generation": item["generation"],
		":updated": stringValue(now.Format(time.RFC3339)),
	}
	dataCondition := "attribute_not_exists(#status)"
	if unit.ExpectedGeneration != "" && unit.ExpectedGeneration != generationAbsent {
		dataCondition = "#status = :current AND generation = :expected_generation"
		dataValues[":expected_generation"] = stringValue(unit.ExpectedGeneration)
	}
	_, err := h.ddb.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: []dynamodbtypes.TransactWriteItem{
		{ConditionCheck: &dynamodbtypes.ConditionCheck{
			TableName: aws.String(h.cfg.TableName), Key: key(jobPartition, jobSortPrefix+unit.JobID),
			ConditionExpression:      aws.String("#status IN (:queued, :running, :waiting)"),
			ExpressionAttributeNames: map[string]string{"#status": "status"}, ExpressionAttributeValues: jobValues,
		}},
		{ConditionCheck: &dynamodbtypes.ConditionCheck{
			TableName: aws.String(h.cfg.TableName), Key: key(unitPartitionBase+unit.JobID, unitKey(symbol, date)),
			ConditionExpression:      aws.String("#status = :running AND lease_token = :lease AND lease_expires_at > :now"),
			ExpressionAttributeNames: map[string]string{"#status": "status"}, ExpressionAttributeValues: unitValues,
		}},
		{Update: &dynamodbtypes.Update{
			TableName: aws.String(h.cfg.TableName), Key: key(dataKey(symbol, date), currentSortKey),
			UpdateExpression:    aws.String("SET entity = :entity, #status = :current, symbol = :symbol, date = :date, object_key = :object_key, generation = :new_generation, updated_at = :updated"),
			ConditionExpression: aws.String(dataCondition), ExpressionAttributeNames: map[string]string{"#status": "status"},
			ExpressionAttributeValues: dataValues,
		}},
	}})
	if err != nil {
		_, _ = h.s3.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(h.cfg.Bucket), Key: aws.String(objectKey)})
		if isConditionalFailure(err) || isTransactionCanceled(err) {
			return errPublicationFence
		}
		return fmt.Errorf("publish current data reference: %w", err)
	}
	return nil
}

func (h *Handler) artifactFence(ctx context.Context, unit unitState) error {
	_, err := h.ddb.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: h.artifactFenceItems(unit)})
	if err != nil {
		if isConditionalFailure(err) || isTransactionCanceled(err) {
			return errPublicationFence
		}
		return fmt.Errorf("check ingestion artifact fence: %w", err)
	}
	return nil
}

func (h *Handler) artifactFenceItems(unit unitState) []dynamodbtypes.TransactWriteItem {
	now := h.now().UTC()
	dataCondition := "attribute_not_exists(#status)"
	dataValues := map[string]dynamodbtypes.AttributeValue{":current": stringValue("current")}
	if unit.ExpectedGeneration != "" && unit.ExpectedGeneration != generationAbsent {
		dataCondition = "#status = :current AND generation = :expected_generation"
		dataValues[":expected_generation"] = stringValue(unit.ExpectedGeneration)
	}
	return []dynamodbtypes.TransactWriteItem{
		{ConditionCheck: &dynamodbtypes.ConditionCheck{
			TableName: aws.String(h.cfg.TableName), Key: key(jobPartition, jobSortPrefix+unit.JobID),
			ConditionExpression:      aws.String("#status IN (:queued, :running, :waiting)"),
			ExpressionAttributeNames: map[string]string{"#status": "status"}, ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
				":queued": stringValue("queued"), ":running": stringValue("running"), ":waiting": stringValue("waiting"),
			},
		}},
		{ConditionCheck: &dynamodbtypes.ConditionCheck{
			TableName: aws.String(h.cfg.TableName), Key: key(unitPartitionBase+unit.JobID, unitKey(unit.Symbol, unit.Date)),
			ConditionExpression:      aws.String("#status = :running AND lease_token = :lease AND lease_expires_at > :now"),
			ExpressionAttributeNames: map[string]string{"#status": "status"}, ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
				":running": stringValue("running"), ":lease": stringValue(unit.LeaseToken), ":now": numberValue(now.UnixMilli()),
			},
		}},
		{ConditionCheck: &dynamodbtypes.ConditionCheck{
			TableName: aws.String(h.cfg.TableName), Key: key(dataKey(unit.Symbol, unit.Date), currentSortKey),
			ConditionExpression: aws.String(dataCondition), ExpressionAttributeNames: map[string]string{"#status": "status"},
			ExpressionAttributeValues: dataValues,
		}},
	}
}

func (h *Handler) claimUnit(ctx context.Context, unit unitState) (unitState, bool, error) {
	attempt := unit.Attempts + 1
	leaseToken, err := randomID("lease")
	if err != nil {
		return unit, false, err
	}
	now := h.now().UTC()
	if _, err := h.ddb.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:                aws.String(h.cfg.TableName),
		Key:                      key(unitPartitionBase+unit.JobID, unitKey(unit.Symbol, unit.Date)),
		UpdateExpression:         aws.String("SET #status = :running, attempts = :attempts, last_attempt = :last_attempt, lease_token = :lease, lease_expires_at = :expires, updated_at = :updated"),
		ConditionExpression:      aws.String("(#status IN (:queued, :waiting) AND (attribute_not_exists(not_before) OR not_before <= :now)) OR (#status = :running AND (attribute_not_exists(lease_expires_at) OR lease_expires_at <= :now))"),
		ExpressionAttributeNames: map[string]string{"#status": "status"},
		ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
			":running":      stringValue("running"),
			":attempts":     numberValue(attempt),
			":last_attempt": stringValue(now.Format(time.RFC3339)),
			":lease":        stringValue(leaseToken),
			":expires":      numberValue(now.Add(leaseDuration).UnixMilli()),
			":updated":      stringValue(now.Format(time.RFC3339)),
			":now":          numberValue(now.UnixMilli()),
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
	unit.Status = "running"
	unit.LeaseToken = leaseToken
	unit.LeaseExpiresAt = now.Add(leaseDuration).UnixMilli()
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
		":lease":      stringValue(unit.LeaseToken),
		":empty":      stringValue(""),
		":zero":       numberValue(0),
		":object_key": stringValue(objectKey),
		":candidate":  stringValue(candidateKey),
		":quarantine": stringValue(quarantineKey),
	}
	if _, err := h.ddb.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:                 aws.String(h.cfg.TableName),
		Key:                       key(unitPartitionBase+unit.JobID, unitKey(unit.Symbol, unit.Date)),
		UpdateExpression:          aws.String("SET #status = :status, rows = :rows, invalid_rows = :invalid, #error = :error, object_key = :object_key, candidate_key = :candidate, quarantine_key = :quarantine, not_before = :zero, lease_token = :empty, lease_expires_at = :zero, updated_at = :updated"),
		ConditionExpression:       aws.String("#status = :running AND lease_token = :lease"),
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
	if previous.Status == "running" {
		delta["rows"] = rows
		delta["invalid_rows"] = invalidRows
	} else {
		delta["rows"] = rows - previous.Rows
		delta["invalid_rows"] = invalidRows - previous.InvalidRows
	}
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
		UpdateExpression:         aws.String("SET #status = :waiting, attempts = :zero, #error = :empty, object_key = :empty, candidate_key = :empty, quarantine_key = :empty, not_before = :zero, lease_token = :empty, lease_expires_at = :zero, updated_at = :updated"),
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

func (h *Handler) clearExclusion(ctx context.Context, symbol, date, deletedAt string) (bool, error) {
	_, err := h.ddb.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String(h.cfg.TableName), Key: key(dataKey(symbol, date), currentSortKey),
		ConditionExpression:       aws.String("#status = :deleted AND updated_at = :deleted_at"),
		ExpressionAttributeNames:  map[string]string{"#status": "status"},
		ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":deleted": stringValue("deleted"), ":deleted_at": stringValue(deletedAt)},
	})
	if err != nil {
		if isConditionalFailure(err) {
			return false, nil
		}
		return false, fmt.Errorf("clear data exclusion: %w", err)
	}
	return true, nil
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

func (h *Handler) startUnitDispatch(ctx context.Context, jobID string, messages []UnitMessage) error {
	manifestKey := manifestObjectKey(jobID)
	data, err := json.Marshal(messages)
	if err != nil {
		return fmt.Errorf("encode ingestion manifest: %w", err)
	}
	if _, err := h.s3.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(h.cfg.Bucket), Key: aws.String(manifestKey), Body: bytes.NewReader(data),
		ContentType: aws.String("application/json"),
	}); err != nil {
		return fmt.Errorf("write ingestion manifest: %w", err)
	}
	input, err := json.Marshal(map[string]string{"job_id": jobID, "manifest_key": manifestKey})
	if err != nil {
		return fmt.Errorf("encode ingestion workflow input: %w", err)
	}
	_, err = h.sfn.StartExecution(ctx, &sfn.StartExecutionInput{
		StateMachineArn: aws.String(h.cfg.StateMachineARN),
		Name:            aws.String(stableID("execution", jobID)),
		Input:           aws.String(string(input)),
	})
	if err != nil && !strings.Contains(err.Error(), "ExecutionAlreadyExists") {
		_, _ = h.s3.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(h.cfg.Bucket), Key: aws.String(manifestKey)})
		return fmt.Errorf("start ingestion workflow: %w", err)
	}
	return nil
}

func (h *Handler) enqueue(ctx context.Context, messages []UnitMessage) error {
	return h.enqueueWithDelay(ctx, messages, 0)
}

func (h *Handler) enqueueWithDelay(ctx context.Context, messages []UnitMessage, delay time.Duration) error {
	if delay < 0 {
		delay = 0
	}
	if delay > 15*time.Minute {
		delay = 15 * time.Minute
	}
	delaySeconds := int32(delay / time.Second)
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
			entry := sqstypes.SendMessageBatchRequestEntry{Id: aws.String(strconv.Itoa(index)), MessageBody: aws.String(string(body)), DelaySeconds: delaySeconds}
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

func randomID(prefix string) (string, error) {
	bytesValue := make([]byte, 16)
	if _, err := cryptorand.Read(bytesValue); err != nil {
		return "", fmt.Errorf("generate %s id: %w", prefix, err)
	}
	return prefix + "-" + hex.EncodeToString(bytesValue), nil
}

func dataKey(symbol, date string) string { return dataPartitionBase + symbol + "#" + date }

func unitKey(symbol, date string) string { return unitSortPrefix + date + "#" + symbol }

func publishedObjectKey(jobID, symbol, date, leaseToken string) string {
	return fmt.Sprintf("data/provider=massive/symbol=%s/date=%s/execution=%s/lease=%s.parquet", symbol, date, jobID, leaseToken)
}

func candidateObjectKey(jobID, symbol, date, leaseToken string) string {
	return fmt.Sprintf("candidates/provider=massive/symbol=%s/date=%s/execution=%s/lease=%s.parquet", symbol, date, jobID, leaseToken)
}

func quarantineObjectKey(jobID, symbol, date, leaseToken string) string {
	return fmt.Sprintf("quarantine/provider=massive/symbol=%s/date=%s/execution=%s/lease=%s.jsonl.gz", symbol, date, jobID, leaseToken)
}

func stagePageKey(jobID, symbol, date string, page int, leaseToken string) string {
	return fmt.Sprintf("staging/provider=massive/symbol=%s/date=%s/job=%s/page=%06d/lease=%s.json.gz", symbol, date, jobID, page, leaseToken)
}

func manifestObjectKey(jobID string) string {
	return fmt.Sprintf("manifests/provider=massive/job=%s/units.json", jobID)
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
		"pk":                  stringValue(unitPartitionBase + unit.JobID),
		"sk":                  stringValue(unitKey(unit.Symbol, unit.Date)),
		"entity":              stringValue("unit"),
		"job_id":              stringValue(unit.JobID),
		"symbol":              stringValue(unit.Symbol),
		"date":                stringValue(unit.Date),
		"status":              stringValue(unit.Status),
		"attempts":            numberValue(unit.Attempts),
		"rows":                numberValue(unit.Rows),
		"invalid_rows":        numberValue(unit.InvalidRows),
		"error":               stringValue(unit.Error),
		"cursor":              stringValue(unit.Cursor),
		"page_number":         numberValue(unit.PageNumber),
		"previous_ts":         numberValue(unit.PreviousTS),
		"staged_pages":        jsonValue(unit.StageKeys),
		"not_before":          numberValue(unit.NotBefore),
		"lease_token":         stringValue(unit.LeaseToken),
		"lease_expires_at":    numberValue(unit.LeaseExpiresAt),
		"expected_generation": stringValue(unit.ExpectedGeneration),
		"object_key":          stringValue(""),
		"candidate_key":       stringValue(""),
		"quarantine_key":      stringValue(""),
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
	return unitState{
		JobID: stringAttribute(item, "job_id"), Symbol: stringAttribute(item, "symbol"), Date: stringAttribute(item, "date"),
		Status: stringAttribute(item, "status"), Attempts: int(numberAttribute(item, "attempts")), Rows: numberAttribute(item, "rows"),
		InvalidRows: numberAttribute(item, "invalid_rows"), Error: stringAttribute(item, "error"), Cursor: stringAttribute(item, "cursor"),
		PageNumber: int(numberAttribute(item, "page_number")), PreviousTS: numberAttribute(item, "previous_ts"), LeaseToken: stringAttribute(item, "lease_token"),
		StageKeys: stringSliceJSONAttribute(item, "staged_pages"), NotBefore: numberAttribute(item, "not_before"), LeaseExpiresAt: numberAttribute(item, "lease_expires_at"),
		ExpectedGeneration: stringAttribute(item, "expected_generation"),
	}
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

func stringSliceJSONAttribute(item map[string]dynamodbtypes.AttributeValue, name string) []string {
	value := stringAttribute(item, name)
	if value == "" {
		return nil
	}
	var result []string
	if json.Unmarshal([]byte(value), &result) != nil {
		return nil
	}
	return result
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

func isTransactionCanceled(err error) bool {
	var target *dynamodbtypes.TransactionCanceledException
	return errors.As(err, &target)
}
