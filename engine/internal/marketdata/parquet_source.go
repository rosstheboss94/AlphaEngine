package marketdata

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/parquet-go/parquet-go"
)

// parquetMinute mirrors the supported sample's required fields. Pointer
// fields preserve null-vs-zero so null values are rejected by shared loading
// validation rather than being silently treated as zero.
type parquetMinute struct {
	Open    *float64   `parquet:"open,optional"`
	High    *float64   `parquet:"high,optional"`
	Low     *float64   `parquet:"low,optional"`
	Close   *float64   `parquet:"close,optional"`
	Volume  *uint64    `parquet:"volume,optional,uint(64)"`
	Symbol  *string    `parquet:"symbol,optional"`
	TsEvent *time.Time `parquet:"ts_event,optional,timestamp(nanosecond:utc)"`
}

type parquetSource struct {
	path       string
	file       *os.File
	reader     *parquet.GenericReader[parquetMinute]
	buffer     []parquetMinute
	bufferPos  int
	rowIndex   int64
	pendingErr error
	closed     bool
}

// OpenParquet opens one local Parquet source after checking its required
// physical and timestamp schema. The returned source must be closed by Load
// or by the caller.
func OpenParquet(path string) (MinuteSource, error) {
	if strings.TrimSpace(path) == "" {
		return nil, &LoadError{Path: path, Row: -1, Reason: "source path is empty"}
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, &LoadError{Path: path, Row: -1, Reason: "open parquet file", Err: err}
	}
	schemaReader := parquet.NewReader(file)
	if err := validateSchema(schemaReader.Schema()); err != nil {
		_ = file.Close()
		return nil, &LoadError{Path: path, Row: -1, Reason: "unsupported parquet schema", Err: err}
	}
	reader := parquet.NewGenericReader[parquetMinute](file)

	return &parquetSource{
		path:   path,
		file:   file,
		reader: reader,
		buffer: make([]parquetMinute, 0, 256),
	}, nil
}

func (s *parquetSource) Next() (RawMinute, error) {
	if s.closed {
		return RawMinute{}, &LoadError{Path: s.path, Row: s.rowIndex, Reason: "read from closed source"}
	}
	for {
		if s.bufferPos < len(s.buffer) {
			row := s.buffer[s.bufferPos]
			s.buffer[s.bufferPos] = parquetMinute{}
			s.bufferPos++
			index := s.rowIndex
			s.rowIndex++
			return RawMinute{
				Symbol:        row.Symbol,
				IntervalStart: row.TsEvent,
				Open:          row.Open,
				High:          row.High,
				Low:           row.Low,
				Close:         row.Close,
				Volume:        row.Volume,
				SourceFile:    s.path,
				SourceRow:     index,
			}, nil
		}

		if s.pendingErr != nil {
			err := s.pendingErr
			s.pendingErr = nil
			if errors.Is(err, io.EOF) {
				return RawMinute{}, io.EOF
			}
			return RawMinute{}, &LoadError{Path: s.path, Row: s.rowIndex, Reason: "read parquet rows", Err: err}
		}

		s.buffer = s.buffer[:cap(s.buffer)]
		n, readErr := s.reader.Read(s.buffer)
		if n > 0 {
			s.buffer = s.buffer[:n]
			s.bufferPos = 0
			if readErr != nil {
				s.pendingErr = readErr
			}
			continue
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				return RawMinute{}, io.EOF
			}
			return RawMinute{}, &LoadError{Path: s.path, Row: s.rowIndex, Reason: "read parquet rows", Err: readErr}
		}
		return RawMinute{}, &LoadError{Path: s.path, Row: s.rowIndex, Reason: "reader made no progress"}
	}
}

func (s *parquetSource) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	readerErr := s.reader.Close()
	fileErr := s.file.Close()
	if readerErr != nil {
		return readerErr
	}
	return fileErr
}

func validateSchema(schema *parquet.Schema) error {
	type expected struct {
		kind parquet.Kind
	}
	columns := map[string]expected{
		"open": {kind: parquet.Double}, "high": {kind: parquet.Double},
		"low": {kind: parquet.Double}, "close": {kind: parquet.Double},
		"volume": {kind: parquet.Int64}, "symbol": {kind: parquet.ByteArray},
		"ts_event": {kind: parquet.Int64},
	}
	for name, want := range columns {
		column, ok := schema.Lookup(name)
		if !ok {
			return fmt.Errorf("missing required column %q", name)
		}
		if column.Node.Type().Kind() != want.kind {
			return fmt.Errorf("column %q has physical type %s, want %s", name, column.Node.Type().Kind(), want.kind)
		}
	}
	ts, _ := schema.Lookup("ts_event")
	tsType := strings.ToLower(ts.Node.Type().String())
	if !strings.Contains(tsType, "timestamp") || !strings.Contains(tsType, "nano") || !strings.Contains(tsType, "true") {
		return fmt.Errorf("column %q must be a UTC nanosecond timestamp, got %s", "ts_event", ts.Node.Type())
	}
	return nil
}
