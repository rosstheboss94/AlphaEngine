package marketdata

import (
	"encoding/csv"
	"errors"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
)

var canonicalCSVHeader = []string{"symbol", "ts_event", "open", "high", "low", "close", "volume"}

type csvSource struct {
	path     string
	file     *os.File
	reader   *csv.Reader
	rowIndex int64
	closed   bool
}

// OpenCSV opens one local CSV source. The first row must be the exact
// canonical header; values are parsed strictly and timestamps must carry an
// explicit UTC offset.
func OpenCSV(path string) (MinuteSource, error) {
	if strings.TrimSpace(path) == "" {
		return nil, &LoadError{Path: path, Row: -1, Reason: "source path is empty"}
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, &LoadError{Path: path, Row: -1, Reason: "open csv file", Err: err}
	}
	reader := csv.NewReader(file)
	header, err := reader.Read()
	if err != nil {
		_ = file.Close()
		if errors.Is(err, io.EOF) {
			return nil, &LoadError{Path: path, Row: -1, Column: "header", Reason: "csv header is missing"}
		}
		return nil, &LoadError{Path: path, Row: -1, Column: "header", Reason: "read csv header", Err: err}
	}
	if !sameStrings(header, canonicalCSVHeader) {
		_ = file.Close()
		return nil, &LoadError{Path: path, Row: -1, Column: "header", Reason: "unsupported csv header"}
	}
	reader.FieldsPerRecord = len(canonicalCSVHeader)

	return &csvSource{path: path, file: file, reader: reader}, nil
}

func (s *csvSource) Next() (RawMinute, error) {
	if s.closed {
		return RawMinute{}, &LoadError{Path: s.path, Row: s.rowIndex, Reason: "read from closed source"}
	}
	record, err := s.reader.Read()
	if errors.Is(err, io.EOF) {
		return RawMinute{}, io.EOF
	}
	if err != nil {
		return RawMinute{}, &LoadError{Path: s.path, Row: s.rowIndex, Reason: "read csv row", Err: err}
	}
	index := s.rowIndex
	s.rowIndex++
	return parseCSVRecord(s.path, index, record)
}

func (s *csvSource) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	return s.file.Close()
}

func parseCSVRecord(path string, index int64, record []string) (RawMinute, error) {
	row := RawMinute{SourceFile: path, SourceRow: index}
	var err error
	if row.Symbol, err = parseOptionalString(record[0]); err != nil {
		return RawMinute{}, csvFieldError(path, index, "symbol", "invalid string", err)
	}
	if row.IntervalStart, err = parseOptionalTimestamp(record[1]); err != nil {
		return RawMinute{}, csvFieldError(path, index, "ts_event", "invalid UTC timestamp", err)
	}
	if row.Open, err = parseOptionalFloat(record[2]); err != nil {
		return RawMinute{}, csvFieldError(path, index, "open", "invalid number", err)
	}
	if row.High, err = parseOptionalFloat(record[3]); err != nil {
		return RawMinute{}, csvFieldError(path, index, "high", "invalid number", err)
	}
	if row.Low, err = parseOptionalFloat(record[4]); err != nil {
		return RawMinute{}, csvFieldError(path, index, "low", "invalid number", err)
	}
	if row.Close, err = parseOptionalFloat(record[5]); err != nil {
		return RawMinute{}, csvFieldError(path, index, "close", "invalid number", err)
	}
	if row.Volume, err = parseOptionalUint(record[6]); err != nil {
		return RawMinute{}, csvFieldError(path, index, "volume", "invalid unsigned integer", err)
	}
	return row, nil
}

func parseOptionalString(value string) (*string, error) {
	if value == "" {
		return nil, nil
	}
	return &value, nil
}

func parseOptionalTimestamp(value string) (*time.Time, error) {
	if value == "" {
		return nil, nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return nil, err
	}
	_, offset := parsed.Zone()
	if offset != 0 {
		return nil, errors.New("timestamp must have a UTC offset")
	}
	return &parsed, nil
}

func parseOptionalFloat(value string) (*float64, error) {
	if value == "" {
		return nil, nil
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return nil, err
	}
	return &parsed, nil
}

func parseOptionalUint(value string) (*uint64, error) {
	if value == "" {
		return nil, nil
	}
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return nil, err
	}
	return &parsed, nil
}

func csvFieldError(path string, index int64, column, reason string, err error) error {
	return &LoadError{Path: path, Row: index, Column: column, Reason: reason, Err: err}
}

func sameStrings(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range want {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
