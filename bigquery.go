package main

import (
	"context"
	"errors"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"text/template"
	"time"

	_ "embed"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
)

//go:embed query.sql.gtpl
var _queryTemplate string

var queryTemplate = template.Must(template.New("").Funcs(template.FuncMap{
	"sanitize":  Sanitize,
	"dt_format": DateTimeFormat,
}).Parse(_queryTemplate))

type JobRow struct {
	ProjectID   string     `biguery:"project_id" json:"project_id"`
	JobID       string     `bigquery:"job_id" json:"job_id"`
	TotalSlotMS int        `bigquery:"total_slot_ms" json:"total_slot_ms"`
	JobStages   []JobStage `bigquery:"job_stages" json:"job_stages"`
}

type JobStage struct {
	Name                      string    `bigquery:"name" json:"name"`
	ID                        int       `bigquery:"id" json:"id"`
	StartMs                   int       `bigquery:"start_ms" json:"start_ms"`
	EndMs                     int       `bigquery:"end_ms" json:"end_ms"`
	InputStages               []int     `bigquery:"input_stages" json:"input_stages"`
	WaitRatioAvg              float64   `bigquery:"wait_ratio_avg" json:"wait_ratio_avg"`
	WaitMsAvg                 int       `bigquery:"wait_ms_avg" json:"wait_ms_avg"`
	WaitRatioMax              float64   `bigquery:"wait_ratio_max" json:"wait_ratio_max"`
	WaitMsMax                 int       `bigquery:"wait_ms_max" json:"wait_ms_max"`
	ReadRatioAvg              float64   `bigquery:"read_ratio_avg" json:"read_ratio_avg"`
	ReadMsAvg                 int       `bigquery:"read_ms_avg" json:"read_ms_avg"`
	ReadRatioMax              float64   `bigquery:"read_ratio_max" json:"read_ratio_max"`
	ReadMsMax                 int       `bigquery:"read_ms_max" json:"read_ms_max"`
	ComputeRatioAvg           float64   `bigquery:"compute_ratio_avg" json:"compute_ratio_avg"`
	ComputeMsAvg              int       `bigquery:"compute_ms_avg" json:"compute_ms_avg"`
	ComputeRatioMax           float64   `bigquery:"compute_ratio_max" json:"compute_ratio_max"`
	ComputeMsMax              int       `bigquery:"compute_ms_max" json:"compute_ms_max"`
	WriteRatioAvg             float64   `bigquery:"write_ratio_avg" json:"write_ratio_avg"`
	WriteMsAvg                int       `bigquery:"write_ms_avg" json:"write_ms_avg"`
	WriteRatioMax             float64   `bigquery:"write_ratio_max" json:"write_ratio_max"`
	WriteMsMax                int       `bigquery:"write_ms_max" json:"write_ms_max"`
	ShuffleOutputBytes        int       `bigquery:"shuffle_output_bytes" json:"shuffle_output_bytes"`
	ShuffleOutputBytesSpilled int       `bigquery:"shuffle_output_bytes_spilled" json:"shuffle_output_bytes_spilled"`
	RecordsRead               int       `bigquery:"records_read" json:"records_read"`
	RecordsWritten            int       `bigquery:"records_written" json:"records_written"`
	ParallelInputs            int       `bigquery:"parallel_inputs" json:"parallel_inputs"`
	CompletedParallelInputs   int       `bigquery:"completed_parallel_inputs" json:"completed_parallel_inputs"`
	SlotMs                    int       `bigquery:"slot_ms" json:"slot_ms"`
	Status                    string    `bigquery:"status" json:"status"`
	Steps                     []JobStep `bigquery:"steps" json:"steps"`
}

type JobStep struct {
	Kind     string   `bigquery:"kind" json:"kind"`
	Substeps []string `bigquery:"substeps" json:"substeps"`
}

// RunQuery gets the first JobRow that matches the provided project and job id
func RunQuery(ctx context.Context, billingProject string, project string, region string, jobID string, start time.Time, end time.Time) (*JobRow, error) {
	sql, err := formatQuery(project, region, jobID, start, end)
	if err != nil {
		return nil, err
	}

	client, err := bigquery.NewClient(ctx, billingProject)
	if err != nil {
		return nil, err
	}
	defer client.Close()
	q := client.Query(sql)
	it, err := q.Read(ctx)
	if err != nil {
		return nil, err
	}

	row := JobRow{}
	err = it.Next(&row)
	if err == iterator.Done {
		return nil, errors.New("Not found")
	}
	if err != nil {
		return nil, err
	}
	return &row, err
}

var jsFieldMap = func() map[string]int {
	t := reflect.TypeOf(JobStage{})
	fieldMap := map[string]int{}
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		fieldMap[field.Name] = i

		jtag, jok := field.Tag.Lookup("json")
		if jok {
			fieldMap[jtag] = i
		}

		btag, bok := field.Tag.Lookup("bigquery")
		if bok {
			fieldMap[btag] = i
			continue
		}
	}
	return fieldMap
}()

// JobStageFieldLookup takes a JobStage and allows name lookups. These names can
// be the field names or struct tag aliases for bigquery or json. This is gross,
// but it means not having to hand-extract and format each of these values.
func JobStageFieldLookup(sp *JobStage) func(string) string {
	v := reflect.ValueOf(*sp)
	return func(k string) string {
		i, ok := jsFieldMap[k]
		if !ok {
			return "NOT_FOUND"
		}

		field := v.Field(i)
		switch field.Kind() {
		case reflect.String:
			return field.String()
		case reflect.Int:
			fallthrough
		case reflect.Int8:
			fallthrough
		case reflect.Int16:
			fallthrough
		case reflect.Int32:
			fallthrough
		case reflect.Int64:
			return strconv.FormatInt(field.Int(), 10)
		case reflect.Uint:
			fallthrough
		case reflect.Uint8:
			fallthrough
		case reflect.Uint16:
			fallthrough
		case reflect.Uint32:
			fallthrough
		case reflect.Uint64:
			return strconv.FormatUint(field.Uint(), 10)
		case reflect.Float32:
			fallthrough
		case reflect.Float64:
			return strconv.FormatFloat(field.Float(), 'g', -1, 64)
		}
		return "INVALID KIND: " + field.Kind().String()
	}
}

func formatQuery(project string, region string, jobID string, start time.Time, end time.Time) (string, error) {
	var b strings.Builder

	err := queryTemplate.Execute(&b, struct {
		Project   string
		Region    string
		JobID     string
		StartTime time.Time
		EndTime   time.Time
	}{
		project,
		region,
		jobID,
		start,
		end,
	})
	if err != nil {
		return "", err
	}
	return b.String(), err
}

// Sanitize prevents bad command-line input from hurting the database
func Sanitize(v string) string {
	rx := regexp.MustCompile(`[^a-zA-Z0-9\-_]`)
	return rx.ReplaceAllString(v, "_")
}

func DateTimeFormat(v time.Time) string {
	return v.Format(time.RFC3339Nano)
}
