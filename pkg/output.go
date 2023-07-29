package pkg

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"go.k6.io/k6/metrics"
	"go.k6.io/k6/output"
	"go.k6.io/k6/output/csv"
	"go.uber.org/zap"
)

const NameOutput = "clickhouse"

type Output struct {
	output.SampleBuffer

	periodicFlusher *output.PeriodicFlusher

	resTags       []string
	row           []string
	ignoreMetrics []string

	clickConnect clickhouse.Conn
	ctx          context.Context

	logger *zap.SugaredLogger
	config Config
}

// New Creates new instance of CSV output.
func New(params output.Params) (output.Output, error) {
	return newOutput(params)
}

func newOutput(params output.Params) (*Output, error) {
	logger, err := zap.NewProduction()
	if err != nil {
		return &Output{}, fmt.Errorf("could not create logger: %w", err)
	}
	config, err := getConsolidatedConfig(params.JSONConfig, params.Environment, params.ConfigArgument)
	if err != nil {
		return &Output{}, fmt.Errorf("could not parse configs: %w", err)
	}

	clickConn, err := clickhouse.Open(config.ClickConfig)
	if err != nil {
		return &Output{}, fmt.Errorf("could not connect to clickhouse: %w", err)
	}

	var resTags []string
	for tag := range params.ScriptOptions.SystemTags.Map() {
		systemTag, err := metrics.SystemTagString(tag)
		if err != nil {
			return nil, err
		}

		if metrics.NonIndexableSystemTags.Has(systemTag) {
			continue
		}

		resTags = append(resTags, tag)
	}
	sort.Strings(resTags)

	return &Output{
		logger:        logger.Sugar(),
		config:        config,
		clickConnect:  clickConn,
		resTags:       resTags,
		row:           make([]string, 3+len(resTags)+2),
		ctx:           context.Background(),
		ignoreMetrics: config.IgnMetrics,
	}, nil
}

// Description returns a human-readable description of the output.
func (o *Output) Description() string {
	return NameOutput
}

// Start writes the csv header and starts a new output.PeriodicFlusher.
func (o *Output) Start() error {
	o.logger.Debug("Starting...")

	if err := o.clickConnect.Exec(o.ctx,
		fmt.Sprintf("create DATABASE if not exists %s", o.config.ClickConfig.Auth.Database)); err != nil {
		o.logger.Errorf("Start: Couldn't create database %s: %v", o.config.ClickConfig.Auth.Database, err)
		return err
	}

	if err := o.clickConnect.Exec(o.ctx, o.createSchemaDB()); err != nil {
		o.logger.Errorf("Start: Couldn't create schema in DB %s: %v", o.config.ClickConfig.Auth.Database, err)
		return err
	}

	pf, err := output.NewPeriodicFlusher(time.Duration(o.config.PushInterval.Duration), o.flushMetrics)
	if err != nil {
		o.logger.Errorf("Start: Couldn't set flush interval: %v", err)
		return err
	}

	o.logger.Debug("Started!")
	o.periodicFlusher = pf

	return nil
}

// Stop flushes any remaining metrics and stops the goroutine.
func (o *Output) Stop() error {
	o.logger.Debug("Stopping...")
	defer o.logger.Debug("Stopped!")
	o.periodicFlusher.Stop()
	if err := o.clickConnect.Close(); err != nil {
		return err
	}

	return nil
}

func (o *Output) flushMetrics() {
	samples := o.GetBufferedSamples()
	if len(samples) == 0 {
		return
	}
	start := time.Now()
	o.logger.Debugf("flushMetrics (contain %d samples): Collecting... ", len(samples))

	batch, err := o.clickConnect.PrepareBatch(o.ctx, fmt.Sprintf(
		"insert into %s.testData", o.config.ClickConfig.Auth.Database))
	if err != nil {
		o.logger.Errorf("Couldn't prepare batch: %v", err)
		return
	}
	var timestamp int64
	var row []string
	for _, sc := range samples {
		for _, sample := range sc.GetSamples() {
			if isIgnoreMetrics(sample.Metric.Name, o.ignoreMetrics) {
				continue
			}
			timestamp, row = sampleToRow(&sample, o.resTags, o.row)
			for k, v := range row {
				switch k {
				case 0: // timestamp colum
					if err := batch.Column(k).Append([]int64{timestamp}); err != nil {
						o.logger.Errorf("Couldn't create string timestamp for batch: %v", err)
					}
					continue
				case 2: // metric_value colum
					floatCode, _ := strconv.ParseFloat(v, 32)
					if err := batch.Column(k).Append([]float64{floatCode}); err != nil {
						o.logger.Errorf("Couldn't create string metric_value for batch: %v", err)
					}
					continue
				case 5, 13: // error_code and status colum
					intCode, _ := strconv.ParseInt(v, 10, 64)
					if err := batch.Column(k).Append([]int64{intCode}); err != nil {
						o.logger.Errorf("Couldn't create string error_code or status for batch: %v", err)
					}
					continue
				default:
					if err := batch.Column(k).Append([]string{v}); err != nil {
						o.logger.Errorf("Couldn't create string for batch: %v", err)
					}
				}
			}
		}
	}
	if err := batch.Send(); err != nil {
		o.logger.Errorf("Couldn't send batch: %v", err)
	}
	t := time.Since(start)
	o.logger.Debug("flushMetrics: Samples committed on %s nano sec!", t)
}

// sampleToRow converts sample into array of strings like k6 csv output.
func sampleToRow(sample *metrics.Sample, resTags []string, row []string) (int64, []string) {
	timestamp := sample.Time.Unix()
	row[1] = sample.Metric.Name
	row[2] = fmt.Sprintf("%f", sample.Value)
	// TODO: optimize all of this - do not use tags.Map(), flip resTags, fix the
	// for loops, get rid of IsStringInSlice(), etc.
	sampleTags := sample.Tags.Map()
	for ind, tag := range resTags {
		row[ind+3] = sampleTags[tag]
	}

	extraTags := bytes.Buffer{}
	prev := false
	writeTag := func(tag, val string) bool {
		if csv.IsStringInSlice(resTags, tag) {
			return true // continue
		}
		if prev {
			if _, err := extraTags.WriteString("&"); err != nil {
				return false
			}
		}

		if _, err := extraTags.WriteString(tag); err != nil {
			return false
		}

		if _, err := extraTags.WriteString("="); err != nil {
			return false
		}

		if _, err := extraTags.WriteString(val); err != nil {
			return false
		}
		prev = true

		return true
	}

	for tag, val := range sampleTags {
		if !writeTag(tag, val) {
			break
		}
	}
	row[len(row)-2] = extraTags.String()
	extraTags.Reset()
	prev = false

	for key, val := range sample.Metadata {
		if !writeTag(key, val) {
			break
		}
	}
	row[len(row)-1] = extraTags.String()
	var outString string
	for _, v := range row {
		outString += fmt.Sprintf("'%s',", v)
	}

	return timestamp, row
}

// createSchemaDB for init table into Clickhouse.
func (o *Output) createSchemaDB() string {
	// get name colum from k6 response metric
	var metricsName string
	for _, tag := range o.resTags {
		if strings.EqualFold(tag, "error_code") || strings.EqualFold(tag, "status") {
			metricsName += fmt.Sprintf("%s Nullable(Int64),\n", tag)
		} else {
			metricsName += fmt.Sprintf("%s Nullable(String),\n", tag)
		}
	}

	return fmt.Sprintf(`create table if not exists %s.testData
		(
			timestamp DateTime default toUnixTimestamp(now64()),
    		metric_name String,
			metric_value Float64,
    		%s
			extra_tags Nullable(String), 
			metadata Nullable(String)
		) ENGINE = MergeTree()
			ORDER BY (timestamp, metric_name)
			PRIMARY KEY (timestamp, metric_name);`, o.config.ClickConfig.Auth.Database, metricsName)
}

// check isIgnoreMetrics list or not.
func isIgnoreMetrics(metricsName string, ignoreMetrics []string) bool {
	for _, ignMetric := range ignoreMetrics {
		if metricsName == ignMetric {
			return true
		}
	}
	return false
}
