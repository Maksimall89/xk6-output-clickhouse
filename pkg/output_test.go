package pkg

import (
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
	"go.k6.io/k6/output"
	"testing"
)

func Test_createSchemaDB(t *testing.T) {
	t.Parallel()

	outputConfig := Output{
		SampleBuffer:    output.SampleBuffer{},
		periodicFlusher: nil,
		resTags: []string{"check", "error", "error_code", "expected_response", "group", "method", "name",
			"proto", "scenario", "service", "status", "subproto", "tls_version", "url"},
		row:          nil,
		clickConnect: nil,
		ctx:          nil,
		logger:       nil,
		config: Config{
			ClickConfig: &clickhouse.Options{
				Addr: nil,
				Auth: clickhouse.Auth{
					Database: "mydbname",
				},
			},
		},
	}

	schema := outputConfig.createSchemaDB()
	schemaExpect := `create table if not exists mydbname.testData
		(
			timestamp DateTime default toUnixTimestamp(now64()),
    		metric_name String,
			metric_value Float64,
    		check Nullable(String),
error Nullable(String),
error_code Nullable(Int64),
expected_response Nullable(String),
group Nullable(String),
method Nullable(String),
name Nullable(String),
proto Nullable(String),
scenario Nullable(String),
service Nullable(String),
status Nullable(Int64),
subproto Nullable(String),
tls_version Nullable(String),
url Nullable(String),

			extra_tags Nullable(String), 
			metadata Nullable(String)
		) ENGINE = MergeTree()
			ORDER BY (timestamp, metric_name)
			PRIMARY KEY (timestamp, metric_name);`

	assert.NotEmpty(t, schema)
	assert.Equal(t, schemaExpect, schema)
}
