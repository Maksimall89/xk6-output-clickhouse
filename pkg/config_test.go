package pkg

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

const DsnTest = "clickhouse://user:password@localhost:9000/mydbname"
const IgnoreMetricTest = "http_req_blocked,http_req_connecting"

var IgnoreMetricsTest = []string{"http_req_blocked", "http_req_connecting"}

func Test_getConsolidatedConfig_SetArg(t *testing.T) {
	t.Parallel()
	conf, err := getConsolidatedConfig(nil, nil, DsnTest)

	assert.NoError(t, err)
	assert.Equal(t, DsnTest, conf.DSN.String)
}

func Test_getConsolidatedConfig_SetEnv(t *testing.T) {
	t.Parallel()

	conf, err := getConsolidatedConfig(nil, map[string]string{
		"K6_CLICKHOUSE_DSN":            DsnTest,
		"K6_CLICKHOUSE_IGNORE_METRICS": IgnoreMetricTest,
	}, "")

	assert.NoError(t, err)
	assert.Equal(t, DsnTest, conf.DSN.String)
	assert.Equal(t, IgnoreMetricsTest, conf.IgnMetrics)
}

func Test_getConsolidatedConfig_SetEnvArg(t *testing.T) {
	t.Parallel()
	const dsnEnvTest = "clickhouse://userENV:passwordENV@localhostENV:9000/mydbnameENV"

	conf, err := getConsolidatedConfig(nil, map[string]string{
		"K6_CLICKHOUSE_DSN":            dsnEnvTest,
		"K6_CLICKHOUSE_IGNORE_METRICS": IgnoreMetricTest,
	}, DsnTest)

	assert.NoError(t, err)
	assert.Equal(t, IgnoreMetricsTest, conf.IgnMetrics)
	assert.Equal(t, conf.ClickConfig.Auth.Database, "mydbname")
	assert.Equal(t, conf.ClickConfig.Auth.Password, "password")
	assert.Equal(t, conf.ClickConfig.Auth.Username, "user")
}
