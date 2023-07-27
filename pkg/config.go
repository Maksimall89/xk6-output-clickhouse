package pkg

import (
	"encoding/json"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/mstoykov/envconfig"
	"go.k6.io/k6/lib/types"
	"gopkg.in/guregu/null.v4"
	"time"
)

// Config xk6-clickhouse-output.
type Config struct {
	PushInterval types.NullDuration `json:"pushInterval" envconfig:"K6_CLICKHOUSE_PUSH_INTERVAL"`
	DSN          null.String        `json:"dsn" envconfig:"K6_CLICKHOUSE_DSN"`
	ClickConfig  *clickhouse.Options
}

// newConfig by default parameters.
func newConfig() Config {
	return Config{
		PushInterval: types.NewNullDuration(time.Second*30, false),
		DSN:          null.NewString("clickhouse://default:pass@localhost:9000/k6DB", false),
	}
}

func getConsolidatedConfig(jsonRawCfg json.RawMessage, env map[string]string, confArg string) (Config, error) {
	consolidatedCfg := newConfig()
	var err error
	if jsonRawCfg != nil {
		jsonCfg, err := parseJSON(jsonRawCfg)
		if err != nil {
			return consolidatedCfg, err
		}
		consolidatedCfg = consolidatedCfg.apply(jsonCfg)
	}

	envConfig := Config{}
	if err := envconfig.Process("K6_CLICKHOUSE_", &envConfig, func(key string) (string, bool) {
		v, ok := env[key]
		return v, ok
	}); err != nil {
		return consolidatedCfg, err
	}
	consolidatedCfg = consolidatedCfg.apply(envConfig)

	if confArg != "" {
		consolidatedCfg.DSN.String = confArg
	}

	consolidatedCfg.ClickConfig, err = parseDSN(consolidatedCfg.DSN.String)
	if err != nil {
		return consolidatedCfg, err
	}

	return consolidatedCfg, nil
}

func (c Config) apply(cfg Config) Config {
	if cfg.PushInterval.Valid {
		c.PushInterval = cfg.PushInterval
	}
	if cfg.DSN.Valid {
		c.DSN = cfg.DSN
	}

	return c
}

func parseJSON(data json.RawMessage) (Config, error) {
	cfg := Config{}
	err := json.Unmarshal(data, &cfg)

	return cfg, err
}

// parseDSN for Clickhouse DB.
func parseDSN(dsn string) (*clickhouse.Options, error) {
	clickConfig, err := clickhouse.ParseDSN(dsn)
	if err != nil {
		return &clickhouse.Options{}, fmt.Errorf("could not parse DSN string: %w", err)
	}

	return clickConfig, nil
}
