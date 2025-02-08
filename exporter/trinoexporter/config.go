package trinoexporter // import "github.com/dougkirkley/opentelemetry-collector-trino/exporter/trinoexporter"

import (
	"database/sql"
	"errors"
	"time"

	"github.com/trinodb/trino-go-client/trino"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/pdata/pcommon"
	conventions "go.opentelemetry.io/collector/semconv/v1.27.0"
)

// Config defines configuration for trino exporter.
type Config struct {
	TimeoutSettings           exporterhelper.TimeoutConfig `mapstructure:",squash"`
	configretry.BackOffConfig `mapstructure:"retry_on_failure"`
	QueueSettings             exporterhelper.QueueConfig `mapstructure:"sending_queue"`

	trinoConfig trino.Config

	// Endpoint is the trino endpoint.
	Endpoint string `mapstructure:"endpoint"`
	Catalog  string `mapstructure:"catalog"`
	Schema   string `mapstructure:"schema"`
	// LogsTable is the table name for logs. default is `otel_logs`.
	LogsTable string `mapstructure:"logs_table"`
	// TTL is The data time-to-live example 30m, 48h. 0 means no ttl.
	TTL time.Duration `mapstructure:"ttl"`
}

var (
	errConfigNoEndpoint = errors.New("endpoint must be specified")
	errConfigCatalog    = errors.New("catalog must be specified")
	errConfigSchema     = errors.New("schema must be specified")
)

// Validate the Trino server configuration.
func (cfg *Config) Validate() (err error) {
	if cfg.Endpoint == "" {
		return errConfigNoEndpoint
	}
	if cfg.Catalog == "" {
		return errConfigCatalog
	}

	if cfg.Schema == "" {
		return errConfigSchema
	}

	// Validate DSN with trino driver.
	// Last chance to catch invalid config.
	_, err = cfg.trinoConfig.FormatDSN()
	if err != nil {
		return err
	}

	return err
}

func GetServiceName(resAttr pcommon.Map) string {
	var serviceName string
	if v, ok := resAttr.Get(conventions.AttributeServiceName); ok {
		serviceName = v.AsString()
	}

	return serviceName
}

func (cfg *Config) buildDB() (*sql.DB, error) {
	dsn, err := cfg.trinoConfig.FormatDSN()
	if err != nil {
		return nil, err
	}

	// Trino sql driver will read trino settings from the DSN string.
	// It also ensures defaults.
	conn, err := sql.Open(driverName, dsn)
	if err != nil {
		return nil, err
	}

	return conn, nil
}
