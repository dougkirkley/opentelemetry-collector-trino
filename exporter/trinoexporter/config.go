package trinoexporter // import "github.com/dougkirkley/opentelemetry-collector-trino/exporter/trinoexporter"

import (
	"database/sql"
	"encoding/hex"
	"errors"
	"net/http"
	"time"

	"github.com/trinodb/trino-go-client/trino"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/pdata/pcommon"
	conventions "go.opentelemetry.io/collector/semconv/v1.27.0"
)

// Config defines configuration for trino exporter.
type Config struct {
	confighttp.ClientConfig   `mapstructure:",squash"` // squash ensures fields are correctly decoded in embedded struct.
	configretry.BackOffConfig `mapstructure:"retry_on_failure"`
	QueueSettings             exporterhelper.QueueConfig `mapstructure:"sending_queue"`

	Bucket       string `mapstructure:"bucket"`
	BucketPrefix string `mapstructure:"bucket_prefix,omitempty"`
	Catalog      string `mapstructure:"catalog"`
	Schema       string `mapstructure:"schema,omitempty"`
	// LogsTable is the table name for logs. default is `logs`.
	LogsTable string `mapstructure:"logs_table,omitempty"`
	// TTL is The data time-to-live example 30m, 48h. 0 means no ttl.
	TTL time.Duration `mapstructure:"ttl"`
}

var (
	errConfigNoEndpoint = errors.New("endpoint must be specified")
	errConfigCatalog    = errors.New("catalog must be specified")
	errConfigNoBucket   = errors.New("bucket must be specified")
)

// Validate the Trino server configuration.
func (cfg *Config) Validate() (err error) {
	if cfg.Endpoint == "" {
		return errConfigNoEndpoint
	}

	if cfg.Catalog == "" {
		return errConfigCatalog
	}

	if cfg.Bucket == "" {
		return errConfigNoBucket
	}

	// validate config settings
	config := trino.Config{
		ServerURI: cfg.Endpoint,
		Source:    "trinoexporter",
		Catalog:   cfg.Catalog,
	}

	if _, err = config.FormatDSN(); err != nil {
		return err
	}

	return err
}

func (cfg *Config) buildDB(httpClient *http.Client) (*sql.DB, error) {
	if err := trino.RegisterCustomClient("otel", httpClient); err != nil {
		return nil, err
	}

	config := trino.Config{
		ServerURI:        cfg.Endpoint,
		Source:           "trinoexporter",
		Catalog:          cfg.Catalog,
		Schema:           cfg.Schema,
		CustomClientName: "otel",
	}
	dsn, err := config.FormatDSN()
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

func getServiceName(resAttr pcommon.Map) string {
	var serviceName string
	if v, ok := resAttr.Get(conventions.AttributeServiceName); ok {
		serviceName = v.AsString()
	}

	return serviceName
}

// SpanIDToHexOrEmptyString returns a hex string from SpanID.
// An empty string is returned, if SpanID is empty.
func SpanIDToHexOrEmptyString(id pcommon.SpanID) string {
	if id.IsEmpty() {
		return ""
	}
	return hex.EncodeToString(id[:])
}

// TraceIDToHexOrEmptyString returns a hex string from TraceID.
// An empty string is returned, if TraceID is empty.
func TraceIDToHexOrEmptyString(id pcommon.TraceID) string {
	if id.IsEmpty() {
		return ""
	}
	return hex.EncodeToString(id[:])
}
