//go:generate mdatagen metadata.yaml

package trinoexporter // import "github.com/dougkirkley/opentelemetry-collector-trino/exporter/trinoexporter"

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"

	"github.com/dougkirkley/opentelemetry-collector-trino/exporter/trinoexporter/internal/metadata"
)

// NewFactory creates a factory for ClickHouse exporter.
func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		metadata.Type,
		createDefaultConfig,
		exporter.WithLogs(createLogsExporter, metadata.LogsStability),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		QueueSettings: exporterhelper.NewDefaultQueueConfig(),
		BackOffConfig: configretry.NewDefaultBackOffConfig(),
		Schema:        "otel",
		LogsTable:     "logs",
	}
}

// createLogsExporter creates a new exporter for logs.
// Logs are directly inserted into Trino.
func createLogsExporter(ctx context.Context, set exporter.Settings, cfg component.Config) (exporter.Logs, error) {
	c := cfg.(*Config)
	e, err := newLogsExporter(set.Logger, c)
	if err != nil {
		return nil, fmt.Errorf("cannot configure trino logs exporter: %w", err)
	}

	return exporterhelper.NewLogs(
		ctx,
		set,
		cfg,
		e.pushLogsData,
		exporterhelper.WithStart(e.start),
		exporterhelper.WithShutdown(e.shutdown),
		exporterhelper.WithQueue(c.QueueSettings),
		exporterhelper.WithRetry(c.BackOffConfig),
	)
}

func generateTTLExpr(ttl time.Duration, timeField string) string {
	if ttl > 0 {
		switch {
		case ttl%(24*time.Hour) == 0:
			return fmt.Sprintf(`TTL %s + toIntervalDay(%d)`, timeField, ttl/(24*time.Hour))
		case ttl%(time.Hour) == 0:
			return fmt.Sprintf(`TTL %s + toIntervalHour(%d)`, timeField, ttl/time.Hour)
		case ttl%(time.Minute) == 0:
			return fmt.Sprintf(`TTL %s + toIntervalMinute(%d)`, timeField, ttl/time.Minute)
		default:
			return fmt.Sprintf(`TTL %s + toIntervalSecond(%d)`, timeField, ttl/time.Second)
		}
	}
	return ""
}
