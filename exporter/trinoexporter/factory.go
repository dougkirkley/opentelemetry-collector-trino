//go:generate mdatagen metadata.yaml

package trinoexporter // import "github.com/dougkirkley/opentelemetry-collector-trino/exporter/trinoexporter"

import (
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

// NewFactory creates a factory for ClickHouse exporter.
func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		metadata.Type,
		createDefaultConfig,
		exporter.WithLogs(createLogsExporter, metadata.LogsStability),
		exporter.WithTraces(createTracesExporter, metadata.TracesStability),
		exporter.WithMetrics(createMetricExporter, metadata.MetricsStability),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		TimeoutSettings:  exporterhelper.NewDefaultTimeoutConfig(),
		QueueSettings:    exporterhelper.NewDefaultQueueConfig(),
		BackOffConfig:    configretry.NewDefaultBackOffConfig(),
		ConnectionParams: map[string]string{},
		Database:         defaultDatabase,
		LogsTableName:    "otel_logs",
		TracesTableName:  "otel_traces",
		TTL:              0,
		MetricsTables: MetricTablesConfig{
			Gauge:                internal.MetricTypeConfig{Name: defaultMetricTableName + defaultGaugeSuffix},
			Sum:                  internal.MetricTypeConfig{Name: defaultMetricTableName + defaultSumSuffix},
			Summary:              internal.MetricTypeConfig{Name: defaultMetricTableName + defaultSummarySuffix},
			Histogram:            internal.MetricTypeConfig{Name: defaultMetricTableName + defaultHistogramSuffix},
			ExponentialHistogram: internal.MetricTypeConfig{Name: defaultMetricTableName + defaultExpHistogramSuffix},
		},
	}
}

// createLogsExporter creates a new exporter for logs.
// Logs are directly inserted into ClickHouse.
func createLogsExporter(
	ctx context.Context,
	set exporter.Settings,
	cfg component.Config,
) (exporter.Logs, error) {
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
		exporterhelper.WithTimeout(c.TimeoutSettings),
		exporterhelper.WithQueue(c.QueueSettings),
		exporterhelper.WithRetry(c.BackOffConfig),
	)
}

// createTracesExporter creates a new exporter for traces.
// Traces are directly inserted into ClickHouse.
func createTracesExporter(
	ctx context.Context,
	set exporter.Settings,
	cfg component.Config,
) (exporter.Traces, error) {
	c := cfg.(*Config)
	e, err := newTracesExporter(set.Logger, c)
	if err != nil {
		return nil, fmt.Errorf("cannot configure trino traces exporter: %w", err)
	}

	return exporterhelper.NewTraces(
		ctx,
		set,
		cfg,
		e.pushTraceData,
		exporterhelper.WithStart(e.start),
		exporterhelper.WithShutdown(e.shutdown),
		exporterhelper.WithTimeout(c.TimeoutSettings),
		exporterhelper.WithQueue(c.QueueSettings),
		exporterhelper.WithRetry(c.BackOffConfig),
	)
}

func createMetricExporter(
	ctx context.Context,
	set exporter.Settings,
	cfg component.Config,
) (exporter.Metrics, error) {
	c := cfg.(*Config)
	e, err := newMetricsExporter(set.Logger, c)
	if err != nil {
		return nil, fmt.Errorf("cannot configure trino metrics exporter: %w", err)
	}

	return exporterhelper.NewMetrics(
		ctx,
		set,
		cfg,
		e.pushMetricsData,
		exporterhelper.WithStart(e.start),
		exporterhelper.WithShutdown(e.shutdown),
		exporterhelper.WithTimeout(c.TimeoutSettings),
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
