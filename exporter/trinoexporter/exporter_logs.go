package trinoexporter // import "github.com/dougkirkley/opentelemetry-collector-trino/exporter/trinoexporter"

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/trinodb/trino-go-client/trino" // For register database driver.
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
)

type logsExporter struct {
	cfg       *Config
	db        *sql.DB
	insertSQL string

	logger   *zap.Logger
	settings component.TelemetrySettings
}

func newLogsExporter(logger *zap.Logger, cfg *Config) (*logsExporter, error) {
	return &logsExporter{
		cfg:       cfg,
		insertSQL: renderInsertLogsSQL(cfg),
		logger:    logger,
	}, nil
}

func (e *logsExporter) start(ctx context.Context, host component.Host) error {
	client, err := e.cfg.ClientConfig.ToClient(ctx, host, e.settings)
	if err != nil {
		return err
	}

	db, err := e.cfg.buildDB(client)
	if err != nil {
		return err
	}
	e.db = db

	if err := e.createSchema(ctx, e.cfg); err != nil {
		return err
	}

	return e.createLogsTable(ctx, e.cfg)
}

// shutdown will shut down the exporter.
func (e *logsExporter) shutdown(_ context.Context) error {
	if e.db != nil {
		return e.db.Close()
	}
	return nil
}

func (e *logsExporter) pushLogsData(ctx context.Context, ld plog.Logs) error {
	start := time.Now()
	err := doWithTx(ctx, e.db, func(tx *sql.Tx) error {
		statement, err := tx.PrepareContext(ctx, e.insertSQL)
		if err != nil {
			return fmt.Errorf("PrepareContext: %w", err)
		}

		defer func() {
			_ = statement.Close()
		}()

		for i := 0; i < ld.ResourceLogs().Len(); i++ {
			logs := ld.ResourceLogs().At(i)
			res := logs.Resource()
			resURL := logs.SchemaUrl()
			resAttr := res.Attributes()
			serviceName := getServiceName(res.Attributes())

			for j := 0; j < logs.ScopeLogs().Len(); j++ {
				rs := logs.ScopeLogs().At(j).LogRecords()
				scopeURL := logs.ScopeLogs().At(j).SchemaUrl()
				scopeName := logs.ScopeLogs().At(j).Scope().Name()
				scopeVersion := logs.ScopeLogs().At(j).Scope().Version()
				scopeAttr := logs.ScopeLogs().At(j).Scope().Attributes()

				for k := 0; k < rs.Len(); k++ {
					r := rs.At(k)

					timestamp := r.Timestamp()
					if timestamp == 0 {
						timestamp = r.ObservedTimestamp()
					}

					logAttr := r.Attributes()
					_, err = statement.ExecContext(ctx,
						timestamp.AsTime(),
						TraceIDToHexOrEmptyString(r.TraceID()),
						SpanIDToHexOrEmptyString(r.SpanID()),
						uint32(r.Flags()),
						r.SeverityText(),
						int32(r.SeverityNumber()),
						serviceName,
						r.Body().AsString(),
						resURL,
						resAttr,
						scopeURL,
						scopeName,
						scopeVersion,
						scopeAttr,
						logAttr,
					)
					if err != nil {
						return fmt.Errorf("ExecContext: %w", err)
					}
				}
			}
		}
		return nil
	})
	duration := time.Since(start)
	e.logger.Debug("insert logs", zap.Int("records", ld.LogRecordCount()),
		zap.String("cost", duration.String()))
	return err
}

const (
	createLogsTableSQL = `CREATE TABLE IF NOT EXISTS "%s.%s.%s" (
		Timestamp TIMESTAMP(9),
		TimestampTime TIMESTAMP,
		TraceId VARCHAR,
		SpanId VARCHAR,
		TraceFlags TINYINT,
		SeverityText VARCHAR,
		SeverityNumber TINYINT,
		ServiceName VARCHAR,
		Body VARCHAR,
		ResourceSchemaUrl VARCHAR,
		ResourceAttributes MAP(VARCHAR, VARCHAR),
		ScopeSchemaUrl VARCHAR,
		ScopeName VARCHAR,
		ScopeVersion VARCHAR,
		ScopeAttributes MAP(VARCHAR, VARCHAR),
		LogAttributes MAP(VARCHAR, VARCHAR)
	)
	WITH (
		format = 'PARQUET',
		format_version = 2,
		partitioning = ARRAY['date(TimestampTime)'],
		sorted_by = ARRAY['ServiceName', 'TimestampTime'],
		location = '%s'
	)`

	insertLogsSQLTemplate = `INSERT INTO "%s.%s.%s" (
    	Timestamp,
    	TraceId,
    	SpanId,
    	TraceFlags,
    	SeverityText,
    	SeverityNumber,
    	ServiceName,
    	Body,
    	ResourceSchemaUrl,
    	ResourceAttributes,
    	ScopeSchemaUrl,
    	ScopeName,
    	ScopeVersion,
    	ScopeAttributes,
    	LogAttributes
	) 
	VALUES (
    	?,
		?,
		?,
		?,
		?,
		?,
		?,
		?,
		?,
		?,
		?,
		?,
		?,
		?,
		?
		)`
)

var driverName = "trino" // for testing

func (e *logsExporter) createSchema(ctx context.Context, cfg *Config) error {
	defer func() {
		_ = e.db.Close()
	}()
	query := fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS "%s.%s"`, cfg.Catalog, cfg.Schema)
	_, err := e.db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("create schema: %w", err)
	}
	return nil
}

func (e *logsExporter) createLogsTable(ctx context.Context, cfg *Config) error {
	if _, err := e.db.ExecContext(ctx, renderCreateLogsTableSQL(cfg)); err != nil {
		return fmt.Errorf("exec create logs table sql: %w", err)
	}
	return nil
}

func renderCreateLogsTableSQL(cfg *Config) string {
	location := fmt.Sprintf("s3://%s%s", cfg.Bucket, cfg.BucketPrefix)
	return fmt.Sprintf(createLogsTableSQL, cfg.Catalog, cfg.Schema, cfg.LogsTable, location)
}

func renderInsertLogsSQL(cfg *Config) string {
	return fmt.Sprintf(insertLogsSQLTemplate, cfg.Catalog, cfg.Schema, cfg.LogsTable)
}

func doWithTx(_ context.Context, db *sql.DB, fn func(tx *sql.Tx) error) error {
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("db.Begin: %w", err)
	}

	defer func() {
		_ = tx.Rollback()
	}()

	if err := fn(tx); err != nil {
		return err
	}

	return tx.Commit()
}
