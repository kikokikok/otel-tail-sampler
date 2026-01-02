use super::{IngestResult, StorageType, StoreStats, TraceStore};
use crate::sampling::policies::TraceSummary;
use crate::state::{BufferedSpan, SpanKind};
use anyhow::{Context, Result};
use arrow_array::{Array, ArrayRef, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use async_trait::async_trait;
use datafusion::prelude::*;
use futures::TryStreamExt;
use iceberg::expr::Reference;
use iceberg::spec::DataFileFormat;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, CatalogBuilder, TableIdent};
use iceberg_catalog_rest::RestCatalogBuilder;
use iceberg_datafusion::IcebergTableProvider;
use parking_lot::RwLock;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use parquet::file::properties::WriterProperties;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::OnceCell;
use tracing::{debug, info};

#[derive(Debug, Clone)]
pub struct IcebergConfig {
    pub catalog_uri: String,
    pub warehouse: String,
    pub namespace: String,
    pub table_name: String,
    pub inactivity_threshold_ms: i64,
    /// Project ID for Lakekeeper REST catalog (used in x-project-id header)
    pub project_id: Option<String>,
    /// S3 endpoint URL (for MinIO or custom S3-compatible storage)
    pub s3_endpoint: Option<String>,
    /// S3 access key ID
    pub s3_access_key_id: Option<String>,
    /// S3 secret access key
    pub s3_secret_access_key: Option<String>,
    /// S3 region
    pub s3_region: String,
    /// Use path-style S3 access (required for MinIO)
    pub s3_path_style: bool,
}

pub struct IcebergTraceStore {
    config: IcebergConfig,
    catalog: OnceCell<Arc<dyn Catalog>>,
    table_ident: TableIdent,
    metadata_cache: Arc<RwLock<TraceMetadataCache>>,
    inactivity_threshold: Duration,
    session_ctx: Arc<tokio::sync::Mutex<Option<SessionContext>>>,
}

struct TraceMetadataCache {
    traces: HashMap<String, CachedTraceMeta>,
    last_refresh: Instant,
}

#[derive(Debug, Clone)]
struct CachedTraceMeta {
    has_error: bool,
    max_duration_ms: i64,
    span_count: usize,
    min_timestamp_ms: i64,
    max_timestamp_ms: i64,
    last_seen: Instant,
    processing: bool,
}

impl IcebergTraceStore {
    pub async fn new(config: IcebergConfig) -> Result<Self> {
        let table_ident =
            TableIdent::from_strs([config.namespace.as_str(), config.table_name.as_str()])
                .context("Invalid table identifier")?;

        let inactivity_threshold = Duration::from_millis(config.inactivity_threshold_ms as u64);

        let store = Self {
            config,
            catalog: OnceCell::new(),
            table_ident,
            metadata_cache: Arc::new(RwLock::new(TraceMetadataCache {
                traces: HashMap::new(),
                last_refresh: Instant::now(),
            })),
            inactivity_threshold,
            session_ctx: Arc::new(tokio::sync::Mutex::new(None)),
        };

        store.init_catalog().await?;
        store.init_datafusion().await?;

        Ok(store)
    }

    async fn init_catalog(&self) -> Result<()> {
        let mut props = HashMap::from([
            ("uri".to_string(), self.config.catalog_uri.clone()),
            ("warehouse".to_string(), self.config.warehouse.clone()),
        ]);

        if let Some(project_id) = &self.config.project_id {
            props.insert("header.x-project-id".to_string(), project_id.clone());
        }

        if let Some(endpoint) = &self.config.s3_endpoint {
            props.insert("s3.endpoint".to_string(), endpoint.clone());
        }
        if let Some(access_key) = &self.config.s3_access_key_id {
            props.insert("s3.access-key-id".to_string(), access_key.clone());
        }
        if let Some(secret_key) = &self.config.s3_secret_access_key {
            props.insert("s3.secret-access-key".to_string(), secret_key.clone());
        }
        props.insert("s3.region".to_string(), self.config.s3_region.clone());
        if self.config.s3_path_style {
            props.insert("s3.path-style-access".to_string(), "true".to_string());
        }

        let catalog = RestCatalogBuilder::default()
            .load("lakekeeper", props)
            .await
            .context("Failed to connect to Iceberg catalog")?;

        self.catalog
            .set(Arc::new(catalog))
            .map_err(|_| anyhow::anyhow!("Catalog already initialized"))?;

        info!(
            "Connected to Iceberg catalog at {}",
            self.config.catalog_uri
        );

        Ok(())
    }

    async fn init_datafusion(&self) -> Result<()> {
        let catalog = self.get_catalog()?;
        let table = catalog
            .load_table(&self.table_ident)
            .await
            .context("Failed to load Iceberg table for DataFusion")?;

        let ctx = SessionContext::new();
        let provider = IcebergTableProvider::try_new_from_table(table).await?;
        ctx.register_table("spans", Arc::new(provider))?;

        let mut session = self.session_ctx.lock().await;
        *session = Some(ctx);

        info!("DataFusion session initialized with Iceberg table 'spans'");

        Ok(())
    }

    pub async fn refresh_datafusion(&self) -> Result<()> {
        let catalog = self.get_catalog()?;
        let table = catalog
            .load_table(&self.table_ident)
            .await
            .context("Failed to load Iceberg table for DataFusion refresh")?;

        let ctx = SessionContext::new();
        let provider = IcebergTableProvider::try_new_from_table(table).await?;
        ctx.register_table("spans", Arc::new(provider))?;

        let mut session = self.session_ctx.lock().await;
        *session = Some(ctx);

        info!("DataFusion session refreshed");

        Ok(())
    }

    pub async fn query_sql(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        self.refresh_datafusion().await?;
        
        let session = self.session_ctx.lock().await;
        let ctx = session
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("DataFusion session not initialized"))?;

        let df: DataFrame = ctx.sql(sql).await.context("SQL query failed")?;
        df.collect().await.context("Failed to collect results")
    }

    pub async fn slowest_traces(&self, limit: usize) -> Result<Vec<RecordBatch>> {
        let sql = format!(
            "SELECT trace_id, MAX(duration_ms) as max_duration_ms, COUNT(*) as span_count 
             FROM spans 
             GROUP BY trace_id 
             ORDER BY max_duration_ms DESC 
             LIMIT {}",
            limit
        );
        self.query_sql(&sql).await
    }

    pub async fn error_traces(&self, limit: usize) -> Result<Vec<RecordBatch>> {
        let sql = format!(
            "SELECT trace_id, service_name, operation_name, duration_ms 
             FROM spans 
             WHERE status_code = 2 
             ORDER BY timestamp_ms DESC 
             LIMIT {}",
            limit
        );
        self.query_sql(&sql).await
    }

    pub async fn service_stats(&self) -> Result<Vec<RecordBatch>> {
        let sql = "SELECT service_name, 
                          COUNT(*) as span_count,
                          COUNT(DISTINCT trace_id) as trace_count,
                          AVG(duration_ms) as avg_duration_ms,
                          MAX(duration_ms) as max_duration_ms,
                          SUM(CASE WHEN status_code = 2 THEN 1 ELSE 0 END) as error_count
                   FROM spans 
                   GROUP BY service_name 
                   ORDER BY span_count DESC";
        self.query_sql(sql).await
    }

    fn get_catalog(&self) -> Result<&Arc<dyn Catalog>> {
        self.catalog
            .get()
            .ok_or_else(|| anyhow::anyhow!("Catalog not initialized"))
    }

    fn spans_to_record_batch(&self, spans: &[BufferedSpan]) -> Result<RecordBatch> {
        let trace_ids: Vec<&str> = spans.iter().map(|s| s.trace_id.as_str()).collect();
        let span_ids: Vec<&str> = spans.iter().map(|s| s.span_id.as_str()).collect();
        let parent_span_ids: Vec<Option<&str>> = spans
            .iter()
            .map(|s| s.parent_span_id.as_deref())
            .collect();
        let timestamps: Vec<i64> = spans.iter().map(|s| s.timestamp_ms).collect();
        let durations: Vec<i64> = spans.iter().map(|s| s.duration_ms).collect();
        let status_codes: Vec<i32> = spans.iter().map(|s| s.status_code).collect();
        let span_kinds: Vec<i32> = spans.iter().map(|s| s.span_kind.clone() as i32).collect();
        let service_names: Vec<&str> = spans.iter().map(|s| s.service_name.as_str()).collect();
        let operation_names: Vec<&str> = spans.iter().map(|s| s.operation_name.as_str()).collect();

        fn field_with_id(id: i32, name: &str, data_type: DataType, nullable: bool) -> Field {
            Field::new(name, data_type, nullable).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                id.to_string(),
            )]))
        }

        let schema = Arc::new(ArrowSchema::new(vec![
            field_with_id(1, "trace_id", DataType::Utf8, false),
            field_with_id(2, "span_id", DataType::Utf8, false),
            field_with_id(3, "parent_span_id", DataType::Utf8, true),
            field_with_id(4, "timestamp_ms", DataType::Int64, false),
            field_with_id(5, "duration_ms", DataType::Int64, false),
            field_with_id(6, "status_code", DataType::Int32, false),
            field_with_id(7, "span_kind", DataType::Int32, true),
            field_with_id(8, "service_name", DataType::Utf8, false),
            field_with_id(9, "operation_name", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(trace_ids)) as ArrayRef,
                Arc::new(StringArray::from(span_ids)) as ArrayRef,
                Arc::new(StringArray::from(parent_span_ids)) as ArrayRef,
                Arc::new(Int64Array::from(timestamps)) as ArrayRef,
                Arc::new(Int64Array::from(durations)) as ArrayRef,
                Arc::new(Int32Array::from(status_codes)) as ArrayRef,
                Arc::new(Int32Array::from(span_kinds)) as ArrayRef,
                Arc::new(StringArray::from(service_names)) as ArrayRef,
                Arc::new(StringArray::from(operation_names)) as ArrayRef,
            ],
        )
        .context("Failed to create RecordBatch")?;

        Ok(batch)
    }

    fn update_cache_for_spans(&self, spans: &[BufferedSpan]) {
        let mut cache = self.metadata_cache.write();
        let now = Instant::now();

        for span in spans {
            let entry = cache
                .traces
                .entry(span.trace_id.clone())
                .or_insert(CachedTraceMeta {
                    has_error: false,
                    max_duration_ms: 0,
                    span_count: 0,
                    min_timestamp_ms: i64::MAX,
                    max_timestamp_ms: 0,
                    last_seen: now,
                    processing: false,
                });

            entry.span_count += 1;
            entry.has_error = entry.has_error || span.status_code == 2;
            entry.max_duration_ms = entry.max_duration_ms.max(span.duration_ms);
            entry.min_timestamp_ms = entry.min_timestamp_ms.min(span.timestamp_ms);
            entry.max_timestamp_ms = entry.max_timestamp_ms.max(span.timestamp_ms);
            entry.last_seen = now;
        }
    }

    async fn refresh_metadata_cache(&self) -> Result<()> {
        let catalog = self.get_catalog()?;
        let table = catalog
            .load_table(&self.table_ident)
            .await
            .context("Failed to load Iceberg table")?;

        let stream = table
            .scan()
            .select(["trace_id", "status_code", "duration_ms", "timestamp_ms"])
            .with_batch_size(Some(8192))
            .with_concurrency_limit(4)
            .with_row_group_filtering_enabled(true)
            .build()?
            .to_arrow()
            .await?;

        let batches: Vec<_> = stream.try_collect().await?;

        let mut trace_summaries: HashMap<String, CachedTraceMeta> = HashMap::new();

        for batch in batches {
            let trace_ids: Option<&StringArray> = batch
                .column_by_name("trace_id")
                .and_then(|c| c.as_any().downcast_ref());
            let status_codes: Option<&Int32Array> = batch
                .column_by_name("status_code")
                .and_then(|c| c.as_any().downcast_ref());
            let durations: Option<&Int64Array> = batch
                .column_by_name("duration_ms")
                .and_then(|c| c.as_any().downcast_ref());
            let timestamps: Option<&Int64Array> = batch
                .column_by_name("timestamp_ms")
                .and_then(|c| c.as_any().downcast_ref());

            if let (Some(trace_ids), Some(status_codes), Some(durations), Some(timestamps)) =
                (trace_ids, status_codes, durations, timestamps)
            {
                for i in 0..batch.num_rows() {
                    let trace_id = trace_ids.value(i).to_string();
                    let status_code = status_codes.value(i);
                    let duration = durations.value(i);
                    let timestamp = timestamps.value(i);

                    let entry = trace_summaries.entry(trace_id).or_insert(CachedTraceMeta {
                        has_error: false,
                        max_duration_ms: 0,
                        span_count: 0,
                        min_timestamp_ms: i64::MAX,
                        max_timestamp_ms: 0,
                        last_seen: Instant::now(),
                        processing: false,
                    });

                    entry.span_count += 1;
                    entry.has_error = entry.has_error || status_code == 2;
                    entry.max_duration_ms = entry.max_duration_ms.max(duration);
                    entry.min_timestamp_ms = entry.min_timestamp_ms.min(timestamp);
                    entry.max_timestamp_ms = entry.max_timestamp_ms.max(timestamp);
                }
            }
        }

        let mut cache = self.metadata_cache.write();
        for (trace_id, meta) in cache.traces.iter() {
            if meta.processing {
                if let Some(new_meta) = trace_summaries.get_mut(trace_id) {
                    new_meta.processing = true;
                }
            }
        }
        cache.traces = trace_summaries;
        cache.last_refresh = Instant::now();

        debug!(
            "Refreshed Iceberg metadata cache: {} traces",
            cache.traces.len()
        );

        Ok(())
    }

    async fn query_trace_spans(&self, trace_id: &str) -> Result<Vec<BufferedSpan>> {
        let catalog = self.get_catalog()?;
        let table = catalog
            .load_table(&self.table_ident)
            .await
            .context("Failed to load Iceberg table")?;

        let predicate = Reference::new("trace_id").equal_to(iceberg::spec::Datum::string(trace_id));

        let stream = table
            .scan()
            .select([
                "trace_id",
                "span_id",
                "parent_span_id",
                "timestamp_ms",
                "duration_ms",
                "status_code",
                "span_kind",
                "service_name",
                "operation_name",
            ])
            .with_filter(predicate)
            .with_batch_size(Some(1024))
            .build()?
            .to_arrow()
            .await?;

        let batches: Vec<_> = stream.try_collect().await?;
        let mut spans = Vec::new();

        for batch in batches {
            let trace_ids: Option<&StringArray> = batch
                .column_by_name("trace_id")
                .and_then(|c| c.as_any().downcast_ref());
            let span_ids: Option<&StringArray> = batch
                .column_by_name("span_id")
                .and_then(|c| c.as_any().downcast_ref());
            let parent_span_ids: Option<&StringArray> = batch
                .column_by_name("parent_span_id")
                .and_then(|c| c.as_any().downcast_ref());
            let timestamps: Option<&Int64Array> = batch
                .column_by_name("timestamp_ms")
                .and_then(|c| c.as_any().downcast_ref());
            let durations: Option<&Int64Array> = batch
                .column_by_name("duration_ms")
                .and_then(|c| c.as_any().downcast_ref());
            let status_codes: Option<&Int32Array> = batch
                .column_by_name("status_code")
                .and_then(|c| c.as_any().downcast_ref());
            let span_kinds: Option<&Int32Array> = batch
                .column_by_name("span_kind")
                .and_then(|c| c.as_any().downcast_ref());
            let service_names: Option<&StringArray> = batch
                .column_by_name("service_name")
                .and_then(|c| c.as_any().downcast_ref());
            let operation_names: Option<&StringArray> = batch
                .column_by_name("operation_name")
                .and_then(|c| c.as_any().downcast_ref());

            if let (
                Some(trace_ids),
                Some(span_ids),
                Some(timestamps),
                Some(durations),
                Some(status_codes),
                Some(service_names),
                Some(operation_names),
            ) = (
                trace_ids,
                span_ids,
                timestamps,
                durations,
                status_codes,
                service_names,
                operation_names,
            ) {
                for i in 0..batch.num_rows() {
                    let parent_span_id = parent_span_ids.and_then(|p| {
                        if p.is_null(i) {
                            None
                        } else {
                            let val = p.value(i);
                            if val.is_empty() {
                                None
                            } else {
                                Some(val.to_string())
                            }
                        }
                    });

                    let span_kind = span_kinds
                        .map(|k| SpanKind::from(k.value(i)))
                        .unwrap_or(SpanKind::Unspecified);

                    spans.push(BufferedSpan {
                        trace_id: trace_ids.value(i).to_string(),
                        span_id: span_ids.value(i).to_string(),
                        parent_span_id,
                        timestamp_ms: timestamps.value(i),
                        duration_ms: durations.value(i),
                        status_code: status_codes.value(i),
                        span_kind,
                        service_name: service_names.value(i).to_string(),
                        operation_name: operation_names.value(i).to_string(),
                        attributes: HashMap::new(),
                    });
                }
            }
        }

        Ok(spans)
    }
}

#[async_trait]
impl TraceStore for IcebergTraceStore {
    async fn ingest(&self, spans: &[BufferedSpan]) -> Result<IngestResult> {
        if spans.is_empty() {
            return Ok(IngestResult {
                added: 0,
                dropped: 0,
            });
        }

        let catalog = self.get_catalog()?;
        let batch = self.spans_to_record_batch(spans)?;
        let span_count = batch.num_rows();

        // Retry loop for optimistic concurrency conflicts
        const MAX_RETRIES: u32 = 5;
        let mut last_error = None;

        for attempt in 0..MAX_RETRIES {
            // Load fresh table metadata on each attempt to get latest version
            let table = match catalog.load_table(&self.table_ident).await {
                Ok(t) => t,
                Err(e) => {
                    last_error = Some(anyhow::anyhow!("Failed to load table: {}", e));
                    debug!(attempt = attempt + 1, error = %e, "Failed to load table");
                    tokio::time::sleep(Duration::from_millis(50 * (attempt as u64 + 1))).await;
                    continue;
                }
            };

            let location_generator = match DefaultLocationGenerator::new(table.metadata().clone()) {
                Ok(g) => g,
                Err(e) => {
                    last_error = Some(anyhow::anyhow!("Failed to create location generator: {}", e));
                    debug!(attempt = attempt + 1, error = %e, "Failed to create location generator");
                    continue;
                }
            };
            // Use UUID suffix to ensure unique file names across writer instances
            // Without this, each writer starts file_count at 0, causing "data-00000.parquet" collisions
            let unique_suffix = uuid::Uuid::new_v4().to_string();
            let file_name_generator =
                DefaultFileNameGenerator::new("data".to_string(), Some(unique_suffix), DataFileFormat::Parquet);

            let parquet_props = WriterProperties::builder().build();

            let parquet_writer_builder = ParquetWriterBuilder::new(
                parquet_props,
                table.metadata().current_schema().clone(),
                None,
                table.file_io().clone(),
                location_generator,
                file_name_generator,
            );

            let data_file_writer_builder =
                DataFileWriterBuilder::new(parquet_writer_builder, None, 0);
            let mut data_file_writer = match data_file_writer_builder.build().await {
                Ok(w) => w,
                Err(e) => {
                    last_error = Some(anyhow::anyhow!("Failed to build data file writer: {}", e));
                    debug!(attempt = attempt + 1, error = %e, "Failed to build data file writer");
                    continue;
                }
            };

            if let Err(e) = data_file_writer.write(batch.clone()).await {
                last_error = Some(anyhow::anyhow!("Failed to write batch: {}", e));
                debug!(attempt = attempt + 1, error = %e, "Failed to write batch");
                continue;
            }

            let data_files = match data_file_writer.close().await {
                Ok(f) => f,
                Err(e) => {
                    last_error = Some(anyhow::anyhow!("Failed to close writer: {}", e));
                    debug!(attempt = attempt + 1, error = %e, "Failed to close writer");
                    continue;
                }
            };

            if data_files.is_empty() {
                return Ok(IngestResult {
                    added: 0,
                    dropped: 0,
                });
            }

            debug!(attempt = attempt + 1, file_count = data_files.len(), "Data files written, creating transaction");

            let tx = Transaction::new(&table);
            let action = tx.fast_append().add_data_files(data_files);

            let tx = match action.apply(tx) {
                Ok(tx) => tx,
                Err(e) => {
                    last_error = Some(anyhow::anyhow!("Failed to apply action: {}", e));
                    debug!(attempt = attempt + 1, error = %e, "Failed to apply transaction action");
                    continue;
                }
            };

            debug!(attempt = attempt + 1, "Committing transaction to catalog");

            match tx.commit(catalog.as_ref()).await {
                Ok(_updated_table) => {
                    self.update_cache_for_spans(spans);

                    if attempt > 0 {
                        debug!(
                            span_count = span_count,
                            attempt = attempt + 1,
                            "Ingested spans to Iceberg after retry"
                        );
                    } else {
                        info!(span_count = span_count, "Ingested spans to Iceberg");
                    }

                    return Ok(IngestResult {
                        added: span_count,
                        dropped: 0,
                    });
                }
                Err(e) => {
                    last_error = Some(anyhow::anyhow!("Failed to commit transaction: {}", e));
                    debug!(
                        attempt = attempt + 1,
                        max_retries = MAX_RETRIES,
                        error = %e,
                        "Iceberg commit failed, retrying with fresh table metadata"
                    );
                    // Small delay before retry to reduce contention
                    tokio::time::sleep(Duration::from_millis(50 * (attempt as u64 + 1))).await;
                }
            }
        }

        Err(last_error.unwrap_or_else(|| anyhow::anyhow!("Failed to commit after {} retries", MAX_RETRIES)))
    }

    async fn get_ready_traces(&self, inactivity_threshold: Duration) -> Result<Vec<String>> {
        {
            let cache = self.metadata_cache.read();
            if cache.last_refresh.elapsed() < Duration::from_secs(5) {
                let ready: Vec<String> = cache
                    .traces
                    .iter()
                    .filter(|(_, meta)| {
                        !meta.processing && meta.last_seen.elapsed() > inactivity_threshold
                    })
                    .map(|(id, _)| id.clone())
                    .collect();
                return Ok(ready);
            }
        }

        self.refresh_metadata_cache().await?;

        let cache = self.metadata_cache.read();
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let threshold_ms = inactivity_threshold.as_millis() as i64;

        let ready: Vec<String> = cache
            .traces
            .iter()
            .filter(|(_, meta)| !meta.processing && (now_ms - meta.max_timestamp_ms) > threshold_ms)
            .map(|(id, _)| id.clone())
            .collect();

        Ok(ready)
    }

    async fn get_trace_summary(&self, trace_id: &str) -> Result<Option<TraceSummary>> {
        {
            let cache = self.metadata_cache.read();
            if let Some(meta) = cache.traces.get(trace_id) {
                return Ok(Some(TraceSummary {
                    trace_id: trace_id.to_string(),
                    service_name: String::new(),
                    span_count: meta.span_count,
                    has_error: meta.has_error,
                    max_duration_ms: meta.max_duration_ms,
                    min_timestamp_ms: meta.min_timestamp_ms,
                    max_timestamp_ms: meta.max_timestamp_ms,
                    operations: vec![],
                    total_spans: meta.span_count,
                    error_count: if meta.has_error { 1 } else { 0 },
                    root_span_id: None,
                }));
            }
        }

        let spans = self.query_trace_spans(trace_id).await?;
        if spans.is_empty() {
            return Ok(None);
        }

        let mut has_error = false;
        let mut error_count = 0;
        let mut max_duration_ms = 0i64;
        let mut min_timestamp_ms = i64::MAX;
        let mut max_timestamp_ms = 0i64;
        let mut operations: HashSet<String> = HashSet::new();
        let mut root_span_id = None;
        let mut service_name = String::new();

        for span in &spans {
            if span.status_code == 2 {
                has_error = true;
                error_count += 1;
            }
            max_duration_ms = max_duration_ms.max(span.duration_ms);
            min_timestamp_ms = min_timestamp_ms.min(span.timestamp_ms);
            max_timestamp_ms = max_timestamp_ms.max(span.timestamp_ms);
            operations.insert(span.operation_name.clone());
            if span.parent_span_id.is_none() {
                root_span_id = Some(span.span_id.clone());
            }
            if service_name.is_empty() {
                service_name = span.service_name.clone();
            }
        }

        Ok(Some(TraceSummary {
            trace_id: trace_id.to_string(),
            service_name,
            span_count: spans.len(),
            has_error,
            max_duration_ms,
            min_timestamp_ms,
            max_timestamp_ms,
            operations: operations.into_iter().collect(),
            total_spans: spans.len(),
            error_count,
            root_span_id,
        }))
    }

    async fn get_trace_spans(&self, trace_id: &str) -> Result<Option<Vec<BufferedSpan>>> {
        let spans = self.query_trace_spans(trace_id).await?;
        if spans.is_empty() {
            Ok(None)
        } else {
            Ok(Some(spans))
        }
    }

    async fn remove_traces(&self, trace_ids: &[String]) -> Result<usize> {
        let mut cache = self.metadata_cache.write();
        let mut removed = 0;
        for trace_id in trace_ids {
            if cache.traces.remove(trace_id).is_some() {
                removed += 1;
            }
        }
        Ok(removed)
    }

    async fn mark_processing(&self, trace_ids: &[String]) -> Result<()> {
        let mut cache = self.metadata_cache.write();
        for trace_id in trace_ids {
            if let Some(meta) = cache.traces.get_mut(trace_id) {
                meta.processing = true;
            }
        }
        Ok(())
    }

    async fn get_all_trace_ids(&self) -> Result<Vec<String>> {
        let cache = self.metadata_cache.read();
        Ok(cache.traces.keys().cloned().collect())
    }

    async fn stats(&self) -> Result<StoreStats> {
        let cache = self.metadata_cache.read();
        let total_spans: usize = cache.traces.values().map(|m| m.span_count).sum();
        let cache_size = cache.traces.len() * std::mem::size_of::<CachedTraceMeta>();

        Ok(StoreStats {
            span_count: total_spans,
            trace_count: cache.traces.len(),
            memory_bytes: cache_size,
            storage_type: StorageType::Iceberg,
        })
    }

    fn storage_type(&self) -> StorageType {
        StorageType::Iceberg
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl std::fmt::Debug for IcebergTraceStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IcebergTraceStore")
            .field("config", &self.config)
            .field("table_ident", &self.table_ident)
            .finish()
    }
}
