use crate::context::{DynamoContext, TaskState};
use crate::error::{LibError, Result};
use apalis_core::backend::Backend;
use apalis_core::codec::json::JsonCodec;
use apalis_core::codec::Codec;
use apalis_core::error::Error as ApalisError;
use apalis_core::layers::{Ack, AckLayer};
use apalis_core::poller::controller::Controller;
use apalis_core::poller::stream::BackendStream;
use apalis_core::poller::Poller;
use apalis_core::request::{Parts, Request, RequestStream};
use apalis_core::response::Response;
use apalis_core::storage::Storage;
use apalis_core::task::attempt::Attempt;
use apalis_core::task::namespace::Namespace;
use apalis_core::task::task_id::TaskId;
use apalis_core::worker::{Context, Event, Worker, WorkerId};
use async_stream::try_stream;
use aws_sdk_dynamodb::{
    client::Client,
    error::SdkError,
    operation::put_item::PutItemError,
    types::{
        AttributeDefinition, AttributeValue, KeySchemaElement, KeyType, ProvisionedThroughput,
        ScalarAttributeType,
    },
};
use chrono::Utc;
use futures::{FutureExt, Stream, StreamExt, TryStreamExt};
use serde::{de::DeserializeOwned, Serialize};
use std::collections::HashMap;
use std::marker::PhantomData;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use std::{fmt, io};

const ATTR_TASK_STATUS: &str = "status";
const ATTR_TASK_RUNAT: &str = "run_at";
const ATTR_TASK_ATTEMPTS: &str = "attempts";
const ATTR_TASK_MAX_ATTEMPTS: &str = "max_attempts";
const ATTR_TASK_LAST_ERROR: &str = "last_error";
const ATTR_TASK_LOCK_AT: &str = "lock_at";
const ATTR_TASK_LOCK_BY: &str = "lock_by";
const ATTR_TASK_DONE_AT: &str = "done_at";

const TASK_PARTITION_KEY_NAME: &str = "task";
const WORKER_PARTITION_KEY_NAME: &str = "worker";

type AttributeMap = HashMap<String, AttributeValue>;

/// Config for DynamoDB storage
#[derive(Debug, Clone)]
pub struct Config {
    keep_alive: Duration,
    buffer_size: usize,
    poll_interval: Duration,
    reenqueue_orphaned_after: Duration,
    namespace: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            keep_alive: Duration::from_secs(30),
            buffer_size: 10,
            poll_interval: Duration::from_millis(50),
            reenqueue_orphaned_after: Duration::from_secs(300),
            namespace: String::from("apalis::dynamo"),
        }
    }
}

impl Config {
    /// Create a config with a specific job namespace
    pub fn new(namespace: &str) -> Self {
        Config::default().set_namespace(namespace)
    }

    /// Set the namespace (job type discriminator)
    pub fn set_namespace(mut self, namespace: &str) -> Self {
        self.namespace = namespace.to_string();
        self
    }

    /// Interval between database poll queries. Defaults to 50ms.
    pub fn poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    /// Interval between worker keep-alive updates. Defaults to 30s.
    pub fn keep_alive(mut self, keep_alive: Duration) -> Self {
        self.keep_alive = keep_alive;
        self
    }

    /// Job buffer size per poll cycle. Defaults to 10.
    pub fn buffer_size(mut self, buffer_size: usize) -> Self {
        self.buffer_size = buffer_size;
        self
    }

    /// Time before a running job with a dead worker is re-queued. Defaults to 5 minutes.
    pub fn reenqueue_orphaned_after(mut self, after: Duration) -> Self {
        self.reenqueue_orphaned_after = after;
        self
    }
}

/// Represents a [`Storage`] that persists to DynamoDB
///
/// Uses a single-table design:
/// - Task items: PK = `task#<id>`, SK = `Pending` (immutable)
/// - Worker items: PK = `worker#<id>`, SK = `<job_type>`
/// - Mutable status is stored in the `status` attribute
pub struct DynamoStorage<T, C = JsonCodec<String>> {
    /// The DynamoDB client
    pub client: Client,
    /// The DynamoDB table name
    pub table_name: String,
    /// Controller for the job stream
    pub controller: Controller,
    /// Storage configuration
    pub config: Config,
    codec: PhantomData<(T, C)>,
}

impl<T, C> fmt::Debug for DynamoStorage<T, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DynamoStorage")
            .field("table_name", &self.table_name)
            .field("controller", &self.controller)
            .field("config", &self.config)
            .field("codec", &std::any::type_name::<C>())
            .finish()
    }
}

impl<T, C> Clone for DynamoStorage<T, C> {
    fn clone(&self) -> Self {
        DynamoStorage {
            client: self.client.clone(),
            controller: self.controller.clone(),
            config: self.config.clone(),
            codec: PhantomData,
            table_name: self.table_name.clone(),
        }
    }
}

// ── Table helpers ──────────────────────────────────────────────────────────────

async fn create_table(
    client: &Client,
    table_name: String,
    partition_key: String,
    sort_key: Option<String>,
) -> Result<()> {
    let ad = AttributeDefinition::builder()
        .attribute_name(&partition_key)
        .attribute_type(ScalarAttributeType::S)
        .build()?;

    let ks = KeySchemaElement::builder()
        .attribute_name(&partition_key)
        .key_type(KeyType::Hash)
        .build()?;

    let pt = ProvisionedThroughput::builder()
        .read_capacity_units(5)
        .write_capacity_units(5)
        .build()?;

    let mut req = client
        .create_table()
        .table_name(table_name)
        .attribute_definitions(ad)
        .key_schema(ks)
        .provisioned_throughput(pt);

    if let Some(sk) = &sort_key {
        req = req
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name(sk.clone())
                    .attribute_type(ScalarAttributeType::S)
                    .build()?,
            )
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name(sk.clone())
                    .key_type(KeyType::Range)
                    .build()?,
            );
    }

    let _ = req.send().await?;
    Ok(())
}

async fn put(db: &Client, table_name: &str, item: AttributeMap) -> Result<()> {
    if let Err(e) = db.put_item().table_name(table_name).set_item(Some(item)).send().await {
        if matches!(
            &e,
            SdkError::<PutItemError>::ServiceError(err)
            if matches!(err.err(), PutItemError::ConditionalCheckFailedException(_))
        ) {
            return Err(LibError::Concurrency);
        }
        return Err(LibError::DynamoPut(e));
    }
    Ok(())
}

fn context_to_attr(task: DynamoContext, partition_key: &str) -> AttributeMap {
    let mut m: AttributeMap = HashMap::new();
    let task_id = task.id.to_string();

    m.insert("#pk".into(), AttributeValue::S(format!("{partition_key}#{task_id}")));
    m.insert("#sk".into(), AttributeValue::S(TaskState::Pending.to_string()));
    m.insert("id".into(), AttributeValue::S(task_id));
    m.insert(ATTR_TASK_STATUS.into(), AttributeValue::S(task.status.to_string()));
    m.insert(ATTR_TASK_RUNAT.into(), AttributeValue::N(task.run_at().to_string()));
    m.insert(ATTR_TASK_ATTEMPTS.into(), AttributeValue::N(task.attempts().to_string()));
    m.insert(ATTR_TASK_MAX_ATTEMPTS.into(), AttributeValue::N(task.max_attempts().to_string()));

    if let Some(err) = task.last_error() {
        m.insert(ATTR_TASK_LAST_ERROR.into(), AttributeValue::S(err.clone()));
    }
    if let Some(lock_at) = task.lock_at() {
        m.insert(ATTR_TASK_LOCK_AT.into(), AttributeValue::N(lock_at.to_string()));
    }
    if let Some(lock_by) = task.lock_by() {
        m.insert(ATTR_TASK_LOCK_BY.into(), AttributeValue::S(lock_by.to_string()));
    }
    if let Some(done_at) = task.done_at() {
        m.insert(ATTR_TASK_DONE_AT.into(), AttributeValue::N(done_at.to_string()));
    }

    m
}

fn attr_to_context(item: &AttributeMap) -> Result<DynamoContext> {
    let id = item
        .get("id")
        .ok_or_else(|| LibError::MalformedObject("missing id".into()))?
        .as_s()
        .map_err(|_| LibError::MalformedObject("id is not a string".into()))
        .and_then(|s| TaskId::from_str(s).map_err(|e| LibError::MalformedObject(e.to_string())))?;

    let status = item
        .get(ATTR_TASK_STATUS)
        .ok_or_else(|| LibError::MalformedObject("missing status".into()))?
        .as_s()
        .map_err(|_| LibError::MalformedObject("status is not a string".into()))
        .and_then(|s| TaskState::from_str(s).map_err(|e| LibError::MalformedObject(e.to_string())))?;

    let run_at = item
        .get(ATTR_TASK_RUNAT)
        .ok_or_else(|| LibError::MalformedObject("missing run_at".into()))?
        .as_n()
        .map_err(|_| LibError::MalformedObject("run_at is not a number".into()))
        .and_then(|s| s.parse::<i64>().map_err(|e| LibError::MalformedObject(e.to_string())))?;

    let attempts = item
        .get(ATTR_TASK_ATTEMPTS)
        .ok_or_else(|| LibError::MalformedObject("missing attempts".into()))?
        .as_n()
        .map_err(|_| LibError::MalformedObject("attempts is not a number".into()))
        .and_then(|s| s.parse::<i32>().map_err(|e| LibError::MalformedObject(e.to_string())))?;

    let max_attempts = item
        .get(ATTR_TASK_MAX_ATTEMPTS)
        .ok_or_else(|| LibError::MalformedObject("missing max_attempts".into()))?
        .as_n()
        .map_err(|_| LibError::MalformedObject("max_attempts is not a number".into()))
        .and_then(|s| s.parse::<i32>().map_err(|e| LibError::MalformedObject(e.to_string())))?;

    let last_error = item.get(ATTR_TASK_LAST_ERROR).and_then(|v| v.as_s().ok()).cloned();
    let lock_at = item.get(ATTR_TASK_LOCK_AT).and_then(|v| v.as_n().ok()).and_then(|s| s.parse::<i64>().ok());
    let lock_by = item.get(ATTR_TASK_LOCK_BY).and_then(|v| v.as_s().ok()).and_then(|s| WorkerId::from_str(s).ok());
    let done_at = item.get(ATTR_TASK_DONE_AT).and_then(|v| v.as_n().ok()).and_then(|s| s.parse::<i64>().ok());

    Ok(DynamoContext { id, status, run_at, attempts, max_attempts, last_error, lock_at, lock_by, done_at })
}

// ── DynamoStorage construction ─────────────────────────────────────────────────

impl<T> DynamoStorage<T> {
    /// Create a new storage, optionally creating the table.
    /// The namespace defaults to `std::any::type_name::<T>()`.
    pub async fn new(
        client: Client,
        check_table_exists: bool,
        table_name: String,
    ) -> Result<Self> {
        if check_table_exists {
            let resp = client.list_tables().send().await?;
            let names = resp.table_names();
            if !names.contains(&table_name) {
                tracing::info!("table not found, creating now");
                create_table(&client, table_name.clone(), "#pk".to_string(), Some("#sk".to_string())).await?;
            }
        }
        Ok(Self {
            client,
            controller: Controller::new(),
            config: Config::new(std::any::type_name::<T>()),
            codec: PhantomData,
            table_name,
        })
    }

    /// Create a new storage with a custom config
    pub fn new_with_config(client: Client, table_name: String, config: Config) -> Self {
        Self {
            client,
            controller: Controller::new(),
            config,
            codec: PhantomData,
            table_name,
        }
    }
}

impl<T, C> DynamoStorage<T, C> {
    /// Create the DynamoDB table if it does not already exist.
    /// Call this once at application start when using [`new_with_config`].
    pub async fn ensure_table_exists(&self) -> Result<()> {
        let resp = self.client.list_tables().send().await?;
        if !resp.table_names().contains(&self.table_name) {
            tracing::info!("table {} not found, creating", self.table_name);
            create_table(
                &self.client,
                self.table_name.clone(),
                "#pk".to_string(),
                Some("#sk".to_string()),
            )
            .await?;
        }
        Ok(())
    }
}

impl<T, C> DynamoStorage<T, C> {
    /// Expose the DynamoDB client for custom operations
    pub fn pool(&self) -> &Client {
        &self.client
    }

    /// Register or update a worker's heartbeat timestamp
    pub async fn keep_alive_at(&mut self, worker_id: &WorkerId, last_seen: i64) -> Result<()> {
        let worker_type = self.config.namespace.clone();
        let storage_name = std::any::type_name::<Self>();
        let partition_key = format!("{WORKER_PARTITION_KEY_NAME}#{worker_id}");

        let mut attr_values: AttributeMap = HashMap::new();
        attr_values.insert(":last_seen".into(), AttributeValue::N(last_seen.to_string()));
        attr_values.insert(":worker_type".into(), AttributeValue::S(worker_type.clone()));
        attr_values.insert(":storage_name".into(), AttributeValue::S(storage_name.to_string()));
        attr_values.insert(":worker_id".into(), AttributeValue::S(worker_id.to_string()));

        self.client
            .update_item()
            .table_name(&self.table_name)
            .key("#pk", AttributeValue::S(partition_key))
            .key("#sk", AttributeValue::S(worker_type))
            .update_expression("SET last_seen = :last_seen, id = :worker_id, storage_name = :storage_name, worker_type = :worker_type")
            .set_expression_attribute_values(Some(attr_values))
            .send()
            .await
            .map_err(|e| LibError::DynamoUpdate(e))?;

        Ok(())
    }

    /// Put a running job back to Pending so another worker can claim it
    pub async fn retry(&mut self, worker_id: &WorkerId, job_id: &TaskId) -> Result<()> {
        let partition_key = format!("{TASK_PARTITION_KEY_NAME}#{job_id}");

        let mut attr_values: AttributeMap = HashMap::new();
        attr_values.insert(":pending".into(), AttributeValue::S(TaskState::Pending.to_string()));
        attr_values.insert(":lock_by".into(), AttributeValue::S(worker_id.to_string()));

        self.client
            .update_item()
            .table_name(&self.table_name)
            .key("#pk", AttributeValue::S(partition_key))
            .key("#sk", AttributeValue::S(TaskState::Pending.to_string()))
            .condition_expression("lock_by = :lock_by")
            .update_expression("SET #status = :pending REMOVE lock_by, lock_at, done_at")
            .expression_attribute_names("#status", "status")
            .set_expression_attribute_values(Some(attr_values))
            .send()
            .await
            .map_err(|e| LibError::DynamoUpdate(e))?;

        Ok(())
    }

    /// Permanently kill a job
    pub async fn kill(&mut self, worker_id: &WorkerId, job_id: &TaskId) -> Result<()> {
        let partition_key = format!("{TASK_PARTITION_KEY_NAME}#{job_id}");
        let done_at = Utc::now().timestamp();

        let mut attr_values: AttributeMap = HashMap::new();
        attr_values.insert(":status".into(), AttributeValue::S(TaskState::Killed.to_string()));
        attr_values.insert(":done_at".into(), AttributeValue::N(done_at.to_string()));
        attr_values.insert(":lock_by".into(), AttributeValue::S(worker_id.to_string()));

        self.client
            .update_item()
            .table_name(&self.table_name)
            .key("#pk", AttributeValue::S(partition_key))
            .key("#sk", AttributeValue::S(TaskState::Pending.to_string()))
            .condition_expression("lock_by = :lock_by")
            .update_expression("SET #status = :status, done_at = :done_at")
            .expression_attribute_names("#status", "status")
            .set_expression_attribute_values(Some(attr_values))
            .send()
            .await
            .map_err(|e| LibError::DynamoUpdate(e))?;

        Ok(())
    }

    /// Re-enqueue failed jobs that still have remaining attempts
    pub async fn reenqueue_failed(&self) -> Result<()> {
        let job_type = &self.config.namespace;
        let scan_output = self
            .client
            .scan()
            .table_name(&self.table_name)
            .filter_expression(
                "begins_with(#task_pk, :task_prefix) AND #status = :failed AND attempts < max_attempts AND job_type = :job_type",
            )
            .expression_attribute_names("#task_pk", "#pk")
            .expression_attribute_names("#status", "status")
            .expression_attribute_values(":task_prefix", AttributeValue::S(format!("{TASK_PARTITION_KEY_NAME}#")))
            .expression_attribute_values(":failed", AttributeValue::S(TaskState::Failed.to_string()))
            .expression_attribute_values(":job_type", AttributeValue::S(job_type.clone()))
            .send()
            .await
            .map_err(|e| LibError::DynamoScanItems(e))?;

        for item in scan_output.items.unwrap_or_default() {
            if let Some(id) = item.get("id").and_then(|v| v.as_s().ok()) {
                let partition_key = format!("{TASK_PARTITION_KEY_NAME}#{id}");
                let mut attr_values = HashMap::new();
                attr_values.insert(":pending".to_string(), AttributeValue::S(TaskState::Pending.to_string()));

                self.client
                    .update_item()
                    .table_name(&self.table_name)
                    .key("#pk", AttributeValue::S(partition_key))
                    .key("#sk", AttributeValue::S(TaskState::Pending.to_string()))
                    .update_expression("SET #status = :pending REMOVE lock_by, lock_at, done_at")
                    .expression_attribute_names("#status", "status")
                    .set_expression_attribute_values(Some(attr_values))
                    .send()
                    .await
                    .map_err(|e| LibError::DynamoUpdate(e))?;
            }
        }

        Ok(())
    }

    /// Re-enqueue jobs whose workers have disappeared (last_seen < timeout).
    pub async fn reenqueue_orphaned(&self, timeout: i64) -> Result<()> {
        let job_type = &self.config.namespace;

        let worker_scan = self
            .client
            .scan()
            .table_name(&self.table_name)
            .filter_expression(
                "begins_with(#task_pk, :worker_prefix) AND worker_type = :worker_type AND last_seen < :last_seen",
            )
            .expression_attribute_names("#task_pk", "#pk")
            .expression_attribute_values(":worker_prefix", AttributeValue::S(format!("{WORKER_PARTITION_KEY_NAME}#")))
            .expression_attribute_values(":worker_type", AttributeValue::S(job_type.clone()))
            .expression_attribute_values(":last_seen", AttributeValue::N(timeout.to_string()))
            .send()
            .await
            .map_err(|e| LibError::DynamoScanItems(e))?;

        let worker_ids: Vec<String> = worker_scan
            .items
            .unwrap_or_default()
            .into_iter()
            .filter_map(|item| {
                item.get("#pk")
                    .and_then(|v| v.as_s().ok())
                    .and_then(|s| s.strip_prefix(&format!("{WORKER_PARTITION_KEY_NAME}#")))
                    .map(|id| id.to_string())
            })
            .collect();

        if worker_ids.is_empty() {
            return Ok(());
        }

        let running_scan = self
            .client
            .scan()
            .table_name(&self.table_name)
            .filter_expression("begins_with(#task_pk, :task_prefix) AND #status = :running")
            .expression_attribute_names("#task_pk", "#pk")
            .expression_attribute_names("#status", "status")
            .expression_attribute_values(":task_prefix", AttributeValue::S(format!("{TASK_PARTITION_KEY_NAME}#")))
            .expression_attribute_values(":running", AttributeValue::S(TaskState::Running.to_string()))
            .send()
            .await
            .map_err(|e| LibError::DynamoScanItems(e))?;

        let orphaned_ids: Vec<String> = running_scan
            .items
            .unwrap_or_default()
            .into_iter()
            .filter_map(|item| {
                let job_id = item.get("id").and_then(|v| v.as_s().ok()).map(|s| s.to_string())?;
                let lock_by = item.get(ATTR_TASK_LOCK_BY).and_then(|v| v.as_s().ok()).map(|s| s.to_string())?;
                if worker_ids.contains(&lock_by) { Some(job_id) } else { None }
            })
            .collect();

        for job_id in orphaned_ids {
            let partition_key = format!("{TASK_PARTITION_KEY_NAME}#{job_id}");
            let mut attr_values = HashMap::new();
            attr_values.insert(":pending".to_string(), AttributeValue::S(TaskState::Pending.to_string()));
            attr_values.insert(":last_error".to_string(), AttributeValue::S("Job was abandoned".to_string()));

            self.client
                .update_item()
                .table_name(&self.table_name)
                .key("#pk", AttributeValue::S(partition_key))
                .key("#sk", AttributeValue::S(TaskState::Pending.to_string()))
                .update_expression("SET #status = :pending, last_error = :last_error REMOVE lock_by, lock_at, done_at")
                .expression_attribute_names("#status", "status")
                .set_expression_attribute_values(Some(attr_values))
                .send()
                .await
                .map_err(|e| LibError::DynamoUpdate(e))?;
        }

        Ok(())
    }
}

// ── fetch_next: atomically lock a pending job ──────────────────────────────────

async fn fetch_next<T>(
    db: Client,
    worker_id: &WorkerId,
    id: String,
    table_name: &str,
    partition_key: &str,
    job_type: &str,
) -> Result<(DynamoContext, String)> {
    let now: i64 = Utc::now().timestamp();

    let mut attr_values = HashMap::new();
    attr_values.insert(":job_type".into(), AttributeValue::S(job_type.to_string()));
    attr_values.insert(":pending".into(), AttributeValue::S(TaskState::Pending.to_string()));
    attr_values.insert(":running".into(), AttributeValue::S(TaskState::Running.to_string()));
    attr_values.insert(":lock_by".into(), AttributeValue::S(worker_id.to_string()));
    attr_values.insert(":lock_at".into(), AttributeValue::N(now.to_string()));

    // Conditionally transition the job from Pending to Running
    let _ = db
        .update_item()
        .table_name(table_name)
        .key("#pk", AttributeValue::S(format!("{partition_key}#{id}")))
        .key("#sk", AttributeValue::S(TaskState::Pending.to_string()))
        .condition_expression("job_type = :job_type AND #status = :pending AND attribute_not_exists(lock_by)")
        .update_expression("SET #status = :running, lock_by = :lock_by, lock_at = :lock_at")
        .expression_attribute_names("#status", "status")
        .set_expression_attribute_values(Some(attr_values))
        .send()
        .await;

    // Fetch the full item to get the updated context and job payload
    let result = db
        .get_item()
        .table_name(table_name)
        .key("#pk", AttributeValue::S(format!("{partition_key}#{id}")))
        .key("#sk", AttributeValue::S(TaskState::Pending.to_string()))
        .send()
        .await?;

    let Some(item) = result.item else {
        return Err(LibError::ItemNotFound);
    };

    let job_type_found = item
        .get("job_type")
        .ok_or_else(|| LibError::MalformedObject("missing job_type".into()))?
        .as_s()
        .map_err(|_| LibError::MalformedObject("job_type is not a string".into()))?;

    if job_type_found != job_type {
        return Err(LibError::ItemNotFound);
    }

    let job_json = item
        .get("job")
        .ok_or_else(|| LibError::MalformedObject("missing job payload".into()))?
        .as_s()
        .map_err(|_| LibError::MalformedObject("job is not a string".into()))?
        .clone();

    let mut context = attr_to_context(&item)?;
    context.status = TaskState::Running;
    context.lock_by = Some(worker_id.clone());

    Ok((context, job_json))
}

// ── stream_jobs ────────────────────────────────────────────────────────────────

impl<T, C> DynamoStorage<T, C>
where
    T: DeserializeOwned + Send + Unpin,
    C: Codec<Compact = String> + Send + Sync + 'static,
    C::Error: std::error::Error + Send + Sync + 'static,
{
    fn stream_jobs(
        &self,
        worker_id: WorkerId,
        interval: Duration,
        _buffer_size: usize,
    ) -> impl Stream<Item = Result<Option<Request<T, DynamoContext>>>> {
        let client = self.client.clone();
        let table_name = self.table_name.clone();
        let namespace = Namespace(self.config.namespace.clone());
        let job_type = self.config.namespace.clone();
        let partition_key = TASK_PARTITION_KEY_NAME.to_string();

        try_stream! {
            loop {
                apalis_core::sleep(interval).await;

                let now: i64 = Utc::now().timestamp();

                let mut attr_names: HashMap<String, String> = HashMap::new();
                attr_names.insert("#task_pk".into(), "#pk".into());
                attr_names.insert("#status".into(), "status".into());

                let mut attr_value: AttributeMap = HashMap::new();
                attr_value.insert(":task_prefix".into(), AttributeValue::S(format!("{TASK_PARTITION_KEY_NAME}#")));
                attr_value.insert(":pending".into(), AttributeValue::S(TaskState::Pending.to_string()));
                attr_value.insert(":failed".into(), AttributeValue::S(TaskState::Failed.to_string()));
                attr_value.insert(":run_at".into(), AttributeValue::N(now.to_string()));
                attr_value.insert(":job_type".into(), AttributeValue::S(job_type.clone()));

                let result = client
                    .scan()
                    .table_name(&table_name)
                    .filter_expression("begins_with(#task_pk, :task_prefix) AND (#status = :pending OR (#status = :failed AND attempts < max_attempts)) AND run_at <= :run_at AND job_type = :job_type")
                    .set_expression_attribute_names(Some(attr_names))
                    .set_expression_attribute_values(Some(attr_value))
                    .send()
                    .await?;

                let pending_items = result.items.unwrap_or_default();

                for item in pending_items {
                    let id = match item.get("id").and_then(|v| v.as_s().ok()) {
                        Some(id) => id.clone(),
                        None => continue,
                    };

                    let res = fetch_next::<T>(
                        client.clone(),
                        &worker_id,
                        id,
                        &table_name,
                        &partition_key,
                        &job_type,
                    )
                    .await;

                    yield match res {
                        Err(_) => None::<Request<T, DynamoContext>>,
                        Ok((context, job_json)) => {
                            let args = C::decode(job_json).map_err(|e| {
                                LibError::InvalidData(io::Error::new(io::ErrorKind::InvalidData, e))
                            })?;
                            let mut parts = Parts::<DynamoContext>::default();
                            parts.task_id = context.id.clone();
                            parts.attempt = Attempt::new_with_value(context.attempts as usize);
                            parts.context = context;
                            parts.namespace = Some(namespace.clone());
                            Some(Request::new_with_parts(args, parts))
                        }
                    };
                }
            }
        }
    }
}

// ── Storage trait ──────────────────────────────────────────────────────────────

fn calculate_status<Res>(ctx: &DynamoContext, res: &Response<Res>) -> TaskState {
    match &res.inner {
        Ok(_) => TaskState::Done,
        Err(e) => match e {
            ApalisError::Abort(_) => TaskState::Killed,
            ApalisError::Failed(_) if ctx.max_attempts() as usize <= res.attempt.current() => {
                TaskState::Killed
            }
            _ => TaskState::Failed,
        },
    }
}

impl<T, C> Storage for DynamoStorage<T, C>
where
    T: Serialize + DeserializeOwned + Send + 'static + Unpin + Sync,
    C: Codec<Compact = String> + Send + 'static + Sync,
    C::Error: std::error::Error + Send + Sync + 'static,
{
    type Job = T;
    type Error = LibError;
    type Context = DynamoContext;
    type Compact = String;

    async fn push_request(
        &mut self,
        job: Request<Self::Job, DynamoContext>,
    ) -> std::result::Result<Parts<DynamoContext>, LibError> {
        let (task, parts) = job.take_parts();
        let raw = C::encode(&task)
            .map_err(|e| LibError::InvalidData(io::Error::new(io::ErrorKind::InvalidData, e)))?;
        let job_type = self.config.namespace.clone();

        let context = DynamoContext::new(parts.task_id.clone());
        let mut item = context_to_attr(context, TASK_PARTITION_KEY_NAME);
        item.insert("job_type".into(), AttributeValue::S(job_type));
        item.insert("job".into(), AttributeValue::S(raw));

        put(&self.client, &self.table_name, item).await?;
        Ok(parts)
    }

    async fn push_raw_request(
        &mut self,
        job: Request<String, DynamoContext>,
    ) -> std::result::Result<Parts<DynamoContext>, LibError> {
        let (raw, parts) = job.take_parts();
        let job_type = self.config.namespace.clone();

        let context = DynamoContext::new(parts.task_id.clone());
        let mut item = context_to_attr(context, TASK_PARTITION_KEY_NAME);
        item.insert("job_type".into(), AttributeValue::S(job_type));
        item.insert("job".into(), AttributeValue::S(raw));

        put(&self.client, &self.table_name, item).await?;
        Ok(parts)
    }

    async fn schedule_request(
        &mut self,
        req: Request<Self::Job, DynamoContext>,
        on: i64,
    ) -> std::result::Result<Parts<DynamoContext>, LibError> {
        let (task, parts) = req.take_parts();
        let raw = C::encode(&task)
            .map_err(|e| LibError::InvalidData(io::Error::new(io::ErrorKind::InvalidData, e)))?;
        let job_type = self.config.namespace.clone();
        let id = &parts.task_id;
        let partition_key = format!("{TASK_PARTITION_KEY_NAME}#{id}");

        let mut attr_values: AttributeMap = HashMap::new();
        attr_values.insert(":job".into(), AttributeValue::S(raw));
        attr_values.insert(":job_type".into(), AttributeValue::S(job_type));
        attr_values.insert(":status".into(), AttributeValue::S(TaskState::Pending.to_string()));
        attr_values.insert(":run_at".into(), AttributeValue::N(on.to_string()));
        attr_values.insert(":id".into(), AttributeValue::S(id.to_string()));
        attr_values.insert(":attempts".into(), AttributeValue::N("0".into()));
        attr_values.insert(":max_attempts".into(), AttributeValue::N(parts.context.max_attempts.to_string()));

        self.client
            .update_item()
            .table_name(&self.table_name)
            .key("#pk", AttributeValue::S(partition_key))
            .key("#sk", AttributeValue::S(TaskState::Pending.to_string()))
            .update_expression("SET job = :job, job_type = :job_type, #status = :status, run_at = :run_at, id = :id, attempts = :attempts, max_attempts = :max_attempts")
            .expression_attribute_names("#status", "status")
            .set_expression_attribute_values(Some(attr_values))
            .send()
            .await
            .map_err(|e| LibError::DynamoUpdate(e))?;

        Ok(parts)
    }

    async fn fetch_by_id(
        &mut self,
        job_id: &TaskId,
    ) -> std::result::Result<Option<Request<T, DynamoContext>>, LibError> {
        let partition_key = format!("{TASK_PARTITION_KEY_NAME}#{job_id}");

        let result = self
            .client
            .get_item()
            .table_name(&self.table_name)
            .key("#pk", AttributeValue::S(partition_key))
            .key("#sk", AttributeValue::S(TaskState::Pending.to_string()))
            .send()
            .await
            .map_err(|e| LibError::DynamoGetItem(e))?;

        match result.item {
            None => Ok(None),
            Some(item) => {
                let context = attr_to_context(&item)?;
                let job_json = item
                    .get("job")
                    .ok_or_else(|| LibError::MalformedObject("missing job payload".into()))?
                    .as_s()
                    .map_err(|_| LibError::MalformedObject("job is not a string".into()))?;
                let args = C::decode(job_json.clone())
                    .map_err(|e| LibError::InvalidData(io::Error::new(io::ErrorKind::InvalidData, e)))?;

                let mut parts = Parts::<DynamoContext>::default();
                parts.task_id = context.id.clone();
                parts.attempt = Attempt::new_with_value(context.attempts as usize);
                parts.namespace = Some(Namespace(self.config.namespace.clone()));
                parts.context = context;

                Ok(Some(Request::new_with_parts(args, parts)))
            }
        }
    }

    async fn len(&mut self) -> std::result::Result<i64, LibError> {
        use aws_sdk_dynamodb::types::Select;

        let scan_response = self
            .client
            .scan()
            .table_name(&self.table_name)
            .filter_expression("#status = :pending AND begins_with(#task_pk, :task_prefix)")
            .expression_attribute_names("#status", "status")
            .expression_attribute_names("#task_pk", "#pk")
            .expression_attribute_values(":pending", AttributeValue::S(TaskState::Pending.to_string()))
            .expression_attribute_values(":task_prefix", AttributeValue::S(format!("{TASK_PARTITION_KEY_NAME}#")))
            .select(Select::Count)
            .send()
            .await
            .map_err(|e| LibError::DynamoScanItems(e))?;

        Ok(scan_response.count().into())
    }

    async fn reschedule(
        &mut self,
        job: Request<T, DynamoContext>,
        wait: Duration,
    ) -> std::result::Result<(), LibError> {
        let task_id = &job.parts.task_id;
        let wait_until = Utc::now().timestamp() + wait.as_secs() as i64;
        let partition_key = format!("{TASK_PARTITION_KEY_NAME}#{task_id}");

        let mut attr_values: AttributeMap = HashMap::new();
        attr_values.insert(":status".into(), AttributeValue::S(TaskState::Failed.to_string()));
        attr_values.insert(":run_at".into(), AttributeValue::N(wait_until.to_string()));

        self.client
            .update_item()
            .table_name(&self.table_name)
            .key("#pk", AttributeValue::S(partition_key))
            .key("#sk", AttributeValue::S(TaskState::Pending.to_string()))
            .update_expression("SET #status = :status, run_at = :run_at REMOVE lock_by, lock_at, done_at")
            .expression_attribute_names("#status", "status")
            .set_expression_attribute_values(Some(attr_values))
            .send()
            .await
            .map_err(|e| LibError::DynamoUpdate(e))?;

        Ok(())
    }

    async fn update(
        &mut self,
        job: Request<T, DynamoContext>,
    ) -> std::result::Result<(), LibError> {
        let ctx = &job.parts.context;
        let task_id = &job.parts.task_id;
        let partition_key = format!("{TASK_PARTITION_KEY_NAME}#{task_id}");

        let mut attr_values: AttributeMap = HashMap::new();
        attr_values.insert(":status".into(), AttributeValue::S(ctx.status.to_string()));
        attr_values.insert(":attempts".into(), AttributeValue::N(ctx.attempts.to_string()));

        let mut set_parts = vec!["#status = :status", "attempts = :attempts"];

        if let Some(done_at) = ctx.done_at {
            attr_values.insert(":done_at".into(), AttributeValue::N(done_at.to_string()));
            set_parts.push("done_at = :done_at");
        }
        if let Some(lock_at) = ctx.lock_at {
            attr_values.insert(":lock_at".into(), AttributeValue::N(lock_at.to_string()));
            set_parts.push("lock_at = :lock_at");
        }
        if let Some(lock_by) = &ctx.lock_by {
            attr_values.insert(":lock_by".into(), AttributeValue::S(lock_by.to_string()));
            set_parts.push("lock_by = :lock_by");
        }
        if let Some(last_error) = &ctx.last_error {
            attr_values.insert(":last_error".into(), AttributeValue::S(last_error.clone()));
            set_parts.push("last_error = :last_error");
        }

        let update_expr = format!("SET {}", set_parts.join(", "));

        self.client
            .update_item()
            .table_name(&self.table_name)
            .key("#pk", AttributeValue::S(partition_key))
            .key("#sk", AttributeValue::S(TaskState::Pending.to_string()))
            .update_expression(update_expr)
            .expression_attribute_names("#status", "status")
            .set_expression_attribute_values(Some(attr_values))
            .send()
            .await
            .map_err(|e| LibError::DynamoUpdate(e))?;

        Ok(())
    }

    async fn is_empty(&mut self) -> std::result::Result<bool, LibError> {
        Ok(self.len().await? == 0)
    }

    async fn vacuum(&mut self) -> std::result::Result<usize, LibError> {
        let scan_output = self
            .client
            .scan()
            .table_name(&self.table_name)
            .filter_expression(
                "begins_with(#task_pk, :task_prefix) AND (#status = :done OR #status = :killed)",
            )
            .expression_attribute_names("#task_pk", "#pk")
            .expression_attribute_names("#status", "status")
            .expression_attribute_values(":task_prefix", AttributeValue::S(format!("{TASK_PARTITION_KEY_NAME}#")))
            .expression_attribute_values(":done", AttributeValue::S(TaskState::Done.to_string()))
            .expression_attribute_values(":killed", AttributeValue::S(TaskState::Killed.to_string()))
            .send()
            .await
            .map_err(|e| LibError::DynamoScanItems(e))?;

        let mut deleted: usize = 0;
        for item in scan_output.items.unwrap_or_default() {
            if let (Some(pk), Some(sk)) = (item.get("#pk"), item.get("#sk")) {
                self.client
                    .delete_item()
                    .table_name(&self.table_name)
                    .key("#pk", pk.clone())
                    .key("#sk", sk.clone())
                    .send()
                    .await
                    .map_err(|e| LibError::DynamoDelete(e))?;
                deleted += 1;
            }
        }

        Ok(deleted)
    }
}

// ── Backend trait ──────────────────────────────────────────────────────────────

impl<T, C> Backend<Request<T, DynamoContext>> for DynamoStorage<T, C>
where
    C: Codec<Compact = String> + Send + 'static + Sync,
    C::Error: std::error::Error + Send + Sync + 'static,
    T: Serialize + DeserializeOwned + Sync + Send + Unpin + 'static,
{
    type Stream = BackendStream<RequestStream<Request<T, DynamoContext>>>;
    type Layer = AckLayer<DynamoStorage<T, C>, T, DynamoContext, C>;
    type Codec = C;

    fn poll(mut self, worker: &Worker<Context>) -> Poller<Self::Stream, Self::Layer> {
        let layer = AckLayer::new(self.clone());
        let config = self.config.clone();
        let controller = self.controller.clone();
        let worker_id = worker.id().clone();
        let stream = self
            .stream_jobs(worker_id.clone(), config.poll_interval, config.buffer_size)
            .map_err(|e| ApalisError::SourceError(Arc::new(Box::new(e))));
        let stream = BackendStream::new(stream.boxed(), controller);
        let w = worker.clone();
        let heartbeat = async move {
            loop {
                let now = Utc::now().timestamp();
                if let Err(e) = self.keep_alive_at(&worker_id, now).await {
                    w.emit(Event::Error(Box::new(e)));
                }
                apalis_core::sleep(config.keep_alive).await;
            }
        }
        .boxed();
        Poller::new_with_layer(stream, heartbeat, layer)
    }
}

// ── Ack trait ──────────────────────────────────────────────────────────────────

impl<T: Sync + Send, C: Send, Res: Serialize + Sync> Ack<T, Res, C> for DynamoStorage<T, C> {
    type Context = DynamoContext;
    type AckError = LibError;

    async fn ack(&mut self, ctx: &DynamoContext, res: &Response<Res>) -> std::result::Result<(), LibError> {
        let job_id = res.task_id.to_string();
        let status = calculate_status(ctx, res);
        let done_at = Utc::now().timestamp();
        let attempts = res.attempt.current() as i32;
        let last_error = res.inner.as_ref().err().map(|e| e.to_string());
        let partition_key = format!("{TASK_PARTITION_KEY_NAME}#{job_id}");

        let mut attr_values: AttributeMap = HashMap::new();
        attr_values.insert(":status".into(), AttributeValue::S(status.to_string()));
        attr_values.insert(":done_at".into(), AttributeValue::N(done_at.to_string()));
        attr_values.insert(":attempts".into(), AttributeValue::N(attempts.to_string()));

        let update_expr = if let Some(err) = &last_error {
            attr_values.insert(":last_error".into(), AttributeValue::S(err.clone()));
            "SET #status = :status, done_at = :done_at, attempts = :attempts, last_error = :last_error"
        } else {
            "SET #status = :status, done_at = :done_at, attempts = :attempts"
        };

        self.client
            .update_item()
            .table_name(&self.table_name)
            .key("#pk", AttributeValue::S(partition_key))
            .key("#sk", AttributeValue::S(TaskState::Pending.to_string()))
            .update_expression(update_expr)
            .expression_attribute_names("#status", "status")
            .set_expression_attribute_values(Some(attr_values))
            .send()
            .await
            .map_err(|e| LibError::DynamoUpdate(e))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::TaskState;
    use aws_config::{meta::region::RegionProviderChain, BehaviorVersion};
    use chrono::Utc;
    use futures::StreamExt;
    use serde::Deserialize;

    const TEST_DYNAMO_TABLE: &str = "dynamo-local";

    #[derive(Debug, Deserialize, Serialize, Clone)]
    struct Email {
        to: String,
        subject: String,
        text: String,
    }

    async fn setup() -> DynamoStorage<Email> {
        let region_provider = RegionProviderChain::default_provider();
        let aws_config = aws_config::defaults(BehaviorVersion::latest())
            .region(region_provider)
            .load()
            .await;
        let client = Client::new(&aws_config);

        DynamoStorage::<Email>::new(client, true, TEST_DYNAMO_TABLE.to_string())
            .await
            .unwrap()
    }

    fn example_email() -> Email {
        Email {
            subject: "Test Subject".to_string(),
            to: "example@test".to_string(),
            text: "Some Text".to_string(),
        }
    }

    async fn consume_one(storage: &DynamoStorage<Email>, worker_id: &WorkerId) -> Request<Email, DynamoContext> {
        let s1 = storage.clone();
        let mut stream = s1
            .stream_jobs(worker_id.clone(), std::time::Duration::from_secs(10), 1)
            .boxed();
        stream
            .next()
            .await
            .expect("stream is empty")
            .expect("failed to poll job")
            .expect("no job is pending")
    }

    async fn register_worker_at(storage: &mut DynamoStorage<Email>, last_seen: i64) -> WorkerId {
        let worker_id = WorkerId::new("test-worker");
        storage
            .keep_alive_at(&worker_id, last_seen)
            .await
            .expect("failed to register worker");
        worker_id
    }

    async fn register_worker(storage: &mut DynamoStorage<Email>) -> WorkerId {
        register_worker_at(storage, Utc::now().timestamp()).await
    }

    async fn push_email(storage: &mut DynamoStorage<Email>, email: Email) {
        storage.push(email).await.expect("failed to push a job");
    }

    async fn get_job(storage: &mut DynamoStorage<Email>, job_id: &TaskId) -> Request<Email, DynamoContext> {
        storage
            .fetch_by_id(job_id)
            .await
            .expect("failed to fetch job by id")
            .expect("no job found by id")
    }

    #[tokio::test]
    async fn test_inmemory_sqlite_worker() {
        let mut storage = setup().await;
        storage
            .push(Email {
                subject: "Test Subject".to_string(),
                to: "example@sqlite".to_string(),
                text: "Some Text".to_string(),
            })
            .await
            .expect("Unable to push job");
        let len = storage.len().await.expect("Could not fetch the jobs count");
        assert_eq!(len, 1);
    }

    #[tokio::test]
    async fn test_consume_last_pushed_job() {
        let mut storage = setup().await;
        push_email(&mut storage, example_email()).await;

        let worker_id = register_worker(&mut storage).await;
        let job = consume_one(&storage, &worker_id).await;
        let ctx = &job.parts.context;
        assert_eq!(*ctx.status(), TaskState::Running);
        assert_eq!(*ctx.lock_by(), Some(worker_id.clone()));
        assert!(ctx.lock_at().is_some());
    }

    #[tokio::test]
    async fn test_acknowledge_job() {
        let mut storage = setup().await;
        push_email(&mut storage, example_email()).await;

        let worker_id = register_worker(&mut storage).await;
        let job = consume_one(&storage, &worker_id).await;
        let job_id = job.parts.task_id.clone();

        storage
            .ack(
                &job.parts.context,
                &apalis_core::response::Response::success((), job_id.clone(), job.parts.attempt.clone()),
            )
            .await
            .expect("failed to acknowledge the job");

        let job = get_job(&mut storage, &job_id).await;
        let ctx = &job.parts.context;
        assert_eq!(*ctx.status(), TaskState::Done);
        assert!(ctx.done_at().is_some());
    }

    #[tokio::test]
    async fn test_kill_job() {
        let mut storage = setup().await;
        push_email(&mut storage, example_email()).await;

        let worker_id = register_worker(&mut storage).await;
        let job = consume_one(&storage, &worker_id).await;
        let job_id = job.parts.task_id.clone();

        storage
            .kill(&worker_id, &job_id)
            .await
            .expect("failed to kill job");

        let job = get_job(&mut storage, &job_id).await;
        let ctx = &job.parts.context;
        assert_eq!(*ctx.status(), TaskState::Killed);
        assert!(ctx.done_at().is_some());
    }

    #[tokio::test]
    async fn test_heartbeat_renqueueorphaned_pulse_last_seen_6min() {
        let mut storage = setup().await;

        push_email(&mut storage, example_email()).await;

        let six_minutes_ago = Utc::now() - Duration::from_secs(6 * 60);
        let now = Utc::now();

        let worker_id = register_worker_at(&mut storage, six_minutes_ago.timestamp()).await;
        let job = consume_one(&storage, &worker_id).await;
        let job_id = job.parts.task_id.clone();

        storage
            .reenqueue_orphaned(now.timestamp())
            .await
            .expect("failed to reenqueue orphaned jobs");

        let job = get_job(&mut storage, &job_id).await;
        let ctx = &job.parts.context;
        assert_eq!(*ctx.status(), TaskState::Pending);
        assert!(ctx.done_at().is_none());
        assert!(ctx.lock_by().is_none());
        assert!(ctx.lock_at().is_none());
        assert_eq!(*ctx.last_error(), Some("Job was abandoned".to_string()));
    }

    #[tokio::test]
    async fn test_heartbeat_renqueueorphaned_pulse_last_seen_4min() {
        let mut storage = setup().await;

        push_email(&mut storage, example_email()).await;

        let four_minutes_ago = Utc::now() - Duration::from_secs(4 * 60);
        let worker_id = register_worker_at(&mut storage, four_minutes_ago.timestamp()).await;
        let job = consume_one(&storage, &worker_id).await;
        let job_id = job.parts.task_id.clone();

        storage
            .reenqueue_orphaned(four_minutes_ago.timestamp())
            .await
            .expect("failed to heartbeat");

        let job = get_job(&mut storage, &job_id).await;
        let ctx = &job.parts.context;
        assert_eq!(*ctx.status(), TaskState::Running);
        assert_eq!(*ctx.lock_by(), Some(worker_id));
    }
}
