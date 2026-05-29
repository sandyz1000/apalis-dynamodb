# apalis-dynamodb

A [DynamoDB](https://aws.amazon.com/dynamodb/) storage backend for [apalis](https://apalis.dev/) — a simple, extensible multithreaded background job processing library for Rust.

## Overview

`apalis-dynamodb` implements the `Storage` and `Backend` traits from `apalis-core`, allowing you to use DynamoDB as the persistence layer for your apalis workers. It uses a **single-table design** where both task and worker records share one table, differentiated by their partition key prefix.

### Table Layout

| Entity | `#pk` (Partition Key) | `#sk` (Sort Key)  | Description                                                           |
|--------|-----------------------|-------------------|-----------------------------------------------------------------------|
| Task   | `task#<ulid>`         | `Pending`         | Sort key is immutable; mutable state lives in the `status` attribute  |
| Worker | `worker#<worker_id>`  | `<job_namespace>` | Heartbeat record for each worker                                      |

Notable task attributes: `id`, `status`, `run_at`, `attempts`, `max_attempts`, `job`, `job_type`, `lock_by`, `lock_at`, `done_at`, `last_error`.

## Setup

Add the dependency to `Cargo.toml`:

```toml
[dependencies]
apalis-dynamodb = { git = "https://github.com/your-repo/apalis-dynamodb" }
apalis = { version = "0.7", features = ["tokio-comp"] }
tokio = { version = "1", features = ["full"] }
serde = { version = "1", features = ["derive"] }
aws-config = "1"
aws-sdk-dynamodb = "1"
```

### Local DynamoDB (for development/testing)

Run DynamoDB Local with Docker:

```bash
docker run -p 8000:8000 amazon/dynamodb-local
```

Or with Docker Compose:

```yaml
# docker-compose.yml
services:
  dynamodb-local:
    image: amazon/dynamodb-local
    ports:
      - "8000:8000"
    command: ["-jar", "DynamoDBLocal.jar", "-sharedDb"]
```

Set dummy credentials for local use:

```bash
export AWS_DEFAULT_REGION=us-east-1
export AWS_ACCESS_KEY_ID=dummy
export AWS_SECRET_ACCESS_KEY=dummy
```

## Quick Start

```rust
use apalis::prelude::*;
use apalis_dynamodb::{DynamoStorage, Config};
use aws_config::BehaviorVersion;
use aws_sdk_dynamodb::Client;
use serde::{Deserialize, Serialize};
use std::time::Duration;

// 1. Define your job type
#[derive(Debug, Serialize, Deserialize, Clone)]
struct SendEmail {
    pub to: String,
    pub subject: String,
    pub body: String,
}

// 2. Define the job handler
async fn send_email(job: SendEmail) -> Result<(), Error> {
    println!("Sending email to {}: {}", job.to, job.subject);
    // ... your email sending logic
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 3. Connect to DynamoDB (uses AWS_* env vars or ~/.aws/credentials)
    let aws_config = aws_config::defaults(BehaviorVersion::latest()).load().await;
    let client = Client::new(&aws_config);

    // 4. Create the storage (creates the table if it doesn't exist)
    let mut storage = DynamoStorage::<SendEmail>::new(
        client,
        true,             // create table if missing
        "apalis-jobs".to_string(),
    )
    .await?;

    // 5. Push some jobs
    storage.push(SendEmail {
        to: "alice@example.com".into(),
        subject: "Welcome!".into(),
        body: "Hello, world!".into(),
    })
    .await?;

    // 6. Start the worker
    Monitor::new()
        .register(
            WorkerBuilder::new("email-worker")
                .backend(storage)
                .build(service_fn(send_email)),
        )
        .run()
        .await?;

    Ok(())
}
```

## Scheduled Jobs

Push a job to run at a specific future timestamp:

```rust
use chrono::{Utc, Duration};

let run_at = (Utc::now() + Duration::hours(2)).timestamp();
storage.schedule(SendEmail { ... }, run_at).await?;
```

## Configuration

`Config` is a builder that lets you tune the storage behaviour:

```rust
use apalis_dynamodb::Config;
use std::time::Duration;

let config = Config::new("my_app::SendEmail")  // job namespace / type discriminator
    .poll_interval(Duration::from_millis(100)) // how often to scan for pending jobs (default: 50ms)
    .keep_alive(Duration::from_secs(30))       // worker heartbeat interval (default: 30s)
    .buffer_size(20)                           // jobs fetched per poll cycle (default: 10)
    .reenqueue_orphaned_after(Duration::from_secs(300)); // dead-worker timeout (default: 5 min)

let storage = DynamoStorage::new_with_config(client, "apalis-jobs".to_string(), config);
```

### Namespace (job type)

The namespace acts as the job type discriminator stored in the `job_type` attribute. Workers only consume jobs whose `job_type` matches their namespace. When you use `DynamoStorage::<T>::new(...)`, the namespace defaults to `std::any::type_name::<T>()`.

Set it explicitly to use a stable, human-readable name:

```rust
Config::new("my_app::SendEmail")
```

## Additional Operations

```rust
// Re-queue a running job (e.g., after a transient error)
storage.retry(&worker_id, &job_id).await?;

// Permanently kill a job
storage.kill(&worker_id, &job_id).await?;

// Re-queue failed jobs that still have remaining attempts
storage.reenqueue_failed().await?;

// Re-queue jobs from workers that haven't heartbeated since `cutoff`
let cutoff = (Utc::now() - Duration::minutes(5)).timestamp();
storage.reenqueue_orphaned(cutoff).await?;

// Remove all Done and Killed jobs (cleanup)
let removed = storage.vacuum().await?;

// Check queue depth
let pending = storage.len().await?;
```

## Running Tests

Tests require a local DynamoDB instance on port 8000:

```bash
docker run -d -p 8000:8000 amazon/dynamodb-local

AWS_DEFAULT_REGION=us-east-1 \
AWS_ACCESS_KEY_ID=dummy \
AWS_SECRET_ACCESS_KEY=dummy \
cargo test -- --test-threads=1
```

`--test-threads=1` is required because all tests share the same DynamoDB table.

## Design Notes

- **Single-table design**: Tasks and workers coexist in one table, separated by key prefix (`task#` / `worker#`).
- **Immutable sort key**: The `#sk` attribute is set to `"Pending"` at creation and never changed (DynamoDB does not allow updating primary keys). The mutable job state lives in the `status` attribute.
- **Optimistic locking**: `fetch_next` uses a conditional `UpdateItem` to atomically transition a job from `Pending` to `Running`, preventing two workers from claiming the same job.
- **Scan-based queries**: Because `status` is not part of the primary key, this backend uses DynamoDB `Scan` with `FilterExpression`. This is suitable for moderate workloads; for large tables, consider adding a GSI on `status` + `run_at`.

## License

MIT
