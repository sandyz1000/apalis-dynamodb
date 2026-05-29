//! A minimal example showing how to push and process Email jobs with apalis-dynamodb.
//!
//! Requirements:
//!   - DynamoDB Local running on port 8000 (see docker-compose.yml):
//!       docker compose up -d
//!
//! Environment variables are loaded from `.env.local` automatically.
//! Copy and edit the file if needed — the defaults target DynamoDB Local:
//!   AWS_DEFAULT_REGION=us-east-1
//!   AWS_ACCESS_KEY_ID=dummy
//!   AWS_SECRET_ACCESS_KEY=dummy
//!   AWS_ENDPOINT_URL=http://localhost:8000
//!
//! Run:
//!   cargo run --example email_worker

use apalis_core::{
    builder::{WorkerBuilder, WorkerFactory},
    monitor::Monitor,
    service_fn::service_fn,
    storage::Storage,
};
use apalis_dynamodb::{Config, DynamoStorage};
use dotenvy::from_filename;
use aws_config::BehaviorVersion;
use aws_sdk_dynamodb::Client;
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Email {
    pub to: String,
    pub subject: String,
    pub body: String,
}

async fn handle_email(email: Email) -> Result<(), std::io::Error> {
    println!(
        "[worker] Processing email → to={} subject={}",
        email.to, email.subject
    );
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load .env.local before aws-config reads env vars (silently ok if file is absent)
    from_filename(".env.local").ok();

    tracing_subscriber::fmt::init();

    // Connect to DynamoDB (reads AWS_* env vars, including those loaded above)
    let aws_cfg = aws_config::defaults(BehaviorVersion::latest()).load().await;
    let client = Client::new(&aws_cfg);

    let config = Config::new("examples::Email")
        .poll_interval(Duration::from_millis(200))
        .keep_alive(Duration::from_secs(30))
        .buffer_size(5);

    let mut storage =
        DynamoStorage::new_with_config(client, "apalis-email-example".to_string(), config);

    // Create the table if it doesn't exist (idempotent)
    storage.ensure_table_exists().await?;

    // Push a few jobs before starting the worker
    for i in 1..=3 {
        storage
            .push(Email {
                to: format!("user{}@example.com", i),
                subject: format!("Hello #{}", i),
                body: format!("This is message number {}.", i),
            })
            .await?;
        println!("[main] Pushed job {}", i);
    }

    println!("[main] Starting worker — press Ctrl-C to stop");

    Monitor::new()
        .register(
            WorkerBuilder::new("email-worker-1")
                .backend(storage)
                .build(service_fn(handle_email)),
        )
        .run()
        .await?;

    Ok(())
}
