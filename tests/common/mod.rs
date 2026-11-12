//! Shared harness for the integration tests that run against a real
//! LocalStack (Kinesis) started with `testcontainers` — no mocks, no AWS
//! account. Each test starts its own container.

#![allow(dead_code)]

use std::time::Duration;

use aws_sdk_kinesis::config::{BehaviorVersion, Credentials, Region};
use aws_sdk_kinesis::types::StreamStatus;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage};

pub const LOCALSTACK_TAG: &str = "4.4";
pub const REGION: &str = "us-east-1";

/// LocalStack running, plus a Kinesis client already pointed at it. The
/// container is destroyed when this value is dropped.
pub struct LocalStack {
    _container: ContainerAsync<GenericImage>,
    pub endpoint: String,
    pub kinesis: aws_sdk_kinesis::Client,
}

impl LocalStack {
    pub async fn start() -> Self {
        let container = GenericImage::new("localstack/localstack", LOCALSTACK_TAG)
            .with_exposed_port(4566.tcp())
            .with_wait_for(WaitFor::message_on_stdout("Ready."))
            .start()
            .await
            .expect("the LocalStack test container must start");
        let port = container
            .get_host_port_ipv4(4566.tcp())
            .await
            .expect("LocalStack must publish port 4566");
        let endpoint = format!("http://127.0.0.1:{port}");
        let kinesis = aws_sdk_kinesis::Client::from_conf(
            aws_sdk_kinesis::Config::builder()
                .behavior_version(BehaviorVersion::latest())
                .region(Region::new(REGION))
                .endpoint_url(&endpoint)
                .credentials_provider(Credentials::new(
                    "test",
                    "test",
                    None,
                    None,
                    "localstack-test",
                ))
                .build(),
        );
        Self {
            _container: container,
            endpoint,
            kinesis,
        }
    }

    /// Creates the one-shard stream `name` and waits until it is `ACTIVE`.
    pub async fn create_stream(&self, name: &str) {
        self.kinesis
            .create_stream()
            .stream_name(name)
            .shard_count(1)
            .send()
            .await
            .expect("create the stream");
        for _ in 0..100 {
            let status = self
                .kinesis
                .describe_stream_summary()
                .stream_name(name)
                .send()
                .await
                .expect("describe the stream")
                .stream_description_summary
                .expect("stream summary")
                .stream_status;
            if status == StreamStatus::Active {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("stream {name} never became ACTIVE");
    }
}
