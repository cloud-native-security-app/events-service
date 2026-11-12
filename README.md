# events-service

Read side of the `security-events` Kinesis stream. It will own the Kinesis
consumer, the aggregator (counters and time series) and the admin SSE feed
that the gateway proxies to the admin panel.

Internal only: it sits behind the gateway (Service Connect, no public load
balancer) and rejects every request except `/health` unless it carries the
shared secret.

> Status: scaffold. Config, `/health` and the shared-secret middleware exist;
> the consumer and admin routes move here from the gateway in a later task.

Language: this README and the code comments are in English; the other service
READMEs are in Spanish.

## Configuration

| Variable | Required | Description |
|---|---|---|
| `HTTP_HOST` | yes | Host to bind, for example `0.0.0.0`. |
| `HTTP_PORT` | yes | TCP port to bind, for example `8080`. |
| `GATEWAY_SHARED_SECRET` | yes | Secret the gateway sends in the `X-Gateway-Secret` header. Blank counts as missing. Never logged. |
| `SECURITY_EVENTS_STREAM_NAME` | no | Kinesis stream to read (used by the consumer later). |
| `AWS_REGION` | no | AWS region (used by the consumer later). |
| `RUST_LOG` | no | `tracing` filter, default `info`. |

## HTTP

- `GET /health`: unauthenticated, `200` when the process is up.
- Every other route requires `X-Gateway-Secret` (constant-time compare);
  otherwise `401`.

## Run and test

```sh
HTTP_HOST=127.0.0.1 HTTP_PORT=8080 GATEWAY_SHARED_SECRET=dev-only cargo run
curl -i localhost:8080/health

make verify   # cargo fmt --check, clippy -D warnings, tests, docs
```

Config tests use `Config::from_lookup`, so they never touch the process
environment.

## CI and deploy

- `ci.yml`: `make verify` and a Docker build on pull requests and pushes to
  `develop`.
- `deploy.yml`: on push to `main`, builds and pushes
  `security-app/events-service` to ECR and rolls the `events-service` ECS
  service in `security-app-cluster-v2`.
