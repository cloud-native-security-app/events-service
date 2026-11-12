# events-service

Read side of the `security-events` Kinesis stream. It owns the Kinesis
consumer, the aggregator (KPIs, critical findings, recent events, scans in
flight) and the admin feed (JSON snapshot and SSE) that the gateway proxies to
the admin panel.

Internal only: it sits behind the gateway (Service Connect, no public load
balancer) and rejects every request except `/health` unless it carries the
shared secret.

> Status: the consumer, aggregator, event contract validator and admin routes
> moved here from the gateway with the same behavior and payloads. Time series
> and OpenAPI come later (AP-05).

Language: this README and the code comments are in English; the other service
READMEs are in Spanish.

## Configuration

| Variable | Required | Description |
|---|---|---|
| `HTTP_HOST` | yes | Host to bind, for example `0.0.0.0`. |
| `HTTP_PORT` | yes | TCP port to bind, for example `8080`. |
| `GATEWAY_SHARED_SECRET` | yes | Secret the gateway sends in the `X-Gateway-Secret` header. Blank counts as missing. Never logged. |
| `SECURITY_EVENTS_STREAM_NAME` | no | Kinesis stream to read. When unset the consumer does not start, a warning is logged and the admin routes serve an empty aggregator. |
| `AWS_REGION` | no | AWS region of the stream, default `us-east-1`. |
| `AWS_ENDPOINT_URL` | no | Endpoint override, only for LocalStack. |
| `RUST_LOG` | no | `tracing` filter, default `info`. |

## HTTP

- `GET /health`: unauthenticated, `200` when the process is up.
- Every other route requires `X-Gateway-Secret` (constant-time compare);
  otherwise `401`.
- `GET /metrics`: JSON snapshot (`seq`, `kpis`, `scans_in_flight`,
  `critical_findings`, `recent_events`).
- `GET /events`: `text/event-stream`. First a `snapshot` event, then one
  `delta` event per new event or malformed record, a fresh `snapshot` after a
  client lag and every 60 s, and `: ping` comments every 15 s. The stream ends
  when the service shuts down.

The admin identity is not checked here: authorization (session, admin role and
its periodic re-check) stays in the gateway.

## Behavior after a restart

The consumer reads the stream directly (no KCL, no checkpoint store). The first
iterator of each shard worker is `AT_TIMESTAMP` at `now - 15 min`, so a
restarted instance replays the last window and the panel is not empty. The
`event_id` dedupe absorbs any overlap. If an iterator expires while running,
the worker resumes from `LATEST` (the gap is lost, a `warn` is logged). A shard
created by a resharding also starts with the 15-minute replay.

## Shutdown

`SIGTERM` (ECS) and `Ctrl-C` cancel one token: the consumer loop stops, open
SSE streams end, the HTTP server drains (10 s at most) and the process exits.

The moved modules keep their original Spanish comments.

## Run and test

```sh
HTTP_HOST=127.0.0.1 HTTP_PORT=8080 GATEWAY_SHARED_SECRET=dev-only cargo run
curl -i localhost:8080/health

make verify            # fmt --check, clippy -D warnings, tests, LocalStack tests, docs
make check             # same without the Docker-dependent tests
make test-integration  # only the LocalStack round trip (needs Docker)
```

Config tests use `Config::from_lookup`, so they never touch the process
environment.

## CI and deploy

- `ci.yml`: `make verify` and a Docker build on pull requests and pushes to
  `develop`.
- `deploy.yml`: on push to `main`, builds and pushes
  `security-app/events-service` to ECR and rolls the `events-service` ECS
  service in `security-app-cluster-v2`.
