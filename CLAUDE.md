# CLAUDE.md — Apache DolphinScheduler

Apache DolphinScheduler is a distributed, visual DAG workflow-scheduling platform. This is the monorepo: backend servers (master / worker / api / alert), a Vue 3 frontend, plugin families for tasks / datasources / storage / alerting / scheduling, and the release tooling.

**This file is an index.** Each module has its own `CLAUDE.md` with the details — do not duplicate module contents here.

---

## Tech stack (project-wide)

- **Java 1.8** (do not assume 11+ APIs; `dolphinscheduler-api-test` is the only Java 11 island).
- **Spring Boot 2.6.1** across servers, **Jetty** (Tomcat is excluded transitively).
- **MyBatis-Plus** for ORM; **HikariCP** for the metadata DB pool, **Druid** inside user-facing datasource plugins.
- **Quartz** for cron scheduling (via `scheduler-plugin`).
- **Netty / gRPC** for inter-server RPC (see `extract-base`).
- **Vue 3 + Vite + TypeScript + Naive UI** for the frontend.
- **Maven** multi-module reactor (26 modules in root `pom.xml` + 2 test modules).
- **Zookeeper 3.8** by default for the registry (Etcd and JDBC also supported).

## Runnable services

A production deployment runs **four independent services** (plus an external registry and metadata DB). A fifth entry point — `StandaloneServer` — embeds all four in one JVM for development.

| Service | Module | Main class | Default ports |
|---------|--------|------------|---------------|
| **API** | [`dolphinscheduler-api`](dolphinscheduler-api/CLAUDE.md) | `org.apache.dolphinscheduler.api.ApiApplicationServer` | `12345` (HTTP / UI + REST) |
| **Master** | [`dolphinscheduler-master`](dolphinscheduler-master/CLAUDE.md) | `org.apache.dolphinscheduler.server.master.MasterServer` | `5679` (RPC) |
| **Worker** | [`dolphinscheduler-worker`](dolphinscheduler-worker/CLAUDE.md) | `org.apache.dolphinscheduler.server.worker.WorkerServer` | `1235` (RPC) |
| **Alert** | [`dolphinscheduler-alert`](dolphinscheduler-alert/CLAUDE.md) (→ `-alert-server`) | `org.apache.dolphinscheduler.alert.AlertServer` | `50053` (HTTP), `50052` (RPC) |
| Standalone (dev only) | [`dolphinscheduler-standalone-server`](dolphinscheduler-standalone-server/CLAUDE.md) | `org.apache.dolphinscheduler.StandaloneServer` | `12345` + `50052` (API + alert; master/worker use in-JVM calls) |

Every service is a `@SpringBootApplication` on Jetty and implements `IStoppable`. Scale Master / Worker / Alert horizontally; coordination happens via the registry (Zookeeper by default). API is stateless and also scales horizontally behind a load balancer.

Ports are overridable via `server.port` / service-specific keys in each service's `application.yaml`.

## Build & run

```bash
# Full build (release profile; produces dist tarball)
./mvnw clean install -Prelease

# Zookeeper 3.4 legacy
./mvnw clean install -Prelease -Dzk-3.4

# Skip UI build (faster iteration on backend only)
./mvnw -pl '!dolphinscheduler-ui' clean install

# Build one module (+ its required siblings)
./mvnw -pl dolphinscheduler-master -am clean install

# Format (Spotless is configured)
./mvnw spotless:apply

# Standalone server (after building)
cd dolphinscheduler-standalone-server/target && ./bin/start.sh
```

Binary artifact: `dolphinscheduler-dist/target/apache-dolphinscheduler-*-bin.tar.gz`.

How to run a module's tests is documented in that module's own `CLAUDE.md` — commands, fork settings, and per-module gotchas vary too much to centralize here.

---

## Module index

Click into a module's `CLAUDE.md` for details. Each description is one line here on purpose.

### Core execution

- [`dolphinscheduler-master`](dolphinscheduler-master/CLAUDE.md) — workflow orchestration engine; consumes `Command`s, runs the DAG state machine, dispatches to workers.
- [`dolphinscheduler-worker`](dolphinscheduler-worker/CLAUDE.md) — runs physical tasks dispatched from master; hosts task plugins.
- [`dolphinscheduler-task-executor`](dolphinscheduler-task-executor/CLAUDE.md) — reusable task-lifecycle framework embedded by the worker.
- [`dolphinscheduler-alert`](dolphinscheduler-alert/CLAUDE.md) — alert server + channel plugins (email, Feishu, DingTalk, …).

### API layer

- [`dolphinscheduler-api`](dolphinscheduler-api/CLAUDE.md) — REST API server (entry point for UI, Python SDK, external clients).
- [`dolphinscheduler-api-test`](dolphinscheduler-api-test/CLAUDE.md) — integration tests against the REST API (Docker Compose + Testcontainers).
- [`dolphinscheduler-authentication`](dolphinscheduler-authentication/CLAUDE.md) — Actuator-endpoint auth + AWS credential helpers (NOT the main login path).

### Shared libraries

- [`dolphinscheduler-common`](dolphinscheduler-common/CLAUDE.md) — foundation utilities (everything depends on this).
- [`dolphinscheduler-dao`](dolphinscheduler-dao/CLAUDE.md) — MyBatis DAO layer + SQL migration scripts.
- [`dolphinscheduler-service`](dolphinscheduler-service/CLAUDE.md) — business logic between DAO and the servers.
- [`dolphinscheduler-spi`](dolphinscheduler-spi/CLAUDE.md) — Service-Provider Interface root (every plugin depends on this).
- [`dolphinscheduler-extract`](dolphinscheduler-extract/CLAUDE.md) — RPC interface contracts between servers.
- [`dolphinscheduler-eventbus`](dolphinscheduler-eventbus/CLAUDE.md) — in-process event-bus abstractions.
- [`dolphinscheduler-registry`](dolphinscheduler-registry/CLAUDE.md) — pluggable registry (Zookeeper / Etcd / JDBC).
- [`dolphinscheduler-meter`](dolphinscheduler-meter/CLAUDE.md) — metrics (Prometheus) + server load-protection primitives.

### Plugin families

- [`dolphinscheduler-task-plugin`](dolphinscheduler-task-plugin/CLAUDE.md) — task-type plugins (shell, SQL, Spark, Flink, K8s, EMR, …). 33 concrete plugins.
- [`dolphinscheduler-datasource-plugin`](dolphinscheduler-datasource-plugin/CLAUDE.md) — user-facing datasource plugins (MySQL, Hive, Trino, Snowflake, …). 28 concrete plugins.
- [`dolphinscheduler-storage-plugin`](dolphinscheduler-storage-plugin/CLAUDE.md) — resource storage (S3, HDFS, OSS, GCS, ABS, OBS, COS).
- [`dolphinscheduler-scheduler-plugin`](dolphinscheduler-scheduler-plugin/CLAUDE.md) — cron scheduler (Quartz today).
- [`dolphinscheduler-dao-plugin`](dolphinscheduler-dao-plugin/CLAUDE.md) — metadata-DB dialect support (MySQL / PostgreSQL / H2).

### Build, ops, tools

- [`dolphinscheduler-bom`](dolphinscheduler-bom/CLAUDE.md) — Maven BOM; central dependency version pinning.
- [`dolphinscheduler-dist`](dolphinscheduler-dist/CLAUDE.md) — assembles the release tarball + Docker images.
- [`dolphinscheduler-standalone-server`](dolphinscheduler-standalone-server/CLAUDE.md) — all-in-one JVM with H2 (dev / smoke tests).
- [`dolphinscheduler-tools`](dolphinscheduler-tools/CLAUDE.md) — CLIs for schema upgrade + resource / lineage migration.
- [`dolphinscheduler-microbench`](dolphinscheduler-microbench/CLAUDE.md) — JMH micro-benchmarks.
- [`dolphinscheduler-yarn-aop`](dolphinscheduler-yarn-aop/CLAUDE.md) — AspectJ weaver capturing YARN ApplicationIds.

### Frontend & E2E

- [`dolphinscheduler-ui`](dolphinscheduler-ui/CLAUDE.md) — Vue 3 frontend.
- [`dolphinscheduler-e2e`](dolphinscheduler-e2e/CLAUDE.md) — Selenium browser tests.

---

## Architecture overview (one paragraph)

A **user** hits the UI, which calls the API server. The API server writes to the **metadata DB** and, for runtime operations (start / kill / pause workflow), talks to the **master** over RPC. The master consumes `t_ds_command` rows, runs the workflow state machine, and dispatches tasks to **workers**. Workers execute task plugins (shell, SQL, Spark, …) and stream lifecycle events back to master. Failures and SLA breaches flow to the **alert server**, which fans out through alert plugins. **Registry** (Zookeeper / Etcd / JDBC) provides service discovery, leader election, and distributed locks. **Storage plugins** back the resource center and distributed-task artifacts. **Quartz** (via scheduler plugin) fires scheduled workflows, which become new `Command` rows.

## Where things live (quick lookup)

| Looking for… | Start here |
|--------------|------------|
| A REST endpoint | `dolphinscheduler-api/src/main/java/.../api/controller/` |
| Workflow execution logic | `dolphinscheduler-master/src/main/java/.../server/master/engine/` |
| Task execution logic | `dolphinscheduler-worker` + the specific `task-plugin/<type>` |
| How "X" is stored | `dolphinscheduler-dao/src/main/java/.../dao/entity/` |
| SQL schema / upgrade | `dolphinscheduler-dao/src/main/resources/sql/` |
| RPC contract between servers | `dolphinscheduler-extract/dolphinscheduler-extract-<role>` |
| UI page source | `dolphinscheduler-ui/src/views/<feature>/` |
| API call in the UI | `dolphinscheduler-ui/src/service/modules/<resource>.ts` |
| Version of a dependency | `dolphinscheduler-bom/pom.xml` |

## Project-wide conventions

- **Formatting**: `./mvnw spotless:apply`. CI will fail PRs that aren't formatted. Java imports are ordered; license headers are enforced.
- **Branching**: `dev` is the main integration branch (not `main`/`master`).
- **PRs must link a GitHub issue** and keep their scope tight — one module / one concern.
- **Do not break wire / DB compatibility** silently. Changes to `extract-*` RPC interfaces, `dao` entities, enum values, and `spi.DbType` ripple to deployed clusters mid-upgrade.
- **Only one registry / storage / DB dialect is active at runtime**. Code paths that check "which one" belong inside the plugin SPI, not sprinkled through services.

### Commit message template

```
[<Type>-<ISSUE_ID>][<Scope>] <Imperative subject — what changed, present tense>

<Optional body: WHY this change is needed, and the WHY behind any non-obvious
choice. Wrap at ~72 chars. Skip if the subject already says everything.>
```

- `<Type>`: one of `Fix` (bug fix), `Improvement` (enhancement to existing behavior), `Feature` (net-new capability), `Chore` (build/CI/refactor with no behavior change), `Doc` (docs only), `DSIP-N` (an accepted DolphinScheduler Improvement Proposal).
- `<ISSUE_ID>`: the linked GitHub issue number, no `#`. Omit `-<ISSUE_ID>` only for `Chore` items that genuinely have no tracking issue (e.g. `[Chore]`, `[Chore][CI]`).
- `<Scope>`: the affected module's short name in the existing commit log: `Master`, `Worker`, `API`, `UI`, `TaskPlugin`, `JdbcRegistry`, `Pom`, `CI`, `Doc`, etc. Pick the narrowest accurate one.
- `<Subject>`: imperative mood (`Fix X`, `Add Y`, `Remove Z`), no trailing period, ≤72 chars. Don't restate the type or scope.
- No space between the bracket groups (`[Fix-18222][Master]`, not `[Fix-18222] [Master]`) — that's the prevailing style in recent history.
- Don't append `(#PR_ID)` manually; GitHub adds it on squash-merge.

Examples from the existing log:

```
[Fix-18222][JdbcRegistry] Reuse a singleton scheduler executor in JdbcRegistryThreadFactory
[Improvement-17795][Master] Add dispatch timeout checking logic to handle cases where the worker group does not exist or no workers are available
[Chore][API] Remove deprecated ProjectService#checkProjectAndAuth
[Doc-18193][dolphinscheduler-alert-http] Fix incorrect alert param doc
```

