# 5. Operations

How to set the project up, run it, test it, release it, and recover when
something goes wrong. For *why* things work this way, see the earlier pages.

## First-time setup

1. Install [uv](https://docs.astral.sh/uv/), the [Go Task](https://taskfile.dev)
   runner and the [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install).
2. Authenticate to the workspace: `databricks configure`.
3. Copy `.env.example` to `.env` and fill it in. `.env` is ignored by git and must
   never contain tokens. It names the connection and catalog settings:

   | Variable | Used for |
   |---|---|
   | `DATABRICKS_CONFIG_PROFILE`, `DATABRICKS_HOST` | Which workspace to talk to |
   | `DATABRICKS_SQL_HTTP_PATH`, `DATABRICKS_SQL_WAREHOUSE_ID` | The SQL warehouse dbt and queries use |
   | `DATABRICKS_TEST_CATALOG`, `DATABRICKS_DEV_CATALOG` | The catalogs of the TEST and DEV environments |
   | `GITHUB_ANALYTICS_*` | Catalog, repository and secret references for local runs |

   The GitHub token itself lives in a Databricks secret scope, not in this file.
4. `task setup`, then `task check`.

> **Always run tools through `uv run` or `task`.** A `dbt` or `pytest` started
> outside the project environment lacks the Databricks adapters and fails with
> messages such as "Could not find adapter type databricks".

## Environments

Defined as targets in [`databricks.yml`](../databricks.yml).

| Target | Catalog | Mode | Notes |
|---|---|---|---|
| `dev` | `github_engineering_analytics_dev` | development | Default target. Resources are prefixed with `[dev <user>]` and schedules are paused. |
| `test` | `github_engineering_analytics_test` | development | Where experiments, replays and snapshots run. |
| `prod` | `github_engineering_analytics_prod` | production | Single managed copy; not used for experiments. |

Deploying is always a deliberate act: nothing deploys on its own.

### Why replays and snapshots run only in TEST

This is a **choice**, not a technical limit. Each environment has its own job:

| Environment | Its job in this project |
|---|---|
| `dev` | Reserved for the integration tests, which **empty tables** in a dedicated DEV catalog. Nothing you want to keep belongs there. |
| `test` | Experiments, replays, snapshots and the evidence that something works. It is the place where breaking things is allowed. |
| `prod` | Only what is already proven. |

Replays and snapshots are *learning tools*: they write new tables and read frozen
history. Keeping them in TEST keeps the blast radius small while the ideas are
still being learned, and keeps experimental output away from the live watermark and
the real Gold tables.

How strongly this is enforced differs:

- **Replay: enforced in code.** It refuses any catalog that does not end in
  `_test`, and writes only to a fixed schema (`github_analytics_v2min`).
- **Snapshots: a convention.** The Job is named `[TEST] ...`, but the models would
  run in any catalog you deploy to.
- **Jobs travel with the bundle.** Both Jobs are defined once in `resources/` and
  are deployed to whichever target you deploy (the replay Job also appears under
  `dev` if you deploy there). The replay's guard is what protects you, not its location.

The long Bronze history that replays depend on was also set up **for TEST only**:
the TEST Bronze table keeps 30 days of Delta history. The other catalogs keep
Delta's defaults, so a replay there could fail because the Bronze version it needs
is already gone.

**To run them outside TEST you would have to:**

1. Relax the `_test` guard on purpose, and decide per environment whether that is acceptable.
2. Choose where the output lives and who may read it.
3. Raise the Bronze history retention to cover the versions you want to reproduce.
4. For daily snapshots, add a schedule (today they are started by hand) and monitoring.

Until there is a reason to do that, TEST is the right place.

## Jobs

All jobs are defined in [`resources/`](../resources) and deployed together.

| Job (resource key) | Purpose | Start with |
|---|---|---|
| `GitHub Issues Bronze-to-Gold load` (`github_issues_full_load`) | The normal pipeline: extract, Silver, Gold, then finalize. | `databricks bundle run -t test github_issues_full_load` |
| `GitHub Analytics Gold build` (`github_gold_build`) | Rebuild all Gold from Silver. | `databricks bundle run -t test github_gold_build` |
| `[TEST] GitHub Gold failure-path probe` (`github_gold_failure_probe`) | Proves a Gold failure leaves the watermark untouched. | `databricks bundle run -t test github_gold_failure_probe` |
| `[TEST] GitHub Silver replay` (`github_silver_replay`) | Replays a manifest into isolated Silver tables. | `task replay:silver:test -- manifest_id=<id>,execution_id=<new id>` |
| `[TEST] GitHub daily snapshot` (`github_daily_snapshot`) | Builds the daily snapshot models for a manifest and date range. | `databricks bundle run -t test github_daily_snapshot --params manifest_id=<id>,start_date=<date>,end_date=<date>` |

The replay and snapshot jobs have **empty default parameters on purpose**: a
forgotten value fails immediately instead of reading the wrong data.

### Passing parameters to a wheel task

A Databricks job parameter reaches a Python wheel task under **its own name**,
as `--manifest_id` (underscore). Values you list under `named_parameters` are
passed as written (for example `--token-secret-scope`). Do not repeat a job
parameter under `named_parameters`: it would arrive twice, under two spellings,
and the command-line parser would reject the unknown one.

## Commands

Run `task --list` for the full list. The ones you will use most:

| Task | What it does |
|---|---|
| `task setup` | Install dependencies. |
| `task check` | Lint, format check, type check and unit tests. Run before every commit. |
| `task test:unit` | Unit tests only; no Databricks needed. |
| `task test:integration` | Opt-in tests that need Databricks Connect and a dedicated DEV catalog. |
| `task lint`, `task format`, `task typecheck` | Individual checks. |
| `task bundle:validate:test` | Validate the TEST bundle. |
| `task bundle:deploy:test` | Deploy the TEST bundle (never implicit). |
| `task replay:silver:test -- manifest_id=...,execution_id=...` | Start a replay. |
| `task dbt:build-gold` | Build every Gold model. |
| `task dbt:test-sources` | Run the Silver contract tests. |
| `task dbt:docs SNAPSHOT_MANIFEST_ID=... SNAPSHOT_START_DATE=... SNAPSHOT_END_DATE=...` | Generate dbt documentation. The vars are required because the staging models read them. |
| `task test:baseline` | Run the TEST pipeline Job once, then show the run and watermark. Use it to give TEST a known starting point after a reset. It does not deploy. |
| `task monitor:health:test` | Recent runs and the committed watermark. |
| `task version:patch` / `minor` / `major` | Cut a release (see below). |

## Testing

| Layer | What | Needs Databricks |
|---|---|---|
| **Python unit tests** (`tests/unit`) | Logic with mocks: transformers, writers, repositories, entry points, reset helper, and a guard that dbt's JSON schema matches the Python schemas. | No |
| **Python integration tests** (`tests/integration`) | The same logic against real Delta tables in a dedicated DEV catalog. | Yes, opt-in |
| **dbt source tests** | Silver contracts: unique keys, required columns. Run as the gate before Gold. | Yes |
| **dbt model tests** | Gold grain and relationships. | Yes |
| **dbt unit tests** | The snapshot models against a fixture, with no real tables. | Yes (runs SQL) |

Quality gates enforced by `task check`: Ruff lint and format, basedpyright with
errors only as the gate, and `pytest -m "not integration"`.

Habit worth copying: after writing a test, **break the code on purpose** and
confirm the test fails (a *mutation check*). A test that cannot fail proves nothing.

## Releasing

```bash
task check
task version:patch    # or minor / major
```

Each version task refuses a dirty working tree, updates `pyproject.toml` and
`uv.lock`, creates a local commit and an annotated `v<version>` tag. It never
pushes. `task version:show-bump` previews the next versions. A bundle deploy
builds the wheel from the current version; the jobs reference it by path.

## Resetting a test catalog

Destructive, and double-keyed on purpose: you must set the catalog **and** repeat
its name as a confirmation, so a stale shell variable cannot reset the wrong one.

| Task | Empties |
|---|---|
| `task test:reset-data` | The Python-owned tables: Bronze, the four Silver tables, the watermark and `pipeline_runs`. |
| `task test:reset-all` | The above, plus the dbt Gold tables, the daily snapshot tables and `replay_manifests`. |
| `task dev:reset-data`, `task dev:reset-all` | The same for the DEV catalog. |

A reset uses `TRUNCATE`, which keeps the tables and their Delta history, so time
travel to older versions still works until the retention window ends. Replay
output tables (`github_analytics_v2min`) are never reset: remove them **by exact
name**. After a reset, ids written in notes or earlier output (manifests, runs)
no longer exist as rows, and the next pipeline run is a full load again.

## Troubleshooting

| Symptom | Likely cause and fix |
|---|---|
| "Could not find adapter type databricks" | The tool ran outside the project environment. Use `uv run ...` or `task`. |
| `NoSuchOption: --execution_id` (or similar) in a wheel task | A job parameter was also listed under `named_parameters`, or the option spelling differs. Job parameters arrive with underscores; see above. |
| `Not able to get columns for unit test ... relation doesn't exist` | A dbt unit test mocks an `ephemeral` model or an unbuilt table as dict rows. Use `format: sql` with explicit types. |
| dbt parse fails for the whole project | A unit test refers to a model that does not exist yet, or a required var is missing. Add the model, or pass `--vars`. |
| `Unknown manifest: ...` while compiling the snapshot models | The `snapshot_manifest_id` does not exist (for example after a reset). Create a new manifest. |
| A replay stops with "target tables already exist" | Execution ids cannot be reused. Pick a new `execution_id`. |
| Timestamps shifted by hours when read locally | Databricks Connect returns naive local time. Compare in Spark (`to_date`, UTC session) rather than in Python. |
| Run stuck in `running` | See [recovering failed runs](03-pipeline.md#when-something-fails). |

Related: [6. Change Data Feed](06-change-data-feed.md) covers the scratch scripts
and the integration tests for finding changed Issues, and
[7. The incremental Gold candidate](07-incremental-gold-candidate.md) covers the
scratch schemas and scripts of the incremental build.

Back to the [README](../README.md).
