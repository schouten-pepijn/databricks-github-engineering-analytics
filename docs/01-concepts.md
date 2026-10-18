# 1. Concepts

This page explains the **ideas** behind the repository. No tables or commands
yet; those come in the next documents. If you understand this page, the rest is
detail.

## The problem we are solving

We want to answer questions such as "how many issues are open?", "who opens the
most?" and "how old is the backlog?" about a GitHub repository.

That sounds easy: call the API, put the answer in a table. It stops being easy
when you ask the questions that real data platforms must answer:

- What if the job **fails halfway**? Do we have half the data? Do we re-read it?
- What if GitHub **changes an issue** after we read it? Which version is true?
- What if we find a **bug in our cleaning logic** next week? Can we fix the past?
- What did the backlog look like **last Tuesday**, not just today?

Every part of this repository exists to answer one of those questions.

## Four kinds of table

Data moves through layers, each with one job. This is the *medallion*
architecture.

| Layer | Analogy | Job | Rule |
|---|---|---|---|
| **Bronze** | A drawer of original receipts | Keep exactly what we read, when we read it | Append only. Never edit, never delete. |
| **Silver** | A tidy filing cabinet | Turn raw JSON into clean rows: one row per issue, user, label | Keep the **latest** observation per business key. |
| **Gold** | A finished report | Shape data for questions: facts and dimensions (a star schema) | Built by dbt, tested, rebuilt from Silver. |
| **Control** | The logbook | Record runs, progress, and replay recipes | Not analytics. Never feeds a report. |

Why separate layers? Because each answers a different failure:

- If cleaning is wrong, **Bronze is still intact**, so we can clean again.
- If a report model is wrong, **Silver is still intact**, so we can rebuild it.
- If a run dies, **Control** knows exactly how far it got.

## The principles

### 1. Bronze is evidence, so it is append-only

A Bronze row says: "at this moment, run X read issue 42 and GitHub said this".
If the issue changes tomorrow, we **add another row**; we do not edit the old
one. The old row remains true, because it is a statement about the past.

Consequence: Bronze holds many rows per issue. Deciding which row is "current" is
Silver's job, never Bronze's.

### 2. Silver is current state, keyed by a business key

Silver keeps one row per **business key**: the natural identity of a thing.
For an issue that is (repository owner, repository name, issue id). When a newer
observation arrives, the row is updated; an older observation can never
overwrite a newer one. This makes the load **idempotent**: running it twice with
the same input gives the same result.

### 3. Gold uses surrogate keys and a star shape

Reports are fast and easy when the data is a **star**: a central *fact* table
(one row per issue) surrounded by *dimension* tables (repository, user, date,
label). Gold joins them with **surrogate keys**: a hash of the business key.
The hash formula is identical in every model, so tables join without lookups.

### 4. Progress is committed only after the checks pass

The pipeline remembers how far it read in a **watermark** (the newest
`updated_at` it has seen). If it advanced the watermark right after reading, a
later failure would lose data: the next run would skip what was never finished.

So the pipeline works in two phases:

1. **Candidate**: after Bronze and Silver succeed, record a *candidate*
   watermark. Nothing is committed yet.
2. **Commit**: only after Gold has been built and tested does a separate
   finalizer task commit the candidate. If anything fails, the committed
   watermark does not move and the next run simply tries again.

Think of it as writing a cheque (candidate) and only cashing it (commit) once
every check has cleared.

### 5. Overlap protects against late changes

GitHub can change an issue just after the moment we recorded. So each run starts
a few minutes *before* the watermark (300 seconds). That re-reads a few rows.
Bronze keeps the duplicates; Silver keeps only the newest; the analytics never
see them.

### 6. The manifest freezes an input, so the past can be replayed

Because Bronze never changes, we can describe an input exactly: "this Bronze
table, at this Delta version, only the rows of these runs". That description is a
**replay manifest**. It is immutable and has a content-based ID. Given the same
manifest, a replay produces the same result today, next week, or after a thousand
more appends. This is how we test logic changes safely.

A manifest is **not** a watermark. The watermark says "where should the *next live
run* start". The manifest says "which *past input* do I want to reproduce".
Creating or using a manifest never moves the watermark.

### 7. A gap is not a zero

When we build daily snapshots, a day on which no run happened must be shown as
**missing**, not as "zero open issues". Inventing history before the first
observation, or filling gaps with zeros, would make the backlog look like it
fell to nothing. The snapshot models make gaps visible instead.

### 8. Prove it, then trust it

Almost every behaviour has a test that fails if the behaviour breaks. Tests use
small fixtures with a known answer. Where it matters, the tests were checked by
*mutation*: break the code on purpose and confirm the test fails. A test that
cannot fail proves nothing.

## Glossary

| Term | Meaning |
|---|---|
| **Business key** | The natural identity of a thing, such as (owner, repo, issue id). |
| **Surrogate key** | A hash of the business key, used to join Gold tables. |
| **Grain** | What exactly one row of a table represents. Always state it. |
| **Observation** | One Bronze row: what GitHub returned for an issue during one run. |
| **Current state** | The newest known version of each entity (Silver). |
| **Watermark** | The newest source position (`updated_at`) fully processed. |
| **Candidate watermark** | A watermark seen during a run but not yet committed. |
| **Overlap** | Re-reading a few minutes before the watermark to catch late changes. |
| **Run** | One attempt of the pipeline, identified by the Databricks job-run id. |
| **Idempotent** | Doing it twice has the same effect as doing it once. |
| **Delta time travel** | Reading a Delta table as it was at an earlier version. |
| **Manifest** | An immutable recipe for one frozen input (table, version, runs). |
| **Replay** | Rebuilding Silver from a manifest into separate, isolated tables. |
| **Snapshot** | The state of the backlog on a given UTC day. |
| **Coverage** | Whether a day was `observed` (a run finished that day) or `missing`. |
| **Medallion** | The Bronze, Silver, Gold layering pattern. |
| **DAB** | Declarative Automation Bundle: jobs defined as code and deployed together. |

Next: [2. Data model](02-data-model.md).
