# dbt tests

_Updated 04.06.2026_

## Context

This document is part of a **group coursework project** in data engineering. I was responsible for **designing, documenting, and maintaining the dbt data quality tests** described below.

The pipeline runs on **real operational data**. **Project details, source systems, and business context are confidential** and are intentionally omitted from this repository. Test names, logic, and run instructions are shared for academic and portfolio purposes only.

| | |
|--|--|
| **My role** | Data quality tests (dbt) — test catalogue and documentation |
| **Data** | Real production-like data |
| **Disclosure** | Internal / confidential — no business or system specifics |

---

**Prerequisite:** services and the data pipeline workflow are running — see [seadistamine.md](seadistamine.md); tests run automatically.

The project runs **75** `dbt test` checks in total; this guide covers **33 data quality tests**.

The test selection is largely based on controls described in legacy pre-dbt QA — see [scripts/legacy/qa/](../scripts/legacy/qa/) ([scripts/legacy/README.md](../scripts/legacy/README.md)).

## Table of contents

1. [Test quick reference](#test-quick-reference)
2. [33 test results — 04.06.2026](#33-test-results--04062026)
3. [How to read results?](#how-to-read-results)
4. [Test descriptions](#test-descriptions)
   - [Staging](#staging-dbtmodelsstaging)
     - [`negative_cost_stg_marketing`](#negative_cost_stg_marketing)
     - [`not_null_campaign_name_stg_marketing`](#not_null_campaign_name_stg_marketing)
     - [`not_null_external_lead_id_generate_lead_google`](#not_null_external_lead_id_generate_lead_google)
     - [`not_null_external_transaction_id_purchase_google`](#not_null_external_transaction_id_purchase_google)
     - [`invalid_external_lead_id_generate_lead`](#invalid_external_lead_id_generate_lead)
     - [`unique_external_lead_id_generate_lead`](#unique_external_lead_id_generate_lead)
     - [`invalid_external_transaction_id_purchase`](#invalid_external_transaction_id_purchase)
     - [`unique_external_transaction_id_purchase`](#unique_external_transaction_id_purchase)
     - [`unique_external_lead_id_stg_orders`](#unique_external_lead_id_stg_orders)
     - [`recency_stg_marketing_25_days`](#recency_stg_marketing_25_days)
     - [`recency_stg_analytics_25_days`](#recency_stg_analytics_25_days)
     - [`no_future_date` (generic)](#no_future_date-generic)
   - [Marts](#marts-dbtmodelsmarts)
     - [`unique_fact_contracts_system_transaction_id`](#unique_fact_contracts_system_transaction_id)
     - [`unique_fact_orders_system_transaction_id`](#unique_fact_orders_system_transaction_id)
     - [`not_null_fact_orders_system_transaction_id`](#not_null_fact_orders_system_transaction_id)
     - [`B2B_B2C_customer_segment_fact_orders`](#b2b_b2c_customer_segment_fact_orders)
     - [`lead_opportunity_gte_created_dim_lead`](#lead_opportunity_gte_created_dim_lead)
     - [`lead_offer_gte_opportunity_dim_lead`](#lead_offer_gte_opportunity_dim_lead)
     - [`lead_quote_gte_offer_dim_lead`](#lead_quote_gte_offer_dim_lead)
     - [`lead_no_offer_without_opportunity_dim_lead`](#lead_no_offer_without_opportunity_dim_lead)
     - [`lead_no_quote_without_offer_dim_lead`](#lead_no_quote_without_offer_dim_lead)
   - [Singular](#singular-dbttests)
     - [`camp_name_gap_marketing_analytics`](#camp_name_gap_marketing_analytics)
     - [`camp_name_case_mismatch_marketing_analytics`](#camp_name_case_mismatch_marketing_analytics)
     - [Examples](#examples)
     - [Gap vs case (brief)](#gap-vs-case-brief)
     - [`no_ghost_camp`](#no_ghost_camp)
     - [`no_zombie_camp`](#no_zombie_camp)
     - [`orphan_handshake_ext_lead_id_analytics_orders_fact_orders`](#orphan_handshake_ext_lead_id_analytics_orders_fact_orders)
     - [`orphan_handshake_ext_trans_id_analytics_contracts_fact_contracts`](#orphan_handshake_ext_trans_id_analytics_contracts_fact_contracts)
     - [`month_gaps_against_stg_marketing`](#month_gaps_against_stg_marketing)
     - [`month_gaps_against_stg_analytics`](#month_gaps_against_stg_analytics)
5. [Monitoring (`dbt/models/monitoring/`)](#monitoring-dbtmodelsmonitoring)

---

## Test quick reference

| Location | Model | Test | Severity | Type |
|----------|-------|------|----------|------|
| `dbt/models/staging` | | | | |
| | `stg_marketing` | `negative_cost_stg_marketing` | ERROR | generic |
| | `stg_marketing` | `not_null_campaign_name_stg_marketing` | WARN | generic |
| | `stg_analytics` | `not_null_external_lead_id_generate_lead_google` | WARN | generic |
| | `stg_analytics` | `not_null_external_transaction_id_purchase_google` | WARN | generic |
| | `stg_analytics` | `invalid_external_lead_id_generate_lead` | WARN | generic |
| | `stg_analytics` | `unique_external_lead_id_generate_lead` | WARN | generic |
| | `stg_analytics` | `invalid_external_transaction_id_purchase` | WARN | generic |
| | `stg_analytics` | `unique_external_transaction_id_purchase` | WARN | generic |
| | `stg_orders` | `unique_external_lead_id_stg_orders` | WARN | generic |
| | `stg_marketing` | `recency_stg_marketing_25_days` | WARN | generic |
| | `stg_analytics` | `recency_stg_analytics_25_days` | WARN | generic |
| | `stg_analytics` | `no_future_date_stg_analytics_date_key` | ERROR | generic (`no_future_date`) |
| | `stg_marketing` | `no_future_date_stg_marketing_date_key` | ERROR | generic (`no_future_date`) |
| | `stg_orders` | `no_future_date_stg_orders_date_key` | ERROR | generic (`no_future_date`) |
| | `stg_contracts` | `no_future_date_stg_contracts_date_key` | ERROR | generic (`no_future_date`) |
| | `stg_leads` | `no_future_date_stg_leads_date_key` | ERROR | generic (`no_future_date`) |
| `dbt/models/marts` | | | | |
| | `fact_contracts` | `unique_fact_contracts_system_transaction_id` | WARN | generic |
| | `fact_orders` | `unique_fact_orders_system_transaction_id` | WARN | generic |
| | `fact_orders` | `not_null_fact_orders_system_transaction_id` | WARN | generic |
| | `fact_orders` | `B2B_B2C_customer_segment_fact_orders` | WARN | generic |
| | `dim_lead` | `lead_opportunity_gte_created_dim_lead` | WARN | generic (`dbt_expectations`) |
| | `dim_lead` | `lead_offer_gte_opportunity_dim_lead` | WARN | generic (`dbt_expectations`) |
| | `dim_lead` | `lead_quote_gte_offer_dim_lead` | WARN | generic (`dbt_expectations`) |
| | `dim_lead` | `lead_no_offer_without_opportunity_dim_lead` | WARN | generic |
| | `dim_lead` | `lead_no_quote_without_offer_dim_lead` | WARN | generic |
| `dbt/tests` | | | | |
| | `stg_marketing`, `stg_analytics` | `camp_name_gap_marketing_analytics` | WARN | singular |
| | `stg_marketing`, `stg_analytics` | `camp_name_case_mismatch_marketing_analytics` | WARN | singular |
| | `stg_marketing`, `stg_analytics` | `no_ghost_camp` | WARN | singular |
| | `stg_analytics`, `stg_marketing` | `no_zombie_camp` | WARN | singular |
| | `stg_analytics`, `stg_orders`, `fact_orders` | `orphan_handshake_ext_lead_id_analytics_orders_fact_orders` | WARN | singular |
| | `stg_analytics`, `stg_contracts`, `fact_contracts` | `orphan_handshake_ext_trans_id_analytics_contracts_fact_contracts` | WARN | singular |
| | `stg_marketing`, `stg_analytics`, `stg_orders`, `stg_contracts` | `month_gaps_against_stg_marketing` | WARN | singular |
| | `date_coverage_gaps_by_month` | `month_gaps_against_stg_analytics` | WARN | singular |


---

## 33 test results — 04.06.2026


**Summary:** PASS=26 · WARN=7 · ERROR=0 · TOTAL=33


| Test | Result | Violations |
|------|--------|------------|
| `camp_name_gap_marketing_analytics` | WARN | 89 |
| `month_gaps_against_stg_analytics` | WARN | 8 |
| `month_gaps_against_stg_marketing` | WARN | 8 |
| `no_zombie_camp` | WARN | 38 |
| `not_null_external_lead_id_generate_lead_google` | WARN | 9 |
| `orphan_handshake_ext_lead_id_analytics_orders_fact_orders` | WARN | 1366 |
| `orphan_handshake_ext_trans_id_analytics_contracts_fact_contracts` | WARN | 1659 |


---

## How to read results?

**dbt result (single test, `Done.` line):**

| Result | Meaning | Log | Pipeline |
|--------|---------|-----|----------|
| **PASS** | test passes | `PASS=1 WARN=0 ERROR=0 TOTAL=1` | continues |
| **WARN** | warning; test has `severity: warn` | `PASS=0 WARN=1 ERROR=0 TOTAL=1` | continues |
| **ERROR** | failure; test has `severity: error` | `PASS=0 WARN=0 ERROR=1 TOTAL=1` | pipeline stops |

---

## Test descriptions

### Staging (`dbt/models/staging`)

#### `negative_cost_stg_marketing`

| | |
|--|--|
| **Model** | `stg_marketing` |
| **File** | `dbt/models/staging/_stg__models.yml` |
| **Severity** | ERROR |
| **store_failures** | yes |
| **What it tests** | column `cost` may be `0`, but must not be negative |

| Step | When | Command | Expected |
|------|------|---------|----------|
| 1. Run | | `docker compose run --rm -w /app/dbt etl dbt test --select negative_cost_stg_marketing` | `PASS=1 ERROR=0` |
| 2. Bad rows | `ERROR=1` | `docker compose run --rm -w /app/dbt etl dbt show --select negative_cost_stg_marketing --limit 20` | rows with negative `cost` in terminal |
| 3. Audit | `ERROR=1` | `SELECT * FROM public_dbt_test__audit.negative_cost_stg_marketing LIMIT 20;` | same rows as step 2; empty on PASS |

---

#### `not_null_campaign_name_stg_marketing`

| | |
|--|--|
| **Model** | `stg_marketing` |
| **File** | `dbt/models/staging/_stg__models.yml` |
| **Severity** | WARN |
| **store_failures** | no |
| **What it tests** | `campaign_name` must not be NULL |

| Step | When | Command | Expected |
|------|------|---------|----------|
| 1. Run | | `docker compose run --rm -w /app/dbt etl dbt test --select not_null_campaign_name_stg_marketing` | `PASS=1 WARN=0` |
| 2. Bad rows | `WARN=1` | `docker compose run --rm -w /app/dbt etl dbt show --select not_null_campaign_name_stg_marketing --limit 20` | rows where `campaign_name` is NULL |

---

#### `not_null_external_lead_id_generate_lead_google`

| | |
|--|--|
| **Model** | `stg_analytics` |
| **File** | `dbt/models/staging/_stg__models.yml` |
| **Severity** | WARN |
| **store_failures** | no |
| **What it tests** | `event_name = 'generate_lead'` and `utm_source = 'google'`: `external_lead_id` must not be NULL |

| Step | When | Command | Expected |
|------|------|---------|----------|
| 1. Run | | `docker compose run --rm -w /app/dbt etl dbt test --select not_null_external_lead_id_generate_lead_google` | `PASS=1 WARN=0` |
| 2. Bad rows | `WARN=1` | `docker compose run --rm -w /app/dbt etl dbt show --select not_null_external_lead_id_generate_lead_google --limit 20` | rows without `external_lead_id` when source is 'google' |

---

#### `not_null_external_transaction_id_purchase_google`

| | |
|--|--|
| **Model** | `stg_analytics` |
| **File** | `dbt/models/staging/_stg__models.yml` |
| **Severity** | WARN |
| **store_failures** | no |
| **What it tests** | `event_name = 'purchase'` and `utm_source = 'google'`: `external_transaction_id` must not be NULL |

| Step | When | Command | Expected |
|------|------|---------|----------|
| 1. Run | | `docker compose run --rm -w /app/dbt etl dbt test --select not_null_external_transaction_id_purchase_google` | `PASS=1 WARN=0` |
| 2. Bad rows | `WARN=1` | `docker compose run --rm -w /app/dbt etl dbt show --select not_null_external_transaction_id_purchase_google --limit 20` | rows without `external_transaction_id` when source is 'google'|

---

#### `invalid_external_lead_id_generate_lead`

| | |
|--|--|
| **Model** | `stg_analytics` |
| **File** | `dbt/models/staging/_stg__models.yml` |
| **Severity** | WARN |
| **store_failures** | no |
| **What it tests** | `event_name = 'generate_lead'`: `external_lead_id` must not be `(not set)` or `(none)` |

| Step | When | Command | Expected |
|------|------|---------|----------|
| 1. Run | | `docker compose run --rm -w /app/dbt etl dbt test --select invalid_external_lead_id_generate_lead` | `PASS=1 WARN=0` |
| 2. Bad rows | `WARN=1` | `docker compose run --rm -w /app/dbt etl dbt show --select invalid_external_lead_id_generate_lead --limit 20` | `external_lead_id` is `(not set)` or `(none)` |

---

#### `unique_external_lead_id_generate_lead`

| | |
|--|--|
| **Model** | `stg_analytics` |
| **File** | `dbt/models/staging/_stg__models.yml` |
| **Severity** | WARN |
| **store_failures** | no |
| **What it tests** | `event_name = 'generate_lead'` and `external_lead_id IS NOT NULL`: the same `external_lead_id` must not repeat |

| Step | When | Command | Expected |
|------|------|---------|----------|
| 1. Run | | `docker compose run --rm -w /app/dbt etl dbt test --select unique_external_lead_id_generate_lead` | `PASS=1 WARN=0` |
| 2. Bad rows | `WARN=1` | `docker compose run --rm -w /app/dbt etl dbt show --select unique_external_lead_id_generate_lead --limit 20` | same `external_lead_id` appears more than once |

---

#### `invalid_external_transaction_id_purchase`

| | |
|--|--|
| **Model** | `stg_analytics` |
| **File** | `dbt/models/staging/_stg__models.yml` |
| **Severity** | WARN |
| **store_failures** | no |
| **What it tests** | `event_name = 'purchase'`: `external_transaction_id` must not be `(not set)` or `(none)` |

| Step | When | Command | Expected |
|------|------|---------|----------|
| 1. Run | | `docker compose run --rm -w /app/dbt etl dbt test --select invalid_external_transaction_id_purchase` | `PASS=1 WARN=0` |
| 2. Bad rows | `WARN=1` | `docker compose run --rm -w /app/dbt etl dbt show --select invalid_external_transaction_id_purchase --limit 20` | `external_transaction_id` is `(not set)` or `(none)` |

---

#### `unique_external_transaction_id_purchase`

| | |
|--|--|
| **Model** | `stg_analytics` |
| **File** | `dbt/models/staging/_stg__models.yml` |
| **Severity** | WARN |
| **store_failures** | no |
| **What it tests** | `event_name = 'purchase'` and `external_transaction_id IS NOT NULL`: the same `external_transaction_id` must not repeat |

| Step | When | Command | Expected |
|------|------|---------|----------|
| 1. Run | | `docker compose run --rm -w /app/dbt etl dbt test --select unique_external_transaction_id_purchase` | `PASS=1 WARN=0` |
| 2. Bad rows | `WARN=1` | `docker compose run --rm -w /app/dbt etl dbt show --select unique_external_transaction_id_purchase --limit 20` | same `external_transaction_id` appears more than once |

---

#### `unique_external_lead_id_stg_orders`

| | |
|--|--|
| **Model** | `stg_orders` |
| **File** | `dbt/models/staging/_stg__models.yml` |
| **Severity** | WARN |
| **store_failures** | no |
| **What it tests** | the same `external_lead_id` must not repeat in `stg_orders` (NULLs excluded) |

| Step | When | Command | Expected |
|------|------|---------|----------|
| 1. Run | | `docker compose run --rm -w /app/dbt etl dbt test --select unique_external_lead_id_stg_orders` | `PASS=1 WARN=0` |
| 2. Bad rows | `WARN=1` | `docker compose run --rm -w /app/dbt etl dbt show --select unique_external_lead_id_stg_orders --limit 20` | same `external_lead_id` appears more than once |

---

#### `recency_stg_marketing_25_days`

| | |
|--|--|
| **Model** | `stg_marketing` |
| **File** | `dbt/models/staging/_stg__models.yml` |
| **Severity** | WARN |
| **store_failures** | no |
| **What it tests** | whether `stg_marketing` has at least one row whose `date_key` is within the last **25 days** (`dbt_utils.recency`, `ignore_time_component: true`). Google Ads data is loaded manually / via proxy — 25 days is a buffer. |

**Check in data:**

```sql
SELECT MAX(date_key) AS latest_date,
       MAX(date_key) FILTER (WHERE cost > 0) AS latest_spend_date
FROM public_staging.stg_marketing;
```

| Step | When | Command | Expected |
|------|------|---------|----------|
| 1. Run | | `docker compose run --rm -w /app/dbt etl dbt test --select recency_stg_marketing_25_days` | `PASS=1 WARN=0` |
| 2. Bad rows | `WARN=1` | `docker compose run --rm -w /app/dbt etl dbt show --select recency_stg_marketing_25_days --limit 20` | stale or missing `date_key` |

---

#### `recency_stg_analytics_25_days`

| | |
|--|--|
| **Model** | `stg_analytics` |
| **File** | `dbt/models/staging/_stg__models.yml` |
| **Severity** | WARN |
| **store_failures** | no |
| **What it tests** | whether `stg_analytics` has at least one row whose `date_key` is within the last **25 days**. GA4 export is loaded manually, roughly once a month. |

**Check in data:**

```sql
SELECT MAX(date_key) AS latest_date
FROM public_staging.stg_analytics;
```

| Step | When | Command | Expected |
|------|------|---------|----------|
| 1. Run | | `docker compose run --rm -w /app/dbt etl dbt test --select recency_stg_analytics_25_days` | `PASS=1 WARN=0` |
| 2. Bad rows | `WARN=1` | `docker compose run --rm -w /app/dbt etl dbt show --select recency_stg_analytics_25_days --limit 20` | stale or missing `date_key` |

---

#### `no_future_date` (generic)

| | |
|--|--|
| **Models** | `stg_analytics`, `stg_marketing`, `stg_orders`, `stg_contracts`, `stg_leads` |
| **File** | `dbt/tests/generic/no_future_date.sql` · column test in `_stg__models.yml` |
| **Severity** | ERROR |
| **store_failures** | no |
| **What it tests** | `date_key` must not be in the future (`cast(date_key as date) > current_date`) |

| dbt test name | Model |
|---------------|-------|
| `no_future_date_stg_analytics_date_key` | `stg_analytics` |
| `no_future_date_stg_marketing_date_key` | `stg_marketing` |
| `no_future_date_stg_orders_date_key` | `stg_orders` |
| `no_future_date_stg_contracts_date_key` | `stg_contracts` |
| `no_future_date_stg_leads_date_key` | `stg_leads` |

| Step | When | Command | Expected |
|------|------|---------|----------|
| 1. Run (all) | | `docker compose run --rm -w /app/dbt etl dbt test --select test_name:no_future_date` | `PASS=5 WARN=0 ERROR=0 TOTAL=5` |
| 2. Single model | | `docker compose run --rm -w /app/dbt etl dbt test --select no_future_date_stg_marketing_date_key` | `PASS=1 ERROR=0` |
| 3. Bad rows | `ERROR=1` | `docker compose run --rm -w /app/dbt etl dbt show --select no_future_date_stg_analytics_date_key --limit 20` | rows with future `date_key` |

---

### Marts (`dbt/models/marts`)

#### `unique_fact_contracts_system_transaction_id`

| | |
|--|--|
| **Model** | `fact_contracts` |
| **File** | `dbt/models/marts/_marts__models.yml` |
| **Severity** | WARN |
| **store_failures** | no |
| **What it tests** | `system_transaction_id` must be unique in `fact_contracts` |

| Step | When | Command | Expected |
|------|------|---------|----------|
| 1. Run | | `docker compose run --rm -w /app/dbt etl dbt test --select unique_fact_contracts_system_transaction_id` | `PASS=1 WARN=0` |
| 2. Bad rows | `WARN=1` | `docker compose run --rm -w /app/dbt etl dbt show --select unique_fact_contracts_system_transaction_id --limit 20` | same `system_transaction_id` appears more than once |
---

#### `unique_fact_orders_system_transaction_id`

| | |
|--|--|
| **Model** | `fact_orders` |
| **File** | `dbt/models/marts/_marts__models.yml` |
| **Severity** | WARN |
| **store_failures** | no |
| **What it tests** | `system_transaction_id` must be unique in `fact_orders` |

| Step | When | Command | Expected |
|------|------|---------|----------|
| 1. Run | | `docker compose run --rm -w /app/dbt etl dbt test --select unique_fact_orders_system_transaction_id` | `PASS=1 WARN=0` |
| 2. Bad rows | `WARN=1` | `docker compose run --rm -w /app/dbt etl dbt show --select unique_fact_orders_system_transaction_id --limit 20` | same `system_transaction_id` appears more than once |

---

#### `not_null_fact_orders_system_transaction_id`

| | |
|--|--|
| **Model** | `fact_orders` |
| **File** | `dbt/models/marts/_marts__models.yml` |
| **Severity** | WARN |
| **store_failures** | no |
| **What it tests** | `system_transaction_id` must not be NULL |

| Step | When | Command | Expected |
|------|------|---------|----------|
| 1. Run | | `docker compose run --rm -w /app/dbt etl dbt test --select not_null_fact_orders_system_transaction_id` | `PASS=1 WARN=0` |
| 2. Bad rows | `WARN=1` | `docker compose run --rm -w /app/dbt etl dbt show --select not_null_fact_orders_system_transaction_id --limit 20` | `system_transaction_id` is NULL |

---

#### `B2B_B2C_customer_segment_fact_orders`

| | |
|--|--|
| **Model** | `fact_orders` |
| **File** | `dbt/models/marts/_marts__models.yml` |
| **Severity** | WARN |
| **store_failures** | no |
| **What it tests** | `customer_segment` must be `b2b` or `b2c` (or NULL) |

| Step | When | Command | Expected |
|------|------|---------|----------|
| 1. Run | | `docker compose run --rm -w /app/dbt etl dbt test --select B2B_B2C_customer_segment_fact_orders` | `PASS=1 WARN=0` |
| 2. Bad rows | `WARN=1` | `docker compose run --rm -w /app/dbt etl dbt show --select B2B_B2C_customer_segment_fact_orders --limit 20` | invalid `customer_segment` |
---

#### `lead_opportunity_gte_created_dim_lead`

| | |
|--|--|
| **Model** | `dim_lead` |
| **File** | `dbt/models/marts/_marts__models.yml` |
| **Severity** | WARN |
| **store_failures** | no |
| **What it tests** | `opportunity_at` ≥ `lead_created_at` (when both are set) |

| Step | When | Command | Expected |
|------|------|---------|----------|
| 1. Run | | `docker compose run --rm -w /app/dbt etl dbt test --select lead_opportunity_gte_created_dim_lead` | `PASS=1 WARN=0` |
| 2. Bad rows | `WARN=1` | `docker compose run --rm -w /app/dbt etl dbt show --select lead_opportunity_gte_created_dim_lead --limit 20` | `opportunity_at` < `lead_created_at` |
---
#### `lead_offer_gte_opportunity_dim_lead`

| | |
|--|--|
| **Model** | `dim_lead` |
| **File** | `dbt/models/marts/_marts__models.yml` |
| **Severity** | WARN |
| **store_failures** | no |
| **What it tests** | `offer_at` ≥ `opportunity_at` (when both are set) |

| Step | When | Command | Expected |
|------|------|---------|----------|
| 1. Run | | `docker compose run --rm -w /app/dbt etl dbt test --select lead_offer_gte_opportunity_dim_lead` | `PASS=1 WARN=0` |
| 2. Bad rows | `WARN=1` | `docker compose run --rm -w /app/dbt etl dbt show --select lead_offer_gte_opportunity_dim_lead --limit 20` | `offer_at` < `opportunity_at` |

---

#### `lead_quote_gte_offer_dim_lead`

| | |
|--|--|
| **Model** | `dim_lead` |
| **File** | `dbt/models/marts/_marts__models.yml` |
| **Severity** | WARN |
| **store_failures** | no |
| **What it tests** | `quote_at` ≥ `offer_at` (when both are set) |

| Step | When | Command | Expected |
|------|------|---------|----------|
| 1. Run | | `docker compose run --rm -w /app/dbt etl dbt test --select lead_quote_gte_offer_dim_lead` | `PASS=1 WARN=0` |
| 2. Bad rows | `WARN=1` | `docker compose run --rm -w /app/dbt etl dbt show --select lead_quote_gte_offer_dim_lead --limit 20` | `quote_at` < `offer_at` |

---

#### `lead_no_offer_without_opportunity_dim_lead`

| | |
|--|--|
| **Model** | `dim_lead` |
| **File** | `dbt/models/marts/_marts__models.yml` |
| **Severity** | WARN |
| **store_failures** | no |
| **What it tests** | must not have `offer_at` without `opportunity_at` |

| Step | When | Command | Expected |
|------|------|---------|----------|
| 1. Run | | `docker compose run --rm -w /app/dbt etl dbt test --select lead_no_offer_without_opportunity_dim_lead` | `PASS=1 WARN=0` |
| 2. Bad rows | `WARN=1` | `docker compose run --rm -w /app/dbt etl dbt show --select lead_no_offer_without_opportunity_dim_lead --limit 20` | `offer_at` set, `opportunity_at` NULL |

---

#### `lead_no_quote_without_offer_dim_lead`

| | |
|--|--|
| **Model** | `dim_lead` |
| **File** | `dbt/models/marts/_marts__models.yml` |
| **Severity** | WARN |
| **store_failures** | no |
| **What it tests** | must not have `quote_at` without `offer_at` |

| Step | When | Command | Expected |
|------|------|---------|----------|
| 1. Run | | `docker compose run --rm -w /app/dbt etl dbt test --select lead_no_quote_without_offer_dim_lead` | `PASS=1 WARN=0` |
| 2. Bad rows | `WARN=1` | `docker compose run --rm -w /app/dbt etl dbt show --select lead_no_quote_without_offer_dim_lead --limit 20` | `quote_at` set, `offer_at` NULL |

---

### Singular (`dbt/tests`)

#### `camp_name_gap_marketing_analytics`

| | |
|--|--|
| **Model** | `stg_marketing`, `stg_analytics` |
| **File** | `dbt/tests/camp_name_gap_marketing_analytics.sql` |
| **Severity** | WARN |
| **store_failures** | yes |
| **What it tests** | campaign name overlap in both directions (exact `campaign_name`; `missing_in_analytics` / `missing_in_marketing`) |

| Step | When | Command | Expected |
|------|------|---------|----------|
| 1. Run | | `docker compose run --rm -w /app/dbt etl dbt test --select camp_name_gap_marketing_analytics` | `PASS=1 WARN=0` |
| 2. Bad rows | `WARN=1` | `docker compose run --rm -w /app/dbt etl dbt show --select camp_name_gap_marketing_analytics --limit 20` | gap rows with `direction` column |
| 3. Audit | `WARN=1` | `SELECT * FROM public_dbt_test__audit.camp_name_gap_marketing_analytics LIMIT 20;` | same as step 2 |

---

#### `camp_name_case_mismatch_marketing_analytics`

| | |
|--|--|
| **Model** | `stg_marketing`, `stg_analytics` |
| **File** | `dbt/tests/camp_name_case_mismatch_marketing_analytics.sql` |
| **Severity** | WARN |
| **store_failures** | yes |
| **What it tests** | same campaign with different casing (`trim(lower(...))` matches, raw name differs) |

| Step | When | Command | Expected |
|------|------|---------|----------|
| 1. Run | | `docker compose run --rm -w /app/dbt etl dbt test --select camp_name_case_mismatch_marketing_analytics` | `PASS=1 WARN=0` |
| 2. Bad rows | `WARN=1` | `docker compose run --rm -w /app/dbt etl dbt show --select camp_name_case_mismatch_marketing_analytics --limit 20` | `normalized_name`, `variants` |
| 3. Audit | `WARN=1` | `SELECT * FROM public_dbt_test__audit.camp_name_case_mismatch_marketing_analytics LIMIT 20;` | same as step 2 |

---

#### Examples

| Marketing | Analytics | Gap test | Case test |
|-----------|-----------|----------|-----------|
| `Summer_Sale` | `summer_sale` | WARN (exact string does not match) | **WARN** — 2 variants, `normalized_name = summer_sale` |
| `Summer_Sale` | *(missing)* | WARN (`missing_in_analytics`) | **PASS** — only 1 variant in entire dataset |
| `Summer_Sale` + `summer_sale` | *(both in `stg_marketing`)* | WARN | **WARN** — variants in same source |
| `summer_sale` | `summer_sale` | PASS | PASS |

---

#### Gap vs case (brief)

| | **gap** | **case** |
|---|--------|--------|
| Question | is the name **missing** in the other source? | is the **same** name **cased differently**? |
| Comparison | exact `campaign_name` | `trim(lower(...))` |
| Direction | yes — `missing_in_analytics` / `missing_in_marketing` | no — shared check |

---

#### `no_ghost_camp`

| | |
|--|--|
| **Model** | `stg_marketing`, `stg_analytics` |
| **File** | `dbt/tests/no_ghost_camp.sql` |
| **Severity** | WARN |
| **store_failures** | yes |
| **What it tests** | `stg_marketing` has spend (`cost > 0`), `stg_analytics` has no matching campaign in the same country |

| Step | When | Command | Expected |
|------|------|---------|----------|
| 1. Run | | `docker compose run --rm -w /app/dbt etl dbt test --select no_ghost_camp` | `PASS=1 WARN=0` |
| 2. Bad rows | `WARN=1` | `docker compose run --rm -w /app/dbt etl dbt show --select no_ghost_camp --limit 20` | `campaign_name`, `wasted_spend` |
| 3. Audit | `WARN=1` | `SELECT * FROM public_dbt_test__audit.no_ghost_camp LIMIT 20;` | same as step 2 |

---
#### `no_zombie_camp`

| | |
|--|--|
| **Model** | `stg_analytics`, `stg_marketing` |
| **File** | `dbt/tests/no_zombie_camp.sql` |
| **Severity** | WARN |
| **store_failures** | yes |
| **What it tests** | analytics has paid traffic, marketing has no matching campaign in the same country |

| Step | When | Command | Expected |
|------|------|---------|----------|
| 1. Run | | `docker compose run --rm -w /app/dbt etl dbt test --select no_zombie_camp` | `PASS=1 WARN=0` |
| 2. Bad rows | `WARN=1` | `docker compose run --rm -w /app/dbt etl dbt show --select no_zombie_camp --limit 20` | `session_count`, utm columns |
| 3. Audit | `WARN=1` | `SELECT * FROM public_dbt_test__audit.no_zombie_camp LIMIT 20;` | same as step 2 |

---
#### `orphan_handshake_ext_lead_id_analytics_orders_fact_orders`

| | |
|--|--|
| **Model** | `stg_analytics`, `stg_orders`, `fact_orders` |
| **File** | `dbt/tests/orphan_handshake_ext_lead_id_analytics_orders_fact_orders.sql` |
| **Severity** | WARN |
| **store_failures** | yes |
| **What it tests** | `external_lead_id` (`generate_lead`) must exist across analytics → orders → fact_orders chain |

| `gap_status` | Meaning |
|--------------|---------|
| `In Analytics -> Missing in Orders` | `external_lead_id` in `stg_analytics`, missing in `stg_orders` |
| `In Orders -> Missing in Analytics` | `external_lead_id` in `stg_orders`, missing in `stg_analytics` |
| `In Orders -> Missing in fact_orders` | `external_lead_id` in `stg_orders`, missing in `fact_orders` |
| `Other handshake gap` | other break — remaining combinations not covered above (e.g. `external_lead_id` only in `fact_orders`) |

| Step | When | Command | Expected |
|------|------|---------|----------|
| 1. Run | | `docker compose run --rm -w /app/dbt etl dbt test --select orphan_handshake_ext_lead_id_analytics_orders_fact_orders` | `PASS=1 WARN=0` |
| 2. Bad rows | `WARN=1` | `docker compose run --rm -w /app/dbt etl dbt show --select orphan_handshake_ext_lead_id_analytics_orders_fact_orders --limit 20` | `external_lead_id`, `gap_status` |
| 3. Audit | `WARN=1` | `SELECT * FROM public_dbt_test__audit.orphan_handshake_ext_lead_id_analytics_orders_fact_orders LIMIT 20;` | same as step 2 |

---
#### `orphan_handshake_ext_trans_id_analytics_contracts_fact_contracts`

| | |
|--|--|
| **Model** | `stg_analytics`, `stg_contracts`, `fact_contracts` |
| **File** | `dbt/tests/orphan_handshake_ext_trans_id_analytics_contracts_fact_contracts.sql` |
| **Severity** | WARN |
| **store_failures** | yes (alias) |
| **What it tests** | `external_transaction_id` (`purchase`) must exist across analytics → contracts → fact_contracts chain |

| `gap_status` | Meaning |
|--------------|---------|
| `In Analytics -> Missing in Contracts` | `external_transaction_id` in `stg_analytics`, missing in `stg_contracts` |
| `In Contracts -> Missing in Analytics` | `external_transaction_id` in `stg_contracts`, missing in `stg_analytics` |
| `In Contracts -> Missing in fact_contracts` | `external_transaction_id` in `stg_contracts`, missing in `fact_contracts` |
| `Other handshake gap` | other break — remaining combinations not covered above (e.g. `external_transaction_id` only in `fact_contracts`) |

| Step | When | Command | Expected |
|------|------|---------|----------|
| 1. Run | | `docker compose run --rm -w /app/dbt etl dbt test --select orphan_handshake_ext_trans_id_analytics_contracts_fact_contracts` | `PASS=1 WARN=0` |
| 2. Bad rows | `WARN=1` | `docker compose run --rm -w /app/dbt etl dbt show --select orphan_handshake_ext_trans_id_analytics_contracts_fact_contracts --limit 20` | `external_transaction_id`, `gap_status` |
| 3. Audit | `WARN=1` | `SELECT * FROM public_dbt_test__audit.orphan_hs_ext_trans_id_analytics_contracts_fc LIMIT 20;` | shortened audit table name |

---

#### `month_gaps_against_stg_marketing`

| | |
|--|--|
| **Model** | `stg_marketing`, `stg_analytics`, `stg_orders`, `stg_contracts` |
| **File** | `dbt/tests/month_gaps_against_stg_marketing.sql` |
| **Severity** | WARN |
| **store_failures** | yes |
| **What it tests** | month-level gaps where one staging table has rows in a month and the partner table has 0 rows in the same month. **`stg_marketing`** is the comparison baseline. |

| `gap_status` (examples) | Meaning |
|-------------------------|---------|
| `In Marketing -> Missing in Analytics` | month exists in `stg_marketing`, missing in `stg_analytics` |
| `In Analytics -> Missing in Marketing` | month exists in `stg_analytics`, missing in `stg_marketing` |
| `In Marketing -> Missing in Orders` | month exists in `stg_marketing`, missing in `stg_orders` |
| `In Orders -> Missing in Marketing` | month exists in `stg_orders`, missing in `stg_marketing` |
| `In Marketing -> Missing in Contracts` | month exists in `stg_marketing`, missing in `stg_contracts` |
| `In Contracts -> Missing in Marketing` | month exists in `stg_contracts`, missing in `stg_marketing` |

| Step | When | Command | Expected |
|------|------|---------|----------|
| 1. Run | | `docker compose run --rm -w /app/dbt etl dbt test --select month_gaps_against_stg_marketing` | `PASS=1 WARN=0` |
| 2. Bad rows | `WARN=1` | `docker compose run --rm -w /app/dbt etl dbt show --select month_gaps_against_stg_marketing --limit 20` | `month_start`, `left_table`, `right_table`, `gap_status` |
| 3. Audit | `WARN=1` | `SELECT * FROM public_dbt_test__audit.month_gaps_against_stg_marketing LIMIT 20;` | same as step 2 |

---

#### `month_gaps_against_stg_analytics`

| | |
|--|--|
| **Model** | `date_coverage_gaps_by_month` (monitoring; covers `stg_marketing`, `stg_analytics`, `stg_orders`, `stg_contracts`) |
| **File** | `dbt/tests/month_gaps_against_stg_analytics.sql` · logic in `dbt/models/monitoring/date_coverage_gaps_by_month.sql` |
| **Severity** | WARN |
| **store_failures** | yes |
| **What it tests** | month-level gaps where one staging table has rows in a month and the partner table has 0 rows in the same month — against **stg_analytics** |

| `gap_status` (examples) | Meaning |
|-------------------------|---------|
| `In Analytics -> Missing in Marketing` | month exists in `stg_analytics`, missing in `stg_marketing` |
| `In Analytics -> Missing in Orders` | month exists in `stg_analytics`, missing in `stg_orders` |
| `In Analytics -> Missing in Contracts` | month exists in `stg_analytics`, missing in `stg_contracts` |
| `In Orders -> Missing in Analytics` | month exists in `stg_orders`, missing in `stg_analytics` |
| `In Contracts -> Missing in Analytics` | month exists in `stg_contracts`, missing in `stg_analytics` |
| `In Marketing -> Missing in Analytics` | month exists in `stg_marketing`, missing in `stg_analytics` |

| Step | When | Command | Expected |
|------|------|---------|----------|
| 1. Run | | `docker compose run --rm -w /app/dbt etl dbt test --select month_gaps_against_stg_analytics` | `PASS=1 WARN=0` |
| 2. Bad rows | `WARN=1` | `docker compose run --rm -w /app/dbt etl dbt show --select month_gaps_against_stg_analytics --limit 20` | `month_start`, `left_table`, `right_table`, `gap_status` |
| 3. Audit | `WARN=1` | `SELECT * FROM public_dbt_test__audit.month_gaps_against_stg_analytics LIMIT 20;` | same as step 2 |

---

## Monitoring (`dbt/models/monitoring/`)

Models duplicate the logic of 3 singular tests (**orphan handshake**, **month gaps**). Results can be viewed in **SQL Lab** or **BI** (e.g. Superset) without re-running `dbt test` — as long as `dbt run` has built the monitoring models.

| Model | Related test |
|-------|--------------|
| `orphan_handshake_ext_lead_id_analytics_orders_fact_orders` | `orphan_handshake_ext_lead_id_analytics_orders_fact_orders` |
| `orphan_handshake_ext_trans_id_analytics_contracts_fact_contracts` | `orphan_handshake_ext_trans_id_analytics_contracts_fact_contracts` |
| `date_coverage_gaps_by_month` | `month_gaps_against_stg_marketing`, `month_gaps_against_stg_analytics` |

**Persistent monitoring tables** (`public`):

```sql
SELECT * FROM public.orphan_handshake_ext_lead_id_analytics_orders_fact_orders LIMIT 20;

SELECT * FROM public.orphan_handshake_ext_trans_id_analytics_contracts_fact_contracts LIMIT 20;

SELECT * FROM public.date_coverage_gaps_by_month ORDER BY month_start LIMIT 20;
```
