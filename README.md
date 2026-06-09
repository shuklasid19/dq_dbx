# DQ Accelerator — Data Quality Framework for Databricks

A scalable, configuration-driven data quality framework built on top of [Databricks Labs DQX](https://github.com/databrickslabs/dqx). Supports **68 data quality checks** — 35 built-in DQX checks and 33 custom checks — with full compatibility for **Databricks Serverless (Spark Connect)**.

---

## Key Features

- **68 Total Checks** — 35 built-in + 33 custom, covering null/empty, range, regex, date, IP, JSON, uniqueness, and more
- **Databricks Serverless Compatible** — Works on both Classic and Serverless compute
- **YAML-Driven Configuration** — Define all checks declaratively, no code changes needed
- **Dynamic Custom Checks** — Add/modify checks via a Python script, no wheel rebuild required
- **Smart Quarantine** — Only ERROR rows are quarantined; WARNING rows stay in valid output
- **Unity Catalog Integration** — Writes valid data, quarantine, metrics, and summary to Unity Catalog tables
- **Auto-Scalable** — New DQX built-in checks work automatically with library updates

---

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                    YAML Configuration                         │
│              (table_customers.yaml)                           │
│                                                              │
│  checks:                                                     │
│    - function: is_not_null           ← DQX Built-in          │
│    - function: email_format_validator ← Custom Check          │
└─────────────────────┬────────────────────────────────────────┘
                      │
                      ▼
┌──────────────────────────────────────────────────────────────┐
│                  DQ Accelerator Engine                         │
│                                                              │
│  1. Load custom checks from user's Python script              │
│  2. Validate all check configurations                         │
│  3. Apply checks via DQX native engine                        │
│  4. Split: ERROR rows → quarantine, WARNING rows → valid      │
│  5. Write results to Unity Catalog                            │
└─────────────────────┬────────────────────────────────────────┘
                      │
           ┌──────────┴──────────┐
           ▼                     ▼
┌──────────────────┐   ┌────────────────────────────┐
│   DQX Library    │   │  custom_checks_library.py   │
│  (35 built-in)   │   │  (33+ user-defined)         │
│                  │   │                              │
│  Scales with     │   │  Add/modify/remove checks   │
│  DQX updates     │   │  without rebuilding wheel    │
└──────────────────┘   └────────────────────────────┘
```

---

## Quick Start

### 1. Install the Wheel

```python
%pip install /Volumes/<catalog>/<schema>/wheels/dq_accelerator-1.0.0-py3-none-any.whl --force-reinstall
%restart_python
```

### 2. Configure Checks (YAML)

```yaml
table_name: customers
layer: bronze
custom_checks_module: /Workspace/Users/<user>/custom_checks_library.py
enabled: true

checks:
  - criticality: error
    check:
      function: is_not_null
      arguments:
        column: customer_id

  - criticality: warn
    check:
      function: email_format_validator
      arguments:
        column: email
```

### 3. Run

```python
from dq_accelerator.engine.dq_engine import DQAccelerator

dq = DQAccelerator(spark=spark)
results = dq.run_checks(df=source_df, table_config=table_config, layer_config=layer_config)
```

---

## Supported Checks (68 Total)

### Built-in DQX Checks (35)

These are handled natively by the DQX engine. They auto-scale — when DQX adds new checks, they work automatically.

#### Null / Empty Checks (7)

| # | Function | Description | Arguments |
|---|----------|-------------|-----------|
| 1 | `is_not_null` | Value must not be NULL | `column` |
| 2 | `is_null` | Value must be NULL | `column` |
| 3 | `is_not_empty` | Must not be empty string | `column` |
| 4 | `is_empty` | Must be empty string | `column`, `trim_strings` |
| 5 | `is_not_null_and_not_empty` | Not NULL and not empty | `column` |
| 6 | `is_null_or_empty` | Must be NULL or empty | `column`, `trim_strings` |
| 7 | `is_not_null_and_not_empty_array` | Array not NULL/empty | `column` |

#### List Checks (3)

| # | Function | Description | Arguments |
|---|----------|-------------|-----------|
| 8 | `is_in_list` | Value in allowed list | `column`, `allowed` |
| 9 | `is_not_in_list` | Value NOT in forbidden list | `column`, `forbidden` |
| 10 | `is_not_null_and_is_in_list` | Not null AND in list | `column`, `allowed` |

#### Range / Comparison Checks (6)

| # | Function | Description | Arguments |
|---|----------|-------------|-----------|
| 11 | `is_in_range` | Value within min-max | `column`, `min_limit`, `max_limit` |
| 12 | `is_not_in_range` | Value outside range | `column`, `min_limit`, `max_limit` |
| 13 | `is_equal_to` | Value equals expected | `column`, `value` |
| 14 | `is_not_equal_to` | Value not equal | `column`, `value` |
| 15 | `is_not_less_than` | Value >= threshold | `column`, `limit` |
| 16 | `is_not_greater_than` | Value <= threshold | `column`, `limit` |

#### Date / Time Checks (6)

| # | Function | Description | Arguments |
|---|----------|-------------|-----------|
| 17 | `is_valid_date` | Parseable as date | `column`, `date_format` |
| 18 | `is_valid_timestamp` | Parseable as timestamp | `column`, `date_format` |
| 19 | `is_not_in_future` | Not in the future | `column`, `offset` |
| 20 | `is_not_in_near_future` | Not in near future | `column`, `offset` |
| 21 | `is_older_than_n_days` | Older than N days | `column`, `n_days` |
| 22 | `is_older_than_col2_for_n_days` | Col1 older than Col2 by N days | `column`, `column2`, `n_days` |

#### Pattern / Regex Checks (2)

| # | Function | Description | Arguments |
|---|----------|-------------|-----------|
| 23 | `regex_match` | Matches regex pattern | `column`, `regex`, `negate` |
| 24 | `col_regex_match` | Alias for regex_match | `column`, `regex`, `negate` |

#### SQL Checks (2)

| # | Function | Description | Arguments |
|---|----------|-------------|-----------|
| 25 | `sql_expression` | Custom SQL expression | `expression` |
| 26 | `sql_query` | Custom SQL query | `query` |

#### IP Address Checks (4)

| # | Function | Description | Arguments |
|---|----------|-------------|-----------|
| 27 | `is_valid_ipv4_address` | Valid IPv4 format | `column` |
| 28 | `is_ipv4_address_in_cidr` | IPv4 in CIDR range | `column`, `cidr` |
| 29 | `is_valid_ipv6_address` | Valid IPv6 format | `column` |
| 30 | `is_ipv6_address_in_cidr` | IPv6 in CIDR range | `column`, `cidr` |

#### JSON Checks (3)

| # | Function | Description | Arguments |
|---|----------|-------------|-----------|
| 31 | `is_valid_json` | Valid JSON string | `column` |
| 32 | `has_json_keys` | JSON has required keys | `column`, `keys`, `require_all` |
| 33 | `has_valid_json_schema` | JSON matches schema | `column`, `schema` |

#### Uniqueness / Foreign Key (2)

| # | Function | Description | Arguments |
|---|----------|-------------|-----------|
| 34 | `is_unique` | Values are unique (dataset-level) | `columns` |
| 35 | `foreign_key` | Value exists in reference table | `column`, `ref_table`, `ref_column` |

> **Note:** `is_unique` is a dataset-level check and may be slower on large datasets.

---

### Custom Checks (33)

User-defined checks loaded from `custom_checks_library.py`. Add new checks anytime — no wheel rebuild needed.

#### Data Type Validation (3)

| # | Function | Description | Arguments |
|---|----------|-------------|-----------|
| 1 | `column_datatype_validator` | Cast to expected type | `column`, `expected_type`, `cast_if_possible` |
| 2 | `is_numeric` | Contains numeric values | `column` |
| 3 | `is_boolean_string` | Valid boolean representation | `column`, `true_values`, `false_values` |

**Supported types:** `int`, `bigint`, `double`, `float`, `decimal`, `date`, `timestamp`, `boolean`, `string`

#### Format Validation (6)

| # | Function | Description | Arguments |
|---|----------|-------------|-----------|
| 4 | `email_format_validator` | Valid email format | `column` |
| 5 | `phone_number_validator` | Valid phone number | `column`, `country_code` (US/UK/IN) |
| 6 | `url_validator` | Valid URL format | `column`, `require_https` |
| 7 | `ip_address_validator` | Valid IP address | `column`, `version` (v4/v6) |
| 8 | `credit_card_validator` | Valid credit card format | `column` |
| 9 | `ssn_validator` | Valid SSN format | `column`, `country` |

#### Date / Time Validation (5)

| # | Function | Description | Arguments |
|---|----------|-------------|-----------|
| 10 | `date_range_validator` | Date within min/max | `column`, `min_date`, `max_date` |
| 11 | `is_future_date` | Date is in the future | `column`, `allow_today` |
| 12 | `is_past_date` | Date is in the past | `column`, `allow_today` |
| 13 | `is_business_day` | Date is Mon-Fri | `column` |
| 14 | `timestamp_range_validator` | Timestamp within range | `column`, `min_timestamp`, `max_timestamp` |

#### String Validation (8)

| # | Function | Description | Arguments |
|---|----------|-------------|-----------|
| 15 | `string_length_range` | Length within range | `column`, `min_length`, `max_length` |
| 16 | `string_pattern_validator` | Matches regex pattern | `column`, `pattern` |
| 17 | `starts_with_validator` | Starts with prefix | `column`, `prefix` |
| 18 | `ends_with_validator` | Ends with suffix | `column`, `suffix` |
| 19 | `contains_validator` | Contains substring | `column`, `substring` |
| 20 | `no_special_characters` | Only alphanumeric | `column`, `allowed_chars` |
| 21 | `is_uppercase` | All uppercase | `column` |
| 22 | `is_lowercase` | All lowercase | `column` |

#### Numeric Validation (6)

| # | Function | Description | Arguments |
|---|----------|-------------|-----------|
| 23 | `numeric_precision_validator` | Precision and scale | `column`, `max_precision`, `max_scale` |
| 24 | `is_positive` | Value > 0 | `column`, `allow_zero` |
| 25 | `is_negative` | Value < 0 | `column`, `allow_zero` |
| 26 | `is_percentage` | Value 0-100 | `column` |
| 27 | `is_even` | Even integer | `column` |
| 28 | `is_odd` | Odd integer | `column` |

#### Business Logic Validation (5)

| # | Function | Description | Arguments |
|---|----------|-------------|-----------|
| 29 | `age_from_birthdate_validator` | Age within range | `birthdate_column`, `min_age`, `max_age` |
| 30 | `compare_columns` | Compare two columns | `column1`, `column2`, `operator` |
| 31 | `json_string_validator` | Valid JSON string | `column` |
| 32 | `currency_code_validator` | ISO 4217 currency code | `column` |
| 33 | `country_code_validator` | ISO country code | `column`, `code_type` (alpha2/alpha3) |

---

## Adding a New Custom Check

**3 steps — no wheel rebuild needed:**

### Step 1: Add function to `custom_checks_library.py`

```python
def my_new_check(column: str, threshold: int = 100) -> Column:
    """Check that value does not exceed threshold."""
    col_expr = F.col(column)
    return col_expr.isNull() | (col_expr <= threshold)
```

### Step 2: Add to table YAML

```yaml
checks:
  - criticality: warn
    check:
      function: my_new_check
      arguments:
        column: order_amount
        threshold: 10000
```

### Step 3: Run the notebook — done!

### Rules for Custom Checks

1. Return a PySpark `Column` expression (boolean: `True` = valid, `False` = invalid)
2. Always handle NULLs: `col_expr.isNull() | (your_condition)`
3. First parameter must be `column: str`
4. Use plain types for defaults — no `Optional` (DQX bug): use `str = ""`, `int = -1`

---

## Configuration

### Table Config (`table_customers.yaml`)

```yaml
table_name: customers
layer: bronze
custom_checks_module: /Workspace/Users/<user>/custom_checks_library.py
enabled: true

output:
  valid_data:
    catalog: my_catalog
    schema: silver
    table: customers
  quarantine_data:
    catalog: my_catalog
    schema: bronze_quarantine
    table: q_customers
  metrics:
    catalog: my_catalog
    schema: dq_observe
    table: dq_metrics
  summary:
    catalog: my_catalog
    schema: dq_observe
    table: dq_summary

checks:
  - criticality: error
    check:
      function: is_not_null
      arguments:
        column: customer_id
  - criticality: warn
    check:
      function: email_format_validator
      arguments:
        column: email
```

### Layer Config (`layer_bronze.yaml`)

```yaml
layer: bronze
default_criticality: warn
output:
  quarantine_data:
    catalog: my_catalog
    schema: bronze_quarantine
    table_prefix: q_
```

---

## Output Tables

| Table | Description |
|-------|-------------|
| `silver.customers` | Valid rows (passed all ERROR checks) |
| `bronze_quarantine.q_customers` | Quarantined rows (failed at least one ERROR check) |
| `dq_observe.dq_metrics` | Per-check failure counts and rates |
| `dq_observe.dq_summary` | Overall pass rate and row counts per execution |

---

## Databricks SQL Dashboard

### Prerequisites

1. Databricks Workspace with SQL warehouses
2. Unity Catalog configured
3. DQ Accelerator installed and test data generated

### Setup

```bash
# Generate test data
python scripts/generate_test_data.py --catalog dbx_dataquality --customers 1000 --days 90

# Create dashboard views
python scripts/setup_dashboard.py --catalog dbx_dataquality --schema dq_observe --recreate
```

### Verify

```sql
SELECT COUNT(*) FROM dbx_dataquality.dq_observe.dq_metrics;
SELECT COUNT(*) FROM dbx_dataquality.dq_observe.dq_summary;
SELECT * FROM dbx_dataquality.dq_observe.vw_daily_quality_scores ORDER BY date DESC LIMIT 10;
```

---

## Technical Notes

- **Spark Connect Compatibility**: Includes `isinstance` monkey-patch for DQX 0.12.0 `typing.Union` bug in Python 3.10+
- **Custom Check Wrapper**: `functools.wraps` preserves function signatures for DQX validation
- **Boolean-to-DQX Conversion**: Custom checks return `True/False`; framework auto-converts to DQX format (`null`/error message)
- **Delta Schema Evolution**: `mergeSchema` enabled for append operations
- **Quarantine Logic**: Only ERROR rows quarantined; WARNING rows remain in valid output

---

## Databricks SQL Dashboard — Detailed Setup

### Prerequisites

1. **Databricks Workspace** with SQL warehouses enabled
2. **Unity Catalog** configured
3. **DQ Accelerator** installed and configured
4. **Test Data** generated

### Step 1: Data Preparation

#### Step 1.1: Generate Test Data

```bash
# Generate financial test data
python scripts/generate_test_data.py \
    --catalog dbx_dataquality \
    --customers 1000 \
    --days 90

# Generate DQ metrics test data
python scripts/generate_dq_test_data.py \
    --catalog dbx_dataquality \
    --schema dq_observe \
    --days 30
```

#### Step 1.2: Verify Test Data

```sql
-- Check metrics data
SELECT COUNT(*) as metric_count FROM dbx_dataquality.dq_observe.dq_metrics;

-- Check summary data
SELECT COUNT(*) as summary_count FROM dbx_dataquality.dq_observe.dq_summary;

-- Verify date range
SELECT 
    MIN(DATE(execution_date)) as earliest,
    MAX(DATE(execution_date)) as latest
FROM dbx_dataquality.dq_observe.dq_summary;
```

### Step 2: Dashboard Views Setup

#### Step 2.1: Create Dashboard Views

```bash
# Create all dashboard views
python scripts/setup_dashboard.py \
    --catalog dbx_dataquality \
    --schema dq_observe \
    --recreate
```

#### Step 2.2: Verify Views Created

```sql
-- List all views
SHOW VIEWS IN dbx_dataquality.dq_observe;

-- Test a view
SELECT * FROM dbx_dataquality.dq_observe.vw_daily_quality_scores
ORDER BY date DESC
LIMIT 10;
```
