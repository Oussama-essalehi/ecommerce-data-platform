"""Data quality: automated checks on the lake, and a history of their results.

Checks are written with Great Expectations and grouped by what they
protect (the "dimension"):

    completeness   required values are there
    uniqueness     no duplicate keys
    validity       values are in the expected set, range or format
    consistency    values agree with each other (totals, dates, references)
    freshness      the data is recent
    volume         row counts and reject rates are in their usual range

Every check has a severity:

    error     the data is wrong. The job exits with a failure, which stops
              the pipeline before the warehouse is loaded.
    warning   something to look at, but the data is usable. Freshness checks
              are warnings: refusing to publish stale data only makes it
              staler.

Each run appends its results to the Delta table `quality.check_results`,
passed or not, so a check that starts drifting can be seen over time.
"""

TABLES = ["check_results", "reject_history"]

DIMENSIONS = ["completeness", "uniqueness", "validity", "consistency", "freshness", "volume"]
ERROR = "error"
WARNING = "warning"

CHECK_RESULTS_SCHEMA = """
    run_id STRING, checked_at TIMESTAMP, layer STRING, table_name STRING,
    check_name STRING, dimension STRING, severity STRING, description STRING,
    expectation STRING, column_name STRING, success BOOLEAN, status STRING,
    element_count BIGINT, unexpected_count BIGINT, unexpected_percent DOUBLE,
    observed_value STRING, sample STRING
"""

REJECT_HISTORY_SCHEMA = """
    run_id STRING, checked_at TIMESTAMP, source STRING, reason STRING, rejected BIGINT
"""
