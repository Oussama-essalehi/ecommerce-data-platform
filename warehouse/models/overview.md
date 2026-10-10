{% docs __overview__ %}

# E-commerce data warehouse

This site documents the PostgreSQL warehouse of the e-commerce data platform:
every table, every column, the tests that guard them and how they depend on
each other.

## How the warehouse is organised

| Schema | Built by | Holds |
| --- | --- | --- |
| `lake` | the `warehouse-load` Spark job | the gold tables of the data lake, copied as-is, and the history of the quality checks |
| `reference` | dbt seeds | small hand-maintained tables: French public holidays, sales periods |
| `staging` | dbt | one view per source table, names and types settled |
| `core` | dbt | the star schema: `dim_date`, `dim_channels`, `dim_customers`, `dim_products`, `fct_sales`, `fct_orders` |
| `marts` | dbt | tables shaped for one use: dashboards, customer analysis, demand-forecast features, data-quality monitoring |

## Where to start

- **The lineage graph**: the round button at the bottom right of any page.
  Type `+exposure:sales_dashboard` in the `--select` box to see everything
  the Power BI report depends on, back to the lake.
- **The star schema**: in the left panel, Projects > ecommerce > models > core.
- **What reads the warehouse**: the Exposures section of the left panel.

## Conventions

- Surrogate keys are a hash of the business key, so a dimension can be
  rebuilt without breaking the facts that point to it.
- Every dimension has an "Unknown" member. A sale whose customer or product
  cannot be identified points to it instead of being dropped.
- Cancelled orders stay in the facts. `units_sold`, `net_sales_amount` and
  `billed_amount` count them as zero; the other amounts keep what was ordered.
- Amounts are in euros. Timestamps are stored in UTC; `order_date` is the
  day in Paris.

{% enddocs %}
