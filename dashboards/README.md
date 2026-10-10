# Sales dashboard (Power BI)

A four-page report on top of the warehouse: sales, customers, order
operations and data quality.

A `.pbix` file cannot be generated from code, so this folder holds what is
needed to build it step by step and to prove its numbers are right:

| File | What it is |
| --- | --- |
| `measures.dax` | The 34 measures of the report, as one DAX query that also prints their values |
| `check_figures.sql` | The same figures computed in SQL on the warehouse, to compare with the report |
| `theme.json` | Colours and fonts of the report |

Requirements: Power BI Desktop (Windows), and the platform running with at
least one complete pipeline run, so that the `core` and `marts` schemas are
filled (`docker compose ps` shows `warehouse` as healthy).

## 1. Turn off relationship autodetection

File > Options and settings > Options > Current file > Data Load: untick
**Autodetect new relationships after data is loaded**.

Left on, Power BI links the two fact tables on `order_id`, which makes some
filters travel by two paths and the model ambiguous. The relationships are
created by hand in step 3.

## 2. Load the tables

Home > Get data > More > **PostgreSQL database**.

| Field | Value |
| --- | --- |
| Server | `localhost:5432` (or the port set as `WAREHOUSE_HOST_PORT` in `.env`) |
| Database | `warehouse` |
| Data connectivity mode | Import |

When asked for credentials, pick the **Database** tab: user `warehouse`,
password `warehouse` (or `WAREHOUSE_PASSWORD` from `.env`). Power BI then
warns that the connection is not encrypted: accept, the database is on your
own machine.

In the Navigator, tick these nine tables and click **Transform Data**:

| Schema | Tables |
| --- | --- |
| `core` | `dim_date`, `dim_channels`, `dim_customers`, `dim_products`, `fct_sales`, `fct_orders` |
| `marts` | `mart_customers`, `mart_data_quality`, `mart_reject_history` |

Power BI names each query with its schema in front (`core dim_date` or
`core.dim_date`, depending on the version). In the Power Query editor,
**rename every query to the table name alone**: `dim_date`, `fct_sales`,
and so on. The measures refer to the tables by these names. Then Home >
Close & Apply.

`mart_daily_sales` and `mart_product_demand_features` are not loaded. The
first is a pre-aggregated version of what the star schema already gives;
the second is a training table for a forecasting model, not for a report.

## 3. Build the model

In the Model view, create these relationships by dragging the column of the
first table onto the column of the second.

| From (many) | To (one) | Cardinality | Cross-filter |
| --- | --- | --- | --- |
| `fct_sales[order_date_key]` | `dim_date[date_key]` | Many to one | Single |
| `fct_sales[channel]` | `dim_channels[channel]` | Many to one | Single |
| `fct_sales[customer_key]` | `dim_customers[customer_key]` | Many to one | Single |
| `fct_sales[product_key]` | `dim_products[product_key]` | Many to one | Single |
| `fct_orders[order_date_key]` | `dim_date[date_key]` | Many to one | Single |
| `fct_orders[channel]` | `dim_channels[channel]` | Many to one | Single |
| `fct_orders[customer_key]` | `dim_customers[customer_key]` | Many to one | Single |
| `mart_customers[customer_key]` | `dim_customers[customer_key]` | One to one | Both |

`mart_data_quality` and `mart_reject_history` stay unconnected: they
describe the pipeline, not the sales.

There must be no relationship between `fct_sales` and `fct_orders`. If
Power BI created one, delete it.

Then three settings:

- **Date table**: select `dim_date`, Table tools > Mark as date table, date
  column `date_day`. The month-over-month and 7-day measures need it.
- **Sort order**: select the column `dim_date[month_name]`, Column tools >
  Sort by column > `month`. Same for `dim_date[day_name]`, sorted by
  `day_of_week`. Without it, months and days are listed alphabetically.
- **Slicers and axes always use the dimensions**: `dim_channels[channel_name]`
  rather than the `channel` column of a fact, `dim_date[date_day]` rather
  than `order_date`. A dimension filters both facts; a fact column only
  filters its own table.

## 4. Add the measures

Open the DAX query view, paste the whole content of `measures.dax` and
click **Run**. The result grid shows every measure over the whole
warehouse; keep it for step 7. Then click **Update model with changes** to
save the 34 measures into the model.

If your version has no DAX query view, the header of `measures.dax` explains
how to create the measures one by one.

Set the formats in the Measure tools ribbon:

| Format | Measures |
| --- | --- |
| Currency, euro, 0 decimals | Net Sales, Gross Sales, Discounts, Billed Amount, Shipping Fees, Lifetime Spend, Net Sales Previous Month, Net Sales 7-Day Average |
| Currency, euro, 2 decimals | Average Basket, Average Order Value, Average Spend per Buyer |
| Percentage, 1 decimal | Discount Rate, Cancellation Rate, Return Rate, Repeat Rate, Pass Rate, Run Pass Rate, Net Sales MoM % |
| Whole number, thousands separator | Units Sold, Orders, Orders Placed, Cancelled Orders, Returned Orders, Customers, Buyers, Repeat Buyers, Checks and its three variants, Rejected Records, Rejected Records per Run |
| Decimal number, 1 decimal | Average Hours to Ship, Average Days to Deliver |

## 5. Apply the theme

View > Themes > Browse for themes > `theme.json`.

## 6. Build the pages

Four pages, 16:9. On the first three, put the slicers in a strip at the top
and the cards in a row under it.

### Page 1: Sales overview

Slicers: `dim_date[date_day]` (between), `dim_channels[channel_name]`,
`dim_products[category]`.

| Visual | Fields |
| --- | --- |
| 5 cards | Net Sales, Orders, Units Sold, Average Basket, Discount Rate |
| Line chart, full width | X: `dim_date[date_day]`. Y: Net Sales, Net Sales 7-Day Average |
| Clustered column chart | X: `dim_date[year_month]`. Y: Net Sales. Tooltip: Net Sales MoM % |
| Bar chart | Y: `dim_products[category]`. X: Net Sales |
| Donut chart | Legend: `dim_channels[channel_name]`. Values: Net Sales |
| Table, top 10 | `dim_products[product_name]`, Net Sales, Units Sold. Filter on this visual: Top N = 10 by Net Sales |

Every measure of this page comes from `fct_sales`, so the three slicers act
on all of it. The last month is always incomplete: its month-over-month
figure compares a few days with a full month.

### Page 2: Customers

No date slicer: this page shows lifetime figures, one row per customer.
Slicer: `mart_customers[customer_segment]`.

| Visual | Fields |
| --- | --- |
| 4 cards | Customers, Buyers, Repeat Rate, Average Spend per Buyer |
| Clustered column chart | X: `mart_customers[customer_segment]`. Y: Customers |
| Clustered column chart | X: `mart_customers[customer_segment]`. Y: Lifetime Spend |
| Bar chart, top 10 | Y: `dim_customers[department_code]`. X: Net Sales. Filter on this visual: Top N = 10 by Net Sales |
| Histogram of recency | Column chart. X: `mart_customers[days_since_last_order]`, grouped in bins of 30 (right-click the field > New group). Y: Customers |
| Table, top 20 | `mart_customers[full_name]`, `city`, `orders`, `total_spent`, `last_order_date`. Top N = 20 by `total_spent` |

### Page 3: Operations

Slicers: `dim_date[date_day]` (between), `dim_channels[channel_name]`.

| Visual | Fields |
| --- | --- |
| 5 cards | Orders Placed, Cancellation Rate, Return Rate, Average Hours to Ship, Average Days to Deliver |
| Bar chart | Y: `fct_orders[order_status]`. X: Orders Placed |
| Line chart | X: `dim_date[year_month]`. Y: Cancellation Rate, Return Rate |
| Line chart | X: `dim_date[year_month]`. Y: Average Days to Deliver |
| Clustered column chart | X: `fct_orders[payment_method]`. Y: Orders Placed |
| Matrix | Rows: `dim_channels[channel_name]`. Values: Orders Placed, Billed Amount, Average Order Value, Cancellation Rate, Return Rate |

This page reads `fct_orders`, which has no product: do not add a category
slicer here, it would filter nothing.

### Page 4: Data quality

No slicer needed: the cards and the two detail visuals show the latest run
of the quality job.

| Visual | Fields |
| --- | --- |
| 5 cards | Checks, Checks Passed, Checks in Warning, Checks Failed, Rejected Records |
| Matrix | Rows: `mart_data_quality[layer]`. Columns: `mart_data_quality[dimension]`. Values: Checks |
| Table of problems | `checked_table`, `check_name`, `severity`, `status`, `description`, `observed_value`. Filters on this visual: `is_latest_run` is True, `is_problem` is True |
| Bar chart | Y: `mart_reject_history[reason]`. X: Rejected Records. Legend: `mart_reject_history[source]` |
| Line chart | X: `mart_data_quality[checked_at]`. Y: Run Pass Rate |
| Line chart | X: `mart_reject_history[checked_at]`. Y: Rejected Records per Run. Legend: `reason` |

The two line charts need a few runs of the pipeline before they show a
trend. Give `status` its colours by hand in the table (conditional
formatting on the cell background): passed green, warning amber, failed red.

## 7. Check the numbers

Run the check figures on the warehouse:

```bash
docker compose cp dashboards/check_figures.sql warehouse:/tmp/check_figures.sql
docker compose exec warehouse psql -U warehouse -d warehouse -f /tmp/check_figures.sql
```

With every slicer cleared, each card and each chart of the report must show
the figure of the query of the same name. The result grid of step 4 gives
the same comparison for all the measures at once. Percentages are printed
as 3.03 by the SQL and shown as 3.0 % by Power BI.

When a number differs:

| Symptom | Likely cause |
| --- | --- |
| A total is right but the same value repeats on every bar | The relationship between the fact and that dimension is missing |
| Orders Placed ignores the category slicer | Expected: see page 3 |
| Net Sales Previous Month is empty for every month | `dim_date` is not marked as a date table |
| Everything is slightly lower than the SQL | The pipeline ran after the last refresh: Home > Refresh |
| A measure is in error | A query was not renamed in step 2 |

## 8. Save and publish

- Save the report as `rapport.pbix` at the root of the repository and commit it. The
  file embeds the imported data, so expect a few megabytes.
- Export one image per page (File > Export > Export to PDF, or a screenshot)
  to `docs/images/` as `dashboard_sales.png`, `dashboard_customers.png`,
  `dashboard_operations.png` and `dashboard_quality.png`. The main README
  already links to these four files.

After each new run of the pipeline, Home > Refresh reloads the nine tables.
