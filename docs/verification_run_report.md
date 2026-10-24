# DataOps Sentinel - pipeline report

Logical dates 2026-01-01 to 2026-01-09. Generated 2026-10-23 18:32:55.114785+03 from the audit tables.
All source data is synthetic.

## Runs

| Logical date | Run | Trigger | Status | Duration (s) | Error |
|---|---|---|---|---|---|
| 2026-01-01 | verify__2026-01-01 | verify | success | 5.06 | - |
| 2026-01-02 | verify__2026-01-02 | verify | success | 8.69 | - |
| 2026-01-03 | verify__2026-01-03 | verify | success | 8.72 | - |
| 2026-01-04 | verify__2026-01-04 | verify | success | 5.17 | - |
| 2026-01-05 | verify__2026-01-05 | verify | success | 4.91 | - |
| 2026-01-06 | verify__2026-01-06 | verify | success | 5.06 | - |
| 2026-01-07 | verify__2026-01-07 | verify | success | 8.31 | - |
| 2026-01-08 | verify__2026-01-08 | verify | success | 8.65 | - |
| 2026-01-09 | verify__2026-01-09 | verify | success | 9.37 | - |

## Records

| Logical date | Dataset | Read | Accepted | Rejected | Reject % | Loaded on try |
|---|---|---|---|---|---|---|
| 2026-01-01 | customers | 150 | 149 | 1 | 0.67 | 1 |
| 2026-01-01 | orders | 111 | 108 | 3 | 2.70 | 1 |
| 2026-01-01 | payments | 104 | 103 | 1 | 0.96 | 1 |
| 2026-01-01 | products | 60 | 60 | 0 | 0.00 | 1 |
| 2026-01-02 | customers | 12 | 12 | 0 | 0.00 | 3 |
| 2026-01-02 | orders | 129 | 126 | 3 | 2.33 | 3 |
| 2026-01-02 | payments | 123 | 122 | 1 | 0.81 | 3 |
| 2026-01-02 | products | 3 | 3 | 0 | 0.00 | 3 |
| 2026-01-03 | customers | 5 | 5 | 0 | 0.00 | 3 |
| 2026-01-03 | orders | 162 | 160 | 2 | 1.23 | 3 |
| 2026-01-03 | payments | 162 | 161 | 1 | 0.62 | 3 |
| 2026-01-03 | products | 0 | 0 | 0 | 0.00 | 3 |
| 2026-01-04 | customers | 10 | 10 | 0 | 0.00 | 1 |
| 2026-01-04 | orders | 145 | 142 | 3 | 2.07 | 1 |
| 2026-01-04 | payments | 146 | 145 | 1 | 0.68 | 1 |
| 2026-01-04 | products | 2 | 2 | 0 | 0.00 | 1 |
| 2026-01-05 | customers | 14 | 14 | 0 | 0.00 | 1 |
| 2026-01-05 | orders | 134 | 131 | 3 | 2.24 | 2 |
| 2026-01-05 | payments | 121 | 120 | 1 | 0.83 | 1 |
| 2026-01-05 | products | 2 | 2 | 0 | 0.00 | 1 |
| 2026-01-06 | customers | 9 | 9 | 0 | 0.00 | 1 |
| 2026-01-06 | orders | 123 | 120 | 3 | 2.44 | 1 |
| 2026-01-06 | payments | 124 | 123 | 1 | 0.81 | 1 |
| 2026-01-06 | products | 1 | 1 | 0 | 0.00 | 1 |
| 2026-01-07 | customers | 11 | 11 | 0 | 0.00 | 1 |
| 2026-01-07 | orders | 109 | 106 | 3 | 2.75 | 1 |
| 2026-01-07 | payments | 111 | 110 | 1 | 0.90 | 1 |
| 2026-01-07 | products | 2 | 2 | 0 | 0.00 | 1 |
| 2026-01-08 | customers | 8 | 8 | 0 | 0.00 | 2 |
| 2026-01-08 | orders | 136 | 134 | 2 | 1.47 | 2 |
| 2026-01-08 | payments | 136 | 135 | 1 | 0.74 | 2 |
| 2026-01-08 | products | 2 | 2 | 0 | 0.00 | 2 |
| 2026-01-09 | customers | 11 | 11 | 0 | 0.00 | 2 |
| 2026-01-09 | orders | 139 | 136 | 3 | 2.16 | 2 |
| 2026-01-09 | payments | 131 | 130 | 1 | 0.76 | 2 |
| 2026-01-09 | products | 3 | 3 | 0 | 0.00 | 2 |

## Data quality

Outcomes: pass=98, skipped=2, warn=35.

| Logical date | Stage | Check | Severity | Outcome | Observed | Details |
|---|---|---|---|---|---|---|
| 2026-01-01 | raw | order_value_vs_captured_payments | blocking | warn | 0.02 | non-cancelled order value vs captured payments for the same orders: expected 20026.54, actual 19676.95, difference 349.59 |
| 2026-01-01 | raw | payments_order_fk | blocking | warn | 0.02 | 2/103 payments reference an unknown order |
| 2026-01-01 | raw | reject_rate_orders | blocking | warn | 0.03 | orders: 3/111 records rejected |
| 2026-01-01 | raw | rejected_records_by_reason | warning | warn | 5.00 | customers.missing_required_field=1, orders.invalid_status=1, orders.invalid_timestamp=1, orders.negative_amount=1, payments.negative_amount=1 |
| 2026-01-02 | raw | orders_customer_fk | blocking | warn | 0.01 | 1/126 orders reference an unknown customer |
| 2026-01-02 | raw | payments_order_fk | blocking | warn | 0.02 | 3/122 payments reference an unknown order |
| 2026-01-02 | raw | reject_rate_orders | blocking | warn | 0.02 | orders: 3/129 records rejected |
| 2026-01-02 | raw | rejected_records_by_reason | warning | warn | 4.00 | orders.invalid_timestamp=1, orders.missing_required_field=2, payments.negative_amount=1 |
| 2026-01-03 | raw | orders_customer_fk | blocking | warn | 0.01 | 1/160 orders reference an unknown customer |
| 2026-01-03 | raw | payments_order_fk | blocking | warn | 0.01 | 1/161 payments reference an unknown order |
| 2026-01-03 | raw | reject_rate_orders | blocking | warn | 0.01 | orders: 2/162 records rejected |
| 2026-01-03 | raw | rejected_records_by_reason | warning | warn | 4.00 | orders.duplicate_key=1, orders.invalid_timestamp=1, orders.negative_amount=1, payments.negative_amount=1 |
| 2026-01-04 | raw | order_value_vs_captured_payments | blocking | warn | 0.01 | non-cancelled order value vs captured payments for the same orders: expected 28021.62, actual 27628.77, difference 392.85 |
| 2026-01-04 | raw | payments_order_fk | blocking | warn | 0.03 | 4/145 payments reference an unknown order |
| 2026-01-04 | raw | reject_rate_orders | blocking | warn | 0.02 | orders: 3/145 records rejected |
| 2026-01-04 | raw | rejected_records_by_reason | warning | warn | 4.00 | orders.inconsistent_total=1, orders.invalid_status=1, orders.negative_amount=1, payments.negative_amount=1 |
| 2026-01-05 | raw | order_value_vs_captured_payments | blocking | warn | 0.01 | non-cancelled order value vs captured payments for the same orders: expected 22360.78, actual 22099.58, difference 261.20 |
| 2026-01-05 | raw | payments_order_fk | blocking | warn | 0.01 | 1/120 payments reference an unknown order |
| 2026-01-05 | raw | reject_rate_orders | blocking | warn | 0.02 | orders: 3/134 records rejected |
| 2026-01-05 | raw | rejected_records_by_reason | warning | warn | 4.00 | orders.duplicate_key=1, orders.inconsistent_total=1, orders.negative_amount=1, payments.negative_amount=1 |
| 2026-01-06 | raw | payments_order_fk | blocking | warn | 0.02 | 3/123 payments reference an unknown order |
| 2026-01-06 | raw | reject_rate_orders | blocking | warn | 0.02 | orders: 3/123 records rejected |
| 2026-01-06 | raw | rejected_records_by_reason | warning | warn | 4.00 | orders.inconsistent_total=1, orders.missing_required_field=1, orders.negative_amount=1, payments.negative_amount=1 |
| 2026-01-07 | raw | order_value_vs_captured_payments | blocking | warn | 0.01 | non-cancelled order value vs captured payments for the same orders: expected 18883.41, actual 18680.26, difference 203.15 |
| 2026-01-07 | raw | payments_order_fk | blocking | warn | 0.01 | 1/110 payments reference an unknown order |
| 2026-01-07 | raw | reject_rate_orders | blocking | warn | 0.03 | orders: 3/109 records rejected |
| 2026-01-07 | raw | rejected_records_by_reason | warning | warn | 4.00 | orders.duplicate_key=2, orders.missing_required_field=1, payments.negative_amount=1 |
| 2026-01-08 | raw | orders_customer_fk | blocking | warn | 0.01 | 1/134 orders reference an unknown customer |
| 2026-01-08 | raw | payments_order_fk | blocking | warn | 0.01 | 2/135 payments reference an unknown order |
| 2026-01-08 | raw | reject_rate_orders | blocking | warn | 0.01 | orders: 2/136 records rejected |
| 2026-01-08 | raw | rejected_records_by_reason | warning | warn | 3.00 | orders.inconsistent_total=1, orders.negative_amount=1, payments.negative_amount=1 |
| 2026-01-09 | raw | orders_customer_fk | blocking | warn | 0.01 | 1/136 orders reference an unknown customer |
| 2026-01-09 | raw | payments_order_fk | blocking | warn | 0.02 | 3/130 payments reference an unknown order |
| 2026-01-09 | raw | reject_rate_orders | blocking | warn | 0.02 | orders: 3/139 records rejected |
| 2026-01-09 | raw | rejected_records_by_reason | warning | warn | 4.00 | orders.invalid_timestamp=1, orders.missing_required_field=1, orders.negative_amount=1, payments.negative_amount=1 |

## Task errors

| Logical date | Task | Try | Error type | Error |
|---|---|---|---|---|
| 2026-01-05 | ingest_orders | 1 | TransientPipelineError | injected failure (ingest_orders 2026-01-05, first attempt) |
| 2026-01-08 | quality_raw | 1 | DataQualityError | 2 blocking quality check(s) failed for 2026-01-08: reject_rate_orders, payments_order_fk |
| 2026-01-09 | ingest_orders | 1 | SourceFileError | orders: missing required columns ['status'] in /var/tmp/sentinel-work/landing/2026-01-09/orders.csv |

## Repeated attempts

Tasks that ran more than once within the same run (Airflow retries or manual re-runs).

| Logical date | Task | Attempts | Final status |
|---|---|---|---|
| 2026-01-02 | ingest_customers | 2 | success |
| 2026-01-02 | ingest_products | 2 | success |
| 2026-01-02 | ingest_orders | 2 | success |
| 2026-01-02 | ingest_payments | 2 | success |
| 2026-01-02 | quality_raw | 2 | success |
| 2026-01-02 | dbt_build | 2 | success |
| 2026-01-02 | quality_marts | 2 | success |
| 2026-01-03 | ingest_customers | 3 | success |
| 2026-01-03 | ingest_products | 3 | success |
| 2026-01-03 | ingest_orders | 3 | success |
| 2026-01-03 | ingest_payments | 3 | success |
| 2026-01-03 | quality_raw | 3 | success |
| 2026-01-03 | dbt_build | 3 | success |
| 2026-01-03 | quality_marts | 3 | success |
| 2026-01-05 | ingest_orders | 2 | success |
| 2026-01-08 | ingest_customers | 2 | success |
| 2026-01-08 | ingest_products | 2 | success |
| 2026-01-08 | ingest_orders | 2 | success |
| 2026-01-08 | ingest_payments | 2 | success |
| 2026-01-08 | quality_raw | 2 | success |
| 2026-01-09 | ingest_customers | 2 | success |
| 2026-01-09 | ingest_products | 2 | success |
| 2026-01-09 | ingest_orders | 2 | success |
