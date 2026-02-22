/* @bruin

name: dashboard.hello_bq
type: bq.sql

materialization:
  type: table

depends:
  - hello_python
  - uri: bigquery://project_id/dataset_id/table_id

columns:
  - name: one
    type: integer
    description: Just a number
    primary_key: true
    checks:
      - name: unique
      - name: not_null
      - name: positive
      - name: accepted_values
        value:
          - 1
          - 2

custom_checks:
  - name: This is a custom check name
    value: 2
    query: select count(*) from dashboard.hello_bq

@bruin */

with segment_map as (
    select *
    from (
        values
            (1, 'Enterprise'),
            (2, 'Startup'),
            (3, 'Research')
    ) as t(customer_id, segment)
)
select
    customers.customer_id,
    customers.customer_name,
    coalesce(segment_map.segment, 'General') as segment
from hello_python as customers
left join segment_map
    on customers.customer_id = segment_map.customer_id
order by customers.customer_id
