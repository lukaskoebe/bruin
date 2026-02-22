/* @bruin

name: my_sql_asset_4
type: duckdb.sql
meta:
  web_table_dense: "false"
  web_view: table

materialization:
  type: table

depends:
  - my_sql_asset_3

columns:
  - name: col1
    type: BIGINT
  - name: col2
    type: VARCHAR
  - name: col3
    type: DOUBLE

@bruin */

select * from my_sql_asset_3