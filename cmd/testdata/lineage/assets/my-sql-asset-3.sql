/* @bruin

name: my_sql_asset_3
type: duckdb.sql
meta:
  web_table_dense: "false"
  web_view: table

materialization:
  type: table

columns:
  - name: col1
    type: BIGINT
  - name: col2
    type: VARCHAR
  - name: col3
    type: DOUBLE
  - name: "1"
    type: INTEGER
  - name: asdf
    type: INTEGER

@bruin */

select 2 as blub