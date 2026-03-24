import { format } from "sql-formatter";

type SQLFormatterLanguage = NonNullable<Parameters<typeof format>[1]>["language"];

const ASSET_TYPE_TO_SQL_FORMATTER_LANGUAGE: Record<string, SQLFormatterLanguage> = {
  "athena.sql": "trino",
  "bq.sql": "bigquery",
  "databricks.sql": "spark",
  "duckdb.sql": "duckdb",
  "fabric.sql": "transactsql",
  "motherduck.sql": "duckdb",
  "ms.sql": "transactsql",
  "my.sql": "mysql",
  "oracle.sql": "plsql",
  "pg.sql": "postgresql",
  "rs.sql": "redshift",
  "sf.sql": "snowflake",
  "synapse.sql": "transactsql",
  "trino.sql": "trino",
};

export function formatAssetSQL(sql: string, assetType?: string | null) {
  const language =
    ASSET_TYPE_TO_SQL_FORMATTER_LANGUAGE[(assetType ?? "").trim().toLowerCase()] ?? "sql";

  try {
    return format(sql, {
      language,
      tabWidth: 2,
    });
  } catch {
    return null;
  }
}
