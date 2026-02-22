import {
  SiClickhouse,
  SiDatabricks,
  SiDuckdb,
  SiGooglebigquery,
  SiMysql,
  SiPostgresql,
  SiSnowflake,
  SiTrino,
  SiGoogledataproc,
  SiPython,
  SiDbt,
  SiAirbyte,
  SiPrometheus,
  SiGrafana,
  SiSqlite,
  SiR,
} from "react-icons/si";
import { GiBearFace } from "react-icons/gi";

type AssetTypeIconProps = {
  assetType?: string;
  connection?: string;
  meta?: Record<string, string>;
  className?: string;
};

export function AssetTypeIcon({
  assetType,
  connection,
  meta,
  className,
}: AssetTypeIconProps) {
  const icon = resolveAssetIcon(assetType, connection, meta);

  if (!icon) {
    return null;
  }

  return <span className={className}>{icon}</span>;
}

function resolveAssetIcon(
  assetType?: string,
  connection?: string,
  meta?: Record<string, string>
): React.ReactNode | null {
  const type = normalize(assetType);
  const provider = providerFromAssetType(type);
  const fallback = normalize(
    [
      connection,
      meta?.connection,
      meta?.platform,
      meta?.engine,
      meta?.destination,
    ]
      .filter(Boolean)
      .join(" ")
  );
  const value = provider || fallback;

  if (isPythonType(type)) {
    return SiPython({ size: 16 });
  }
  if (isRType(type)) {
    return SiR({ size: 16 });
  }
  if (isIngestrType(type)) {
    return GiBearFace({ size: 16 });
  }
  if (isSensorType(type)) {
    return SiPrometheus({ size: 16 });
  }
  if (isSeedType(type)) {
    return SiDbt({ size: 16 });
  }
  if (isDashboardType(type)) {
    return SiGrafana({ size: 16 });
  }

  if (has(value, "athena")) {
    return null;
  }
  if (has(value, "clickhouse")) {
    return SiClickhouse({ size: 16 });
  }
  if (has(value, "databricks")) {
    return SiDatabricks({ size: 16 });
  }
  if (has(value, "motherduck")) {
    return SiDuckdb({ size: 16 });
  }
  if (has(value, "duckdb")) {
    return SiDuckdb({ size: 16 });
  }
  if (has(value, "oracle")) {
    return null;
  }
  if (has(value, "bigquery")) {
    return SiGooglebigquery({ size: 16 });
  }
  if (has(value, "microsoft sql server", "sqlserver", "mssql")) {
    return null;
  }
  if (has(value, "fabric")) {
    return null;
  }
  if (has(value, "mysql")) {
    return SiMysql({ size: 16 });
  }
  if (has(value, "postgres", "postgresql")) {
    return SiPostgresql({ size: 16 });
  }
  if (has(value, "redshift")) {
    return null;
  }
  if (has(value, "snowflake")) {
    return SiSnowflake({ size: 16 });
  }
  if (has(value, "synapse")) {
    return null;
  }
  if (has(value, "amazons3", "s3")) {
    return null;
  }
  if (has(value, "trino")) {
    return SiTrino({ size: 16 });
  }
  if (has(value, "emr")) {
    return null;
  }
  if (has(value, "dataproc")) {
    return null;
  }

  if (has(type, ".sql") || has(value, "sql")) {
    return SiSqlite({ size: 16 });
  }

  return null;
}

function providerFromAssetType(assetType: string): string {
  if (!assetType) {
    return "";
  }

  const [prefix] = assetType.split(".");

  const providersByPrefix: Record<string, string> = {
    athena: "athena",
    bq: "bigquery",
    clickhouse: "clickhouse",
    databricks: "databricks",
    dataproc_serverless: "dataproc",
    duckdb: "duckdb",
    emr_serverless: "emr",
    fabric: "fabric",
    fw: "fabric",
    motherduck: "motherduck",
    ms: "mssql",
    my: "mysql",
    oracle: "oracle",
    pg: "postgres",
    rs: "redshift",
    sf: "snowflake",
    synapse: "synapse",
    trino: "trino",
    s3: "s3",
  };

  return providersByPrefix[prefix] ?? assetType;
}

function isSeedType(assetType: string) {
  return assetType.endsWith(".seed");
}

function isSensorType(assetType: string) {
  return assetType.includes(".sensor.");
}

function isIngestrType(assetType: string) {
  return assetType === "ingestr";
}

function isPythonType(assetType: string) {
  return assetType === "python" || assetType.includes("python_sdk");
}

function isRType(assetType: string) {
  return assetType === "r";
}

function isDashboardType(assetType: string) {
  return assetType.includes("dashboard") || assetType === "grafana";
}

function normalize(value?: string) {
  return (value ?? "").trim().toLowerCase();
}

function has(value: string, ...tokens: string[]) {
  return tokens.some((token) => value.includes(token));
}
