"use client";

import { Link } from "@tanstack/react-router";
import { CopyPlus, PencilLine, Plus, Settings2 } from "lucide-react";
import { ReactNode } from "react";

import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { WorkspaceConfigEnvironment } from "@/lib/types";

type WorkspaceConfigContentProps = {
  view: "environments" | "connections";
  configPath: string;
  defaultEnvironment?: string;
  selectedEnvironmentName?: string | null;
  environments: WorkspaceConfigEnvironment[];
  selectedConnectionName?: string | null;
  loading: boolean;
  onSelectEnvironment: (name: string) => void;
  onSelectConnection: (name: string) => void;
  onCreateEnvironment: () => void;
  onCloneEnvironment: () => void;
  onCreateConnection: () => void;
};

export function WorkspaceConfigContent({
  view,
  configPath,
  defaultEnvironment,
  selectedEnvironmentName,
  environments,
  selectedConnectionName,
  loading,
  onSelectEnvironment,
  onSelectConnection,
  onCreateEnvironment,
  onCloneEnvironment,
  onCreateConnection,
}: WorkspaceConfigContentProps) {
  const activeEnvironment =
    environments.find((environment) => environment.name === selectedEnvironmentName) ??
    environments[0] ??
    null;

  return (
    <div className="flex h-full flex-col bg-muted/10 p-8">
      <div className="mx-auto flex h-full w-full max-w-6xl min-h-0 flex-col gap-6">
        <div className="rounded-2xl border bg-card p-6 shadow-sm">
          <div className="flex items-start justify-between gap-4">
            <div>
              <div className="flex items-center gap-2 text-lg font-semibold">
                <Settings2 className="size-5" />
                {view === "environments" ? "Environments" : "Connections"}
              </div>
              <p className="mt-2 text-sm text-muted-foreground">
                {view === "environments"
                  ? "Manage the environments available in this workspace and jump directly into the matching connection setup."
                  : "Inspect the configured connections for the selected environment while editing the current connection in the right-hand panel."}
              </p>
            </div>
            <div className="text-right text-xs text-muted-foreground">{configPath}</div>
          </div>

          <div className="mt-4 grid gap-3 sm:grid-cols-3">
            <SummaryCard label="Configured environments" value={String(environments.length)} />
            <SummaryCard
              label="Default environment"
              value={defaultEnvironment || "default"}
            />
            <SummaryCard
              label="Configured connections"
              value={String(
                environments.reduce(
                  (total, environment) => total + environment.connections.length,
                  0
                )
              )}
            />
          </div>

          <div className="mt-4 flex flex-wrap gap-2">
            {view === "environments" ? (
              <>
                <Button type="button" onClick={onCreateEnvironment}>
                  <Plus className="mr-2 size-4" />
                  New Environment
                </Button>
                <Button
                  type="button"
                  variant="outline"
                  onClick={onCloneEnvironment}
                  disabled={environments.length === 0}
                >
                  <CopyPlus className="mr-2 size-4" />
                  Clone Environment
                </Button>
              </>
            ) : (
              <Button
                type="button"
                onClick={onCreateConnection}
                disabled={!activeEnvironment}
              >
                <Plus className="mr-2 size-4" />
                New Connection
              </Button>
            )}
          </div>
        </div>

        {view === "environments" ? (
          <DataTableCard
            loading={loading}
            columns={["Environment", "Schema Prefix", "Connections", "Actions"]}
            emptyState="No environments are configured yet."
          >
            {environments.map((environment) => (
              <tr
                key={environment.name}
                className={rowClassName(
                  environment.name === selectedEnvironmentName
                )}
              >
                <td className="px-4 py-3 font-medium">
                  <div className="flex items-center gap-2">
                    {environment.name}
                    {environment.name === defaultEnvironment && (
                      <span className="rounded-full border border-emerald-500/30 bg-emerald-500/10 px-2 py-0.5 text-[10px] uppercase tracking-wide text-emerald-700 dark:text-emerald-300">
                        default
                      </span>
                    )}
                  </div>
                </td>
                <td className="px-4 py-3 text-muted-foreground">
                  {environment.schema_prefix || "none"}
                </td>
                <td className="px-4 py-3 text-muted-foreground">
                  {environment.connections.length}
                </td>
                <td className="px-4 py-3">
                  <div className="flex flex-wrap items-center gap-3">
                    <Button type="button" variant="outline" onClick={() => onSelectEnvironment(environment.name)}>
                      <PencilLine className="mr-2 size-4" />
                      Edit
                    </Button>
                    <Button asChild type="button" variant="link">
                      <Link
                        to="/settings/connections"
                        search={{ environment: environment.name }}
                      >
                        Open Connections
                      </Link>
                    </Button>
                  </div>
                </td>
              </tr>
            ))}
          </DataTableCard>
        ) : activeEnvironment ? (
          <DataTableCard
            loading={loading}
            columns={["Connection", "Type", "Environment", "Actions"]}
            emptyState={`No connections are configured for ${activeEnvironment.name}.`}
          >
            {activeEnvironment.connections.map((connection) => (
              <tr
                key={connection.name}
                className={rowClassName(
                  connection.name === selectedConnectionName
                )}
              >
                <td className="px-4 py-3 font-medium">{connection.name}</td>
                <td className="px-4 py-3 text-muted-foreground">
                  {connection.type}
                </td>
                <td className="px-4 py-3 text-muted-foreground">
                  {activeEnvironment.name}
                </td>
                <td className="px-4 py-3">
                  <Button type="button" variant="outline" onClick={() => onSelectConnection(connection.name)}>
                    <PencilLine className="mr-2 size-4" />
                    Edit
                  </Button>
                </td>
              </tr>
            ))}
          </DataTableCard>
        ) : (
          <EmptyState text="Create an environment first before configuring connections." />
        )}
      </div>
    </div>
  );
}

function SummaryCard({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-xl border bg-background/80 p-4">
      <div className="text-xs uppercase tracking-wide text-muted-foreground">{label}</div>
      <div className="mt-1 text-2xl font-semibold">{value}</div>
    </div>
  );
}

function DataTableCard({
  columns,
  children,
  emptyState,
  loading,
}: {
  columns: string[];
  children: ReactNode;
  emptyState: string;
  loading: boolean;
}) {
  const rows = Array.isArray(children) ? children.filter(Boolean) : children ? [children] : [];

  return (
    <div className="min-h-0 flex-1 overflow-hidden rounded-2xl border bg-card shadow-sm">
      <div className="h-full overflow-auto">
        <table className="w-full min-w-[720px] border-collapse text-sm">
          <thead className="sticky top-0 z-10 bg-card/95 backdrop-blur">
            <tr className="border-b">
              {columns.map((column) => (
                <th
                  key={column}
                  className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wide text-muted-foreground"
                >
                  {column}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {loading ? (
              <LoadingRows columnCount={columns.length} />
            ) : rows.length > 0 ? (
              children
            ) : (
              <tr>
                <td className="px-4 py-8 text-muted-foreground" colSpan={columns.length}>
                  {emptyState}
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function LoadingRows({ columnCount }: { columnCount: number }) {
  return (
    <>
      {Array.from({ length: 5 }, (_, rowIndex) => (
        <tr key={rowIndex} className="border-b last:border-b-0">
          {Array.from({ length: columnCount }, (_, columnIndex) => (
            <td key={columnIndex} className="px-4 py-3">
              <Skeleton className="h-5 w-full max-w-[12rem]" />
            </td>
          ))}
        </tr>
      ))}
    </>
  );
}

function rowClassName(isSelected: boolean) {
  return isSelected ? "border-b bg-primary/5 last:border-b-0" : "border-b last:border-b-0";
}

function EmptyState({ text }: { text: string }) {
  return (
    <div className="rounded-2xl border border-dashed bg-card/60 p-6 text-sm text-muted-foreground">
      {text}
    </div>
  );
}