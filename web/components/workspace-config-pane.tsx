"use client";

import {
  AlertCircle,
  CopyPlus,
  Plus,
  RefreshCcw,
  Save,
  Trash2,
} from "lucide-react";
import { CSSProperties, useEffect, useMemo, useState } from "react";
import { Panel } from "react-resizable-panels";

import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Switch } from "@/components/ui/switch";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import {
  WorkspaceConfigConnection,
  WorkspaceConfigConnectionType,
  WorkspaceConfigEnvironment,
  WorkspaceConfigResponse,
} from "@/lib/types";

type WorkspaceConfigPaneProps = {
  configPath: string;
  defaultEnvironment?: string;
  selectedEnvironment?: string;
  environments: WorkspaceConfigEnvironment[];
  connectionTypes: WorkspaceConfigConnectionType[];
  loading: boolean;
  busy: boolean;
  parseError?: string;
  helpMode?: boolean;
  highlighted?: boolean;
  highlightStyle?: CSSProperties;
  statusMessage?: string | null;
  statusTone?: "error" | "success" | null;
  onReload: () => void;
  onCreateEnvironment: (input: {
    name: string;
    schema_prefix?: string;
    set_as_default?: boolean;
  }) => Promise<WorkspaceConfigResponse>;
  onUpdateEnvironment: (input: {
    name: string;
    new_name?: string;
    schema_prefix?: string;
    set_as_default?: boolean;
  }) => Promise<WorkspaceConfigResponse>;
  onCloneEnvironment: (input: {
    source_name: string;
    target_name: string;
    schema_prefix?: string;
    set_as_default?: boolean;
  }) => Promise<WorkspaceConfigResponse>;
  onDeleteEnvironment: (name: string) => Promise<WorkspaceConfigResponse>;
  onCreateConnection: (input: {
    environment_name: string;
    name: string;
    type: string;
    values: Record<string, unknown>;
  }) => Promise<WorkspaceConfigResponse>;
  onUpdateConnection: (input: {
    environment_name: string;
    current_name?: string;
    name: string;
    type: string;
    values: Record<string, unknown>;
  }) => Promise<WorkspaceConfigResponse>;
  onDeleteConnection: (input: {
    environment_name: string;
    name: string;
  }) => Promise<WorkspaceConfigResponse>;
};

type EnvironmentMode = "edit" | "create" | "clone";
type ConnectionMode = "edit" | "create";

export function WorkspaceConfigPane({
  configPath,
  defaultEnvironment,
  selectedEnvironment,
  environments,
  connectionTypes,
  loading,
  busy,
  parseError,
  helpMode = false,
  highlighted = false,
  highlightStyle,
  statusMessage,
  statusTone,
  onReload,
  onCreateEnvironment,
  onUpdateEnvironment,
  onCloneEnvironment,
  onDeleteEnvironment,
  onCreateConnection,
  onUpdateConnection,
  onDeleteConnection,
}: WorkspaceConfigPaneProps) {
  const [activeTab, setActiveTab] = useState<"environments" | "connections">("environments");
  const [environmentMode, setEnvironmentMode] = useState<EnvironmentMode>("edit");
  const [connectionMode, setConnectionMode] = useState<ConnectionMode>("edit");
  const [selectedEnvironmentName, setSelectedEnvironmentName] = useState<string | null>(null);
  const [selectedConnectionName, setSelectedConnectionName] = useState<string | null>(null);
  const [environmentForm, setEnvironmentForm] = useState({
    name: "",
    schemaPrefix: "",
    setAsDefault: false,
    cloneSourceName: "",
  });
  const [connectionForm, setConnectionForm] = useState({
    environmentName: "",
    name: "",
    type: "",
    values: {} as Record<string, string | number | boolean>,
  });

  const normalizedEnvironments = useMemo(
    () => [...environments].sort((left, right) => left.name.localeCompare(right.name)),
    [environments]
  );

  const activeEnvironment = useMemo(
    () => normalizedEnvironments.find((environment) => environment.name === selectedEnvironmentName) ?? null,
    [normalizedEnvironments, selectedEnvironmentName]
  );

  const activeConnection = useMemo(
    () => activeEnvironment?.connections.find((connection) => connection.name === selectedConnectionName) ?? null,
    [activeEnvironment, selectedConnectionName]
  );

  const selectedConnectionType = useMemo(
    () => connectionTypes.find((connectionType) => connectionType.type_name === connectionForm.type) ?? null,
    [connectionForm.type, connectionTypes]
  );

  useEffect(() => {
    if (normalizedEnvironments.length === 0) {
      setSelectedEnvironmentName(null);
      return;
    }

    if (
      selectedEnvironmentName &&
      normalizedEnvironments.some((environment) => environment.name === selectedEnvironmentName)
    ) {
      return;
    }

    setSelectedEnvironmentName(selectedEnvironment || defaultEnvironment || normalizedEnvironments[0]?.name || null);
  }, [defaultEnvironment, normalizedEnvironments, selectedEnvironment, selectedEnvironmentName]);

  useEffect(() => {
    if (!activeEnvironment) {
      setSelectedConnectionName(null);
      return;
    }

    if (
      selectedConnectionName &&
      activeEnvironment.connections.some((connection) => connection.name === selectedConnectionName)
    ) {
      return;
    }

    setSelectedConnectionName(activeEnvironment.connections[0]?.name ?? null);
  }, [activeEnvironment, selectedConnectionName]);

  useEffect(() => {
    if (environmentMode === "create") {
      setEnvironmentForm({
        name: "",
        schemaPrefix: "",
        setAsDefault: normalizedEnvironments.length === 0,
        cloneSourceName: selectedEnvironmentName ?? "",
      });
      return;
    }

    if (environmentMode === "clone") {
      const sourceName = selectedEnvironmentName ?? normalizedEnvironments[0]?.name ?? "";
      const sourceEnvironment = normalizedEnvironments.find((environment) => environment.name === sourceName);
      setEnvironmentForm({
        name: "",
        schemaPrefix: sourceEnvironment?.schema_prefix ?? "",
        setAsDefault: false,
        cloneSourceName: sourceName,
      });
      return;
    }

    if (!activeEnvironment) {
      setEnvironmentForm({
        name: "",
        schemaPrefix: "",
        setAsDefault: false,
        cloneSourceName: "",
      });
      return;
    }

    setEnvironmentForm({
      name: activeEnvironment.name,
      schemaPrefix: activeEnvironment.schema_prefix ?? "",
      setAsDefault: activeEnvironment.name === defaultEnvironment,
      cloneSourceName: activeEnvironment.name,
    });
  }, [activeEnvironment, defaultEnvironment, environmentMode, normalizedEnvironments, selectedEnvironmentName]);

  useEffect(() => {
    if (connectionMode === "create") {
      const fallbackType = connectionTypes[0]?.type_name ?? "";
      setConnectionForm({
        environmentName: selectedEnvironmentName ?? defaultEnvironment ?? normalizedEnvironments[0]?.name ?? "",
        name: "",
        type: fallbackType,
        values: buildInitialConnectionValues(connectionTypes, fallbackType, null),
      });
      return;
    }

    if (!activeConnection || !activeEnvironment) {
      setConnectionForm({
        environmentName: selectedEnvironmentName ?? "",
        name: "",
        type: connectionTypes[0]?.type_name ?? "",
        values: buildInitialConnectionValues(connectionTypes, connectionTypes[0]?.type_name ?? "", null),
      });
      return;
    }

    setConnectionForm({
      environmentName: activeEnvironment.name,
      name: activeConnection.name,
      type: activeConnection.type,
      values: buildInitialConnectionValues(connectionTypes, activeConnection.type, activeConnection),
    });
  }, [activeConnection, activeEnvironment, connectionMode, connectionTypes, defaultEnvironment, normalizedEnvironments, selectedEnvironmentName]);

  const handleEnvironmentSave = async () => {
    if (environmentMode === "create") {
      const response = await onCreateEnvironment({
        name: environmentForm.name.trim(),
        schema_prefix: environmentForm.schemaPrefix.trim(),
        set_as_default: environmentForm.setAsDefault,
      });
      setEnvironmentMode("edit");
      setSelectedEnvironmentName(environmentForm.name.trim() || response.default_environment || response.environments[0]?.name || null);
      return;
    }

    if (environmentMode === "clone") {
      const response = await onCloneEnvironment({
        source_name: environmentForm.cloneSourceName,
        target_name: environmentForm.name.trim(),
        schema_prefix: environmentForm.schemaPrefix.trim(),
        set_as_default: environmentForm.setAsDefault,
      });
      setEnvironmentMode("edit");
      setSelectedEnvironmentName(environmentForm.name.trim() || response.default_environment || response.environments[0]?.name || null);
      return;
    }

    if (!activeEnvironment) {
      return;
    }

    const response = await onUpdateEnvironment({
      name: activeEnvironment.name,
      new_name: environmentForm.name.trim(),
      schema_prefix: environmentForm.schemaPrefix.trim(),
      set_as_default: environmentForm.setAsDefault,
    });
    setSelectedEnvironmentName(environmentForm.name.trim() || response.default_environment || activeEnvironment.name);
  };

  const handleEnvironmentDelete = async () => {
    if (!activeEnvironment) {
      return;
    }

    const response = await onDeleteEnvironment(activeEnvironment.name);
    setEnvironmentMode("edit");
    setSelectedEnvironmentName(response.default_environment || response.environments[0]?.name || null);
  };

  const handleConnectionSave = async () => {
    const payload = {
      environment_name: connectionForm.environmentName,
      name: connectionForm.name.trim(),
      type: connectionForm.type,
      values: connectionForm.values,
    };

    if (connectionMode === "create") {
      const response = await onCreateConnection(payload);
      const environment = response.environments.find(
        (candidate) => candidate.name === connectionForm.environmentName
      );
      setConnectionMode("edit");
      setSelectedEnvironmentName(connectionForm.environmentName);
      setSelectedConnectionName(connectionForm.name.trim() || environment?.connections[0]?.name || null);
      setActiveTab("connections");
      return;
    }

    const response = await onUpdateConnection({
      ...payload,
      current_name: activeConnection?.name,
    });
    const environment = response.environments.find(
      (candidate) => candidate.name === connectionForm.environmentName
    );
    setSelectedConnectionName(connectionForm.name.trim() || environment?.connections[0]?.name || null);
  };

  const handleConnectionDelete = async () => {
    if (!activeConnection || !activeEnvironment) {
      return;
    }

    const response = await onDeleteConnection({
      environment_name: activeEnvironment.name,
      name: activeConnection.name,
    });
    const environment = response.environments.find(
      (candidate) => candidate.name === activeEnvironment.name
    );
    setSelectedConnectionName(environment?.connections[0]?.name ?? null);
  };

  return (
    <Panel defaultSize={32} minSize={24}>
      <div className="flex h-full min-h-0 min-w-0 flex-col border-l bg-background">
        <div
          className={`border-b px-4 py-3 ${
            helpMode && highlighted ? "ring-2 ring-primary/70 ring-inset" : ""
          }`}
          style={helpMode && highlighted ? highlightStyle : undefined}
        >
          <div className="mb-2 text-sm font-semibold">Connections & Environments</div>
          <div className="text-xs opacity-70">{configPath}</div>
          <div className="mt-2 flex gap-2">
            <Button size="sm" type="button" variant="outline" disabled={loading || busy} onClick={onReload}>
              <RefreshCcw className="mr-1 inline size-3" />
              Reload
            </Button>
          </div>
          {statusMessage && (
            <div
              className={`mt-2 rounded-md border px-2 py-1 text-xs ${
                statusTone === "error"
                  ? "border-destructive/40 bg-destructive/10 text-destructive"
                  : "border-emerald-500/40 bg-emerald-500/10 text-emerald-700 dark:text-emerald-300"
              }`}
            >
              {statusMessage}
            </div>
          )}
          {parseError && (
            <div className="mt-2 rounded-md border border-amber-500/40 bg-amber-500/10 px-2 py-1 text-xs text-amber-800 dark:text-amber-300">
              <div className="flex items-center gap-1 font-medium">
                <AlertCircle className="size-3" />
                Workspace config could not be parsed
              </div>
              <div className="mt-1 whitespace-pre-wrap">{parseError}</div>
            </div>
          )}
        </div>

        <div className="flex min-h-0 min-w-0 flex-1 flex-col overflow-hidden p-4">
          <Tabs
            className="flex min-h-0 min-w-0 flex-1 flex-col"
            value={activeTab}
            onValueChange={(value) => setActiveTab(value as "environments" | "connections")}
          >
            <TabsList className="grid w-full grid-cols-2">
              <TabsTrigger value="environments">Environments</TabsTrigger>
              <TabsTrigger value="connections">Connections</TabsTrigger>
            </TabsList>

            <TabsContent className="mt-3 min-h-0 flex-1 overflow-auto" value="environments">
              <div className="grid gap-3">
                <div className="grid grid-cols-2 gap-3 text-sm">
                  <SummaryCard label="Default environment" value={defaultEnvironment || "default"} />
                  <SummaryCard label="Configured environments" value={String(environments.length)} />
                </div>

                <div className="flex gap-2">
                  <Button size="sm" type="button" variant="outline" onClick={() => setEnvironmentMode("create")} disabled={busy}>
                    <Plus className="mr-1 inline size-3" />
                    New Environment
                  </Button>
                  <Button
                    size="sm"
                    type="button"
                    variant="outline"
                    onClick={() => setEnvironmentMode("clone")}
                    disabled={busy || environments.length === 0}
                  >
                    <CopyPlus className="mr-1 inline size-3" />
                    Clone
                  </Button>
                </div>

                <SelectionList
                  emptyMessage="No environments configured yet."
                  items={normalizedEnvironments.map((environment) => ({
                    id: environment.name,
                    title: environment.name,
                    subtitle: environment.schema_prefix || "No schema prefix",
                    active: environment.name === selectedEnvironmentName,
                  }))}
                  onSelect={(id) => {
                    setSelectedEnvironmentName(id);
                    setEnvironmentMode("edit");
                  }}
                />

                <div className="rounded-lg border bg-card/60 p-3">
                  <div className="mb-3 flex items-center justify-between gap-2">
                    <div className="font-medium">
                      {environmentMode === "create"
                        ? "Create Environment"
                        : environmentMode === "clone"
                          ? "Clone Environment"
                          : "Edit Environment"}
                    </div>
                    {environmentMode === "edit" && activeEnvironment && (
                      <Button
                        size="sm"
                        type="button"
                        variant="destructive"
                        onClick={() => void handleEnvironmentDelete()}
                        disabled={busy}
                      >
                        <Trash2 className="mr-1 inline size-3" />
                        Delete
                      </Button>
                    )}
                  </div>

                  <div className="grid gap-3">
                    {environmentMode === "clone" && (
                      <div className="grid gap-1">
                        <Label>Source environment</Label>
                        <Select
                          value={environmentForm.cloneSourceName}
                          onValueChange={(value) =>
                            setEnvironmentForm((current) => {
                              const sourceEnvironment = normalizedEnvironments.find(
                                (environment) => environment.name === value
                              );
                              return {
                                ...current,
                                cloneSourceName: value,
                                schemaPrefix: sourceEnvironment?.schema_prefix ?? current.schemaPrefix,
                              };
                            })
                          }
                        >
                          <SelectTrigger className="w-full">
                            <SelectValue placeholder="Select source environment" />
                          </SelectTrigger>
                          <SelectContent>
                            {normalizedEnvironments.map((environment) => (
                              <SelectItem key={environment.name} value={environment.name}>
                                {environment.name}
                              </SelectItem>
                            ))}
                          </SelectContent>
                        </Select>
                      </div>
                    )}

                    <div className="grid gap-1">
                      <Label>Name</Label>
                      <Input
                        value={environmentForm.name}
                        onChange={(event) =>
                          setEnvironmentForm((current) => ({
                            ...current,
                            name: event.target.value,
                          }))
                        }
                        placeholder="staging"
                      />
                    </div>
                    <div className="grid gap-1">
                      <Label>Schema prefix</Label>
                      <Input
                        value={environmentForm.schemaPrefix}
                        onChange={(event) =>
                          setEnvironmentForm((current) => ({
                            ...current,
                            schemaPrefix: event.target.value,
                          }))
                        }
                        placeholder="staging_"
                      />
                    </div>
                    <div className="flex items-center justify-between rounded-md border bg-background/70 px-3 py-2 text-sm">
                      <div>
                        <div className="font-medium">Use as default environment</div>
                        <div className="text-xs text-muted-foreground">
                          This becomes the active environment across the workspace.
                        </div>
                      </div>
                      <Switch
                        checked={environmentForm.setAsDefault}
                        onCheckedChange={(checked) =>
                          setEnvironmentForm((current) => ({
                            ...current,
                            setAsDefault: checked,
                          }))
                        }
                      />
                    </div>
                    <Button type="button" onClick={() => void handleEnvironmentSave()} disabled={busy || !environmentForm.name.trim()}>
                      <Save className="mr-1 inline size-3" />
                      {environmentMode === "create"
                        ? "Create Environment"
                        : environmentMode === "clone"
                          ? "Clone Environment"
                          : "Save Environment"}
                    </Button>
                  </div>
                </div>
              </div>
            </TabsContent>

            <TabsContent className="mt-3 min-h-0 flex-1 overflow-auto" value="connections">
              <div className="grid gap-3">
                <div className="grid grid-cols-2 gap-3 text-sm">
                  <SummaryCard label="Active environment" value={selectedEnvironment || defaultEnvironment || "default"} />
                  <SummaryCard
                    label="Connections in selected environment"
                    value={String(activeEnvironment?.connections.length ?? 0)}
                  />
                </div>

                <div className="grid gap-1">
                  <Label>Environment</Label>
                  <Select
                    value={connectionForm.environmentName || selectedEnvironmentName || undefined}
                    onValueChange={(value) => {
                      setSelectedEnvironmentName(value);
                      setConnectionMode("edit");
                      setConnectionForm((current) => ({
                        ...current,
                        environmentName: value,
                      }));
                    }}
                  >
                    <SelectTrigger className="w-full">
                      <SelectValue placeholder="Select environment" />
                    </SelectTrigger>
                    <SelectContent>
                      {normalizedEnvironments.map((environment) => (
                        <SelectItem key={environment.name} value={environment.name}>
                          {environment.name}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>

                <div className="flex gap-2">
                  <Button size="sm" type="button" variant="outline" onClick={() => setConnectionMode("create")} disabled={busy || normalizedEnvironments.length === 0}>
                    <Plus className="mr-1 inline size-3" />
                    New Connection
                  </Button>
                </div>

                <SelectionList
                  emptyMessage="No connections in this environment yet."
                  items={(activeEnvironment?.connections ?? []).map((connection) => ({
                    id: connection.name,
                    title: connection.name,
                    subtitle: connection.type,
                    active: connection.name === selectedConnectionName,
                  }))}
                  onSelect={(id) => {
                    setSelectedConnectionName(id);
                    setConnectionMode("edit");
                  }}
                />

                <div className="rounded-lg border bg-card/60 p-3">
                  <div className="mb-3 flex items-center justify-between gap-2">
                    <div className="font-medium">
                      {connectionMode === "create" ? "Create Connection" : "Edit Connection"}
                    </div>
                    {connectionMode === "edit" && activeConnection && (
                      <Button
                        size="sm"
                        type="button"
                        variant="destructive"
                        onClick={() => void handleConnectionDelete()}
                        disabled={busy}
                      >
                        <Trash2 className="mr-1 inline size-3" />
                        Delete
                      </Button>
                    )}
                  </div>

                  <div className="grid gap-3">
                    <div className="grid gap-1">
                      <Label>Name</Label>
                      <Input
                        value={connectionForm.name}
                        onChange={(event) =>
                          setConnectionForm((current) => ({
                            ...current,
                            name: event.target.value,
                          }))
                        }
                        placeholder="MY_CONNECTION"
                      />
                    </div>
                    <div className="grid gap-1">
                      <Label>Type</Label>
                      <Select
                        value={connectionForm.type || undefined}
                        onValueChange={(value) =>
                          setConnectionForm((current) => ({
                            ...current,
                            type: value,
                            values: buildInitialConnectionValues(
                              connectionTypes,
                              value,
                              connectionMode === "edit" ? activeConnection : null,
                              current.values
                            ),
                          }))
                        }
                      >
                        <SelectTrigger className="w-full">
                          <SelectValue placeholder="Select connection type" />
                        </SelectTrigger>
                        <SelectContent>
                          {connectionTypes.map((connectionType) => (
                            <SelectItem key={connectionType.type_name} value={connectionType.type_name}>
                              {connectionType.type_name}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>

                    {selectedConnectionType?.fields.map((field) => {
                      const fieldValue = connectionForm.values[field.name];
                      if (field.type === "bool") {
                        return (
                          <div key={field.name} className="flex items-center justify-between rounded-md border bg-background/70 px-3 py-2 text-sm">
                            <div>
                              <div className="font-medium">{field.name}</div>
                              <div className="text-xs text-muted-foreground">
                                {field.is_required ? "Required boolean field" : "Optional boolean field"}
                              </div>
                            </div>
                            <Switch
                              checked={Boolean(fieldValue)}
                              onCheckedChange={(checked) =>
                                setConnectionForm((current) => ({
                                  ...current,
                                  values: {
                                    ...current.values,
                                    [field.name]: checked,
                                  },
                                }))
                              }
                            />
                          </div>
                        );
                      }

                      return (
                        <div key={field.name} className="grid gap-1">
                          <Label>{field.name}</Label>
                          <Input
                            type={field.type === "int" ? "number" : secretInputType(field.name)}
                            value={fieldValue === undefined || fieldValue === null ? "" : String(fieldValue)}
                            onChange={(event) =>
                              setConnectionForm((current) => ({
                                ...current,
                                values: {
                                  ...current.values,
                                  [field.name]: field.type === "int"
                                    ? event.target.value
                                    : event.target.value,
                                },
                              }))
                            }
                            placeholder={field.default_value || (field.is_required ? "Required" : "Optional")}
                          />
                        </div>
                      );
                    })}

                    <Button
                      type="button"
                      onClick={() => void handleConnectionSave()}
                      disabled={busy || !connectionForm.environmentName || !connectionForm.name.trim() || !connectionForm.type}
                    >
                      <Save className="mr-1 inline size-3" />
                      {connectionMode === "create" ? "Create Connection" : "Save Connection"}
                    </Button>
                  </div>
                </div>
              </div>
            </TabsContent>
          </Tabs>
        </div>
      </div>
    </Panel>
  );
}

function SummaryCard({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-lg border bg-card/60 p-3">
      <div className="text-xs text-muted-foreground">{label}</div>
      <div className="mt-1 text-sm font-medium">{value}</div>
    </div>
  );
}

function SelectionList({
  items,
  emptyMessage,
  onSelect,
}: {
  items: Array<{
    id: string;
    title: string;
    subtitle?: string;
    active?: boolean;
  }>;
  emptyMessage: string;
  onSelect: (id: string) => void;
}) {
  if (items.length === 0) {
    return (
      <div className="rounded-md border border-dashed bg-muted/20 px-3 py-4 text-sm text-muted-foreground">
        {emptyMessage}
      </div>
    );
  }

  return (
    <div className="grid gap-2">
      {items.map((item) => (
        <button
          key={item.id}
          className={`rounded-md border px-3 py-2 text-left transition-colors ${
            item.active
              ? "border-primary/50 bg-primary/10"
              : "bg-background/70 hover:bg-muted/40"
          }`}
          onClick={() => onSelect(item.id)}
          type="button"
        >
          <div className="font-medium">{item.title}</div>
          {item.subtitle && (
            <div className="text-xs uppercase tracking-wide text-muted-foreground">
              {item.subtitle}
            </div>
          )}
        </button>
      ))}
    </div>
  );
}

function buildInitialConnectionValues(
  connectionTypes: WorkspaceConfigConnectionType[],
  typeName: string,
  existingConnection: WorkspaceConfigConnection | null,
  previousValues?: Record<string, string | number | boolean>
) {
  const connectionType = connectionTypes.find(
    (candidate) => candidate.type_name === typeName
  );
  const values: Record<string, string | number | boolean> = {};

  for (const field of connectionType?.fields ?? []) {
    const existingValue = existingConnection?.values[field.name];
    const previousValue = previousValues?.[field.name];
    if (existingValue !== undefined && existingValue !== null) {
      values[field.name] = existingValue as string | number | boolean;
      continue;
    }
    if (previousValue !== undefined) {
      values[field.name] = previousValue;
      continue;
    }
    if (field.type === "bool") {
      values[field.name] = field.default_value === "true";
      continue;
    }
    if (field.type === "int") {
      values[field.name] = field.default_value ? Number(field.default_value) : "";
      continue;
    }
    values[field.name] = field.default_value ?? "";
  }

  return values;
}

function secretInputType(name: string) {
  return isSecretField(name) ? "password" : "text";
}

function isSecretField(name: string) {
  const lower = name.toLowerCase();
  return ["password", "secret", "token", "api_key", "private_key", "access_key"].some((part) =>
    lower.includes(part)
  );
}
