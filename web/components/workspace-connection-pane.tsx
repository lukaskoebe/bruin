"use client";

import { CSSProperties, useState } from "react";
import { CheckCircle2, LoaderCircle, Save, Trash2 } from "lucide-react";

import { WorkspaceConfigPaneLayout } from "@/components/workspace-config-pane-layout";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { testWorkspaceConnection } from "@/lib/api";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Switch } from "@/components/ui/switch";
import {
  ConnectionMode,
  useWorkspaceConnectionForm,
} from "@/hooks/use-workspace-connection-form";
import {
  WorkspaceConfigConnectionType,
  WorkspaceConfigEnvironment,
  WorkspaceConfigResponse,
} from "@/lib/types";

type WorkspaceConnectionPaneProps = {
  configPath: string;
  defaultEnvironment?: string;
  selectedEnvironment?: string | null;
  selectedConnectionName?: string | null;
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
  mode: ConnectionMode;
  requestedConnectionType?: string;
  onModeChange: (mode: ConnectionMode) => void;
  onSelectedEnvironmentChange: (name: string | null) => void;
  onSelectedConnectionChange: (name: string | null) => void;
  onReload: () => void;
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

export function WorkspaceConnectionPane({
  configPath,
  defaultEnvironment,
  selectedEnvironment,
  selectedConnectionName,
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
  mode,
  requestedConnectionType,
  onModeChange,
  onSelectedEnvironmentChange,
  onSelectedConnectionChange,
  onReload,
  onCreateConnection,
  onUpdateConnection,
  onDeleteConnection,
}: WorkspaceConnectionPaneProps) {
  const {
    activeConnection,
    connectionForm,
    selectedConnectionType,
    setConnectionForm,
    handleDelete,
    handleSave,
  } = useWorkspaceConnectionForm({
    connectionTypes,
    defaultEnvironment,
    environments,
    mode,
    onCreateConnection,
    onDeleteConnection,
    onModeChange,
    onSelectedConnectionChange,
    onSelectedEnvironmentChange,
    onUpdateConnection,
    selectedConnectionName,
    selectedEnvironmentName: selectedEnvironment,
    requestedConnectionType,
  });
  const [validateBusy, setValidateBusy] = useState(false);
  const [validateMessage, setValidateMessage] = useState<string | null>(null);
  const [validateTone, setValidateTone] = useState<"error" | "success" | null>(null);

  const canValidate =
    Boolean(connectionForm.environmentName) && Boolean(connectionForm.name.trim());

  const handleValidate = async () => {
    if (!canValidate) {
      return;
    }

    setValidateBusy(true);
    setValidateMessage(null);
    setValidateTone(null);
    try {
      const response = await testWorkspaceConnection({
        environment_name: connectionForm.environmentName,
        name: connectionForm.name.trim(),
      });
      setValidateMessage(response.message ?? "Connection validated.");
      setValidateTone("success");
    } catch (error) {
      setValidateMessage(
        error instanceof Error ? error.message : "Connection validation failed."
      );
      setValidateTone("error");
    } finally {
      setValidateBusy(false);
    }
  };

  return (
    <WorkspaceConfigPaneLayout
      title="Connection Editor"
      configPath={configPath}
      loading={loading}
      busy={busy}
      parseError={parseError}
      helpMode={helpMode}
      highlighted={highlighted}
      highlightStyle={highlightStyle}
      statusMessage={statusMessage}
      statusTone={statusTone}
      onReload={onReload}
    >
      <div className="grid gap-3">
        <div className="grid gap-1">
          <Label>Environment</Label>
          <Select
            value={connectionForm.environmentName || selectedEnvironment || undefined}
            onValueChange={(value) => {
              onSelectedEnvironmentChange(value);
              onModeChange("edit");
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
              {environments.map((environment) => (
                <SelectItem key={environment.name} value={environment.name}>
                  {environment.name}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>

        <div className="rounded-lg border bg-card/60 p-3 sm:p-4">
          <div className="mb-3 flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
            <div className="font-medium">
              {mode === "create" ? "Create Connection" : "Edit Connection"}
            </div>
            {mode === "edit" && activeConnection && (
              <Button
                size="sm"
                type="button"
                variant="destructive"
                onClick={() => void handleDelete()}
                disabled={busy}
                className="w-full sm:w-auto"
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
                    values: buildTypeValues({
                      activeConnection,
                      connectionTypes,
                      mode,
                      previousValues: current.values,
                      typeName: value,
                    }),
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
                  <div key={field.name} className="flex flex-col gap-3 rounded-md border bg-background/70 px-3 py-3 text-sm sm:flex-row sm:items-center sm:justify-between">
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
                          [field.name]: field.type === "int" ? event.target.value : event.target.value,
                        },
                      }))
                    }
                    placeholder={field.default_value || (field.is_required ? "Required" : "Optional")}
                  />
                </div>
              );
            })}

            {validateMessage ? (
              <div
                className={`rounded-md border px-3 py-2 text-sm ${
                  validateTone === "error"
                    ? "border-destructive/40 bg-destructive/10 text-destructive"
                    : "border-emerald-500/40 bg-emerald-500/10 text-emerald-700 dark:text-emerald-300"
                }`}
              >
                <div className="font-medium">
                  {validateTone === "error"
                    ? "Connection validation failed"
                    : "Connection validation succeeded"}
                </div>
                <div className="mt-1 whitespace-pre-wrap text-xs sm:text-sm">
                  {validateMessage}
                </div>
              </div>
            ) : null}

            <div className="flex flex-col gap-2 sm:flex-row">
              <Button
                type="button"
                variant="outline"
                className="w-full sm:w-auto"
                onClick={() => void handleValidate()}
                disabled={busy || validateBusy || !canValidate}
              >
                {validateBusy ? (
                  <LoaderCircle className="mr-1 inline size-3 animate-spin" />
                ) : (
                  <CheckCircle2 className="mr-1 inline size-3" />
                )}
                Validate Connection
              </Button>
              <Button
                className="w-full sm:w-auto"
                type="button"
                onClick={() => void handleSave()}
                disabled={
                  busy || !connectionForm.environmentName || !connectionForm.name.trim() || !connectionForm.type
                }
              >
                <Save className="mr-1 inline size-3" />
                {mode === "create" ? "Create Connection" : "Save Connection"}
              </Button>
            </div>
          </div>
        </div>
      </div>
    </WorkspaceConfigPaneLayout>
  );
}

function buildTypeValues({
  activeConnection,
  connectionTypes,
  mode,
  previousValues,
  typeName,
}: {
  activeConnection: { type: string } | null;
  connectionTypes: WorkspaceConfigConnectionType[];
  mode: ConnectionMode;
  previousValues: Record<string, string | number | boolean>;
  typeName: string;
}) {
  const connectionType = connectionTypes.find(
    (candidate) => candidate.type_name === typeName
  );
  const values: Record<string, string | number | boolean> = {};

  for (const field of connectionType?.fields ?? []) {
    const previousValue = previousValues[field.name];
    if (mode === "edit" && activeConnection?.type === typeName && previousValue !== undefined) {
      values[field.name] = previousValue;
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
