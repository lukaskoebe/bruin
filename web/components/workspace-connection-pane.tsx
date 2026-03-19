"use client";

import { CSSProperties } from "react";
import { Save, Trash2 } from "lucide-react";

import { WorkspaceConfigPaneLayout } from "@/components/workspace-config-pane-layout";
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
  });

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

        <div className="rounded-lg border bg-card/60 p-3">
          <div className="mb-3 flex items-center justify-between gap-2">
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
                          [field.name]: field.type === "int" ? event.target.value : event.target.value,
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
