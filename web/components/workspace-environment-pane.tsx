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
  EnvironmentMode,
  useWorkspaceEnvironmentForm,
} from "@/hooks/use-workspace-environment-form";
import {
  WorkspaceConfigEnvironment,
  WorkspaceConfigResponse,
} from "@/lib/types";

type WorkspaceEnvironmentPaneProps = {
  configPath: string;
  defaultEnvironment?: string;
  selectedEnvironment?: string | null;
  environments: WorkspaceConfigEnvironment[];
  loading: boolean;
  busy: boolean;
  parseError?: string;
  helpMode?: boolean;
  highlighted?: boolean;
  highlightStyle?: CSSProperties;
  statusMessage?: string | null;
  statusTone?: "error" | "success" | null;
  mode: EnvironmentMode;
  onModeChange: (mode: EnvironmentMode) => void;
  onSelectedEnvironmentChange: (name: string | null) => void;
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
};

export function WorkspaceEnvironmentPane({
  configPath,
  defaultEnvironment,
  selectedEnvironment,
  environments,
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
  onReload,
  onCreateEnvironment,
  onUpdateEnvironment,
  onCloneEnvironment,
  onDeleteEnvironment,
}: WorkspaceEnvironmentPaneProps) {
  const { activeEnvironment, environmentForm, setEnvironmentForm, handleDelete, handleSave } =
    useWorkspaceEnvironmentForm({
      defaultEnvironment,
      environments,
      mode,
      onCloneEnvironment,
      onCreateEnvironment,
      onDeleteEnvironment,
      onModeChange,
      onSelectedEnvironmentChange,
      onUpdateEnvironment,
      selectedEnvironmentName: selectedEnvironment,
    });

  return (
    <WorkspaceConfigPaneLayout
      title="Environment Editor"
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
      <div className="rounded-lg border bg-card/60 p-3 sm:p-4">
        <div className="mb-3 flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
          <div className="font-medium">
            {mode === "create"
              ? "Create Environment"
              : mode === "clone"
                ? "Clone Environment"
                : "Edit Environment"}
          </div>
          {mode === "edit" && activeEnvironment && (
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
          {mode === "clone" && (
            <div className="grid gap-1">
              <Label>Source environment</Label>
              <Select
                value={environmentForm.cloneSourceName}
                onValueChange={(value) =>
                  setEnvironmentForm((current) => {
                    const sourceEnvironment = environments.find(
                      (environment) => environment.name === value
                    );
                    return {
                      ...current,
                      cloneSourceName: value,
                      schemaPrefix:
                        sourceEnvironment?.schema_prefix ?? current.schemaPrefix,
                    };
                  })
                }
              >
                <SelectTrigger className="w-full">
                  <SelectValue placeholder="Select source environment" />
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
          <div className="flex flex-col gap-3 rounded-md border bg-background/70 px-3 py-3 text-sm sm:flex-row sm:items-center sm:justify-between">
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
          <Button className="w-full sm:w-auto" type="button" onClick={() => void handleSave()} disabled={busy || !environmentForm.name.trim()}>
            <Save className="mr-1 inline size-3" />
            {mode === "create"
              ? "Create Environment"
              : mode === "clone"
                ? "Clone Environment"
                : "Save Environment"}
          </Button>
        </div>
      </div>
    </WorkspaceConfigPaneLayout>
  );
}
