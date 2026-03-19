"use client";

import { Dispatch, SetStateAction, useEffect, useMemo, useState } from "react";

import {
  WorkspaceConfigConnection,
  WorkspaceConfigConnectionType,
  WorkspaceConfigEnvironment,
  WorkspaceConfigResponse,
} from "@/lib/types";

export type ConnectionMode = "edit" | "create";

type ConnectionFormState = {
  environmentName: string;
  name: string;
  type: string;
  values: Record<string, string | number | boolean>;
};

export function useWorkspaceConnectionForm({
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
  selectedEnvironmentName,
}: {
  connectionTypes: WorkspaceConfigConnectionType[];
  defaultEnvironment?: string;
  environments: WorkspaceConfigEnvironment[];
  mode: ConnectionMode;
  onCreateConnection: (input: {
    environment_name: string;
    name: string;
    type: string;
    values: Record<string, unknown>;
  }) => Promise<WorkspaceConfigResponse>;
  onDeleteConnection: (input: {
    environment_name: string;
    name: string;
  }) => Promise<WorkspaceConfigResponse>;
  onModeChange: (mode: ConnectionMode) => void;
  onSelectedConnectionChange: (name: string | null) => void;
  onSelectedEnvironmentChange: (name: string | null) => void;
  onUpdateConnection: (input: {
    environment_name: string;
    current_name?: string;
    name: string;
    type: string;
    values: Record<string, unknown>;
  }) => Promise<WorkspaceConfigResponse>;
  selectedConnectionName?: string | null;
  selectedEnvironmentName?: string | null;
}) {
  const [connectionForm, setConnectionForm] = useState<ConnectionFormState>({
    environmentName: "",
    name: "",
    type: "",
    values: {},
  });

  const activeEnvironment = useMemo(
    () =>
      environments.find(
        (environment) => environment.name === selectedEnvironmentName
      ) ?? null,
    [environments, selectedEnvironmentName]
  );

  const activeConnection = useMemo(
    () =>
      activeEnvironment?.connections.find(
        (connection) => connection.name === selectedConnectionName
      ) ?? null,
    [activeEnvironment, selectedConnectionName]
  );

  const selectedConnectionType = useMemo(
    () =>
      connectionTypes.find(
        (connectionType) => connectionType.type_name === connectionForm.type
      ) ?? null,
    [connectionForm.type, connectionTypes]
  );

  useEffect(() => {
    if (mode === "create") {
      const fallbackType = connectionTypes[0]?.type_name ?? "";
      setConnectionForm({
        environmentName:
          selectedEnvironmentName || defaultEnvironment || environments[0]?.name || "",
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
        values: buildInitialConnectionValues(
          connectionTypes,
          connectionTypes[0]?.type_name ?? "",
          null
        ),
      });
      return;
    }

    setConnectionForm({
      environmentName: activeEnvironment.name,
      name: activeConnection.name,
      type: activeConnection.type,
      values: buildInitialConnectionValues(
        connectionTypes,
        activeConnection.type,
        activeConnection
      ),
    });
  }, [
    activeConnection,
    activeEnvironment,
    connectionTypes,
    defaultEnvironment,
    environments,
    mode,
    selectedEnvironmentName,
  ]);

  const handleSave = async () => {
    const payload = {
      environment_name: connectionForm.environmentName,
      name: connectionForm.name.trim(),
      type: connectionForm.type,
      values: connectionForm.values,
    };

    if (mode === "create") {
      const response = await onCreateConnection(payload);
      const environment = response.environments.find(
        (candidate) => candidate.name === connectionForm.environmentName
      );
      onModeChange("edit");
      onSelectedEnvironmentChange(connectionForm.environmentName);
      onSelectedConnectionChange(
        connectionForm.name.trim() || environment?.connections[0]?.name || null
      );
      return;
    }

    const response = await onUpdateConnection({
      ...payload,
      current_name: activeConnection?.name,
    });
    const environment = response.environments.find(
      (candidate) => candidate.name === connectionForm.environmentName
    );
    onSelectedConnectionChange(
      connectionForm.name.trim() || environment?.connections[0]?.name || null
    );
  };

  const handleDelete = async () => {
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
    onSelectedConnectionChange(environment?.connections[0]?.name ?? null);
  };

  return {
    activeConnection,
    activeEnvironment,
    connectionForm,
    selectedConnectionType,
    setConnectionForm: setConnectionForm as Dispatch<SetStateAction<ConnectionFormState>>,
    handleDelete,
    handleSave,
  };
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
