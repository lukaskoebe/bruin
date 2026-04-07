"use client";

import { useAtom, useSetAtom } from "jotai";
import { useCallback, useEffect, useMemo } from "react";
import { useNavigate } from "@tanstack/react-router";

import {
  onboardingBusyAtom,
  onboardingDiscoveryBusyAtom,
  onboardingDiscoveryErrorAtom,
  onboardingDiscoveryStateAtom,
  onboardingDraftValuesAtom,
  onboardingImportFormAtom,
  onboardingImportResultAtom,
  onboardingSelectedTablesAtom,
  onboardingSelectedTypeAtom,
  onboardingStepAtom,
  OnboardingStep,
  syncOnboardingAtomsAtom,
} from "@/lib/atoms/onboarding";
import {
  importOnboardingDatabase,
  previewOnboardingDiscovery,
  testWorkspaceConnection,
  updateOnboardingState,
} from "@/lib/api";
import { buildConnectionFieldDefaults } from "@/lib/settings-form-utils";
import {
  OnboardingSessionState,
  WorkspaceConfigConnectionType,
  WorkspaceConfigResponse,
} from "@/lib/types";

type Params = {
  workspaceConfig: WorkspaceConfigResponse;
  onboardingState: OnboardingSessionState;
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
  onReloadConfig: () => Promise<void> | void;
  onReloadWorkspace?: () => Promise<void> | void;
};

const FEATURED_TYPES = ["postgres", "duckdb", "snowflake", "google_cloud_platform", "redshift", "databricks"];

export function useOnboardingFlow({
  workspaceConfig,
  onboardingState,
  onCreateConnection,
  onUpdateConnection,
  onReloadConfig,
  onReloadWorkspace,
}: Params) {
  const navigate = useNavigate();
  const setSyncedState = useSetAtom(syncOnboardingAtomsAtom);
  const [step, setStep] = useAtom(onboardingStepAtom);
  const [selectedType, setSelectedType] = useAtom(onboardingSelectedTypeAtom);
  const [busy, setBusy] = useAtom(onboardingBusyAtom);
  const [discoveryBusy, setDiscoveryBusy] = useAtom(onboardingDiscoveryBusyAtom);
  const [discoveryError, setDiscoveryError] = useAtom(onboardingDiscoveryErrorAtom);
  const [discoveryState, setDiscoveryState] = useAtom(onboardingDiscoveryStateAtom);
  const [selectedTables, setSelectedTables] = useAtom(onboardingSelectedTablesAtom);
  const [draftValues, setDraftValues] = useAtom(onboardingDraftValuesAtom);
  const [importForm, setImportForm] = useAtom(onboardingImportFormAtom);
  const [importResult, setImportResult] = useAtom(onboardingImportResultAtom);

  const environments = workspaceConfig.environments;
  const connectionTypes = workspaceConfig.connection_types;
  const defaultEnvironment =
    onboardingState.environment_name ||
    workspaceConfig.selected_environment ||
    workspaceConfig.default_environment ||
    environments[0]?.name ||
    "default";

  const featuredTypes = useMemo(
    () =>
      FEATURED_TYPES.map((type) => connectionTypes.find((item) => item.type_name === type)).filter(Boolean) as WorkspaceConfigConnectionType[],
    [connectionTypes]
  );

  useEffect(() => {
    setSyncedState({
      ...onboardingState,
      selected_type_fallback: featuredTypes[0]?.type_name || "postgres",
    });
  }, [featuredTypes, onboardingState, setSyncedState]);

  const defaultDraftValues = useMemo(
    () =>
      applyOnboardingDraftDefaults(
        selectedType,
        buildConnectionFieldDefaults({
          connectionTypes,
          existingConnection: null,
          typeName: selectedType,
        })
      ),
    [connectionTypes, selectedType]
  );

  useEffect(() => {
    if (
      step !== "connection-config" ||
      !selectedType ||
      Object.keys(draftValues).length > 0 ||
      Object.keys(onboardingState.draft_values ?? {}).length > 0
    ) {
      return;
    }
    setDraftValues(defaultDraftValues);
  }, [defaultDraftValues, draftValues, onboardingState.draft_values, selectedType, setDraftValues, step]);

  const connectionName = `${selectedType}-default`;

  const persistState = useCallback(
    async (overrides: Partial<OnboardingSessionState> = {}) => {
      await updateOnboardingState({
        active: true,
        step,
        selected_type: selectedType,
        environment_name: defaultEnvironment,
        draft_values: draftValues,
        import_form: {
          database: importForm.database,
          pipeline_name: importForm.pipelineName,
          schema: importForm.schema,
          pattern: importForm.pattern,
          disable_columns: importForm.disableColumns,
        },
        selected_tables: selectedTables,
        import_result: importResult
          ? {
              output: importResult.output,
              error: importResult.error,
              pipeline_path: importResult.pipeline_path,
              asset_paths: importResult.asset_paths,
            }
          : null,
        ...overrides,
      });
    },
    [defaultEnvironment, draftValues, importForm, importResult, selectedTables, selectedType, step]
  );

  useEffect(() => {
    if (step !== "import" || discoveryBusy || discoveryState.databases.length > 0) {
      return;
    }

    const loadSavedDiscovery = async () => {
      setDiscoveryBusy(true);
      setDiscoveryError(null);
      try {
        const resumeDatabase =
          selectedType === "duckdb"
            ? importForm.database
            : selectedTables.length > 0
              ? importForm.database
              : undefined;

        const response = await previewOnboardingDiscovery({
          environment_name: defaultEnvironment,
          type: selectedType,
          values: draftValues,
          database: resumeDatabase,
        });
        setDiscoveryState(response);
        if ((response.tables ?? []).length > 0 && selectedTables.length === 0) {
          setSelectedTables(response.tables.map((table) => table.name));
        }
      } catch (error) {
        setDiscoveryError(
          error instanceof Error ? error.message : "Failed to inspect available objects."
        );
      } finally {
        setDiscoveryBusy(false);
      }
    };

    void loadSavedDiscovery();
  }, [defaultEnvironment, discoveryBusy, discoveryState.databases.length, draftValues, importForm.database, selectedTables.length, selectedType, setDiscoveryBusy, setDiscoveryError, setDiscoveryState, setSelectedTables, step]);

  const runDiscovery = useCallback(
    async (values: Record<string, string | number | boolean>, database?: string) => {
      setDiscoveryBusy(true);
      setDiscoveryError(null);
      try {
        await testWorkspaceConnection({
          environment_name: defaultEnvironment,
          name: connectionName,
          type: selectedType,
          values,
        });

        const response = await previewOnboardingDiscovery({
          environment_name: defaultEnvironment,
          type: selectedType,
          values,
          database,
        });

        const nextDatabase =
          selectedType === "duckdb"
            ? response.selected_database ?? database ?? importForm.database
            : database ?? importForm.database;
        const nextSchema =
          importForm.schema || response.tables.find((table) => table.schema_name)?.schema_name || importForm.schema;
        const nextSelectedTables = (response.tables ?? []).map((table) => table.name);

        setDiscoveryState(response);
        setDraftValues(values);
        setImportForm((current) => ({
          ...current,
          database: nextDatabase,
          schema: nextSchema,
        }));
        setSelectedTables(nextSelectedTables);
        setStep("import");

        await updateOnboardingState({
          active: true,
          step: "import",
          selected_type: selectedType,
          environment_name: defaultEnvironment,
          draft_values: values,
          import_form: {
            database: nextDatabase,
            pipeline_name: importForm.pipelineName,
            schema: nextSchema,
            pattern: importForm.pattern,
            disable_columns: importForm.disableColumns,
          },
          selected_tables: nextSelectedTables,
          import_result: null,
        });
        void navigate({ to: "/onboarding/import", replace: true });
      } catch (error) {
        setDiscoveryState({ status: "ok", databases: [], tables: [] });
        setDiscoveryError(error instanceof Error ? error.message : "Connection validation failed.");
      } finally {
        setDiscoveryBusy(false);
      }
    },
    [connectionName, defaultEnvironment, importForm.database, importForm.disableColumns, importForm.pattern, importForm.pipelineName, importForm.schema, navigate, selectedType, setDiscoveryBusy, setDiscoveryError, setDiscoveryState, setDraftValues, setImportForm, setSelectedTables, setStep]
  );

  const navigateToStep = useCallback(
    async (nextStep: OnboardingStep) => {
      setStep(nextStep);
      await persistState({ step: nextStep });
      const nextPath =
        nextStep === "connection-type"
          ? "/onboarding"
          : nextStep === "connection-config"
            ? "/onboarding/connection"
            : nextStep === "import"
              ? "/onboarding/import"
              : "/onboarding/success";
      void navigate({ to: nextPath, replace: true });
    },
    [navigate, persistState, setStep]
  );

  const chooseType = useCallback(
    async (nextType: string) => {
      const nextDraftValues = applyOnboardingDraftDefaults(
        nextType,
        buildConnectionFieldDefaults({
          connectionTypes,
          existingConnection: null,
          typeName: nextType,
        })
      );
      setSelectedType(nextType);
      setDraftValues(nextDraftValues);
      setDiscoveryState({ status: "ok", databases: [], tables: [] });
      setDiscoveryError(null);
      setSelectedTables([]);
      setImportResult(null);
      setStep("connection-config");
      await updateOnboardingState({
        active: true,
        step: "connection-config",
        selected_type: nextType,
        environment_name: defaultEnvironment,
        draft_values: nextDraftValues,
        import_form: {
          pipeline_name: importForm.pipelineName,
        },
        selected_tables: [],
        import_result: null,
      });
      void navigate({ to: "/onboarding/connection", replace: true });
    },
    [connectionTypes, defaultEnvironment, importForm.pipelineName, navigate, setDiscoveryError, setDiscoveryState, setDraftValues, setImportResult, setSelectedTables, setSelectedType, setStep]
  );

  const handleSaveAndImport = useCallback(async () => {
    if (!selectedType || !connectionName || (selectedType !== "duckdb" && !importForm.database.trim())) {
      return;
    }

    setBusy(true);
    try {
      const values = {
        ...draftValues,
        ...(selectedType === "duckdb" ? {} : { database: importForm.database.trim() }),
      };

      await onReloadConfig();
      const existingConnection = workspaceConfig.environments
        .find((item) => item.name === defaultEnvironment)
        ?.connections.find((item) => item.name === connectionName);

      if (existingConnection) {
        await onUpdateConnection({
          environment_name: defaultEnvironment,
          current_name: connectionName,
          name: connectionName,
          type: selectedType,
          values,
        });
      } else {
        await onCreateConnection({
          environment_name: defaultEnvironment,
          name: connectionName,
          type: selectedType,
          values,
        });
      }

      const response = await importOnboardingDatabase({
        connection_name: connectionName,
        environment_name: defaultEnvironment,
        pipeline_name: importForm.pipelineName.trim(),
        schema: importForm.schema.trim(),
        pattern: importForm.pattern.trim(),
        tables: normalizeOnboardingTableNames(selectedTables),
        disable_columns: importForm.disableColumns,
        create_if_missing: true,
      });

      setImportResult(response);
      if (response.status === "ok") {
        await onReloadWorkspace?.();
        setStep("success");
        await updateOnboardingState({
          active: true,
          step: "success",
          selected_type: selectedType,
          environment_name: defaultEnvironment,
          draft_values: values,
          import_form: {
            database: importForm.database,
            pipeline_name: importForm.pipelineName,
            schema: importForm.schema,
            pattern: importForm.pattern,
            disable_columns: importForm.disableColumns,
          },
          selected_tables: selectedTables,
          import_result: {
            output: response.output,
            error: response.error,
            pipeline_path: response.pipeline_path,
            asset_paths: response.asset_paths,
          },
        });
        void navigate({ to: "/onboarding/success", replace: true });
      }
    } finally {
      setBusy(false);
    }
  }, [connectionName, defaultEnvironment, draftValues, importForm, onCreateConnection, onReloadConfig, onReloadWorkspace, onUpdateConnection, selectedTables, selectedType, setBusy, setImportResult, setStep, workspaceConfig.environments, navigate]);

  const handleSkip = useCallback(async () => {
    await updateOnboardingState({ active: false });
    void navigate({ to: "/", replace: true });
  }, [navigate]);

  const handleComplete = useCallback(async () => {
    await updateOnboardingState({ active: false });
    await onReloadConfig();
    void navigate({ to: "/", replace: true });
  }, [navigate, onReloadConfig]);

  const importDisabled =
    busy ||
    discoveryBusy ||
    !importForm.pipelineName.trim() ||
    (selectedType !== "duckdb" && !selectedTables.length) ||
    (selectedType !== "duckdb" && !importForm.database.trim());

  return {
    connectionName,
    defaultDraftValues,
    defaultEnvironment,
    discoveryBusy,
    discoveryError,
    discoveryState,
    draftValues,
    featuredTypes,
    busy,
    importDisabled,
    importForm,
    importResult,
    navigateToStep,
    handleComplete,
    handleSaveAndImport,
    handleSkip,
    handleSelectDatabase: async (database: string) => {
      setImportForm((current) => ({ ...current, database }));
      await runDiscovery(draftValues, database);
    },
    runDiscovery,
    selectedTables,
    selectedType,
    setImportForm,
    setSelectedTables,
    chooseType,
    step,
  };
}

function applyOnboardingDraftDefaults(
  typeName: string,
  values: Record<string, string | number | boolean>
) {
  if (typeName === "postgres") {
    return {
      ...values,
      ssl_mode: "disable",
    };
  }

  return values;
}

function normalizeOnboardingTableNames(tables: string[]) {
  return tables
    .map((table) => table.trim())
    .filter(Boolean)
    .map((table) => {
      const parts = table.split(".");
      return parts.length >= 3 ? parts.slice(-2).join(".") : table;
    });
}
