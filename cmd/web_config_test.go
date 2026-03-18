package cmd

import "testing"

func TestBuildWorkspaceConfigConnectionTypesPreservesDeclaredFieldOrder(t *testing.T) {
	t.Parallel()

	connectionTypes := buildWorkspaceConfigConnectionTypes()
	var postgresFields []workspaceConfigFieldDef
	for _, connectionType := range connectionTypes {
		if connectionType.TypeName == "postgres" {
			postgresFields = connectionType.Fields
			break
		}
	}

	if len(postgresFields) == 0 {
		t.Fatal("expected postgres connection type")
	}

	want := []string{
		"username",
		"password",
		"host",
		"port",
		"database",
		"schema",
		"pool_max_conns",
		"ssl_mode",
	}

	if len(postgresFields) != len(want) {
		t.Fatalf("expected %d postgres fields, got %d", len(want), len(postgresFields))
	}

	for index, field := range postgresFields {
		if field.Name != want[index] {
			t.Fatalf("field %d: expected %q, got %q", index, want[index], field.Name)
		}
	}
}
