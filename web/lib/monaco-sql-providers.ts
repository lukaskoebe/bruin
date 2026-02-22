import type * as MonacoNS from "monaco-editor";

import { SchemaTable, findTableByIdentifier } from "@/lib/sql-schema";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

type Monaco = typeof MonacoNS;

// ---------------------------------------------------------------------------
// SQL keywords (top completion group for context, but lower priority than schema)
// ---------------------------------------------------------------------------

const SQL_KEYWORDS = [
  "SELECT", "FROM", "WHERE", "AND", "OR", "NOT", "IN", "IS", "NULL",
  "JOIN", "LEFT", "RIGHT", "INNER", "OUTER", "CROSS", "FULL", "ON",
  "GROUP", "BY", "ORDER", "ASC", "DESC", "HAVING", "LIMIT", "OFFSET",
  "INSERT", "INTO", "VALUES", "UPDATE", "SET", "DELETE", "CREATE", "TABLE",
  "VIEW", "DROP", "ALTER", "AS", "WITH", "UNION", "ALL", "DISTINCT",
  "CASE", "WHEN", "THEN", "ELSE", "END", "BETWEEN", "LIKE", "EXISTS",
  "TRUE", "FALSE", "COUNT", "SUM", "AVG", "MIN", "MAX", "CAST",
  "COALESCE", "NULLIF", "OVER", "PARTITION", "ROW_NUMBER", "RANK",
  "DENSE_RANK", "LAG", "LEAD", "FIRST_VALUE", "LAST_VALUE",
];

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/**
 * Extract the word immediately before the cursor (or the dot-qualified prefix)
 * so we can figure out if the user is requesting column completions for a
 * specific table alias/name.
 *
 * Returns `{ tablePart, columnPrefix }` when the text before cursor looks like
 * `tableName.col…`, otherwise returns `null`.
 */
function parseDotPrefix(
  textBeforeCursor: string,
): { tablePart: string; columnPrefix: string } | null {
  // Match `identifier.` possibly followed by a partial column name.
  const match = textBeforeCursor.match(/([\w.]+)\.\s*([\w]*)$/);
  if (!match) {
    return null;
  }

  return { tablePart: match[1], columnPrefix: match[2] };
}

/**
 * Extract the SQL identifier (possibly dot-qualified) under or adjacent to the
 * cursor position.  Used for go-to-definition.
 */
function identifierAtPosition(
  model: MonacoNS.editor.ITextModel,
  position: MonacoNS.Position,
): string | null {
  const line = model.getLineContent(position.lineNumber);
  const col = position.column - 1; // 0-based

  // Walk left to find start of identifier.
  let start = col;
  while (start > 0 && /[\w.]/.test(line[start - 1])) {
    start--;
  }

  // Walk right to find end.
  let end = col;
  while (end < line.length && /[\w.]/.test(line[end])) {
    end++;
  }

  const word = line.slice(start, end).trim();
  return word.length > 0 ? word : null;
}

/**
 * Resolve the table referenced at the cursor position, when any.
 */
export function resolveTableAtPosition(
  model: MonacoNS.editor.ITextModel,
  position: MonacoNS.Position,
  tables: SchemaTable[],
): SchemaTable | null {
  const identifier = identifierAtPosition(model, position);
  if (!identifier) {
    return null;
  }

  return findTableByIdentifier(tables, identifier) ?? null;
}

/**
 * Check whether the cursor is in a position where table names are expected
 * (after FROM, JOIN, etc.).  This is a simple heuristic.
 */
function isInTablePosition(textBeforeCursor: string): boolean {
  const normalized = textBeforeCursor.replace(/\s+/g, " ").toUpperCase();
  return /(?:FROM|JOIN|INTO|UPDATE|TABLE|VIEW)\s+[\w."]*$/i.test(normalized);
}

// ---------------------------------------------------------------------------
// Provider registration
// ---------------------------------------------------------------------------

/**
 * Register SQL autocompletion and go-to-definition providers for the Monaco
 * editor.  Returns a `Disposable` that removes all registrations — call it on
 * unmount or when the schema changes.
 *
 * The providers close over the `tables` array so they always reflect the
 * latest workspace state.  When the workspace updates, dispose + re-register.
 */
export function registerSQLProviders(
  monaco: Monaco,
  tables: SchemaTable[],
): MonacoNS.IDisposable {
  const disposables: MonacoNS.IDisposable[] = [];

  // -----------------------------------------------------------------------
  // Completion
  // -----------------------------------------------------------------------
  disposables.push(
    monaco.languages.registerCompletionItemProvider("sql", {
      triggerCharacters: ["."],

      provideCompletionItems(
        model: MonacoNS.editor.ITextModel,
        position: MonacoNS.Position,
      ) {
        const wordInfo = model.getWordUntilPosition(position);
        const range: MonacoNS.IRange = {
          startLineNumber: position.lineNumber,
          endLineNumber: position.lineNumber,
          startColumn: wordInfo.startColumn,
          endColumn: wordInfo.endColumn,
        };

        const lineContent = model.getLineContent(position.lineNumber);
        const textBeforeCursor = lineContent.slice(0, position.column - 1);

        const suggestions: MonacoNS.languages.CompletionItem[] = [];

        // --- Column completions after `table.` ---
        const dotPrefix = parseDotPrefix(textBeforeCursor);
        if (dotPrefix) {
          const table = findTableByIdentifier(tables, dotPrefix.tablePart);
          if (table && table.columns.length > 0) {
            // Adjust the replacement range to cover only the column prefix.
            const columnRange: MonacoNS.IRange = {
              startLineNumber: position.lineNumber,
              endLineNumber: position.lineNumber,
              startColumn: position.column - dotPrefix.columnPrefix.length,
              endColumn: position.column,
            };

            for (const column of table.columns) {
              const typeLabel = column.type ? ` (${column.type})` : "";
              suggestions.push({
                label: column.name,
                kind: monaco.languages.CompletionItemKind.Field,
                detail: `${table.shortName}${typeLabel}`,
                documentation: column.description || undefined,
                insertText: column.name,
                range: columnRange,
                sortText: column.primaryKey ? "0" : "1",
              });
            }

            return { suggestions };
          }
        }

        // --- Table completions ---
        const inTableCtx = isInTablePosition(textBeforeCursor);

        for (const table of tables) {
          const priority = table.isBruinAsset ? "0" : "1";
          const kindTag = table.isBruinAsset ? "Asset" : "Table";

          suggestions.push({
            label: {
              label: table.name,
              description: kindTag,
            },
            kind: monaco.languages.CompletionItemKind.Struct,
            detail: `${kindTag}: ${table.name}`,
            documentation: table.columns.length > 0
              ? `Columns: ${table.columns.map((c) => c.name).join(", ")}`
              : undefined,
            insertText: table.name,
            range,
            sortText: inTableCtx ? priority : `2${priority}`,
          });

          // Also offer the short name if it differs.
          if (table.shortName !== table.name) {
            suggestions.push({
              label: {
                label: table.shortName,
                description: `${kindTag} (${table.name})`,
              },
              kind: monaco.languages.CompletionItemKind.Struct,
              detail: `${kindTag}: ${table.name}`,
              insertText: table.name,
              range,
              sortText: inTableCtx ? `${priority}b` : `2${priority}b`,
            });
          }
        }

        // --- SQL keyword completions (lowest priority) ---
        for (const keyword of SQL_KEYWORDS) {
          suggestions.push({
            label: keyword,
            kind: monaco.languages.CompletionItemKind.Keyword,
            insertText: keyword,
            range,
            sortText: "9",
          });
        }

        return { suggestions };
      },
    }),
  );

  // -----------------------------------------------------------------------
  // Definition (used for Ctrl/Cmd-hover underline + F12 availability)
  // -----------------------------------------------------------------------
  disposables.push(
    monaco.languages.registerDefinitionProvider("sql", {
      provideDefinition(
        model: MonacoNS.editor.ITextModel,
        position: MonacoNS.Position,
      ) {
        const table = resolveTableAtPosition(model, position, tables);
        if (!table) {
          return null;
        }

        const wordInfo = model.getWordAtPosition(position);
        const range = wordInfo
          ? new monaco.Range(
              position.lineNumber,
              wordInfo.startColumn,
              position.lineNumber,
              wordInfo.endColumn,
            )
          : new monaco.Range(
              position.lineNumber,
              position.column,
              position.lineNumber,
              position.column,
            );

        return {
          uri: model.uri,
          range,
        };
      },
    }),
  );

  // -----------------------------------------------------------------------
  // Hover — table/column documentation
  // -----------------------------------------------------------------------
  disposables.push(
    monaco.languages.registerHoverProvider("sql", {
      provideHover(
        model: MonacoNS.editor.ITextModel,
        position: MonacoNS.Position,
      ) {
        const identifier = identifierAtPosition(model, position);
        if (!identifier) {
          return null;
        }

        // Check for `table.column` pattern.
        const dotIndex = identifier.lastIndexOf(".");
        if (dotIndex > 0) {
          const tablePart = identifier.slice(0, dotIndex);
          const columnPart = identifier.slice(dotIndex + 1);
          const table = findTableByIdentifier(tables, tablePart);
          if (table) {
            const column = table.columns.find(
              (c) => c.name.toLowerCase() === columnPart.toLowerCase(),
            );
            if (column) {
              const parts: string[] = [`**${table.name}.${column.name}**`];
              if (column.type) {
                parts.push(`Type: \`${column.type}\``);
              }
              if (column.primaryKey) {
                parts.push("🔑 Primary Key");
              }
              if (column.description) {
                parts.push(column.description);
              }

              const wordInfo = model.getWordAtPosition(position);
              return {
                range: wordInfo
                  ? new monaco.Range(
                      position.lineNumber,
                      wordInfo.startColumn,
                      position.lineNumber,
                      wordInfo.endColumn,
                    )
                  : new monaco.Range(
                      position.lineNumber,
                      position.column,
                      position.lineNumber,
                      position.column,
                    ),
                contents: [{ value: parts.join("\n\n") }],
              };
            }
          }
        }

        // Plain table name.
        const table = findTableByIdentifier(tables, identifier);
        if (!table) {
          return null;
        }

        const parts: string[] = [`**${table.name}**`];
        if (table.isBruinAsset) {
          parts.push("_Bruin Asset_ — Ctrl+Click to navigate");
        }
        if (table.assetPath) {
          parts.push(`Defined by: \`${table.assetPath}\``);
        }
        if (table.columns.length > 0) {
          const columnList = table.columns
            .map((c) => {
              const pk = c.primaryKey ? " 🔑" : "";
              const ty = c.type ? `: ${c.type}` : "";
              return `- \`${c.name}\`${ty}${pk}`;
            })
            .join("\n");
          parts.push(`**Columns**\n${columnList}`);
        }

        const wordInfo = model.getWordAtPosition(position);
        return {
          range: wordInfo
            ? new monaco.Range(
                position.lineNumber,
                wordInfo.startColumn,
                position.lineNumber,
                wordInfo.endColumn,
              )
            : new monaco.Range(
                position.lineNumber,
                position.column,
                position.lineNumber,
                position.column,
              ),
          contents: [{ value: parts.join("\n\n") }],
        };
      },
    }),
  );

  return {
    dispose() {
      for (const disposable of disposables) {
        disposable.dispose();
      }
    },
  };
}
