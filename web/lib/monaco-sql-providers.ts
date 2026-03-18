import type * as MonacoNS from "monaco-editor";

import { SchemaTable, findTableByIdentifier } from "@/lib/sql-schema";

type Monaco = typeof MonacoNS;

type RemoteSQLResolver = {
  provideTableContextSuggestions(args: {
    monaco: Monaco;
    prefix: string;
    range: MonacoNS.IRange;
  }): Promise<MonacoNS.languages.CompletionItem[]>;
  provideColumnSuggestions(args: {
    monaco: Monaco;
    tableIdentifier: string;
    columnPrefix: string;
    range: MonacoNS.IRange;
  }): Promise<MonacoNS.languages.CompletionItem[]>;
  providePathSuggestions(args: {
    monaco: Monaco;
    prefix: string;
    range: MonacoNS.IRange;
  }): Promise<MonacoNS.languages.CompletionItem[]>;
};

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

type SQLClauseContext =
  | "select"
  | "from"
  | "join"
  | "where"
  | "group-by"
  | "order-by"
  | "having"
  | "into"
  | "update"
  | "set"
  | "values"
  | null;

function parseDotPrefix(
  textBeforeCursor: string,
): { tablePart: string; columnPrefix: string } | null {
  const match = textBeforeCursor.match(/([\w."]+)\.\s*([\w]*)$/);
  if (!match) {
    return null;
  }

  return { tablePart: match[1].replace(/"/g, ""), columnPrefix: match[2] };
}

function buildAliasMap(
  sqlText: string,
  tables: SchemaTable[],
): Map<string, SchemaTable> {
  const aliasMap = new Map<string, SchemaTable>();
  const relationPattern =
    /\b(?:from|join|into|update)\s+([\w."]+)(?:\s+(?:as\s+)?([a-zA-Z_]\w*))?/gi;

  for (const match of sqlText.matchAll(relationPattern)) {
    const identifier = match[1]?.replace(/"/g, "");
    const alias = match[2];
    if (!identifier || !alias) {
      continue;
    }

    const table = findTableByIdentifier(tables, identifier);
    if (!table) {
      continue;
    }

    aliasMap.set(alias.toLowerCase(), table);
  }

  return aliasMap;
}

function resolveTableReference(
  tables: SchemaTable[],
  aliasMap: Map<string, SchemaTable>,
  identifier: string,
): SchemaTable | undefined {
  const normalized = identifier.replace(/"/g, "").toLowerCase();
  const aliasMatch = aliasMap.get(normalized);
  if (aliasMatch) {
    return aliasMatch;
  }

  return findTableByIdentifier(tables, normalized);
}

function resolveReferencedTables(
  tables: SchemaTable[],
  upstreamNames: string[],
  aliasMap: Map<string, SchemaTable>,
): SchemaTable[] {
  const referenced: SchemaTable[] = [];
  const seen = new Set<string>();

  for (const upstreamName of upstreamNames) {
    const table = findTableByIdentifier(tables, upstreamName);
    if (!table) {
      continue;
    }

    const key = table.name.toLowerCase();
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    referenced.push(table);
  }

  for (const table of aliasMap.values()) {
    const key = table.name.toLowerCase();
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    referenced.push(table);
  }

  return referenced;
}

function collectColumnSuggestions(
  monaco: Monaco,
  scopedTables: SchemaTable[],
  range: MonacoNS.IRange,
): MonacoNS.languages.CompletionItem[] {
  const suggestions: MonacoNS.languages.CompletionItem[] = [];
  const seen = new Set<string>();

  for (const table of scopedTables) {
    for (const column of table.columns) {
      const key = column.name.toLowerCase();
      if (seen.has(key)) {
        continue;
      }
      seen.add(key);

      const typeLabel = column.type ? ` (${column.type})` : "";
      suggestions.push({
        label: column.name,
        kind: monaco.languages.CompletionItemKind.Field,
        detail: `${table.shortName}.${column.name}${typeLabel}`,
        documentation: column.description || undefined,
        insertText: column.name,
        range,
        sortText: column.primaryKey ? "1" : "2",
      });
    }
  }

  return suggestions;
}

function stripSQLCommentsAndStrings(sqlText: string): string {
  let result = "";
  let index = 0;

  while (index < sqlText.length) {
    const current = sqlText[index];
    const next = sqlText[index + 1];

    if (current === "-" && next === "-") {
      while (index < sqlText.length && sqlText[index] !== "\n") {
        result += " ";
        index++;
      }
      continue;
    }

    if (current === "/" && next === "*") {
      result += "  ";
      index += 2;
      while (index < sqlText.length) {
        const blockCurrent = sqlText[index];
        const blockNext = sqlText[index + 1];
        if (blockCurrent === "*" && blockNext === "/") {
          result += "  ";
          index += 2;
          break;
        }
        result += blockCurrent === "\n" ? "\n" : " ";
        index++;
      }
      continue;
    }

    if (current === "'" || current === '"' || current === "`") {
      const quote = current;
      result += " ";
      index++;
      while (index < sqlText.length) {
        const char = sqlText[index];
        if (char === quote) {
          if (quote === "'" && sqlText[index + 1] === quote) {
            result += "  ";
            index += 2;
            continue;
          }
          result += " ";
          index++;
          break;
        }
        result += char === "\n" ? "\n" : " ";
        index++;
      }
      continue;
    }

    result += current;
    index++;
  }

  return result;
}

function getSQLClauseContext(sqlTextBeforeCursor: string): SQLClauseContext {
  const sanitized = stripSQLCommentsAndStrings(sqlTextBeforeCursor);
  const tokenPattern = /\b([a-zA-Z_][\w]*)\b|([(),;])/g;
  let depth = 0;
  let lastClause: SQLClauseContext = null;
  let previousWord = "";

  for (const match of sanitized.matchAll(tokenPattern)) {
    const word = match[1]?.toLowerCase();
    const punctuation = match[2];

    if (punctuation === "(") {
      depth++;
      continue;
    }
    if (punctuation === ")") {
      depth = Math.max(0, depth - 1);
      continue;
    }
    if (punctuation === ";") {
      lastClause = null;
      previousWord = "";
      depth = 0;
      continue;
    }

    if (!word || depth > 0) {
      continue;
    }

    if (word === "group") {
      previousWord = word;
      continue;
    }
    if (word === "by" && previousWord === "group") {
      lastClause = "group-by";
      previousWord = word;
      continue;
    }
    if (word === "order") {
      previousWord = word;
      continue;
    }
    if (word === "by" && previousWord === "order") {
      lastClause = "order-by";
      previousWord = word;
      continue;
    }

    if (word === "select") {
      lastClause = "select";
    } else if (word === "from") {
      lastClause = "from";
    } else if (word === "join") {
      lastClause = "join";
    } else if (word === "where") {
      lastClause = "where";
    } else if (word === "having") {
      lastClause = "having";
    } else if (word === "into") {
      lastClause = "into";
    } else if (word === "update") {
      lastClause = "update";
    } else if (word === "set") {
      lastClause = "set";
    } else if (word === "values") {
      lastClause = "values";
    }

    previousWord = word;
  }

  return lastClause;
}

function getCompletionContext(sqlTextBeforeCursor: string): {
  inTableCtx: boolean;
  inColumnCtx: boolean;
} {
  const clause = getSQLClauseContext(sqlTextBeforeCursor);

  switch (clause) {
    case "from":
    case "join":
    case "into":
    case "update":
      return { inTableCtx: true, inColumnCtx: false };
    case "select":
    case "where":
    case "having":
    case "group-by":
    case "order-by":
    case "set":
    case "values":
      return { inTableCtx: false, inColumnCtx: true };
    default:
      return { inTableCtx: false, inColumnCtx: false };
  }
}

function getQuotedPathContext(
  lineContent: string,
  position: MonacoNS.Position,
): { prefix: string; range: MonacoNS.IRange } | null {
  const cursorIndex = position.column - 1;
  let activeQuote: "'" | '"' | null = null;
  let quoteStart = -1;

  for (let index = 0; index < cursorIndex; index += 1) {
    const current = lineContent[index];
    const next = lineContent[index + 1];

    if (!activeQuote && current === "-" && next === "-") {
      break;
    }

    if (!activeQuote) {
      if (current === "'" || current === '"') {
        activeQuote = current;
        quoteStart = index;
      }
      continue;
    }

    if (current !== activeQuote) {
      continue;
    }

    if (next === activeQuote) {
      index += 1;
      continue;
    }

    activeQuote = null;
    quoteStart = -1;
  }

  if (!activeQuote || quoteStart < 0) {
    return null;
  }

  const prefix = lineContent.slice(quoteStart + 1, cursorIndex);
  if (
    !prefix.startsWith("s3://") &&
    !prefix.startsWith("./") &&
    !prefix.startsWith("/")
  ) {
    return null;
  }

  return {
    prefix,
    range: {
      startLineNumber: position.lineNumber,
      endLineNumber: position.lineNumber,
      startColumn: quoteStart + 2,
      endColumn: position.column,
    },
  };
}

function identifierAtPosition(
  model: MonacoNS.editor.ITextModel,
  position: MonacoNS.Position,
): string | null {
  return identifierInfoAtPosition(model, position)?.identifier ?? null;
}

function identifierInfoAtPosition(
  model: MonacoNS.editor.ITextModel,
  position: MonacoNS.Position,
): { identifier: string; range: MonacoNS.IRange } | null {
  const line = model.getLineContent(position.lineNumber);
  const col = position.column - 1;

  let start = col;
  while (start > 0 && /[\w."]/.test(line[start - 1])) {
    start--;
  }

  let end = col;
  while (end < line.length && /[\w."]/.test(line[end])) {
    end++;
  }

  const word = line.slice(start, end).trim().replace(/"/g, "");
  if (word.length === 0) {
    return null;
  }

  return {
    identifier: word,
    range: {
      startLineNumber: position.lineNumber,
      endLineNumber: position.lineNumber,
      startColumn: start + 1,
      endColumn: end + 1,
    },
  };
}

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

export function registerSQLProviders(
  monaco: Monaco,
  tables: SchemaTable[],
  upstreamNames: string[],
  remoteResolver?: RemoteSQLResolver,
): MonacoNS.IDisposable {
  const disposables: MonacoNS.IDisposable[] = [];

  disposables.push(
    monaco.languages.registerCompletionItemProvider("sql", {
      triggerCharacters: [".", "/", "'", '"'],

      async provideCompletionItems(
        model: MonacoNS.editor.ITextModel,
        position: MonacoNS.Position,
      ) {
        const identifierInfo = identifierInfoAtPosition(model, position);
        const wordInfo = model.getWordUntilPosition(position);
        const range: MonacoNS.IRange = identifierInfo?.range ?? {
          startLineNumber: position.lineNumber,
          endLineNumber: position.lineNumber,
          startColumn: wordInfo.startColumn,
          endColumn: wordInfo.endColumn,
        };
        const identifierPrefix = identifierInfo?.identifier ?? "";

        const lineContent = model.getLineContent(position.lineNumber);
        const textBeforeCursor = lineContent.slice(0, position.column - 1);
        const quotedPathContext = getQuotedPathContext(lineContent, position);
        if (quotedPathContext && remoteResolver) {
          const pathSuggestions = await remoteResolver.providePathSuggestions({
            monaco,
            prefix: quotedPathContext.prefix,
            range: quotedPathContext.range,
          });

          if (pathSuggestions.length > 0) {
            return { suggestions: pathSuggestions };
          }
        }

        const sqlTextBeforeCursor = model.getValueInRange({
          startLineNumber: 1,
          startColumn: 1,
          endLineNumber: position.lineNumber,
          endColumn: position.column,
        });
        const aliasMap = buildAliasMap(sqlTextBeforeCursor, tables);
        const referencedTables = resolveReferencedTables(
          tables,
          upstreamNames,
          aliasMap,
        );
        const columnSuggestionTables =
          referencedTables.length > 0 ? referencedTables : tables;
        const { inTableCtx, inColumnCtx } = getCompletionContext(
          sqlTextBeforeCursor,
        );

        const suggestions: MonacoNS.languages.CompletionItem[] = [];

        const dotPrefix = parseDotPrefix(textBeforeCursor);
        if (dotPrefix) {
          const table = resolveTableReference(
            tables,
            aliasMap,
            dotPrefix.tablePart,
          );
          if (table && table.columns.length > 0) {
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
                detail: `${table.shortName}.${column.name}${typeLabel}`,
                documentation: column.description || undefined,
                insertText: column.name,
                range: columnRange,
                sortText: column.primaryKey ? "0" : "1",
              });
            }

            return { suggestions };
          }

          if (remoteResolver) {
            const remoteSuggestions = await remoteResolver.provideColumnSuggestions(
              {
                monaco,
                tableIdentifier: dotPrefix.tablePart,
                columnPrefix: dotPrefix.columnPrefix,
                range: {
                  startLineNumber: position.lineNumber,
                  endLineNumber: position.lineNumber,
                  startColumn: position.column - dotPrefix.columnPrefix.length,
                  endColumn: position.column,
                },
              },
            );

            if (remoteSuggestions.length > 0) {
              return { suggestions: remoteSuggestions };
            }
          }
        }

        if (inColumnCtx) {
          const scopedColumnSuggestions = collectColumnSuggestions(
            monaco,
            columnSuggestionTables,
            range,
          );

          suggestions.push(...scopedColumnSuggestions);

          if (
            scopedColumnSuggestions.length === 0 &&
            columnSuggestionTables !== tables
          ) {
            suggestions.push(...collectColumnSuggestions(monaco, tables, range));
          }
        }

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
            sortText: inTableCtx ? priority : `4${priority}`,
          });

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
              sortText: inTableCtx ? `${priority}b` : `4${priority}b`,
            });
          }
        }

        if (inTableCtx && remoteResolver) {
          suggestions.push(
            ...(await remoteResolver.provideTableContextSuggestions({
              monaco,
              prefix: identifierPrefix,
              range,
            }))
          );
        }

        for (const keyword of SQL_KEYWORDS) {
          suggestions.push({
            label: keyword,
            kind: monaco.languages.CompletionItemKind.Keyword,
            insertText: keyword,
            range,
            sortText: "9",
          });
        }

        const dedupedSuggestions: MonacoNS.languages.CompletionItem[] = [];
        const seen = new Set<string>();

        for (const suggestion of suggestions) {
          const label =
            typeof suggestion.label === "string"
              ? suggestion.label
              : suggestion.label.label;
          const key = `${label.toLowerCase()}::${suggestion.kind}`;
          if (seen.has(key)) {
            continue;
          }

          seen.add(key);
          dedupedSuggestions.push(suggestion);
        }

        return { suggestions: dedupedSuggestions };
      },
    }),
  );

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

        const sqlTextBeforeCursor = model.getValueInRange({
          startLineNumber: 1,
          startColumn: 1,
          endLineNumber: position.lineNumber,
          endColumn: position.column,
        });
        const aliasMap = buildAliasMap(sqlTextBeforeCursor, tables);

        const dotIndex = identifier.lastIndexOf(".");
        if (dotIndex > 0) {
          const tablePart = identifier.slice(0, dotIndex);
          const columnPart = identifier.slice(dotIndex + 1);
          const table = resolveTableReference(tables, aliasMap, tablePart);
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
