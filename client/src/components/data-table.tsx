import { Loader2, ArrowUp, ArrowDown, ArrowUpDown } from "lucide-react";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import type { ColumnInfo } from "@/lib/types";

export interface SortColumn {
  column: string;
  direction: "asc" | "desc";
}

export type SortConfig = SortColumn[];

interface DataTableProps {
  columns: ColumnInfo[];
  rows: Record<string, unknown>[];
  isLoading: boolean;
  sort?: SortConfig | null;
  onSort?: (column: string, isMultiSort: boolean) => void;
}

function formatCellValue(value: unknown): string {
  if (value === null) return "NULL";
  if (value === undefined) return "";
  if (typeof value === "object") {
    return JSON.stringify(value);
  }
  return String(value);
}

export function DataTable({ columns, rows, isLoading, sort, onSort }: DataTableProps) {
  if (isLoading) {
    return (
      <div className="flex items-center justify-center h-64">
        <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
      </div>
    );
  }

  if (columns.length === 0) {
    return (
      <div className="flex items-center justify-center h-64 text-muted-foreground">
        Select a table to view data
      </div>
    );
  }

  if (rows.length === 0) {
    return (
      <div className="flex items-center justify-center h-64 text-muted-foreground">
        No rows found
      </div>
    );
  }

  return (
    <div className="border rounded-lg bg-card flex flex-col h-full overflow-hidden">
      <div className="overflow-auto flex-1 scrollbar-always-visible">
        <Table className="min-w-max">
          <TableHeader className="sticky top-0 bg-card z-10">
            <TableRow>
              {columns.map((col) => {
                const sortIndex = sort?.findIndex(s => s.column === col.name) ?? -1;
                const isSorted = sortIndex >= 0;
                const sortDirection = isSorted ? sort![sortIndex].direction : null;
                const sortOrder = isSorted ? sortIndex + 1 : null;
                const isMultiSort = (sort?.length ?? 0) > 1;
                
                return (
                  <TableHead
                    key={col.name}
                    className="min-w-[120px] font-medium whitespace-nowrap"
                  >
                    <button
                      type="button"
                      onClick={(e) => onSort?.(col.name, e.shiftKey)}
                      className="flex flex-col items-start w-full text-left hover:bg-muted/50 rounded px-1 py-0.5 -mx-1 transition-colors"
                      data-testid={`sort-column-${col.name}`}
                      title="Click to sort, Shift+click to add to multi-sort"
                    >
                      <div className="flex items-center gap-1">
                        <span className="font-mono text-sm">{col.name}</span>
                        {isSorted ? (
                          <span className="flex items-center">
                            {sortDirection === "asc" ? (
                              <ArrowUp className="h-3 w-3 text-primary" />
                            ) : (
                              <ArrowDown className="h-3 w-3 text-primary" />
                            )}
                            {isMultiSort && (
                              <span className="text-[10px] text-primary font-bold ml-0.5">{sortOrder}</span>
                            )}
                          </span>
                        ) : (
                          <ArrowUpDown className="h-3 w-3 text-muted-foreground opacity-50" />
                        )}
                      </div>
                      <span className="text-xs text-muted-foreground font-normal">
                        {col.dataType}
                        {col.isPrimaryKey && " (PK)"}
                      </span>
                    </button>
                  </TableHead>
                );
              })}
            </TableRow>
          </TableHeader>
          <TableBody>
            {rows.map((row, rowIdx) => (
              <TableRow
                key={rowIdx}
                className="hover-elevate"
                data-testid={`row-data-${rowIdx}`}
              >
                {columns.map((col) => (
                  <TableCell
                    key={col.name}
                    className="font-mono text-xs max-w-[300px] truncate"
                    title={formatCellValue(row[col.name])}
                  >
                    {row[col.name] === null ? (
                      <span className="text-muted-foreground italic">NULL</span>
                    ) : (
                      formatCellValue(row[col.name])
                    )}
                  </TableCell>
                ))}
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </div>
    </div>
  );
}
