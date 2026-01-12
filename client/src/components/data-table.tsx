import { Loader2 } from "lucide-react";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import type { ColumnInfo } from "@/lib/types";

interface DataTableProps {
  columns: ColumnInfo[];
  rows: Record<string, unknown>[];
  isLoading: boolean;
}

function formatCellValue(value: unknown): string {
  if (value === null) return "NULL";
  if (value === undefined) return "";
  if (typeof value === "object") {
    return JSON.stringify(value);
  }
  return String(value);
}

export function DataTable({ columns, rows, isLoading }: DataTableProps) {
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
    <div className="border rounded-lg bg-card overflow-x-auto">
      <Table className="min-w-max">
        <TableHeader className="sticky top-0 bg-card z-10">
          <TableRow>
            {columns.map((col) => (
              <TableHead
                key={col.name}
                className="min-w-[120px] font-medium whitespace-nowrap"
              >
                <div className="flex flex-col">
                  <span className="font-mono text-sm">{col.name}</span>
                  <span className="text-xs text-muted-foreground font-normal">
                    {col.dataType}
                    {col.isPrimaryKey && " (PK)"}
                  </span>
                </div>
              </TableHead>
            ))}
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
  );
}
