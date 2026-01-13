import { useRef, useEffect, useCallback } from "react";
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
  const topScrollRef = useRef<HTMLDivElement>(null);
  const topScrollInnerRef = useRef<HTMLDivElement>(null);
  const tableContainerRef = useRef<HTMLDivElement>(null);
  const isScrollingSyncRef = useRef(false);

  const syncScroll = useCallback((source: "top" | "table") => {
    if (isScrollingSyncRef.current) return;
    isScrollingSyncRef.current = true;

    const topScroll = topScrollRef.current;
    const tableContainer = tableContainerRef.current;

    if (topScroll && tableContainer) {
      if (source === "top") {
        tableContainer.scrollLeft = topScroll.scrollLeft;
      } else {
        topScroll.scrollLeft = tableContainer.scrollLeft;
      }
    }

    requestAnimationFrame(() => {
      isScrollingSyncRef.current = false;
    });
  }, []);

  useEffect(() => {
    const topScroll = topScrollRef.current;
    const tableContainer = tableContainerRef.current;
    const topScrollInner = topScrollInnerRef.current;

    if (!topScroll || !tableContainer || !topScrollInner) return;

    const handleTopScroll = () => syncScroll("top");
    const handleTableScroll = () => syncScroll("table");

    topScroll.addEventListener("scroll", handleTopScroll);
    tableContainer.addEventListener("scroll", handleTableScroll);

    const resizeObserver = new ResizeObserver(() => {
      topScrollInner.style.width = `${tableContainer.scrollWidth}px`;
    });
    resizeObserver.observe(tableContainer);
    topScrollInner.style.width = `${tableContainer.scrollWidth}px`;

    return () => {
      topScroll.removeEventListener("scroll", handleTopScroll);
      tableContainer.removeEventListener("scroll", handleTableScroll);
      resizeObserver.disconnect();
    };
  }, [syncScroll]);

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
    <div className="border rounded-lg bg-card">
      <div 
        ref={topScrollRef}
        className="overflow-x-auto overflow-y-hidden"
        style={{ height: "12px" }}
      >
        <div ref={topScrollInnerRef} style={{ height: "1px" }} />
      </div>
      <div ref={tableContainerRef} className="overflow-x-auto">
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
    </div>
  );
}
