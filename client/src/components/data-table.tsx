import { useRef, useCallback, useState, useLayoutEffect } from "react";
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

interface CustomScrollbarProps {
  scrollWidth: number;
  clientWidth: number;
  scrollLeft: number;
  onScroll: (scrollLeft: number) => void;
}

function CustomScrollbar({ scrollWidth, clientWidth, scrollLeft, onScroll }: CustomScrollbarProps) {
  const trackRef = useRef<HTMLDivElement>(null);
  const isDraggingRef = useRef(false);
  const startXRef = useRef(0);
  const startScrollLeftRef = useRef(0);

  const thumbWidth = scrollWidth > 0 ? Math.max((clientWidth / scrollWidth) * clientWidth, 40) : 0;
  const maxThumbLeft = clientWidth - thumbWidth;
  const thumbLeft = scrollWidth > clientWidth 
    ? (scrollLeft / (scrollWidth - clientWidth)) * maxThumbLeft 
    : 0;

  const showScrollbar = scrollWidth > clientWidth;

  const handleMouseDown = useCallback((e: React.MouseEvent) => {
    e.preventDefault();
    isDraggingRef.current = true;
    startXRef.current = e.clientX;
    startScrollLeftRef.current = scrollLeft;

    const handleMouseMove = (e: MouseEvent) => {
      if (!isDraggingRef.current) return;
      const deltaX = e.clientX - startXRef.current;
      const scrollRatio = (scrollWidth - clientWidth) / maxThumbLeft;
      const newScrollLeft = Math.max(0, Math.min(scrollWidth - clientWidth, startScrollLeftRef.current + deltaX * scrollRatio));
      onScroll(newScrollLeft);
    };

    const handleMouseUp = () => {
      isDraggingRef.current = false;
      document.removeEventListener("mousemove", handleMouseMove);
      document.removeEventListener("mouseup", handleMouseUp);
    };

    document.addEventListener("mousemove", handleMouseMove);
    document.addEventListener("mouseup", handleMouseUp);
  }, [scrollLeft, scrollWidth, clientWidth, maxThumbLeft, onScroll]);

  const handleTrackClick = useCallback((e: React.MouseEvent) => {
    if (!trackRef.current) return;
    const rect = trackRef.current.getBoundingClientRect();
    const clickX = e.clientX - rect.left;
    const clickRatio = clickX / clientWidth;
    const newScrollLeft = Math.max(0, Math.min(scrollWidth - clientWidth, clickRatio * scrollWidth - clientWidth / 2));
    onScroll(newScrollLeft);
  }, [clientWidth, scrollWidth, onScroll]);

  if (!showScrollbar) return null;

  return (
    <div 
      ref={trackRef}
      className="h-3 bg-muted/30 rounded-md cursor-pointer relative"
      onClick={handleTrackClick}
    >
      <div
        className="absolute top-0.5 bottom-0.5 bg-muted-foreground/40 rounded-md cursor-grab active:cursor-grabbing hover:bg-muted-foreground/60 transition-colors"
        style={{
          left: thumbLeft,
          width: thumbWidth,
        }}
        onMouseDown={handleMouseDown}
        onClick={(e) => e.stopPropagation()}
      />
    </div>
  );
}

export function DataTable({ columns, rows, isLoading }: DataTableProps) {
  const tableContainerRef = useRef<HTMLDivElement>(null);
  const [scrollState, setScrollState] = useState({ scrollWidth: 0, clientWidth: 0, scrollLeft: 0 });

  const handleTableScroll = useCallback(() => {
    if (tableContainerRef.current) {
      setScrollState(prev => ({
        ...prev,
        scrollLeft: tableContainerRef.current!.scrollLeft,
      }));
    }
  }, []);

  const handleCustomScroll = useCallback((newScrollLeft: number) => {
    if (tableContainerRef.current) {
      tableContainerRef.current.scrollLeft = newScrollLeft;
      setScrollState(prev => ({ ...prev, scrollLeft: newScrollLeft }));
    }
  }, []);

  useLayoutEffect(() => {
    const tableContainer = tableContainerRef.current;
    if (!tableContainer) return;

    const updateDimensions = () => {
      setScrollState({
        scrollWidth: tableContainer.scrollWidth,
        clientWidth: tableContainer.clientWidth,
        scrollLeft: tableContainer.scrollLeft,
      });
    };

    updateDimensions();

    const resizeObserver = new ResizeObserver(updateDimensions);
    resizeObserver.observe(tableContainer);

    return () => {
      resizeObserver.disconnect();
    };
  }, [columns, rows]);

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
      <div className="px-2 pt-2">
        <CustomScrollbar
          scrollWidth={scrollState.scrollWidth}
          clientWidth={scrollState.clientWidth}
          scrollLeft={scrollState.scrollLeft}
          onScroll={handleCustomScroll}
        />
      </div>
      <div 
        ref={tableContainerRef} 
        className="overflow-x-auto scrollbar-always-visible"
        onScroll={handleTableScroll}
      >
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
