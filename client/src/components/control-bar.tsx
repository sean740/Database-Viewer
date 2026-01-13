import { RefreshCw, Download, Settings, Loader2 } from "lucide-react";
import { Button } from "@/components/ui/button";
import { ColumnSelector } from "@/components/column-selector";
import type { ColumnInfo } from "@/lib/types";

interface ControlBarProps {
  selectedTable: string;
  totalCount: number;
  currentPage: number;
  totalPages: number;
  onReload: () => void;
  onExport: () => void;
  onOpenSettings: () => void;
  isLoading: boolean;
  isExporting: boolean;
  columns: ColumnInfo[];
  hiddenColumns: string[];
  onSaveColumns: (hiddenColumns: string[]) => void;
  onLocalColumnsChange?: (hiddenColumns: string[]) => void;
  isSavingColumns?: boolean;
}

export function ControlBar({
  selectedTable,
  totalCount,
  currentPage,
  totalPages,
  onReload,
  onExport,
  onOpenSettings,
  isLoading,
  isExporting,
  columns,
  hiddenColumns,
  onSaveColumns,
  onLocalColumnsChange,
  isSavingColumns = false,
}: ControlBarProps) {
  return (
    <div className="flex items-center justify-between px-4 py-3 border-b bg-card gap-4">
      <div className="flex items-center gap-4">
        {selectedTable && (
          <>
            <h2 className="font-mono text-lg font-medium">{selectedTable}</h2>
            <span className="text-sm text-muted-foreground">
              Page {currentPage} of {totalPages} ({totalCount.toLocaleString()} total rows)
            </span>
          </>
        )}
      </div>

      <div className="flex items-center gap-2">
        <ColumnSelector
          columns={columns}
          hiddenColumns={hiddenColumns}
          onSave={onSaveColumns}
          onLocalChange={onLocalColumnsChange}
          isSaving={isSavingColumns}
          disabled={!selectedTable}
        />

        <Button
          variant="outline"
          size="sm"
          onClick={onReload}
          disabled={isLoading || !selectedTable}
          data-testid="button-reload"
        >
          {isLoading ? (
            <Loader2 className="h-4 w-4 animate-spin" />
          ) : (
            <RefreshCw className="h-4 w-4" />
          )}
          <span className="ml-1">Reload</span>
        </Button>

        <Button
          variant="outline"
          size="sm"
          onClick={onExport}
          disabled={isExporting || !selectedTable}
          data-testid="button-export-csv"
        >
          {isExporting ? (
            <Loader2 className="h-4 w-4 animate-spin" />
          ) : (
            <Download className="h-4 w-4" />
          )}
          <span className="ml-1">Export CSV</span>
        </Button>

        <Button
          variant="ghost"
          size="icon"
          onClick={onOpenSettings}
          disabled={!selectedTable}
          data-testid="button-admin-settings"
          aria-label="Admin Settings"
        >
          <Settings className="h-4 w-4" />
        </Button>
      </div>
    </div>
  );
}
