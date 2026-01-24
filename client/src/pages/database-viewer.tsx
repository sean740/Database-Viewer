import { useState, useEffect, useCallback, useMemo, useRef } from "react";
import { useQuery, useMutation, keepPreviousData } from "@tanstack/react-query";
import { queryClient, apiRequest } from "@/lib/queryClient";
import { useToast } from "@/hooks/use-toast";
import { useAuth } from "@/hooks/use-auth";

import { Header } from "@/components/header";
import { TableSidebar } from "@/components/table-sidebar";
import { ControlBar } from "@/components/control-bar";
import { DynamicFilter } from "@/components/dynamic-filter";
import { NLQPanel } from "@/components/nlq-panel";
import { DataTable, type SortConfig } from "@/components/data-table";
import { PaginationControls } from "@/components/pagination-controls";
import { AdminSettingsModal } from "@/components/admin-settings-modal";
import { ErrorBanner } from "@/components/error-banner";
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog";
import { AlertTriangle, ShieldAlert } from "lucide-react";

import type {
  DatabaseConnection,
  TableInfo,
  ColumnInfo,
  FilterDefinition,
  ActiveFilter,
  QueryResponse,
  NLQPlan,
} from "@/lib/types";

interface TableSettingsMap {
  [key: string]: {
    isVisible: boolean;
    displayName: string | null;
    hiddenColumns?: string[];
  };
}

export default function DatabaseViewer() {
  const { toast } = useToast();
  const { user } = useAuth();
  const isAdmin = user?.role === "admin";

  // State
  const [selectedDatabase, setSelectedDatabase] = useState("");
  const [selectedTable, setSelectedTable] = useState("");
  const [currentPage, setCurrentPage] = useState(1);
  const [activeFilters, setActiveFilters] = useState<ActiveFilter[]>([]);
  const [isSettingsOpen, setIsSettingsOpen] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [isExporting, setIsExporting] = useState(false);
  const [lastNLQPlan, setLastNLQPlan] = useState<NLQPlan | null>(null);
  const [localHiddenColumns, setLocalHiddenColumns] = useState<string[]>([]);
  const [sort, setSort] = useState<SortConfig | null>(null);
  
  // Per-table state storage to preserve filters/page/NLQ/sort when switching tables
  const [tableStateCache, setTableStateCache] = useState<Record<string, {
    filters: ActiveFilter[];
    page: number;
    nlqPlan: NLQPlan | null;
    sort: SortConfig | null;
  }>>({})
  
  // Export dialog states
  const [exportDialogOpen, setExportDialogOpen] = useState(false);
  const [exportDialogType, setExportDialogType] = useState<"warning" | "blocked" | "limit">("warning");
  const [exportRowCount, setExportRowCount] = useState(0);
  const [exportMaxRows, setExportMaxRows] = useState(0);

  // Fetch databases
  const { data: databases = [], isLoading: isLoadingDatabases } = useQuery<DatabaseConnection[]>({
    queryKey: ["/api/databases"],
  });

  // Fetch NLQ status
  const { data: nlqStatus } = useQuery<{ enabled: boolean }>({
    queryKey: ["/api/nlq/status"],
  });
  const nlqEnabled = nlqStatus?.enabled ?? false;

  // Fetch tables when database changes
  const { data: tables = [], isLoading: isLoadingTables } = useQuery<TableInfo[]>({
    queryKey: ["/api/tables", selectedDatabase],
    enabled: !!selectedDatabase,
  });

  // Fetch columns when table changes
  const { data: columns = [], isLoading: isLoadingColumns } = useQuery<ColumnInfo[]>({
    queryKey: ["/api/columns", selectedDatabase, selectedTable],
    enabled: !!selectedDatabase && !!selectedTable,
  });

  // Fetch filter definitions for selected table
  const { data: filterDefinitions = [] } = useQuery<FilterDefinition[]>({
    queryKey: ["/api/filters", selectedTable],
    enabled: !!selectedTable,
  });

  // Fetch table settings (for column visibility) - all users can access read-only settings
  const { data: tableSettings = {} } = useQuery<TableSettingsMap>({
    queryKey: ["/api/table-settings"],
  });

  // Get hidden columns for current table (admin settings)
  const settingsKey = selectedDatabase && selectedTable ? `${selectedDatabase}:${selectedTable}` : "";
  const currentTableSettings = tableSettings[settingsKey];
  const adminHiddenColumns = currentTableSettings?.hiddenColumns || [];

  // Combined hidden columns: admin settings + local user preferences
  const effectiveHiddenColumns = useMemo(() => {
    const combined = new Set([...adminHiddenColumns, ...localHiddenColumns]);
    return Array.from(combined);
  }, [adminHiddenColumns, localHiddenColumns]);

  // Filter columns to only show visible ones
  const visibleColumns = useMemo(() => {
    if (effectiveHiddenColumns.length === 0) return columns;
    return columns.filter((col) => !effectiveHiddenColumns.includes(col.name));
  }, [columns, effectiveHiddenColumns]);

  // Reset local hidden columns when table changes
  useEffect(() => {
    setLocalHiddenColumns([]);
  }, [selectedTable]);

  // Mutation to save column visibility
  const saveColumnsMutation = useMutation({
    mutationFn: async (newHiddenColumns: string[]) => {
      return apiRequest("POST", "/api/admin/table-settings", {
        database: selectedDatabase,
        tableName: selectedTable,
        isVisible: currentTableSettings?.isVisible !== false,
        displayName: currentTableSettings?.displayName || null,
        hiddenColumns: newHiddenColumns,
      });
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["/api/table-settings"] });
      queryClient.invalidateQueries({ queryKey: ["/api/admin/table-settings"] });
      toast({
        title: "Column visibility saved",
        description: "The column settings have been updated.",
      });
    },
    onError: (err) => {
      toast({
        title: "Failed to save columns",
        description: err instanceof Error ? err.message : "An error occurred",
        variant: "destructive",
      });
    },
  });

  // Fetch rows
  const {
    data: queryResult,
    isLoading: isLoadingRows,
    refetch: refetchRows,
  } = useQuery<QueryResponse>({
    queryKey: ["/api/rows", selectedDatabase, selectedTable, currentPage, activeFilters, sort],
    queryFn: async () => {
      const res = await fetch("/api/rows", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          database: selectedDatabase,
          table: selectedTable,
          page: currentPage,
          filters: activeFilters,
          sort: sort,
        }),
      });
      if (!res.ok) {
        const data = await res.json();
        throw new Error(data.error || "Failed to fetch rows");
      }
      return res.json();
    },
    enabled: !!selectedDatabase && !!selectedTable,
    placeholderData: keepPreviousData,
  });

  // Handle column sort
  const handleSort = useCallback((column: string) => {
    setSort((prevSort) => {
      if (prevSort?.column === column) {
        // Toggle direction or clear sort
        if (prevSort.direction === "asc") {
          return { column, direction: "desc" };
        } else {
          return null; // Clear sort on third click
        }
      } else {
        // New column, start with ascending
        return { column, direction: "asc" };
      }
    });
    setCurrentPage(1); // Reset to page 1 when sorting changes
  }, []);

  // Save filter definitions mutation
  const saveFiltersMutation = useMutation({
    mutationFn: async (filters: FilterDefinition[]) => {
      return apiRequest("POST", "/api/filters", {
        table: selectedTable,
        filters,
      });
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["/api/filters", selectedTable] });
      toast({
        title: "Filters saved",
        description: "Filter definitions have been updated.",
      });
    },
    onError: (err) => {
      toast({
        title: "Failed to save filters",
        description: err instanceof Error ? err.message : "An error occurred",
        variant: "destructive",
      });
    },
  });

  // Use refs to track current values for saving before table switch
  const filtersRef = useRef(activeFilters);
  const pageRef = useRef(currentPage);
  const nlqPlanRef = useRef(lastNLQPlan);
  const sortRef = useRef(sort);
  const prevTableRef = useRef("");
  
  // Keep refs up to date
  useEffect(() => { filtersRef.current = activeFilters; }, [activeFilters]);
  useEffect(() => { pageRef.current = currentPage; }, [currentPage]);
  useEffect(() => { nlqPlanRef.current = lastNLQPlan; }, [lastNLQPlan]);
  useEffect(() => { sortRef.current = sort; }, [sort]);
  
  // Save current table state before switching and restore when returning
  useEffect(() => {
    const prevTable = prevTableRef.current;
    
    // Save state from previous table (if any) using refs for current values
    if (prevTable && prevTable !== selectedTable) {
      setTableStateCache(prev => ({
        ...prev,
        [prevTable]: {
          filters: filtersRef.current,
          page: pageRef.current,
          nlqPlan: nlqPlanRef.current,
          sort: sortRef.current,
        }
      }));
    }
    
    // Restore or reset state for new table
    if (selectedTable) {
      const cached = tableStateCache[selectedTable];
      if (cached) {
        setActiveFilters(cached.filters);
        setCurrentPage(cached.page);
        setLastNLQPlan(cached.nlqPlan);
        setSort(cached.sort);
      } else {
        setCurrentPage(1);
        setActiveFilters([]);
        setLastNLQPlan(null);
        setSort(null);
      }
    }
    
    prevTableRef.current = selectedTable;
  }, [selectedTable]);

  // Reset state when database changes (clear cache for that database)
  useEffect(() => {
    setSelectedTable("");
    setCurrentPage(1);
    setActiveFilters([]);
    setLastNLQPlan(null);
    setSort(null);
    // Clear table cache when database changes
    setTableStateCache({});
  }, [selectedDatabase]);

  // Auto-select first database if only one
  useEffect(() => {
    if (databases.length === 1 && !selectedDatabase) {
      setSelectedDatabase(databases[0].name);
    }
  }, [databases, selectedDatabase]);

  const handleDatabaseChange = useCallback((name: string) => {
    setSelectedDatabase(name);
    setError(null);
  }, []);

  const handleTableSelect = useCallback((fullName: string) => {
    setSelectedTable(fullName);
    setError(null);
  }, []);

  const handleApplyFilters = useCallback((filters: ActiveFilter[]) => {
    setActiveFilters(filters);
    setCurrentPage(1);
  }, []);

  const handleClearFilters = useCallback(() => {
    setActiveFilters([]);
    setCurrentPage(1);
  }, []);

  const handlePageChange = useCallback((page: number) => {
    setCurrentPage(page);
  }, []);

  const handleReload = useCallback(() => {
    refetchRows();
  }, [refetchRows]);

  // Perform the actual export download
  const performExport = useCallback(async (rowCount?: number) => {
    if (!selectedDatabase || !selectedTable) return;

    setIsExporting(true);
    try {
      const params = new URLSearchParams({
        database: selectedDatabase,
        table: selectedTable,
        exportAll: "true",
      });

      if (activeFilters.length > 0) {
        params.set("filters", JSON.stringify(activeFilters));
      }

      const response = await fetch(`/api/export?${params}`);
      if (!response.ok) {
        const data = await response.json().catch(() => ({ error: "Failed to export CSV" }));
        throw new Error(data.error || "Failed to export CSV");
      }

      const blob = await response.blob();
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = `${selectedTable.replace(".", "_")}_export.csv`;
      document.body.appendChild(a);
      a.click();
      window.URL.revokeObjectURL(url);
      document.body.removeChild(a);

      // Use passed rowCount or fall back to state
      const displayCount = rowCount ?? exportRowCount;
      toast({
        title: "Export complete",
        description: `${displayCount.toLocaleString()} rows have been exported.`,
      });
    } catch (err) {
      toast({
        title: "Export failed",
        description: err instanceof Error ? err.message : "An error occurred",
        variant: "destructive",
      });
    } finally {
      setIsExporting(false);
    }
  }, [selectedDatabase, selectedTable, activeFilters, toast, exportRowCount]);

  // Handle export button click - check limits first
  const handleExport = useCallback(async () => {
    if (!selectedDatabase || !selectedTable) return;

    setIsExporting(true);
    try {
      // First, check the row count and limits
      const response = await fetch("/api/export/check", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          database: selectedDatabase,
          table: selectedTable,
          filters: activeFilters,
        }),
      });

      if (!response.ok) {
        const data = await response.json().catch(() => ({ error: "Failed to check export" }));
        throw new Error(data.error || "Failed to check export");
      }

      const check = await response.json();
      setExportRowCount(check.totalCount);
      setExportMaxRows(check.maxRowsForRole);

      // If exceeds absolute maximum (50,000), show limit dialog
      if (check.totalCount > 50000) {
        setExportDialogType("limit");
        setExportDialogOpen(true);
        setIsExporting(false);
        return;
      }

      // If exceeds role-based limit (10,000 for non-admins), show blocked dialog
      if (check.exceedsLimit) {
        setExportDialogType("blocked");
        setExportDialogOpen(true);
        setIsExporting(false);
        return;
      }

      // If needs warning (>2,000 rows), show warning dialog
      if (check.needsWarning) {
        setExportDialogType("warning");
        setExportDialogOpen(true);
        setIsExporting(false);
        return;
      }

      // Otherwise, proceed with export directly - pass row count to avoid stale state
      await performExport(check.totalCount);
    } catch (err) {
      toast({
        title: "Export failed",
        description: err instanceof Error ? err.message : "An error occurred",
        variant: "destructive",
      });
      setIsExporting(false);
    }
  }, [selectedDatabase, selectedTable, activeFilters, toast, performExport]);

  // Handle export confirmation from dialog
  const handleExportConfirm = useCallback(async () => {
    setExportDialogOpen(false);
    // Use the stored exportRowCount from when dialog was opened
    await performExport(exportRowCount);
  }, [performExport, exportRowCount]);

  const handleNLQParsed = useCallback((plan: NLQPlan) => {
    setLastNLQPlan(plan);
    
    // Apply the parsed plan
    if (plan.table !== selectedTable) {
      setSelectedTable(plan.table);
    }
    
    const newFilters: ActiveFilter[] = plan.filters.map((f) => ({
      column: f.column,
      operator: f.op,
      value: f.value,
    }));
    
    setActiveFilters(newFilters);
    setCurrentPage(plan.page);
  }, [selectedTable]);

  const handleSaveFilters = useCallback(
    async (filters: FilterDefinition[]) => {
      await saveFiltersMutation.mutateAsync(filters);
    },
    [saveFiltersMutation]
  );

  const handleSaveColumns = useCallback(
    (newHiddenColumns: string[]) => {
      saveColumnsMutation.mutate(newHiddenColumns);
    },
    [saveColumnsMutation]
  );

  const isLoading = isLoadingDatabases || isLoadingTables || isLoadingColumns || isLoadingRows;

  return (
    <div className="h-screen flex flex-col bg-background">
      <Header
        databases={databases}
        selectedDatabase={selectedDatabase}
        onDatabaseChange={handleDatabaseChange}
        isLoading={isLoadingDatabases}
      />

      {error && (
        <ErrorBanner message={error} onDismiss={() => setError(null)} />
      )}

      <div className="flex-1 flex overflow-hidden">
        <TableSidebar
          tables={tables}
          selectedTable={selectedTable}
          onTableSelect={handleTableSelect}
          isLoading={isLoadingTables}
        />

        <main className="flex-1 flex flex-col min-h-0 overflow-hidden">
          {selectedTable && (
            <ControlBar
              selectedTable={selectedTable}
              totalCount={queryResult?.totalCount || 0}
              currentPage={currentPage}
              totalPages={queryResult?.totalPages || 1}
              onReload={handleReload}
              onExport={handleExport}
              onOpenSettings={() => setIsSettingsOpen(true)}
              isLoading={isLoadingRows}
              isExporting={isExporting}
              columns={columns}
              hiddenColumns={effectiveHiddenColumns}
              onSaveColumns={handleSaveColumns}
              onLocalColumnsChange={setLocalHiddenColumns}
              isSavingColumns={saveColumnsMutation.isPending}
            />
          )}

          <div className="flex-1 flex flex-col min-h-0 overflow-hidden p-6 gap-4">
            <div className="shrink-0">
              {nlqEnabled && selectedDatabase && (
                <NLQPanel
                  isEnabled={nlqEnabled}
                  selectedDatabase={selectedDatabase}
                  selectedTable={selectedTable}
                  onQueryParsed={handleNLQParsed}
                  lastPlan={lastNLQPlan}
                  resultCount={queryResult?.totalCount}
                  isLoadingResults={isLoadingRows}
                />
              )}
            </div>

            <div className="shrink-0">
              {selectedTable && visibleColumns.length > 0 && (
                <DynamicFilter
                  columns={visibleColumns}
                  activeFilters={activeFilters}
                  onApplyFilters={handleApplyFilters}
                  database={selectedDatabase}
                  table={selectedTable}
                />
              )}
            </div>

            <div className="flex-1 min-h-0 overflow-hidden">
              <DataTable
                columns={visibleColumns}
                rows={queryResult?.rows || []}
                isLoading={isLoadingRows}
                sort={sort}
                onSort={handleSort}
              />
            </div>
          </div>

          {selectedTable && queryResult && queryResult.totalCount > 0 && (
            <div className="shrink-0">
              <PaginationControls
                currentPage={currentPage}
                totalPages={queryResult.totalPages}
                totalCount={queryResult.totalCount}
                pageSize={queryResult.pageSize}
                onPageChange={handlePageChange}
                isLoading={isLoadingRows}
              />
            </div>
          )}
        </main>
      </div>

      <AdminSettingsModal
        isOpen={isSettingsOpen}
        onClose={() => setIsSettingsOpen(false)}
        tableName={selectedTable}
        columns={columns}
        filterDefinitions={filterDefinitions}
        onSave={handleSaveFilters}
      />

      <AlertDialog open={exportDialogOpen} onOpenChange={setExportDialogOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            {exportDialogType === "warning" && (
              <>
                <AlertDialogTitle className="flex items-center gap-2">
                  <AlertTriangle className="h-5 w-5 text-yellow-500" />
                  Large Export Warning
                </AlertDialogTitle>
                <AlertDialogDescription>
                  You are about to export <strong>{exportRowCount.toLocaleString()}</strong> rows.
                  This may take a moment to download. Do you want to continue?
                </AlertDialogDescription>
              </>
            )}
            {exportDialogType === "blocked" && (
              <>
                <AlertDialogTitle className="flex items-center gap-2">
                  <ShieldAlert className="h-5 w-5 text-destructive" />
                  Export Limit Exceeded
                </AlertDialogTitle>
                <AlertDialogDescription>
                  This export contains <strong>{exportRowCount.toLocaleString()}</strong> rows,
                  which exceeds your limit of <strong>{exportMaxRows.toLocaleString()}</strong> rows.
                  <br /><br />
                  Please contact an administrator if you need to export larger datasets.
                </AlertDialogDescription>
              </>
            )}
            {exportDialogType === "limit" && (
              <>
                <AlertDialogTitle className="flex items-center gap-2">
                  <ShieldAlert className="h-5 w-5 text-destructive" />
                  Maximum Export Limit
                </AlertDialogTitle>
                <AlertDialogDescription>
                  This export contains <strong>{exportRowCount.toLocaleString()}</strong> rows,
                  which exceeds the maximum export limit of <strong>50,000</strong> rows.
                  <br /><br />
                  Please apply filters to reduce the number of rows before exporting.
                </AlertDialogDescription>
              </>
            )}
          </AlertDialogHeader>
          <AlertDialogFooter>
            {exportDialogType === "warning" ? (
              <>
                <AlertDialogCancel data-testid="button-export-cancel">Cancel</AlertDialogCancel>
                <AlertDialogAction onClick={handleExportConfirm} data-testid="button-export-confirm">
                  Export {exportRowCount.toLocaleString()} rows
                </AlertDialogAction>
              </>
            ) : (
              <AlertDialogAction onClick={() => setExportDialogOpen(false)} data-testid="button-export-ok">
                OK
              </AlertDialogAction>
            )}
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  );
}
