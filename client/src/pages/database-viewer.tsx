import { useState, useEffect, useCallback, useMemo } from "react";
import { useQuery, useMutation } from "@tanstack/react-query";
import { queryClient, apiRequest } from "@/lib/queryClient";
import { useToast } from "@/hooks/use-toast";
import { useAuth } from "@/hooks/use-auth";

import { Header } from "@/components/header";
import { TableSidebar } from "@/components/table-sidebar";
import { ControlBar } from "@/components/control-bar";
import { DynamicFilter } from "@/components/dynamic-filter";
import { NLQPanel } from "@/components/nlq-panel";
import { DataTable } from "@/components/data-table";
import { PaginationControls } from "@/components/pagination-controls";
import { AdminSettingsModal } from "@/components/admin-settings-modal";
import { ErrorBanner } from "@/components/error-banner";

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
  const [nlqEnabled, setNlqEnabled] = useState(false);
  const [localHiddenColumns, setLocalHiddenColumns] = useState<string[]>([]);

  // Fetch databases
  const { data: databases = [], isLoading: isLoadingDatabases } = useQuery<DatabaseConnection[]>({
    queryKey: ["/api/databases"],
  });

  // Fetch NLQ status
  useQuery({
    queryKey: ["/api/nlq/status"],
    queryFn: async () => {
      const res = await fetch("/api/nlq/status");
      const data = await res.json();
      setNlqEnabled(data.enabled);
      return data;
    },
  });

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
    queryKey: ["/api/rows", selectedDatabase, selectedTable, currentPage, activeFilters],
    queryFn: async () => {
      const res = await fetch("/api/rows", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          database: selectedDatabase,
          table: selectedTable,
          page: currentPage,
          filters: activeFilters,
        }),
      });
      if (!res.ok) {
        const data = await res.json();
        throw new Error(data.error || "Failed to fetch rows");
      }
      return res.json();
    },
    enabled: !!selectedDatabase && !!selectedTable,
  });

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

  // Reset state when database changes
  useEffect(() => {
    setSelectedTable("");
    setCurrentPage(1);
    setActiveFilters([]);
    setLastNLQPlan(null);
  }, [selectedDatabase]);

  // Reset page and filters when table changes
  useEffect(() => {
    setCurrentPage(1);
    setActiveFilters([]);
    setLastNLQPlan(null);
  }, [selectedTable]);

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

  const handleExport = useCallback(async () => {
    if (!selectedDatabase || !selectedTable) return;

    setIsExporting(true);
    try {
      const params = new URLSearchParams({
        database: selectedDatabase,
        table: selectedTable,
        page: String(currentPage),
      });

      if (activeFilters.length > 0) {
        params.set("filters", JSON.stringify(activeFilters));
      }

      const response = await fetch(`/api/export?${params}`);
      if (!response.ok) {
        throw new Error("Failed to export CSV");
      }

      const blob = await response.blob();
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = `${selectedTable.replace(".", "_")}_page${currentPage}.csv`;
      document.body.appendChild(a);
      a.click();
      window.URL.revokeObjectURL(url);
      document.body.removeChild(a);

      toast({
        title: "Export complete",
        description: "CSV file has been downloaded.",
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
  }, [selectedDatabase, selectedTable, currentPage, activeFilters, toast]);

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

        <main className="flex-1 flex flex-col overflow-hidden">
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

          <div className="flex-1 overflow-auto p-6 space-y-4">
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

            {selectedTable && visibleColumns.length > 0 && (
              <DynamicFilter
                columns={visibleColumns}
                activeFilters={activeFilters}
                onApplyFilters={handleApplyFilters}
              />
            )}

            <DataTable
              columns={visibleColumns}
              rows={queryResult?.rows || []}
              isLoading={isLoadingRows}
            />
          </div>

          {selectedTable && queryResult && queryResult.totalCount > 0 && (
            <PaginationControls
              currentPage={currentPage}
              totalPages={queryResult.totalPages}
              totalCount={queryResult.totalCount}
              pageSize={queryResult.pageSize}
              onPageChange={handlePageChange}
              isLoading={isLoadingRows}
            />
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
    </div>
  );
}
