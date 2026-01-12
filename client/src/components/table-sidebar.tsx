import { useState } from "react";
import { Search, Table2, Loader2 } from "lucide-react";
import { Input } from "@/components/ui/input";
import { ScrollArea } from "@/components/ui/scroll-area";
import { cn } from "@/lib/utils";
import type { TableInfo } from "@/lib/types";

interface TableSidebarProps {
  tables: TableInfo[];
  selectedTable: string;
  onTableSelect: (fullName: string) => void;
  isLoading: boolean;
}

export function TableSidebar({
  tables,
  selectedTable,
  onTableSelect,
  isLoading,
}: TableSidebarProps) {
  const [search, setSearch] = useState("");

  const filteredTables = tables.filter((t) =>
    t.fullName.toLowerCase().includes(search.toLowerCase())
  );

  return (
    <aside className="w-64 border-r bg-sidebar flex flex-col h-full">
      <div className="p-4 border-b border-sidebar-border">
        <div className="relative">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
          <Input
            type="search"
            placeholder="Search tables..."
            className="pl-9"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            data-testid="input-search-tables"
          />
        </div>
      </div>

      <ScrollArea className="flex-1">
        <div className="p-2">
          {isLoading ? (
            <div className="flex items-center justify-center py-8">
              <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
            </div>
          ) : filteredTables.length === 0 ? (
            <div className="text-center py-8 text-muted-foreground text-sm">
              {tables.length === 0
                ? "Select a database to view tables"
                : "No tables found"}
            </div>
          ) : (
            <div className="space-y-1">
              {filteredTables.map((table) => (
                <button
                  key={table.fullName}
                  onClick={() => onTableSelect(table.fullName)}
                  className={cn(
                    "w-full flex items-center gap-2 px-3 py-2 rounded-md text-sm text-left transition-colors hover-elevate",
                    selectedTable === table.fullName
                      ? "bg-sidebar-accent text-sidebar-accent-foreground"
                      : "text-sidebar-foreground"
                  )}
                  data-testid={`button-table-${table.fullName}`}
                >
                  <Table2 className="h-4 w-4 shrink-0" />
                  <span className="font-mono text-xs truncate">{table.fullName}</span>
                </button>
              ))}
            </div>
          )}
        </div>
      </ScrollArea>

      <div className="p-4 border-t border-sidebar-border text-xs text-muted-foreground">
        {tables.length} table{tables.length !== 1 ? "s" : ""} available
      </div>
    </aside>
  );
}
