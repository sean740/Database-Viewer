import { useState } from "react";
import { Search, Table2, Loader2, EyeOff, FileText, BarChart3 } from "lucide-react";
import { Link } from "wouter";
import { Input } from "@/components/ui/input";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";
import type { TableInfo } from "@/lib/types";
import { useAuth } from "@/hooks/use-auth";

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
  const { user } = useAuth();
  const isAdmin = user?.role === "admin";

  const filteredTables = tables.filter((t) => {
    const searchLower = search.toLowerCase();
    return (
      t.fullName.toLowerCase().includes(searchLower) ||
      (t.displayName && t.displayName.toLowerCase().includes(searchLower))
    );
  });

  return (
    <aside className="w-64 border-r bg-sidebar flex flex-col h-full">
      <div className="p-4 border-b border-sidebar-border space-y-3">
        <Link href="/my-reports">
          <Button variant="outline" className="w-full gap-2" data-testid="link-my-reports">
            <FileText className="h-4 w-4" />
            My Reports
          </Button>
        </Link>
        <Link href="/weekly-performance">
          <Button variant="outline" className="w-full gap-2" data-testid="link-weekly-performance">
            <BarChart3 className="h-4 w-4" />
            Weekly Dashboard
          </Button>
        </Link>
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
              {filteredTables.map((table) => {
                const isHidden = table.isVisible === false;
                return (
                  <button
                    key={table.fullName}
                    onClick={() => onTableSelect(table.fullName)}
                    className={cn(
                      "w-full flex flex-col gap-0.5 px-3 py-2 rounded-md text-sm text-left transition-colors hover-elevate",
                      selectedTable === table.fullName
                        ? "bg-sidebar-accent text-sidebar-accent-foreground"
                        : "text-sidebar-foreground",
                      isHidden && isAdmin && "opacity-60"
                    )}
                    data-testid={`button-table-${table.fullName}`}
                  >
                    <div className="flex items-center gap-2">
                      <Table2 className="h-4 w-4 shrink-0" />
                      <span className={cn("text-xs truncate", isHidden && isAdmin && "line-through")}>
                        {table.displayName || table.fullName}
                      </span>
                      {isHidden && isAdmin && (
                        <Badge variant="outline" className="text-[10px] px-1 py-0 h-4 shrink-0">
                          <EyeOff className="h-3 w-3 mr-0.5" />
                          Hidden
                        </Badge>
                      )}
                    </div>
                    {table.displayName && (
                      <span className="font-mono text-[10px] text-muted-foreground truncate ml-6">
                        {table.fullName}
                      </span>
                    )}
                  </button>
                );
              })}
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
