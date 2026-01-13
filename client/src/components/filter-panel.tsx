import { useState } from "react";
import { ChevronDown, ChevronUp, X, Clock, Trash2 } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Badge } from "@/components/ui/badge";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import { useQuery, useMutation } from "@tanstack/react-query";
import { queryClient, apiRequest } from "@/lib/queryClient";
import type { FilterDefinition, ActiveFilter, FilterHistoryEntry } from "@/lib/types";
import { OPERATOR_LABELS } from "@/lib/types";

interface FilterPanelProps {
  filterDefinitions: FilterDefinition[];
  activeFilters: ActiveFilter[];
  onApplyFilters: (filters: ActiveFilter[]) => void;
  onClearFilters: () => void;
  database?: string;
  table?: string;
}

export function FilterPanel({
  filterDefinitions,
  activeFilters,
  onApplyFilters,
  onClearFilters,
  database,
  table,
}: FilterPanelProps) {
  const [isOpen, setIsOpen] = useState(true);
  const [filterValues, setFilterValues] = useState<Record<string, string>>(() => {
    const initial: Record<string, string> = {};
    activeFilters.forEach((f) => {
      const def = filterDefinitions.find((d) => d.column === f.column && d.operator === f.operator);
      if (def) {
        initial[def.id] = typeof f.value === 'string' ? f.value : f.value.join(',');
      }
    });
    return initial;
  });

  const { data: filterHistory = [] } = useQuery<FilterHistoryEntry[]>({
    queryKey: ["/api/filters/history", database, table],
    enabled: !!database && !!table,
  });

  const saveHistoryMutation = useMutation({
    mutationFn: async (filters: ActiveFilter[]) => {
      await apiRequest("POST", "/api/filters/history", { database, table, filters });
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["/api/filters/history", database, table] });
    },
  });

  const deleteHistoryMutation = useMutation({
    mutationFn: async (id: string) => {
      await apiRequest("DELETE", `/api/filters/history/${id}`);
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["/api/filters/history", database, table] });
    },
  });

  const handleApply = () => {
    const filters: ActiveFilter[] = [];
    filterDefinitions.forEach((def) => {
      const value = filterValues[def.id]?.trim();
      if (value) {
        filters.push({
          column: def.column,
          operator: def.operator,
          value,
        });
      }
    });
    onApplyFilters(filters);
    
    if (filters.length > 0 && database && table) {
      saveHistoryMutation.mutate(filters);
    }
  };

  const handleClear = () => {
    setFilterValues({});
    onClearFilters();
  };

  const handleRemoveFilter = (defId: string) => {
    const newValues = { ...filterValues };
    delete newValues[defId];
    setFilterValues(newValues);
    
    const def = filterDefinitions.find((d) => d.id === defId);
    if (def) {
      const newFilters = activeFilters.filter(
        (f) => !(f.column === def.column && f.operator === def.operator)
      );
      onApplyFilters(newFilters);
    }
  };

  const handleApplyFromHistory = (entry: FilterHistoryEntry) => {
    const newValues: Record<string, string> = {};
    entry.filters.forEach((f) => {
      const def = filterDefinitions.find((d) => d.column === f.column && d.operator === f.operator);
      if (def) {
        newValues[def.id] = typeof f.value === 'string' ? f.value : f.value.join(',');
      }
    });
    setFilterValues(newValues);
    onApplyFilters(entry.filters);
    
    if (database && table) {
      saveHistoryMutation.mutate(entry.filters);
    }
  };

  const formatFilterSummary = (filters: ActiveFilter[]): string => {
    return filters
      .slice(0, 3)
      .map((f) => `${f.column} ${OPERATOR_LABELS[f.operator]} "${typeof f.value === 'string' ? f.value : f.value.join('-')}"`)
      .join(", ") + (filters.length > 3 ? ` +${filters.length - 3} more` : "");
  };

  if (filterDefinitions.length === 0) {
    return null;
  }

  return (
    <Collapsible open={isOpen} onOpenChange={setIsOpen} className="border rounded-lg bg-card">
      <CollapsibleTrigger asChild>
        <Button
          variant="ghost"
          className="w-full flex items-center justify-between px-4 py-3 h-auto"
          data-testid="button-toggle-filters"
        >
          <span className="font-medium">Filters</span>
          <div className="flex items-center gap-2">
            {activeFilters.length > 0 && (
              <Badge variant="secondary" className="text-xs">
                {activeFilters.length} active
              </Badge>
            )}
            {isOpen ? <ChevronUp className="h-4 w-4" /> : <ChevronDown className="h-4 w-4" />}
          </div>
        </Button>
      </CollapsibleTrigger>

      <CollapsibleContent>
        <div className="px-4 pb-4 space-y-4">
          {filterHistory.length > 0 && (
            <div className="space-y-2">
              <div className="flex items-center gap-2 text-sm text-muted-foreground">
                <Clock className="h-4 w-4" />
                <span>Recent Filters</span>
              </div>
              <div className="flex flex-wrap gap-2">
                {filterHistory.map((entry) => (
                  <div
                    key={entry.id}
                    className="flex items-center gap-1 bg-muted rounded-md px-2 py-1"
                  >
                    <Button
                      variant="ghost"
                      size="sm"
                      className="h-auto p-0 text-xs font-normal hover:bg-transparent hover:underline"
                      onClick={() => handleApplyFromHistory(entry)}
                      data-testid={`button-apply-history-${entry.id}`}
                    >
                      {formatFilterSummary(entry.filters)}
                    </Button>
                    <button
                      onClick={() => deleteHistoryMutation.mutate(entry.id)}
                      className="ml-1 text-muted-foreground hover:text-destructive"
                      data-testid={`button-delete-history-${entry.id}`}
                    >
                      <Trash2 className="h-3 w-3" />
                    </button>
                  </div>
                ))}
              </div>
            </div>
          )}

          {activeFilters.length > 0 && (
            <div className="flex flex-wrap gap-2">
              {activeFilters.map((filter, idx) => {
                const def = filterDefinitions.find(
                  (d) => d.column === filter.column && d.operator === filter.operator
                );
                return (
                  <Badge
                    key={idx}
                    variant="outline"
                    className="flex items-center gap-1 font-mono text-xs"
                  >
                    <span>{filter.column}</span>
                    <span className="text-muted-foreground">{OPERATOR_LABELS[filter.operator]}</span>
                    <span>"{typeof filter.value === 'string' ? filter.value : filter.value.join('-')}"</span>
                    {def && (
                      <button
                        onClick={() => handleRemoveFilter(def.id)}
                        className="ml-1 hover:text-destructive"
                        data-testid={`button-remove-filter-${idx}`}
                      >
                        <X className="h-3 w-3" />
                      </button>
                    )}
                  </Badge>
                );
              })}
            </div>
          )}

          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
            {filterDefinitions.map((def) => (
              <div key={def.id} className="space-y-2">
                <Label htmlFor={def.id} className="text-sm">
                  {def.name}
                  <span className="ml-2 text-xs text-muted-foreground">
                    ({OPERATOR_LABELS[def.operator]})
                  </span>
                </Label>
                <Input
                  id={def.id}
                  value={filterValues[def.id] || ""}
                  onChange={(e) =>
                    setFilterValues((prev) => ({ ...prev, [def.id]: e.target.value }))
                  }
                  placeholder={`Filter by ${def.column}...`}
                  data-testid={`input-filter-${def.id}`}
                />
              </div>
            ))}
          </div>

          <div className="flex gap-2">
            <Button onClick={handleApply} data-testid="button-apply-filters">
              Apply Filters
            </Button>
            <Button variant="outline" onClick={handleClear} data-testid="button-clear-filters">
              Clear All
            </Button>
          </div>
        </div>
      </CollapsibleContent>
    </Collapsible>
  );
}
