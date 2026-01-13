import { useState } from "react";
import { Plus, X, Filter, Clock, Trash2 } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover";
import { useQuery, useMutation } from "@tanstack/react-query";
import { queryClient, apiRequest } from "@/lib/queryClient";
import type { ColumnInfo, ActiveFilter, FilterHistoryEntry } from "@/lib/types";
import { OPERATOR_LABELS } from "@/lib/types";

interface DynamicFilterProps {
  columns: ColumnInfo[];
  activeFilters: ActiveFilter[];
  onApplyFilters: (filters: ActiveFilter[]) => void;
  database?: string;
  table?: string;
}

const OPERATORS = [
  { value: "eq", label: "Equals" },
  { value: "contains", label: "Contains" },
  { value: "gt", label: "Greater than" },
  { value: "gte", label: "Greater or equal" },
  { value: "lt", label: "Less than" },
  { value: "lte", label: "Less or equal" },
];

export function DynamicFilter({
  columns,
  activeFilters,
  onApplyFilters,
  database,
  table,
}: DynamicFilterProps) {
  const [isOpen, setIsOpen] = useState(false);
  const [selectedColumn, setSelectedColumn] = useState("");
  const [selectedOperator, setSelectedOperator] = useState("eq");
  const [filterValue, setFilterValue] = useState("");

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

  const handleAddFilter = () => {
    if (!selectedColumn || !filterValue.trim()) return;

    const newFilter: ActiveFilter = {
      column: selectedColumn,
      operator: selectedOperator as ActiveFilter["operator"],
      value: filterValue.trim(),
    };

    const newFilters = [...activeFilters, newFilter];
    onApplyFilters(newFilters);
    
    if (database && table && newFilters.length > 0) {
      saveHistoryMutation.mutate(newFilters);
    }
    
    setSelectedColumn("");
    setSelectedOperator("eq");
    setFilterValue("");
    setIsOpen(false);
  };

  const handleRemoveFilter = (index: number) => {
    const newFilters = activeFilters.filter((_, i) => i !== index);
    onApplyFilters(newFilters);
  };

  const handleClearAll = () => {
    onApplyFilters([]);
  };

  const handleApplyFromHistory = (entry: FilterHistoryEntry) => {
    onApplyFilters(entry.filters);
    if (database && table) {
      saveHistoryMutation.mutate(entry.filters);
    }
  };

  const formatFilterSummary = (filters: ActiveFilter[]): string => {
    return filters
      .slice(0, 2)
      .map((f) => `${f.column} ${OPERATOR_LABELS[f.operator]} "${typeof f.value === 'string' ? f.value : f.value.join('-')}"`)
      .join(", ") + (filters.length > 2 ? ` +${filters.length - 2}` : "");
  };

  return (
    <div className="flex items-center gap-2 flex-wrap">
      {filterHistory.length > 0 && (
        <Popover>
          <PopoverTrigger asChild>
            <Button variant="outline" size="sm" data-testid="button-recent-filters">
              <Clock className="h-4 w-4 mr-2" />
              Recent
            </Button>
          </PopoverTrigger>
          <PopoverContent className="w-80" align="start">
            <div className="space-y-2">
              <div className="text-sm font-medium mb-2">Recent Filters</div>
              {filterHistory.map((entry) => (
                <div
                  key={entry.id}
                  className="flex items-center justify-between gap-2 p-2 rounded-md hover:bg-muted"
                >
                  <button
                    className="flex-1 text-left text-sm truncate"
                    onClick={() => handleApplyFromHistory(entry)}
                    data-testid={`button-apply-history-${entry.id}`}
                  >
                    {formatFilterSummary(entry.filters)}
                  </button>
                  <Button
                    variant="ghost"
                    size="icon"
                    className="h-6 w-6 shrink-0"
                    onClick={() => deleteHistoryMutation.mutate(entry.id)}
                    data-testid={`button-delete-history-${entry.id}`}
                  >
                    <Trash2 className="h-3 w-3" />
                  </Button>
                </div>
              ))}
            </div>
          </PopoverContent>
        </Popover>
      )}

      <Popover open={isOpen} onOpenChange={setIsOpen}>
        <PopoverTrigger asChild>
          <Button variant="outline" size="sm" data-testid="button-add-filter">
            <Filter className="h-4 w-4 mr-2" />
            Filter
            {activeFilters.length > 0 && (
              <Badge variant="secondary" className="ml-2">
                {activeFilters.length}
              </Badge>
            )}
          </Button>
        </PopoverTrigger>
        <PopoverContent className="w-80" align="start">
          <div className="space-y-4">
            <div className="space-y-2">
              <label className="text-sm font-medium">Column</label>
              <Select value={selectedColumn} onValueChange={setSelectedColumn}>
                <SelectTrigger data-testid="select-filter-column">
                  <SelectValue placeholder="Select column..." />
                </SelectTrigger>
                <SelectContent>
                  {columns.map((col) => (
                    <SelectItem key={col.name} value={col.name}>
                      {col.name}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>

            <div className="space-y-2">
              <label className="text-sm font-medium">Operator</label>
              <Select value={selectedOperator} onValueChange={setSelectedOperator}>
                <SelectTrigger data-testid="select-filter-operator">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {OPERATORS.map((op) => (
                    <SelectItem key={op.value} value={op.value}>
                      {op.label}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>

            <div className="space-y-2">
              <label className="text-sm font-medium">Value</label>
              <Input
                value={filterValue}
                onChange={(e) => setFilterValue(e.target.value)}
                placeholder="Enter filter value..."
                onKeyDown={(e) => {
                  if (e.key === "Enter") handleAddFilter();
                }}
                data-testid="input-filter-value"
              />
            </div>

            <Button
              onClick={handleAddFilter}
              disabled={!selectedColumn || !filterValue.trim()}
              className="w-full"
              data-testid="button-apply-filter"
            >
              <Plus className="h-4 w-4 mr-2" />
              Add Filter
            </Button>
          </div>
        </PopoverContent>
      </Popover>

      {activeFilters.map((filter, idx) => (
        <Badge
          key={idx}
          variant="secondary"
          className="flex items-center gap-1 py-1 px-2"
        >
          <span className="font-medium">{filter.column}</span>
          <span className="text-muted-foreground">{OPERATOR_LABELS[filter.operator]}</span>
          <span>"{filter.value}"</span>
          <button
            onClick={() => handleRemoveFilter(idx)}
            className="ml-1 hover:text-destructive"
            data-testid={`button-remove-filter-${idx}`}
          >
            <X className="h-3 w-3" />
          </button>
        </Badge>
      ))}

      {activeFilters.length > 0 && (
        <Button
          variant="ghost"
          size="sm"
          onClick={handleClearAll}
          data-testid="button-clear-filters"
        >
          Clear all
        </Button>
      )}
    </div>
  );
}
