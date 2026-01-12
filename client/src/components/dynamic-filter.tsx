import { useState } from "react";
import { Plus, X, Filter } from "lucide-react";
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
import type { ColumnInfo, ActiveFilter } from "@/lib/types";
import { OPERATOR_LABELS } from "@/lib/types";

interface DynamicFilterProps {
  columns: ColumnInfo[];
  activeFilters: ActiveFilter[];
  onApplyFilters: (filters: ActiveFilter[]) => void;
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
}: DynamicFilterProps) {
  const [isOpen, setIsOpen] = useState(false);
  const [selectedColumn, setSelectedColumn] = useState("");
  const [selectedOperator, setSelectedOperator] = useState("eq");
  const [filterValue, setFilterValue] = useState("");

  const handleAddFilter = () => {
    if (!selectedColumn || !filterValue.trim()) return;

    const newFilter: ActiveFilter = {
      column: selectedColumn,
      operator: selectedOperator as ActiveFilter["operator"],
      value: filterValue.trim(),
    };

    onApplyFilters([...activeFilters, newFilter]);
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

  return (
    <div className="flex items-center gap-2 flex-wrap">
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
