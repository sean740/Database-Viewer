import { useState } from "react";
import { ChevronDown, ChevronUp, X } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Badge } from "@/components/ui/badge";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import type { FilterDefinition, ActiveFilter } from "@/lib/types";
import { OPERATOR_LABELS } from "@/lib/types";

interface FilterPanelProps {
  filterDefinitions: FilterDefinition[];
  activeFilters: ActiveFilter[];
  onApplyFilters: (filters: ActiveFilter[]) => void;
  onClearFilters: () => void;
}

export function FilterPanel({
  filterDefinitions,
  activeFilters,
  onApplyFilters,
  onClearFilters,
}: FilterPanelProps) {
  const [isOpen, setIsOpen] = useState(true);
  const [filterValues, setFilterValues] = useState<Record<string, string>>(() => {
    const initial: Record<string, string> = {};
    activeFilters.forEach((f) => {
      const def = filterDefinitions.find((d) => d.column === f.column && d.operator === f.operator);
      if (def) {
        initial[def.id] = f.value;
      }
    });
    return initial;
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
                    <span>"{filter.value}"</span>
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
