import { useState } from "react";
import { Plus, Trash2, Save, X, Loader2 } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogFooter,
} from "@/components/ui/dialog";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Card } from "@/components/ui/card";
import type { FilterDefinition, ColumnInfo, FilterOperator } from "@/lib/types";
import { OPERATOR_LABELS } from "@/lib/types";

interface AdminSettingsModalProps {
  isOpen: boolean;
  onClose: () => void;
  tableName: string;
  columns: ColumnInfo[];
  filterDefinitions: FilterDefinition[];
  onSave: (filters: FilterDefinition[]) => Promise<void>;
}

const OPERATORS: FilterOperator[] = ["eq", "contains", "gt", "gte", "lt", "lte"];

export function AdminSettingsModal({
  isOpen,
  onClose,
  tableName,
  columns,
  filterDefinitions,
  onSave,
}: AdminSettingsModalProps) {
  const [filters, setFilters] = useState<FilterDefinition[]>(filterDefinitions);
  const [isSaving, setIsSaving] = useState(false);

  const handleAddFilter = () => {
    const newFilter: FilterDefinition = {
      id: `filter-${Date.now()}`,
      name: "",
      column: columns[0]?.name || "",
      operator: "eq",
    };
    setFilters([...filters, newFilter]);
  };

  const handleRemoveFilter = (id: string) => {
    setFilters(filters.filter((f) => f.id !== id));
  };

  const handleUpdateFilter = (id: string, updates: Partial<FilterDefinition>) => {
    setFilters(
      filters.map((f) => (f.id === id ? { ...f, ...updates } : f))
    );
  };

  const handleSave = async () => {
    setIsSaving(true);
    try {
      const validFilters = filters.filter(
        (f) => f.name.trim() && f.column && f.operator
      );
      await onSave(validFilters);
      onClose();
    } catch (error) {
      console.error("Failed to save filters:", error);
    } finally {
      setIsSaving(false);
    }
  };

  const handleCancel = () => {
    setFilters(filterDefinitions);
    onClose();
  };

  return (
    <Dialog open={isOpen} onOpenChange={handleCancel}>
      <DialogContent className="max-w-2xl max-h-[80vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            Filter Definitions for{" "}
            <span className="font-mono text-primary">{tableName}</span>
          </DialogTitle>
        </DialogHeader>

        <div className="space-y-4 py-4">
          {filters.length === 0 ? (
            <div className="text-center py-8 text-muted-foreground">
              No filters configured. Click "Add Filter" to create one.
            </div>
          ) : (
            <div className="space-y-3">
              {filters.map((filter) => (
                <Card key={filter.id} className="p-4">
                  <div className="grid grid-cols-1 md:grid-cols-4 gap-3 items-end">
                    <div className="space-y-2">
                      <Label htmlFor={`name-${filter.id}`}>Label</Label>
                      <Input
                        id={`name-${filter.id}`}
                        value={filter.name}
                        onChange={(e) =>
                          handleUpdateFilter(filter.id, { name: e.target.value })
                        }
                        placeholder="Filter name..."
                        data-testid={`input-filter-name-${filter.id}`}
                      />
                    </div>

                    <div className="space-y-2">
                      <Label htmlFor={`column-${filter.id}`}>Column</Label>
                      <Select
                        value={filter.column}
                        onValueChange={(value) =>
                          handleUpdateFilter(filter.id, { column: value })
                        }
                      >
                        <SelectTrigger
                          id={`column-${filter.id}`}
                          data-testid={`select-filter-column-${filter.id}`}
                        >
                          <SelectValue placeholder="Select column" />
                        </SelectTrigger>
                        <SelectContent>
                          {columns.map((col) => (
                            <SelectItem key={col.name} value={col.name}>
                              <span className="font-mono text-sm">{col.name}</span>
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>

                    <div className="space-y-2">
                      <Label htmlFor={`operator-${filter.id}`}>Operator</Label>
                      <Select
                        value={filter.operator}
                        onValueChange={(value) =>
                          handleUpdateFilter(filter.id, {
                            operator: value as FilterOperator,
                          })
                        }
                      >
                        <SelectTrigger
                          id={`operator-${filter.id}`}
                          data-testid={`select-filter-operator-${filter.id}`}
                        >
                          <SelectValue placeholder="Select operator" />
                        </SelectTrigger>
                        <SelectContent>
                          {OPERATORS.map((op) => (
                            <SelectItem key={op} value={op}>
                              {OPERATOR_LABELS[op]}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>

                    <Button
                      variant="ghost"
                      size="icon"
                      onClick={() => handleRemoveFilter(filter.id)}
                      className="text-destructive hover:text-destructive"
                      data-testid={`button-delete-filter-${filter.id}`}
                    >
                      <Trash2 className="h-4 w-4" />
                    </Button>
                  </div>
                </Card>
              ))}
            </div>
          )}

          <Button
            variant="outline"
            onClick={handleAddFilter}
            className="w-full"
            disabled={columns.length === 0}
            data-testid="button-add-filter"
          >
            <Plus className="h-4 w-4 mr-2" />
            Add Filter
          </Button>
        </div>

        <DialogFooter>
          <Button variant="outline" onClick={handleCancel} data-testid="button-cancel-settings">
            <X className="h-4 w-4 mr-2" />
            Cancel
          </Button>
          <Button onClick={handleSave} disabled={isSaving} data-testid="button-save-settings">
            {isSaving ? (
              <Loader2 className="h-4 w-4 mr-2 animate-spin" />
            ) : (
              <Save className="h-4 w-4 mr-2" />
            )}
            Save Changes
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
