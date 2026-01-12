import { useState, useEffect } from "react";
import { Columns3, Loader2, Check } from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover";
import { Checkbox } from "@/components/ui/checkbox";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import type { ColumnInfo } from "@/lib/types";
import { useAuth } from "@/hooks/use-auth";

interface ColumnSelectorProps {
  columns: ColumnInfo[];
  hiddenColumns: string[];
  onSave: (hiddenColumns: string[]) => void;
  isSaving?: boolean;
  disabled?: boolean;
}

export function ColumnSelector({
  columns,
  hiddenColumns,
  onSave,
  isSaving = false,
  disabled = false,
}: ColumnSelectorProps) {
  const { user } = useAuth();
  const isAdmin = user?.role === "admin";
  const [localHidden, setLocalHidden] = useState<Set<string>>(new Set(hiddenColumns));
  const [search, setSearch] = useState("");
  const [isOpen, setIsOpen] = useState(false);

  useEffect(() => {
    setLocalHidden(new Set(hiddenColumns));
  }, [hiddenColumns]);

  if (!isAdmin) {
    return null;
  }

  const filteredColumns = columns.filter((col) =>
    col.name.toLowerCase().includes(search.toLowerCase())
  );

  const visibleCount = columns.length - localHidden.size;

  const handleToggle = (columnName: string, checked: boolean) => {
    const newHidden = new Set(localHidden);
    if (checked) {
      newHidden.delete(columnName);
    } else {
      newHidden.add(columnName);
    }
    setLocalHidden(newHidden);
  };

  const handleSelectAll = () => {
    setLocalHidden(new Set());
  };

  const handleSelectNone = () => {
    setLocalHidden(new Set(columns.map((c) => c.name)));
  };

  const handleSave = () => {
    onSave(Array.from(localHidden));
    setIsOpen(false);
  };

  const hasChanges = 
    localHidden.size !== hiddenColumns.length ||
    Array.from(localHidden).some((col) => !hiddenColumns.includes(col));

  return (
    <Popover open={isOpen} onOpenChange={setIsOpen}>
      <PopoverTrigger asChild>
        <Button
          variant="outline"
          size="sm"
          disabled={disabled || columns.length === 0}
          data-testid="button-column-selector"
        >
          <Columns3 className="h-4 w-4 mr-1" />
          Columns ({visibleCount}/{columns.length})
        </Button>
      </PopoverTrigger>
      <PopoverContent className="w-72 p-0" align="end">
        <div className="p-3 border-b">
          <div className="flex items-center justify-between mb-2">
            <Label className="text-sm font-medium">Visible Columns</Label>
            <div className="flex gap-1">
              <Button
                variant="ghost"
                size="sm"
                className="h-7 text-xs"
                onClick={handleSelectAll}
                data-testid="button-select-all-columns"
              >
                All
              </Button>
              <Button
                variant="ghost"
                size="sm"
                className="h-7 text-xs"
                onClick={handleSelectNone}
                data-testid="button-select-none-columns"
              >
                None
              </Button>
            </div>
          </div>
          <Input
            placeholder="Search columns..."
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            className="h-8 text-sm"
            data-testid="input-search-columns"
          />
        </div>

        <ScrollArea className="h-64">
          <div className="p-2 space-y-1">
            {filteredColumns.map((col) => {
              const isVisible = !localHidden.has(col.name);
              return (
                <label
                  key={col.name}
                  className="flex items-center gap-2 px-2 py-1.5 rounded-md cursor-pointer hover-elevate"
                  data-testid={`checkbox-column-${col.name}`}
                >
                  <Checkbox
                    checked={isVisible}
                    onCheckedChange={(checked) =>
                      handleToggle(col.name, checked === true)
                    }
                  />
                  <div className="flex flex-col min-w-0 flex-1">
                    <span className="font-mono text-xs truncate">{col.name}</span>
                    <span className="text-[10px] text-muted-foreground">
                      {col.dataType}
                      {col.isPrimaryKey && " (PK)"}
                    </span>
                  </div>
                </label>
              );
            })}
            {filteredColumns.length === 0 && (
              <div className="text-center py-4 text-sm text-muted-foreground">
                No columns match your search
              </div>
            )}
          </div>
        </ScrollArea>

        <div className="p-3 border-t flex justify-end gap-2">
          <Button
            variant="outline"
            size="sm"
            onClick={() => setIsOpen(false)}
            data-testid="button-cancel-columns"
          >
            Cancel
          </Button>
          <Button
            size="sm"
            onClick={handleSave}
            disabled={!hasChanges || isSaving}
            data-testid="button-save-columns"
          >
            {isSaving ? (
              <Loader2 className="h-4 w-4 animate-spin mr-1" />
            ) : (
              <Check className="h-4 w-4 mr-1" />
            )}
            Save
          </Button>
        </div>
      </PopoverContent>
    </Popover>
  );
}
