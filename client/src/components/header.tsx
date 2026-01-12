import { Database } from "lucide-react";
import { ThemeToggle } from "./theme-toggle";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import type { DatabaseConnection } from "@/lib/types";

interface HeaderProps {
  databases: DatabaseConnection[];
  selectedDatabase: string;
  onDatabaseChange: (name: string) => void;
  isLoading: boolean;
}

export function Header({
  databases,
  selectedDatabase,
  onDatabaseChange,
  isLoading,
}: HeaderProps) {
  return (
    <header className="h-16 border-b bg-card flex items-center justify-between px-6 gap-4">
      <div className="flex items-center gap-3">
        <Database className="h-6 w-6 text-primary" />
        <h1 className="text-xl font-medium">Heroku Postgres Database Viewer</h1>
      </div>

      <div className="flex items-center gap-4">
        <Select
          value={selectedDatabase}
          onValueChange={onDatabaseChange}
          disabled={isLoading || databases.length === 0}
        >
          <SelectTrigger
            className="w-[200px]"
            data-testid="select-database"
          >
            <SelectValue placeholder="Select database..." />
          </SelectTrigger>
          <SelectContent>
            {databases.map((db) => (
              <SelectItem key={db.name} value={db.name} data-testid={`option-database-${db.name}`}>
                {db.name}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
        <ThemeToggle />
      </div>
    </header>
  );
}
