import { Database, Shield, LogOut, FileText } from "lucide-react";
import { Link } from "wouter";
import { ThemeToggle } from "./theme-toggle";
import { Button } from "@/components/ui/button";
import { Avatar, AvatarFallback, AvatarImage } from "@/components/ui/avatar";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { useAuth } from "@/hooks/use-auth";
import type { DatabaseConnection, User } from "@/lib/types";

interface HeaderProps {
  databases?: DatabaseConnection[];
  selectedDatabase?: string;
  onDatabaseChange?: (name: string) => void;
  isLoading?: boolean;
  showDatabaseSelector?: boolean;
}

export function Header({
  databases = [],
  selectedDatabase = "",
  onDatabaseChange = () => {},
  isLoading = false,
  showDatabaseSelector = true,
}: HeaderProps) {
  const { user, logout } = useAuth();

  const initials = user
    ? (user.firstName?.[0] || "") + (user.lastName?.[0] || "") ||
      user.email?.[0]?.toUpperCase() ||
      "U"
    : "U";

  return (
    <header className="h-16 border-b bg-card flex items-center justify-between px-6 gap-4">
      <div className="flex items-center gap-3">
        <Database className="h-6 w-6 text-primary" />
        <h1 className="text-xl font-medium">WashOS DataScope</h1>
      </div>

      <div className="flex items-center gap-4">
        {showDatabaseSelector && (
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
        )}

        <ThemeToggle />

        <DropdownMenu>
          <DropdownMenuTrigger asChild>
            <Button variant="ghost" className="relative h-9 w-9 rounded-full" data-testid="button-user-menu">
              <Avatar className="h-9 w-9">
                <AvatarImage src={user?.profileImageUrl || undefined} alt={user?.firstName || "User"} />
                <AvatarFallback>{initials}</AvatarFallback>
              </Avatar>
            </Button>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="end" className="w-56">
            <div className="px-2 py-1.5">
              <p className="text-sm font-medium">{user?.firstName} {user?.lastName}</p>
              <p className="text-xs text-muted-foreground">{user?.email}</p>
              <p className="text-xs text-muted-foreground capitalize mt-1">
                {user?.role?.replace("_", " ")}
              </p>
            </div>
            <DropdownMenuSeparator />
            <DropdownMenuItem asChild>
              <Link href="/my-reports" className="flex items-center cursor-pointer" data-testid="link-my-reports">
                <FileText className="mr-2 h-4 w-4" />
                My Reports
              </Link>
            </DropdownMenuItem>
            {user?.role === "admin" && (
              <DropdownMenuItem asChild>
                <Link href="/admin" className="flex items-center cursor-pointer" data-testid="link-admin">
                  <Shield className="mr-2 h-4 w-4" />
                  Admin Panel
                </Link>
              </DropdownMenuItem>
            )}
            <DropdownMenuSeparator />
            <DropdownMenuItem 
              onClick={() => logout()} 
              className="flex items-center cursor-pointer text-destructive" 
              data-testid="link-logout"
            >
              <LogOut className="mr-2 h-4 w-4" />
              Sign Out
            </DropdownMenuItem>
          </DropdownMenuContent>
        </DropdownMenu>
      </div>
    </header>
  );
}
