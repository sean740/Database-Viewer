import { useState } from "react";
import { useQuery, useMutation } from "@tanstack/react-query";
import { useLocation } from "wouter";
import { queryClient, apiRequest } from "@/lib/queryClient";
import { useAuth } from "@/hooks/use-auth";
import { useToast } from "@/hooks/use-toast";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Switch } from "@/components/ui/switch";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogDescription, DialogFooter } from "@/components/ui/dialog";
import { Avatar, AvatarFallback, AvatarImage } from "@/components/ui/avatar";
import { ArrowLeft, Users, Table, Shield, Trash2, Plus, Loader2, UserPlus, Eye, EyeOff, Pencil } from "lucide-react";
import type { User, TableGrant, UserRole, DatabaseConnection, TableInfo, TableSettings } from "@/lib/types";

export default function AdminPage() {
  const [, navigate] = useLocation();
  const { user: currentUser, isLoading: isAuthLoading } = useAuth();
  const { toast } = useToast();
  const [selectedUserId, setSelectedUserId] = useState<string | null>(null);
  const [isGrantDialogOpen, setIsGrantDialogOpen] = useState(false);
  const [grantDatabase, setGrantDatabase] = useState("");
  const [grantTable, setGrantTable] = useState("");
  const [isCreateUserDialogOpen, setIsCreateUserDialogOpen] = useState(false);
  const [newUserEmail, setNewUserEmail] = useState("");
  const [newUserPassword, setNewUserPassword] = useState("");
  const [newUserFirstName, setNewUserFirstName] = useState("");
  const [newUserLastName, setNewUserLastName] = useState("");
  const [newUserRole, setNewUserRole] = useState<UserRole>("external_customer");
  const [visibilityDatabase, setVisibilityDatabase] = useState("");
  const [editingDisplayName, setEditingDisplayName] = useState<string | null>(null);
  const [displayNameValue, setDisplayNameValue] = useState("");

  const { data: users = [], isLoading: isLoadingUsers } = useQuery<User[]>({
    queryKey: ["/api/admin/users"],
    enabled: currentUser?.role === "admin",
  });

  const { data: databases = [] } = useQuery<DatabaseConnection[]>({
    queryKey: ["/api/databases"],
  });

  const { data: tables = [] } = useQuery<TableInfo[]>({
    queryKey: ["/api/tables", grantDatabase],
    enabled: !!grantDatabase,
  });

  const { data: selectedUserGrants = [] } = useQuery<TableGrant[]>({
    queryKey: ["/api/admin/grants", selectedUserId],
    enabled: !!selectedUserId,
  });

  const { data: visibilityTables = [], isLoading: isLoadingVisibilityTables } = useQuery<TableInfo[]>({
    queryKey: ["/api/tables", visibilityDatabase],
    enabled: !!visibilityDatabase,
  });

  const { data: tableSettings = {} } = useQuery<Record<string, { isVisible: boolean; displayName: string | null }>>({
    queryKey: ["/api/admin/table-settings"],
  });

  const updateUserMutation = useMutation({
    mutationFn: async ({ userId, updates }: { userId: string; updates: { role?: UserRole; isActive?: boolean } }) => {
      return apiRequest("PATCH", `/api/admin/users/${userId}`, updates);
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["/api/admin/users"] });
      toast({ title: "User updated", description: "User settings have been saved." });
    },
    onError: (err) => {
      toast({ title: "Error", description: err instanceof Error ? err.message : "Failed to update user", variant: "destructive" });
    },
  });

  const addGrantMutation = useMutation({
    mutationFn: async (grant: { userId: string; database: string; tableName: string }) => {
      return apiRequest("POST", "/api/admin/grants", grant);
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["/api/admin/grants", selectedUserId] });
      setIsGrantDialogOpen(false);
      setGrantDatabase("");
      setGrantTable("");
      toast({ title: "Access granted", description: "Table access has been granted." });
    },
    onError: (err) => {
      toast({ title: "Error", description: err instanceof Error ? err.message : "Failed to grant access", variant: "destructive" });
    },
  });

  const deleteGrantMutation = useMutation({
    mutationFn: async (grantId: string) => {
      return apiRequest("DELETE", `/api/admin/grants/${grantId}`);
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["/api/admin/grants", selectedUserId] });
      toast({ title: "Access revoked", description: "Table access has been removed." });
    },
    onError: (err) => {
      toast({ title: "Error", description: err instanceof Error ? err.message : "Failed to revoke access", variant: "destructive" });
    },
  });

  const createUserMutation = useMutation({
    mutationFn: async (userData: { email: string; password: string; firstName?: string; lastName?: string; role: UserRole }) => {
      return apiRequest("POST", "/api/admin/users", userData);
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["/api/admin/users"] });
      setIsCreateUserDialogOpen(false);
      setNewUserEmail("");
      setNewUserPassword("");
      setNewUserFirstName("");
      setNewUserLastName("");
      setNewUserRole("external_customer");
      toast({ title: "User created", description: "New user account has been created." });
    },
    onError: (err) => {
      toast({ title: "Error", description: err instanceof Error ? err.message : "Failed to create user", variant: "destructive" });
    },
  });

  const deleteUserMutation = useMutation({
    mutationFn: async (userId: string) => {
      return apiRequest("DELETE", `/api/admin/users/${userId}`);
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["/api/admin/users"] });
      toast({ title: "User deleted", description: "User account has been removed." });
    },
    onError: (err) => {
      toast({ title: "Error", description: err instanceof Error ? err.message : "Failed to delete user", variant: "destructive" });
    },
  });

  const updateTableSettingsMutation = useMutation({
    mutationFn: async (data: { database: string; tableName: string; isVisible: boolean; displayName: string | null }) => {
      return apiRequest("POST", "/api/admin/table-settings", data);
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["/api/admin/table-settings"] });
      queryClient.invalidateQueries({ queryKey: ["/api/tables"] });
      setEditingDisplayName(null);
      toast({ title: "Settings updated", description: "Table settings have been saved." });
    },
    onError: (err) => {
      toast({ title: "Error", description: err instanceof Error ? err.message : "Failed to update settings", variant: "destructive" });
    },
  });

  if (isAuthLoading) {
    return (
      <div className="flex items-center justify-center h-screen">
        <Loader2 className="h-8 w-8 animate-spin text-primary" />
      </div>
    );
  }

  if (currentUser?.role !== "admin") {
    return (
      <div className="flex flex-col items-center justify-center h-screen gap-4">
        <Shield className="h-16 w-16 text-muted-foreground" />
        <h1 className="text-2xl font-semibold">Access Denied</h1>
        <p className="text-muted-foreground">You need admin privileges to access this page.</p>
        <Button onClick={() => navigate("/")} data-testid="button-back-home">
          <ArrowLeft className="mr-2 h-4 w-4" /> Back to Home
        </Button>
      </div>
    );
  }

  const roleLabels: Record<UserRole, string> = {
    admin: "Admin",
    washos_user: "WashOS User",
    external_customer: "External Customer",
  };

  const roleColors: Record<UserRole, "default" | "secondary" | "outline"> = {
    admin: "default",
    washos_user: "secondary",
    external_customer: "outline",
  };

  const selectedUser = users.find(u => u.id === selectedUserId);

  return (
    <div className="min-h-screen bg-background">
      <header className="border-b bg-card">
        <div className="container mx-auto px-4 h-16 flex items-center justify-between">
          <div className="flex items-center gap-4">
            <Button variant="ghost" size="icon" onClick={() => navigate("/")} data-testid="button-back">
              <ArrowLeft className="h-5 w-5" />
            </Button>
            <div className="flex items-center gap-2">
              <Shield className="h-5 w-5 text-primary" />
              <span className="font-semibold">Admin Panel</span>
            </div>
          </div>
        </div>
      </header>

      <main className="container mx-auto px-4 py-8">
        <Tabs defaultValue="users" className="space-y-6">
          <TabsList>
            <TabsTrigger value="users" className="gap-2" data-testid="tab-users">
              <Users className="h-4 w-4" /> Users
            </TabsTrigger>
            <TabsTrigger value="access" className="gap-2" data-testid="tab-access">
              <Table className="h-4 w-4" /> Table Access
            </TabsTrigger>
            <TabsTrigger value="visibility" className="gap-2" data-testid="tab-visibility">
              <Eye className="h-4 w-4" /> Table Visibility
            </TabsTrigger>
          </TabsList>

          <TabsContent value="users" className="space-y-4">
            <Card>
              <CardHeader className="flex flex-row items-center justify-between gap-2">
                <div>
                  <CardTitle>User Management</CardTitle>
                  <CardDescription>Manage user roles and account status</CardDescription>
                </div>
                <Button onClick={() => setIsCreateUserDialogOpen(true)} data-testid="button-add-user">
                  <UserPlus className="h-4 w-4 mr-2" /> Add User
                </Button>
              </CardHeader>
              <CardContent>
                {isLoadingUsers ? (
                  <div className="flex justify-center py-8">
                    <Loader2 className="h-6 w-6 animate-spin" />
                  </div>
                ) : (
                  <div className="space-y-4">
                    {users.map((user) => (
                      <div
                        key={user.id}
                        className="flex items-center justify-between p-4 rounded-lg border bg-card"
                        data-testid={`card-user-${user.id}`}
                      >
                        <div className="flex items-center gap-4">
                          <Avatar>
                            <AvatarImage src={user.profileImageUrl || undefined} />
                            <AvatarFallback>
                              {(user.firstName?.[0] || "") + (user.lastName?.[0] || "") || user.email?.[0]?.toUpperCase() || "U"}
                            </AvatarFallback>
                          </Avatar>
                          <div>
                            <div className="font-medium">
                              {user.firstName} {user.lastName}
                            </div>
                            <div className="text-sm text-muted-foreground">{user.email}</div>
                          </div>
                        </div>
                        <div className="flex items-center gap-4">
                          <Select
                            value={user.role}
                            onValueChange={(value) => updateUserMutation.mutate({ userId: user.id, updates: { role: value as UserRole } })}
                            disabled={user.id === currentUser?.id}
                          >
                            <SelectTrigger className="w-[180px]" data-testid={`select-role-${user.id}`}>
                              <SelectValue />
                            </SelectTrigger>
                            <SelectContent>
                              <SelectItem value="admin">Admin</SelectItem>
                              <SelectItem value="washos_user">WashOS User</SelectItem>
                              <SelectItem value="external_customer">External Customer</SelectItem>
                            </SelectContent>
                          </Select>
                          <div className="flex items-center gap-2">
                            <span className="text-sm text-muted-foreground">Active</span>
                            <Switch
                              checked={user.isActive}
                              onCheckedChange={(checked) => updateUserMutation.mutate({ userId: user.id, updates: { isActive: checked } })}
                              disabled={user.id === currentUser?.id}
                              data-testid={`switch-active-${user.id}`}
                            />
                          </div>
                          <Button
                            variant="ghost"
                            size="icon"
                            onClick={() => {
                              if (confirm(`Are you sure you want to delete ${user.email}?`)) {
                                deleteUserMutation.mutate(user.id);
                              }
                            }}
                            disabled={user.id === currentUser?.id}
                            data-testid={`button-delete-${user.id}`}
                          >
                            <Trash2 className="h-4 w-4 text-destructive" />
                          </Button>
                        </div>
                      </div>
                    ))}
                  </div>
                )}
              </CardContent>
            </Card>
          </TabsContent>

          <TabsContent value="access" className="space-y-4">
            <div className="grid md:grid-cols-2 gap-6">
              <Card>
                <CardHeader>
                  <CardTitle>External Customers</CardTitle>
                  <CardDescription>Select a customer to manage their table access</CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="space-y-2">
                    {users
                      .filter((u) => u.role === "external_customer")
                      .map((user) => (
                        <Button
                          key={user.id}
                          variant={selectedUserId === user.id ? "secondary" : "ghost"}
                          className="w-full justify-start"
                          onClick={() => setSelectedUserId(user.id)}
                          data-testid={`button-select-customer-${user.id}`}
                        >
                          <Avatar className="h-6 w-6 mr-2">
                            <AvatarImage src={user.profileImageUrl || undefined} />
                            <AvatarFallback className="text-xs">
                              {(user.firstName?.[0] || "") + (user.lastName?.[0] || "")}
                            </AvatarFallback>
                          </Avatar>
                          {user.firstName} {user.lastName}
                        </Button>
                      ))}
                    {users.filter((u) => u.role === "external_customer").length === 0 && (
                      <p className="text-sm text-muted-foreground text-center py-4">
                        No external customers found
                      </p>
                    )}
                  </div>
                </CardContent>
              </Card>

              <Card>
                <CardHeader className="flex flex-row items-center justify-between gap-2">
                  <div>
                    <CardTitle>Table Access</CardTitle>
                    <CardDescription>
                      {selectedUser
                        ? `Tables accessible by ${selectedUser.firstName} ${selectedUser.lastName}`
                        : "Select a customer to view their access"}
                    </CardDescription>
                  </div>
                  {selectedUserId && (
                    <Button
                      size="sm"
                      onClick={() => setIsGrantDialogOpen(true)}
                      data-testid="button-add-grant"
                    >
                      <Plus className="h-4 w-4 mr-1" /> Grant Access
                    </Button>
                  )}
                </CardHeader>
                <CardContent>
                  {selectedUserId ? (
                    <div className="space-y-2">
                      {selectedUserGrants.map((grant) => (
                        <div
                          key={grant.id}
                          className="flex items-center justify-between p-3 rounded-lg border"
                          data-testid={`grant-${grant.id}`}
                        >
                          <div className="flex items-center gap-2">
                            <Table className="h-4 w-4 text-muted-foreground" />
                            <span className="font-mono text-sm">
                              {grant.database}:{grant.tableName}
                            </span>
                          </div>
                          <Button
                            variant="ghost"
                            size="icon"
                            onClick={() => deleteGrantMutation.mutate(grant.id)}
                            data-testid={`button-revoke-${grant.id}`}
                          >
                            <Trash2 className="h-4 w-4 text-destructive" />
                          </Button>
                        </div>
                      ))}
                      {selectedUserGrants.length === 0 && (
                        <p className="text-sm text-muted-foreground text-center py-4">
                          No table access granted
                        </p>
                      )}
                    </div>
                  ) : (
                    <p className="text-sm text-muted-foreground text-center py-4">
                      Select a customer from the left to manage their access
                    </p>
                  )}
                </CardContent>
              </Card>
            </div>
          </TabsContent>

          <TabsContent value="visibility" className="space-y-4">
            <Card>
              <CardHeader>
                <CardTitle>Table Visibility & Display Names</CardTitle>
                <CardDescription>
                  Control which tables are visible to non-admin users and set custom display names
                </CardDescription>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="space-y-2">
                  <Label>Select Database</Label>
                  <Select value={visibilityDatabase} onValueChange={setVisibilityDatabase}>
                    <SelectTrigger data-testid="select-visibility-database">
                      <SelectValue placeholder="Select a database" />
                    </SelectTrigger>
                    <SelectContent>
                      {databases.map((db) => (
                        <SelectItem key={db.name} value={db.name}>
                          {db.name}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>

                {visibilityDatabase && (
                  <div className="space-y-2">
                    {isLoadingVisibilityTables ? (
                      <div className="flex justify-center py-8">
                        <Loader2 className="h-6 w-6 animate-spin" />
                      </div>
                    ) : visibilityTables.length === 0 ? (
                      <p className="text-sm text-muted-foreground text-center py-4">
                        No tables found in this database
                      </p>
                    ) : (
                      <div className="border rounded-lg divide-y">
                        {visibilityTables.map((table) => {
                          const settingsKey = `${visibilityDatabase}:${table.fullName}`;
                          const settings = tableSettings[settingsKey];
                          const isVisible = settings?.isVisible !== false;
                          const displayName = settings?.displayName || null;
                          const isEditing = editingDisplayName === settingsKey;

                          return (
                            <div
                              key={table.fullName}
                              className="flex items-center justify-between p-4"
                              data-testid={`table-settings-${table.fullName}`}
                            >
                              <div className="flex-1 min-w-0">
                                <div className="flex items-center gap-2">
                                  <Table className="h-4 w-4 text-muted-foreground shrink-0" />
                                  <span className="font-mono text-sm truncate">{table.fullName}</span>
                                </div>
                                {isEditing ? (
                                  <div className="flex items-center gap-2 mt-2">
                                    <Input
                                      value={displayNameValue}
                                      onChange={(e) => setDisplayNameValue(e.target.value)}
                                      placeholder="Enter display name"
                                      className="h-8 text-sm"
                                      data-testid={`input-display-name-${table.fullName}`}
                                    />
                                    <Button
                                      size="sm"
                                      onClick={() => {
                                        updateTableSettingsMutation.mutate({
                                          database: visibilityDatabase,
                                          tableName: table.fullName,
                                          isVisible,
                                          displayName: displayNameValue || null,
                                        });
                                      }}
                                      disabled={updateTableSettingsMutation.isPending}
                                      data-testid={`button-save-display-name-${table.fullName}`}
                                    >
                                      Save
                                    </Button>
                                    <Button
                                      size="sm"
                                      variant="ghost"
                                      onClick={() => setEditingDisplayName(null)}
                                      data-testid={`button-cancel-display-name-${table.fullName}`}
                                    >
                                      Cancel
                                    </Button>
                                  </div>
                                ) : displayName ? (
                                  <div className="flex items-center gap-2 mt-1">
                                    <span className="text-sm text-muted-foreground">Display name:</span>
                                    <span className="text-sm font-medium">{displayName}</span>
                                    <Button
                                      size="icon"
                                      variant="ghost"
                                      className="h-6 w-6"
                                      onClick={() => {
                                        setEditingDisplayName(settingsKey);
                                        setDisplayNameValue(displayName);
                                      }}
                                      data-testid={`button-edit-display-name-${table.fullName}`}
                                    >
                                      <Pencil className="h-3 w-3" />
                                    </Button>
                                  </div>
                                ) : (
                                  <Button
                                    variant="ghost"
                                    className="h-auto p-0 mt-1 text-sm text-primary"
                                    onClick={() => {
                                      setEditingDisplayName(settingsKey);
                                      setDisplayNameValue("");
                                    }}
                                    data-testid={`button-add-display-name-${table.fullName}`}
                                  >
                                    <Pencil className="h-3 w-3 mr-1" /> Set display name
                                  </Button>
                                )}
                              </div>
                              <div className="flex items-center gap-2 ml-4">
                                <span className="text-sm text-muted-foreground">
                                  {isVisible ? "Visible" : "Hidden"}
                                </span>
                                <Switch
                                  checked={isVisible}
                                  onCheckedChange={(checked) => {
                                    updateTableSettingsMutation.mutate({
                                      database: visibilityDatabase,
                                      tableName: table.fullName,
                                      isVisible: checked,
                                      displayName,
                                    });
                                  }}
                                  data-testid={`switch-visibility-${table.fullName}`}
                                />
                                {isVisible ? (
                                  <Eye className="h-4 w-4 text-muted-foreground" />
                                ) : (
                                  <EyeOff className="h-4 w-4 text-muted-foreground" />
                                )}
                              </div>
                            </div>
                          );
                        })}
                      </div>
                    )}
                  </div>
                )}
              </CardContent>
            </Card>
          </TabsContent>
        </Tabs>
      </main>

      <Dialog open={isGrantDialogOpen} onOpenChange={setIsGrantDialogOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Grant Table Access</DialogTitle>
            <DialogDescription>
              Select a database and table to grant access to {selectedUser?.firstName} {selectedUser?.lastName}
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-4 py-4">
            <div className="space-y-2">
              <label className="text-sm font-medium">Database</label>
              <Select value={grantDatabase} onValueChange={setGrantDatabase}>
                <SelectTrigger data-testid="select-grant-database">
                  <SelectValue placeholder="Select database" />
                </SelectTrigger>
                <SelectContent>
                  {databases.map((db) => (
                    <SelectItem key={db.name} value={db.name}>
                      {db.name}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div className="space-y-2">
              <label className="text-sm font-medium">Table</label>
              <Select value={grantTable} onValueChange={setGrantTable} disabled={!grantDatabase}>
                <SelectTrigger data-testid="select-grant-table">
                  <SelectValue placeholder="Select table" />
                </SelectTrigger>
                <SelectContent>
                  {tables.map((t) => (
                    <SelectItem key={t.fullName} value={t.fullName}>
                      {t.fullName}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setIsGrantDialogOpen(false)}>
              Cancel
            </Button>
            <Button
              onClick={() => {
                if (selectedUserId && grantDatabase && grantTable) {
                  addGrantMutation.mutate({
                    userId: selectedUserId,
                    database: grantDatabase,
                    tableName: grantTable,
                  });
                }
              }}
              disabled={!grantDatabase || !grantTable || addGrantMutation.isPending}
              data-testid="button-confirm-grant"
            >
              {addGrantMutation.isPending ? <Loader2 className="h-4 w-4 animate-spin mr-2" /> : null}
              Grant Access
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog open={isCreateUserDialogOpen} onOpenChange={setIsCreateUserDialogOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Create New User</DialogTitle>
            <DialogDescription>
              Add a new user account to the system
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-4 py-4">
            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-2">
                <Label htmlFor="new-first-name">First Name</Label>
                <Input
                  id="new-first-name"
                  value={newUserFirstName}
                  onChange={(e) => setNewUserFirstName(e.target.value)}
                  placeholder="John"
                  data-testid="input-new-first-name"
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="new-last-name">Last Name</Label>
                <Input
                  id="new-last-name"
                  value={newUserLastName}
                  onChange={(e) => setNewUserLastName(e.target.value)}
                  placeholder="Doe"
                  data-testid="input-new-last-name"
                />
              </div>
            </div>
            <div className="space-y-2">
              <Label htmlFor="new-email">Email</Label>
              <Input
                id="new-email"
                type="email"
                value={newUserEmail}
                onChange={(e) => setNewUserEmail(e.target.value)}
                placeholder="user@example.com"
                required
                data-testid="input-new-email"
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="new-password">Password</Label>
              <Input
                id="new-password"
                type="password"
                value={newUserPassword}
                onChange={(e) => setNewUserPassword(e.target.value)}
                placeholder="Enter password"
                required
                data-testid="input-new-password"
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="new-role">Role</Label>
              <Select value={newUserRole} onValueChange={(value) => setNewUserRole(value as UserRole)}>
                <SelectTrigger data-testid="select-new-role">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="admin">Admin</SelectItem>
                  <SelectItem value="washos_user">WashOS User</SelectItem>
                  <SelectItem value="external_customer">External Customer</SelectItem>
                </SelectContent>
              </Select>
            </div>
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setIsCreateUserDialogOpen(false)}>
              Cancel
            </Button>
            <Button
              onClick={() => {
                if (newUserEmail && newUserPassword) {
                  createUserMutation.mutate({
                    email: newUserEmail,
                    password: newUserPassword,
                    firstName: newUserFirstName || undefined,
                    lastName: newUserLastName || undefined,
                    role: newUserRole,
                  });
                }
              }}
              disabled={!newUserEmail || !newUserPassword || createUserMutation.isPending}
              data-testid="button-create-user"
            >
              {createUserMutation.isPending ? <Loader2 className="h-4 w-4 animate-spin mr-2" /> : null}
              Create User
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}
