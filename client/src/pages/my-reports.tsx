import { useState, useEffect } from "react";
import { useQuery, useMutation } from "@tanstack/react-query";
import { queryClient, apiRequest } from "@/lib/queryClient";
import { useToast } from "@/hooks/use-toast";
import { useAuth } from "@/hooks/use-auth";
import { Header } from "@/components/header";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Separator } from "@/components/ui/separator";
import { Badge } from "@/components/ui/badge";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  BarChart,
  Bar,
  LineChart,
  Line,
  PieChart,
  Pie,
  AreaChart,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Cell,
} from "recharts";
import {
  Plus,
  FileText,
  BarChart3,
  Hash,
  Type,
  Trash2,
  Send,
  Loader2,
  Table,
  PieChartIcon,
  TrendingUp,
  Bot,
  Sparkles,
  ChevronLeft,
  ChevronRight,
} from "lucide-react";
import { Link } from "wouter";

interface ReportPage {
  id: string;
  userId: string;
  title: string;
  description: string | null;
  isArchived: boolean;
  createdAt: string;
  updatedAt: string;
  blocks?: ReportBlock[];
}

interface ReportBlock {
  id: string;
  pageId: string;
  kind: "table" | "chart" | "metric" | "text";
  title: string | null;
  position: { row: number; col: number; width: number; height: number };
  config: any;
  createdAt: string;
  updatedAt: string;
}

interface BlockQueryResult {
  type: "table" | "chart" | "metric" | "text";
  rows?: any[];
  rowCount?: number;
  totalCount?: number;
  page?: number;
  pageSize?: number;
  totalPages?: number;
  data?: any[];
  chartType?: string;
  value?: number;
  label?: string;
  format?: string;
  content?: string;
}

const CHART_COLORS = ["hsl(var(--primary))", "hsl(var(--accent))", "hsl(var(--secondary))", "#8884d8", "#82ca9d", "#ffc658"];

export default function MyReports() {
  const { toast } = useToast();
  const { user } = useAuth();
  const [selectedPageId, setSelectedPageId] = useState<string | null>(null);
  const [isCreateDialogOpen, setIsCreateDialogOpen] = useState(false);
  const [newPageTitle, setNewPageTitle] = useState("");
  const [newPageDescription, setNewPageDescription] = useState("");
  const [chatMessage, setChatMessage] = useState("");
  const [chatHistory, setChatHistory] = useState<{ role: string; content: string }[]>([]);
  const [isChatLoading, setIsChatLoading] = useState(false);

  const { data: pages = [], isLoading: isLoadingPages } = useQuery<ReportPage[]>({
    queryKey: ["/api/reports/pages"],
  });

  const { data: selectedPage, isLoading: isLoadingPage } = useQuery<ReportPage>({
    queryKey: ["/api/reports/pages", selectedPageId],
    enabled: !!selectedPageId,
  });

  const { data: chatData } = useQuery<{ messages: { role: string; content: string; timestamp?: string }[] }>({
    queryKey: ["/api/reports/pages", selectedPageId, "chat"],
    enabled: !!selectedPageId,
  });

  useEffect(() => {
    if (chatData?.messages) {
      setChatHistory(chatData.messages.map(m => ({ role: m.role, content: m.content })));
    } else {
      setChatHistory([]);
    }
  }, [chatData, selectedPageId]);

  const createPageMutation = useMutation({
    mutationFn: async (data: { title: string; description: string }) => {
      const res = await apiRequest("POST", "/api/reports/pages", data);
      return res.json();
    },
    onSuccess: (newPage) => {
      queryClient.invalidateQueries({ queryKey: ["/api/reports/pages"] });
      setSelectedPageId(newPage.id);
      setIsCreateDialogOpen(false);
      setNewPageTitle("");
      setNewPageDescription("");
      toast({ title: "Report created", description: `"${newPage.title}" has been created.` });
    },
    onError: (err: any) => {
      toast({ title: "Error", description: err.message || "Failed to create report", variant: "destructive" });
    },
  });

  const deletePageMutation = useMutation({
    mutationFn: async (id: string) => {
      await apiRequest("DELETE", `/api/reports/pages/${id}`);
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["/api/reports/pages"] });
      if (selectedPageId === selectedPage?.id) {
        setSelectedPageId(null);
      }
      toast({ title: "Report deleted" });
    },
  });

  const createBlockMutation = useMutation({
    mutationFn: async (data: { pageId: string; kind: string; title: string; config: any }) => {
      const res = await apiRequest("POST", `/api/reports/pages/${data.pageId}/blocks`, {
        kind: data.kind,
        title: data.title,
        position: { row: 0, col: 0, width: 6, height: 4 },
        config: data.config,
      });
      return res.json();
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["/api/reports/pages", selectedPageId] });
      toast({ title: "Block added" });
    },
    onError: (err: any) => {
      toast({ title: "Error", description: err.message || "Failed to add block", variant: "destructive" });
    },
  });

  const deleteBlockMutation = useMutation({
    mutationFn: async (id: string) => {
      await apiRequest("DELETE", `/api/reports/blocks/${id}`);
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["/api/reports/pages", selectedPageId] });
      toast({ title: "Block deleted" });
    },
  });

  const handleSendChat = async () => {
    if (!chatMessage.trim() || !selectedPageId) return;

    const userMessage = chatMessage.trim();
    setChatMessage("");
    setChatHistory((prev) => [...prev, { role: "user", content: userMessage }]);
    setIsChatLoading(true);

    try {
      const res = await apiRequest("POST", "/api/reports/ai/chat", {
        pageId: selectedPageId,
        message: userMessage,
      });
      const data = await res.json();

      setChatHistory((prev) => [...prev, { role: "assistant", content: data.message }]);

      // Handle single block creation
      if (data.action?.action === "create_block" && data.action.block) {
        const block = data.action.block;
        await createBlockMutation.mutateAsync({
          pageId: selectedPageId,
          kind: block.kind,
          title: block.title,
          config: {
            ...block.config,
            database: block.config?.database || "Default",
            rowLimit: block.config?.rowLimit || 500,
            filters: block.config?.filters || [],
          },
        });
      }
      
      // Handle multiple blocks creation (for comparisons)
      if (data.action?.action === "create_blocks" && Array.isArray(data.action.blocks)) {
        for (const block of data.action.blocks) {
          await createBlockMutation.mutateAsync({
            pageId: selectedPageId,
            kind: block.kind,
            title: block.title,
            config: {
              ...block.config,
              database: block.config?.database || "Default",
              rowLimit: block.config?.rowLimit || 500,
              filters: block.config?.filters || [],
            },
          });
        }
      }
    } catch (err: any) {
      setChatHistory((prev) => [
        ...prev,
        { role: "assistant", content: "Sorry, I encountered an error. Please try again." },
      ]);
    } finally {
      setIsChatLoading(false);
    }
  };

  return (
    <div className="min-h-screen bg-background" data-testid="my-reports-page">
      <Header showDatabaseSelector={false} />

      <div className="flex h-[calc(100vh-64px)]">
        <div className="w-64 border-r bg-muted/30 flex flex-col">
          <div className="p-4 border-b">
            <div className="flex items-center justify-between gap-2 mb-3">
              <h2 className="font-semibold text-sm">My Reports</h2>
              <Dialog open={isCreateDialogOpen} onOpenChange={setIsCreateDialogOpen}>
                <DialogTrigger asChild>
                  <Button size="icon" variant="ghost" data-testid="button-create-report">
                    <Plus className="h-4 w-4" />
                  </Button>
                </DialogTrigger>
                <DialogContent>
                  <DialogHeader>
                    <DialogTitle>Create New Report</DialogTitle>
                    <DialogDescription>
                      Give your report a name and optional description.
                    </DialogDescription>
                  </DialogHeader>
                  <div className="space-y-4 py-4">
                    <div>
                      <label className="text-sm font-medium">Title</label>
                      <Input
                        value={newPageTitle}
                        onChange={(e) => setNewPageTitle(e.target.value)}
                        placeholder="Monthly Sales Report"
                        data-testid="input-report-title"
                      />
                    </div>
                    <div>
                      <label className="text-sm font-medium">Description (optional)</label>
                      <Textarea
                        value={newPageDescription}
                        onChange={(e) => setNewPageDescription(e.target.value)}
                        placeholder="Track monthly sales performance..."
                        data-testid="input-report-description"
                      />
                    </div>
                  </div>
                  <DialogFooter>
                    <Button
                      onClick={() => createPageMutation.mutate({ title: newPageTitle, description: newPageDescription })}
                      disabled={!newPageTitle.trim() || createPageMutation.isPending}
                      data-testid="button-submit-create-report"
                    >
                      {createPageMutation.isPending ? <Loader2 className="h-4 w-4 animate-spin" /> : "Create"}
                    </Button>
                  </DialogFooter>
                </DialogContent>
              </Dialog>
            </div>
            <Link href="/">
              <Button variant="ghost" size="sm" className="w-full justify-start gap-2 text-muted-foreground" data-testid="link-back-to-viewer">
                <ChevronLeft className="h-4 w-4" />
                Back to Data Viewer
              </Button>
            </Link>
          </div>

          <ScrollArea className="flex-1">
            <div className="p-2 space-y-1">
              {isLoadingPages ? (
                <div className="flex items-center justify-center py-8">
                  <Loader2 className="h-5 w-5 animate-spin text-muted-foreground" />
                </div>
              ) : pages.length === 0 ? (
                <div className="text-center py-8 text-muted-foreground text-sm">
                  <FileText className="h-8 w-8 mx-auto mb-2 opacity-50" />
                  <p>No reports yet</p>
                  <p className="text-xs">Click + to create one</p>
                </div>
              ) : (
                pages.map((page) => (
                  <button
                    key={page.id}
                    onClick={() => {
                      setSelectedPageId(page.id);
                    }}
                    className={`w-full text-left px-3 py-2 rounded-md text-sm transition-colors hover-elevate ${
                      selectedPageId === page.id ? "bg-primary/10 text-primary" : "hover:bg-muted"
                    }`}
                    data-testid={`button-select-report-${page.id}`}
                  >
                    <div className="font-medium truncate">{page.title}</div>
                    <div className="text-xs text-muted-foreground truncate">
                      {new Date(page.updatedAt).toLocaleDateString()}
                    </div>
                  </button>
                ))
              )}
            </div>
          </ScrollArea>
        </div>

        <div className="flex-1 flex flex-col overflow-hidden">
          {!selectedPageId ? (
            <div className="flex-1 flex items-center justify-center text-muted-foreground">
              <div className="text-center">
                <Sparkles className="h-12 w-12 mx-auto mb-4 opacity-50" />
                <h3 className="text-lg font-medium mb-2">Select or Create a Report</h3>
                <p className="text-sm max-w-md">
                  Create custom reports with tables, charts, and metrics. Use the AI assistant to help you build reports from your data.
                </p>
              </div>
            </div>
          ) : isLoadingPage ? (
            <div className="flex-1 flex items-center justify-center">
              <Loader2 className="h-8 w-8 animate-spin text-primary" />
            </div>
          ) : selectedPage ? (
            <>
              <div className="border-b p-4 flex items-center justify-between gap-4">
                <div>
                  <h1 className="text-xl font-semibold">{selectedPage.title}</h1>
                  {selectedPage.description && (
                    <p className="text-sm text-muted-foreground">{selectedPage.description}</p>
                  )}
                </div>
                <div className="flex items-center gap-2">
                  <Button
                    variant="destructive"
                    size="sm"
                    onClick={() => deletePageMutation.mutate(selectedPage.id)}
                    disabled={deletePageMutation.isPending}
                    data-testid="button-delete-report"
                  >
                    <Trash2 className="h-4 w-4 mr-1" />
                    Delete
                  </Button>
                </div>
              </div>

              <div className="flex-1 flex overflow-hidden">
                <div className="flex-1 overflow-auto p-6">
                  {selectedPage.blocks?.length === 0 ? (
                    <div className="h-full flex items-center justify-center">
                      <Card className="max-w-md">
                        <CardHeader>
                          <CardTitle className="flex items-center gap-2">
                            <Bot className="h-5 w-5" />
                            Get Started with AI
                          </CardTitle>
                          <CardDescription>
                            Use the chat panel on the right to describe what you want to see. For example:
                          </CardDescription>
                        </CardHeader>
                        <CardContent>
                          <ul className="space-y-2 text-sm text-muted-foreground">
                            <li>"Show me a table of recent bookings"</li>
                            <li>"Create a pie chart of booking statuses"</li>
                            <li>"Display the total count of customers"</li>
                            <li>"Add a bar chart showing bookings by month"</li>
                          </ul>
                        </CardContent>
                      </Card>
                    </div>
                  ) : (
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                      {selectedPage.blocks?.map((block) => (
                        <ReportBlockCard key={block.id} block={block} onDelete={() => deleteBlockMutation.mutate(block.id)} />
                      ))}
                    </div>
                  )}
                </div>

                <div className="w-80 border-l flex flex-col bg-muted/20">
                  <div className="p-4 border-b">
                    <h3 className="font-semibold flex items-center gap-2">
                      <Sparkles className="h-4 w-4 text-primary" />
                      AI Assistant
                    </h3>
                    <p className="text-xs text-muted-foreground mt-1">
                      Describe what you want to add to your report
                    </p>
                  </div>

                  <ScrollArea className="flex-1 p-4">
                    <div className="space-y-4">
                      {chatHistory.length === 0 && (
                        <div className="text-center text-muted-foreground text-sm py-8">
                          <Bot className="h-8 w-8 mx-auto mb-2 opacity-50" />
                          <p>Ask me to help build your report!</p>
                        </div>
                      )}
                      {chatHistory.map((msg, idx) => (
                        <div
                          key={idx}
                          className={`flex ${msg.role === "user" ? "justify-end" : "justify-start"}`}
                        >
                          <div
                            className={`max-w-[90%] rounded-lg px-3 py-2 text-sm ${
                              msg.role === "user"
                                ? "bg-primary text-primary-foreground"
                                : "bg-muted"
                            }`}
                          >
                            <p className="whitespace-pre-wrap">{msg.content}</p>
                          </div>
                        </div>
                      ))}
                      {isChatLoading && (
                        <div className="flex justify-start">
                          <div className="bg-muted rounded-lg px-3 py-2">
                            <Loader2 className="h-4 w-4 animate-spin" />
                          </div>
                        </div>
                      )}
                    </div>
                  </ScrollArea>

                  <div className="p-4 border-t">
                    <div className="flex gap-2">
                      <Input
                        value={chatMessage}
                        onChange={(e) => setChatMessage(e.target.value)}
                        onKeyDown={(e) => e.key === "Enter" && !e.shiftKey && handleSendChat()}
                        placeholder="Describe what you want..."
                        disabled={isChatLoading}
                        data-testid="input-chat-message"
                      />
                      <Button
                        size="icon"
                        onClick={handleSendChat}
                        disabled={!chatMessage.trim() || isChatLoading}
                        data-testid="button-send-chat"
                      >
                        <Send className="h-4 w-4" />
                      </Button>
                    </div>
                  </div>
                </div>
              </div>
            </>
          ) : null}
        </div>
      </div>
    </div>
  );
}

function ReportBlockCard({ block, onDelete }: { block: ReportBlock; onDelete: () => void }) {
  const [currentPage, setCurrentPage] = useState(1);
  
  const { data: result, isLoading, error, refetch } = useQuery<BlockQueryResult>({
    queryKey: ["/api/reports/blocks", block.id, "run", currentPage],
    queryFn: async () => {
      const res = await apiRequest("POST", `/api/reports/blocks/${block.id}/run`, { page: currentPage });
      return res.json();
    },
  });

  const getBlockIcon = () => {
    switch (block.kind) {
      case "table":
        return <Table className="h-4 w-4" />;
      case "chart":
        return <BarChart3 className="h-4 w-4" />;
      case "metric":
        return <Hash className="h-4 w-4" />;
      case "text":
        return <Type className="h-4 w-4" />;
      default:
        return null;
    }
  };
  
  const handlePrevPage = () => {
    if (currentPage > 1) {
      setCurrentPage(currentPage - 1);
    }
  };
  
  const handleNextPage = () => {
    if (result?.totalPages && currentPage < result.totalPages) {
      setCurrentPage(currentPage + 1);
    }
  };

  return (
    <Card className="overflow-hidden" data-testid={`report-block-${block.id}`}>
      <CardHeader className="py-3 px-4 flex flex-row items-center justify-between gap-2">
        <div className="flex items-center gap-2">
          {getBlockIcon()}
          <CardTitle className="text-sm font-medium">{block.title || `${block.kind} block`}</CardTitle>
        </div>
        <Button size="icon" variant="ghost" onClick={onDelete} data-testid={`button-delete-block-${block.id}`}>
          <Trash2 className="h-4 w-4" />
        </Button>
      </CardHeader>
      <CardContent className="p-4 pt-0">
        {isLoading ? (
          <div className="flex items-center justify-center h-32">
            <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
          </div>
        ) : error ? (
          <div className="text-sm text-destructive text-center py-4">Failed to load data</div>
        ) : result?.type === "table" ? (
          <div className="overflow-x-auto">
            <div className="max-h-80">
              <table className="w-full text-xs">
                <thead>
                  <tr className="border-b">
                    {result.rows && result.rows[0] &&
                      Object.keys(result.rows[0]).map((key) => (
                        <th key={key} className="text-left py-2 px-2 font-medium text-muted-foreground">
                          {key}
                        </th>
                      ))}
                  </tr>
                </thead>
                <tbody>
                  {result.rows?.map((row, idx) => (
                    <tr key={idx} className="border-b last:border-0">
                      {Object.values(row).map((val, i) => (
                        <td key={i} className="py-2 px-2 truncate max-w-[150px]">
                          {String(val ?? "")}
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            {/* Pagination controls */}
            {result.totalPages && result.totalPages > 1 && (
              <div className="flex items-center justify-between mt-3 pt-3 border-t">
                <p className="text-xs text-muted-foreground">
                  {result.totalCount?.toLocaleString()} total rows
                </p>
                <div className="flex items-center gap-2">
                  <Button 
                    size="sm" 
                    variant="outline" 
                    onClick={handlePrevPage} 
                    disabled={currentPage <= 1}
                    data-testid={`button-prev-page-${block.id}`}
                  >
                    <ChevronLeft className="h-4 w-4" />
                  </Button>
                  <span className="text-xs text-muted-foreground">
                    Page {result.page} of {result.totalPages}
                  </span>
                  <Button 
                    size="sm" 
                    variant="outline" 
                    onClick={handleNextPage} 
                    disabled={currentPage >= (result.totalPages || 1)}
                    data-testid={`button-next-page-${block.id}`}
                  >
                    <ChevronRight className="h-4 w-4" />
                  </Button>
                </div>
              </div>
            )}
            {result.totalPages === 1 && result.totalCount && (
              <p className="text-xs text-muted-foreground text-center mt-2">
                {result.totalCount?.toLocaleString()} rows
              </p>
            )}
          </div>
        ) : result?.type === "chart" ? (
          <div className="h-48">
            <ResponsiveContainer width="100%" height="100%">
              {result.chartType === "pie" ? (
                <PieChart>
                  <Pie
                    data={result.data}
                    dataKey="value"
                    nameKey="label"
                    cx="50%"
                    cy="50%"
                    outerRadius={60}
                    label={({ label }) => label}
                  >
                    {result.data?.map((_, idx) => (
                      <Cell key={idx} fill={CHART_COLORS[idx % CHART_COLORS.length]} />
                    ))}
                  </Pie>
                  <Tooltip />
                </PieChart>
              ) : result.chartType === "line" ? (
                <LineChart data={result.data}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="label" tick={{ fontSize: 10 }} />
                  <YAxis tick={{ fontSize: 10 }} />
                  <Tooltip />
                  <Line type="monotone" dataKey="value" stroke="hsl(var(--primary))" strokeWidth={2} />
                </LineChart>
              ) : result.chartType === "area" ? (
                <AreaChart data={result.data}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="label" tick={{ fontSize: 10 }} />
                  <YAxis tick={{ fontSize: 10 }} />
                  <Tooltip />
                  <Area type="monotone" dataKey="value" fill="hsl(var(--primary))" stroke="hsl(var(--primary))" />
                </AreaChart>
              ) : (
                <BarChart data={result.data}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="label" tick={{ fontSize: 10 }} />
                  <YAxis tick={{ fontSize: 10 }} />
                  <Tooltip />
                  <Bar dataKey="value" fill="hsl(var(--primary))" />
                </BarChart>
              )}
            </ResponsiveContainer>
          </div>
        ) : result?.type === "metric" ? (
          <div className="text-center py-4">
            <div className="text-3xl font-bold text-primary">
              {result.format === "currency"
                ? `$${Number(result.value).toLocaleString()}`
                : result.format === "percentage"
                ? `${Number(result.value).toFixed(1)}%`
                : Number(result.value).toLocaleString()}
            </div>
            <div className="text-sm text-muted-foreground mt-1">{result.label}</div>
          </div>
        ) : result?.type === "text" ? (
          <div className="prose prose-sm max-w-none">{result.content}</div>
        ) : null}
      </CardContent>
    </Card>
  );
}
