import { useState, useEffect, useRef } from "react";
import { useQuery, useMutation } from "@tanstack/react-query";
import { Link } from "wouter";
import { ArrowLeft, TrendingUp, TrendingDown, Minus, RefreshCw, Loader2, Calendar, Star, Truck, Users, BarChart3, Percent, ChevronDown, AlertTriangle, Clock, UserCheck, UserMinus, Bot, User, Send, X, MessageSquare, Download, Table2 } from "lucide-react";
import { Header } from "@/components/header";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { useToast } from "@/hooks/use-toast";
import { apiRequest } from "@/lib/queryClient";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible";
import { cn } from "@/lib/utils";
import type { DatabaseConnection } from "@/lib/types";

interface OperationsMetrics {
  bookingsCompleted: number;
  emergencies: number;
  deliveryRate: number;
  defectPercent: number;
  overbookedPercent: number;
  avgRating: number;
  responseRate: number;
  stripeMargin: number;
  activeVendors: number;
  vendorLevelCounts: Record<string, number>;
  newVendors: number;
  dismissedVendors: number;
  scheduledHours: number;
  utilization: number;
}

interface PeriodData {
  periodLabel: string;
  periodStart: string;
  periodEnd: string;
  periodType: "weekly" | "monthly";
  metrics: OperationsMetrics;
  variance: Record<string, number | null> | null;
}

interface OperationsPerformanceResponse {
  periods: PeriodData[];
  stripeConnected: boolean;
  periodType: string;
}

interface DrilldownData {
  metricId: string;
  metricName: string;
  periodStart: string;
  periodEnd: string;
  columns: string[];
  rows: Record<string, unknown>[];
  totalCount: number;
  previewCount: number;
  hasMore: boolean;
  csvExportAvailable: boolean;
}

interface ChatMessage {
  role: "user" | "assistant";
  content: string;
  timestamp: Date;
  drilldownData?: DrilldownData[];
}

type MetricConfig = {
  key: keyof OperationsMetrics;
  label: string;
  category: "Network Management" | "Supply Management";
  format: "number" | "percent" | "rating" | "hours";
  isPercentPoint?: boolean;
  invertVariance?: boolean;
};

const networkManagementMetrics: MetricConfig[] = [
  { key: "bookingsCompleted", label: "Bookings Completed", category: "Network Management", format: "number" },
  { key: "emergencies", label: "Emergencies", category: "Network Management", format: "number", invertVariance: true },
  { key: "deliveryRate", label: "Delivery Rate", category: "Network Management", format: "percent", isPercentPoint: true },
  { key: "defectPercent", label: "Defect %", category: "Network Management", format: "percent", isPercentPoint: true, invertVariance: true },
  { key: "overbookedPercent", label: "Overbooked %", category: "Network Management", format: "percent", isPercentPoint: true, invertVariance: true },
  { key: "avgRating", label: "Rating", category: "Network Management", format: "rating" },
  { key: "responseRate", label: "Response Rate", category: "Network Management", format: "percent", isPercentPoint: true },
  { key: "stripeMargin", label: "Margin", category: "Network Management", format: "percent", isPercentPoint: true },
];

const supplyManagementMetrics: MetricConfig[] = [
  { key: "activeVendors", label: "Active Vendors", category: "Supply Management", format: "number" },
  { key: "newVendors", label: "New Vendors", category: "Supply Management", format: "number" },
  { key: "dismissedVendors", label: "Dismissed Vendors", category: "Supply Management", format: "number", invertVariance: true },
  { key: "scheduledHours", label: "Scheduled Hours", category: "Supply Management", format: "hours" },
  { key: "utilization", label: "Utilization", category: "Supply Management", format: "percent", isPercentPoint: true },
];

function formatValue(value: number, format: "number" | "percent" | "rating" | "hours"): string {
  if (value === null || value === undefined || isNaN(value)) return "-";
  switch (format) {
    case "percent":
      return `${value.toFixed(1)}%`;
    case "rating":
      return value.toFixed(2);
    case "hours":
      return new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 }).format(value);
    case "number":
    default:
      return new Intl.NumberFormat("en-US").format(value);
  }
}

function VarianceBadge({ 
  value, 
  isPercentPoint,
  invertVariance 
}: { 
  value: number | null; 
  isPercentPoint?: boolean;
  invertVariance?: boolean;
}) {
  if (value === null || value === undefined) return null;
  
  let isPositive = value > 0;
  if (invertVariance) isPositive = value < 0;
  const isNeutral = value === 0;
  const displayValue = isPercentPoint 
    ? `${value > 0 ? "+" : ""}${value.toFixed(1)}pp` 
    : `${value > 0 ? "+" : ""}${value.toFixed(1)}%`;
  
  return (
    <Badge 
      variant="outline" 
      className={cn(
        "text-[10px] font-medium gap-0.5 px-1.5",
        isPositive && "text-green-600 border-green-200 bg-green-50",
        !isPositive && !isNeutral && "text-red-600 border-red-200 bg-red-50",
        isNeutral && "text-muted-foreground"
      )}
    >
      {isPositive && <TrendingUp className="h-3 w-3" />}
      {!isPositive && !isNeutral && <TrendingDown className="h-3 w-3" />}
      {isNeutral && <Minus className="h-3 w-3" />}
      {displayValue}
    </Badge>
  );
}

function CategoryIcon({ category }: { category: string }) {
  switch (category) {
    case "Network Management":
      return <Truck className="h-4 w-4 text-blue-500" />;
    case "Supply Management":
      return <Users className="h-4 w-4 text-purple-500" />;
    default:
      return <BarChart3 className="h-4 w-4 text-muted-foreground" />;
  }
}

function MetricsTable({ 
  metrics, 
  periods, 
  selectedPeriodIndex, 
  setSelectedPeriodIndex,
  category
}: { 
  metrics: MetricConfig[];
  periods: PeriodData[];
  selectedPeriodIndex: number;
  setSelectedPeriodIndex: (index: number) => void;
  category: string;
}) {
  return (
    <div className="overflow-x-auto">
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead className="sticky left-0 z-10 bg-card min-w-[180px]">
              Period
            </TableHead>
            {metrics.map((metric) => (
              <TableHead key={metric.key} className="text-right min-w-[120px]">
                <div className="flex flex-col items-end gap-1">
                  <span className="text-xs">{metric.label}</span>
                </div>
              </TableHead>
            ))}
          </TableRow>
        </TableHeader>
        <TableBody>
          {periods.map((period, index) => (
            <TableRow 
              key={period.periodLabel} 
              className={cn(
                "cursor-pointer transition-colors hover-elevate",
                index === selectedPeriodIndex && "bg-accent",
                index === 0 && index !== selectedPeriodIndex && "bg-muted"
              )}
              onClick={() => setSelectedPeriodIndex(index)}
              data-testid={`row-period-${category.toLowerCase().replace(/\s+/g, '-')}-${index}`}
            >
              <TableCell className={cn(
                "sticky left-0 z-10 font-medium",
                index === selectedPeriodIndex ? "bg-accent" : index === 0 ? "bg-muted" : "bg-card"
              )}>
                <div className="flex flex-col">
                  <span>{period.periodLabel}</span>
                  <div className="flex gap-1 mt-1">
                    {index === 0 && (
                      <Badge variant="secondary" className="text-[10px] w-fit">
                        Current
                      </Badge>
                    )}
                    {index === selectedPeriodIndex && (
                      <Badge variant="default" className="text-[10px] w-fit">
                        Selected
                      </Badge>
                    )}
                  </div>
                </div>
              </TableCell>
              {metrics.map((metric) => {
                const value = period.metrics[metric.key];
                const displayValue = typeof value === 'object' ? '-' : formatValue(value as number, metric.format);
                return (
                  <TableCell key={metric.key} className="text-right">
                    <div className="flex flex-col items-end gap-1">
                      <span className="font-medium">
                        {displayValue}
                      </span>
                      {period.variance && metric.key !== 'vendorLevelCounts' && (
                        <VarianceBadge 
                          value={period.variance[metric.key] as number | null} 
                          isPercentPoint={metric.isPercentPoint}
                          invertVariance={metric.invertVariance}
                        />
                      )}
                    </div>
                  </TableCell>
                );
              })}
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </div>
  );
}

export default function OperationsPerformance() {
  const [selectedDatabase, setSelectedDatabase] = useState<string>("");
  const [selectedPeriodIndex, setSelectedPeriodIndex] = useState<number>(0);
  const [periodType, setPeriodType] = useState<"weekly" | "monthly">("weekly");
  const [networkManagementOpen, setNetworkManagementOpen] = useState(true);
  const [supplyManagementOpen, setSupplyManagementOpen] = useState(true);
  const forceRefreshRef = useRef(false);
  
  // Chat state
  const [isChatOpen, setIsChatOpen] = useState(false);
  const [chatInput, setChatInput] = useState("");
  const [chatMessages, setChatMessages] = useState<ChatMessage[]>([]);
  const chatScrollRef = useRef<HTMLDivElement>(null);
  const { toast } = useToast();

  const { data: databases, isLoading: databasesLoading } = useQuery<DatabaseConnection[]>({
    queryKey: ["/api/databases"],
  });

  // Auto-select first database when available
  useEffect(() => {
    if (databases && databases.length > 0 && !selectedDatabase) {
      setSelectedDatabase(databases[0].name);
    }
  }, [databases, selectedDatabase]);

  const { 
    data: operationsData, 
    isLoading: operationsLoading, 
    refetch,
    isFetching 
  } = useQuery<OperationsPerformanceResponse>({
    queryKey: ["/api/operations-performance", selectedDatabase, periodType],
    queryFn: async () => {
      if (!selectedDatabase) return null;
      const refreshParam = forceRefreshRef.current ? "&refresh=true" : "";
      const response = await fetch(`/api/operations-performance/${encodeURIComponent(selectedDatabase)}?periodType=${periodType}${refreshParam}`);
      if (!response.ok) throw new Error("Failed to fetch operations data");
      const data = await response.json();
      forceRefreshRef.current = false;
      return data;
    },
    enabled: !!selectedDatabase,
    staleTime: 1000 * 60 * 5,
  });

  const handleRefresh = () => {
    forceRefreshRef.current = true;
    refetch();
  };

  const periods = operationsData?.periods || [];
  const selectedPeriod = periods[selectedPeriodIndex];

  // AI Chat mutation
  const chatMutation = useMutation({
    mutationFn: async (message: string) => {
      const response = await apiRequest("POST", `/api/operations-performance/${selectedDatabase}/chat`, {
        message,
        dashboardData: operationsData,
        selectedPeriod: selectedPeriod ? {
          periodLabel: selectedPeriod.periodLabel,
          periodStart: selectedPeriod.periodStart,
          periodEnd: selectedPeriod.periodEnd,
        } : undefined,
        periodType,
      });
      return response.json();
    },
    onSuccess: (data) => {
      setChatMessages((prev) => [
        ...prev,
        { 
          role: "assistant", 
          content: data.message, 
          timestamp: new Date(),
          drilldownData: data.drilldownData,
        },
      ]);
      scrollChatToBottom();
    },
    onError: (error) => {
      toast({
        title: "Chat Error",
        description: error instanceof Error ? error.message : "Failed to get AI response. Please try again.",
        variant: "destructive",
      });
    },
  });
  
  // CSV export handler for drilldown data
  const handleExportCSV = async (drilldown: DrilldownData) => {
    const params = new URLSearchParams({
      metricId: drilldown.metricId,
      periodStart: drilldown.periodStart,
      periodEnd: drilldown.periodEnd,
    });
    const url = `/api/operations-performance/${selectedDatabase}/drilldown-export?${params.toString()}`;
    
    try {
      const response = await fetch(url, { credentials: "include" });
      if (!response.ok) {
        if (response.status === 403) {
          toast({
            title: "Export Not Available",
            description: "You don't have permission to export this data.",
            variant: "destructive",
          });
        } else {
          toast({
            title: "Export Failed",
            description: "Failed to export data. Please try again.",
            variant: "destructive",
          });
        }
        return;
      }
      
      const blob = await response.blob();
      const downloadUrl = window.URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = downloadUrl;
      a.download = `${drilldown.metricId}_${drilldown.periodStart}_to_${drilldown.periodEnd}.csv`;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      window.URL.revokeObjectURL(downloadUrl);
    } catch (err) {
      toast({
        title: "Export Failed",
        description: "Failed to download the file. Please try again.",
        variant: "destructive",
      });
    }
  };

  const scrollChatToBottom = () => {
    setTimeout(() => {
      chatScrollRef.current?.scrollTo({ top: chatScrollRef.current.scrollHeight, behavior: "smooth" });
    }, 100);
  };

  const handleSendMessage = (messageOverride?: string) => {
    const messageToSend = messageOverride || chatInput.trim();
    if (!messageToSend || !selectedDatabase) return;
    
    setChatMessages((prev) => [
      ...prev,
      { role: "user", content: messageToSend, timestamp: new Date() },
    ]);
    setChatInput("");
    chatMutation.mutate(messageToSend);
    scrollChatToBottom();
  };

  return (
    <div className="min-h-screen bg-background">
      <Header />
      
      <div className="container mx-auto py-6 px-4 max-w-[1600px]">
        <div className="mb-6">
          <div className="flex items-center gap-4 mb-4">
            <Link href="/">
              <Button variant="ghost" size="sm" data-testid="button-back">
                <ArrowLeft className="h-4 w-4 mr-2" />
                Back
              </Button>
            </Link>
            <h1 className="text-2xl font-bold">Operations Performance Dashboard</h1>
          </div>
          
          <div className="flex flex-wrap items-center gap-4">
            <div className="flex items-center gap-2">
              <span className="text-sm text-muted-foreground">Database:</span>
              <Select
                value={selectedDatabase}
                onValueChange={(value) => {
                  setSelectedDatabase(value);
                  setSelectedPeriodIndex(0);
                }}
              >
                <SelectTrigger className="w-[200px]" data-testid="select-database">
                  <SelectValue placeholder="Select database" />
                </SelectTrigger>
                <SelectContent>
                  {databases?.map((db) => (
                    <SelectItem key={db.name} value={db.name}>
                      {db.name}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>

            <div className="flex items-center gap-2">
              <span className="text-sm text-muted-foreground">View:</span>
              <Select
                value={periodType}
                onValueChange={(value: "weekly" | "monthly") => {
                  setPeriodType(value);
                  setSelectedPeriodIndex(0);
                }}
              >
                <SelectTrigger className="w-[120px]" data-testid="select-period-type">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="weekly">Weekly</SelectItem>
                  <SelectItem value="monthly">Monthly</SelectItem>
                </SelectContent>
              </Select>
            </div>

            {selectedDatabase && (
              <Button
                variant="outline"
                size="sm"
                onClick={handleRefresh}
                disabled={isFetching}
                data-testid="button-refresh"
              >
                {isFetching ? (
                  <Loader2 className="h-4 w-4 animate-spin" />
                ) : (
                  <RefreshCw className="h-4 w-4" />
                )}
                <span className="ml-2">Refresh</span>
              </Button>
            )}
          </div>
        </div>

        {!selectedDatabase && (
          <Card>
            <CardContent className="py-12 text-center">
              <BarChart3 className="h-12 w-12 mx-auto mb-4 text-muted-foreground opacity-50" />
              <p className="text-muted-foreground">
                Select a database to view operations performance metrics
              </p>
            </CardContent>
          </Card>
        )}

        {selectedDatabase && operationsLoading && (
          <Card>
            <CardContent className="py-12 text-center">
              <Loader2 className="h-8 w-8 animate-spin mx-auto mb-4" />
              <p className="text-muted-foreground">Loading operations data...</p>
            </CardContent>
          </Card>
        )}

        {selectedDatabase && !operationsLoading && periods.length > 0 && (
          <div className="space-y-6">
            {/* Selected Period Summary */}
            {selectedPeriod && (
              <Card>
                <CardHeader className="pb-2">
                  <CardTitle className="text-lg flex items-center gap-2">
                    <Calendar className="h-5 w-5" />
                    {selectedPeriod.periodLabel}
                    <Badge variant="outline" className="ml-2">
                      {periodType === "weekly" ? "Week" : "Month"} Selected
                    </Badge>
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-6 gap-4">
                    <div className="p-3 bg-muted rounded-lg">
                      <div className="text-xs text-muted-foreground">Bookings Completed</div>
                      <div className="text-xl font-bold">{formatValue(selectedPeriod.metrics.bookingsCompleted, "number")}</div>
                    </div>
                    <div className="p-3 bg-muted rounded-lg">
                      <div className="text-xs text-muted-foreground">Delivery Rate</div>
                      <div className="text-xl font-bold">{formatValue(selectedPeriod.metrics.deliveryRate, "percent")}</div>
                    </div>
                    <div className="p-3 bg-muted rounded-lg">
                      <div className="text-xs text-muted-foreground">Rating</div>
                      <div className="text-xl font-bold flex items-center gap-1">
                        <Star className="h-4 w-4 text-yellow-500 fill-yellow-500" />
                        {formatValue(selectedPeriod.metrics.avgRating, "rating")}
                      </div>
                    </div>
                    <div className="p-3 bg-muted rounded-lg">
                      <div className="text-xs text-muted-foreground">Active Vendors</div>
                      <div className="text-xl font-bold">{formatValue(selectedPeriod.metrics.activeVendors, "number")}</div>
                    </div>
                    <div className="p-3 bg-muted rounded-lg">
                      <div className="text-xs text-muted-foreground">Utilization</div>
                      <div className="text-xl font-bold">{formatValue(selectedPeriod.metrics.utilization, "percent")}</div>
                    </div>
                    <div className="p-3 bg-muted rounded-lg">
                      <div className="text-xs text-muted-foreground">Margin</div>
                      <div className="text-xl font-bold">{formatValue(selectedPeriod.metrics.stripeMargin, "percent")}</div>
                    </div>
                  </div>
                  
                  {/* Vendor Level Breakdown */}
                  {selectedPeriod.metrics.vendorLevelCounts && Object.keys(selectedPeriod.metrics.vendorLevelCounts).length > 0 && (
                    <div className="mt-4">
                      <h4 className="text-sm font-medium mb-2">Vendor Level Distribution</h4>
                      <div className="flex flex-wrap gap-2">
                        {Object.entries(selectedPeriod.metrics.vendorLevelCounts).map(([level, count]) => (
                          <Badge key={level} variant="secondary" className="text-xs">
                            {level}: {count}
                          </Badge>
                        ))}
                      </div>
                    </div>
                  )}
                </CardContent>
              </Card>
            )}

            {/* Network Management Section */}
            <Collapsible open={networkManagementOpen} onOpenChange={setNetworkManagementOpen}>
              <Card>
                <CollapsibleTrigger asChild>
                  <CardHeader className="cursor-pointer hover-elevate pb-2">
                    <CardTitle className="text-lg flex items-center justify-between">
                      <div className="flex items-center gap-2">
                        <Truck className="h-5 w-5 text-blue-500" />
                        Network Management
                      </div>
                      <ChevronDown className={cn(
                        "h-5 w-5 transition-transform",
                        networkManagementOpen && "transform rotate-180"
                      )} />
                    </CardTitle>
                  </CardHeader>
                </CollapsibleTrigger>
                <CollapsibleContent>
                  <CardContent className="pt-0">
                    <MetricsTable
                      metrics={networkManagementMetrics}
                      periods={periods}
                      selectedPeriodIndex={selectedPeriodIndex}
                      setSelectedPeriodIndex={setSelectedPeriodIndex}
                      category="Network Management"
                    />
                  </CardContent>
                </CollapsibleContent>
              </Card>
            </Collapsible>

            {/* Supply Management Section */}
            <Collapsible open={supplyManagementOpen} onOpenChange={setSupplyManagementOpen}>
              <Card>
                <CollapsibleTrigger asChild>
                  <CardHeader className="cursor-pointer hover-elevate pb-2">
                    <CardTitle className="text-lg flex items-center justify-between">
                      <div className="flex items-center gap-2">
                        <Users className="h-5 w-5 text-purple-500" />
                        Supply Management
                      </div>
                      <ChevronDown className={cn(
                        "h-5 w-5 transition-transform",
                        supplyManagementOpen && "transform rotate-180"
                      )} />
                    </CardTitle>
                  </CardHeader>
                </CollapsibleTrigger>
                <CollapsibleContent>
                  <CardContent className="pt-0">
                    <MetricsTable
                      metrics={supplyManagementMetrics}
                      periods={periods}
                      selectedPeriodIndex={selectedPeriodIndex}
                      setSelectedPeriodIndex={setSelectedPeriodIndex}
                      category="Supply Management"
                    />
                  </CardContent>
                </CollapsibleContent>
              </Card>
            </Collapsible>
          </div>
        )}

        {selectedDatabase && !operationsLoading && periods.length === 0 && (
          <Card>
            <CardContent className="py-12 text-center">
              <AlertTriangle className="h-12 w-12 mx-auto mb-4 text-muted-foreground opacity-50" />
              <p className="text-muted-foreground">
                No operations data available for the selected period
              </p>
            </CardContent>
          </Card>
        )}
      </div>

      {/* Floating Chat Button */}
      {!isChatOpen && selectedDatabase && (
        <div className="fixed bottom-6 right-6 z-50">
          <Button
            size="icon"
            className="rounded-full shadow-lg"
            onClick={() => setIsChatOpen(true)}
            data-testid="button-open-chat"
          >
            <MessageSquare className="h-5 w-5" />
          </Button>
        </div>
      )}

      {/* Chat Panel */}
      {isChatOpen && (
        <div 
          className="fixed bottom-6 right-6 w-96 h-[500px] bg-card border rounded-lg shadow-xl flex flex-col z-50"
          data-testid="chat-panel"
        >
          <div className="p-4 border-b flex items-center justify-between bg-muted rounded-t-lg">
            <div className="flex items-center gap-2">
              <Bot className="h-5 w-5 text-primary" />
              <span className="font-semibold">Operations Assistant</span>
            </div>
            <Button 
              variant="ghost" 
              size="icon" 
              onClick={() => setIsChatOpen(false)}
              data-testid="button-close-chat"
            >
              <X className="h-4 w-4" />
            </Button>
          </div>
          
          <div 
            ref={chatScrollRef}
            className="flex-1 overflow-y-auto p-4 space-y-4"
          >
            {chatMessages.length === 0 && (
              <div className="text-center text-muted-foreground text-sm py-8">
                <Bot className="h-12 w-12 mx-auto mb-4 opacity-50" />
                <p className="font-medium">Ask about your operations</p>
                <p className="text-xs mt-2">
                  I can help you understand metrics, compare periods, and identify trends.
                </p>
                <div className="mt-4 space-y-2">
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() => handleSendMessage("How is this period performing compared to last period?")}
                    className="w-full justify-start text-xs h-auto py-2 whitespace-normal text-left"
                    data-testid="button-suggestion-compare"
                  >
                    "How is this period performing compared to last period?"
                  </Button>
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() => handleSendMessage("What are the key insights from the operations data?")}
                    className="w-full justify-start text-xs h-auto py-2 whitespace-normal text-left"
                    data-testid="button-suggestion-insights"
                  >
                    "What are the key insights from the operations data?"
                  </Button>
                </div>
              </div>
            )}
            
            {chatMessages.map((msg, index) => (
              <div
                key={index}
                className={cn(
                  "flex gap-2",
                  msg.role === "user" ? "justify-end" : "justify-start"
                )}
              >
                {msg.role === "assistant" && (
                  <div className="flex-shrink-0 h-8 w-8 rounded-full bg-primary/10 flex items-center justify-center">
                    <Bot className="h-4 w-4 text-primary" />
                  </div>
                )}
                <div
                  className={cn(
                    "max-w-[85%] rounded-lg px-3 py-2 text-sm",
                    msg.role === "user"
                      ? "bg-primary text-primary-foreground"
                      : "bg-muted"
                  )}
                >
                  <p className="whitespace-pre-wrap">{msg.content}</p>
                  
                  {msg.drilldownData && msg.drilldownData.length > 0 && (
                    <div className="mt-3 space-y-3" data-testid="drilldown-container">
                      {msg.drilldownData.map((drilldown, dIdx) => (
                        <div 
                          key={dIdx} 
                          className="border rounded bg-background p-2"
                          data-testid={`drilldown-preview-${drilldown.metricId}-${dIdx}`}
                        >
                          <div className="flex items-center justify-between flex-wrap gap-2 mb-2">
                            <div className="flex items-center gap-2">
                              <Table2 className="h-3 w-3 text-muted-foreground" />
                              <span className="text-xs font-medium" data-testid={`text-metric-name-${drilldown.metricId}`}>
                                {drilldown.metricName}
                              </span>
                            </div>
                            <div className="flex items-center gap-2">
                              <span className="text-xs text-muted-foreground" data-testid={`text-row-count-${drilldown.metricId}`}>
                                {drilldown.previewCount} of {drilldown.totalCount} rows
                              </span>
                              <Button
                                size="sm"
                                variant="outline"
                                onClick={() => handleExportCSV(drilldown)}
                                className="text-xs gap-1"
                                data-testid={`button-export-csv-${drilldown.metricId}`}
                              >
                                <Download className="h-3 w-3" />
                                CSV
                              </Button>
                            </div>
                          </div>
                          
                          <div className="overflow-x-auto max-h-40 border rounded">
                            <Table className="text-xs">
                              <TableHeader>
                                <TableRow>
                                  {drilldown.columns.map((col) => (
                                    <TableHead 
                                      key={col} 
                                      className="text-xs p-1 font-medium"
                                      data-testid={`header-${drilldown.metricId}-${col}`}
                                    >
                                      {col}
                                    </TableHead>
                                  ))}
                                </TableRow>
                              </TableHeader>
                              <TableBody>
                                {drilldown.rows.slice(0, 10).map((row, rIdx) => (
                                  <TableRow key={rIdx} data-testid={`row-${drilldown.metricId}-${rIdx}`}>
                                    {drilldown.columns.map((col) => (
                                      <TableCell 
                                        key={col} 
                                        className="p-1 truncate max-w-[100px] text-xs" 
                                        title={String(row[col] ?? "")}
                                        data-testid={`cell-${drilldown.metricId}-${rIdx}-${col}`}
                                      >
                                        {row[col] === null ? <span className="text-muted-foreground italic">NULL</span> : String(row[col] ?? "")}
                                      </TableCell>
                                    ))}
                                  </TableRow>
                                ))}
                              </TableBody>
                            </Table>
                            {drilldown.rows.length > 10 && (
                              <p className="text-xs text-muted-foreground text-center py-1">
                                Showing first 10 of {drilldown.previewCount} preview rows...
                              </p>
                            )}
                          </div>
                          
                          {drilldown.hasMore && (
                            <p className="text-xs text-muted-foreground mt-1" data-testid={`text-has-more-${drilldown.metricId}`}>
                              Download CSV to see all {drilldown.totalCount} rows
                            </p>
                          )}
                        </div>
                      ))}
                    </div>
                  )}
                </div>
                {msg.role === "user" && (
                  <div className="flex-shrink-0 h-8 w-8 rounded-full bg-primary flex items-center justify-center">
                    <User className="h-4 w-4 text-primary-foreground" />
                  </div>
                )}
              </div>
            ))}
            
            {chatMutation.isPending && (
              <div className="flex gap-2 justify-start">
                <div className="flex-shrink-0 h-8 w-8 rounded-full bg-primary/10 flex items-center justify-center">
                  <Bot className="h-4 w-4 text-primary" />
                </div>
                <div className="bg-muted rounded-lg px-3 py-2">
                  <Loader2 className="h-4 w-4 animate-spin" />
                </div>
              </div>
            )}
          </div>
          
          <div className="p-4 border-t">
            <form
              onSubmit={(e) => {
                e.preventDefault();
                handleSendMessage();
              }}
              className="flex gap-2"
            >
              <Input
                value={chatInput}
                onChange={(e) => setChatInput(e.target.value)}
                placeholder="Ask about operations..."
                disabled={chatMutation.isPending || !selectedDatabase}
                className="flex-1"
                data-testid="input-chat-message"
              />
              <Button 
                type="submit" 
                size="icon"
                disabled={!chatInput.trim() || chatMutation.isPending}
                data-testid="button-send-chat"
              >
                <Send className="h-4 w-4" />
              </Button>
            </form>
          </div>
        </div>
      )}
    </div>
  );
}
