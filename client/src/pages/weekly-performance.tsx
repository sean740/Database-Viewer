import { useState, useEffect, useRef } from "react";
import { useQuery, useMutation } from "@tanstack/react-query";
import { Link } from "wouter";
import { ArrowLeft, TrendingUp, TrendingDown, Minus, RefreshCw, Loader2, Calendar, DollarSign, Users, BarChart3, Percent, MessageCircle, Send, X, Bot, User, Download, Table2, MapPin, Check, ChevronDown, CreditCard, AlertCircle } from "lucide-react";
import { Header } from "@/components/header";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Input } from "@/components/ui/input";
import { useToast } from "@/hooks/use-toast";
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
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover";
import { cn } from "@/lib/utils";
import { apiRequest } from "@/lib/queryClient";
import type { DatabaseConnection } from "@/lib/types";

interface DrilldownData {
  metricId: string;
  subSourceId?: string;
  metricName: string;
  columns: string[];
  rows: Record<string, unknown>[];
  totalCount: number;
  previewCount: number;
  hasMore: boolean;
  weekStart: string;
  weekEnd: string;
}

interface ChatMessage {
  role: "user" | "assistant";
  content: string;
  timestamp: Date;
  drilldownData?: DrilldownData[];
}

interface WeekMetrics {
  bookingsCreated: number;
  bookingsDue: number;
  bookingsCompleted: number;
  avgPerDay: number;
  conversion: number;
  avgBookingPrice: number;
  totalRevenue: number;
  totalProfit: number;
  marginPercent: number;
  signups: number;
  newUsersWithBookings: number;
  newUserConversion: number;
  subscriptionRevenue: number;
  subscriptionFees: number;
  memberBookings: number;
  membershipRevenuePercent: number;
  newSubscriptions: number;
}

interface WeekData {
  weekLabel: string;
  weekStart: string;
  weekEnd: string;
  metrics: WeekMetrics;
  variance: WeekMetrics | null;
}

interface WeeklyPerformanceResponse {
  weeks: WeekData[];
  generatedAt: string;
  selectedZones?: string[] | null;
}

interface StripeMetrics {
  grossVolume: number;
  netVolume: number;
  refunds: number;
  disputes: number;
  transactionCount: number;
  refundCount: number;
  disputeCount: number;
}

interface StripeMetricsResponse {
  weekStart: string;
  weekEnd: string;
  metrics: StripeMetrics;
}

const metricConfig: {
  key: keyof WeekMetrics;
  label: string;
  category: string;
  format: "number" | "currency" | "percent" | "decimal";
  isPercentPoint?: boolean;
}[] = [
  { key: "bookingsCreated", label: "Bookings Created", category: "Bookings", format: "number" },
  { key: "bookingsDue", label: "Bookings Due", category: "Bookings", format: "number" },
  { key: "bookingsCompleted", label: "Bookings Completed", category: "Bookings", format: "number" },
  { key: "avgPerDay", label: "Avg Per Day", category: "Bookings", format: "decimal" },
  { key: "conversion", label: "Conversion (Done/Due)", category: "Bookings", format: "percent", isPercentPoint: true },
  { key: "avgBookingPrice", label: "Avg Booking Price", category: "Revenue", format: "currency" },
  { key: "totalRevenue", label: "Total Revenue", category: "Revenue", format: "currency" },
  { key: "totalProfit", label: "Gross Profit", category: "Revenue", format: "currency" },
  { key: "marginPercent", label: "Margin %", category: "Revenue", format: "percent", isPercentPoint: true },
  { key: "signups", label: "Sign Ups", category: "Users", format: "number" },
  { key: "newUsersWithBookings", label: "New Users (w/ Booking)", category: "Users", format: "number" },
  { key: "newUserConversion", label: "New User Conversion", category: "Users", format: "percent", isPercentPoint: true },
  { key: "subscriptionRevenue", label: "Subscription Revenue", category: "Membership", format: "currency" },
  { key: "subscriptionFees", label: "Subscription Fees", category: "Membership", format: "currency" },
  { key: "memberBookings", label: "Member Bookings", category: "Membership", format: "number" },
  { key: "membershipRevenuePercent", label: "% Revenue from Members", category: "Membership", format: "percent", isPercentPoint: true },
  { key: "newSubscriptions", label: "New Memberships", category: "Membership", format: "number" },
];

function formatValue(value: number, format: "number" | "currency" | "percent" | "decimal"): string {
  switch (format) {
    case "currency":
      return new Intl.NumberFormat("en-US", { style: "currency", currency: "USD" }).format(value);
    case "percent":
      return `${value.toFixed(1)}%`;
    case "decimal":
      return value.toFixed(2);
    case "number":
    default:
      return new Intl.NumberFormat("en-US").format(value);
  }
}

function VarianceBadge({ 
  value, 
  isPercentPoint 
}: { 
  value: number | null; 
  isPercentPoint?: boolean;
}) {
  if (value === null || value === undefined) return null;
  
  const isPositive = value > 0;
  const isNeutral = value === 0;
  const displayValue = isPercentPoint ? `${value > 0 ? "+" : ""}${value.toFixed(1)}pp` : `${value > 0 ? "+" : ""}${value.toFixed(1)}%`;
  
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
    case "Bookings":
      return <Calendar className="h-4 w-4 text-blue-500" />;
    case "Revenue":
      return <DollarSign className="h-4 w-4 text-green-500" />;
    case "Users":
      return <Users className="h-4 w-4 text-purple-500" />;
    case "Membership":
      return <BarChart3 className="h-4 w-4 text-orange-500" />;
    default:
      return <Percent className="h-4 w-4 text-muted-foreground" />;
  }
}

export default function WeeklyPerformance() {
  const [selectedDatabase, setSelectedDatabase] = useState<string>("");
  const [selectedWeekIndex, setSelectedWeekIndex] = useState<number>(0);
  const [selectedZones, setSelectedZones] = useState<string[]>([]);
  const [zonesPopoverOpen, setZonesPopoverOpen] = useState(false);
  const [isChatOpen, setIsChatOpen] = useState(false);
  const [chatMessages, setChatMessages] = useState<ChatMessage[]>([]);
  const [chatInput, setChatInput] = useState("");
  const forceRefreshRef = useRef(false);
  const chatScrollRef = useRef<HTMLDivElement>(null);
  const { toast } = useToast();
  
  const { data: databases = [], isLoading: databasesLoading } = useQuery<DatabaseConnection[]>({
    queryKey: ["/api/databases"],
  });
  
  // Fetch available zones for the selected database
  const { data: zonesData } = useQuery<string[]>({
    queryKey: ["/api/zones", selectedDatabase],
    queryFn: async () => {
      const response = await fetch(`/api/zones/${selectedDatabase}`, {
        credentials: "include",
      });
      if (!response.ok) {
        throw new Error("Failed to fetch zones");
      }
      return response.json();
    },
    enabled: !!selectedDatabase,
  });
  
  const availableZones = zonesData || [];
  
  // Auto-select first database using useEffect to avoid state updates during render
  useEffect(() => {
    if (databases.length > 0 && !selectedDatabase) {
      setSelectedDatabase(databases[0].name);
    }
  }, [databases, selectedDatabase]);
  
  // Reset selected week and zones when database changes
  useEffect(() => {
    setSelectedWeekIndex(0);
    setSelectedZones([]);
  }, [selectedDatabase]);
  
  // Build query string for zones filter
  const zonesQueryParam = selectedZones.length > 0 ? `?zones=${selectedZones.join(",")}` : "";
  
  const { 
    data: performanceData, 
    isLoading: dataLoading, 
    refetch,
    isRefetching 
  } = useQuery<WeeklyPerformanceResponse>({
    queryKey: ["/api/weekly-performance", selectedDatabase, selectedZones.join(",")],
    queryFn: async () => {
      const refreshParam = forceRefreshRef.current ? (zonesQueryParam ? "&refresh=true" : "?refresh=true") : "";
      const response = await fetch(`/api/weekly-performance/${selectedDatabase}${zonesQueryParam}${refreshParam}`, {
        credentials: "include",
      });
      if (!response.ok) {
        throw new Error("Failed to fetch weekly performance data");
      }
      const data = await response.json();
      forceRefreshRef.current = false;
      return data;
    },
    enabled: !!selectedDatabase,
  });
  
  const handleRefresh = () => {
    forceRefreshRef.current = true;
    refetch();
  };
  
  // Zone selection handlers
  const toggleZone = (zone: string) => {
    setSelectedZones((prev) =>
      prev.includes(zone)
        ? prev.filter((z) => z !== zone)
        : [...prev, zone]
    );
  };
  
  const clearZones = () => {
    setSelectedZones([]);
  };
  
  const selectAllZones = () => {
    setSelectedZones([...availableZones]);
  };
  
  const isLoading = databasesLoading || dataLoading;
  const weeks = performanceData?.weeks || [];
  
  // Get the selected week
  const selectedWeek = weeks[selectedWeekIndex];
  
  // Fetch Stripe status
  const { data: stripeStatus } = useQuery<{ connected: boolean }>({
    queryKey: ["/api/stripe-status"],
    queryFn: async () => {
      const response = await fetch("/api/stripe-status", {
        credentials: "include",
      });
      if (!response.ok) {
        return { connected: false };
      }
      return response.json();
    },
  });
  
  // Fetch Stripe metrics for the selected week
  const { 
    data: stripeMetricsData, 
    isLoading: stripeMetricsLoading,
    error: stripeMetricsError 
  } = useQuery<StripeMetricsResponse>({
    queryKey: ["/api/stripe-metrics", selectedWeek?.weekStart, selectedWeek?.weekEnd],
    queryFn: async () => {
      if (!selectedWeek) throw new Error("No week selected");
      const params = new URLSearchParams({
        weekStart: selectedWeek.weekStart,
        weekEnd: selectedWeek.weekEnd,
      });
      const response = await fetch(`/api/stripe-metrics?${params}`, {
        credentials: "include",
      });
      if (!response.ok) {
        const errorData = await response.json().catch(() => ({}));
        throw new Error(errorData.message || "Failed to fetch Stripe metrics");
      }
      return response.json();
    },
    enabled: !!selectedWeek && stripeStatus?.connected === true,
    retry: false,
  });
  
  // Get the comparison week (one after selected)
  const comparisonWeek = weeks[selectedWeekIndex + 1];
  
  // Group metrics by category
  const categories = ["Bookings", "Revenue", "Users", "Membership"];
  
  // Get the currently selected week data
  const selectedWeekData = performanceData?.weeks?.[selectedWeekIndex];
  
  // AI Chat mutation
  const chatMutation = useMutation({
    mutationFn: async (message: string) => {
      const response = await apiRequest("POST", `/api/weekly-performance/${selectedDatabase}/chat`, {
        message,
        dashboardData: performanceData,
        selectedWeek: selectedWeekData ? {
          weekLabel: selectedWeekData.weekLabel,
          weekStart: selectedWeekData.weekStart,
          weekEnd: selectedWeekData.weekEnd,
        } : undefined,
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
  
  // CSV export handler
  const handleExportCSV = (drilldown: DrilldownData) => {
    const params = new URLSearchParams({
      metricId: drilldown.metricId,
      weekStart: drilldown.weekStart,
      weekEnd: drilldown.weekEnd,
    });
    if (drilldown.subSourceId) {
      params.set("subSourceId", drilldown.subSourceId);
    }
    window.open(`/api/weekly-performance/${selectedDatabase}/drilldown-export?${params.toString()}`, "_blank");
  };
  
  const scrollChatToBottom = () => {
    setTimeout(() => {
      chatScrollRef.current?.scrollTo({ top: chatScrollRef.current.scrollHeight, behavior: "smooth" });
    }, 100);
  };
  
  const handleSendMessage = (messageOverride?: string) => {
    const messageToSend = messageOverride || chatInput.trim();
    if (!messageToSend || chatMutation.isPending) return;
    
    const userMessage: ChatMessage = {
      role: "user",
      content: messageToSend,
      timestamp: new Date(),
    };
    
    setChatMessages((prev) => [...prev, userMessage]);
    setChatInput("");
    chatMutation.mutate(messageToSend);
    scrollChatToBottom();
  };
  
  return (
    <div className="h-screen flex flex-col bg-background">
      <Header 
        databases={databases}
        selectedDatabase={selectedDatabase}
        onDatabaseChange={setSelectedDatabase}
        isLoading={databasesLoading}
        showDatabaseSelector={true}
      />
      
      <div className="flex-1 overflow-hidden flex flex-col">
        <div className="border-b bg-card px-6 py-4">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-4">
              <Link href="/">
                <Button variant="ghost" size="sm" className="gap-2" data-testid="button-back">
                  <ArrowLeft className="h-4 w-4" />
                  Back
                </Button>
              </Link>
              <div>
                <h1 className="text-2xl font-bold tracking-tight">Marketing Performance Dashboard</h1>
                <p className="text-sm text-muted-foreground">
                  Track key metrics week over week (Monday - Sunday, PST)
                </p>
              </div>
            </div>
            <div className="flex items-center gap-2">
              {/* Zone Filter */}
              {availableZones.length > 0 && (
                <Popover open={zonesPopoverOpen} onOpenChange={setZonesPopoverOpen}>
                  <PopoverTrigger asChild>
                    <Button
                      variant="outline"
                      size="sm"
                      className="gap-2"
                      data-testid="button-zone-filter"
                    >
                      <MapPin className="h-4 w-4" />
                      {selectedZones.length === 0 
                        ? "All Zones" 
                        : selectedZones.length === availableZones.length
                        ? "All Zones"
                        : `${selectedZones.length} Zone${selectedZones.length === 1 ? "" : "s"}`}
                      <ChevronDown className="h-3 w-3 opacity-50" />
                    </Button>
                  </PopoverTrigger>
                  <PopoverContent className="w-64 p-2" align="end">
                    <div className="space-y-2">
                      <div className="flex items-center justify-between px-2 pb-2 border-b">
                        <span className="text-sm font-medium">Filter by Zone</span>
                        <div className="flex gap-1">
                          <Button
                            variant="ghost"
                            size="sm"
                            className="h-6 px-2 text-xs"
                            onClick={selectAllZones}
                            data-testid="button-select-all-zones"
                          >
                            All
                          </Button>
                          <Button
                            variant="ghost"
                            size="sm"
                            className="h-6 px-2 text-xs"
                            onClick={clearZones}
                            data-testid="button-clear-zones"
                          >
                            Clear
                          </Button>
                        </div>
                      </div>
                      <div className="max-h-[280px] overflow-y-auto space-y-0.5">
                        {availableZones.map((zone) => (
                          <button
                            key={zone}
                            onClick={() => toggleZone(zone)}
                            className={cn(
                              "w-full flex items-center justify-between px-2 py-1.5 rounded text-sm",
                              "hover-elevate transition-colors",
                              selectedZones.includes(zone) && "bg-accent"
                            )}
                            data-testid={`zone-option-${zone}`}
                          >
                            <span>{zone}</span>
                            {selectedZones.includes(zone) && (
                              <Check className="h-4 w-4 text-primary" />
                            )}
                          </button>
                        ))}
                      </div>
                      {selectedZones.length > 0 && (
                        <div className="pt-2 border-t">
                          <p className="text-xs text-muted-foreground px-2">
                            Note: Zone filter applies to booking-related metrics only
                          </p>
                        </div>
                      )}
                    </div>
                  </PopoverContent>
                </Popover>
              )}
              
              <Button 
                variant="outline" 
                size="sm" 
                onClick={handleRefresh}
                disabled={isRefetching}
                className="gap-2"
                data-testid="button-refresh"
              >
                <RefreshCw className={cn("h-4 w-4", isRefetching && "animate-spin")} />
                Refresh
              </Button>
            </div>
          </div>
        </div>
        
        {isLoading ? (
          <div className="flex-1 flex items-center justify-center">
            <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
          </div>
        ) : !selectedDatabase ? (
          <div className="flex-1 flex items-center justify-center text-muted-foreground">
            Select a database to view performance data
          </div>
        ) : weeks.length === 0 ? (
          <div className="flex-1 flex items-center justify-center text-muted-foreground">
            No performance data available
          </div>
        ) : (
          <div className="flex-1 overflow-y-auto">
            <div className="p-6 space-y-6">
              {selectedWeek && (
                <div className="space-y-4">
                  <div className="flex items-center justify-between flex-wrap gap-2">
                    <div className="flex items-center gap-3">
                      <h2 className="text-lg font-semibold">
                        {selectedWeekIndex === 0 ? "Current Week" : "Selected Week"}: {selectedWeek.weekLabel}
                      </h2>
                      {selectedZones.length > 0 && selectedZones.length < availableZones.length && (
                        <Badge variant="outline" className="gap-1 text-xs" data-testid="badge-zone-filter">
                          <MapPin className="h-3 w-3" />
                          {selectedZones.length === 1 
                            ? selectedZones[0] 
                            : `${selectedZones.length} zones`}
                        </Badge>
                      )}
                    </div>
                    {selectedWeek.variance && comparisonWeek && (
                      <p className="text-sm text-muted-foreground">
                        Compared to previous week ({comparisonWeek.weekLabel})
                      </p>
                    )}
                  </div>
                  
                  <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
                    {categories.map((category) => (
                      <Card key={category}>
                        <CardHeader className="pb-2">
                          <CardTitle className="flex items-center gap-2 text-sm font-medium">
                            <CategoryIcon category={category} />
                            {category}
                          </CardTitle>
                        </CardHeader>
                        <CardContent className="space-y-3">
                          {metricConfig
                            .filter((m) => m.category === category)
                            .map((metric) => (
                              <div key={metric.key} className="flex items-center justify-between">
                                <span className="text-xs text-muted-foreground">{metric.label}</span>
                                <div className="flex items-center gap-2">
                                  <span className="text-sm font-medium">
                                    {formatValue(selectedWeek.metrics[metric.key], metric.format)}
                                  </span>
                                  {selectedWeek.variance && (
                                    <VarianceBadge 
                                      value={selectedWeek.variance[metric.key]} 
                                      isPercentPoint={metric.isPercentPoint}
                                    />
                                  )}
                                </div>
                              </div>
                            ))}
                        </CardContent>
                      </Card>
                    ))}
                  </div>
                  
                  {/* Stripe Financial Metrics Card */}
                  {stripeStatus?.connected && (
                    <Card className="mt-4" data-testid="card-stripe-metrics">
                      <CardHeader className="pb-2">
                        <CardTitle className="flex items-center gap-2 text-sm font-medium">
                          <CreditCard className="h-4 w-4 text-primary" />
                          Stripe Financial Metrics
                          {stripeMetricsLoading && <Loader2 className="h-3 w-3 animate-spin" />}
                        </CardTitle>
                      </CardHeader>
                      <CardContent>
                        {stripeMetricsError ? (
                          <div className="flex items-center gap-2 text-sm text-muted-foreground" data-testid="text-stripe-error">
                            <AlertCircle className="h-4 w-4 text-destructive" />
                            Unable to load Stripe data
                          </div>
                        ) : stripeMetricsLoading ? (
                          <div className="text-sm text-muted-foreground" data-testid="text-stripe-loading">Loading Stripe data...</div>
                        ) : stripeMetricsData?.metrics ? (
                          <div className="grid grid-cols-2 md:grid-cols-4 gap-4" data-testid="container-stripe-metrics">
                            <div className="space-y-1" data-testid="metric-gross-volume">
                              <span className="text-xs text-muted-foreground">Gross Volume</span>
                              <p className="text-lg font-semibold text-green-600 dark:text-green-400">
                                {formatValue(stripeMetricsData.metrics.grossVolume, "currency")}
                              </p>
                              <span className="text-[10px] text-muted-foreground">
                                {stripeMetricsData.metrics.transactionCount} transactions
                              </span>
                            </div>
                            <div className="space-y-1" data-testid="metric-net-volume">
                              <span className="text-xs text-muted-foreground">Net Volume</span>
                              <p className="text-lg font-semibold">
                                {formatValue(stripeMetricsData.metrics.netVolume, "currency")}
                              </p>
                              <span className="text-[10px] text-muted-foreground">
                                After Stripe fees
                              </span>
                            </div>
                            <div className="space-y-1" data-testid="metric-refunds">
                              <span className="text-xs text-muted-foreground">Refunds</span>
                              <p className="text-lg font-semibold text-red-600 dark:text-red-400">
                                {formatValue(stripeMetricsData.metrics.refunds, "currency")}
                              </p>
                              <span className="text-[10px] text-muted-foreground">
                                {stripeMetricsData.metrics.refundCount} refunds
                              </span>
                            </div>
                            <div className="space-y-1" data-testid="metric-disputes">
                              <span className="text-xs text-muted-foreground">Disputes</span>
                              <p className="text-lg font-semibold text-amber-600 dark:text-amber-400">
                                {formatValue(stripeMetricsData.metrics.disputes, "currency")}
                              </p>
                              <span className="text-[10px] text-muted-foreground">
                                {stripeMetricsData.metrics.disputeCount} disputes
                              </span>
                            </div>
                          </div>
                        ) : (
                          <div className="text-sm text-muted-foreground" data-testid="text-stripe-no-data">No data available</div>
                        )}
                        
                        {/* Revenue Comparison */}
                        {stripeMetricsData?.metrics && selectedWeek && (
                          <div className="mt-4 pt-4 border-t" data-testid="container-revenue-comparison">
                            <h4 className="text-xs font-medium text-muted-foreground mb-2">Revenue Comparison</h4>
                            <div className="grid grid-cols-3 gap-4 text-sm">
                              <div data-testid="metric-db-revenue">
                                <span className="text-xs text-muted-foreground">Database Revenue</span>
                                <p className="font-medium">{formatValue(selectedWeek.metrics.totalRevenue, "currency")}</p>
                              </div>
                              <div data-testid="metric-stripe-gross">
                                <span className="text-xs text-muted-foreground">Stripe Gross</span>
                                <p className="font-medium">{formatValue(stripeMetricsData.metrics.grossVolume, "currency")}</p>
                              </div>
                              <div data-testid="metric-difference">
                                <span className="text-xs text-muted-foreground">Difference</span>
                                <p className={cn(
                                  "font-medium",
                                  (stripeMetricsData.metrics.grossVolume - selectedWeek.metrics.totalRevenue) > 0 
                                    ? "text-green-600 dark:text-green-400" 
                                    : (stripeMetricsData.metrics.grossVolume - selectedWeek.metrics.totalRevenue) < 0 
                                      ? "text-red-600 dark:text-red-400" 
                                      : ""
                                )}>
                                  {formatValue(stripeMetricsData.metrics.grossVolume - selectedWeek.metrics.totalRevenue, "currency")}
                                </p>
                              </div>
                            </div>
                          </div>
                        )}
                      </CardContent>
                    </Card>
                  )}
                </div>
              )}
              
              <div className="space-y-4">
                <div className="flex items-center justify-between">
                  <h2 className="text-lg font-semibold">All Weeks</h2>
                  <p className="text-xs text-muted-foreground">Click a row to view details above</p>
                </div>
                <Card className="overflow-hidden">
                  <div className="overflow-x-auto">
                    <Table className="min-w-max">
                      <TableHeader>
                        <TableRow>
                          <TableHead className="sticky left-0 bg-card z-10 min-w-[140px] whitespace-nowrap">Week</TableHead>
                          {metricConfig.map((metric) => (
                            <TableHead key={metric.key} className="text-right min-w-[110px] whitespace-nowrap">
                              <div className="flex flex-col items-end">
                                <span className="text-xs">{metric.label}</span>
                                <span className="text-[10px] text-muted-foreground font-normal">
                                  {metric.category}
                                </span>
                              </div>
                            </TableHead>
                          ))}
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {weeks.map((week, index) => (
                          <TableRow 
                            key={week.weekLabel} 
                            className={cn(
                              "cursor-pointer transition-colors hover-elevate",
                              index === selectedWeekIndex && "bg-accent",
                              index === 0 && index !== selectedWeekIndex && "bg-muted"
                            )}
                            onClick={() => setSelectedWeekIndex(index)}
                            data-testid={`row-week-${index}`}
                          >
                            <TableCell className={cn(
                              "sticky left-0 z-10 font-medium",
                              index === selectedWeekIndex ? "bg-accent" : index === 0 ? "bg-muted" : "bg-card"
                            )}>
                              <div className="flex flex-col">
                                <span>{week.weekLabel}</span>
                                <div className="flex gap-1 mt-1">
                                  {index === 0 && (
                                    <Badge variant="secondary" className="text-[10px] w-fit">
                                      Current
                                    </Badge>
                                  )}
                                  {index === selectedWeekIndex && (
                                    <Badge variant="default" className="text-[10px] w-fit">
                                      Selected
                                    </Badge>
                                  )}
                                </div>
                              </div>
                            </TableCell>
                            {metricConfig.map((metric) => (
                              <TableCell key={metric.key} className="text-right">
                                <div className="flex flex-col items-end gap-1">
                                  <span className="font-medium">
                                    {formatValue(week.metrics[metric.key], metric.format)}
                                  </span>
                                  {week.variance && (
                                    <VarianceBadge 
                                      value={week.variance[metric.key]} 
                                      isPercentPoint={metric.isPercentPoint}
                                    />
                                  )}
                                </div>
                              </TableCell>
                            ))}
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  </div>
                </Card>
              </div>
              
              {performanceData?.generatedAt && (
                <p className="text-xs text-muted-foreground text-center">
                  Data generated at: {new Date(performanceData.generatedAt).toLocaleString("en-US", { timeZone: "America/Los_Angeles" })} PST
                </p>
              )}
            </div>
          </div>
        )}
      </div>
      
      {/* AI Chat Button */}
      {selectedDatabase && !isChatOpen && (
        <Button
          onClick={() => setIsChatOpen(true)}
          className="fixed bottom-6 right-6 shadow-lg z-50"
          size="icon"
          data-testid="button-open-chat"
        >
          <MessageCircle className="h-5 w-5" />
        </Button>
      )}
      
      {/* AI Chat Panel */}
      {isChatOpen && (
        <div className="fixed bottom-6 right-6 w-96 h-[500px] bg-card border rounded-lg shadow-xl z-50 flex flex-col">
          <div className="flex items-center justify-between p-4 border-b">
            <div className="flex items-center gap-2">
              <Bot className="h-5 w-5 text-primary" />
              <span className="font-semibold">Dashboard Assistant</span>
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
                <p className="font-medium">Ask about your dashboard</p>
                <p className="text-xs mt-2">
                  I can help you understand metrics, compare weeks, and identify trends.
                </p>
                <div className="mt-4 space-y-2">
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() => handleSendMessage("How is this week performing compared to last week?")}
                    className="w-full justify-start text-xs h-auto py-2 whitespace-normal text-left"
                    data-testid="button-suggestion-compare"
                  >
                    "How is this week performing compared to last week?"
                  </Button>
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() => handleSendMessage("What are the key insights from the current data?")}
                    className="w-full justify-start text-xs h-auto py-2 whitespace-normal text-left"
                    data-testid="button-suggestion-insights"
                  >
                    "What are the key insights from the current data?"
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
                                {drilldown.subSourceId && ` (${drilldown.subSourceId})`}
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
                placeholder="Ask about the dashboard..."
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
