import { useState, useEffect } from "react";
import { useQuery } from "@tanstack/react-query";
import { Link } from "wouter";
import { ArrowLeft, TrendingUp, TrendingDown, Minus, RefreshCw, Loader2, Calendar, DollarSign, Users, BarChart3, Percent } from "lucide-react";
import { Header } from "@/components/header";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { ScrollArea } from "@/components/ui/scroll-area";
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
import { cn } from "@/lib/utils";
import type { DatabaseConnection } from "@/lib/types";

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
  
  const { data: databases = [], isLoading: databasesLoading } = useQuery<DatabaseConnection[]>({
    queryKey: ["/api/databases"],
  });
  
  // Auto-select first database using useEffect to avoid state updates during render
  useEffect(() => {
    if (databases.length > 0 && !selectedDatabase) {
      setSelectedDatabase(databases[0].name);
    }
  }, [databases, selectedDatabase]);
  
  // Reset selected week when database changes
  useEffect(() => {
    setSelectedWeekIndex(0);
  }, [selectedDatabase]);
  
  const { 
    data: performanceData, 
    isLoading: dataLoading, 
    refetch,
    isRefetching 
  } = useQuery<WeeklyPerformanceResponse>({
    queryKey: ["/api/weekly-performance", selectedDatabase],
    enabled: !!selectedDatabase,
  });
  
  const isLoading = databasesLoading || dataLoading;
  const weeks = performanceData?.weeks || [];
  
  // Get the selected week and the one after it for comparison
  const selectedWeek = weeks[selectedWeekIndex];
  const comparisonWeek = weeks[selectedWeekIndex + 1];
  
  // Group metrics by category
  const categories = ["Bookings", "Revenue", "Users", "Membership"];
  
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
                <h1 className="text-2xl font-bold tracking-tight">Weekly Performance Dashboard</h1>
                <p className="text-sm text-muted-foreground">
                  Track key metrics week over week (Monday - Sunday, PST)
                </p>
              </div>
            </div>
            <Button 
              variant="outline" 
              size="sm" 
              onClick={() => refetch()}
              disabled={isRefetching}
              className="gap-2"
              data-testid="button-refresh"
            >
              <RefreshCw className={cn("h-4 w-4", isRefetching && "animate-spin")} />
              Refresh
            </Button>
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
          <ScrollArea className="flex-1">
            <div className="p-6 space-y-6">
              {selectedWeek && (
                <div className="space-y-4">
                  <div className="flex items-center justify-between">
                    <h2 className="text-lg font-semibold">
                      {selectedWeekIndex === 0 ? "Current Week" : "Selected Week"}: {selectedWeek.weekLabel}
                    </h2>
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
                </div>
              )}
              
              <div className="space-y-4">
                <div className="flex items-center justify-between">
                  <h2 className="text-lg font-semibold">All Weeks</h2>
                  <p className="text-xs text-muted-foreground">Click a row to view details above</p>
                </div>
                <Card>
                  <ScrollArea className="w-full">
                    <Table>
                      <TableHeader>
                        <TableRow>
                          <TableHead className="sticky left-0 bg-card z-10 min-w-[140px]">Week</TableHead>
                          {metricConfig.map((metric) => (
                            <TableHead key={metric.key} className="text-right min-w-[120px]">
                              <div className="flex flex-col items-end">
                                <span>{metric.label}</span>
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
                              index === selectedWeekIndex && "bg-accent/50",
                              index === 0 && index !== selectedWeekIndex && "bg-muted/30"
                            )}
                            onClick={() => setSelectedWeekIndex(index)}
                            data-testid={`row-week-${index}`}
                          >
                            <TableCell className={cn(
                              "sticky left-0 z-10 font-medium",
                              index === selectedWeekIndex ? "bg-accent/50" : index === 0 ? "bg-muted/30" : "bg-card"
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
                  </ScrollArea>
                </Card>
              </div>
              
              {performanceData?.generatedAt && (
                <p className="text-xs text-muted-foreground text-center">
                  Data generated at: {new Date(performanceData.generatedAt).toLocaleString("en-US", { timeZone: "America/Los_Angeles" })} PST
                </p>
              )}
            </div>
          </ScrollArea>
        )}
      </div>
    </div>
  );
}
