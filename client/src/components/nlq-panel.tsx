import { useState } from "react";
import { Sparkles, Loader2, ArrowRight } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Textarea } from "@/components/ui/textarea";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import type { NLQPlan } from "@/lib/types";
import { OPERATOR_LABELS } from "@/lib/types";

interface NLQPanelProps {
  isEnabled: boolean;
  selectedDatabase: string;
  selectedTable: string;
  onQueryParsed: (plan: NLQPlan) => void;
  lastPlan: NLQPlan | null;
}

export function NLQPanel({
  isEnabled,
  selectedDatabase,
  selectedTable,
  onQueryParsed,
  lastPlan,
}: NLQPanelProps) {
  const [query, setQuery] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [isLoading, setIsLoading] = useState(false);

  if (!isEnabled) {
    return null;
  }

  const handleAsk = async () => {
    if (!query.trim() || !selectedDatabase) return;

    setError(null);
    setIsLoading(true);

    try {
      const response = await fetch("/api/nlq", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          database: selectedDatabase,
          query: query.trim(),
          table: selectedTable || undefined,
        }),
      });

      if (!response.ok) {
        const data = await response.json();
        throw new Error(data.error || "Failed to process query");
      }

      const plan = await response.json();
      onQueryParsed(plan);
    } catch (err) {
      setError(err instanceof Error ? err.message : "An error occurred");
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <Card className="border-primary/20 bg-gradient-to-r from-primary/5 to-transparent">
      <CardHeader className="pb-3">
        <CardTitle className="flex items-center gap-2 text-base">
          <Sparkles className="h-5 w-5 text-primary" />
          Ask in Plain English
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-3">
        <div className="flex gap-2">
          <Textarea
            placeholder={`e.g., "show rows where status contains failed" or "find orders greater than 100"`}
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            className="resize-none min-h-[60px]"
            disabled={isLoading}
            data-testid="textarea-nlq-query"
          />
          <Button
            onClick={handleAsk}
            disabled={isLoading || !query.trim()}
            className="shrink-0"
            data-testid="button-nlq-ask"
          >
            {isLoading ? (
              <Loader2 className="h-4 w-4 animate-spin" />
            ) : (
              <>
                Ask <ArrowRight className="ml-1 h-4 w-4" />
              </>
            )}
          </Button>
        </div>

        {error && (
          <div className="text-sm text-destructive bg-destructive/10 p-2 rounded-md" data-testid="text-nlq-error">
            {error}
          </div>
        )}

        {lastPlan && (
          <div className="text-sm bg-muted/50 p-3 rounded-md space-y-2" data-testid="text-nlq-plan">
            <div className="font-medium">Interpreted query:</div>
            <div className="flex flex-wrap gap-2">
              <Badge variant="outline" className="font-mono">
                Table: {lastPlan.table}
              </Badge>
              {lastPlan.filters.map((f, idx) => (
                <Badge key={idx} variant="secondary" className="font-mono text-xs">
                  {f.column} {OPERATOR_LABELS[f.op]} "{f.value}"
                </Badge>
              ))}
              <Badge variant="outline">Page {lastPlan.page}</Badge>
            </div>
          </div>
        )}
      </CardContent>
    </Card>
  );
}
