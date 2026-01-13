import { useState, useRef, useEffect } from "react";
import { Sparkles, Loader2, Send, X, MessageSquare } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Card, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { ScrollArea } from "@/components/ui/scroll-area";
import type { NLQPlan } from "@/lib/types";
import { OPERATOR_LABELS } from "@/lib/types";

interface Message {
  id: string;
  role: "user" | "assistant";
  content: string;
  plan?: NLQPlan;
  isError?: boolean;
}

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
  const [messages, setMessages] = useState<Message[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [context, setContext] = useState("");
  const scrollRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [messages]);

  useEffect(() => {
    setMessages([]);
    setContext("");
  }, [selectedTable]);

  if (!isEnabled) {
    return null;
  }

  const handleAsk = async () => {
    if (!query.trim() || !selectedDatabase || !selectedTable) return;

    const userMessage: Message = {
      id: Date.now().toString(),
      role: "user",
      content: query.trim(),
    };

    setMessages((prev) => [...prev, userMessage]);
    setQuery("");
    setIsLoading(true);

    try {
      const response = await fetch("/api/nlq", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          database: selectedDatabase,
          query: query.trim(),
          table: selectedTable,
          context: context,
        }),
      });

      if (!response.ok) {
        const data = await response.json();
        throw new Error(data.error || "Failed to process query");
      }

      const plan: NLQPlan = await response.json();

      if (plan.needsClarification && plan.clarificationQuestion) {
        const assistantMessage: Message = {
          id: (Date.now() + 1).toString(),
          role: "assistant",
          content: plan.clarificationQuestion,
          plan: plan,
        };
        setMessages((prev) => [...prev, assistantMessage]);
        setContext((prev) => 
          prev + `\nUser asked: ${query.trim()}\nAssistant asked for clarification: ${plan.clarificationQuestion}`
        );
      } else {
        const summary = plan.summary || formatPlanSummary(plan);
        const assistantMessage: Message = {
          id: (Date.now() + 1).toString(),
          role: "assistant",
          content: summary,
          plan: plan,
        };
        setMessages((prev) => [...prev, assistantMessage]);
        setContext((prev) => 
          prev + `\nUser asked: ${query.trim()}\nApplied filters: ${summary}`
        );
        onQueryParsed(plan);
      }
    } catch (err) {
      const errorMessage: Message = {
        id: (Date.now() + 1).toString(),
        role: "assistant",
        content: err instanceof Error ? err.message : "An error occurred",
        isError: true,
      };
      setMessages((prev) => [...prev, errorMessage]);
    } finally {
      setIsLoading(false);
      inputRef.current?.focus();
    }
  };

  const formatPlanSummary = (plan: NLQPlan): string => {
    if (plan.filters.length === 0) {
      return "Showing all rows (no filters applied)";
    }
    const filterDescriptions = plan.filters.map(
      (f) => `${f.column} ${OPERATOR_LABELS[f.op]} "${f.value}"`
    );
    return `Filtering where ${filterDescriptions.join(" AND ")}`;
  };

  const handleClearConversation = () => {
    setMessages([]);
    setContext("");
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      handleAsk();
    }
  };

  const handleColumnClick = (column: string) => {
    setQuery((prev) => {
      const newQuery = prev.trim() ? `${prev.trim()} ${column}` : column;
      return newQuery;
    });
    inputRef.current?.focus();
  };

  return (
    <Card className="border-primary/20 bg-gradient-to-r from-primary/5 to-transparent">
      <CardContent className="p-4 space-y-3">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <Sparkles className="h-5 w-5 text-primary" />
            <span className="font-medium">AI Query Assistant</span>
            {selectedTable && (
              <Badge variant="outline" className="font-mono text-xs">
                {selectedTable}
              </Badge>
            )}
          </div>
          {messages.length > 0 && (
            <Button
              variant="ghost"
              size="sm"
              onClick={handleClearConversation}
              className="h-7 text-xs"
              data-testid="button-clear-chat"
            >
              <X className="h-3 w-3 mr-1" />
              Clear
            </Button>
          )}
        </div>

        {messages.length > 0 && (
          <ScrollArea className="h-48 rounded-md border bg-background/50 p-3" ref={scrollRef}>
            <div className="space-y-3">
              {messages.map((msg) => (
                <div
                  key={msg.id}
                  className={`flex ${msg.role === "user" ? "justify-end" : "justify-start"}`}
                >
                  <div
                    className={`max-w-[85%] rounded-lg px-3 py-2 text-sm ${
                      msg.role === "user"
                        ? "bg-primary text-primary-foreground"
                        : msg.isError
                        ? "bg-destructive/10 text-destructive border border-destructive/20"
                        : "bg-muted"
                    }`}
                    data-testid={`message-${msg.role}-${msg.id}`}
                  >
                    <div>{msg.content}</div>
                    {msg.plan && !msg.plan.needsClarification && msg.plan.filters.length > 0 && (
                      <div className="flex flex-wrap gap-1 mt-2">
                        {msg.plan.filters.map((f, idx) => (
                          <Badge key={idx} variant="secondary" className="font-mono text-xs">
                            {f.column} {OPERATOR_LABELS[f.op]} "{f.value}"
                          </Badge>
                        ))}
                      </div>
                    )}
                    {msg.plan?.needsClarification && msg.plan.ambiguousColumns && msg.plan.ambiguousColumns.length > 0 && (
                      <div className="flex flex-wrap gap-1 mt-2">
                        {msg.plan.ambiguousColumns.map((col) => (
                          <Button
                            key={col}
                            variant="outline"
                            size="sm"
                            className="h-6 text-xs"
                            onClick={() => handleColumnClick(col)}
                            data-testid={`button-column-${col}`}
                          >
                            {col}
                          </Button>
                        ))}
                      </div>
                    )}
                  </div>
                </div>
              ))}
              {isLoading && (
                <div className="flex justify-start">
                  <div className="bg-muted rounded-lg px-3 py-2">
                    <Loader2 className="h-4 w-4 animate-spin" />
                  </div>
                </div>
              )}
            </div>
          </ScrollArea>
        )}

        <div className="flex gap-2">
          <div className="relative flex-1">
            <MessageSquare className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
            <Input
              ref={inputRef}
              placeholder={
                selectedTable
                  ? `Ask about ${selectedTable}... (e.g., "show bookings from 2026")`
                  : "Select a table first"
              }
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              onKeyDown={handleKeyDown}
              disabled={isLoading || !selectedTable}
              className="pl-9"
              data-testid="input-nlq-query"
            />
          </div>
          <Button
            onClick={handleAsk}
            disabled={isLoading || !query.trim() || !selectedTable}
            data-testid="button-nlq-ask"
          >
            {isLoading ? (
              <Loader2 className="h-4 w-4 animate-spin" />
            ) : (
              <Send className="h-4 w-4" />
            )}
          </Button>
        </div>

        {lastPlan && lastPlan.filters.length > 0 && (
          <div className="flex items-center gap-2 text-xs text-muted-foreground">
            <span>Active AI filters:</span>
            <div className="flex flex-wrap gap-1">
              {lastPlan.filters.map((f, idx) => (
                <Badge key={idx} variant="secondary" className="font-mono text-xs">
                  {f.column} {OPERATOR_LABELS[f.op]} "{f.value}"
                </Badge>
              ))}
            </div>
          </div>
        )}
      </CardContent>
    </Card>
  );
}
