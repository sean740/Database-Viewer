import { AlertCircle, X } from "lucide-react";
import { Button } from "@/components/ui/button";

interface ErrorBannerProps {
  message: string;
  onDismiss: () => void;
}

export function ErrorBanner({ message, onDismiss }: ErrorBannerProps) {
  return (
    <div
      className="bg-destructive/10 border border-destructive/20 text-destructive px-4 py-3 flex items-center justify-between gap-4"
      role="alert"
      data-testid="banner-error"
    >
      <div className="flex items-center gap-3">
        <AlertCircle className="h-5 w-5 shrink-0" />
        <span className="text-sm">{message}</span>
      </div>
      <Button
        variant="ghost"
        size="icon"
        onClick={onDismiss}
        className="shrink-0 text-destructive hover:text-destructive"
        data-testid="button-dismiss-error"
      >
        <X className="h-4 w-4" />
      </Button>
    </div>
  );
}
