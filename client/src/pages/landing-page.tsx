import { Database, Shield, Users, ArrowRight } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";

export default function LandingPage() {
  return (
    <div className="min-h-screen bg-background">
      <nav className="fixed top-0 w-full border-b bg-background/80 backdrop-blur-sm z-50">
        <div className="container mx-auto px-4 h-16 flex items-center justify-between">
          <div className="flex items-center gap-2">
            <Database className="h-6 w-6 text-primary" />
            <span className="font-semibold text-lg">Database Viewer</span>
          </div>
          <Button asChild data-testid="button-login">
            <a href="/api/login">
              Sign In <ArrowRight className="ml-2 h-4 w-4" />
            </a>
          </Button>
        </div>
      </nav>

      <main className="pt-16">
        <section className="container mx-auto px-4 py-24 md:py-32">
          <div className="max-w-3xl mx-auto text-center space-y-8">
            <h1 className="text-4xl md:text-5xl font-bold tracking-tight">
              Explore Your Databases
              <span className="text-primary"> Safely</span>
            </h1>
            <p className="text-xl text-muted-foreground max-w-2xl mx-auto">
              A read-only database viewer for your team. Browse tables, filter data, 
              export to CSV, and query using natural language — all without the risk 
              of accidental modifications.
            </p>
            <div className="flex flex-col sm:flex-row gap-4 justify-center">
              <Button size="lg" asChild data-testid="button-get-started">
                <a href="/api/login">
                  Get Started <ArrowRight className="ml-2 h-4 w-4" />
                </a>
              </Button>
            </div>
          </div>
        </section>

        <section className="container mx-auto px-4 py-16">
          <div className="grid md:grid-cols-3 gap-6 max-w-5xl mx-auto">
            <Card className="bg-card/50 hover-elevate">
              <CardContent className="pt-6 space-y-4">
                <div className="h-12 w-12 rounded-lg bg-primary/10 flex items-center justify-center">
                  <Database className="h-6 w-6 text-primary" />
                </div>
                <h3 className="text-xl font-semibold">Browse Tables</h3>
                <p className="text-muted-foreground">
                  View all tables in your connected databases with paginated results 
                  and powerful filtering options.
                </p>
              </CardContent>
            </Card>

            <Card className="bg-card/50 hover-elevate">
              <CardContent className="pt-6 space-y-4">
                <div className="h-12 w-12 rounded-lg bg-primary/10 flex items-center justify-center">
                  <Shield className="h-6 w-6 text-primary" />
                </div>
                <h3 className="text-xl font-semibold">Read-Only Safety</h3>
                <p className="text-muted-foreground">
                  All queries are strictly read-only. Browse your data with confidence 
                  knowing nothing can be accidentally modified.
                </p>
              </CardContent>
            </Card>

            <Card className="bg-card/50 hover-elevate">
              <CardContent className="pt-6 space-y-4">
                <div className="h-12 w-12 rounded-lg bg-primary/10 flex items-center justify-center">
                  <Users className="h-6 w-6 text-primary" />
                </div>
                <h3 className="text-xl font-semibold">Role-Based Access</h3>
                <p className="text-muted-foreground">
                  Admins, team members, and external customers each get appropriate 
                  access levels to your data.
                </p>
              </CardContent>
            </Card>
          </div>
        </section>

        <footer className="container mx-auto px-4 py-8 border-t">
          <div className="text-center text-sm text-muted-foreground">
            Database Viewer — Secure, read-only database browsing
          </div>
        </footer>
      </main>
    </div>
  );
}
