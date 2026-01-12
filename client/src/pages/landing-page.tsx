import { useState } from "react";
import { Database, Shield, Users, ArrowRight, Loader2 } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { useAuth } from "@/hooks/use-auth";
import { useToast } from "@/hooks/use-toast";

export default function LandingPage() {
  const [activeTab, setActiveTab] = useState("login");
  const [loginEmail, setLoginEmail] = useState("");
  const [loginPassword, setLoginPassword] = useState("");
  const [registerEmail, setRegisterEmail] = useState("");
  const [registerPassword, setRegisterPassword] = useState("");
  const [registerFirstName, setRegisterFirstName] = useState("");
  const [registerLastName, setRegisterLastName] = useState("");
  
  const { login, isLoggingIn, register, isRegistering } = useAuth();
  const { toast } = useToast();

  const handleLogin = async (e: React.FormEvent) => {
    e.preventDefault();
    try {
      await login({ email: loginEmail, password: loginPassword });
    } catch (error: any) {
      toast({
        title: "Login failed",
        description: error.message || "Invalid email or password",
        variant: "destructive",
      });
    }
  };

  const handleRegister = async (e: React.FormEvent) => {
    e.preventDefault();
    try {
      await register({
        email: registerEmail,
        password: registerPassword,
        firstName: registerFirstName,
        lastName: registerLastName,
      });
    } catch (error: any) {
      toast({
        title: "Registration failed",
        description: error.message || "Could not create account",
        variant: "destructive",
      });
    }
  };

  return (
    <div className="min-h-screen bg-background">
      <nav className="fixed top-0 w-full border-b bg-background/80 backdrop-blur-sm z-50">
        <div className="container mx-auto px-4 h-16 flex items-center justify-between">
          <div className="flex items-center gap-2">
            <Database className="h-6 w-6 text-primary" />
            <span className="font-semibold text-lg">Database Viewer</span>
          </div>
        </div>
      </nav>

      <main className="pt-16">
        <section className="container mx-auto px-4 py-12 md:py-20">
          <div className="grid md:grid-cols-2 gap-12 items-center max-w-6xl mx-auto">
            <div className="space-y-6">
              <h1 className="text-4xl md:text-5xl font-bold tracking-tight">
                Explore Your Databases
                <span className="text-primary"> Safely</span>
              </h1>
              <p className="text-xl text-muted-foreground">
                A read-only database viewer for your team. Browse tables, filter data, 
                export to CSV, and query using natural language — all without the risk 
                of accidental modifications.
              </p>
              <div className="grid grid-cols-1 gap-4 pt-4">
                <div className="flex items-center gap-3">
                  <div className="h-10 w-10 rounded-lg bg-primary/10 flex items-center justify-center">
                    <Database className="h-5 w-5 text-primary" />
                  </div>
                  <span className="text-muted-foreground">Browse tables with pagination and filters</span>
                </div>
                <div className="flex items-center gap-3">
                  <div className="h-10 w-10 rounded-lg bg-primary/10 flex items-center justify-center">
                    <Shield className="h-5 w-5 text-primary" />
                  </div>
                  <span className="text-muted-foreground">Strict read-only access for safety</span>
                </div>
                <div className="flex items-center gap-3">
                  <div className="h-10 w-10 rounded-lg bg-primary/10 flex items-center justify-center">
                    <Users className="h-5 w-5 text-primary" />
                  </div>
                  <span className="text-muted-foreground">Role-based access control</span>
                </div>
              </div>
            </div>

            <Card className="mx-auto w-full max-w-md">
              <CardHeader className="text-center">
                <CardTitle>Welcome</CardTitle>
                <CardDescription>Sign in to access your databases</CardDescription>
              </CardHeader>
              <CardContent>
                <Tabs value={activeTab} onValueChange={setActiveTab}>
                  <TabsList className="grid w-full grid-cols-2">
                    <TabsTrigger value="login" data-testid="tab-login">Sign In</TabsTrigger>
                    <TabsTrigger value="register" data-testid="tab-register">Register</TabsTrigger>
                  </TabsList>
                  
                  <TabsContent value="login" className="space-y-4 mt-4">
                    <form onSubmit={handleLogin} className="space-y-4">
                      <div className="space-y-2">
                        <Label htmlFor="login-email">Email</Label>
                        <Input
                          id="login-email"
                          type="email"
                          placeholder="you@example.com"
                          value={loginEmail}
                          onChange={(e) => setLoginEmail(e.target.value)}
                          required
                          data-testid="input-login-email"
                        />
                      </div>
                      <div className="space-y-2">
                        <Label htmlFor="login-password">Password</Label>
                        <Input
                          id="login-password"
                          type="password"
                          placeholder="Enter your password"
                          value={loginPassword}
                          onChange={(e) => setLoginPassword(e.target.value)}
                          required
                          data-testid="input-login-password"
                        />
                      </div>
                      <Button 
                        type="submit" 
                        className="w-full" 
                        disabled={isLoggingIn}
                        data-testid="button-login"
                      >
                        {isLoggingIn ? (
                          <>
                            <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                            Signing in...
                          </>
                        ) : (
                          <>
                            Sign In <ArrowRight className="ml-2 h-4 w-4" />
                          </>
                        )}
                      </Button>
                    </form>
                  </TabsContent>
                  
                  <TabsContent value="register" className="space-y-4 mt-4">
                    <form onSubmit={handleRegister} className="space-y-4">
                      <div className="grid grid-cols-2 gap-4">
                        <div className="space-y-2">
                          <Label htmlFor="register-first-name">First Name</Label>
                          <Input
                            id="register-first-name"
                            type="text"
                            placeholder="John"
                            value={registerFirstName}
                            onChange={(e) => setRegisterFirstName(e.target.value)}
                            data-testid="input-register-first-name"
                          />
                        </div>
                        <div className="space-y-2">
                          <Label htmlFor="register-last-name">Last Name</Label>
                          <Input
                            id="register-last-name"
                            type="text"
                            placeholder="Doe"
                            value={registerLastName}
                            onChange={(e) => setRegisterLastName(e.target.value)}
                            data-testid="input-register-last-name"
                          />
                        </div>
                      </div>
                      <div className="space-y-2">
                        <Label htmlFor="register-email">Email</Label>
                        <Input
                          id="register-email"
                          type="email"
                          placeholder="you@example.com"
                          value={registerEmail}
                          onChange={(e) => setRegisterEmail(e.target.value)}
                          required
                          data-testid="input-register-email"
                        />
                      </div>
                      <div className="space-y-2">
                        <Label htmlFor="register-password">Password</Label>
                        <Input
                          id="register-password"
                          type="password"
                          placeholder="Create a password"
                          value={registerPassword}
                          onChange={(e) => setRegisterPassword(e.target.value)}
                          required
                          data-testid="input-register-password"
                        />
                      </div>
                      <Button 
                        type="submit" 
                        className="w-full" 
                        disabled={isRegistering}
                        data-testid="button-register"
                      >
                        {isRegistering ? (
                          <>
                            <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                            Creating account...
                          </>
                        ) : (
                          <>
                            Create Account <ArrowRight className="ml-2 h-4 w-4" />
                          </>
                        )}
                      </Button>
                    </form>
                  </TabsContent>
                </Tabs>
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
