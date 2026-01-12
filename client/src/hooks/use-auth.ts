import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import type { User } from "@/lib/types";
import { apiRequest } from "@/lib/queryClient";

interface AuthResponse {
  user: User | null;
}

async function fetchUser(): Promise<User | null> {
  const response = await fetch("/api/auth/me", {
    credentials: "include",
  });

  if (!response.ok) {
    return null;
  }

  const data: AuthResponse = await response.json();
  return data.user;
}

async function loginRequest(credentials: { email: string; password: string }): Promise<User> {
  const response = await fetch("/api/login", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    credentials: "include",
    body: JSON.stringify(credentials),
  });

  if (!response.ok) {
    const data = await response.json();
    throw new Error(data.message || "Login failed");
  }

  const data = await response.json();
  return data.user;
}

async function registerRequest(data: {
  email: string;
  password: string;
  firstName?: string;
  lastName?: string;
}): Promise<User> {
  const response = await fetch("/api/register", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    credentials: "include",
    body: JSON.stringify(data),
  });

  if (!response.ok) {
    const data = await response.json();
    throw new Error(data.message || "Registration failed");
  }

  const result = await response.json();
  return result.user;
}

async function logoutRequest(): Promise<void> {
  const response = await fetch("/api/logout", {
    method: "POST",
    credentials: "include",
  });

  if (!response.ok) {
    throw new Error("Logout failed");
  }
}

export function useAuth() {
  const queryClient = useQueryClient();
  const { data: user, isLoading } = useQuery<User | null>({
    queryKey: ["/api/auth/me"],
    queryFn: fetchUser,
    retry: false,
    staleTime: 1000 * 60 * 5,
  });

  const loginMutation = useMutation({
    mutationFn: loginRequest,
    onSuccess: (user) => {
      queryClient.setQueryData(["/api/auth/me"], user);
    },
  });

  const registerMutation = useMutation({
    mutationFn: registerRequest,
    onSuccess: (user) => {
      queryClient.setQueryData(["/api/auth/me"], user);
    },
  });

  const logoutMutation = useMutation({
    mutationFn: logoutRequest,
    onSuccess: () => {
      queryClient.setQueryData(["/api/auth/me"], null);
      queryClient.clear();
    },
  });

  return {
    user,
    isLoading,
    isAuthenticated: !!user,
    login: loginMutation.mutateAsync,
    isLoggingIn: loginMutation.isPending,
    loginError: loginMutation.error,
    register: registerMutation.mutateAsync,
    isRegistering: registerMutation.isPending,
    registerError: registerMutation.error,
    logout: logoutMutation.mutate,
    isLoggingOut: logoutMutation.isPending,
  };
}
