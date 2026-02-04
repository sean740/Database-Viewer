import "dotenv/config";
import express from "express";
import { registerRoutes } from "./routes";

export const app = express();
app.use(express.json());
app.use(express.urlencoded({ extended: false }));

let serverInitialized = false;

export async function registerAndSetup() {
    if (serverInitialized) return app;

    // Pass the app (which acts as the server in serverless) 
    // Note: Vercel serverless doesn't standardly have an httpServer instance
    // but registerRoutes expects one. We can cast app to any here 
    // or refactor registerRoutes if strictly necessary, but typically 
    // passing the app as the server is enough for express routes.
    await registerRoutes(app as any, app);

    serverInitialized = true;
    return app;
}
