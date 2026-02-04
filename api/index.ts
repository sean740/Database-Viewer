import { app, setupPromise } from "../server/index";
import type { Request, Response } from "express";

export default async function handler(req: Request, res: Response) {
    await setupPromise;
    app(req, res);
}
