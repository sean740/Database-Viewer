// @ts-ignore
// Import from the bundled file we created
import { app, registerAndSetup } from './_lib/index.vercel.js';

export default async function handler(req: any, res: any) {
    await registerAndSetup();
    app(req, res);
}
