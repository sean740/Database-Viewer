
import "dotenv/config";
import { authStorage } from "./replit_integrations/auth/storage";
import bcrypt from "bcryptjs";

async function simulateLogin() {
    const email = "sean@washos.com";
    const password = "Tima11476";

    console.log(`Simulating login for ${email}...`);

    try {
        const user = await authStorage.getUserByEmail(email);

        if (!user) {
            console.log("LOGIN FAILED: User not found.");
            return;
        }

        if (!user.isActive) {
            console.log("LOGIN FAILED: User inactive.");
            return;
        }

        // Check password (mapped to password_digest in schema)
        // Note: In code we use 'user.password' because of Drizzle mapping
        if (!user.password) {
            console.log("LOGIN FAILED: User has no password hash.");
            return;
        }

        const isMatch = await bcrypt.compare(password, user.password);

        if (isMatch) {
            console.log("LOGIN SUCCESS! Credentials are valid.");
            console.log("User ID:", user.id);
        } else {
            console.log("LOGIN FAILED: Invalid password.");
        }

    } catch (error) {
        console.error("LOGIN CRASHED:", error);
    }
    process.exit(0);
}

simulateLogin();
