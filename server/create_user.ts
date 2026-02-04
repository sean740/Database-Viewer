
import "dotenv/config";
import { db } from "./db";
import { users } from "@shared/schema";
import bcrypt from "bcryptjs";

async function main() {
    const email = "sean@washos.com";
    // Hash for 'Tima11476'
    const passwordDigest = '$2b$10$rGlc4McF4NoWE9N8OfbwNeK20Moc3T8IxCjPeclH/WvBGDnYpUTRm';

    try {
        console.log(`Creating user ${email}...`);

        // We are using the Drizzle schema which maps 'password' to 'password_digest' column
        // The previous fix synced this.
        // Wait, did I revert the TS property 'password_digest' access?
        // Yes, 'shared/models/auth.ts' has: password: varchar("password_digest")

        await db.insert(users).values({
            email,
            password: passwordDigest,
            role: "admin", // Assuming admin role for the owner
            isActive: true
        }).onConflictDoUpdate({
            target: users.email,
            set: { password: passwordDigest }
        });

        console.log(`Success: User ${email} created/updated.`);
    } catch (error) {
        console.error("Error creating user:", error);
    }
    process.exit(0);
}

main();
