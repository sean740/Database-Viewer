
import "dotenv/config";
import pg from "pg";
import bcrypt from "bcryptjs";

const { Pool } = pg;

if (!process.env.DATABASE_URL) {
    throw new Error("DATABASE_URL is not set");
}

const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false }
});

async function main() {
    const email = "sean@washos.com";
    const newPassword = "Tima11476";

    try {
        console.log("1. Adding password column if missing...");
        await pool.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS password TEXT;`);
        console.log("Column 'password' ensured.");

        console.log("2. Hashing password...");
        const salt = await bcrypt.genSalt(10);
        const hashedPassword = await bcrypt.hash(newPassword, salt);

        console.log(`3. Updating user ${email}...`);
        const res = await pool.query(
            `UPDATE users SET password = $1 WHERE email = $2`,
            [hashedPassword, email]
        );

        if (res.rowCount === 0) {
            console.log(`Warning: User ${email} not found.`);
        } else {
            console.log(`Success: Password set for ${email}.`);
        }

    } catch (error) {
        console.error("Error setting password:", error);
    } finally {
        await pool.end();
    }
}

main();
