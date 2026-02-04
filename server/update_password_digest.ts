
import "dotenv/config";
import pg from "pg";

const { Pool } = pg;

const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false }
});

async function main() {
    // Hash for 'Tima11476' generated previously
    const newHash = '$2b$10$rGlc4McF4NoWE9N8OfbwNeK20Moc3T8IxCjPeclH/WvBGDnYpUTRm';
    const email = 'sean@washos.com';

    try {
        console.log(`Updating password for ${email}...`);
        const res = await pool.query(
            `UPDATE users SET password_digest = $1 WHERE email = $2`,
            [newHash, email]
        );

        if (res.rowCount === 0) {
            console.log(`Warning: User ${email} not found.`);
        } else {
            console.log(`Success: Password updated for ${email}.`);
        }

    } catch (error) {
        console.error("Error setting password:", error);
    } finally {
        await pool.end();
    }
}

main();
