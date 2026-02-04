
import "dotenv/config";
import pg from "pg";

const { Pool } = pg;

const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false }
});

async function main() {
    try {
        console.log("Dropping sessions table...");
        await pool.query(`DROP TABLE IF EXISTS "sessions" CASCADE;`);

        console.log("Dropping users table...");
        await pool.query(`DROP TABLE IF EXISTS "users" CASCADE;`);

        console.log("Dropping table_grants table...");
        await pool.query(`DROP TABLE IF EXISTS "table_grants" CASCADE;`);

        console.log("Dropping audit_logs table...");
        await pool.query(`DROP TABLE IF EXISTS "audit_logs" CASCADE;`);

        console.log("Tables dropped.");
    } catch (error) {
        console.error("Error dropping tables:", error);
    } finally {
        await pool.end();
    }
}

main();
