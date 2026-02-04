
import "dotenv/config";
import pg from "pg";

const { Pool } = pg;

const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false }
});

async function main() {
    try {
        const res = await pool.query(`
      SELECT to_regclass('public.sessions');
    `);

        const exists = res.rows[0].to_regclass !== null;
        console.log(`Sessions table exists: ${exists}`);

    } catch (error) {
        console.error("Error checking table:", error);
    } finally {
        await pool.end();
    }
}

main();
