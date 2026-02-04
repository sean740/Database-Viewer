
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
      SELECT column_name, data_type 
      FROM information_schema.columns 
      WHERE table_name = 'users';
    `);

        console.log("COLUMNS IN USERS TABLE:");
        console.table(res.rows);

    } catch (error) {
        console.error("Error inspecting schema:", error);
    } finally {
        await pool.end();
    }
}

main();
