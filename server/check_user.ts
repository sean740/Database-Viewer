
import "dotenv/config";
import pg from "pg";

const { Pool } = pg;

const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false }
});

async function main() {
    const email = "sean@washos.com";
    try {
        const res = await pool.query(`
      SELECT id, email, password_digest 
      FROM users 
      WHERE email = $1
    `, [email]);

        if (res.rows.length === 0) {
            console.log("User not found via raw SQL");
        } else {
            console.log("User found via raw SQL:");
            console.log(res.rows[0]);
        }
    } catch (error) {
        console.error("Error inspecting user:", error);
    } finally {
        await pool.end();
    }
}

main();
