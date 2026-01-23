import pg from "pg";
import fs from "fs";

async function exportCreditPacks() {
  // Get database URL from environment
  const dbUrlsRaw = process.env.DATABASE_URLS;
  if (!dbUrlsRaw) {
    console.error("DATABASE_URLS not set");
    process.exit(1);
  }

  let connectionString: string;
  try {
    const parsed = JSON.parse(dbUrlsRaw);
    connectionString = Array.isArray(parsed) ? parsed[0].url : dbUrlsRaw;
  } catch {
    connectionString = dbUrlsRaw;
  }

  const pool = new pg.Pool({
    connectionString,
    ssl: { rejectUnauthorized: false },
  });

  // Jan 12-18 week boundaries (Monday 00:00:00 PST to Monday 00:00:00 PST)
  const weekStart = "2026-01-12T08:00:00.000Z";
  const weekEnd = "2026-01-19T08:00:00.000Z";

  console.log(`Fetching credit pack transactions from ${weekStart} to ${weekEnd}...`);

  const result = await pool.query(`
    SELECT 
      uct.id as transaction_id,
      uct.user_id,
      uct.amount as transaction_amount,
      uct.created_at as transaction_created_at,
      uct.user_credits_transaction_type_id as type_id,
      cp.id as credit_pack_id,
      cp.get_amount,
      cp.pay_amount
    FROM public.user_credits_transactions uct
    LEFT JOIN public.credits_packs cp ON uct.amount = cp.get_amount
    WHERE uct.created_at >= $1 AND uct.created_at < $2
      AND uct.user_credits_transaction_type_id = 16
    ORDER BY uct.created_at
  `, [weekStart, weekEnd]);

  console.log(`Found ${result.rows.length} rows`);

  // Generate CSV
  const headers = ["transaction_id", "user_id", "transaction_amount", "transaction_created_at", "type_id", "credit_pack_id", "get_amount", "pay_amount"];
  const csvRows = [headers.join(",")];

  let totalPayAmount = 0;
  for (const row of result.rows) {
    if (row.pay_amount) {
      totalPayAmount += parseFloat(row.pay_amount);
    }
    csvRows.push(headers.map(h => {
      const val = row[h];
      if (val === null || val === undefined) return "";
      if (typeof val === "string" && (val.includes(",") || val.includes('"'))) {
        return `"${val.replace(/"/g, '""')}"`;
      }
      return String(val);
    }).join(","));
  }

  const filename = "credit_packs_jan12_18.csv";
  fs.writeFileSync(filename, csvRows.join("\n"));
  console.log(`Exported to ${filename}`);
  console.log(`Total pay_amount sum: $${totalPayAmount.toFixed(2)}`);

  await pool.end();
}

exportCreditPacks().catch(console.error);
