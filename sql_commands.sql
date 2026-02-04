-- Run these commands in your Database Console (Neon, Vercel Storage, or pgAdmin)

-- 1. Add the password column (Run this if you haven't already)
ALTER TABLE users ADD COLUMN IF NOT EXISTS password TEXT;

-- 2. Update your user with the hashed password (Run this if you haven't already. Password: Tima11476)
UPDATE users 
SET password_digest = '$2b$10$rGlc4McF4NoWE9N8OfbwNeK20Moc3T8IxCjPeclH/WvBGDnYpUTRm' 
WHERE email = 'sean@washos.com';

-- 3. CRITICAL: Create the sessions table
-- The app is crashing because it cannot create this table automatically due to your permission settings.
CREATE TABLE IF NOT EXISTS "sessions" (
  "sid" varchar NOT NULL COLLATE "default",
  "sess" json NOT NULL,
  "expire" timestamp(6) NOT NULL
)
WITH (OIDS=FALSE);

ALTER TABLE "sessions" ADD CONSTRAINT "session_pkey" PRIMARY KEY ("sid") NOT DEFERRABLE INITIALLY IMMEDIATE;

CREATE INDEX IF NOT EXISTS "IDX_session_expire" ON "sessions" ("expire");
