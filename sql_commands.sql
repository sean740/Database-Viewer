-- Run these commands in your Database Console (Neon, Vercel Storage, or pgAdmin)

-- 1. Add the password column (Your current database user doesn't have permission to do this via the app)
ALTER TABLE users ADD COLUMN IF NOT EXISTS password TEXT;

-- 2. Update your user with the hashed password (Password: Tima11476)
UPDATE users 
SET password = '$2b$10$rGlc4McF4NoWE9N8OfbwNeK20Moc3T8IxCjPeclH/WvBGDnYpUTRm' 
WHERE email = 'sean@washos.com';
