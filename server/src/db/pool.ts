import pg from "pg";

const { Pool } = pg;

function getEnvNumber(name: string, fallback: number): number {
  const raw = process.env[name];
  if (!raw) return fallback;
  const n = Number(raw);
  return Number.isFinite(n) ? n : fallback;
}

export const pool = new Pool({
  host: process.env.DB_HOST ?? "localhost",
  database: process.env.DB_NAME ?? "TPC-H",
  user: process.env.DB_USER ?? "postgres",
  password: process.env.DB_PASSWORD ?? "postgres",
  port: getEnvNumber("DB_PORT", 5432),
  // Fail fast when Postgres is unreachable/misconfigured.
  connectionTimeoutMillis: getEnvNumber("DB_CONNECTION_TIMEOUT_MS", 5000),
  idleTimeoutMillis: getEnvNumber("DB_IDLE_TIMEOUT_MS", 30000),
});
