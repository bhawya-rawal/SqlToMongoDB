import { pool } from "../db/pool";

export type ExplainPlanRoot = Record<string, unknown>;

function tryParseJson(value: unknown): unknown {
  if (typeof value !== "string") return value;
  try {
    return JSON.parse(value);
  } catch {
    return value;
  }
}

export async function explainQuery(sql: string): Promise<ExplainPlanRoot> {
  const explainSql = `EXPLAIN (COSTS, VERBOSE, BUFFERS, FORMAT JSON) ${sql}`;

  const queryTimeoutMsRaw = Number(process.env.DB_QUERY_TIMEOUT_MS ?? 15000);
  const queryTimeoutMs = Number.isFinite(queryTimeoutMsRaw) ? queryTimeoutMsRaw : 15000;

  // Use a transaction-scoped statement_timeout so the server cancels the work.
  // This avoids relying on client-side query timeout behavior (and keeps TS types happy).
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    await client.query(`SET LOCAL statement_timeout = ${Math.max(0, Math.floor(queryTimeoutMs))}`);
    const result = await client.query(explainSql);
    await client.query("COMMIT");

    if (!result.rows[0]) {
      throw new Error("No rows returned from EXPLAIN");
    }

    const firstRow = result.rows[0] as Record<string, unknown>;
    const queryPlan =
      firstRow["QUERY PLAN"] ?? firstRow["query_plan"] ?? firstRow["Plan"] ?? firstRow["plan"];

    const parsed = tryParseJson(queryPlan);

    // Typical shape from Postgres is: [ { Plan: {...}, ... } ]
    if (Array.isArray(parsed) && parsed.length > 0) {
      const first = parsed[0];
      if (typeof first === "object" && first !== null) {
        return first as ExplainPlanRoot;
      }
    }

    if (typeof parsed === "object" && parsed !== null) {
      return parsed as ExplainPlanRoot;
    }

    throw new Error("Unexpected EXPLAIN JSON shape");
  } catch (e) {
    try {
      await client.query("ROLLBACK");
    } catch {
      // ignore rollback failures
    }
    throw e;
  } finally {
    client.release();
  }
}
