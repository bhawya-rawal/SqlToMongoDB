import { pool } from "../db/pool";

export type PredicateDatatype = "integer" | "numeric" | "date" | "string";

export type HistogramCondition = {
  queried_selectivity: number;
  histogram_bounds: Record<string, number | string>;
};

export type HistogramData = {
  relation: string;
  attribute: string;
  datatype: PredicateDatatype;
  conditions: Record<string, HistogramCondition>; // operator -> condition
};

function parsePgArrayLiteral(text: string): string[] {
  const trimmed = text.trim();
  if (!trimmed.startsWith("{") || !trimmed.endsWith("}")) {
    // Fallback: best-effort split
    return trimmed.split(",").map((s) => s.trim()).filter(Boolean);
  }

  const inner = trimmed.slice(1, -1);
  const result: string[] = [];
  let i = 0;

  while (i < inner.length) {
    // Skip separators
    if (inner[i] === ",") {
      i += 1;
      continue;
    }

    // Quoted element
    if (inner[i] === '"') {
      i += 1;
      let value = "";
      while (i < inner.length) {
        const ch = inner[i];
        if (ch === "\\" && i + 1 < inner.length) {
          value += inner[i + 1];
          i += 2;
          continue;
        }
        if (ch === '"') {
          i += 1;
          break;
        }
        value += ch;
        i += 1;
      }
      result.push(value);
      continue;
    }

    // Unquoted element
    let start = i;
    while (i < inner.length && inner[i] !== ",") i += 1;
    const raw = inner.slice(start, i).trim();
    if (raw.length > 0) result.push(raw);
  }

  return result;
}

function normalizeDatatype(raw: string): PredicateDatatype {
  const lower = raw.toLowerCase();
  if (lower === "date") return "date";
  if (lower.includes("int")) return "integer";
  if (lower.includes("numeric") || lower.includes("double") || lower.includes("real") || lower.includes("decimal")) {
    return "numeric";
  }
  return "string";
}

function stripSqlLiteralQuotes(value: string): string {
  const trimmed = value.trim();
  if (trimmed.startsWith("date")) {
    // date 'YYYY-MM-DD'
    const m = trimmed.match(/date\s+'([^']+)'/i);
    if (m?.[1]) return m[1];
  }
  if (trimmed.startsWith("'") && trimmed.endsWith("'")) {
    return trimmed.slice(1, -1);
  }
  return trimmed;
}

function toComparableNumber(datatype: PredicateDatatype, value: number | string): number {
  if (datatype === "date") {
    const iso = String(value);
    const ms = Date.parse(`${iso}T00:00:00.000Z`);
    return Number.isFinite(ms) ? ms : NaN;
  }

  return Number(value);
}

function getSelectableNeighboringSelectivities(selectivity: number): number[] {
  const grid = [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0];
  const lower = grid.filter((v) => v <= selectivity).sort((a, b) => a - b);
  const higher = grid.filter((v) => v >= selectivity).sort((a, b) => a - b);

  const required: number[] = [];

  if (lower.length > 0) {
    const start = Math.max(lower.length - 2, 0);
    required.push(...lower.slice(start));
  }

  if (higher.length > 0) {
    const end = Math.min(higher.length, 2);
    required.push(...higher.slice(0, end));
  }

  return Array.from(new Set(required)).sort((a, b) => a - b);
}

export async function getAttributeDatatype(relation: string, attribute: string): Promise<PredicateDatatype> {
  const result = await pool.query(
    "SELECT data_type FROM information_schema.columns WHERE table_name = $1 AND column_name = $2;",
    [relation, attribute]
  );

  const raw = String(result.rows[0]?.data_type ?? "string");
  return normalizeDatatype(raw);
}

async function getHistogramBounds(relation: string, attribute: string): Promise<string[]> {
  const result = await pool.query(
    "SELECT histogram_bounds FROM pg_stats WHERE tablename = $1 AND attname = $2;",
    [relation, attribute]
  );

  const raw = result.rows[0]?.histogram_bounds;
  if (Array.isArray(raw)) {
    return raw.map((v) => String(v));
  }
  if (typeof raw === "string") {
    return parsePgArrayLiteral(raw);
  }

  return [];
}

export async function getHistogram(
  relation: string,
  attribute: string,
  comparisons: Array<{ operator: string; value: string }>
): Promise<HistogramData> {
  const datatype = await getAttributeDatatype(relation, attribute);

  // The original project only meaningfully supports numeric/date predicates for selectivity exploration.
  if (datatype === "string") {
    return { relation, attribute, datatype, conditions: {} };
  }

  const histogramRaw = await getHistogramBounds(relation, attribute);

  const histogram: Array<number | string> = histogramRaw.map((v) => {
    if (datatype === "integer") return Number.parseInt(v, 10);
    if (datatype === "numeric") return Number.parseFloat(v);
    if (datatype === "date") return v;
    return v;
  });

  const numericHistogram = histogram.map((v) => toComparableNumber(datatype, v));

  const numBuckets = Math.max(0, histogram.length - 1);
  if (numBuckets === 0) {
    return { relation, attribute, datatype, conditions: {} };
  }

  const conditions: Record<string, HistogramCondition> = {};

  for (const { operator, value } of comparisons) {
    const parsedValue = stripSqlLiteralQuotes(value);
    const typedValue: number | string =
      datatype === "integer"
        ? Number.parseInt(parsedValue, 10)
        : datatype === "numeric"
          ? Number.parseFloat(parsedValue)
          : datatype === "date"
            ? parsedValue
            : parsedValue;

    const vNum = toComparableNumber(datatype, typedValue);

    let leftBound = 0;
    for (let i = 0; i < numBuckets; i += 1) {
      const hNum = numericHistogram[i];
      if (Number.isFinite(vNum) && Number.isFinite(hNum) && vNum > hNum) {
        leftBound = i;
      }
    }

    const denom = numericHistogram[leftBound + 1] - numericHistogram[leftBound];
    const ratio = denom === 0 ? 0 : (vNum - numericHistogram[leftBound]) / denom;

    let selectivity = (leftBound + ratio) / numBuckets;

    if (operator === ">" || operator === ">=") {
      selectivity = 1 - selectivity;
    }

    if (selectivity <= 0) selectivity = 0;
    if (selectivity >= 1) selectivity = 1;

    const selectivitiesRequired = getSelectableNeighboringSelectivities(selectivity);

    const valuesRequired: Record<string, number | string> = {};
    for (const s of selectivitiesRequired) {
      const effective = operator === ">" || operator === ">=" ? 1 - s : s;
      const idx = Math.floor(effective * numBuckets);
      const clampedIdx = Math.min(Math.max(idx, 0), histogram.length - 1);
      valuesRequired[String(s)] = histogram[clampedIdx];
    }

    // Include the original queried value for replacement convenience (keyed by queried selectivity).
    valuesRequired[String(selectivity)] = typedValue;

    conditions[operator] = {
      queried_selectivity: selectivity,
      histogram_bounds: valuesRequired,
    };
  }

  return { relation, attribute, datatype, conditions };
}
