export type Comparison = {
  operator: string;
  value: string;
};

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function isWordBoundaryChar(ch: string | undefined): boolean {
  if (!ch) return true;
  return !/[A-Za-z0-9_]/.test(ch);
}

function findKeywordAtDepth0(lowerSql: string, keyword: string, startIndex = 0): number {
  let depth = 0;
  let inString = false;

  for (let i = startIndex; i <= lowerSql.length - keyword.length; i += 1) {
    const ch = lowerSql[i];

    // Handle single-quoted strings.
    if (ch === "'") {
      if (inString) {
        // SQL escapes single quotes by doubling: ''
        if (lowerSql[i + 1] === "'") {
          i += 1;
        } else {
          inString = false;
        }
      } else {
        inString = true;
      }
      continue;
    }

    if (inString) continue;

    if (ch === "(") depth += 1;
    if (ch === ")") depth = Math.max(0, depth - 1);

    if (depth !== 0) continue;

    if (lowerSql.startsWith(keyword, i)) {
      const before = lowerSql[i - 1];
      const after = lowerSql[i + keyword.length];
      if (isWordBoundaryChar(before) && isWordBoundaryChar(after)) {
        return i;
      }
    }
  }

  return -1;
}

export function normalizeSql(sql: string): string {
  // Remove Postgres date keyword: date 'YYYY-MM-DD' -> 'YYYY-MM-DD'
  const withoutDateKeyword = sql.replace(/\bdate\s+'(\d{4}-\d{2}-\d{2})'/gi, "'$1'");
  return withoutDateKeyword.replace(/[\t\r\n]+/g, " ").replace(/\s+/g, " ").trim();
}

export function extractTopLevelWhereClause(sql: string): string | null {
  const lower = sql.toLowerCase();

  const whereIndex = findKeywordAtDepth0(lower, "where");
  if (whereIndex === -1) return null;

  const clauseStart = whereIndex + "where".length;

  const endCandidates = [
    findKeywordAtDepth0(lower, "group by", clauseStart),
    findKeywordAtDepth0(lower, "order by", clauseStart),
    findKeywordAtDepth0(lower, "limit", clauseStart),
    findKeywordAtDepth0(lower, "having", clauseStart),
  ].filter((idx) => idx !== -1);

  const clauseEnd = endCandidates.length > 0 ? Math.min(...endCandidates) : sql.length;

  return sql.slice(clauseStart, clauseEnd).trim();
}

export function stripParenthesizedContent(sqlFragment: string): string {
  let depth = 0;
  let inString = false;
  const chars = sqlFragment.split("");

  for (let i = 0; i < chars.length; i += 1) {
    const ch = chars[i];

    if (ch === "'") {
      if (inString) {
        if (chars[i + 1] === "'") {
          i += 1;
        } else {
          inString = false;
        }
      } else {
        inString = true;
      }
      continue;
    }

    if (inString) continue;

    if (ch === "(") {
      depth += 1;
      chars[i] = " ";
      continue;
    }
    if (ch === ")") {
      depth = Math.max(0, depth - 1);
      chars[i] = " ";
      continue;
    }

    if (depth > 0) {
      chars[i] = " ";
    }
  }

  return chars.join("");
}

export function extractTopLevelComparisons(sql: string, predicate: string): Comparison[] {
  const normalized = normalizeSql(sql);
  const whereClause = extractTopLevelWhereClause(normalized);
  if (!whereClause) return [];

  const depth0Only = stripParenthesizedContent(whereClause);

  const pred = escapeRegExp(predicate);
  const comparatorRegex = new RegExp(
    `\\b${pred}\\b\\s*(<=|>=|<|>)\\s*(date\\s*'[^']+'|'[^']*'|[+-]?\\d+(?:\\.\\d+)?)`,
    "gi"
  );

  const comparisons: Comparison[] = [];
  for (const match of depth0Only.matchAll(comparatorRegex)) {
    const operator = match[1];
    const value = match[2];
    if (!operator || !value) continue;
    comparisons.push({ operator, value });
  }

  // BETWEEN -> treat as >= and <=
  const betweenRegex = new RegExp(
    `\\b${pred}\\b\\s+between\\s+(date\\s*'[^']+'|'[^']*'|[+-]?\\d+(?:\\.\\d+)?)\\s+and\\s+(date\\s*'[^']+'|'[^']*'|[+-]?\\d+(?:\\.\\d+)?)`,
    "gi"
  );

  for (const match of depth0Only.matchAll(betweenRegex)) {
    const low = match[1];
    const high = match[2];
    if (low && high) {
      comparisons.push({ operator: ">=", value: low });
      comparisons.push({ operator: "<=", value: high });
    }
  }

  return comparisons;
}
