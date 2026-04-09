import { equalityComparators, rangeComparators, varPrefixToTable } from "./constants";
import { extractTopLevelComparisons, normalizeSql } from "../utils/sql";
import { getHistogram, type HistogramData } from "./histogram";

export async function getSelectivities(sql: string, predicates: string[]): Promise<HistogramData[]> {
  const normalized = normalizeSql(sql);
  const results: HistogramData[] = [];

  for (const predicate of predicates) {
    const prefix = predicate.split("_")[0] ?? "";
    const relation = varPrefixToTable[prefix];
    if (!relation) continue;

    const comparisons = extractTopLevelComparisons(normalized, predicate).filter((c) => {
      if (equalityComparators.has(c.operator)) return false;
      return rangeComparators.has(c.operator);
    });

    if (comparisons.length === 0) continue;

    const histogramData = await getHistogram(relation, predicate, comparisons);
    // Only keep if we got at least one operator bucket.
    if (Object.keys(histogramData.conditions).length > 0) {
      results.push(histogramData);
    }
  }

  return results;
}
