import { normalizeSql } from "../utils/sql";
import type { HistogramData } from "./histogram";

export type PredicateSelectivityData = {
  attribute: string;
  operator: string;
  queried_value: number | string;
  new_value: number | string;
  queried_selectivity: number;
  new_selectivity: number;
};

function isLessOrEqual(a: number | string, b: number | string, datatype: string): boolean {
  if (datatype === "integer" || datatype === "numeric") {
    return Number(a) <= Number(b);
  }
  // ISO dates compare lexicographically.
  return String(a) <= String(b);
}

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function formatSqlLiteral(value: number | string, datatype: string): string {
  if (datatype === "date") {
    return `'${String(value)}'`;
  }
  return String(value);
}

function replacePredicateValue(
  sqlQuery: string,
  attribute: string,
  operator: string,
  oldValue: number | string,
  newValue: number | string,
  datatype: string
): string {
  const oldLiteral = formatSqlLiteral(oldValue, datatype);
  const newLiteral = formatSqlLiteral(newValue, datatype);

  const pattern = `\\b${escapeRegExp(attribute)}\\b\\s*${escapeRegExp(operator)}\\s*${escapeRegExp(oldLiteral)}`;
  const re = new RegExp(pattern, "g");

  return sqlQuery.replace(re, `${attribute} ${operator} ${newLiteral}`);
}

export function generatePlans(arr: HistogramData[], originalSql: string): Array<[string, PredicateSelectivityData[]]> {
  const normalizedSql = normalizeSql(originalSql);

  const results: Array<[string, PredicateSelectivityData[]]> = [];

  function helper(index: number, sqlPath: string, predicateData: PredicateSelectivityData[]): void {
    if (index === arr.length) {
      results.push([sqlPath, predicateData]);
      return;
    }

    const current = arr[index];
    const operators = Object.keys(current.conditions);

    if (operators.length === 1) {
      const operator = operators[0];
      const condition = current.conditions[operator];

      const queriedSelectivity = condition.queried_selectivity;
      const histogramBounds = condition.histogram_bounds;
      const oldValue = histogramBounds[String(queriedSelectivity)] as number | string;

      for (const [selectivityKey, val] of Object.entries(histogramBounds)) {
        const newSelectivity = Number(selectivityKey);
        const newValue = val;

        const nextSql = replacePredicateValue(
          sqlPath,
          current.attribute,
          operator,
          oldValue,
          newValue,
          current.datatype
        );

        helper(index + 1, nextSql, [
          ...predicateData,
          {
            attribute: current.attribute,
            operator,
            queried_value: oldValue,
            new_value: newValue,
            queried_selectivity: queriedSelectivity,
            new_selectivity: newSelectivity,
          },
        ]);
      }

      return;
    }

    if (operators.length === 2) {
      // Range predicate. Identify less-than vs more-than operator.
      const lessOp = operators.find((op) => op.includes("<")) ?? operators[0];
      const moreOp = operators.find((op) => op.includes(">")) ?? operators[1];

      const less = current.conditions[lessOp];
      const more = current.conditions[moreOp];

      const lessOld = less.histogram_bounds[String(less.queried_selectivity)] as number | string;
      const moreOld = more.histogram_bounds[String(more.queried_selectivity)] as number | string;

      const lessBounds = Object.entries(less.histogram_bounds).map(([sel, v]) => ({
        value: v,
        selectivity: Number(sel),
      }));
      const moreBounds = Object.entries(more.histogram_bounds).map(([sel, v]) => ({
        value: v,
        selectivity: Number(sel),
      }));

      for (const l of lessBounds) {
        for (const m of moreBounds) {
          // Enforce less-than bound > more-than bound
          if (isLessOrEqual(l.value, m.value, current.datatype)) continue;

          const sqlAfterMore = replacePredicateValue(
            sqlPath,
            current.attribute,
            moreOp,
            moreOld,
            m.value,
            current.datatype
          );

          const sqlAfterBoth = replacePredicateValue(
            sqlAfterMore,
            current.attribute,
            lessOp,
            lessOld,
            l.value,
            current.datatype
          );

          helper(index + 1, sqlAfterBoth, [
            ...predicateData,
            {
              attribute: current.attribute,
              operator: lessOp,
              queried_value: lessOld,
              new_value: l.value,
              queried_selectivity: less.queried_selectivity,
              new_selectivity: l.selectivity,
            },
            {
              attribute: current.attribute,
              operator: moreOp,
              queried_value: moreOld,
              new_value: m.value,
              queried_selectivity: more.queried_selectivity,
              new_selectivity: m.selectivity,
            },
          ]);
        }
      }

      return;
    }

    // Unsupported multi-condition predicate, just skip.
    helper(index + 1, sqlPath, predicateData);
  }

  helper(0, normalizedSql, []);

  return results;
}
