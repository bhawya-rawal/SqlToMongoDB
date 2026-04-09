export const varPrefixToTable: Record<string, string> = {
  r: "region",
  n: "nation",
  s: "supplier",
  c: "customer",
  p: "part",
  ps: "partsupp",
  o: "orders",
  l: "lineitem",
};

export const equalityComparators = new Set(["!=", "="]);
export const rangeComparators = new Set(["<=", ">=", ">", "<"]);
