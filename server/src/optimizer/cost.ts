export function calculateEstimatedCostPerRow(qep: any): number {
  const plan = qep?.Plan;
  if (!plan) return NaN;

  const startupCost = Number(plan["Startup Cost"] ?? 0);
  const totalCost = Number(plan["Total Cost"] ?? 0);
  const rows = Number(plan["Plan Rows"] ?? 0);

  if (!Number.isFinite(startupCost) || !Number.isFinite(totalCost)) return NaN;
  if (!Number.isFinite(rows) || rows === 0) return startupCost + totalCost;

  return (startupCost + totalCost) / rows;
}
