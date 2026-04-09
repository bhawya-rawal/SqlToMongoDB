export function getBestPlanId(allGeneratedPlans: Record<number, any>): number {
  const original = allGeneratedPlans[0];
  if (!original) return 0;

  let bestPlanIdCost = Number(original.estimated_cost_per_row);
  let bestPlanId = 0;

  for (const [rawPlanId, plan] of Object.entries(allGeneratedPlans)) {
    const planId = Number(rawPlanId);
    if (!Number.isFinite(planId) || planId === 0) continue;

    const cost = Number(plan.estimated_cost_per_row);
    if (!Number.isFinite(cost)) continue;

    if (cost < bestPlanIdCost) {
      if (JSON.stringify(plan.explanation) !== JSON.stringify(original.explanation)) {
        bestPlanIdCost = cost;
        bestPlanId = planId;
      }
    }
  }

  return bestPlanId;
}
