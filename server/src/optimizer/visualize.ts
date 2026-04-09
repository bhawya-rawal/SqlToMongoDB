type PlanNode = Record<string, any>;

export type GraphNode = {
  id: string;
  node_type: string;
  cost: number;
  depth: number;
  [key: string]: unknown;
};

export type GraphLink = {
  source: string;
  target: string;
};

export type PlanGraph = {
  directed: true;
  multigraph: false;
  graph: Record<string, never>;
  nodes: GraphNode[];
  links: GraphLink[];
};

function stringUniqueId(uniqueId: number): string {
  return `T${uniqueId}`;
}

function craftExplanationString(
  explanation: string,
  nodeType: string,
  childNames: any,
  currName: string
): string {
  explanation += `${nodeType} `;

  if (
    nodeType === "Hash" ||
    nodeType === "Sort" ||
    nodeType === "Incremental Sort" ||
    nodeType === "Gather Merge" ||
    nodeType === "Merge" ||
    nodeType === "Aggregate"
  ) {
    explanation += `${childNames[0]._id} as ${currName}.`;
  } else if (
    nodeType === "Hash Join" ||
    nodeType === "Nested Loop" ||
    nodeType === "Merge Join"
  ) {
    if (nodeType === "Nested Loop") {
      explanation += "Join ";
      explanation +=
        `between ${childNames[0]["Node Type"]} ${childNames[0]._id} (outer) and ` +
        `${childNames[1]["Node Type"]} ${childNames[1]._id} (inner) as ${currName}.`;
    }
  } else {
    try {
      explanation += `${childNames[0]._id} as ${currName}.`;
    } catch {
      explanation += `on ${childNames["Relation Name"]} as ${currName}.`;
    }
  }

  return explanation;
}

function getNodeCost(planNode: PlanNode): number {
  return Number(planNode["Startup Cost"] ?? 0) + Number(planNode["Total Cost"] ?? 0);
}

export function visualizeExplainQuery(planRoot: any): {
  graph: PlanGraph;
  explanation: string[];
} {
  const empty: PlanGraph = {
    directed: true,
    multigraph: false,
    graph: {},
    nodes: [],
    links: [],
  };

  if (!planRoot || typeof planRoot !== "object" || !("Plan" in planRoot)) {
    return { graph: empty, explanation: [] };
  }

  const queue: PlanNode[] = [];
  let uniqueId = 1;

  const nodes: GraphNode[] = [];
  const links: GraphLink[] = [];
  let explanation = "";

  const root = (planRoot as any).Plan as PlanNode;
  root._id = stringUniqueId(uniqueId);
  root._depth = 0;
  uniqueId += 1;

  queue.push(root);
  nodes.push({
    id: root._id,
    node_type: String(root["Node Type"] ?? "Unknown"),
    cost: getNodeCost(root),
    depth: root._depth,
  });

  while (queue.length > 0) {
    const curr = queue.shift() as PlanNode;
    const children = Array.isArray(curr.Plans) ? (curr.Plans as PlanNode[]) : [];

    if (children.length > 0) {
      const depth = Number(curr._depth ?? 0) + 1;

      const childRecords: PlanNode[] = [];
      for (const child of children) {
        child._id = stringUniqueId(uniqueId);
        child._depth = depth;
        uniqueId += 1;

        queue.push(child);
        childRecords.push(child);

        nodes.push({
          id: child._id,
          node_type: String(child["Node Type"] ?? "Unknown"),
          cost: getNodeCost(child),
          depth,
        });

        links.push({ source: String(curr._id), target: String(child._id) });
      }

      explanation = craftExplanationString(
        explanation,
        String(curr["Node Type"] ?? "Unknown"),
        childRecords,
        String(curr._id)
      );
    } else {
      const tableId = stringUniqueId(uniqueId);
      uniqueId += 1;

      nodes.push({
        id: tableId,
        node_type: String(curr["Relation Name"] ?? "Relation"),
        cost: 0,
        depth: Number(curr._depth ?? 0) + 1,
      });

      links.push({ source: String(curr._id), target: tableId });

      explanation = craftExplanationString(
        explanation,
        String(curr["Node Type"] ?? "Unknown"),
        curr,
        String(curr._id)
      );
    }
  }

  const explanationSteps = explanation
    .split(".")
    .map((s) => s.trim())
    .filter((s) => s.length > 0)
    .reverse();

  return {
    graph: {
      directed: true,
      multigraph: false,
      graph: {},
      nodes,
      links,
    },
    explanation: explanationSteps,
  };
}
