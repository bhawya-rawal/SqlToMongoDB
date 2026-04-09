<h1 align="center">Frontend</h1>

This package houses the frontend for our query optimizer. It takes in a user supplied SQL query made for the [TPC-H](http://www.tpc.org/tpch/) dataset and turns it into a query template that is parsed by Postgresql. It then displays various explanations and graphs on how the query optimizer determines the optimal query execution plan to pick from various plans by comparing the estimated costs of each query plan.

## Installation and setup

This package is managed via **npm workspaces** from the repository root.

1. Ensure you have **Node.js** and **npm** installed.
2. From the repository root, install dependencies:

```bash
npm install
```

3. To start the frontend only:

```bash
npm start --workspace sql-query-optimizer-frontend
```

4. To start both frontend + API together (recommended):

```bash
npm run dev
```
