const fs = require("fs");
const path = require("path");

const TABLES = [
  "customer",
  "lineitem",
  "nation",
  "orders",
  "part",
  "partsupp",
  "region",
  "supplier",
];

function stripTrailingPipe(line) {
  // TPC-H .tbl lines usually end with "|\n" (or "|\r\n" on Windows).
  // Remove the final pipe but preserve the newline.
  if (line.endsWith("|\r\n")) return line.slice(0, -3) + "\r\n";
  if (line.endsWith("|\n")) return line.slice(0, -2) + "\n";
  if (line.endsWith("|")) return line.slice(0, -1);
  return line;
}

function convertTblToCsv(inputPath, outputPath) {
  const raw = fs.readFileSync(inputPath, "utf8");
  const lines = raw.split(/\n/);
  const out = lines
    .map((l, i) => (i === lines.length - 1 && l === "" ? "" : stripTrailingPipe(l + "\n")))
    .join("")
    .replace(/\n$/s, raw.endsWith("\n") ? "\n" : "");
  fs.writeFileSync(outputPath, out, "utf8");
}

function main() {
  const dataDir = process.argv[2]
    ? path.resolve(process.argv[2])
    : path.resolve(__dirname, "data");

  if (!fs.existsSync(dataDir)) {
    console.error(`Data directory not found: ${dataDir}`);
    process.exit(1);
  }

  for (const table of TABLES) {
    const tbl = path.join(dataDir, `${table}.tbl`);
    const csv = path.join(dataDir, `${table}.csv`);

    if (!fs.existsSync(tbl)) {
      console.warn(`Skipping (missing): ${tbl}`);
      continue;
    }

    convertTblToCsv(tbl, csv);
    console.log(`Wrote: ${csv}`);
  }
}

main();
