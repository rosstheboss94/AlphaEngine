import test from "node:test";
import assert from "node:assert/strict";
import {
  initialConfig,
  validateConfig,
} from "../../src/features/backtest/config.js";
import { sampleRows } from "../../src/features/backtest/sampleData.js";
import { visibleRows } from "../../src/features/backtest/results.js";
import { toCSV } from "../../src/features/backtest/csv.js";
test("filters and numerical sorting preserve the source rows", () => {
  const original = sampleRows.map((r) => r.id);
  assert.equal(visibleRows(sampleRows, "aal", "All statuses", null).length, 2);
  assert.equal(visibleRows(sampleRows, "MODEL", "Completed", null).length, 3);
  assert.equal(visibleRows(sampleRows, "", "Failed", null).length, 0);
  assert.equal(
    visibleRows(sampleRows, "", "All statuses", { key: "pnl", direction: 1 })[0]
      .pnl,
    -12,
  );
  assert.equal(
    visibleRows(sampleRows, "", "All statuses", {
      key: "pnl",
      direction: -1,
    })[0].pnl,
    101.5,
  );
  assert.deepEqual(
    sampleRows.map((r) => r.id),
    original,
  );
});
test("rejects invalid selections, dates and account settings", () => {
  assert.equal(validateConfig(initialConfig), "");
  for (const patch of [
    { symbols: [] },
    { strategies: [] },
    { symbols: ["<script>"] },
    { start: "2024-02-30" },
    { start: "2024-01-01", end: "2023-01-01" },
    { cash: "NaN" },
    { cash: "-1" },
    { target: "0" },
    { slippage: "10000" },
    { slippage: "Infinity" },
  ])
    assert.notEqual(validateConfig({ ...initialConfig, ...patch }), "");
  assert.equal(
    validateConfig({ ...initialConfig, cash: "0", slippage: "9999.99" }),
    "",
  );
});
test("CSV labels samples, escapes quoted text and neutralises formula strings", () => {
  const csv = toCSV([{ ...sampleRows[0], strategy: '=X("a")' }]);
  assert.ok(csv.includes('"Illustrative"'));
  assert.ok(csv.includes('"\'=X(""a"")"'));
  assert.equal(csv.split("\r\n").length, 2);
  assert.equal(toCSV([]).split("\r\n").length, 1);
});
