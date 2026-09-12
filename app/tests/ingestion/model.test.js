import test from "node:test";
import assert from "node:assert/strict";
import {
  initialIngestionForm,
  localPreview,
  parseSymbols,
  validateIngestionForm,
} from "../../src/features/ingestion/model.js";

test("ingestion symbols normalize and deduplicate", () => {
  assert.deepEqual(parseSymbols(" aal, MSFT, aal "), ["AAL", "MSFT"]);
});

test("ingestion form validates dates and produces a bounded preview", () => {
  assert.equal(validateIngestionForm(initialIngestionForm), "");
  assert.equal(localPreview(initialIngestionForm).units, 4);
  assert.notEqual(
    validateIngestionForm({ ...initialIngestionForm, end: "2023-01-01" }),
    "",
  );
  assert.notEqual(
    validateIngestionForm({ ...initialIngestionForm, start: "2024-02-30" }),
    "",
  );
  assert.notEqual(
    validateIngestionForm({ ...initialIngestionForm, symbols: "bad symbol" }),
    "",
  );
});
