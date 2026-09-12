import { afterEach, expect, test, vi } from "vitest";
import { exportFile } from "../../src/features/backtest/adapters/exportFile.js";

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
  vi.useRealTimers();
  delete window.go;
});

test("browser export downloads CSV and releases its object URL", async () => {
  vi.useFakeTimers();
  const createObjectURL = vi.fn(() => "blob:sample");
  const revokeObjectURL = vi.fn();
  vi.stubGlobal("URL", { createObjectURL, revokeObjectURL });
  let download;
  vi.spyOn(HTMLAnchorElement.prototype, "click").mockImplementation(
    function () {
      download = { href: this.getAttribute("href"), filename: this.download };
    },
  );
  expect(await exportFile("sample,csv")).toBe(true);
  expect(createObjectURL.mock.calls[0][0]).toBeInstanceOf(Blob);
  expect(createObjectURL.mock.calls[0][0].type).toBe("text/csv;charset=utf-8;");
  expect(download).toEqual({
    href: "blob:sample",
    filename: "illustrative-backtests.csv",
  });
  expect(revokeObjectURL).not.toHaveBeenCalled();
  vi.advanceTimersByTime(1000);
  expect(revokeObjectURL).toHaveBeenCalledWith("blob:sample");
});

test("native cancellation does not fall back to a browser download", async () => {
  const save = vi.fn().mockResolvedValue(false);
  window.go = { main: { App: { ExportCSV: save } } };
  const click = vi.spyOn(HTMLAnchorElement.prototype, "click");
  expect(await exportFile("sample,csv")).toBe(false);
  expect(save).toHaveBeenCalledWith("sample,csv");
  expect(click).not.toHaveBeenCalled();
});
