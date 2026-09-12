import React from "react";
import { afterEach, beforeAll, expect, test } from "vitest";
import {
  cleanup,
  fireEvent,
  render,
  screen,
  within,
} from "@testing-library/react";
import App from "../../src/app/App.jsx";
beforeAll(() => {
  HTMLDialogElement.prototype.showModal = function () {
    this.setAttribute("open", "");
  };
  HTMLDialogElement.prototype.close = function () {
    this.removeAttribute("open");
  };
});
afterEach(cleanup);
test("formatted account fields preserve editable values and symbol toggle works", () => {
  render(<App />);
  const cash = screen.getByLabelText("Initial cash");
  expect(cash.value).toBe("1,000.00");
  fireEvent.focus(cash);
  expect(cash.value).toBe("1000.00");
  fireEvent.change(cash, { target: { value: "2500.125" } });
  fireEvent.blur(cash);
  expect(cash.value).toBe("2,500.13");
  fireEvent.focus(cash);
  expect(cash.value).toBe("2500.125");
  fireEvent.change(cash, { target: { value: "invalid" } });
  fireEvent.blur(cash);
  expect(cash.value).toBe("invalid");
  fireEvent.click(screen.getByRole("button", { name: "Run backtests" }));
  expect(
    screen.getByText("Initial cash must be a finite amount of zero or more."),
  ).toBeTruthy();
  fireEvent.click(screen.getByRole("button", { name: "Close dialog" }));
  const toggle = screen.getByRole("button", { name: "Add another symbol" });
  fireEvent.click(toggle);
  expect(toggle.getAttribute("aria-expanded")).toBe("true");
  expect(screen.getByLabelText("New symbol")).toBeTruthy();
  fireEvent.click(toggle);
  expect(screen.queryByLabelText("New symbol")).toBeNull();
});
test("filters rows, sorts returns and opens the matching details", () => {
  render(<App />);
  expect(screen.getAllByRole("row")).toHaveLength(7);
  fireEvent.change(screen.getByLabelText("Filter symbol or strategy"), {
    target: { value: "MSFT" },
  });
  expect(screen.getAllByRole("row")).toHaveLength(3);
  fireEvent.click(screen.getByRole("button", { name: "Return" }));
  expect(screen.getAllByRole("row")[1].textContent).toContain("-1.20%");
  fireEvent.click(
    screen.getByRole("button", {
      name: "View results for MSFT Candle strategy",
    }),
  );
  const dialog = screen.getByRole("dialog");
  expect(within(dialog).getByRole("heading").textContent).toBe(
    "MSFT / Candle strategy",
  );
  expect(within(dialog).getByText("-$12.00")).toBeTruthy();
  fireEvent.click(
    within(dialog).getByRole("button", { name: "Back to batch" }),
  );
  expect(screen.queryByRole("dialog")).toBeNull();
  expect(screen.getByLabelText("Filter symbol or strategy").value).toBe("MSFT");
});
test("configuration edits do not modify sample runs and execution is explicit", () => {
  render(<App />);
  fireEvent.click(screen.getByRole("button", { name: "Remove AAL" }));
  expect(
    screen.getByText("2 symbols × 2 strategies = 4 backtests"),
  ).toBeTruthy();
  expect(screen.getAllByRole("row")).toHaveLength(7);
  fireEvent.click(screen.getByRole("button", { name: "Run backtests" }));
  expect(
    screen.getByRole("heading", {
      name: "Backtest execution is not connected",
    }),
  ).toBeTruthy();
  expect(screen.getByText(/Your selection contains 4 backtests/)).toBeTruthy();
  fireEvent.click(
    within(screen.getByRole("dialog")).getByRole("button", {
      name: "Close",
      exact: true,
    }),
  );
  fireEvent.change(screen.getByLabelText("Buy target"), {
    target: { value: "0" },
  });
  fireEvent.click(screen.getByRole("button", { name: "Run backtests" }));
  expect(
    screen.getByText("Buy target must be greater than zero."),
  ).toBeTruthy();
});
test("adds symbols, handles empty results, and navigates placeholders", () => {
  render(<App />);
  fireEvent.click(screen.getByRole("button", { name: "Add symbol" }));
  fireEvent.change(screen.getByLabelText("New symbol"), {
    target: { value: "nvda" },
  });
  fireEvent.click(screen.getByRole("button", { name: "Confirm symbol" }));
  expect(screen.getByRole("button", { name: "Remove NVDA" })).toBeTruthy();
  fireEvent.change(screen.getByLabelText("Filter status"), {
    target: { value: "Failed" },
  });
  expect(screen.getByText("No matching backtests")).toBeTruthy();
  expect(screen.getByRole("button", { name: /Export CSV/ }).disabled).toBe(
    true,
  );
  fireEvent.click(screen.getByRole("button", { name: "Clear filters" }));
  expect(screen.getAllByRole("row")).toHaveLength(7);
  fireEvent.click(
    screen.getByRole("button", { name: "Model training", exact: true }),
  );
  expect(screen.getByText("Not connected in this preview")).toBeTruthy();
  fireEvent.click(screen.getByRole("button", { name: "Back to backtests" }));
  expect(screen.getByRole("button", { name: "Remove NVDA" })).toBeTruthy();
});

test("native export handles cancellation, success and failure", async () => {
  render(<App />);
  let captured = "";
  window.go = {
    main: {
      App: {
        ExportCSV: async (csv) => {
          captured = csv;
          return false;
        },
      },
    },
  };
  fireEvent.click(screen.getByRole("button", { name: /Export CSV/ }));
  expect(await screen.findByText("Export cancelled.")).toBeTruthy();
  expect(captured).toContain('"Illustrative"');
  window.go.main.App.ExportCSV = async () => true;
  fireEvent.click(screen.getByRole("button", { name: /Export CSV/ }));
  expect(
    await screen.findByText("Exported 6 illustrative results."),
  ).toBeTruthy();
  window.go.main.App.ExportCSV = async () => {
    throw new Error("disk full");
  };
  fireEvent.click(screen.getByRole("button", { name: /Export CSV/ }));
  expect(
    await screen.findByText(/Export failed: Error: disk full/),
  ).toBeTruthy();
  delete window.go;
});
