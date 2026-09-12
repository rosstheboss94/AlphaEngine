import React from "react";
import { afterEach, expect, test } from "vitest";
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import App from "../../src/app/App.jsx";

afterEach(cleanup);

test("Data page previews a range and explains missing AWS setup", async () => {
  render(<App />);
  fireEvent.click(screen.getByRole("button", { name: "Data", exact: true }));
  expect(screen.getByRole("heading", { name: "Data ingestion" })).toBeTruthy();
  expect(screen.getByText("AWS setup required")).toBeTruthy();
  fireEvent.click(screen.getByRole("button", { name: "Preview range" }));
  expect(await screen.findByText(/4 symbol\/date units/)).toBeTruthy();
  expect(
    screen.getByText("Preview updated. No data was downloaded."),
  ).toBeTruthy();
});
