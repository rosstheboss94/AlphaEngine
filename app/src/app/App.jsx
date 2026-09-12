import React, { useState } from "react";
import WindowBar from "./WindowBar.jsx";
import Navigation from "./Navigation.jsx";
import PlaceholderPage from "./PlaceholderPage.jsx";
import Footer from "./Footer.jsx";
import {
  BacktestPage,
  BacktestDialogs,
  useBacktest,
} from "../features/backtest/index.js";
import { DataPage, useIngestion } from "../features/ingestion/index.js";

export default function App() {
  const [page, setPage] = useState("Backtest");
  const backtest = useBacktest();
  const ingestion = useIngestion(page === "Data");
  return (
    <div className="app-shell h-dvh min-h-0 flex flex-col bg-app text-primary overflow-hidden">
      <WindowBar />
      <Navigation page={page} setPage={setPage} />
      {page === "Backtest" ? (
        <BacktestPage {...backtest} />
      ) : page === "Data" ? (
        <DataPage ingestion={ingestion} />
      ) : (
        <PlaceholderPage page={page} setPage={setPage} />
      )}
      <Footer />
      <BacktestDialogs {...backtest} />
    </div>
  );
}
