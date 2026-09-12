import React from "react";
import { Play } from "lucide-react";
import SettingsSidebar from "./components/SettingsSidebar.jsx";
import Results from "./components/Results.jsx";

export function BacktestPage({ config, setConfig, setDetail, run }) {
  return (
    <>
      <header className="batch-header flex h-[77px] shrink-0 items-center justify-between border-b border-line">
        <div className="flex items-center gap-4">
          <h1 className="text-[28px] font-semibold">Batch backtest</h1>
          <span className="badge">Example data</span>
        </div>
        <button className="primary-button" onClick={run}>
          <Play size={19} fill="currentColor" />
          Run backtests
        </button>
      </header>
      <div className="flex min-h-0 flex-1">
        <SettingsSidebar config={config} setConfig={setConfig} />
        <Results onDetails={setDetail} />
      </div>
    </>
  );
}
