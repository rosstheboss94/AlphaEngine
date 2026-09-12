import React from "react";
import { Database, BrainCircuit, Grid2X2, CloudDownload } from "lucide-react";
import Icon from "../ui/Icon.jsx";

export default function Navigation({ page, setPage }) {
  return (
    <nav
      aria-label="Main pages"
      className="main-nav flex h-[51px] shrink-0 border-b border-line"
    >
      {[
        ["Backtest", Database],
        ["Data", CloudDownload],
        ["Model training", BrainCircuit],
        ["Strategies", Grid2X2],
      ].map(([label, icon]) => (
        <button
          key={label}
          aria-current={page === label ? "page" : undefined}
          onClick={() => setPage(label)}
          className={`nav-item ${page === label ? "active" : ""}`}
        >
          <Icon type={icon} size={23} />
          {label}
        </button>
      ))}
    </nav>
  );
}
