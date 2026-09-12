import React, { useState } from "react";
import { Check, Download, ChevronDown, Search, X } from "lucide-react";
import Icon from "../../../ui/Icon.jsx";
import { sampleRows } from "../sampleData.js";
import { visibleRows } from "../results.js";
import { toCSV } from "../csv.js";
import { exportFile } from "../adapters/exportFile.js";
import ResultsTable from "./ResultsTable.jsx";

export default function Results({ onDetails }) {
  const [query, setQuery] = useState(""),
    [status, setStatus] = useState("All statuses"),
    [sort, setSort] = useState(null),
    [notice, setNotice] = useState("");
  const rows = visibleRows(sampleRows, query, status, sort);
  const exportRows = async () => {
    try {
      const csv = toCSV(rows);
      const saved = await exportFile(csv);
      if (!saved) {
        setNotice("Export cancelled.");
        return;
      }
      setNotice(`Exported ${rows.length} illustrative results.`);
    } catch (error) {
      setNotice(`Export failed: ${String(error)}`);
    }
  };
  return (
    <main className="results-pane min-w-0 flex-1 overflow-auto">
      <div className="results-heading flex flex-wrap items-center justify-between gap-3 border-b border-line">
        <div className="flex items-center gap-4">
          <h2 className="text-[23px] font-semibold">Batch results</h2>
          <span className="completion text-accent flex items-center gap-2 text-[18px]">
            <span className="status-check">
              <Icon type={Check} size={13} />
            </span>
            6 of 6 complete
          </span>
        </div>
        <button className="button" onClick={exportRows} disabled={!rows.length}>
          <Icon type={Download} size={18} />
          Export CSV
          <Icon type={ChevronDown} size={14} />
        </button>
      </div>
      <div className="results-filters flex gap-3">
        <label className="control search-control flex items-center gap-3">
          <Icon type={Search} size={19} />
          <input
            aria-label="Filter symbol or strategy"
            className="w-full min-w-0 bg-transparent outline-none placeholder:text-secondary"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder="Filter symbol or strategy"
          />
          {query && (
            <button aria-label="Clear search" onClick={() => setQuery("")}>
              <Icon type={X} size={14} />
            </button>
          )}
        </label>
        <select
          className="control status-filter"
          aria-label="Filter status"
          value={status}
          onChange={(e) => setStatus(e.target.value)}
        >
          {["All statuses", "Completed", "Running", "Failed"].map((x) => (
            <option key={x}>{x}</option>
          ))}
        </select>
      </div>
      <div className="table-scroll overflow-x-auto rounded-md border border-line">
        <ResultsTable
          rows={rows}
          sort={sort}
          setSort={setSort}
          onDetails={onDetails}
        />
        {!rows.length && (
          <div className="text-center py-16">
            <Icon type={Search} size={27} className="text-muted mx-auto mb-3" />
            <h3 className="font-medium">No matching backtests</h3>
            <p className="text-secondary text-sm mt-2">
              Try another symbol, strategy, or status.
            </p>
            <button
              className="text-accent mt-4 text-sm"
              onClick={() => {
                setQuery("");
                setStatus("All statuses");
              }}
            >
              Clear filters
            </button>
          </div>
        )}
      </div>
      <div className="results-note flex flex-wrap justify-between gap-3 text-base text-secondary">
        <p aria-live="polite">Showing {rows.length} backtests</p>
        <p>Open a result to view detailed metrics.</p>
      </div>
      <p role="status" className="text-sm text-secondary mt-4">
        {notice}
      </p>
    </main>
  );
}
