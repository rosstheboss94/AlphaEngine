import React from "react";
import { BrainCircuit, Grid2X2, ArrowLeft } from "lucide-react";
import Icon from "../ui/Icon.jsx";

export default function PlaceholderPage({ page, setPage }) {
  return (
    <main className="flex-1 flex items-center justify-center p-8">
      <div className="max-w-md text-center">
        <div className="mx-auto mb-5 flex h-16 w-16 items-center justify-center rounded-xl border border-line bg-panel text-accent">
          <Icon
            type={page === "Model training" ? BrainCircuit : Grid2X2}
            size={30}
          />
        </div>
        <h1 className="text-2xl font-semibold">{page}</h1>
        <p className="text-secondary mt-4 leading-7">
          {page === "Model training"
            ? "Train and evaluate models that provide strategy signals or choose trading actions."
            : "Create entry and exit rules, combine model signals, or use a model as the strategy."}
        </p>
        <p className="badge mt-5 inline-flex">Not connected in this preview</p>
        <div>
          <button
            className="button mx-auto mt-7"
            onClick={() => setPage("Backtest")}
          >
            <Icon type={ArrowLeft} size={16} />
            Back to backtests
          </button>
        </div>
      </div>
    </main>
  );
}
