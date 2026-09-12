import React from "react";
import { ArrowLeft } from "lucide-react";
import Icon from "../../../ui/Icon.jsx";
import Modal from "../../../ui/Modal.jsx";
import { money, signedMoney, percent } from "../formatting.js";

export default function ResultDialog({ row, onClose }) {
  return (
    <Modal title={`${row.symbol} / ${row.strategy}`} onClose={onClose}>
      <div className="flex items-center justify-between mb-6">
        <span className="badge">Illustrative results</span>
        <span className="text-sm text-secondary">
          {row.start} to {row.end}
        </span>
      </div>
      <div className="grid grid-cols-2 gap-5">
        {[
          ["Total return", percent(row.totalReturn)],
          ["Net P&L", signedMoney(row.pnl)],
          ["Max drawdown", `${row.drawdown.toFixed(2)}%`],
          ["Sharpe ratio", row.sharpe.toFixed(2)],
          ["Closed trades", row.trades],
          ["Initial equity", money(row.initialCash)],
          ["Final equity", money(row.initialCash + row.pnl)],
        ].map(([label, value]) => (
          <div
            className="rounded border border-line bg-white/2 p-4"
            key={label}
          >
            <p className="text-secondary text-sm mb-2">{label}</p>
            <p className="text-2xl font-medium tabular-nums">{value}</p>
          </div>
        ))}
      </div>
      <p className="text-muted text-sm mt-6">
        These values are sample data. Additional trade and risk metrics will
        appear when the engine is connected.
      </p>
      <button className="button mt-6" onClick={onClose}>
        <Icon type={ArrowLeft} size={16} />
        Back to batch
      </button>
    </Modal>
  );
}
