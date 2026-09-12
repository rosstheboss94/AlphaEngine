import React from "react";
import { ChevronsUpDown, ArrowRight, Check } from "lucide-react";
import Icon from "../../../ui/Icon.jsx";
import { signedMoney, percent } from "../formatting.js";

const columns = [
  ["symbol", "Symbol"],
  ["strategy", "Strategy"],
  ["status", "Status"],
  ["totalReturn", "Return"],
  ["pnl", "Net P&L"],
  ["drawdown", "Max DD"],
  ["sharpe", "Sharpe"],
  ["trades", "Trades"],
];
export default function ResultsTable({ rows, sort, setSort, onDetails }) {
  return (
    <table className="results-table w-full">
      <caption className="sr-only">
        Illustrative backtest results, one row per symbol and strategy
      </caption>
      <thead>
        <tr>
          {columns.map(([key, label]) => (
            <th
              key={key}
              scope="col"
              aria-sort={
                sort?.key === key
                  ? sort.direction === 1
                    ? "ascending"
                    : "descending"
                  : "none"
              }
            >
              <button
                onClick={() =>
                  setSort({
                    key,
                    direction: sort?.key === key ? -sort.direction : 1,
                  })
                }
                className="flex items-center gap-2 w-full whitespace-nowrap"
              >
                {label}
                <Icon
                  type={ChevronsUpDown}
                  size={13}
                  className={sort?.key === key ? "text-accent" : "text-muted"}
                />
              </button>
            </th>
          ))}
          <th scope="col">
            <span className="sr-only">Details</span>
          </th>
        </tr>
      </thead>
      <tbody>
        {rows.map((row) => (
          <tr key={row.id}>
            <td>{row.symbol}</td>
            <td className="whitespace-nowrap">{row.strategy}</td>
            <td>
              <span className="text-accent flex items-center gap-2 whitespace-nowrap">
                <span className="status-check">
                  <Icon type={Check} size={12} />
                </span>
                {row.status}
              </span>
            </td>
            <td
              className={row.totalReturn < 0 ? "text-negative" : "text-accent"}
            >
              {percent(row.totalReturn)}
            </td>
            <td className={row.pnl < 0 ? "text-negative" : "text-accent"}>
              {signedMoney(row.pnl)}
            </td>
            <td>{row.drawdown.toFixed(2)}%</td>
            <td>{row.sharpe.toFixed(2)}</td>
            <td>{row.trades}</td>
            <td>
              <button
                className="text-accent flex items-center gap-2 whitespace-nowrap"
                aria-label={`View results for ${row.symbol} ${row.strategy}`}
                onClick={() => onDetails(row)}
              >
                View results
                <Icon type={ArrowRight} size={15} />
              </button>
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}
