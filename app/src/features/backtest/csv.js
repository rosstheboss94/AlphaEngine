export function toCSV(rows) {
  const cell = (value) => {
    let text = String(value);
    if (typeof value === "string" && /^[=+@\-\t\r]/.test(text))
      text = "'" + text;
    return '"' + text.replaceAll('"', '""') + '"';
  };
  const data = [
    [
      "Data kind",
      "Symbol",
      "Strategy",
      "Status",
      "Start",
      "End",
      "Return (%)",
      "Net P&L",
      "Max drawdown (%)",
      "Sharpe",
      "Closed trades",
    ],
    ...rows.map((r) => [
      "Illustrative",
      r.symbol,
      r.strategy,
      r.status,
      r.start,
      r.end,
      r.totalReturn,
      r.pnl,
      r.drawdown,
      r.sharpe,
      r.trades,
    ]),
  ];
  return data.map((row) => row.map(cell).join(",")).join("\r\n");
}
