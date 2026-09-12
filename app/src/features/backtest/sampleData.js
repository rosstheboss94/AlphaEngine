export const sampleRows = [
  ["AAL", "Candle strategy", 4.55, 45.45, 2.1, 1.24, 12],
  ["AAL", "Model + rules", 6.8, 68, 1.85, 1.48, 18],
  ["AAPL", "Candle strategy", 8.2, 82, 3.4, 1.36, 24],
  ["AAPL", "Model + rules", 10.15, 101.5, 2.95, 1.62, 21],
  ["MSFT", "Candle strategy", -1.2, -12, 4.6, -0.18, 16],
  ["MSFT", "Model + rules", 5.4, 54, 2.3, 1.15, 20],
].map(([symbol, strategy, totalReturn, pnl, drawdown, sharpe, trades], id) =>
  Object.freeze({
    id,
    symbol,
    strategy,
    totalReturn,
    pnl,
    drawdown,
    sharpe,
    trades,
    status: "Completed",
    start: "2023-01-01",
    end: "2023-12-31",
    initialCash: 1000,
  }),
);
