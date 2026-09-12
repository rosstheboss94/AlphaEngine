export const strategies = ["Candle strategy", "Model + rules"];
export const initialConfig = {
  symbols: ["AAL", "AAPL", "MSFT"],
  strategies: [...strategies],
  start: "2023-01-01",
  end: "2023-12-31",
  interval: "1 minute",
  cash: "1000.00",
  target: "1000.00",
  slippage: "0",
};
export function validateConfig(c) {
  if (!c.symbols.length || !c.strategies.length)
    return "Select at least one symbol and one strategy.";
  if (
    c.symbols.length > 30 ||
    c.symbols.some((s) => !/^[A-Z][A-Z0-9.-]{0,11}$/.test(s))
  )
    return "Use up to 30 valid stock symbols.";
  const validDate = (s) =>
    /^\d{4}-\d{2}-\d{2}$/.test(s) &&
    Number.isFinite(Date.parse(s)) &&
    new Date(s).toISOString().slice(0, 10) === s;
  if (!validDate(c.start) || !validDate(c.end) || c.start > c.end)
    return "Choose a valid date range with the start on or before the end.";
  const decimal = (s) =>
    /^[+]?(?:\d+(?:\.\d*)?|\.\d+)$/.test(s) && Number.isFinite(Number(s));
  if (!decimal(c.cash) || Number(c.cash) < 0)
    return "Initial cash must be a finite amount of zero or more.";
  if (!decimal(c.target) || Number(c.target) <= 0)
    return "Buy target must be greater than zero.";
  if (
    !decimal(c.slippage) ||
    Number(c.slippage) < 0 ||
    Number(c.slippage) >= 10000
  )
    return "Slippage must be from 0 to less than 10,000 bps.";
  return "";
}
