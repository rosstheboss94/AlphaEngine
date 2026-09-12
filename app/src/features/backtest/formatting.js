export const money = (n) =>
  `${n < 0 ? "-" : ""}$${Math.abs(n).toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
export const signedMoney = (n) => `${n >= 0 ? "+" : ""}${money(n)}`;
export const percent = (n) => `${n >= 0 ? "+" : ""}${n.toFixed(2)}%`;
