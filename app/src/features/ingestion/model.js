export const initialIngestionForm = {
  listName: "",
  symbols: "AAL, MSFT",
  start: "2024-01-01",
  end: "2024-01-02",
};

export function parseSymbols(value) {
  return [
    ...new Set(
      value
        .split(",")
        .map((symbol) => symbol.trim().toUpperCase())
        .filter(Boolean),
    ),
  ];
}

export function validateIngestionForm(form) {
  const symbols = parseSymbols(form.symbols);
  if (!symbols.length) return "Select at least one stock symbol.";
  if (symbols.length > 100) return "Select up to 100 symbols.";
  if (symbols.some((symbol) => !/^[A-Z][A-Z0-9.-]{0,11}$/.test(symbol))) {
    return "Use stock symbols with letters, numbers, dots or dashes.";
  }
  if (!validISODate(form.start)) {
    return "Start must be a valid date.";
  }
  if (!validISODate(form.end)) {
    return "End must be a valid date.";
  }
  if (form.end < form.start) return "End must be on or after start.";
  return "";
}

function validISODate(value) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) return false;
  const date = new Date(`${value}T00:00:00Z`);
  return (
    !Number.isNaN(date.getTime()) && date.toISOString().slice(0, 10) === value
  );
}

export function localPreview(form) {
  const symbols = parseSymbols(form.symbols);
  const start = new Date(`${form.start}T00:00:00Z`);
  const end = new Date(`${form.end}T00:00:00Z`);
  const days = Math.floor((end - start) / 86400000) + 1;
  return {
    symbols,
    start: form.start,
    end: form.end,
    units: symbols.length * Math.max(days, 0),
    provider: "Massive",
    resolution: "1 minute",
    adjustment: "unadjusted",
    limit_message: "Provider access and empty sessions are checked by the job.",
  };
}

export function statusLabel(status) {
  return (
    {
      queued: "Queued",
      running: "Running",
      waiting: "Waiting for provider limit",
      completed: "Completed",
      incomplete: "Incomplete",
      "checked-empty": "Checked empty",
      "needs-attention": "Needs attention",
      failed: "Failed",
      cancelled: "Cancelled",
    }[status] ||
    status ||
    "Unknown"
  );
}
