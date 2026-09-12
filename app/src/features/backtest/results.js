export function visibleRows(rows, query, status, sort) {
  const q = query.trim().toLowerCase();
  const result = rows.filter(
    (r) =>
      `${r.symbol} ${r.strategy}`.toLowerCase().includes(q) &&
      (status === "All statuses" || r.status === status),
  );
  if (sort)
    result.sort((a, b) => {
      const x = a[sort.key],
        y = b[sort.key];
      const comparison =
        typeof x === "number" ? x - y : String(x).localeCompare(String(y));
      return comparison * sort.direction || a.id - b.id;
    });
  return result;
}
