export async function exportFile(csv) {
  if (window.go?.main?.App?.ExportCSV) {
    return window.go.main.App.ExportCSV(csv);
  }
  const url = URL.createObjectURL(
    new Blob([csv], { type: "text/csv;charset=utf-8;" }),
  );
  const a = document.createElement("a");
  a.href = url;
  a.download = "illustrative-backtests.csv";
  a.click();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
  return true;
}
