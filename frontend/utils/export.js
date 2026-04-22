import { getXLSX } from "./xlsx";

export function exportCSV(table) {
  const header = table.headers.join(";");
  const rows   = table.rows.map(r => r.join(";")).join("\n");
  const blob   = new Blob(["\uFEFF" + header + "\n" + rows], { type:"text/csv;charset=utf-8;" });
  const url    = URL.createObjectURL(blob);
  const a      = Object.assign(document.createElement("a"), { href:url, download:`export_${Date.now()}.csv` });
  a.click(); URL.revokeObjectURL(url);
}

export async function exportExcel(table, filename = `export_${Date.now()}.xlsx`) {
  try {
    const XLSX = await getXLSX();
    const ws   = XLSX.utils.aoa_to_sheet([table.headers, ...table.rows]);
    const wb   = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, ws, "Données");
    ws["!cols"] = table.headers.map(() => ({ wch: 18 }));
    XLSX.writeFile(wb, filename);
  } catch (err) {
    alert("Erreur export Excel : " + err.message);
  }
}