const getXLSX = (() => {
  let promise = null;
  return () => {
    if (!promise) {
      promise = new Promise((resolve, reject) => {
        if (window.XLSX) { resolve(window.XLSX); return; }
        const s = document.createElement("script");
        s.src = "https://cdnjs.cloudflare.com/ajax/libs/xlsx/0.18.5/xlsx.full.min.js";
        s.onload  = () => resolve(window.XLSX);
        s.onerror = () => reject(new Error("Failed to load XLSX"));
        document.head.appendChild(s);
      });
    }
    return promise;
  };
})();

export async function readFileData(file) {
  const XLSX = await getXLSX();
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = (e) => {
      try {
        const data = new Uint8Array(e.target.result);
        const wb   = XLSX.read(data, { type: "array" });
        const ws   = wb.Sheets[wb.SheetNames[0]];
        const all  = XLSX.utils.sheet_to_json(ws, { header: 1, defval: "" });
        if (all.length < 2) { reject(new Error("Fichier vide ou sans données")); return; }
        const headers  = all[0].map(String);
        const dataRows = all.slice(1).filter(r => r.some(c => c !== "" && c !== null));
        const rowCount = dataRows.length;
        const preview  = dataRows.slice(0, 30);
        const stats = {};
        headers.forEach((h, ci) => {
          const vals = dataRows.map(r => parseFloat(r[ci])).filter(v => !isNaN(v));
          if (vals.length === 0) return;
          const sum  = vals.reduce((a, b) => a + b, 0);
          const mean = sum / vals.length;
          const sorted = [...vals].sort((a, b) => a - b);
          const median = sorted[Math.floor(sorted.length / 2)];
          const variance = vals.reduce((a, b) => a + (b - mean) ** 2, 0) / vals.length;
          stats[h] = { count: vals.length, min: sorted[0].toFixed(2), max: sorted[sorted.length-1].toFixed(2), mean: mean.toFixed(2), median: median.toFixed(2), std: Math.sqrt(variance).toFixed(2), sum: sum.toFixed(2) };
        });
        resolve({ headers, rows: preview, rowCount, stats, sheetName: wb.SheetNames[0] });
      } catch (err) { reject(err); }
    };
    reader.onerror = () => reject(new Error("Erreur de lecture du fichier"));
    reader.readAsArrayBuffer(file);
  });
}

export function buildAnalysisPrompt(filename, headers, rows, rowCount, stats) {
  const csvPreview = [
    headers.join(" | "),
    ...rows.slice(0, 25).map(r => headers.map((_, ci) => String(r[ci] ?? "")).join(" | ")),
  ].join("\n");
  const statsLines = Object.entries(stats).map(([col, s]) =>
    `- **${col}** : n=${s.count}, min=${s.min}, max=${s.max}, moyenne=${s.mean}, médiane=${s.median}, σ=${s.std}, total=${s.sum}`
  ).join("\n");
  return `📊 **Analyse du fichier : ${filename}**\n\n**Informations générales :**\n- Lignes de données : ${rowCount}\n- Colonnes (${headers.length}) : ${headers.join(", ")}\n\n**Statistiques descriptives (colonnes numériques) :**\n${statsLines || "Aucune colonne numérique détectée."}\n\n**Aperçu des données (${Math.min(25, rows.length)} premières lignes sur ${rowCount}) :**\n${csvPreview}\n\n---\nEn tant qu'expert statisticien et analyste de données, fournis une analyse complète et structurée :\n\n1. **Vue d'ensemble du dataset** : structure, qualité, complétude des données\n2. **Statistiques descriptives** : interprétation des distributions, valeurs clés\n3. **Tendances & patterns** : insights principaux, évolutions notables\n4. **Anomalies & outliers** : valeurs aberrantes, incohérences à signaler\n5. **Corrélations & relations** : liens entre variables si pertinent\n6. **Conclusion & recommandations** : insights actionnables\n\nUtilise des tableaux markdown pour les comparaisons et synthèses. Réponds dans la langue de l'utilisateur.`;
}

export { getXLSX };