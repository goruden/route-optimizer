"use client";
import { useState, useMemo } from "react";
import { useApp } from "@/lib/state";
import { CheckIcon, WarningIcon } from "./icons";

function classifyReason(r: string): { label: string; color: string } {
  const s = r.toLowerCase();
  if (s.includes("capacity exhausted") || s.includes("fleet")) return { label: "Fleet Full",    color: "rgb(239 68 68)" };
  if (s.includes("exceeds"))                                    return { label: "Over Capacity", color: "rgb(239 68 68)" };
  if (s.includes("time window") || s.includes("window"))       return { label: "Time Window",   color: "rgb(245 158 11)" };
  if (s.includes("invalid"))                                    return { label: "Bad Hours",     color: "rgb(249 115 22)" };
  if (s.includes("far") || s.includes("remote"))               return { label: "Too Far",        color: "rgb(251 191 36)" };
  if (s.includes("no") && s.includes("vehicle"))               return { label: "No Vehicle",    color: "rgb(123 130 160)" };
  if (s.includes("solver"))                                     return { label: "Solver Drop",   color: "rgb(139 92 246)" };
  return                                                               { label: "Unknown",       color: "rgb(123 130 160)" };
}

export function UnservedPanel() {
  const { s } = useApp();
  const [search, setSearch] = useState("");
  const [fleetF, setFleetF] = useState("ALL");

  const { unserved } = s;
  const filtered = useMemo(() => {
    const q = search.toLowerCase();
    return unserved.filter(u =>
      (fleetF === "ALL" || u.fleet === fleetF) &&
      (!q || u.store_id.toLowerCase().includes(q) || u.eng_name?.toLowerCase().includes(q) || u.reason?.toLowerCase().includes(q))
    );
  }, [unserved, search, fleetF]);

  if (!unserved.length && s.summary) return (
    <div className="flex flex-col items-center justify-center h-full gap-3">
      <span className="text-green-600"><CheckIcon size="size-10"/></span>
      <p className="font-bold text-[16px] text-green-500">Бүр дэлгүүр амжилттай хүргэгдсэн байна!</p>
      <p className="text-[12px] text-slate-500">Үлдсэн дэлгүүр 0</p>
    </div>
  );
  if (!unserved.length) return (
    <div className="flex flex-col items-center justify-center h-full gap-3 text-slate-500">
      <span className="text-orange-600"><WarningIcon size="size-10" /></span>
      <p className="font-semibold text-orange-500">Өгөгдөл байхгүй байна</p>
      <p className="text-[12px]">Шинээр тооцолол хийнэ үү</p>
    </div>
  );

  /* reason summary */
  const groups: Record<string, number> = {};
  unserved.forEach(u => { const { label } = classifyReason(u.reason); groups[label] = (groups[label] ?? 0) + 1; });

  return (
    <div className="flex flex-col h-full overflow-hidden">
      {/* warning banner */}
      <div className="shrink-0 mx-4 mt-3 p-3 bg-red-500/6 border border-red-500/20 rounded-xl">
        <div className="text-[12px] font-semibold text-red-500 mb-2">
          ⚠ {unserved.length} дэлгүүр хүргэгдээгүй байна
        </div>
        <div className="flex flex-wrap gap-1.5">
          {Object.entries(groups).map(([label, cnt]) => {
            const { color } = classifyReason(label);
            return (
              <span key={label} className="text-[10px] font-bold px-2 py-0.5 rounded-full"
                style={{ background: color + "18", color }}>
                {label}: {cnt}
              </span>
            );
          })}
        </div>
      </div>

      {/* toolbar */}
      <div className="shrink-0 flex items-center gap-2.5 px-4 py-2.5">
        <input value={search} onChange={e => setSearch(e.target.value)} placeholder="Хүргэгдээгүй дэлгүүрүүдээс хайх..."
          className="flex-1 max-w-70 text-[12px] border border-slate-200 rounded-xl px-3 py-1.5 bg-white outline-none focus:border-blue-500" />
        <div className="flex gap-1.5">
          {["ALL","DRY","COLD"].map(f => (
            <button key={f} onClick={() => setFleetF(f)}
              className="px-3 py-1.5 rounded-xl text-[11px] font-semibold border-[1.5px] transition-all"
              style={{ borderColor: fleetF===f?"rgb(91 124 250)":"rgb(226 232 240)", background: fleetF===f?"rgba(91,124,250,0.08)":"#fff", color: fleetF===f?"rgb(91 124 250)":"rgb(123 130 160)" }}>
              {f}
            </button>
          ))}
        </div>
        <span className="text-[11px] text-red-500 font-mono ml-auto">{filtered.length} Дэлгүүр</span>
      </div>

      {/* table */}
      <div className="flex-1 overflow-auto px-4 pb-4">
        <table className="w-full border-collapse">
          <thead className="sticky top-0 z-10 bg-slate-100">
            <tr>
              {["Fleet","Store ID","Name EN","Name MN","Demand kg","Demand m³","→ Dry DC","→ Cold DC","Category","Reason"].map(h => (
                <th key={h} className="px-3 py-2.5 text-left whitespace-nowrap">
                  <span className="text-[10px] font-bold text-slate-500 uppercase tracking-wider">{h}</span>
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {filtered.map((u, i) => {
              const { label, color } = classifyReason(u.reason);
              return (
                <tr key={i} className="border-b border-slate-200 hover:bg-red-500/3 transition-colors bg-red-50">
                  <td className="px-3 py-2">
                    <span className="text-[10px] font-bold px-1.5 py-0.5 rounded-full" style={{ background: u.fleet==="DRY"?"rgba(91,124,250,0.12)":"rgba(14,165,233,0.12)", color: u.fleet==="DRY"?"rgb(91 124 250)":"rgb(2 132 199)" }}>{u.fleet}</span>
                  </td>
                  <td className="px-3 py-2 font-mono text-[11px]">{u.store_id}</td>
                  <td className="px-3 py-2 text-[11px] max-w-35 truncate">{u.eng_name}</td>
                  <td className="px-3 py-2 text-[11px] text-slate-500 max-w-30 truncate">{u.mn_name}</td>
                  <td className="px-3 py-2 font-mono text-[11px]">{u.demand_kg.toFixed(1)}</td>
                  <td className="px-3 py-2 font-mono text-[11px]">{u.demand_m3.toFixed(3)}</td>
                  <td className="px-3 py-2 font-mono text-[11px] text-slate-500">{u.dist_from_Dry_DC_km != null ? u.dist_from_Dry_DC_km + " km" : "—"}</td>
                  <td className="px-3 py-2 font-mono text-[11px] text-slate-500">{u.dist_from_Cold_DC_km != null ? u.dist_from_Cold_DC_km + " km" : "—"}</td>
                  <td className="px-3 py-2">
                    <span className="text-[10px] font-bold px-2 py-0.5 rounded-full whitespace-nowrap" style={{ background: color + "18", color }}>{label}</span>
                  </td>
                  <td className="px-3 py-2 text-[11px] text-slate-500 max-w-65">
                    <span title={u.reason} className="truncate block max-w-60">{u.reason}</span>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}
