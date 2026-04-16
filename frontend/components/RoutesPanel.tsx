"use client";
import { useState, useMemo } from "react";
import { useApp } from "@/lib/state";
import { VehicleIcon } from "./icons";

function utilColor(p: number) {
  return p >= 90 ? "rgb(239 68 68)" : p >= 65 ? "rgb(245 158 11)" : "rgb(16 185 129)";
}

function Bar({ pct, color }: { pct: number; color: string }) {
  return (
    <div className="flex items-center gap-1.5">
      <div className="flex-1 h-1.5 bg-slate-100 rounded-full overflow-hidden">
        <div className="h-full rounded-full" style={{ width: `${Math.min(100, pct)}%`, background: color }} />
      </div>
      <span className="text-[10px] font-bold font-mono w-8 text-right" style={{ color }}>{pct}%</span>
    </div>
  );
}

export function RoutesPanel() {
  const { s } = useApp();
  const [search, setSearch] = useState("");
  const [fleetF, setFleetF] = useState("ALL");

  const rows = s.routeSummary;

  const filtered = useMemo(() => {
    const q = search.toLowerCase();
    return rows
      .filter(r =>
        (fleetF === "ALL" || r.fleet === fleetF) &&
        (!q || r.truck_id.toLowerCase().includes(q) || 
         (r.truck_num && r.truck_num.toLowerCase().includes(q)) ||
         (r.contractor && r.contractor.toLowerCase().includes(q)))
      )
      .sort((a, b) => {
        const fleetOrder = a.fleet === b.fleet ? 0 : a.fleet === "DRY" ? -1 : 1;
        if (fleetOrder !== 0) return fleetOrder;
        const naturalSort = (str: string) => str.replace(/(\d+)/g, (match) => match.padStart(10, '0'));
        return naturalSort(a.truck_id).localeCompare(naturalSort(b.truck_id));
      });
  }, [rows, search, fleetF]);

  if (!rows.length) return (
    <div className="flex flex-col items-center justify-center h-full gap-3 text-slate-500">
      <span className="text-orange-600"><VehicleIcon size="size-10" /></span>
      <p className="font-semibold text-orange-500">Чиглэл байхгүй байна</p>
      <p className="text-[12px]">Шинээр тооцоолол хийнэ үү</p>
    </div>
  );


  return (
    <div className="flex flex-col h-full overflow-hidden">
      <div className="shrink-0 flex items-center gap-2.5 px-4 py-2.5 bg-white border-b border-slate-200">
        <input value={search} onChange={e => setSearch(e.target.value)} placeholder="Search routes…"
          className="flex-1 max-w-65 text-[12px] border border-slate-200 rounded-xl px-3 py-1.5 bg-white outline-none focus:border-red-500" />
        <div className="flex gap-1.5">
          {["ALL","DRY","COLD"].map(f => (
            <button key={f} onClick={() => setFleetF(f)}
              className={`px-3 py-1.5 rounded-xl text-[11px] font-semibold border-[1.5px] transition-all ${fleetF === f ? "bg-red-50 border-red-600 text-red-600" : "border-slate-200 bg-white text-slate-600"}`}
              >
              {f}
            </button>
          ))}
        </div>
        <span className="text-[11px] text-slate-500 font-mono ml-auto">{filtered.length} / {rows.length} чиглэл</span>
      </div>

      <div className="flex-1 overflow-auto">
        <table className="w-full border-collapse">
          <thead className="sticky top-0 z-10 bg-slate-100">
            <tr>
              {["Fleet","Truck ID","Truck #","Contractor","Trip","Departs","Returns","Stops","Dist km","Dur min","Load kg","Load m³","Fuel ₮","Man-hrs","Total ₮"].map(h => (
                <th key={h} className="px-3 py-2.5 text-left whitespace-nowrap">
                  <span className="text-[10px] font-bold text-slate-500 uppercase tracking-wider">{h}</span>
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {filtered.map((r, i) => (
              <tr
                key={`${r.truck_id}_T${r.trip_number}`}
                className={`border-b border-slate-200 hover:bg-blue-500/4 transition-colors ${i % 2 === 0 ? "bg-white" : "bg-slate-50"}`}
              >
                <td className="px-3 py-2">
                  <span className={`text-[10px] font-bold px-1.5 py-0.5 rounded-full ${r.fleet==="DRY"?"bg-orange-200 text-orange-600":"bg-sky-100 text-sky-600"}`}>{r.fleet}</span>
                </td>
                <td className="px-3 py-2 font-mono text-[11px] font-semibold">{r.truck_id}</td>
                <td className="px-3 py-2 font-mono text-[11px] text-slate-500">{r.truck_num || "—"}</td>
                <td className="px-3 py-2">
                  {r.contractor === "Fleet" ? (
                    <span className="text-[10px] font-bold px-2.5 py-1 rounded-full bg-green-100 text-green-700">{r.contractor}</span>
                  ) : (
                    <span className="text-[11px] text-slate-600">{r.contractor || "—"}</span>
                  )}
                </td>
                <td className="px-3 py-2 font-mono text-[11px] text-slate-500">T{r.trip_number}</td>
                <td className="px-3 py-2 font-mono text-[11px]">{r.departs_at}</td>
                <td className="px-3 py-2 font-mono text-[11px] text-amber-500">{r.returns_at ?? "—"}</td>
                <td className="px-3 py-2 font-mono text-[12px] font-bold">{r.stops}</td>
                <td className="px-3 py-2 font-mono text-[11px]">{r.distance_km.toLocaleString()}</td>
                <td className="px-3 py-2 font-mono text-[11px]">{r.duration_min.toLocaleString()}</td>
                <td className="px-3 py-2 min-w-22.5">
                  <div className="text-[10px] text-slate-500 font-mono mb-0.5">{r.load_kg} / {r.cap_kg}</div>
                  <Bar pct={r.util_kg_pct} color={utilColor(r.util_kg_pct)} />
                </td>
                <td className="px-3 py-2 min-w-20">
                  <div className="text-[10px] text-slate-500 font-mono mb-0.5">{r.load_m3} / {r.cap_m3}</div>
                  <Bar pct={r.util_m3_pct} color={utilColor(r.util_m3_pct)} />
                </td>
                <td className="px-3 py-2 font-mono text-[11px]">{Math.round(r.cost_fuel).toLocaleString()}</td>
                <td className="px-3 py-2 font-mono text-[11px] text-violet-500">{(r.man_hours ?? 0).toFixed(1)}h</td>
                <td className="px-3 py-2 font-mono text-[12px] font-bold text-amber-500">{Math.round(r.cost_total).toLocaleString()}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

    </div>
  );
}