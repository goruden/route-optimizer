"use client";
import { useState, useMemo } from "react";
import { useApp } from "@/lib/state";
import { LocationIcon } from "./icons";

export function StopsPanel() {
  const { s } = useApp();
  const [search, setSearch] = useState("");
  const [fleetF, setFleetF] = useState("ALL");

  const { stopDetails } = s;
  const filtered = useMemo(() => {
    const q = search.toLowerCase();
    return stopDetails
      .filter(r =>
        (fleetF === "ALL" || r.fleet === fleetF) &&
        (!q || r.store_id.toLowerCase().includes(q) || r.eng_name?.toLowerCase().includes(q) ||
         r.mn_name?.toLowerCase().includes(q) || r.truck_id.toLowerCase().includes(q) ||
         (r.truck_num && r.truck_num.toLowerCase().includes(q)) ||
         (r.contractor && r.contractor.toLowerCase().includes(q)))
      )
      .sort((a, b) => {
        const fleetOrder = a.fleet === b.fleet ? 0 : a.fleet === "DRY" ? -1 : 1;
        if (fleetOrder !== 0) return fleetOrder;
        const naturalSort = (str: string) => str.replace(/(\d+)/g, (match) => match.padStart(10, '0'));
        return naturalSort(a.truck_id).localeCompare(naturalSort(b.truck_id));
      });
  }, [stopDetails, search, fleetF]);

  if (!stopDetails.length) return (
    <div className="flex flex-col items-center justify-center h-full gap-3 text-slate-500">
      <span className="text-orange-600"><LocationIcon size="size-10" /></span>
      <p className="font-semibold text-orange-500">Зогсоол байхгүй байна</p>
      <p className="text-[12px]">Шинээр тооцолол хийнэ үү</p>
    </div>
  );

  return (
    <div className="flex flex-col h-full overflow-hidden">
      <div className="shrink-0 flex items-center gap-2.5 px-4 py-2.5 bg-white border-b border-slate-200">
        <input value={search} onChange={e => setSearch(e.target.value)} placeholder="Search stops…"
          className="flex-1 max-w-65 text-[12px] border border-slate-200 rounded-xl px-3 py-1.5 bg-white outline-none focus:border-red-500" />
        <div className="flex gap-1.5">
          {["ALL","DRY","COLD"].map(f => (
            <button key={f} onClick={() => setFleetF(f)}
              className={`px-3 py-1.5 rounded-xl text-[11px] font-semibold border-[1.5px] transition-all ${fleetF === f ? "bg-red-50 border-red-600 text-red-600" : "border-slate-200 bg-white text-slate-600"}`}
              // style={{ borderColor: fleetF===f?"rgb(91 124 250)":"rgb(226 232 240)", background: fleetF===f?"rgba(91,124,250,0.08)":"#fff", color: fleetF===f?"rgb(91 124 250)":"rgb(123 130 160)" }}
              >
              {f}
            </button>
          ))}
        </div>
        <span className="text-[11px] text-slate-500 font-mono ml-auto">{filtered.length} / {stopDetails.length} зогсоол</span>
      </div>

      <div className="flex-1 overflow-auto">
        <table className="w-full border-collapse">
          <thead className="sticky top-0 z-10 bg-slate-100">
            <tr>
              {["Fleet","Truck ID","Truck #","Contractor","Trip","#","Store ID","Name EN","Name MN","Address","Arrival","Departure","Day","kg","m³"].map(h => (
                <th key={h} className="px-3 py-2.5 text-left whitespace-nowrap">
                  <span className="text-[10px] font-bold text-slate-500 uppercase tracking-wider">{h}</span>
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {filtered.map((r, i) => (
              <tr key={i} className={`border-b border-slate-200 hover:bg-blue-500/4 transition-colors ${i%2===0?"bg-white":"bg-slate-50"}`}>
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
                <td className="px-3 py-2 font-mono text-[11px] text-slate-500">{r.stop_order}</td>
                <td className="px-3 py-2 font-mono text-[11px]">{r.store_id}</td>
                <td className="px-3 py-2 text-[11px] max-w-35 truncate">{r.eng_name}</td>
                <td className="px-3 py-2 text-[11px] text-slate-500 max-w-30 truncate">{r.mn_name}</td>
                <td className="px-3 py-2 text-[10px] text-slate-500 max-w-40 truncate">{r.address}</td>
                <td className="px-3 py-2 font-mono text-[11px] font-semibold text-green-500">{r.arrival}</td>
                <td className="px-3 py-2 font-mono text-[11px] text-red-500">{r.departure}</td>
                <td className={`px-3 py-2 text-[11px] ${r.delivery_day!=="Same day"?"text-orange-500 font-semibold":"text-slate-500"}`}>{r.delivery_day}</td>
                <td className="px-3 py-2 font-mono text-[11px]">{r.demand_kg.toFixed(1)}</td>
                <td className="px-3 py-2 font-mono text-[11px]">{r.demand_m3.toFixed(3)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
