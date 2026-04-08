"use client";
import { useState, useMemo, useCallback } from "react";
import { useApp } from "@/lib/state";
import { RouteBuilderModal, routesFromSolverData } from "./AddPanel";
import { showToast } from "./ui";
import type { RouteSummary } from "@/types/vrp";
import { EditIcon, VehicleIcon } from "./icons";

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
  const [sortKey, setSortKey] = useState<string>("truck_id");
  const [sortAsc, setSortAsc] = useState(true);
  const [fleetF, setFleetF] = useState("ALL");
  const [showEditModal, setShowEditModal] = useState(false);

  const rows = s.routeSummary;

  // Build initial routes for edit modal from existing stop data
  const editInitialRoutes = useMemo(() => {
    if (!s.routeSummary.length || !s.stopDetails.length) return [];
    return routesFromSolverData(s.routeSummary, s.stopDetails);
  }, [s.routeSummary, s.stopDetails]);

  // Dataset id from active job
  const activeJob = s.jobs.find(j => j.id === s.activeJobId);
  const editDatasetId = activeJob?.dataset_id ?? s.activeDatasetId ?? undefined;
  const editGroupId = activeJob?.group_id ?? undefined;

  const filtered = useMemo(() => {
    const q = search.toLowerCase();
    return rows
      .filter(r => (fleetF === "ALL" || r.fleet === fleetF) &&
        (!q || r.truck_id.toLowerCase().includes(q) || r.fleet.toLowerCase().includes(q)))
      .sort((a: any, b: any) => {
        const av = a[sortKey], bv = b[sortKey];
        if (typeof av === "string") return sortAsc ? av.localeCompare(bv) : bv.localeCompare(av);
        return sortAsc ? av - bv : bv - av;
      });
  }, [rows, search, fleetF, sortKey, sortAsc]);

  if (!rows.length) return (
    <div className="flex flex-col items-center justify-center h-full gap-3 text-slate-500">
      <span className="text-orange-600"><VehicleIcon size="size-10" /></span>
      <p className="font-semibold text-orange-500">Чиглэл байхгүй байна</p>
      <p className="text-[12px]">Шинээр тооцоолол хийнэ үү</p>
    </div>
  );

  const totalFuel     = rows.reduce((a, r) => a + r.cost_fuel,  0);
  const totalFixed    = rows.reduce((a, r) => a + r.cost_fixed, 0);
  const totalLabor    = rows.reduce((a, r) => a + r.cost_labor, 0);
  const totalCost     = rows.reduce((a, r) => a + r.cost_total, 0);
  const totalManHours = rows.reduce((a, r) => a + (r.man_hours ?? 0), 0);
  const avgUtil       = rows.reduce((a, r) => a + r.util_kg_pct, 0) / rows.length;

  function Th({ label, k }: { label: string; k: string }) {
    const active = sortKey === k;
    return (
      <th
        className="px-3 py-2.5 text-left whitespace-nowrap cursor-pointer select-none hover:bg-blue-50"
        onClick={() => { if (active) setSortAsc(a => !a); else { setSortKey(k); setSortAsc(true); } }}
      >
        <span className="text-[10px] font-bold text-slate-500 uppercase tracking-wider flex items-center gap-1">
          {label}{active && <span className="text-blue-500">{sortAsc ? "↑" : "↓"}</span>}
        </span>
      </th>
    );
  }

  return (
    <div className="flex flex-col h-full overflow-hidden">
      {/* KPI bar */}
      <div className="shrink-0 flex gap-3 px-4 pt-3 pb-1 overflow-x-auto">
        {[
          { l: "Дундаж",   v: `${avgUtil.toFixed(1)}%`,                                                                           c: utilColor(avgUtil) },
          { l: "Хүн цаг",  v: `${totalManHours.toLocaleString(undefined,{minimumFractionDigits:1,maximumFractionDigits:1})}h`,     c: "rgb(139 92 246)" },
          { l: "Түлш",       v: `₮${Math.round(totalFuel).toLocaleString()}`,                                                       c: "rgb(245 158 11)" },
          { l: "Тогтмол",      v: `₮${Math.round(totalFixed).toLocaleString()}`,                                                      c: "rgb(123 130 160)" },
          { l: "Ажлын зардал",      v: `₮${Math.round(totalLabor).toLocaleString()}`,                                                      c: "rgb(123 130 160)" },
          { l: "Нийт зардал", v: `₮${Math.round(totalCost).toLocaleString()}`,                                                       c: "rgb(245 158 11)" },
        ].map(k => (
          <div key={k.l} className="shrink-0 bg-white border border-slate-200 rounded-xl px-3 py-2 shadow-sm">
            <div className="text-[10px] text-slate-500 mb-0.5">{k.l}</div>
            <div className="font-mono font-bold text-[13px]" style={{ color: k.c }}>{k.v}</div>
          </div>
        ))}
      </div>

      {/* Toolbar */}
      <div className="shrink-0 flex items-center gap-2.5 px-4 py-2.5">
        <input
          value={search}
          onChange={e => setSearch(e.target.value)}
          placeholder="Search routes…"
          className="flex-1 max-w-52 text-[12px] border border-slate-200 rounded-xl px-3 py-1.5 bg-white outline-none focus:border-blue-500"
        />
        <div className="flex gap-1.5">
          {["ALL","DRY","COLD"].map(f => (
            <button
              key={f}
              onClick={() => setFleetF(f)}
              className={`px-3 py-1.5 rounded-xl text-[11px] font-semibold border-[1.5px] transition-all ${fleetF === f ? "bg-red-50 border-red-600 text-red-600" : "border-slate-200 bg-white text-slate-600"}`}
              // style={{
              //   borderColor: fleetF===f ? "rgb(91 124 250)" : "rgb(226 232 240)",
              //   background: fleetF===f ? "rgba(91,124,250,0.08)" : "#fff",
              //   color: fleetF===f ? "rgb(91 124 250)" : "rgb(123 130 160)",
              // }}
            >
              {f}
            </button>
          ))}
        </div>
        <span className="text-[11px] text-slate-500 font-mono">{filtered.length} зогсоол</span>

        {/* Edit routes button — opens RouteBuilderModal */}
        <div className="ml-auto">
          {editDatasetId ? (
            <button
              onClick={() => {
                if (!s.stopDetails.length) {
                  showToast("Зогсоолын мэдээлэл байхгүй байна", "info");
                  return;
                }
                setShowEditModal(true);
              }}
              className="flex items-center gap-1.5 px-3 py-1.5 rounded-xl text-[11px] font-bold border-[1.5px] border-violet-300 text-violet-500 bg-violet-500/6 hover:bg-violet-500/12 transition-all"
            >
              <EditIcon size="size-4"/> Засах
            </button>
          ) : (
            <span className="text-[10px] text-slate-400 italic">
              (Засвар оруулахын тулд өгөгдөл байх ёстой)
            </span>
          )}
        </div>
      </div>

      {/* Table */}
      <div className="flex-1 overflow-auto px-4 pb-4">
        <table className="w-full border-collapse">
          <thead className="sticky top-0 z-10 bg-slate-100">
            <tr>
              <Th label="Fleet"     k="fleet"       />
              <Th label="Truck"     k="truck_id"    />
              <Th label="Trip"      k="trip_number" />
              <Th label="Departs"   k="departs_at"  />
              <Th label="Returns"   k="returns_at"  />
              <Th label="Stops"     k="stops"       />
              <Th label="Dist km"   k="distance_km" />
              <Th label="Dur min"   k="duration_min"/>
              <Th label="Load kg"   k="util_kg_pct" />
              <Th label="Load m³"   k="util_m3_pct" />
              <Th label="Fuel ₮"    k="cost_fuel"   />
              <Th label="Man-hrs"   k="man_hours"   />
              <Th label="Total ₮"   k="cost_total"  />
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

      {/* Route Builder Modal for editing */}
      <RouteBuilderModal
        open={showEditModal}
        onClose={() => setShowEditModal(false)}
        mode="edit"
        initialTitle={`${s.summary?.mode ?? "Edited"} (edited)`}
        initialRoutes={editInitialRoutes}
        datasetId={editDatasetId}
        groupId={editGroupId ?? undefined}
      />
    </div>
  );
}