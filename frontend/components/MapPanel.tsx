"use client";
import { useEffect, useRef, useState } from "react";
import { useApp, buildBadges } from "@/lib/state";
import { fmtSec } from "@/lib/api";
import type { Store, StopDetail, MapRoute } from "@/types/vrp";
import "leaflet/dist/leaflet.css";

const UB:  [number,number] = [47.9179, 106.9177];
const DRY: [number,number] = [47.8847516, 106.7932466];
const COL: [number,number] = [47.80758101116645, 107.19407110357587];

/* synthesise a Store from stop data when no dataset loaded */
function synthStore(sid: string, stops: StopDetail[]): Store | null {
  const m = stops.find(d => d.store_id === sid);
  if (!m) return null;
  const ds = stops.filter(d => d.store_id === sid && d.fleet === "DRY");
  const cs = stops.filter(d => d.store_id === sid && d.fleet === "COLD");
  return {
    id: 0, dataset_id: "",
    store_id: m.store_id, node_id: m.store_id,
    eng_name: m.eng_name, mn_name: m.mn_name,
    address: m.address, detail_addr: m.detail_addr ?? "",
    lat: m.lat, lon: m.lon, open_s: 0, close_s: 86399,
    dry_cbm: 0, cold_cbm: 0,
    dry_kg:  ds.reduce((a, d) => a + d.demand_kg, 0),
    cold_kg: cs.reduce((a, d) => a + d.demand_kg, 0),
    has_dry: ds.length > 0, has_cold: cs.length > 0,
  };
}

export default function MapPanel() {
  const { s, d } = useApp();
  const { stores, mapData, routeVis, fleetFilter, activeJobId, stopDetails } = s;

  const mapRef     = useRef<HTMLDivElement>(null);
  const mapInst    = useRef<any>(null);
  const storeLyr   = useRef<Map<string, any>>(new Map());
  const routeLyr   = useRef<Map<string, { poly: any; dots: any[] }>>(new Map());
  const inited     = useRef(false);

  /* drawer state — fully local, no global dispatch needed */
  const [drawerStore,  setDrawerStore]  = useState<Store | null>(null);
  const [drawerDels,   setDrawerDels]   = useState<StopDetail[]>([]);
  const [drawerOpen,   setDrawerOpen]   = useState(false);
  const [routeControlCollapsed, setRouteControlCollapsed] = useState(false);
  const [badgesVisible, setBadgesVisible] = useState(true);

  /* stable ref so Leaflet callbacks never go stale */
  const openRef = useRef<(nodeId: string) => void>(() => {});
  useEffect(() => {
    openRef.current = (nodeId: string) => {
      const found = s.stores.find(st => st.node_id === nodeId)
        || synthStore(nodeId, s.stopDetails);
      if (!found) return;
      setDrawerStore(found);
      setDrawerDels(s.stopDetails.filter(dd => dd.store_id === found.store_id));
      setDrawerOpen(true);
      setRouteControlCollapsed(true); // Collapse route control when shop opens
      d({ t: "SET_SEL", v: nodeId });
    };
  }, [s.stores, s.stopDetails, d]);

  /* ── init map once ─────────────────────────────────── */
  useEffect(() => {
    if (typeof window === "undefined" || !mapRef.current || inited.current) return;
    const L = require("leaflet");
    delete (L.Icon.Default.prototype as any)._getIconUrl;

    const map = L.map(mapRef.current, {
      zoomControl: true, attributionControl: false, preferCanvas: true,
    });
    L.tileLayer("https://{s}.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}{r}.png", {
      maxZoom: 19, subdomains: "abcd",
    }).addTo(map);
    map.setView(UB, 12);
    mapInst.current = map;
    // map.on("zoomend", () => {
    //   // force re-render of markers
    //   storeLyr.current.forEach(m => map.removeLayer(m));
    //   storeLyr.current.clear();
    // });
    inited.current  = true;

    /* depot markers */
    [
      { pos: DRY, c: "#5B7CFA", icon: "🏭", label: "Dry DC",  dep: "13:00" },
      { pos: COL, c: "#0EA5E9", icon: "❄️",  label: "Cold DC", dep: "03:00" },
    ].forEach(dep => {
      const ic = L.divIcon({
        html: `<div style="width:40px;height:40px;border-radius:50%;background:${dep.c};display:flex;align-items:center;justify-content:center;font-size:20px;border:3px solid #fff;box-shadow:0 4px 16px ${dep.c}55;">${dep.icon}</div>`,
        className: "", iconSize: [40, 40], iconAnchor: [20, 20], popupAnchor: [0, -22],
      });
      L.marker(dep.pos, { icon: ic, zIndexOffset: 3000 }).addTo(map)
        .bindPopup(`<div style="font-family:Inter,sans-serif"><b style="color:${dep.c};font-size:13px">${dep.label}</b><br/><span style="font-size:11px;color:#7B82A0">Departs <b style="color:#1A1D2E">${dep.dep}</b></span></div>`);
    });

    return () => { map.remove(); mapInst.current = null; inited.current = false; };
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  /* ── store markers — rebuilds when visibility/filter changes ── */
  useEffect(() => {
    if (!mapInst.current || !inited.current) return;
    const L = require("leaflet");
    const map = mapInst.current;
    // const zoom = map.getZoom();
    storeLyr.current.forEach(m => map.removeLayer(m));
    storeLyr.current.clear();

    /* either real stores or synthesised from stop details */
    const effectiveStores: Store[] = stores.length > 0 ? stores : (() => {
      const seen = new Set<string>();
      return stopDetails.flatMap(dd => {
        if (seen.has(dd.store_id)) return [];
        seen.add(dd.store_id);
        const st = synthStore(dd.store_id, stopDetails);
        return st ? [st] : [];
      });
    })();
    if (!effectiveStores.length) return;

    // const badgeMap = buildBadges(stopDetails, mapData, routeVis, fleetFilter);

    effectiveStores.forEach(st => {
      const isActive = mapData.some(route =>
        routeVis[route.route_id] !== false &&
        route.stops.some(s => s.store_id === st.store_id)
      );

      // 🔥 only show badges when zoomed in
      // const showBadges = zoom >= 15;
      const showBadges = badgesVisible;

      let icon;

      if (showBadges && isActive) {
        const badges = buildBadges(stopDetails, mapData, routeVis, fleetFilter)[st.store_id] ?? [];
        icon = smartBadgeIcon(badges);
      } else {
        icon = simpleStoreIcon(st, isActive);
      }

      const { html, size, anchor } = icon;
      const ic = L.divIcon({ html, className: "", iconSize: size, iconAnchor: anchor, popupAnchor: [0, -anchor[1]] });
      const mk = L.marker([st.lat, st.lon], { icon: ic, zIndexOffset: 100 })
        .addTo(map).on("click", () => openRef.current(st.node_id));
      storeLyr.current.set(st.node_id, mk);
    });
    /* NO fitBounds — map never auto-zooms */
  }, [stores, stopDetails, mapData, routeVis, fleetFilter, activeJobId, badgesVisible]);

  /* ── route polylines ───────────────────────────────── */
  useEffect(() => {
    if (!mapInst.current || !inited.current) return;
    const L = require("leaflet");
    const map = mapInst.current;
    routeLyr.current.forEach(({ poly, dots }) => { map.removeLayer(poly); dots.forEach(dot => map.removeLayer(dot)); });
    routeLyr.current.clear();
    if (!mapData.length) return;

    mapData.forEach(route => {
      const coords: [number, number][] = (route.polyline?.length >= 2
        ? route.polyline : route.stops.map(s => [s.lat, s.lon])
      ).map(([a, b]) => [a, b] as [number, number]);

      const poly = L.polyline(coords, {
        color: route.color, weight: 4, opacity: 0.8,
        dashArray: route.line_style === "dashed" ? "8,5" : undefined,
        lineCap: "round", lineJoin: "round",
      }).addTo(map)
        .bindPopup(`<div style="font-family:Inter,sans-serif"><b style="color:${route.color}">${route.fleet} · ${route.truck_id} T${route.trip_number}</b><div style="font-size:11px;color:#7B82A0;margin:3px 0">${route.sched_info}</div><div style="font-size:11px">🏪 ${route.stops.length} · 📏 ${route.summary.distance_km}km · ⏱ ${route.summary.duration_min}min</div></div>`);

      const dots: any[] = route.stops.map(stop => {
        // Small coloured dot — no trip-number label, but fully clickable.
        // zIndexOffset 200 > store markers (100) so dots sit on top;
        // the click handler forwards to the store drawer so nothing is lost.
        const ic = L.divIcon({
          // html: `<div style="width:14px;height:14px;border-radius:50%;background:${route.color};border:2.5px solid #fff;box-shadow:0 2px 8px ${route.color}88;cursor:pointer;"></div>`,
          className: "", iconSize: [14, 14], iconAnchor: [7, 7],
        });
        return L.marker([stop.lat, stop.lon], { icon: ic, zIndexOffset: 200 })
          .addTo(map)
          .on("click", () => openRef.current(stop.store_id));
        });
      routeLyr.current.set(route.route_id, { poly, dots });
    });
    /* NO fitBounds */
  }, [mapData]);

  /* ── visibility sync ───────────────────────────────── */
  useEffect(() => {
    if (!mapInst.current) return;
    const map = mapInst.current;
    routeLyr.current.forEach(({ poly, dots }, rid) => {
      const route = mapData.find(r => r.route_id === rid);
      const ok = fleetFilter === "ALL" || (route?.fleet ?? "") === fleetFilter;
      const show = routeVis[rid] !== false && ok;
      if (show) { if (!map.hasLayer(poly)) map.addLayer(poly); dots.forEach(dot => { if (!map.hasLayer(dot)) map.addLayer(dot); }); }
      else       { if (map.hasLayer(poly)) map.removeLayer(poly); dots.forEach(dot => { if (map.hasLayer(dot)) map.removeLayer(dot); }); }
    });
  }, [routeVis, fleetFilter, mapData]);

  return (
    <div className="w-full h-full relative overflow-hidden">
      <div ref={mapRef} className="w-full h-full" />

      {/* empty hint */}
      {!s.activeDatasetId && !activeJobId && (
        <div className="absolute inset-0 flex items-center justify-center pointer-events-none">
          <div className="bg-white/95 backdrop-blur rounded-2xl p-8 text-center shadow-xl border border-slate-200 max-w-xs">
            <div className="text-5xl mb-3">🗺️</div>
            <div className="text-[16px] font-bold text-slate-900 mb-2">Ulaanbaatar</div>
            <div className="text-[12px] text-slate-500 leading-relaxed">Select a dataset to see store locations,<br/>or load a past job to see routes & badges</div>
          </div>
        </div>
      )}

      {mapData.length > 0 && <RouteControlPanel collapsed={routeControlCollapsed} setCollapsed={setRouteControlCollapsed} badgesVisible={badgesVisible} setBadgesVisible={setBadgesVisible} />}
      <StoreDrawer store={drawerStore} dels={drawerDels} open={drawerOpen} mapData={mapData}
        onClose={() => { setDrawerOpen(false); setRouteControlCollapsed(false); d({ t: "SET_SEL", v: null }); }} />
    </div>
  );
}

/* ═══════════════════════════════════════════════════════
   Route control panel (floating, top-right)
   ═══════════════════════════════════════════════════════ */
function RouteControlPanel({ collapsed, setCollapsed, badgesVisible, setBadgesVisible }: { 
  collapsed: boolean; 
  setCollapsed: (value: boolean) => void;
  badgesVisible: boolean;
  setBadgesVisible: (value: boolean) => void;
}) {
  const { s, d } = useApp();
  const { mapData, routeVis, fleetFilter } = s;

  const visible = fleetFilter === "ALL" ? mapData : mapData.filter(r => r.fleet === fleetFilter);
  const allOn   = visible.every(r => routeVis[r.route_id] !== false);

  return (
    <div className="absolute top-3 right-3 z-1000 bg-white/96 backdrop-blur border border-slate-200 rounded-2xl shadow-xl flex flex-col overflow-hidden"
      style={{ width: collapsed ? 46 : 228, maxHeight: "calc(100vh - 140px)", transition: "width 0.22s ease" }}>

      {/* header */}
      <div className="flex items-center justify-between px-3 py-2.5 border-b border-slate-200 shrink-0">
        {!collapsed && (
          <div className="flex items-center gap-3 flex-1">
            <span className="text-[10px] font-bold text-slate-500 uppercase tracking-wider">Routes</span>
            <div className="flex gap-1.5">
              <button onClick={() => d({ t: "TOGGLE_ALL", v: !allOn })}
                className="px-2 py-1 text-[10px] font-medium text-blue-600 hover:text-blue-700 hover:bg-blue-50 rounded-md transition-colors">
                {allOn ? "Hide all" : "Show all"}
              </button>
              <button onClick={() => setBadgesVisible(!badgesVisible)}
                className="px-2 py-1 text-[10px] font-medium text-purple-600 hover:text-purple-700 hover:bg-purple-50 rounded-md transition-colors">
                {badgesVisible ? "Hide badges" : "Show badges"}
              </button>
            </div>
          </div>
        )}
        <button onClick={() => setCollapsed(!collapsed)}
          className="w-7 h-7 rounded-lg border border-slate-200 bg-slate-50 flex items-center justify-center text-[12px] text-slate-500 hover:border-blue-500 hover:bg-blue-50 hover:text-blue-600 shrink-0 transition-all">
          {collapsed ? "☰" : "◀"}
        </button>
      </div>

      {!collapsed && <>
        {/* fleet filter */}
        <div className="flex gap-1.5 p-2 border-b border-slate-200 shrink-0">
          {(["ALL", "DRY", "COLD"] as const).map(f => {
            const act = fleetFilter === f;
            const c = f === "DRY" ? "rgb(91 124 250)" : f === "COLD" ? "rgb(14 165 233)" : "rgb(123 130 160)";
            return (
              <button key={f} onClick={() => d({ t: "FLEET", v: f })}
                className="flex-1 py-1.5 rounded-lg text-[10px] font-bold border-[1.5px] transition-all"
                style={{ borderColor: act ? c : "rgb(226 232 240)", background: act ? c + "18" : "#fff", color: act ? c : "rgb(123 130 160)" }}>
                {f}
              </button>
            );
          })}
        </div>

        {/* route list */}
        <div className="overflow-y-auto flex flex-col gap-0.5 p-1.5 flex-1">
          {visible.length === 0
            ? <p className="text-[11px] text-slate-500 text-center py-3">No {fleetFilter} routes</p>
            : visible.map(route => {
                const on = routeVis[route.route_id] !== false;
                return (
                  <button key={route.route_id}
                    onClick={() => d({ t: "TOGGLE_ROUTE", v: route.route_id })}
                    className="flex items-center gap-2 px-2 py-1.5 rounded-xl w-full text-left transition-all hover:bg-slate-50"
                    style={{ opacity: on ? 1 : 0.35 }}>
                    <div className="w-7 h-1 rounded-full shrink-0" style={{ background: route.color }} />
                    <div className="flex-1 min-w-0">
                      <div className="text-[11px] font-bold text-slate-900 truncate">
                        {route.truck_id}<span className="font-normal text-slate-500 ml-1">T{route.trip_number}</span>
                      </div>
                      <div className="text-[9px] text-slate-500">{route.fleet} · {route.stops.length} stops · {route.summary.distance_km}km</div>
                    </div>
                    <span className="text-[13px] shrink-0">{on ? "👁" : "🔕"}</span>
                  </button>
                );
              })
          }
        </div>
      </>}
    </div>
  );
}

/* ═══════════════════════════════════════════════════════
   Store detail drawer
   ═══════════════════════════════════════════════════════ */
function StoreDrawer({ store, dels, open, mapData, onClose }: {
  store: Store | null; dels: StopDetail[];
  open: boolean; mapData: MapRoute[]; onClose: () => void;
}) {
  const [tab, setTab] = useState<"info" | "dry" | "cold">("info");
  useEffect(() => { if (open) setTab("info"); }, [open, store?.store_id]);

  const dryDels  = dels.filter(d => d.fleet === "DRY");
  const coldDels = dels.filter(d => d.fleet === "COLD");

  return (
    <>
      {open && <div onClick={onClose} className="absolute inset-0 z-998" style={{ background: "rgba(71,85,105,0.06)", backdropFilter: "blur(1px)" }} />}
      <div className="absolute top-0 right-0 h-full z-999 bg-white border-l border-slate-200 flex flex-col"
        style={{ width: 370, transform: open ? "translateX(0)" : "translateX(100%)", transition: "transform 0.28s cubic-bezier(0.4,0,0.2,1)", boxShadow: open ? "-8px 0 40px rgba(59,130,246,0.12)" : "none" }}>
        {store && <>
          {/* header */}
          <div className="shrink-0 px-4 py-4 border-b border-slate-200" style={{ background: "linear-gradient(135deg,#F8FAFC,#F0F9FF)" }}>
            <div className="flex items-start gap-3 mb-3">
              <div className="w-10 h-10 rounded-xl flex items-center justify-center text-xl shrink-0" style={{ background: "linear-gradient(135deg,#3B82F6,#0EA5E9)" }}>🏪</div>
              <div className="flex-1 min-w-0">
                <div className="text-[14px] font-extrabold text-slate-900 truncate">{store.eng_name || store.store_id}</div>
                <div className="text-[11px] text-slate-500">{store.mn_name}</div>
              </div>
              <button onClick={onClose} className="w-7 h-7 rounded-lg border border-slate-200 bg-white flex items-center justify-center text-[13px] text-slate-500 hover:bg-slate-50 shrink-0">✕</button>
            </div>
            <div className="flex gap-1.5 flex-wrap mb-3">
              <span className="font-mono text-[10px] bg-blue-500/10 text-blue-500 px-2 py-0.5 rounded-md font-bold">#{store.store_id}</span>
              {store.has_dry  && <span className="text-[10px] font-bold px-2 py-0.5 rounded-full bg-blue-500/10 text-blue-500">📦 DRY</span>}
              {store.has_cold && <span className="text-[10px] font-bold px-2 py-0.5 rounded-full bg-sky-500/10 text-sky-600">❄️ COLD</span>}
              {dels.length > 0 && <span className="text-[10px] font-bold px-2 py-0.5 rounded-full bg-green-500/10 text-green-500">✅ {dels.length} delivery</span>}
            </div>
            {/* tabs */}
            <div className="flex gap-1 p-1 rounded-xl" style={{ background: "rgba(255,255,255,0.65)" }}>
              {[
                { k: "info", l: "📋 Details" },
                ...(dryDels.length  > 0 ? [{ k: "dry",  l: `📦 DRY (${dryDels.length})`  }] : []),
                ...(coldDels.length > 0 ? [{ k: "cold", l: `❄️ COLD (${coldDels.length})` }] : []),
              ].map(t => (
                <button key={t.k} onClick={() => setTab(t.k as any)}
                  className="flex-1 py-1.5 rounded-lg text-[11px] border-none cursor-pointer transition-all"
                  style={{ background: tab === t.k ? "#fff" : "transparent", color: tab === t.k ? "rgb(26 29 46)" : "rgb(123 130 160)", fontWeight: tab === t.k ? 700 : 500, boxShadow: tab === t.k ? "0 1px 4px rgba(59,130,246,0.1)" : "none" }}>
                  {t.l}
                </button>
              ))}
            </div>
          </div>

          {/* body */}
          <div className="flex-1 overflow-y-auto p-4">
            {tab === "info"  && <InfoTab store={store} />}
            {tab === "dry"   && <DeliveryTab dels={dryDels}  fleet="DRY"  mapData={mapData} />}
            {tab === "cold"  && <DeliveryTab dels={coldDels} fleet="COLD" mapData={mapData} />}
          </div>
        </>}
      </div>
    </>
  );
}

function InfoTab({ store }: { store: Store }) {
  return (
    <div className="flex flex-col gap-3">
      <Card title="📍 Location">
        {store.address && <Row label="Address" val={store.address} />}
        {store.detail_addr && <Row label="Detail" val={store.detail_addr} />}
        <Row label="Lat / Lon" val={`${store.lat.toFixed(5)}, ${store.lon.toFixed(5)}`} mono />
      </Card>
      <Card title="🕐 Hours">
        <div className="flex items-center gap-4">
          <TimeBox label="Opens"  time={fmtSec(store.open_s)}  color="#10B981" />
          <span className="text-muted">→</span>
          <TimeBox label="Closes" time={fmtSec(store.close_s)} color="#EF4444" />
        </div>
      </Card>
      {(store.has_dry || store.has_cold) && (
        <Card title="📊 Daily Demand">
          {store.has_dry  && <DBar label="DRY weight"  val={store.dry_kg}   unit="kg" color="#5B7CFA" max={5000} />}
          {store.has_dry  && <DBar label="DRY volume"  val={store.dry_cbm}  unit="m³" color="#5B7CFA" max={20}   />}
          {store.has_cold && <DBar label="COLD weight" val={store.cold_kg}  unit="kg" color="#0EA5E9" max={5000} />}
          {store.has_cold && <DBar label="COLD volume" val={store.cold_cbm} unit="m³" color="#0EA5E9" max={20}   />}
        </Card>
      )}
    </div>
  );
}

function DeliveryTab({ dels, fleet, mapData }: { dels: StopDetail[]; fleet: "DRY"|"COLD"; mapData: MapRoute[] }) {
  const baseColor = fleet === "DRY" ? "#5B7CFA" : "#0EA5E9";
  const getColor  = (dd: StopDetail) =>
    mapData.find(r => r.fleet === dd.fleet && r.truck_id === dd.truck_id && r.trip_number === dd.trip_number)?.color ?? baseColor;

  if (!dels.length) return (
    <div className="flex flex-col items-center justify-center py-10 text-slate-500 text-[12px] text-center">
      <span className="text-3xl mb-2">📭</span>No {fleet} deliveries scheduled.
    </div>
  );

  return (
    <div className="flex flex-col gap-3">
      <div className="flex items-center gap-3 p-3 rounded-xl border" style={{ background: baseColor + "0F", borderColor: baseColor + "30" }}>
        <span className="text-2xl">{fleet === "DRY" ? "📦" : "❄️"}</span>
        <div>
          <div className="text-[13px] font-bold" style={{ color: baseColor }}>{fleet === "DRY" ? "DRY DC" : "COLD DC"}</div>
          <div className="text-[11px] text-slate-500">Departs {fleet === "DRY" ? "13:00" : "03:00"} · {dels.length} trip{dels.length > 1 ? "s" : ""}</div>
        </div>
      </div>
      <div className="relative pl-7">
        {/* timeline line */}
        <div className="absolute left-2.25 top-3 bottom-3 w-0.5 rounded-full" style={{ background: `linear-gradient(to bottom,${baseColor},transparent)` }} />
        {dels.map((dd, i) => {
          const rc = getColor(dd);
          return (
            <div key={i} className="relative mb-3 last:mb-0">
              {/* timeline dot */}
              <div className="absolute -left-5.5 top-3 w-3.5 h-3.5 rounded-full bg-white flex items-center justify-center text-[7px] font-extrabold"
                style={{ border: `2.5px solid ${rc}`, color: rc, boxShadow: `0 0 0 3px ${rc}22` }}>{i + 1}</div>
              <div className="bg-white rounded-xl border p-3 shadow-sm" style={{ borderColor: rc + "33" }}>
                <div className="flex items-center justify-between mb-2">
                  <div className="flex items-center gap-2">
                    <div className="w-3 h-3 rounded-[3px] shrink-0" style={{ background: rc }} />
                    <span className="text-[12px] font-bold" style={{ color: rc }}>🚚 {dd.truck_id} · Trip {dd.trip_number}</span>
                  </div>
                  <span className="text-[10px] font-bold px-2 py-0.5 rounded-full" style={{ background: rc + "18", color: rc }}>Stop #{dd.stop_order}</span>
                </div>
                <div className="grid grid-cols-2 gap-2 mb-2">
                  <TBox label="Arrives"  time={dd.arrival}   color="#10B981" />
                  <TBox label="Departs"  time={dd.departure} color="#EF4444" />
                </div>
                <div className="grid grid-cols-2 gap-2">
                  <DPill icon="⚖️" label="Weight" val={`${dd.demand_kg.toFixed(1)} kg`} />
                  <DPill icon="📦" label="Volume" val={`${dd.demand_m3.toFixed(3)} m³`} />
                </div>
                {dd.delivery_day !== "Same day" && (
                  <div className="mt-2 text-[11px] font-semibold text-amber-500 bg-amber-500/8 rounded-lg px-2 py-1">📅 {dd.delivery_day}</div>
                )}
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

/* ── micro components ────────────────────────────────── */
function Card({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div className="bg-white rounded-xl border border-slate-200 p-3.5 shadow-sm">
      <div className="text-[10px] font-extrabold text-slate-500 uppercase tracking-[0.09em] mb-3">{title}</div>
      {children}
    </div>
  );
}
function Row({ label, val, mono }: { label: string; val: string; mono?: boolean }) {
  return (
    <div className="flex justify-between py-1.5 border-b border-slate-200 last:border-none">
      <span className="text-[11px] text-slate-500 shrink-0 mr-3">{label}</span>
      <span className={`text-[11px] text-right wrap-break-word ${mono ? "font-mono" : ""}`}>{val}</span>
    </div>
  );
}
function TimeBox({ label, time, color }: { label: string; time: string; color: string }) {
  return (
    <div className="bg-slate-50 rounded-lg p-2 text-center">
      <div className="text-[9px] text-slate-500 mb-1">{label}</div>
      <div className="text-[16px] font-extrabold font-mono" style={{ color }}>{time}</div>
    </div>
  );
}
function TBox({ label, time, color }: { label: string; time: string; color: string }) {
  return (
    <div className="bg-slate-50 rounded-lg p-1.5 text-center">
      <div className="text-[9px] text-slate-500 mb-0.5">{label}</div>
      <div className="text-[14px] font-bold font-mono" style={{ color }}>{time}</div>
    </div>
  );
}
function DBar({ label, val, unit, color, max }: { label: string; val: number; unit: string; color: string; max: number }) {
  const pct = Math.min(100, (val / max) * 100);
  return (
    <div className="mb-2">
      <div className="flex justify-between mb-1">
        <span className="text-[11px] text-slate-500">{label}</span>
        <span className="text-[11px] font-bold font-mono" style={{ color }}>{val.toFixed(unit === "m³" ? 3 : 1)} {unit}</span>
      </div>
      <div className="h-1.5 bg-slate-50 rounded-full overflow-hidden">
        <div className="h-full rounded-full transition-all" style={{ width: `${pct}%`, background: color }} />
      </div>
    </div>
  );
}
function DPill({ icon, label, val }: { icon: string; label: string; val: string }) {
  return (
    <div className="bg-slate-50 rounded-lg p-2 flex items-center gap-2">
      <span className="text-[13px]">{icon}</span>
      <div>
        <div className="text-[9px] text-slate-500 font-semibold uppercase">{label}</div>
        <div className="text-[11px] font-bold font-mono">{val}</div>
      </div>
    </div>
  );
}

/* ── store marker HTML ───────────────────────────────── */
function simpleStoreIcon(st: Store, isActive: boolean) {
  // default = small neutral
  let color = "#94A3B8"; // gray

  // active route store = green
  if (isActive) color = "#BD4AFF";

  return {
    html: `<div style="
      width: 8px;
      height: 8px;
      border-radius:50%;
      background:${color};
      border:2px solid #fff;
      box-shadow:0 1px 4px rgba(0,0,0,0.2);
    "></div>`,
    size: [8, 8] as [number, number],
    anchor: [4, 4] as [number, number],
  };
}

function smartBadgeIcon(badges: Array<{ color: string; label: string }>) {
  // 🔥 LIMIT BADGES (IMPORTANT)
  const limited = badges.slice(0, 2); // max 2 only

  const chips = limited.map(b =>
    `<div style="
      background:${b.color};
      color:#fff;
      border-radius:4px;
      padding:0 4px;
      height:14px;
      line-height:14px;
      font-size:8px;
      font-weight:700;
    ">${b.label}</div>`
  ).join("");

  return {
    html: `
      <div style="display:flex;flex-direction:column;align-items:center;gap:1px;">
        <div style="
          display:flex;
          gap:2px;
          background:#fff;
          border-radius:6px;
          padding:2px;
          box-shadow:0 1px 4px rgba(0,0,0,0.15);
        ">
          ${chips}
        </div>
        <div style="
          width:6px;height:6px;border-radius:50%;
          background:#22C55E;
          border:2px solid #fff;
        "></div>
      </div>
    `,
    size: [30, 22] as [number, number],
    anchor: [15, 22] as [number, number],
  };
}