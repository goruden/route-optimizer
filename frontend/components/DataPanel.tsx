"use client";
import { useState, useEffect } from "react";
import { useApp } from "@/lib/state";
import * as api from "@/lib/api";
import { Btn, Input, Modal, Confirm, UploadZone, SectionLabel, showToast, Sel, StepProgress, DatasetCreationModal } from "./ui";
import type { Step } from "./ui";
import type { Store, Vehicle, Dataset } from "@/types/vrp";

async function loadBoth(id: string, d: (a: any) => void) {
  const [stores, vehicles] = await Promise.all([api.getStores(id), api.getVehicles(id)]);
  d({ t: "SET_STORES", v: stores });
  d({ t: "SET_VEHICLES", v: vehicles });
}

// ── Dataset Card ──────────────────────────────────────────────────────
function DsCard({
  ds, active, onClick, onDelete, onRebuildMatrix, rebuildingMatrix,
}: {
  ds: Dataset; active: boolean;
  onClick: () => void; onDelete?: () => void;
  onRebuildMatrix?: () => void;
  rebuildingMatrix?: boolean;
}) {
  return (
    <div
      onClick={onClick}
      className={`rounded-xl border-[1.5px] p-2.5 cursor-pointer transition-all ${active ? "border-blue-500 bg-blue-500/5 shadow-sm" : "border-slate-200 bg-white hover:border-blue-500/40 hover:bg-slate-50"}`}
    >
      <div className="flex items-center justify-between mb-1">
        <span className="text-[12px] font-bold text-slate-900 truncate flex-1 min-w-0 mr-2">{ds.name}</span>
        <div onClick={e => e.stopPropagation()}>
          <Confirm onConfirm={() => onDelete?.()}>
            <button className="text-slate-300 hover:text-red-500 text-[11px] w-5 h-5 flex items-center justify-center shrink-0">✕</button>
          </Confirm>
        </div>
      </div>
      <div className="flex gap-2 text-[10px] text-slate-500 mb-1">
        <span>🏪 {ds.store_count}</span>
        <span>🚛 {ds.vehicle_count}</span>
        <span className={ds.has_matrix ? "text-green-500 font-semibold" : "text-amber-500 font-semibold"}>
          {ds.has_matrix ? "✅ matrix" : "⚠ no matrix"}
        </span>
      </div>
      {active && (
        <div className="flex gap-1.5 mt-2 pt-2 border-t border-slate-100" onClick={e => e.stopPropagation()}>
          <button
            onClick={onRebuildMatrix}
            disabled={rebuildingMatrix}
            className={`flex-1 flex items-center justify-center gap-1 text-[10px] font-semibold py-1.5 px-2 rounded-lg border transition-all ${rebuildingMatrix ? "border-blue-200 text-blue-400 cursor-not-allowed bg-blue-50" : "border-blue-200 text-blue-500 hover:bg-blue-50"}`}
          >
            {rebuildingMatrix
              ? <><span className="w-3 h-3 border border-blue-400 border-t-transparent rounded-full animate-spin inline-block" />Матрицыг үүсгэж байна...</>
              : <>↺ шинэчлэх</>
            }
          </button>
          <a
            href={api.exportDatasetUrl(ds.id)}
            download
            className="flex-1 flex items-center justify-center gap-1 text-[10px] font-semibold py-1.5 px-2 rounded-lg border border-green-200 text-green-600 hover:bg-green-50 transition-all no-underline"
            onClick={e => e.stopPropagation()}
          >
            ⬇ Татах
          </a>
        </div>
      )}
    </div>
  );
}

function Empty({ icon, msg }: { icon: string; msg: string }) {
  return <div className="flex flex-col items-center justify-center py-8 text-center"><span className="text-3xl mb-2">{icon}</span><p className="text-[11px] text-slate-500">{msg}</p></div>;
}

function StoreCard({ store: st, onEdit, onDelete }: { store: Store; onEdit: () => void; onDelete: () => void }) {
  const [exp, setExp] = useState(false);
  const fmtSec = (s: number) => api.fmtSec(s);
  const MR = ({ icon, v, mono }: { icon: string; v: string; mono?: boolean }) => (
    <div className="flex items-start gap-1.5 text-[10px]"><span className="shrink-0">{icon}</span><span className={`text-slate-500 wrap-break-word ${mono ? "font-mono" : ""}`}>{v}</span></div>
  );
  return (
    <div className="bg-white rounded-xl border border-slate-200 overflow-hidden shadow-sm">
      <div className="flex items-start gap-2.5 p-2.5">
        <div className="w-8 h-8 rounded-lg flex items-center justify-center text-base shrink-0 bg-blue-500/10">🏪</div>
        <div className="flex-1 min-w-0">
          <div className="flex items-start justify-between gap-2 mb-1">
            <div className="flex-1 min-w-0">
              <div className="text-[12px] font-bold text-slate-900 truncate">{st.eng_name || st.store_id}</div>
              {st.mn_name && <div className="text-[10px] text-slate-500 truncate">{st.mn_name}</div>}
            </div>
            <div className="flex gap-1.5 shrink-0">
              <button onClick={onEdit} className="w-6 h-6 rounded-md border border-slate-200 flex items-center justify-center text-[10px] text-slate-500 hover:border-blue-500">✏</button>
              <Confirm onConfirm={onDelete}><button className="w-6 h-6 rounded-md border border-red-300/50 flex items-center justify-center text-[10px] text-red-500 hover:bg-red-50">🗑</button></Confirm>
            </div>
          </div>
          <div className="flex gap-1.5 flex-wrap">
            <span className="font-mono text-[9px] bg-blue-500/8 text-blue-500 px-1.5 py-0.5 rounded font-bold">#{st.store_id}</span>
            {st.has_dry && <span className="text-[9px] font-bold px-1.5 py-0.5 rounded-full bg-blue-500/10 text-blue-500">DRY {st.dry_kg.toFixed(0)}kg</span>}
            {st.has_cold && <span className="text-[9px] font-bold px-1.5 py-0.5 rounded-full bg-sky-500/10 text-sky-600">COLD {st.cold_kg.toFixed(0)}kg</span>}
          </div>
        </div>
      </div>
      <button onClick={() => setExp(e => !e)} className="w-full py-1.5 bg-slate-50 border-t border-slate-200 text-[10px] text-slate-500 flex items-center justify-center gap-1.5">
        <span className={`inline-block transition-transform ${exp ? "rotate-90" : ""}`}>▶</span>{exp ? "Hide" : "Details"}
      </button>
      {exp && (
        <div className="p-2.5 border-t border-slate-200 flex flex-col gap-1.5">
          {st.address && <MR icon="📍" v={st.address} />}
          <MR icon="🗺" v={`${st.lat.toFixed(5)}, ${st.lon.toFixed(5)}`} mono />
          <MR icon="🕐" v={`${fmtSec(st.open_s)} – ${fmtSec(st.close_s)}`} mono />
          {st.has_dry && <MR icon="📦" v={`DRY: ${st.dry_kg.toFixed(1)}kg / ${st.dry_cbm.toFixed(3)}m³`} />}
          {st.has_cold && <MR icon="❄️" v={`COLD: ${st.cold_kg.toFixed(1)}kg / ${st.cold_cbm.toFixed(3)}m³`} />}
        </div>
      )}
    </div>
  );
}

function VehicleCard({ vehicle: v, onEdit, onDelete }: { vehicle: Vehicle; onEdit: () => void; onDelete: () => void }) {
  const c = v.fleet === "DRY" ? "#5B7CFA" : "#0EA5E9";
  return (
    <div className="bg-white rounded-xl border border-slate-200 p-2.5 shadow-sm flex items-center gap-2.5">
      <div className="w-9 h-9 rounded-xl flex items-center justify-center text-xl shrink-0" style={{ background: c + "18" }}>🚛</div>
      <div className="flex-1 min-w-0">
        <div className="flex items-center justify-between mb-1">
          <div className="flex items-center gap-2">
            <span className="font-bold text-[13px] text-slate-900">{v.truck_id}</span>
            <span className="text-[9px] font-extrabold px-1.5 py-0.5 rounded-md" style={{ background: c + "18", color: c }}>{v.fleet}</span>
          </div>
          <div className="flex gap-1.5">
            <button onClick={onEdit} className="w-6 h-6 rounded-md border border-slate-200 flex items-center justify-center text-[10px] text-slate-500 hover:border-blue-500">✏</button>
            <Confirm onConfirm={onDelete}><button className="w-6 h-6 rounded-md border border-red-300/50 flex items-center justify-center text-[10px] text-red-500 hover:bg-red-50">🗑</button></Confirm>
          </div>
        </div>
        <div className="text-[10px] text-slate-500 truncate mb-1">{v.description || v.depot}</div>
        <div className="flex gap-2 text-[10px]">
          <span className="font-mono font-bold text-slate-900">{v.cap_kg.toLocaleString()} kg</span>
          <span className="text-slate-300">·</span>
          <span className="font-mono font-bold text-slate-900">{v.cap_m3.toFixed(1)} m³</span>
          <span className="text-slate-300">·</span>
          <span className="text-amber-500">₮{v.fuel_cost_km}/km</span>
        </div>
      </div>
    </div>
  );
}

// ── Matrix rebuild progress modal ─────────────────────────────────────
function MatrixProgressModal({ open, steps }: { open: boolean; steps: Step[] }) {
  if (!open) return null;
  const doneCount = steps.filter(s => s.status === "done").length;
  const hasError = steps.some(s => s.status === "error");
  const pct = (doneCount / steps.length) * 100;

  return (
    <div className="fixed inset-0 z-9500 flex items-center justify-center p-4">
      <div className="absolute inset-0 bg-slate-900/30 backdrop-blur-sm" />
      <div className="relative bg-white rounded-2xl shadow-2xl w-full max-w-xs overflow-hidden">
        <div className="px-5 pt-5 pb-4 border-b border-slate-200 bg-linear-to-br from-blue-50 to-slate-50">
          <div className="flex items-center gap-3 mb-3">
            <div className={`w-10 h-10 rounded-xl flex items-center justify-center text-xl shadow-sm text-white ${hasError ? "bg-red-500" : "bg-blue-500"}`}>
              {hasError ? "⚠️" : "🗺"}
            </div>
            <div>
              <div className="text-[14px] font-extrabold text-slate-900">
                {hasError ? "Build Failed" : "Building Matrix"}
              </div>
              <div className="text-[10px] text-slate-500">
                {hasError ? "Check OSRM server" : "Computing all store routes…"}
              </div>
            </div>
          </div>
          <div className="w-full h-1.5 bg-slate-200 rounded-full overflow-hidden">
            <div
              className="h-full rounded-full transition-all duration-500 relative overflow-hidden"
              style={{ width: `${pct}%`, background: hasError ? "#EF4444" : "#5B7CFA" }}
            >
              {!hasError && pct > 0 && pct < 100 && (
                <div className="absolute inset-0 opacity-40" style={{
                  background: "linear-gradient(90deg, transparent, rgba(255,255,255,0.6), transparent)",
                  animation: "shimmer 1.5s infinite",
                }} />
              )}
            </div>
          </div>
        </div>
        <div className="p-5">
          <StepProgress steps={steps} />
        </div>
      </div>
    </div>
  );
}

// ── DataPanel ─────────────────────────────────────────────────────────
export function DataPanel() {
  const { s, d } = useApp();

  const [topHeight, setTopHeight] = useState(260);
  const [dragging, setDragging] = useState(false);
  useEffect(() => {
    function onMouseMove(e: MouseEvent) {
      if (!dragging) return;
      setTopHeight(prev => Math.min(window.innerHeight * 0.7, Math.max(120, prev + e.movementY)));
    }
    function onMouseUp() { setDragging(false); }
    window.addEventListener("mousemove", onMouseMove);
    window.addEventListener("mouseup", onMouseUp);
    document.body.style.userSelect = dragging ? "none" : "auto";
    return () => {
      window.removeEventListener("mousemove", onMouseMove);
      window.removeEventListener("mouseup", onMouseUp);
      document.body.style.userSelect = "auto";
    };
  }, [dragging]);

  const [subTab, setSubTab] = useState<"stores" | "vehicles">("stores");
  const [storeSrch, setStoreSrch] = useState("");
  const [vehSrch, setVehSrch] = useState("");
  const [showNewDs, setShowNewDs] = useState(false);
  const [dsName, setDsName] = useState("");
  const [dsSF, setDsSF] = useState<File | null>(null);

  // ── Creation progress state ──────────────────────────────
  const [creationSteps, setCreationSteps] = useState<Step[]>([]);
  const [showCreationProgress, setShowCreationProgress] = useState(false);

  // ── Matrix rebuild progress state ────────────────────────
  const [matrixSteps, setMatrixSteps] = useState<Step[]>([]);
  const [showMatrixProgress, setShowMatrixProgress] = useState(false);
  const [activeRebuildId, setActiveRebuildId] = useState<string | null>(null);

  const [showAddStore, setShowAS] = useState(false);
  const [editingStore, setEditSt] = useState<Store | null>(null);
  const [showAddVeh, setShowAV] = useState(false);
  const [editingVeh, setEditVh] = useState<Vehicle | null>(null);

  const blank = { store_id: "", eng_name: "", mn_name: "", address: "", lat: 47.9174, lon: 106.9238, open_s: 0, close_s: 86399, dry_kg: 0, dry_cbm: 0, cold_kg: 0, cold_cbm: 0 };
  const [sf, setSf] = useState(blank);
  const vb = { truck_id: "", description: "", depot: "Dry DC", cap_kg: 2500, cap_m3: 13.76, fuel_cost_km: 588, vehicle_cost: 124708, labor_cost: 163343 };
  const [vf, setVf] = useState(vb);

  const fStores = s.stores.filter((st: any) => { const q = storeSrch.toLowerCase(); return !q || st.store_id.toLowerCase().includes(q) || st.eng_name?.toLowerCase().includes(q) || st.mn_name?.toLowerCase().includes(q); });
  const fVehs = s.vehicles.filter((v: any) => { const q = vehSrch.toLowerCase(); return !q || v.truck_id.toLowerCase().includes(q) || v.fleet.toLowerCase().includes(q); });

  function makeCreationSteps(): Step[] {
    return [
      { id: "create", label: "Дата өгөгдлийг үүсгэж байна", status: "waiting" },
      { id: "stores", label: "Өгөгдлийг оруулж байна", status: "waiting" },
      { id: "osrm", label: "OSRM руу холбогдож байна", status: "waiting" },
      { id: "matrix", label: "Зайны матрицыг тооцож байна", sublabel: "1-3 минут хүлээгээрэй", status: "waiting" },
      { id: "save", label: "Матрицыг өгөгдлийн санд хадаглаж байна", status: "waiting" },
    ];
  }

  function makeMatrixSteps(): Step[] {
    return [
      { id: "osrm", label: "OSRM руу холбогдож байна", status: "waiting" },
      { id: "nodes", label: "Дэлгүүрүүдийн байршлыг бэлтгэж байна", status: "waiting" },
      { id: "compute", label: "Бүх замыг тооцоолж байна", sublabel: "OSRM бүх дэлгүүр хоорондын замыг тооцож байна", status: "waiting" },
      { id: "save", label: "Матрицыг өгөгдлийн санд хадаглаж байна", status: "waiting" },
    ];
  }

  function updateStep(steps: Step[], id: string, status: Step["status"], sublabel?: string): Step[] {
    return steps.map(s => s.id === id ? { ...s, status, ...(sublabel ? { sublabel } : {}) } : s);
  }

  // ── Handlers ──────────────────────────────────────────────

  async function createDs() {
    if (!dsName || !dsSF) { showToast("Name + Stores file required", "error"); return; }

    const steps = makeCreationSteps();
    setCreationSteps(steps);
    setShowCreationProgress(true);
    setShowNewDs(false);

    let currentSteps = steps;

    const setStep = (id: string, status: Step["status"], sublabel?: string) => {
      currentSteps = updateStep(currentSteps, id, status, sublabel);
      setCreationSteps([...currentSteps]);
    };

    try {
      setStep("create", "active");
      await new Promise(r => setTimeout(r, 300));
      const ds = await api.createDataset(dsName, dsSF);
      setStep("create", "done");

      setStep("stores", "active");
      d({ t: "SET_DATASETS", v: await api.getDatasets() });
      d({ t: "SET_DS", v: ds.id });
      await loadBoth(ds.id, d);
      setStep("stores", "done");

      setStep("osrm", "active");
      await new Promise(r => setTimeout(r, 500));
      setStep("osrm", "done");

      setStep("matrix", "active");
      await api.rebuildDatasetMatrix(ds.id);
      setStep("matrix", "done");

      setStep("save", "active");
      d({ t: "SET_DATASETS", v: await api.getDatasets() });
      await new Promise(r => setTimeout(r, 400));
      setStep("save", "done");

      setDsName(""); setDsSF(null);
      setTimeout(() => {
        setShowCreationProgress(false);
        showToast("✅ Өгөгдөл бэлэн!", "success");
      }, 1200);

    } catch (e: any) {
      // Mark current active step as error
      const activeStep = currentSteps.find(s => s.status === "active");
      if (activeStep) setStep(activeStep.id, "error", e.message ?? "Failed");

      if (currentSteps.find(s => s.id === "matrix" && s.status === "error")) {
        showToast("Матриц үүсгэхэд алдаа гарлаа", "error");
        setTimeout(() => {
          setShowCreationProgress(false);
        }, 3000);
      } else {
        setTimeout(() => {
          setShowCreationProgress(false);
          showToast(e.message ?? "Өгөгдөл үүсгэхэд алдаа гарлаа", "error");
        }, 2000);
      }
    }
  }

  async function rebuildMatrix(datasetId: string) {
    const steps = makeMatrixSteps();
    setMatrixSteps(steps);
    setShowMatrixProgress(true);
    setActiveRebuildId(datasetId);

    let currentSteps = steps;
    const setStep = (id: string, status: Step["status"], sublabel?: string) => {
      currentSteps = updateStep(currentSteps, id, status, sublabel);
      setMatrixSteps([...currentSteps]);
    };

    try {
      setStep("osrm", "active");
      await new Promise(r => setTimeout(r, 600));
      setStep("osrm", "done");

      setStep("nodes", "active");
      await new Promise(r => setTimeout(r, 400));
      setStep("nodes", "done");

      setStep("compute", "active");
      await api.rebuildDatasetMatrix(datasetId);
      setStep("compute", "done");

      setStep("save", "active");
      d({ t: "SET_DATASETS", v: await api.getDatasets() });
      await new Promise(r => setTimeout(r, 400));
      setStep("save", "done");

      setTimeout(() => {
        setShowMatrixProgress(false);
        setActiveRebuildId(null);
        showToast("✅ Матрицыг дахин үүсэглээ!", "success");
      }, 1000);
    } catch (e: any) {
      const activeStep = currentSteps.find(s => s.status === "active");
      if (activeStep) setStep(activeStep.id, "error", e.message ?? "Failed");
      setTimeout(() => {
        setShowMatrixProgress(false);
        setActiveRebuildId(null);
        showToast(`Матрицыг дахин үүсгэхэд алдаа гарлаа: ${e.message}`, "error");
      }, 2500);
    }
  }

  async function delDs(id: string) {
    await api.deleteDataset(id);
    d({ t: "SET_DATASETS", v: await api.getDatasets() });
    if (s.activeDatasetId === id) d({ t: "SET_DS", v: null });
  }

  async function saveStore() {
    if (!s.activeDatasetId) return;
    try {
      if (editingStore) { await api.updateStore(s.activeDatasetId, editingStore.id, sf as any); }
      else { await api.addStore(s.activeDatasetId, { ...sf, store_id: String(sf.store_id) }); }
      d({ t: "SET_STORES", v: await api.getStores(s.activeDatasetId) });
      setShowAS(false); setEditSt(null); setSf(blank);
      showToast(editingStore ? "Дэлгүүр засагдлаа" : "Дэлгүүр нэмэгдлээ", "success");
    } catch (e: any) { showToast(e.message, "error"); }
  }

  async function delStore(id: number) {
    if (!s.activeDatasetId) return;
    await api.deleteStore(s.activeDatasetId, id);
    d({ t: "SET_STORES", v: await api.getStores(s.activeDatasetId) });
    showToast("Дэлгүүр устгагдлаа", "info");
  }

  async function delVehicle(id: number) {
    if (!s.activeDatasetId) return;
    await api.deleteVehicle(s.activeDatasetId, id);
    d({ t: "SET_VEHICLES", v: await api.getVehicles(s.activeDatasetId) });
    showToast("Тээврийн хэрэгсэл устгагдлаа", "info");
  }

  async function saveVeh() {
    if (!s.activeDatasetId) return;
    try {
      if (editingVeh) await api.updateVehicle(s.activeDatasetId, editingVeh.id, vf as any);
      else await api.addVehicle(s.activeDatasetId, vf as any);
      d({ t: "SET_VEHICLES", v: await api.getVehicles(s.activeDatasetId) });
      setShowAV(false); setEditVh(null); setVf(vb);
      showToast(editingVeh ? "Тээврийн хэрэгсэл засагдлаа" : "Тээврийн хэрэгсэл нэмэгдлээ", "success");
    } catch (e: any) { showToast(e.message, "error"); }
  }

  return (
    <div className="h-full flex flex-col overflow-hidden">

      {/* Progress modals */}
      <DatasetCreationModal
        open={showCreationProgress}
        steps={creationSteps}
        title="Creating Dataset"
      />
      <MatrixProgressModal
        open={showMatrixProgress}
        steps={matrixSteps}
      />

      {/* TOP: Datasets */}
      <div style={{ height: topHeight }} className="shrink-0 p-2.5 border-b border-slate-200 bg-white overflow-y-auto">
        <div className="flex items-center justify-between mb-2">
          <span className="text-[10px] font-bold text-slate-500 uppercase tracking-widest">Өгөгдөл</span>
          <Btn size="sm" variant="ghost" onClick={() => setShowNewDs(true)}>+ Нэмэх</Btn>
        </div>
        <div className="flex flex-col gap-1.5">
          {!s.datasets.length && (
            <div className="flex flex-col items-center justify-center py-6 text-center">
              <span className="text-3xl mb-2">📂</span>
              <p className="text-[11px] text-slate-500">Өгөгдөл байхгүй байна</p>
              <button onClick={() => setShowNewDs(true)} className="mt-2 text-[11px] font-semibold text-blue-500 hover:underline">
                Өгөгдөл үүсгэх →
              </button>
            </div>
          )}
          {s.datasets.map((ds: any) => (
            <DsCard
              key={ds.id} ds={ds} active={s.activeDatasetId === ds.id}
              onClick={() => {
                const id = s.activeDatasetId === ds.id ? null : ds.id;
                d({ t: "SET_DS", v: id });
                if (id) loadBoth(id, d);
              }}
              onDelete={() => delDs(ds.id)}
              onRebuildMatrix={() => rebuildMatrix(ds.id)}
              rebuildingMatrix={showMatrixProgress && activeRebuildId === ds.id}
            />
          ))}
        </div>
      </div>

      {/* Splitter */}
      <div onMouseDown={() => setDragging(true)} className="h-1.5 bg-slate-200 cursor-ns-resize hover:bg-blue-400 transition-colors" title="Drag to resize" />

      {/* BOTTOM: Stores / Vehicles */}
      {s.activeDatasetId ? (
        <>
          <div className="flex shrink-0 border-b border-slate-200 bg-white">
            {(["stores", "vehicles"] as const).map(t => (
              <button key={t} onClick={() => setSubTab(t)}
                className={`flex-1 py-2 text-[11px] font-semibold border-b-[2.5px] flex items-center justify-center gap-1.5 transition-all ${subTab === t ? "border-blue-500 text-blue-500" : "border-transparent text-slate-500"}`}>
                {t === "stores" ? "🏪 Дэлгүүр" : "🚛 Тээврийн хэрэгсэл"}
                <span className={`text-[9px] font-extrabold px-1.5 py-0.5 rounded-full ${subTab === t ? "bg-blue-500/12 text-blue-500" : "bg-slate-100 text-slate-500"}`}>
                  {t === "stores" ? s.stores.length : s.vehicles.length}
                </span>
              </button>
            ))}
          </div>

          <div className="shrink-0 flex gap-2 px-2.5 py-2 bg-slate-50 border-b border-slate-200">
            <input
              value={subTab === "stores" ? storeSrch : vehSrch}
              onChange={e => subTab === "stores" ? setStoreSrch(e.target.value) : setVehSrch(e.target.value)}
              placeholder={`Search ${subTab}…`}
              className="flex-1 text-[11px] border border-slate-200 rounded-lg px-2.5 py-1.5 bg-white outline-none focus:border-blue-500"
            />
            <Btn size="sm" variant="primary"
              onClick={() => {
                if (subTab === "stores") { setSf(blank); setEditSt(null); setShowAS(true); }
                else { setVf(vb); setEditVh(null); setShowAV(true); }
              }}>
              +Нэмэх
            </Btn>
          </div>

          <div className="shrink-0 px-2.5 py-1 bg-slate-50 text-[10px] text-slate-500 border-b border-slate-200">
            {subTab === "stores" ? `${fStores.length}/${s.stores.length} дэлгүүр` : `${fVehs.length}/${s.vehicles.length} тээврийн хэрэгсэл`}
          </div>

          <div className="flex-1 overflow-y-auto p-2.5 space-y-2 min-h-0">
            {subTab === "stores" && (
              fStores.length === 0
                ? <Empty icon="🏪" msg={storeSrch ? "Тохирох хайлт олдсонгүй" : "Дэлгүүр байхгүй байна"} />
                : fStores.map((st: any) => (
                  <StoreCard key={st.id} store={st}
                    onEdit={() => { setSf({ store_id: st.store_id, eng_name: st.eng_name, mn_name: st.mn_name, address: st.address, lat: st.lat, lon: st.lon, open_s: st.open_s, close_s: st.close_s, dry_kg: st.dry_kg, dry_cbm: st.dry_cbm, cold_kg: st.cold_kg, cold_cbm: st.cold_cbm }); setEditSt(st); setShowAS(true); }}
                    onDelete={() => delStore(st.id)} />
                ))
            )}
            {subTab === "vehicles" && (
              fVehs.length === 0
                ? <Empty icon="🚛" msg={vehSrch ? "Тохирох хайлт олдсонгүй" : "Тээврийн хэрэгсэл байхгүй байна"} />
                : fVehs.map((v: any) => (
                  <VehicleCard key={v.id} vehicle={v}
                    onEdit={() => { setVf({ truck_id: v.truck_id, description: v.description, depot: v.depot, cap_kg: v.cap_kg, cap_m3: v.cap_m3, fuel_cost_km: v.fuel_cost_km, vehicle_cost: v.vehicle_cost, labor_cost: v.labor_cost }); setEditVh(v); setShowAV(true); }}
                    onDelete={() => delVehicle(v.id)} />
                ))
            )}
          </div>
        </>
      ) : (
        <div className="flex-1 flex flex-col items-center justify-center text-center p-6 text-slate-500">
          <span className="text-4xl mb-3">📂</span>
          <p className="font-semibold text-slate-900 text-[13px] mb-1">Өгөгдлөөс сонгоно уу</p>
          <p className="text-[11px]">Дээрх өгөгдөлүүдээс сонгоно уу</p>
        </div>
      )}

      {/* ── Modals ── */}
      <Modal title="💾 Шинэ өгөгдөл үүсгэх" open={showNewDs}
        onClose={() => { setShowNewDs(false); setDsName(""); setDsSF(null); }}
        onOk={createDs} okLabel="Create Dataset">
        <div className="flex flex-col gap-3">
          <Input label="Өгөгдлийн нэр *" value={dsName} onChange={e => setDsName(e.target.value)} placeholder="e.g. March 2026" />
          <UploadZone label="Stores + Vehicles Excel *" icon="📋" accept=".xlsx" onFile={setDsSF} fileName={dsSF?.name} />
          <div className="bg-blue-50 border border-blue-200 rounded-xl p-3 text-[11px] text-blue-600">
            <strong>ℹ️ Auto-build:</strong> Өгөгдөлийг үүсгэх үед матриц автоматаар үүснэ.
          </div>
        </div>
      </Modal>

      <Modal title={editingStore ? "✏️ Засах" : "🏪 Нэмэх"} open={showAddStore}
        onClose={() => { setShowAS(false); setEditSt(null); }} onOk={saveStore} okLabel={"Хадгалах"}>
        <div className="grid grid-cols-2 gap-3">
          {!editingStore && <div className="col-span-2"><Input label="Store ID *" value={sf.store_id} onChange={e => setSf({ ...sf, store_id: e.target.value })} /></div>}
          <Input label="Name EN" value={sf.eng_name} onChange={e => setSf({ ...sf, eng_name: e.target.value })} />
          <Input label="Name MN" value={sf.mn_name} onChange={e => setSf({ ...sf, mn_name: e.target.value })} />
          <div className="col-span-2"><Input label="Address" value={sf.address} onChange={e => setSf({ ...sf, address: e.target.value })} /></div>
          <Input type="number" label="Lat" value={sf.lat} onChange={e => setSf({ ...sf, lat: Number(e.target.value) })} step={0.0001} />
          <Input type="number" label="Lon" value={sf.lon} onChange={e => setSf({ ...sf, lon: Number(e.target.value) })} step={0.0001} />
          <Input type="number" label="Open (s)" value={sf.open_s} onChange={e => setSf({ ...sf, open_s: Number(e.target.value) })} />
          <Input type="number" label="Close (s)" value={sf.close_s} onChange={e => setSf({ ...sf, close_s: Number(e.target.value) })} />
          <Input type="number" label="DRY kg/d" value={sf.dry_kg} onChange={e => setSf({ ...sf, dry_kg: Number(e.target.value) })} step={0.1} />
          <Input type="number" label="DRY m³/d" value={sf.dry_cbm} onChange={e => setSf({ ...sf, dry_cbm: Number(e.target.value) })} step={0.001} />
          <Input type="number" label="COLD kg/d" value={sf.cold_kg} onChange={e => setSf({ ...sf, cold_kg: Number(e.target.value) })} step={0.1} />
          <Input type="number" label="COLD m³/d" value={sf.cold_cbm} onChange={e => setSf({ ...sf, cold_cbm: Number(e.target.value) })} step={0.001} />
        </div>
      </Modal>

      <Modal title={editingVeh ? "✏️ Засах" : "🚛 Нэмэх"} open={showAddVeh}
        onClose={() => { setShowAV(false); setEditVh(null); }} onOk={saveVeh} okLabel={"Хадгалах"}>
        <div className="grid grid-cols-2 gap-3">
          {!editingVeh && <Input label="Truck ID *" value={vf.truck_id} onChange={e => setVf({ ...vf, truck_id: e.target.value })} />}
          <Input label="Description" value={vf.description} onChange={e => setVf({ ...vf, description: e.target.value })} />
          <div className="col-span-2">
            <Sel label="Depot" value={vf.depot} onChange={e => setVf({ ...vf, depot: e.target.value })} options={[{ v: "Dry DC", l: "Dry DC" }, { v: "Cold DC", l: "Cold DC" }]} />
          </div>
          <Input type="number" label="Cap kg" value={vf.cap_kg} onChange={e => setVf({ ...vf, cap_kg: Number(e.target.value) })} />
          <Input type="number" label="Cap m³" value={vf.cap_m3} onChange={e => setVf({ ...vf, cap_m3: Number(e.target.value) })} step={0.01} />
          <Input type="number" label="Fuel ₮/km" value={vf.fuel_cost_km} onChange={e => setVf({ ...vf, fuel_cost_km: Number(e.target.value) })} />
          <Input type="number" label="Vehicle ₮/d" value={vf.vehicle_cost} onChange={e => setVf({ ...vf, vehicle_cost: Number(e.target.value) })} />
          <Input type="number" label="Labor ₮/d" value={vf.labor_cost} onChange={e => setVf({ ...vf, labor_cost: Number(e.target.value) })} />
        </div>
      </Modal>
    </div>
  );
}