"use client";
import { useState, useEffect, useRef } from "react";
import { useApp } from "@/lib/state";
import * as api from "@/lib/api";
import { Btn, SectionLabel, showToast, ProgressBar, SolverTerminal } from "./ui";
import { StoreIcon, VehicleIcon, CheckIcon, WarningIcon, LightningIcon, RulerIcon, BalanceIcon, MapIcon, MoneyIcon, FolderIcon, TargetIcon, SettingsIcon, TagIcon } from "./icons";
import type { Dataset } from "@/types/vrp";
import { Tooltip } from "./Tooltip";

const MODE_INFO=[
  { v:"cheapest",  
    e:<MoneyIcon size="size-6" />,
    l:"Хямд",  
    desc:"Min fuel ₮/km",
    tooltip:"Хамгийн бага зардал"
  },
  {v:"fastest",   
    e:<LightningIcon size="size-6" />,
    l:"Хурдан",   
    desc:"Min travel time",
    tooltip:"Хамгийн бага хугацаа"
  },
  {v:"shortest",  
    e:<RulerIcon size="size-6" />,
    l:"Дөт зам",  
    desc:"Min km driven",
    tooltip:"Хамгийн бага зам"
  },
  {v:"balanced",  
    e:<BalanceIcon size="size-6" />,
    l:"Тэнцвэртэй",  
    desc:"Even truck loads",
    tooltip:"Тээврийн хэрэгсэл тэнцвэртэй хувиарлалт"
  },
  {v:"geographic",  
    e:<MapIcon size="size-6" />,
    l:"Газар зүйн",  
    desc:"Tight zone clusters",
    tooltip:"Бүс нутгаар хувиарлалт"
  },
];

export const MODE_COLOR:Record<string,string>={
  cheapest:"#F59E0B",fastest:"#5B7CFA",shortest:"#10B981",balanced:"#8B5CF6",geographic:"#0EA5E9",
  manual:"#7B82A0",
};

function DsCard({ds,active,onClick}:{ds:Dataset;active:boolean;onClick:()=>void;}){
  return(
    <div onClick={onClick} className={`rounded-xl border-[1.5px] p-2.5 cursor-pointer transition-all ${active?"border-red-500 bg-red-500/5 shadow-sm":"border-slate-200 bg-white hover:border-red-500/40 hover:bg-red-50"}`}>
      <div className="flex items-center justify-between mb-1">
        <span className="text-[12px] font-bold text-slate-900 truncate">{ds.name}</span>
        {active && <span className="text-[9px] font-bold px-1.5 py-0.5 rounded-full bg-green-500/10 text-green-500">ACTIVE</span>}
      </div>
      <div className="flex gap-2 text-[10px] text-slate-500">
        <span className="flex items-center gap-1">
          <StoreIcon size="size-4" />
          {ds.store_count}
        </span>
        <span className="flex items-center gap-1">
          <VehicleIcon size="size-4" />
          {ds.vehicle_count}
        </span>
        <span className={ds.has_matrix?"text-green-500 font-semibold":"text-amber-500 font-semibold"}>
          {ds.has_matrix
            ? <div className="flex items-center gap-1">
                <CheckIcon size="size-4" />
                matrix
              </div>
            :<div className="flex items-center gap-1">
                <WarningIcon size="size-4" />
                no matrix
              </div>
          }
        </span>
      </div>
    </div>
  );
}

async function loadBoth(id:string,d:(a:any)=>void){
  const[stores,vehicles]=await Promise.all([api.getStores(id),api.getVehicles(id)]);
  d({t:"SET_STORES",v:stores}); d({t:"SET_VEHICLES",v:vehicles});
}

export function RunPanel(){
  const{s,d}=useApp();
  const[mode,setMode]=useState("cheapest");
  const[trips,setTrips]=useState(2);
  const[time,setTime]=useState(60);
  const[weightFill,setWeightFill]=useState(0.7);
  const[volumeFill,setVolumeFill]=useState(0.8);
  const[targetGroup,setTargetGroup]=useState<string>("none");
  const[versionName,setVersionName]=useState("");
  const[solverStartedAt,setSolverStartedAt]=useState<number|null>(null);
  const[showConfigModal,setShowConfigModal]=useState(false);
  const[customConfig,setCustomConfig]=useState<Record<string,any>>({});
  const canRun = s.activeDatasetId != null && !s.running;

  // Clear timing when not running
  useEffect(()=>{
    if(!s.running){ setSolverStartedAt(null); }
  },[s.running]);

  useEffect(()=>{
    if(targetGroup!=="none"&&!s.runGroups.find((g:any)=>g.id===targetGroup)) setTargetGroup("none");
  },[s.runGroups]);

  async function run(){
    if(!s.activeDatasetId){ showToast("Бодох өгөгдөлийг сонгоно уу","error"); return; }
    const ds = s.datasets.find(d=>d.id===s.activeDatasetId);
    if(ds && !ds.has_matrix){ showToast("Өгөгдөлд матриц байхгүй байна","error"); return; }

    d({t:"SET_RUNNING",v:true});

    try{
      const gid = targetGroup === "none" ? undefined : targetGroup;

      // ── Step 1: POST /api/optimize → returns {job_id} immediately ──
      const {job_id} = await api.optimize({
        mode, max_trips:trips, solver_time:time,
        max_weight_fill:weightFill,
        max_volume_fill:volumeFill,
        dataset_id:s.activeDatasetId,
        group_id:gid,
        version_name:versionName.trim()||undefined,
        season:s.season,
        custom_config: Object.keys(customConfig).length > 0 ? customConfig : undefined,
      });

      // ── Step 2: Store job_id so SolverTerminal opens WebSocket ──
      d({t:"SET_ACTIVE_JOB", v:job_id});
      setSolverStartedAt(Date.now());

      // ── Step 3: Poll GET /api/jobs/{job_id} until done ──────────
      const result = await api.waitForJob(job_id, { intervalMs: 1500 });

      // ── Step 4: Display results ──────────────────────────────────
      d({t:"SET_RESULT", jobId:job_id, r:result});
      d({t:"SET_MAIN", v:"map"});
      await Promise.all([
        api.getJobs().then(v=>d({t:"SET_JOBS",v})),
        api.getRunGroups().then(v=>d({t:"SET_GROUPS",v})),
      ]);
      setVersionName("");
      showToast(`${result.summary.total_served} хүргэгдсэн, ${result.summary.total_unserved} үлдсэн`,"success");

    }catch(e:any){
      showToast(e.message??"Тооцоололд алдаа гарлаа","error");
      d({t:"SET_ACTIVE_JOB",v:null});
    }finally{
      d({t:"SET_RUNNING",v:false});
      setSolverStartedAt(null);
    }
  }

  return(
    <>
      <div className="flex-1 overflow-y-auto flex flex-col gap-0 min-h-0">

        {/* Live solver terminal — shown while running */}
        {s.running && (
          <div className="shrink-0 pt-3">
            <SolverTerminal
              running={s.running}
              solverTime={time}
              maxTrips={trips}   // ← add this
              startedAt={solverStartedAt}
              jobId={s.activeJobId}
            />
          </div>
        )} 

        <div className="flex-1 overflow-y-auto p-3 flex flex-col gap-4">
          {/* Dataset selection */}
          <div>
            <SectionLabel action={
              <FolderIcon />
            } label="Өгөгдөл"/>
            {!s.datasets.length
              ? <p className="text-[11px] text-slate-500 bg-slate-50 rounded-xl p-3 text-center">Өгөгдөл байхгүй байна</p>
              : <div className="flex flex-col gap-1.5 max-h-40 overflow-y-auto pr-0.5">
                  {s.datasets.map((ds:any)=>(
                    <DsCard key={ds.id} ds={ds} active={s.activeDatasetId===ds.id}
                      onClick={()=>{
                        const id=s.activeDatasetId===ds.id?null:ds.id;
                        d({t:"SET_DS",v:id});
                        if(id) loadBoth(id,d);
                      }}/>
                  ))}
                </div>
            }
            {s.activeDatasetId && !s.datasets.find(d=>d.id===s.activeDatasetId)?.has_matrix && (
              <div className="mt-2 p-2.5 bg-amber-50 border border-amber-200 rounded-xl text-[11px] text-amber-600 font-medium">
                <WarningIcon /> Матриц байхгүй байна.
              </div>
            )}
          </div>

          {/* Mode */}
          <div>
            <SectionLabel action={
              <TargetIcon />
            } label="Тооцоолох төрөл"/>
            <div className="grid grid-cols-3 gap-1.5 mb-1.5">
              {MODE_INFO.slice(0,3).map(m=>{
                const act=mode===m.v; const c=MODE_COLOR[m.v];
                return(
                <Tooltip content={m.tooltip} key={m.v}>
                  <button key={m.v} onClick={()=>setMode(m.v)}
                    className="w-full py-2 rounded-xl border-[1.5px] text-[11px] font-semibold justify-center items-center text-center transition-all flex flex-col"
                    style={{borderColor:act?c:"rgb(226 232 240)",background:act?c+"14":"#fff",color:act?c:"rgb(100 116 139)",boxShadow:act?`0 2px 8px ${c}30`:"none"}}>
                    <div className="text-[15px] flex items-center justify-center">{m.e}</div>
                    <div className="font-bold text-[10px]">{m.l}</div>
                    <div className="text-[9px] opacity-60">{m.desc}</div>
                  </button>
                </Tooltip>
                );
              })}
            </div>
            <div className="grid grid-cols-2 gap-1.5">
              {MODE_INFO.slice(3).map(m=>{
                const act=mode===m.v; const c=MODE_COLOR[m.v];
                return(
                <Tooltip content={m.tooltip} key={m.v}>
                  <button key={m.v} onClick={()=>setMode(m.v)}
                    className="w-full py-2 rounded-xl border-[1.5px] text-[11px] font-semibold justify-center items-center text-center transition-all flex flex-col"
                    style={{borderColor:act?c:"rgb(226 232 240)",background:act?c+"14":"#fff",color:act?c:"rgb(100 116 139)",boxShadow:act?`0 2px 8px ${c}30`:"none"}}>
                    <div className="text-[15px] flex items-center justify-center">{m.e}</div>
                    <div className="font-bold text-[10px]">{m.l}</div>
                    <div className="text-[9px] opacity-60">{m.desc}</div>
                  </button>
                </Tooltip>
                );
              })}
            </div>
          </div>

          {/* Parameters */}
          <div>
            <SectionLabel action={
              <SettingsIcon />
            } label="Тохиргоо" extra={
              <span 
                className="text-[10px] text-slate-500 cursor-pointer hover:text-red-500 hover:underline ml-auto mr-3"
                onClick={() => setShowConfigModal(true)}
              >Config</span>
            } />

            <div className="bg-white border border-slate-200 rounded-xl overflow-hidden divide-y divide-slate-100">

              {/* MAX TRIPS */}
              <div className="flex items-center justify-between px-3 py-3">
                <div>
                  <div className="text-[11px] font-semibold text-slate-700">Хүргэлтийн тоо</div>
                  <div className="text-[9px] text-slate-400">Цэнэглэж хүргэлт хийх боломж</div>
                </div>
                <div className="flex items-center gap-2">
                  <button onClick={() => setTrips(t => Math.max(1, t - 1))}
                    className="w-7 h-7 rounded-lg border border-slate-200 flex items-center justify-center hover:bg-slate-50 text-slate-600 font-bold">−</button>
                  <input type="number" value={trips} min={1} max={5}
                    onChange={e => setTrips(Math.max(1, Math.min(5, Number(e.target.value))))}
                    className="w-10 text-center text-[13px] font-mono font-bold border border-slate-200 rounded-lg h-7 outline-none focus:ring-2 focus:ring-red-400 [-moz-appearance:textfield] [&::-webkit-inner-spin-button]:m-0 [&::-webkit-inner-spin-button]:appearance-none [&::-webkit-outer-spin-button]:m-0 [&::-webkit-outer-spin-button]:appearance-none"/>
                  <button onClick={() => setTrips(t => Math.min(5, t + 1))}
                    className="w-7 h-7 rounded-lg border border-slate-200 flex items-center justify-center hover:bg-slate-50 text-slate-600 font-bold">+</button>
                </div>
              </div>

              {/* SOLVER TIME */}
              <div className="px-3 py-3">
                <div className="flex items-center justify-between mb-2">
                  <div>
                    <div className="text-[11px] font-semibold text-slate-700">Тооцоолох цаг</div>
                    <div className="text-[9px] text-slate-400">Хүргэлт + Агуулах</div>
                  </div>
                  <div className="relative">
                    <input type="number" value={time} min={1} max={9999} //! add disabled
                      onChange={e => setTime(Math.max(1, Math.min(9999, Number(e.target.value))))}
                      className="w-14 h-7 text-center text-[12px] font-mono font-bold border border-slate-200 rounded-lg outline-none focus:ring-2 focus:ring-red-400 pr-6 [-moz-appearance:textfield] [&::-webkit-inner-spin-button]:m-0 [&::-webkit-inner-spin-button]:appearance-none [&::-webkit-outer-spin-button]:m-0 [&::-webkit-outer-spin-button]:appearance-none"/>
                    <span className="absolute right-2 top-1/2 -translate-y-1/2 text-[11px] text-slate-500 font-medium pointer-events-none">s</span>
                  </div>
                </div>
                <div className="flex gap-1.5">
                  {[60, 300].map(v => (
                    <button key={v} onClick={() => setTime(v)}
                      className={`flex-1 py-1.5 rounded-lg text-[10px] font-bold border transition-all ${time===v?"border-red-500 bg-red-50 text-red-600":"border-slate-200 text-slate-400 hover:bg-slate-50"}`}>
                      {v === 60 ? 'Хурдан' : 'Нарийвчилсан'}
                    </button>
                  ))}
                </div>
              </div>

              {/* WEIGHT */}
              <div className="px-3 py-3">
                <div className="border border-slate-200 rounded-lg p-2.5">
                  <div className="flex items-center justify-between mb-1.5">
                    <div>
                      <div className="text-[11px] font-semibold text-slate-700">Массын хувь</div>
                      <div className="text-[9px] text-slate-400">Тээврийн хэрэгслийн массын хувь</div>
                    </div>
                    <div className="relative">
                      <input type="number" min="0" max="150" value={Math.round(weightFill * 100)}
                        onChange={e => setWeightFill(Math.max(0, Math.min(150, Number(e.target.value))) / 100)}
                        className="w-16 h-7 text-center text-[12px] font-mono font-bold border border-slate-200 rounded-lg outline-none focus:ring-2 focus:ring-red-400 pr-6 [-moz-appearance:textfield] [&::-webkit-inner-spin-button]:m-0 [&::-webkit-inner-spin-button]:appearance-none [&::-webkit-outer-spin-button]:m-0 [&::-webkit-outer-spin-button]:appearance-none"/>
                      <span className="absolute right-2 top-1/2 -translate-y-1/2 text-[11px] text-slate-500 font-medium pointer-events-none">%</span>
                    </div>
                  </div>
                  <ProgressBar pct={weightFill*100} color={weightFill>0.9?"#EF4444":weightFill>0.75?"#F59E0B":"#10B981"} height={5} animated={false}/>
                </div>
              </div>

              {/* VOLUME */}
              <div className="px-3 py-3">
                <div className="border border-slate-200 rounded-lg p-2.5">
                  <div className="flex items-center justify-between mb-1.5">
                    <div>
                      <div className="text-[11px] font-semibold text-slate-700">Эзэлхүүний хувь</div>
                      <div className="text-[9px] text-slate-400">Тээврийн хэрэгслийн эзэлхүүний хувь</div>
                    </div>
                    <div className="relative">
                      <input type="number" min="0" max="100" value={Math.round(volumeFill * 100)}
                        onChange={e => setVolumeFill(Math.max(0, Math.min(100, Number(e.target.value))) / 100)}
                        className="w-16 h-7 text-center text-[12px] font-mono font-bold border border-slate-200 rounded-lg outline-none focus:ring-2 focus:ring-red-400 pr-6 [-moz-appearance:textfield] [&::-webkit-inner-spin-button]:m-0 [&::-webkit-inner-spin-button]:appearance-none [&::-webkit-outer-spin-button]:m-0 [&::-webkit-outer-spin-button]:appearance-none"/>
                      <span className="absolute right-2 top-1/2 -translate-y-1/2 text-[11px] text-slate-500 font-medium pointer-events-none">%</span>
                    </div>
                  </div>
                  <ProgressBar pct={volumeFill*100} color={volumeFill>0.9?"#EF4444":volumeFill>0.75?"#F59E0B":"#10B981"} height={5} animated={false}/>
                </div>
              </div>

              {/* SEASON */}
              <div className="px-3 py-3">
                <div className="flex items-center justify-between">
                  <div>
                    <div className="text-[11px] font-semibold text-slate-700">Улирал</div>
                    <div className="text-[9px] text-slate-400">Эрэлт хангах улирал</div>
                  </div>
                  <select value={s.season} onChange={e => d({t:"SET_SEASON", v:e.target.value as any})}
                    className="rounded-lg border border-slate-200 bg-white px-3 py-2 text-[11px] font-semibold outline-none focus:ring-2 focus:ring-red-400">
                    <option value="summer">Зун (Summer)</option>
                    <option value="autumn">Намар (Autumn)</option>
                    <option value="winter">Өвөл (Winter)</option>
                    <option value="spring">Хавар (Spring)</option>
                  </select>
                </div>
              </div>

            </div>
          </div>

          {/* Version group */}
          <div>
            <SectionLabel action={
              <TagIcon />
            } label="Group"/>
            <div className="flex flex-col gap-2">
              <select value={targetGroup} onChange={e=>setTargetGroup(e.target.value)}
                className="w-full rounded-xl border border-slate-200 bg-white px-3 py-2 text-[12px] outline-none focus:border-red-500">
                <option value="none">— Standalone —</option>
                {s.runGroups.map((g:any)=><option key={g.id} value={g.id}>{g.name}</option>)}
              </select>
              {targetGroup!=="none"&&(
                <input value={versionName} onChange={e=>setVersionName(e.target.value)}
                  placeholder="Version label (auto if blank)"
                  className="w-full rounded-xl border border-slate-200 bg-white px-3 py-2 text-[12px] outline-none focus:border-red-500"/>
              )}
              <button className="text-[11px] text-red-500 font-semibold text-left hover:underline"
                onClick={async()=>{
                  const name=prompt("Group name:");if(!name?.trim())return;
                  const g=await api.createRunGroup(name.trim(),s.activeDatasetId??undefined);
                  const groups=await api.getRunGroups();
                  d({t:"SET_GROUPS",v:groups});setTargetGroup(g.id);
                }}>+ Create new group</button>
            </div>
          </div>
        </div>
      </div>

      {/* Run button */}
      <div className="shrink-0 p-3 border-t border-slate-200 bg-white">
        {s.running ? (
          <div className="w-full flex flex-col items-center gap-2 py-2">
            <div className="flex items-center gap-2">
              <span className="w-4 h-4 border-2 border-red-500 border-t-transparent rounded-full animate-spin"/>
              <span className="text-[13px] font-bold text-red-500">Замыг тооцоолж байна...</span>
            </div>
            <p className="text-[10px] text-slate-400 text-center">
              OR-Tools ашиглан замыг тооцоолж байна...
            </p>
          </div>
        ) : (
          <Btn variant="primary" size="lg" className="w-full" disabled={!canRun} onClick={run}>
            ▶ Тооцоолох
          </Btn>
        )}
        {!s.activeDatasetId&&!s.running&&(
          <p className="text-center text-[10px] text-slate-400 mt-2">Дата өгөгдлөөс сонгож тооцоолно уу</p>
        )}
      </div>

      {/* Config Modal */}
      {showConfigModal && (
        <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-9999 p-4">
          <div className="bg-white rounded-2xl w-full max-w-2xl max-h-[80vh] overflow-hidden flex flex-col">
            <div className="flex items-center justify-between px-4 py-3 border-b border-slate-200">
              <h2 className="text-[14px] font-bold text-slate-900">Config Settings (Temporary)</h2>
              <button 
                onClick={() => setShowConfigModal(false)}
                className="text-slate-400 hover:text-slate-600 text-[20px] font-bold"
              >×</button>
            </div>
            <div className="flex-1 overflow-y-auto p-4 space-y-3">
              <p className="text-[11px] text-slate-500 mb-2">Changes apply to current run only (reset after reload)</p>
              
              {[
                { key: "SERVICE_TIME_SECONDS", label: "Бараа өгөх хугацаа (seconds)", type: "number", default: 600 },
                { key: "RELOAD_TIME_SECONDS", label: "Бараа дүүргэх хугацаа (seconds)", type: "number", default: 1800 },
                { key: "PENALTY_UNSERVED", label: "Хүргэлт хийхгүй байх шийтгэл", type: "number", default: 10000000000 },
                { key: "VEHICLE_FIXED_COST", label: "Тээврийн хэрэгсэл дахин ашиглах өртөг", type: "number", default: 5000 },
                { key: "BALANCED_SPAN_COEFF", label: "Balanced Span Coeff", type: "number", default: 300 },
                { key: "MAX_TRIPS_PER_VEHICLE", label: "Тээврийн хэрэгсэл цэнэглэх тоо", type: "number", default: 2 },
                { key: "FAR_THRESHOLD_KM", label: "Хол түгээлтийн хязгаар (km)", type: "number", default: 1000 },
                { key: "URBAN_MAX_CAP_M3", label: "Хот доторх эзэлхүүний хязгаарлалт m³", type: "number", default: 15 },
                { key: "URBAN_MAX_CAP_KG", label: "Хот доторх массын хязгаарлалт kg", type: "number", default: 3000 },
                { key: "CONTRACTOR_COST_MULT", label: "Гадны түгээлтийн өртгийн коэффициент", type: "number", default: 4.0 },
                { key: "FLEET_COST_MULT", label: "Fleet түгээлтийн өртгийн коэффициент", type: "number", default: 0.7 },
                { key: "DRY_START_HOUR", label: "DRY эхлэх цаг", type: "number", default: 13 },
                { key: "DRY_MAX_HORIZON_HOUR", label: "DRY хамгийн их зарцуулах хугацаа (цаг)", type: "number", default: 24 },
                { key: "COLD_START_HOUR", label: "COLD эхлэх цаг", type: "number", default: 3 },
                { key: "COLD_MAX_HORIZON_HOUR", label: "COLD хамгийн их зарцуулах хугацаа (цаг)", type: "number", default: 14 },
              ].map((cfg) => (
                <div key={cfg.key} className="flex items-center justify-between py-2 border-b border-slate-100">
                  <div className="flex-1">
                    <div className="text-[12px] font-semibold text-slate-700">{cfg.label}</div>
                    <div className="text-[10px] text-slate-400">{cfg.key}</div>
                  </div>
                  <input
                    type={cfg.type}
                    value={customConfig[cfg.key] ?? cfg.default}
                    onChange={(e) => setCustomConfig(prev => ({
                      ...prev,
                      [cfg.key]: cfg.type === "number" ? Number(e.target.value) : e.target.value
                    }))}
                    className="w-24 text-right text-[12px] font-mono border border-slate-200 rounded-lg px-2 py-1.5 outline-none focus:ring-2 focus:ring-red-400"
                  />
                </div>
              ))}
            </div>
            <div className="flex gap-2 p-4 border-t border-slate-200">
              <button
                onClick={() => {
                  setCustomConfig({});
                  setShowConfigModal(false);
                }}
                className="flex-1 py-2 rounded-xl border border-slate-200 text-[12px] font-semibold text-slate-600 hover:bg-slate-50"
              >
                Reset
              </button>
              <button
                onClick={() => setShowConfigModal(false)}
                className="flex-1 py-2 rounded-xl bg-red-500 text-white text-[12px] font-semibold hover:bg-red-600"
              >
                Apply
              </button>
            </div>
          </div>
        </div>
      )}
    </>
  );
}