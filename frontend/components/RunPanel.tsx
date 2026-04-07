"use client";
import { useState, useEffect, useRef } from "react";
import { useApp } from "@/lib/state";
import * as api from "@/lib/api";
import { Btn, SectionLabel, showToast, ProgressBar, SolverTerminal } from "./ui";
import { StoreIcon, VehicleIcon, CheckIcon, WarningIcon, LightningIcon, RulerIcon, BalanceIcon, MapIcon, MoneyIcon, FolderIcon, TargetIcon, SettingsIcon, TagIcon } from "./icons";
import type { Dataset } from "@/types/vrp";

const MODE_INFO=[
  { v:"cheapest",  
    e:<MoneyIcon size="size-6" />,
    l:"Хямд",  
    desc:"Min fuel ₮/km"
  },
  {v:"fastest",   
    e:<LightningIcon size="size-6" />,
    l:"Хурдан",   
    desc:"Min travel time"
  },
  {v:"shortest",  
    e:<RulerIcon size="size-6" />,
    l:"Дөт зам",  
    desc:"Min km driven"
  },
  {v:"balanced",  
    e:<BalanceIcon size="size-6" />,
    l:"Тэнцвэртэй",  
    desc:"Even truck loads"
  },
  {v:"geographic",  
    e:<MapIcon size="size-6" />,
    l:"Газар зүйн",  
    desc:"Tight zone clusters"
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
                return(<button key={m.v} onClick={()=>setMode(m.v)}
                  className="py-2 rounded-xl border-[1.5px] text-[11px] font-semibold justify-center items-center text-center transition-all flex flex-col"
                  style={{borderColor:act?c:"rgb(226 232 240)",background:act?c+"14":"#fff",color:act?c:"rgb(100 116 139)",boxShadow:act?`0 2px 8px ${c}30`:"none"}}>
                  <div className="text-[15px] flex items-center justify-center">{m.e}</div>
                  <div className="font-bold text-[10px]">{m.l}</div>
                  <div className="text-[9px] opacity-60">{m.desc}</div>
                </button>);
              })}
            </div>
            <div className="grid grid-cols-2 gap-1.5">
              {MODE_INFO.slice(3).map(m=>{
                const act=mode===m.v; const c=MODE_COLOR[m.v];
                return(<button key={m.v} onClick={()=>setMode(m.v)}
                  className="py-2 rounded-xl border-[1.5px] text-[11px] font-semibold justify-center items-center text-center transition-all flex flex-col"
                  style={{borderColor:act?c:"rgb(226 232 240)",background:act?c+"14":"#fff",color:act?c:"rgb(100 116 139)",boxShadow:act?`0 2px 8px ${c}30`:"none"}}>
                  <div className="text-[15px] flex items-center justify-center">{m.e}</div>
                  <div className="font-bold text-[10px]">{m.l}</div>
                  <div className="text-[9px] opacity-60">{m.desc}</div>
                </button>);
              })}
            </div>
          </div>

          {/* Parameters */}
          <div>
            <SectionLabel action={
              <SettingsIcon />
            } label="Тохиргоо" />

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
                    <input type="number" value={time} min={1} max={600} //! add disabled
                      onChange={e => setTime(Math.max(1, Math.min(600, Number(e.target.value))))}
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
              <span className="w-4 h-4 border-2 border-blue-500 border-t-transparent rounded-full animate-spin"/>
              <span className="text-[13px] font-bold text-blue-500">Замыг тооцоолж байна...</span>
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
    </>
  );
}