"use client";
import { useApp } from "@/lib/state";
import { RunPanel } from "./RunPanel";
import { DataPanel } from "./DataPanel";
import { HistoryPanel } from "./HistoryPanel";

export function Sidebar(){
  const{s,d}=useApp();
  const tabs=[
    {key:"run" as const,   icon:"🚀",label:"Тооцоолол"},
    {key:"data" as const,  icon:"📂",label:"Өгөгдөл"},
    {key:"history" as const,icon:"🕑",label:"Түүх",
      badge:s.jobs.filter(j=>j.status==="done").length},
  ];
  return(
    <div className="h-full flex flex-col overflow-hidden">
      <div className="flex shrink-0 border-b border-gray-200 bg-white">
        {tabs.map(t=>(
          <button key={t.key} onClick={()=>d({t:"SET_SIDE",v:t.key})}
            className={`flex-1 flex items-center justify-center gap-1 py-2.5 text-xs font-semibold border-b-2 transition-all ${s.sideTab===t.key?"border-blue-500 text-blue-500":"border-transparent text-gray-500 hover:text-gray-900"}`}>
            {t.icon} {t.label}
            {!!t.badge&&<span className="text-xxs font-extrabold px-1.5 py-0.5 rounded-full bg-blue-500/12 text-blue-500">{t.badge}</span>}
          </button>
        ))}
      </div>
      <div className={`flex-1 min-h-0 overflow-hidden ${s.sideTab==="run"?"flex":"hidden"} flex-col`}><RunPanel/></div>
      <div className={`flex-1 min-h-0 overflow-hidden ${s.sideTab==="data"?"flex":"hidden"} flex-col`}><DataPanel/></div>
      <div className={`flex-1 min-h-0 overflow-hidden ${s.sideTab==="history"?"flex":"hidden"} flex-col`}><HistoryPanel/></div>
    </div>
  );
}
