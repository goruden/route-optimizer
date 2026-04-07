"use client";
import { useState } from "react";
import { useApp } from "@/lib/state";
import * as api from "@/lib/api";
import { Btn, Modal, Confirm, showToast } from "./ui";
import { AddPanel } from "./AddPanel";
import { FolderIcon, TagIcon, EditIcon, DeleteIcon, RefreshIcon, CheckIcon, WarningIcon, VehicleIcon, PackageIcon, ClockIcon, StoreIcon, LightningIcon, RulerIcon, BalanceIcon, MapIcon, MoneyIcon } from "./icons";
import dayjs from "dayjs";
import { MODE_COLOR } from "./RunPanel";

function Empty({icon,msg}:{icon:React.ReactNode;msg:string;}){return<div className="flex flex-col items-center justify-center py-8 text-center"><span className="text-3xl mb-2">{icon}</span><p className="text-[11px] text-slate-500">{msg}</p></div>;}

function GroupCard({group,activeJobId,onClickJob,onForkJob,onRemoveJob,onRename,onDelete}:{group:any;activeJobId:string|null;onClickJob:(id:string)=>void;onForkJob:(id:string,e:React.MouseEvent)=>void;onRemoveJob:(id:string,e:React.MouseEvent)=>void;onRename:()=>void;onDelete:(e:React.MouseEvent)=>void;}){
  const[open,setOpen]=useState(true);
  return(
    <div className="bg-white border border-slate-200 rounded-xl">
      <div className="flex items-center gap-2 px-3 py-2 bg-slate-50 border-b border-slate-200">
        <button onClick={()=>setOpen(o=>!o)} className="text-[10px] text-slate-400 w-4">{open?"▼":"▶"}</button>
        <span className="flex-1 font-semibold text-[12px] text-red-500 truncate"><TagIcon size="size-4" className="inline mr-1"/>{group.name}</span>
        <span className="text-[10px] text-slate-400">{group.jobs.length}v</span>
        <button onClick={onRename} className="text-[11px] text-slate-400 hover:text-blue-500 w-5 h-5 flex items-center justify-center"><EditIcon size="size-4" /></button>
        <Confirm onConfirm={()=>onDelete({} as React.MouseEvent)}><button className="text-[11px] text-slate-300 hover:text-red-500 w-5 h-5 flex items-center justify-center"><DeleteIcon size="size-4" /></button></Confirm>
      </div>
      {open&&<div className="p-1.5 flex flex-col gap-1">{!group.jobs.length&&<p className="text-[11px] text-slate-400 px-2 py-1">Ямар нэгэн хувилбар байхгүй байна</p>}{group.jobs.map((j:any)=><JobCard key={j.id} job={j} active={activeJobId===j.id} onClickJob={onClickJob} onFork={onForkJob} onRemove={onRemoveJob}/>)}</div>}
    </div>
  );
}

function JobCard({job:j,active,onClickJob,onFork,onRemove}:{job:any;active:boolean;onClickJob:(id:string)=>void;onFork:(id:string,e:React.MouseEvent)=>void;onRemove:(id:string,e:React.MouseEvent)=>void;}){
  const mc=MODE_COLOR[j.mode]??"#7B82A0";
  const me:Record<string,React.ReactNode>={
    cheapest:<><MoneyIcon size="size-3" className="inline mr-1" />Хямд</>,
    fastest:<><LightningIcon size="size-3" className="inline mr-1" />Хурдан</>,
    shortest:<><RulerIcon size="size-3" className="inline mr-1" />Дөт</>,
    balanced:<><BalanceIcon size="size-3" className="inline mr-1" />Тэнцвэртэй</>,
    geographic:<><MapIcon size="size-3" className="inline mr-1" />Газар зүйн</>
  };
  return(
    <div onClick={()=>j.status==="done"&&onClickJob(j.id)}
      className={`rounded-xl border-[1.5px] p-2.5 relative transition-all ${active?"border-blue-500 bg-blue-500/4":"border-slate-200 bg-white"} ${j.status==="done"?"cursor-pointer hover:border-blue-500/50":"opacity-60"}`}>
      {active&&<div className="absolute left-0 top-2 bottom-2 w-0.75 bg-blue-500 rounded-r-sm"/>}
      <div className="flex items-center justify-between mb-1">
        <div className="flex items-center gap-1.5 min-w-0">
          <span className="w-1.5 h-1.5 rounded-full shrink-0" style={{background:j.status==="done"?"#10B981":j.status==="error"?"#EF4444":"#5B7CFA"}}/>
          <span className="font-semibold text-[11px] text-slate-700 truncate">{j.version_name||`#${j.id.slice(0,8)}`}</span>
          {j.is_manual&&<span className="text-[9px] px-1.5 py-0.5 rounded-full bg-violet-500/12 text-violet-500 font-bold shrink-0">Manual</span>}
        </div>
        <div className="flex items-center gap-1 shrink-0">
          <span className="text-[9px] font-bold px-1.5 py-0.5 rounded-full" style={{background:mc+"18",color:mc}}>{me[j.mode]??""}</span>
          {j.status==="done"&&<button onClick={e=>onFork(j.id,e)} title="Fork as manual version" className="text-[10px] text-slate-300 hover:text-violet-500 w-5 h-5 flex items-center justify-center">⑂</button>}
          <button onClick={e=>onRemove(j.id,e)} className="text-[10px] text-slate-300 hover:text-red-500 w-5 h-5 flex items-center justify-center">✕</button>
        </div>
      </div>
      {j.status==="done"&&<div className="flex flex-wrap gap-1 mb-1">
        {j.total_served!=null&&<span className="text-[9px] font-bold px-1.5 py-0.5 rounded-full bg-green-500/10 text-green-500"><CheckIcon size="size-3" className="inline mr-1" />{j.total_served}</span>}
        {!!j.total_unserved&&<span className="text-[9px] font-bold px-1.5 py-0.5 rounded-full bg-red-500/10 text-red-500"><WarningIcon size="size-3" className="inline mr-1" />{j.total_unserved}</span>}
        {j.total_routes!=null&&<span className="text-[9px] font-bold px-1.5 py-0.5 rounded-full bg-blue-500/10 text-blue-500"><VehicleIcon size="size-3" className="inline mr-1" />{j.total_routes}</span>}
        {j.total_cost!=null&&<span className="text-[9px] font-bold px-1.5 py-0.5 rounded-full bg-amber-500/10 text-amber-500">₮{Math.round(j.total_cost).toLocaleString()}</span>}
      </div>}
      <div className="text-[9px] text-slate-400">{dayjs(j.created_at).add(8, 'hour').format("MMM DD HH:mm")}</div>
      {j.status==="done"&&<div className={`text-[9px] font-bold mt-1 ${active?"text-green-500":"text-blue-500"}`}>{active?"✓ Газрын зурагт харуулж байна":"Дарж харах →"}</div>}
    </div>
  );
}

export function HistoryPanel(){
  const{s,d}=useApp();
  const[loading,setLoading]=useState(false);
  const[view,setView]=useState<"all"|"groups">("all");
  const[renameId,setRenameId]=useState<string|null>(null);
  const[renameName,setRenameName]=useState("");
  const[showAddPanel,setShowAddPanel]=useState(false);

  async function refresh(){setLoading(true);try{const[jobs,groups]=await Promise.all([api.getJobs(),api.getRunGroups()]);d({t:"SET_JOBS",v:jobs});d({t:"SET_GROUPS",v:groups});}finally{setLoading(false);}}
  async function clickJob(id:string){if(s.activeJobId===id){d({t:"CLEAR"});return;}setLoading(true);try{const r=await api.getJobResult(id);d({t:"SET_RESULT",jobId:id,r});d({t:"SET_MAIN",v:"map"});showToast("Result loaded!","success");}catch(e:any){showToast(e.message,"error");}finally{setLoading(false);}}
  async function removeJob(id:string,e:React.MouseEvent){e.stopPropagation();if(s.activeJobId===id)d({t:"CLEAR"});await api.deleteJob(id);const[jobs,groups]=await Promise.all([api.getJobs(),api.getRunGroups()]);d({t:"SET_JOBS",v:jobs});d({t:"SET_GROUPS",v:groups});}
  async function forkJob(id:string,e:React.MouseEvent){e.stopPropagation();try{const f=await api.forkJob(id);const[jobs,groups]=await Promise.all([api.getJobs(),api.getRunGroups()]);d({t:"SET_JOBS",v:jobs});d({t:"SET_GROUPS",v:groups});showToast(`Forked as "${f.version_name}"!`,"success");}catch(e:any){showToast(e.message,"error");}}
  async function doRename(){if(!renameId||!renameName.trim())return;await api.renameRunGroup(renameId,renameName.trim());const groups=await api.getRunGroups();d({t:"SET_GROUPS",v:groups});setRenameId(null);setRenameName("");}
  async function deleteGroup(id:string,e:React.MouseEvent){e.stopPropagation();await api.deleteRunGroup(id);const[jobs,groups]=await Promise.all([api.getJobs(),api.getRunGroups()]);d({t:"SET_JOBS",v:jobs});d({t:"SET_GROUPS",v:groups});}

  const standaloneJobs=s.jobs.filter((j:any)=>!j.group_id);

  return(
    <>
      <div className="shrink-0 flex items-center gap-2 px-3 py-2.5 bg-white border-b border-slate-200">
        <div className="flex gap-1 flex-1">
          {(["all","groups"] as const).map(v=>(<button key={v} onClick={()=>setView(v)} className={`px-3 py-1 rounded-lg text-[11px] font-semibold border-[1.5px] transition-all ${view===v?"border-blue-500 bg-blue-500/8 text-blue-500":"border-slate-200 text-slate-500"}`}>{v==="all"?"Бүгд":"Бүлгээр"}</button>))}
        </div>
        <Btn size="sm" variant="primary" onClick={()=>setShowAddPanel(true)}>+ Нэмэх</Btn>
        <Btn size="sm" variant="ghost" loading={loading} onClick={refresh}><RefreshIcon size="size-4" /></Btn>
      </div>
      <div className="flex-1 overflow-y-auto p-2.5 flex flex-col gap-2 min-h-0">
        {view==="groups"?(
          <>
            {!s.runGroups.length&&<Empty icon={<TagIcon size="size-6" />} msg="Ямар нэгэн бүлэг алга байна"/>}
            {s.runGroups.map((g:any)=>(
              <GroupCard key={g.id} group={g} activeJobId={s.activeJobId} onClickJob={clickJob} onForkJob={forkJob} onRemoveJob={removeJob}
                onRename={()=>{setRenameId(g.id);setRenameName(g.name);}} onDelete={e=>deleteGroup(g.id,e)}/>
            ))}
          </>
        ):(
          <>
            {s.runGroups.map((g:any)=>(
              <div key={g.id} className="flex flex-col gap-1">
                <div className="flex items-center gap-2 mt-1"><span className="text-[10px] font-bold text-red-500 uppercase tracking-widest flex-1 truncate"><TagIcon size="size-4" className="inline mr-1" />{g.name}</span><span className="text-[9px] text-slate-400">{g.jobs.length}v</span></div>
                {g.jobs.map((j:any)=><JobCard key={j.id} job={j} active={s.activeJobId===j.id} onClickJob={clickJob} onFork={forkJob} onRemove={removeJob}/>)}
              </div>
            ))}
            {standaloneJobs.length>0&&(
              <div className="flex flex-col gap-1">
                {s.runGroups.length>0&&<div className="text-[10px] font-bold text-red-500 uppercase tracking-widest mt-1">Standalone</div>}
                {standaloneJobs.map((j:any)=><JobCard key={j.id} job={j} active={s.activeJobId===j.id} onClickJob={clickJob} onFork={forkJob} onRemove={removeJob}/>)}
              </div>
            )}
            {!s.jobs.length&&<Empty icon={<ClockIcon size="size-6" />} msg="Ямар нэгэн тооцоолол алга байна"/>}
          </>
        )}
      </div>
      <Modal title="Нэр солих" open={!!renameId} onClose={()=>setRenameId(null)} onOk={doRename} okLabel="Хадгалах"><input value={renameName} onChange={e=>setRenameName(e.target.value)} placeholder="Бүлгийн нэр" className="w-full rounded-xl border border-slate-200 bg-white px-3 py-2 text-[12px] outline-none focus:border-blue-500"/></Modal>
      <AddPanel open={showAddPanel} onClose={()=>setShowAddPanel(false)} />
    </>
  );
}
