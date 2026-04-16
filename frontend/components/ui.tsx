"use client";
import { useState, useRef, useEffect, type ReactNode } from "react";
import { WS_BASE } from "@/lib/api";

/* ── Button ──────────────────────────────────────────── */
type BtnVariant="primary"|"secondary"|"ghost"|"danger"|"success";
interface BtnProps extends React.ButtonHTMLAttributes<HTMLButtonElement>{
  variant?:BtnVariant; loading?:boolean; size?:"sm"|"md"|"lg";
}
export function Btn({variant="secondary",loading,size="md",className="",children,...rest}:BtnProps){
  const base="inline-flex items-center justify-center gap-2 font-semibold rounded-xl border transition-all duration-150 cursor-pointer select-none disabled:opacity-40 disabled:cursor-not-allowed";
  const sz={sm:"px-3 py-1.5 text-[11px]",md:"px-4 py-2 text-[12px]",lg:"px-5 py-3 text-[14px] font-bold"}[size];
  const v={
    primary:"bg-red-500 border-red-500 text-white hover:bg-red-600 shadow-[0_4px_14px_rgba(59,130,246,0.3)]",
    secondary:"bg-white border-slate-200 text-slate-900 hover:border-red-500 hover:text-red-500",
    ghost:"bg-transparent border-transparent text-slate-500 hover:text-slate-900 hover:bg-slate-50",
    danger:"bg-white border-red-300 text-red-500 hover:bg-red-50",
    success:"bg-emerald-500 border-emerald-500 text-white hover:bg-emerald-600 shadow-[0_4px_14px_rgba(16,185,129,0.3)]",
  }[variant];
  return(
    <button className={`${base} ${sz} ${v} ${className}`} disabled={loading||rest.disabled} {...rest}>
      {loading&&<span className="w-3.5 h-3.5 border-2 border-current border-t-transparent rounded-full animate-spin inline-block shrink-0"/>}
      {children}
    </button>
  );
}

/* ── Input ───────────────────────────────────────────── */
interface InputProps extends React.InputHTMLAttributes<HTMLInputElement>{label?:string;}
export function Input({label,className="",id,...rest}:InputProps){
  return(
    <div className="flex flex-col gap-1">
      {label&&<label htmlFor={id} className="text-[10px] font-bold text-slate-500 uppercase tracking-wider">{label}</label>}
      <input id={id} className={`w-full rounded-xl border border-slate-200 bg-white px-3 py-2 text-[12px] text-slate-900 outline-none focus:border-red-500 focus:ring-2 focus:ring-red-500/12 placeholder:text-slate-300 transition-all ${className}`} {...rest}/>
    </div>
  );
}

/* ── Select ──────────────────────────────────────────── */
interface SelProps extends React.SelectHTMLAttributes<HTMLSelectElement>{label?:string;options:{v:string;l:string}[];}
export function Sel({label,options,className="",id,...rest}:SelProps){
  return(
    <div className="flex flex-col gap-1">
      {label&&<label htmlFor={id} className="text-[10px] font-bold text-slate-500 uppercase tracking-wider">{label}</label>}
      <select id={id} className={`w-full rounded-xl border border-slate-200 bg-white px-3 py-2 text-[12px] text-slate-900 outline-none focus:border-red-500 transition-all ${className}`} {...rest}>
        {options.map(o=><option key={o.v} value={o.v}>{o.l}</option>)}
      </select>
    </div>
  );
}

/* ── NumberInput ─────────────────────────────────────── */
interface NumProps{label?:string;value:number;min?:number;max?:number;step?:number;onChange:(v:number)=>void;className?:string;}
export function NumInput({label,value,min,max,step=1,onChange,className=""}:NumProps){
  return(
    <div className="flex flex-col gap-1">
      {label&&<span className="text-[10px] font-bold text-slate-500 uppercase tracking-wider">{label}</span>}
      <input type="number" value={value} min={min} max={max} step={step}
        className={`rounded-xl border border-slate-200 bg-white px-3 py-2 text-[12px] font-mono text-slate-900 outline-none focus:border-red-500 focus:ring-2 focus:ring-red-500/12 transition-all ${className}`}
        onChange={e=>onChange(Number(e.target.value))}/>
    </div>
  );
}

/* ── Toggle ──────────────────────────────────────────── */
export function Toggle({checked,onChange,label}:{checked:boolean;onChange:(v:boolean)=>void;label?:string;}){
  return(
    <label className="flex items-center gap-2 cursor-pointer select-none">
      <button type="button" role="switch" aria-checked={checked} onClick={()=>onChange(!checked)}
        className={`relative w-9 h-5 rounded-full transition-colors duration-200 ${checked?"bg-blue-500":"bg-slate-200"}`}>
        <span className={`absolute top-0.5 left-0.5 w-4 h-4 rounded-full bg-white shadow transition-transform duration-200 ${checked?"translate-x-4":""}`}/>
      </button>
      {label&&<span className="text-[11px] text-slate-500">{label}</span>}
    </label>
  );
}

/* ── Modal ───────────────────────────────────────────── */
export function Modal({open,onClose,title,children,onOk,okLabel="Хадгалах",loading}:{
  open:boolean;onClose:()=>void;title:string;children:ReactNode;onOk?:()=>void;okLabel?:string;loading?:boolean;
}){
  if(!open) return null;
  return(
    <div className="fixed inset-0 z-9000 flex items-center justify-center p-4">
      <div className="absolute inset-0 bg-slate-900/20 backdrop-blur-sm" onClick={onClose}/>
      <div className="relative bg-white rounded-2xl shadow-2xl w-full max-w-lg max-h-[85vh] flex flex-col">
        <div className="flex items-center justify-between px-5 py-4 border-b border-slate-200 bg-slate-50 rounded-t-2xl shrink-0">
          <h3 className="font-bold text-[14px] text-slate-900">{title}</h3>
          <button onClick={onClose} className="w-7 h-7 rounded-lg border border-slate-200 flex items-center justify-center text-slate-500 hover:bg-slate-50 text-[12px]">✕</button>
        </div>
        <div className="overflow-y-auto flex-1 p-5">{children}</div>
        {onOk&&(
          <div className="flex justify-end gap-2 px-5 py-3 border-t border-slate-200 shrink-0">
            <Btn variant="ghost" size="sm" onClick={onClose}>Цуцлах</Btn>
            <Btn variant="primary" size="sm" onClick={onOk} loading={loading}>{okLabel}</Btn>
          </div>
        )}
      </div>
    </div>
  );
}

/* ── Confirm ─────────────────────────────────────────── */
export function Confirm({onConfirm,children,message="Итгэлтэй байна уу?",cancelText="Үгүй",confirmText="Тийм"}:{onConfirm:()=>void;children:ReactNode;message?:string;cancelText?:string;confirmText?:string;}){
  const[open,setOpen]=useState(false);
  return(
    <>
      <div onClick={e=>{e.stopPropagation();setOpen(true)}}>{children}</div>
      {open&&(
        <div className="fixed inset-0 z-9999 flex items-center justify-center p-4">
          <div className="absolute inset-0 bg-slate-900/20" onClick={()=>setOpen(false)}/>
          <div className="relative bg-white rounded-2xl shadow-xl p-5 w-64">
            <p className="text-[13px] font-semibold text-slate-900 mb-4">{message}</p>
            <div className="flex gap-2 justify-end">
              <Btn size="sm" variant="ghost" onClick={()=>setOpen(false)}>{cancelText}</Btn>
              <Btn size="sm" variant="danger" onClick={()=>{onConfirm();setOpen(false);}}>{confirmText}</Btn>
            </div>
          </div>
        </div>
      )}
    </>
  );
}

/* ── Toast ───────────────────────────────────────────── */
type ToastType="success"|"error"|"info"|"loading";
const toasts:{id:number;msg:string;type:ToastType}[]=[];
let toastSubs:Array<()=>void>=[];
let nextId=0;
export function showToast(msg:string,type:ToastType="success"){
  const id=++nextId;
  toasts.push({id,msg,type});
  toastSubs.forEach(fn=>fn());
  if(type!=="loading"){
    setTimeout(()=>{const i=toasts.findIndex(t=>t.id===id);if(i>-1)toasts.splice(i,1);toastSubs.forEach(fn=>fn());},3800);
  }
  return id;
}
export function dismissToast(id:number){
  const i=toasts.findIndex(t=>t.id===id);
  if(i>-1){toasts.splice(i,1);toastSubs.forEach(fn=>fn());}
}
export function Toaster(){
  const[,rerender]=useState(0);
  toastSubs=[()=>rerender(n=>n+1)];
  const styles:{[k in ToastType]:{bg:string;icon:string}}={
    success:{bg:"bg-emerald-500",icon:"✓"},
    error:{bg:"bg-red-500",icon:"✕"},
    info:{bg:"bg-blue-500",icon:"ℹ"},
    loading:{bg:"bg-slate-800",icon:"⏳"},
  };
  return(
    <div className="fixed top-4 right-4 z-9999 flex flex-col gap-2">
      {toasts.map(t=>{
        const{bg,icon}=styles[t.type];
        return(
          <div key={t.id} className={`${bg} text-white text-[12px] font-semibold px-4 py-2.5 rounded-xl shadow-lg flex items-center gap-2.5 min-w-52 max-w-80`}
            style={{animation:"slideInRight 0.3s ease"}}>
            <span className={t.type==="loading"?"animate-spin inline-block":""}>{icon}</span>
            <span className="flex-1">{t.msg}</span>
          </div>
        );
      })}
    </div>
  );
}

/* ── ProgressBar ─────────────────────────────────────── */
export function ProgressBar({
  pct, color="#5B7CFA", height=6, animated=true, label, sublabel
}:{pct:number;color?:string;height?:number;animated?:boolean;label?:string;sublabel?:string;}){
  const clampedPct = Math.max(0, Math.min(100, pct));
  return(
    <div className="w-full">
      {(label||sublabel)&&(
        <div className="flex items-center justify-between mb-1.5">
          {label&&<span className="text-[11px] font-semibold text-slate-700">{label}</span>}
          {sublabel&&<span className="text-[10px] text-slate-500 font-mono">{sublabel}</span>}
        </div>
      )}
      <div className="w-full bg-slate-100 rounded-full overflow-hidden" style={{height}}>
        <div
          className="h-full rounded-full relative overflow-hidden transition-all duration-500 ease-out"
          style={{width:`${clampedPct}%`, background:color}}
        >
          {animated&&clampedPct>0&&clampedPct<100&&(
            <div className="absolute inset-0 opacity-40"
              style={{
                background:"linear-gradient(90deg, transparent 0%, rgba(255,255,255,0.6) 50%, transparent 100%)",
                animation:"shimmer 1.5s infinite",
                backgroundSize:"200% 100%",
              }}
            />
          )}
        </div>
      </div>
    </div>
  );
}

/* ── StepProgress ────────────────────────────────────── */
export interface Step {
  id: string;
  label: string;
  sublabel?: string;
  status: "waiting"|"active"|"done"|"error";
}

export function StepProgress({steps}:{steps:Step[]}){
  return(
    <div className="flex flex-col gap-0">
      {steps.map((step, i)=>{
        const isLast = i === steps.length - 1;
        const icon = step.status==="done" ? "✓"
          : step.status==="error" ? "✕"
          : step.status==="active" ? null
          : String(i+1);
        const color = step.status==="done" ? "#10B981"
          : step.status==="error" ? "#EF4444"
          : step.status==="active" ? "#5B7CFA"
          : "#CBD5E1";
        return(
          <div key={step.id} className="flex gap-3">
            <div className="flex flex-col items-center">
              <div
                className="w-7 h-7 rounded-full flex items-center justify-center text-[11px] font-bold text-white shrink-0 transition-all duration-300"
                style={{background:color, boxShadow: step.status==="active" ? `0 0 0 3px ${color}30` : "none"}}
              >
                {step.status==="active"
                  ? <span className="w-3 h-3 border-2 border-white border-t-transparent rounded-full animate-spin"/>
                  : icon
                }
              </div>
              {!isLast&&(
                <div
                  className="w-0.5 flex-1 min-h-3 my-1 transition-all duration-500"
                  style={{background: step.status==="done" ? "#10B981" : "#E2E8F0"}}
                />
              )}
            </div>
            <div className={`flex-1 pb-3 ${isLast?"":"pb-1"}`}>
              <div
                className="text-[12px] font-semibold transition-colors"
                style={{color: step.status==="waiting" ? "#94A3B8" : step.status==="error" ? "#EF4444" : "#1E293B"}}
              >
                {step.label}
              </div>
              {step.sublabel&&(
                <div className="text-[10px] text-slate-400 mt-0.5">{step.sublabel}</div>
              )}
            </div>
          </div>
        );
      })}
    </div>
  );
}

/* ═══════════════════════════════════════════════════════
   SolverTerminal — fixed version
   ═══════════════════════════════════════════════════════ */

// ── Types ────────────────────────────────────────────────────────────────────
interface ParsedLine {
  level: "INFO"|"WARNING"|"ERROR"|"DEBUG"|"OTHER";
  body: string;
}

type PhaseId = "setup"|"dry"|"cold"|"osrm"|"save";
type PhaseStatus = "waiting"|"active"|"done";

// ── Phase definitions (log-driven) ───────────────────────────────────────────
const SOLVER_PHASES: { id: PhaseId; icon: string; label: string; triggers: RegExp[] }[] = [
  { id:"setup", icon:"⚡", label:"Setup",     triggers:[/starting/i, /Loading distance/i] },
  { id:"dry",   icon:"📦", label:"DRY solve", triggers:[/\[DRY\]/, /Starting OR-Tools/i] },
  { id:"cold",  icon:"❄️", label:"COLD solve",triggers:[/\[COLD\]/] },
  { id:"osrm",  icon:"🗺", label:"OSRM",      triggers:[/osrm/i, /route_km/i] },
  { id:"save",  icon:"💾", label:"Saving",  triggers:[/[Ss]av(?:ing|ed)/, /Done\./] },
];
const PHASE_ORDER: PhaseId[] = ["setup","dry","cold","osrm","save"];

const PHASE_STATUS_MSG: Record<PhaseId, string> = {
  setup: "Reading data & preparing matrix…",
  dry:   "DRY run — assigning trucks to stores…",
  cold:  "COLD run — night delivery optimization…",
  osrm:  "Calculating real road distances via OSRM…",
  save:  "Finalizing & saving results…",
};

// ── Helpers ──────────────────────────────────────────────────────────────────
const LOG_LEVEL_COLOR: Record<ParsedLine["level"], string> = {
  INFO:"#86efac", WARNING:"#fde68a", ERROR:"#fca5a5", DEBUG:"#94a3b8", OTHER:"#cbd5e1",
};
const LOG_LEVEL_LABEL: Record<ParsedLine["level"], string> = {
  INFO:"INFO", WARNING:"WARN", ERROR:"ERR ", DEBUG:"DBG ", OTHER:"    ",
};

function parseLogLine(raw: string): ParsedLine {
  const m = raw.match(/^(INFO|WARNING|ERROR|DEBUG)\s+\S+\s+—\s+(.+)$/);
  if (m) return { level: m[1] as ParsedLine["level"], body: m[2] };
  return { level:"OTHER", body: raw };
}

function detectLogPhase(body: string): PhaseId | null {
  for (const p of SOLVER_PHASES) {
    if (p.triggers.some(re => re.test(body))) return p.id;
  }
  return null;
}

function highlightLogBody(body: string): React.ReactNode {
  const parts: React.ReactNode[] = [];
  const re = /(\[DRY\]|\[COLD\]|Trip\s+\d+\/\d+|\d+(?:\.\d+)?s\b|\d{2}:\d{2})/g;
  let last = 0, m: RegExpExecArray | null;
  while ((m = re.exec(body)) !== null) {
    if (m.index > last) parts.push(body.slice(last, m.index));
    const tok = m[0];
    const c = tok.startsWith("[DRY]")   ? "#93c5fd"
             : tok.startsWith("[COLD]") ? "#67e8f9"
             : tok.includes("Trip")     ? "#c4b5fd"
             : tok.match(/\d{2}:\d{2}/)? "#86efac"
             : "#fdba74";
    parts.push(<span key={m.index} style={{color:c, fontWeight:700}}>{tok}</span>);
    last = m.index + tok.length;
  }
  if (last < body.length) parts.push(body.slice(last));
  return parts;
}

// Wall-clock estimate: OR-Tools budget runs once per trip × per fleet (DRY+COLD) + overhead
function estimateWallTime(solverTimeBudget: number, maxTrips: number): number {
  return solverTimeBudget * maxTrips * 2 + 10; // 2 fleets + 10s overhead
}

// ── Sub-components ───────────────────────────────────────────────────────────
function SolverPhaseBar({ statuses }: { statuses: Record<PhaseId, PhaseStatus> }) {
  return (
    <div style={{
      display:"flex", gap:4, padding:"6px 12px",
      borderBottom:"1px solid #21262d", background:"#0d1117",
    }}>
      {SOLVER_PHASES.map(p => {
        const s = statuses[p.id];
        const done   = s === "done";
        const active = s === "active";
        const nameColor = done ? "#22c55e" : active ? "#3b82f6" : "#374151";
        const lineColor = done ? "#22c55e" : active ? "#3b82f6" : "#21262d";
        return (
          <div key={p.id} style={{flex:1, display:"flex", alignItems:"center", gap:3, opacity: done||active ? 1 : 0.4}}>
            <span style={{fontSize:10}}>{p.icon}</span>
            <div style={{flex:1, display:"flex", flexDirection:"column", gap:2, minWidth:0}}>
              <span style={{fontSize:8, fontWeight:700, color:nameColor, whiteSpace:"nowrap", overflow:"hidden"}}>
                {p.label}
              </span>
              <div style={{height:2, borderRadius:99, background:lineColor}}/>
            </div>
            {done   && <span style={{fontSize:8, color:"#22c55e"}}>✓</span>}
            {active && <span style={{
              display:"inline-block", width:6, height:6, borderRadius:"50%",
              background:"#3b82f6", flexShrink:0,
              animation:"vrpPulse 1.4s infinite",
            }}/>}
          </div>
        );
      })}
    </div>
  );
}

function SolverLogLine({ line }: { line: ParsedLine }) {
  return (
    <div style={{display:"flex", gap:8, fontSize:11, lineHeight:"18px"}}>
      <span style={{
        color: LOG_LEVEL_COLOR[line.level], fontSize:9, fontWeight:700,
        width:36, textAlign:"right", flexShrink:0, fontVariantNumeric:"tabular-nums",
      }}>
        {LOG_LEVEL_LABEL[line.level]}
      </span>
      <span style={{color:"#c9d1d9", wordBreak:"break-all", flex:1}}>
        {highlightLogBody(line.body)}
      </span>
    </div>
  );
}

// ── Main export ──────────────────────────────────────────────────────────────
export function SolverTerminal({
  running,
  solverTime,
  maxTrips = 2,
  startedAt,
  jobId,
}: {
  running: boolean;
  solverTime: number;
  maxTrips?: number;        // pass `trips` from RunPanel for accurate estimate
  startedAt: number | null;
  jobId: string | null;
}) {
  const [lines,         setLines]         = useState<ParsedLine[]>([]);
  const [elapsed,       setElapsed]       = useState(0);
  const [wsState,       setWsState]       = useState<"connecting"|"open"|"closed">("connecting");
  const [currentPhase,  setCurrentPhase]  = useState<PhaseId>("setup");
  const [phaseStatuses, setPhaseStatuses] = useState<Record<PhaseId, PhaseStatus>>({
    setup:"active", dry:"waiting", cold:"waiting", osrm:"waiting", save:"waiting",
  });
  const bottomRef = useRef<HTMLDivElement>(null);
  const wsRef     = useRef<WebSocket | null>(null);

  const estimatedTotal = estimateWallTime(solverTime, maxTrips);

  // ── WebSocket ──────────────────────────────────────────────────────────────
  useEffect(() => {
    if (!running || !jobId) {
      setLines([]); 
      setElapsed(0); 
      setWsState("connecting");
      setCurrentPhase("setup");
      // Reset all phases to waiting for new run
      setPhaseStatuses({ 
        setup: "active", 
        dry: "waiting", 
        cold: "waiting", 
        osrm: "waiting", 
        save: "waiting" 
      });
      return;
    }
    const ws = new WebSocket(`${WS_BASE}/ws/logs/${jobId}`);
    wsRef.current = ws;
    setWsState("connecting");
    ws.onopen  = () => setWsState("open");
    ws.onerror = () => setWsState("closed");
    ws.onclose = () => setWsState("closed");
    ws.onmessage = (e: MessageEvent<string>) => {
      const msg = e.data;
      if (msg === "__PING__") return;
      if (msg === "__DONE__") { setWsState("closed"); ws.close(); return; }

      const parsed = parseLogLine(msg);

      // Advance phase based on log content (never go backwards)
      const detected = detectLogPhase(parsed.body);
      if (detected) {
        setCurrentPhase(prev => {
          const prevIdx = PHASE_ORDER.indexOf(prev);
          const newIdx  = PHASE_ORDER.indexOf(detected);
          if (newIdx <= prevIdx) return prev;
          setPhaseStatuses(ps => {
            const next = { ...ps };
            for (let i = 0; i < newIdx; i++) next[PHASE_ORDER[i]] = "done";
            next[detected] = "active";
            return next;
          });
          return detected;
        });
      }

      setLines(prev => [...prev.slice(-200), parsed]);
    };
    return () => { ws.close(); wsRef.current = null; };
  }, [running, jobId]);

  // Mark all phases done only when solver completes successfully (not on connection close)
  useEffect(() => {
    if (wsState === "closed" && lines.length > 0) {
      // Check if the last log indicates completion
      const lastLog = lines[lines.length - 1];
      const isCompleted = lastLog?.body.includes("Done.") || lastLog?.body.includes("completed");
      
      if (isCompleted) {
        setPhaseStatuses({ setup:"done", dry:"done", cold:"done", osrm:"done", save:"done" });
      }
    }
  }, [wsState, lines]);

  // ── Elapsed timer ──────────────────────────────────────────────────────────
  useEffect(() => {
    if (!running || !startedAt) { setElapsed(0); return; }
    const id = setInterval(() => {
      setElapsed(Math.floor((Date.now() - startedAt) / 1000));
    }, 500);
    return () => clearInterval(id);
  }, [running, startedAt]);

  // ── Auto-scroll ────────────────────────────────────────────────────────────
  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior:"smooth" });
  }, [lines]);

  if (!running || !startedAt) return null;

  // Cap at 95% while still running; snap to 100% when WS closes
  const rawPct = (elapsed / estimatedTotal) * 100;
  const pct    = wsState === "closed" ? 100 : Math.min(95, rawPct);
  const isLive = wsState === "open";

  const stateColor = isLive ? "#22c55e" : wsState === "connecting" ? "#f59e0b" : "#6b7280";
  const stateLabel = isLive ? "LIVE"    : wsState === "connecting" ? "CONN"    : "DONE";

  return (
    <>
      <style>{`
        @keyframes vrpPulse   { 0%,100%{opacity:1} 50%{opacity:0.3} }
        @keyframes vrpBlink   { 0%,100%{opacity:1} 50%{opacity:0}   }
        @keyframes vrpBounce  { 0%,80%,100%{transform:translateY(0)} 40%{transform:translateY(-3px)} }
        @keyframes vrpShimmer { 0%{transform:translateX(-100%)} 100%{transform:translateX(250%)} }
      `}</style>

      <div style={{
        margin:"0 12px 12px", borderRadius:12, overflow:"hidden",
        border:"1px solid #30363d",
        fontFamily:"'JetBrains Mono','Fira Mono',monospace",
        background:"#0d1117",
      }}>

        {/* ── Header ───────────────────────────────────────────────────────── */}
        <div style={{background:"#161b22", borderBottom:"1px solid #30363d", padding:"10px 14px", display:"flex", flexDirection:"column", gap:8}}>


          {/* Row 1: status dot + timer */}
          <div style={{display:"flex", alignItems:"center", justifyContent:"space-between"}}>
            <div style={{display:"flex", alignItems:"center", gap:6}}>
              <div style={{
                width:8, height:8, borderRadius:"50%", background:stateColor,
                animation: isLive ? "vrpPulse 1.5s infinite" : "none",
              }}/>
              {isLive && (
                <div style={{display:"flex", alignItems:"center", gap:6}}>
                  <img 
                    src="/route-optimizer/working.gif" 
                    alt="Working" 
                    style={{height:40}}
                  />
                </div>
              )}
            </div>
            <span style={{fontSize:11, color:"#6b7280", fontVariantNumeric:"tabular-nums"}}>
              ⏱ {elapsed}s / ~{estimatedTotal}s
            </span>
          </div>

          {/* Row 2: progress bar with shimmer */}
          <div style={{height:3, background:"#21262d", borderRadius:99, overflow:"hidden", position:"relative"}}>
            <div style={{
              position:"absolute", inset:0, width:`${pct}%`,
              background:"linear-gradient(90deg,#22c55e,#3b82f6)",
              borderRadius:99, transition:"width 0.7s ease-out",
            }}/>
            {isLive && pct > 0 && pct < 95 && (
              <div style={{position:"absolute",top:0,left:0,height:"100%",width:`${pct}%`,overflow:"hidden",borderRadius:99}}>
                <div style={{
                  position:"absolute",top:0,height:"100%",width:"40%",
                  background:"linear-gradient(90deg,transparent,rgba(255,255,255,0.45),transparent)",
                  animation:"vrpShimmer 1.8s infinite",
                }}/>
              </div>
            )}
          </div>

          {/* Row 3: status message */}
          <div style={{fontSize:11, color:"#6b7280"}}>
            {PHASE_STATUS_MSG[currentPhase]}
          </div>
        </div>

        {/* ── Phase bar ────────────────────────────────────────────────────── */}
        <SolverPhaseBar statuses={phaseStatuses} />

        {/* ── Log body ──────────────────────────────────────────────────────── */}
        <div style={{height:168, overflowY:"auto", padding:"8px 14px", display:"flex", flexDirection:"column", gap:2}}>
          {lines.length === 0 ? (
            <div style={{display:"flex", alignItems:"center", gap:8, marginTop:24}}>
              <span style={{color:"#374151", fontSize:13, animation:"vrpBlink 1s step-end infinite"}}>▋</span>
              <span style={{color:"#6b7280", fontSize:11}}>
                {wsState === "connecting" ? "Connecting to solver…" : "Waiting for logs…"}
              </span>
            </div>
          ) : (
            lines.map((ln, i) => <SolverLogLine key={i} line={ln}/>)
          )}
          {lines.length > 0 && isLive && (
            <span style={{color:"#374151", fontSize:13, animation:"vrpBlink 1s step-end infinite", marginTop:2}}>▋</span>
          )}
          <div ref={bottomRef}/>
        </div>
      </div>
    </>
  );
}

/* ── DatasetCreationModal ────────────────────────────── */
export function DatasetCreationModal({
  open, steps, title="Өгөгдөл үүсгэж байна"
}:{open:boolean;steps:Step[];title?:string}){
  if(!open) return null;
  const doneCount = steps.filter(s=>s.status==="done").length;
  const total = steps.length;
  const pct = (doneCount/total)*100;
  const hasError = steps.some(s=>s.status==="error");
  return(
    <div className="fixed inset-0 z-9500 flex items-center justify-center p-4">
      <div className="absolute inset-0 bg-slate-900/30 backdrop-blur-sm"/>
      <div className="relative bg-white rounded-2xl shadow-2xl w-full max-w-sm overflow-hidden">
        <div className="px-5 pt-5 pb-4 bg-linear-to-br from-slate-50 to-blue-50 border-b border-slate-200">
          <div className="flex items-center gap-3 mb-3">
            <div className="w-10 h-10 rounded-xl bg-blue-500 flex items-center justify-center text-white text-xl shadow-sm">
              {hasError?"⚠️":"🔄"}
            </div>
            <div>
              <div className="text-[14px] font-extrabold text-slate-900">{title}</div>
              <div className="text-[10px] text-slate-500">{doneCount} of {total} steps complete</div>
            </div>
          </div>
          <ProgressBar
            pct={pct}
            color={hasError?"#EF4444":"#5B7CFA"}
            height={6}
            animated={!hasError}
          />
        </div>
        <div className="p-5">
          <StepProgress steps={steps}/>
        </div>
      </div>
    </div>
  );
}

/* ── UploadZone ──────────────────────────────────────── */
export function UploadZone({label,icon,accept,onFile,fileName}:{label:string;icon:string;accept:string;onFile:(f:File)=>void;fileName?:string;}){
  return(
    <label className={`flex flex-col items-center justify-center rounded-xl border-2 border-dashed p-3 text-center cursor-pointer relative transition-all duration-150 ${fileName?"border-green-500/50 bg-green-500/4":"border-slate-300 bg-white hover:border-red-500 hover:bg-red-500/4"}`}>
      <input type="file" accept={accept} className="absolute inset-0 opacity-0 cursor-pointer" onChange={e=>{const f=e.target.files?.[0];if(f)onFile(f);}}/>
      <span className="text-lg mb-1">{icon}</span>
      {fileName
        ?<span className="text-[10px] text-green-500 font-mono break-all">✓ {fileName}</span>
        :<span className="text-[10px] text-slate-500">{label}</span>}
    </label>
  );
}

/* ── Pill ────────────────────────────────────────────── */
export function Pill({label,color}:{label:string;color?:string;}){
  const c=color??"#3B82F6";
  return(
    <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[10px] font-bold"
      style={{background:c+"18",color:c}}>
      {label}
    </span>
  );
}

/* ── SectionLabel ────────────────────────────────────── */
export function SectionLabel({label,action,extra}:{label:string;action?:ReactNode;extra?:ReactNode}){
  return(
    <div className="flex items-center gap-2 mb-2">
      {action}
      <span className="text-[10px] font-bold text-slate-500 uppercase tracking-widest">{label}</span>
      {extra}
    </div>
  );
}

/* CSS for shimmer + slide-in */
if(typeof document!=="undefined"){
  const style=document.createElement("style");
  style.textContent=`
    @keyframes shimmer{0%{background-position:-200% 0}100%{background-position:200% 0}}
    @keyframes slideInRight{from{opacity:0;transform:translateX(24px)}to{opacity:1;transform:translateX(0)}}
  `;
  if(!document.head.querySelector("[data-vrp-ui]")){
    style.setAttribute("data-vrp-ui","1");
    document.head.appendChild(style);
  }
}