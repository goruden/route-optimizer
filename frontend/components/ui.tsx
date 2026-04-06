/* ─────────────────────────────────────────────────────
   Pure-Tailwind UI primitives — no external UI library
   ───────────────────────────────────────────────────── */
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
    primary:"bg-blue-500 border-blue-500 text-white hover:bg-blue-600 shadow-[0_4px_14px_rgba(59,130,246,0.3)]",
    secondary:"bg-white border-slate-200 text-slate-900 hover:border-blue-500 hover:text-blue-500",
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
      <input id={id} className={`w-full rounded-xl border border-slate-200 bg-white px-3 py-2 text-[12px] text-slate-900 outline-none focus:border-blue-500 focus:ring-2 focus:ring-blue-500/12 placeholder:text-slate-300 transition-all ${className}`} {...rest}/>
    </div>
  );
}

/* ── Select ──────────────────────────────────────────── */
interface SelProps extends React.SelectHTMLAttributes<HTMLSelectElement>{label?:string;options:{v:string;l:string}[];}
export function Sel({label,options,className="",id,...rest}:SelProps){
  return(
    <div className="flex flex-col gap-1">
      {label&&<label htmlFor={id} className="text-[10px] font-bold text-slate-500 uppercase tracking-wider">{label}</label>}
      <select id={id} className={`w-full rounded-xl border border-slate-200 bg-white px-3 py-2 text-[12px] text-slate-900 outline-none focus:border-blue-500 transition-all ${className}`} {...rest}>
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
        className={`rounded-xl border border-slate-200 bg-white px-3 py-2 text-[12px] font-mono text-slate-900 outline-none focus:border-blue-500 focus:ring-2 focus:ring-blue-500/12 transition-all ${className}`}
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
const toasts:{id:number;msg:string;type:ToastType;progress?:number}[]=[];
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
  useRef(null);
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
            {/* Line + dot */}
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
            {/* Content */}
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

// ── Log line parser ──────────────────────────────────────────────
interface ParsedLine {
  level: "INFO" | "WARNING" | "ERROR" | "DEBUG" | "OTHER";
  source: string;
  body: string;
  raw: string;
}
 
function parseLine(raw: string): ParsedLine {
  // Format:  "INFO     vrp_solver — [DRY] Trip 1/2 …"
  const m = raw.match(/^(INFO|WARNING|ERROR|DEBUG)\s+(\S+)\s+—\s+(.+)$/);
  if (m) {
    return { level: m[1] as ParsedLine["level"], source: m[2], body: m[3], raw };
  }
  return { level: "OTHER", source: "", body: raw, raw };
}
 
const LEVEL_COLOR: Record<ParsedLine["level"], string> = {
  INFO:    "#86efac",  // green-300
  WARNING: "#fde68a",  // amber-200
  ERROR:   "#fca5a5",  // red-300
  DEBUG:   "#94a3b8",  // slate-400
  OTHER:   "#cbd5e1",  // slate-300
};
 
const LEVEL_LABEL: Record<ParsedLine["level"], string> = {
  INFO:    "INFO ",
  WARNING: "WARN ",
  ERROR:   "ERR  ",
  DEBUG:   "DBG  ",
  OTHER:   "     ",
};
 
// Highlight keywords inside the body text
function highlightBody(body: string): React.ReactNode {
  const parts: React.ReactNode[] = [];
  // Color fleet tags  [DRY] / [COLD]
  const re = /(\[DRY\]|\[COLD\]|\d+(?:\.\d+)?s|Trip\s+\d+\/\d+|\d{2}:\d{2})/g;
  let last = 0, m: RegExpExecArray | null;
  while ((m = re.exec(body)) !== null) {
    if (m.index > last) parts.push(body.slice(last, m.index));
    const tok = m[0];
    const c =
      tok.startsWith("[DRY]")  ? "#93c5fd"  :  // blue-300
      tok.startsWith("[COLD]") ? "#67e8f9"  :  // cyan-300
      tok.includes("Trip")     ? "#c4b5fd"  :  // violet-300
      tok.match(/\d{2}:\d{2}/) ? "#86efac"  :  // green-300
      "#fdba74";                                  // orange-300
    parts.push(<span key={m.index} style={{ color: c, fontWeight: 700 }}>{tok}</span>);
    last = m.index + tok.length;
  }
  if (last < body.length) parts.push(body.slice(last));
  return parts;
}

/* ── SolverCountdown ─────────────────────────────────── */
export function SolverTerminal({
  running,
  solverTime,
  startedAt,
  jobId,
}: {
  running: boolean;
  solverTime: number;
  startedAt: number | null;
  jobId: string | null;
}) {
  const [lines,   setLines]   = useState<ParsedLine[]>([]);
  const [elapsed, setElapsed] = useState(0);
  const [wsState, setWsState] = useState<"connecting" | "open" | "closed">("connecting");
  const bottomRef = useRef<HTMLDivElement>(null);
  const wsRef     = useRef<WebSocket | null>(null);
 
  // ── WebSocket lifecycle ────────────────────────────────────
  useEffect(() => {
    if (!running || !jobId) {
      setLines([]);
      setElapsed(0);
      setWsState("connecting");
      return;
    }
 
    const url = `${WS_BASE}/ws/logs/${jobId}`;
    const ws  = new WebSocket(url);
    wsRef.current = ws;
    setWsState("connecting");
 
    ws.onopen = () => {
      setWsState("open");
    };
 
    ws.onmessage = (e: MessageEvent<string>) => {
      const msg = e.data;
      if (msg === "__PING__") return;          // keep-alive, ignore
      if (msg === "__DONE__") {
        setWsState("closed");
        ws.close();
        return;
      }
      setLines(prev => [...prev.slice(-150), parseLine(msg)]);
    };
 
    ws.onerror = () => {
      setWsState("closed");
    };
 
    ws.onclose = () => {
      setWsState("closed");
    };
 
    return () => {
      ws.close();
      wsRef.current = null;
    };
  }, [running, jobId]);
 
  // ── Elapsed timer ──────────────────────────────────────────
  useEffect(() => {
    if (!running || !startedAt) { setElapsed(0); return; }
    const id = setInterval(() => {
      setElapsed(Math.floor((Date.now() - startedAt) / 1000));
    }, 500);
    return () => clearInterval(id);
  }, [running, startedAt]);
 
  // ── Auto-scroll ────────────────────────────────────────────
  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [lines]);
 
  if (!running || !startedAt) return null;
 
  const progress   = Math.min(100, (elapsed / solverTime) * 100);
  const remaining  = Math.max(0, solverTime - elapsed);
  const stateColor = wsState === "open" ? "#22c55e" : wsState === "connecting" ? "#f59e0b" : "#94a3b8";
  const stateLabel = wsState === "open" ? "LIVE" : wsState === "connecting" ? "CONNECTING" : "DONE";
 
  return (
    <div className="mx-3 mb-3 rounded-xl overflow-hidden border border-slate-700 shadow-xl"
      style={{ background: "#0d1117", fontFamily: "'JetBrains Mono', 'Fira Mono', monospace" }}>
 
      {/* ── Header bar ───────────────────────────────────────── */}
      <div className="flex items-center gap-3 px-3 py-2 border-b border-slate-700"
        style={{ background: "#161b22" }}>
 
        {/* Traffic-light dots */}
        {/* <div className="flex gap-1.5 shrink-0">
          <div className="w-3 h-3 rounded-full" style={{ background: "#ff5f57" }} />
          <div className="w-3 h-3 rounded-full" style={{ background: "#febc2e" }} />
          <div className="w-3 h-3 rounded-full" style={{ background: "#28c840" }} />
        </div> */}
 
        {/* Title */}
        {/* <span className="text-[11px] text-slate-400 flex-1 truncate">
          vrp-solver
          {jobId && <span className="text-slate-600 ml-2">#{jobId.slice(0, 8)}</span>}
        </span> */}
 
        {/* WS state badge */}
        <div className="flex items-center gap-1.5 shrink-0">
          <div className="w-1.5 h-1.5 rounded-full animate-pulse" style={{ background: stateColor }} />
          <span className="text-[9px] font-bold tracking-widest" style={{ color: stateColor }}>
            {stateLabel}
          </span>
        </div>
 
        {/* Progress + timer */}
        <div className="flex items-center gap-2 shrink-0">
          <div className="w-20 h-1 rounded-full overflow-hidden" style={{ background: "#30363d" }}>
            <div
              className="h-full rounded-full transition-all duration-500"
              style={{
                width: `${progress}%`,
                background: `linear-gradient(90deg, #22c55e, #3b82f6 ${progress}%)`,
              }}
            />
          </div>
          <span className="text-[10px] text-slate-500 w-14 text-right tabular-nums">
            {elapsed}s / {solverTime}s
          </span>
        </div>
      </div>
 
      {/* ── Phase indicators ──────────────────────────────────── */}
      <PhaseBar elapsed={elapsed} solverTime={solverTime} />
 
      {/* ── Log body ──────────────────────────────────────────── */}
      <div className="overflow-y-auto px-3 py-2 flex flex-col gap-0.5" style={{ height: 168 }}>
        {lines.length === 0 ? (
          <div className="flex items-center gap-2 mt-6">
            <span className="text-slate-600 text-[12px] animate-pulse">▋</span>
            <span className="text-slate-500 text-[11px]">
              {wsState === "connecting" ? "Connecting to solver …" : "Waiting for logs …"}
            </span>
          </div>
        ) : (
          lines.map((ln, i) => (
            <LogLine key={i} line={ln} />
          ))
        )}
        {/* Blinking cursor at the end */}
        {lines.length > 0 && wsState === "open" && (
          <span className="text-slate-600 text-[12px] animate-pulse mt-0.5">▋</span>
        )}
        <div ref={bottomRef} />
      </div>
    </div>
  );
}
 
// ── Sub-components ───────────────────────────────────────────────
 
function LogLine({ line }: { line: ParsedLine }) {
  const color = LEVEL_COLOR[line.level];
  return (
    <div className="flex items-baseline gap-2 text-[11px] leading-5">
      <span className="shrink-0 text-[9px] font-bold w-10 text-right tabular-nums" style={{ color }}>
        {LEVEL_LABEL[line.level]}
      </span>
      <span className="flex-1 text-slate-300 break-all">
        {highlightBody(line.body)}
      </span>
    </div>
  );
}
 
function PhaseBar({ elapsed, solverTime }: { elapsed: number; solverTime: number }) {
  // Rough phase heuristics based on elapsed vs budget
  const phases = [
    { label: "Setup",    icon: "⚡", pct: 0,    end: 5  },
    { label: "DRY solve",icon: "📦", pct: 5,    end: 45 },
    { label: "COLD solve",icon:"❄️", pct: 45,   end: 85 },
    { label: "OSRM",    icon: "🗺", pct: 85,   end: 95 },
    { label: "Хадгалах",    icon: "💾", pct: 95,   end: 100 },
  ];
  const progress = Math.min(100, (elapsed / Math.max(1, solverTime)) * 100);
 
  return (
    <div className="flex px-3 py-1.5 gap-1 border-b border-slate-800" style={{ background: "#0d1117" }}>
      {phases.map((p, i) => {
        const done    = progress > p.end;
        const active  = progress >= p.pct && progress <= p.end;
        const opacity = done ? 1 : active ? 1 : 0.35;
        return (
          <div key={i} className="flex items-center gap-1 flex-1 min-w-0">
            <span className="text-[10px]" style={{ opacity }}>{p.icon}</span>
            <div className="flex-1 flex flex-col gap-0.5 min-w-0">
              <span className="text-[8px] truncate font-semibold"
                style={{ color: done ? "#22c55e" : active ? "#3b82f6" : "#475569", opacity }}>
                {p.label}
              </span>
              <div className="h-0.5 rounded-full" style={{
                background: done ? "#22c55e" : active ? "#3b82f6" : "#1e293b",
              }} />
            </div>
            {done && <span className="text-[9px] text-green-500" style={{ opacity }}>✓</span>}
            {active && <span className="w-1.5 h-1.5 rounded-full bg-blue-500 animate-pulse shrink-0" />}
          </div>
        );
      })}
    </div>
  );
}
 

/* ── DatasetCreationProgress ─────────────────────────── */
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

/* ── Upload zone ─────────────────────────────────────── */
export function UploadZone({label,icon,accept,onFile,fileName}:{label:string;icon:string;accept:string;onFile:(f:File)=>void;fileName?:string;}){
  return(
    <label className={`flex flex-col items-center justify-center rounded-xl border-2 border-dashed p-3 text-center cursor-pointer relative transition-all duration-150 ${fileName?"border-green-500/50 bg-green-500/4":"border-slate-300 bg-white hover:border-blue-500 hover:bg-blue-500/4"}`}>
      <input type="file" accept={accept} className="absolute inset-0 opacity-0 cursor-pointer" onChange={e=>{const f=e.target.files?.[0];if(f)onFile(f);}}/>
      <span className="text-lg mb-1">{icon}</span>
      {fileName
        ?<span className="text-[10px] text-green-500 font-mono break-all">✓ {fileName}</span>
        :<span className="text-[10px] text-slate-500">{label}</span>}
    </label>
  );
}

/* ── Pill / chip ─────────────────────────────────────── */
export function Pill({label,color}:{label:string;color?:string;}){
  const c=color??"#3B82F6";
  return(
    <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[10px] font-bold"
      style={{background:c+"18",color:c}}>
      {label}
    </span>
  );
}

/* ── Section heading ─────────────────────────────────── */
export function SectionLabel({label,action}:{label:string;action?:ReactNode;}){
  return(
    <div className="flex items-center justify-between mb-2">
      <span className="text-[10px] font-bold text-slate-500 uppercase tracking-widest">{label}</span>
      {action}
    </div>
  );
}

/* CSS for shimmer */
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