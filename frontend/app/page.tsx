"use client";

import React, { useState } from "react";
import { useResilienceHub } from "@/hooks/useResilienceHub";
import PipelineFlow from "@/components/PipelineFlow";
import {
  CheckCircle2,
  AlertTriangle,
  ShieldAlert,
  Trash2,
  Database,
  RefreshCw,
  Zap,
  RotateCcw,
  Activity,
  ServerCrash,
  TerminalSquare
} from "lucide-react";

export default function Dashboard() {
  const hubData = useResilienceHub() as any;
  const logs = (hubData.logs || []) as any[];
  const dbOrders = (hubData.dbOrders || []) as any[];
  const connectionStatus = (hubData.connectionStatus || "connecting") as string;
  const activeStage = (hubData.activeStage || null) as string | null;
  const circuitState = (hubData.circuitState || "CLOSED") as string;
  const replayDlq = hubData.replayDlq as any;
  const clearLogs = hubData.clearLogs as any;
  const refreshOrders = hubData.refreshOrders as any;

  const [loadingScenario, setLoadingScenario] = useState(null as any);

  const triggerScenario = async (type: string) => {
    try {
      setLoadingScenario(type);
      await fetch("http://localhost:5000/api/orders/produce?type=" + type, {
        method: "POST",
      });
    } catch (err) {
      console.error("Senaryo tetikleme hatası:", err);
    } finally {
      setTimeout(() => setLoadingScenario(null), 500);
    }
  };

  const handleReplayDlq = async () => {
    try {
      setLoadingScenario("replay");
      if (replayDlq) {
        await replayDlq();
      }
    } catch (err) {
      console.error("DLQ Replay hatası:", err);
    } finally {
      setTimeout(() => setLoadingScenario(null), 600);
    }
  };

  const successCount = logs.filter((l: any) => l.stage === "COMPLETED").length;
  const dlqCount = logs.filter((l: any) => l.stage === "DLQ").length;
  const duplicateBlockedCount = logs.filter(
    (l: any) => l.message && l.message.includes("[DUPLICATE DETECTED]")
  ).length;
  const totalRetries = logs.filter((l: any) => l.stage === "RETRY").length;

  return React.createElement(
    "main",
    { 
      className: "min-h-screen text-slate-100 p-4 md:p-8 font-sans bg-slate-950 bg-[radial-gradient(ellipse_at_top,_var(--tw-gradient-stops))] from-slate-900 via-slate-950 to-black" 
    },
    React.createElement(
      "div",
      { className: "max-w-7xl mx-auto space-y-6" },

      // --- 1. HEADER (Glassmorphism) ---
      React.createElement(
        "header",
        {
          className:
            "relative overflow-hidden bg-slate-900/40 backdrop-blur-xl border border-slate-800/60 rounded-2xl p-6 shadow-2xl flex flex-col md:flex-row justify-between items-start md:items-center gap-6",
        },
        // Süsleme Işığı
        React.createElement("div", { className: "absolute -top-24 -left-24 w-48 h-48 bg-blue-600/20 rounded-full blur-3xl pointer-events-none" }),
        
        React.createElement(
          "div",
          { className: "relative z-10" },
          React.createElement(
            "div",
            { className: "flex items-center gap-3 mb-2" },
            React.createElement(
              "span",
              {
                className:
                  "text-[10px] font-mono font-bold uppercase tracking-widest px-2.5 py-1 rounded bg-blue-500/10 text-blue-400 border border-blue-500/20",
              },
              "Kurumsal POC"
            ),
            React.createElement(
              "span",
              {
                className:
                  "text-[10px] font-mono font-bold uppercase tracking-widest px-2.5 py-1 rounded bg-indigo-500/10 text-indigo-400 border border-indigo-500/20",
              },
              "Dağıtık Mimari"
            )
          ),
          React.createElement(
            "h1",
            { className: "text-3xl font-extrabold tracking-tight bg-clip-text text-transparent bg-gradient-to-r from-white to-slate-400" },
            "Resilience & Hata Yönetimi"
          ),
          React.createElement(
            "p",
            { className: "text-sm text-slate-400 mt-2 max-w-xl leading-relaxed" },
            "Yüksek Trafik Altında: Timeout, Exponential Retry, Circuit Breaker, Redis Idempotency ve DLQ Replay Canlı Simülasyonu."
          )
        ),
        React.createElement(
          "div",
          { className: "relative z-10 flex flex-wrap items-center gap-3" },
          
          React.createElement(
            "div",
            {
              className: `flex items-center gap-2 px-3.5 py-2 rounded-xl border text-xs font-mono font-bold shadow-lg transition-colors ${
                circuitState === "CLOSED"
                  ? "bg-emerald-500/10 border-emerald-500/30 text-emerald-400"
                  : circuitState === "OPEN"
                  ? "bg-rose-500/10 border-rose-500/50 text-rose-400 animate-pulse shadow-rose-900/20"
                  : "bg-amber-500/10 border-amber-500/30 text-amber-400"
              }`,
            },
            React.createElement(Zap, { className: "w-4 h-4" }),
            `CIRCUIT: ${circuitState}`
          ),

          React.createElement(
            "span",
            {
              className: `flex items-center gap-2 text-xs font-medium px-3.5 py-2 rounded-xl border shadow-lg ${
                connectionStatus === "connected"
                  ? "bg-emerald-500/10 text-emerald-400 border-emerald-500/30"
                  : "bg-rose-500/10 text-rose-400 border-rose-500/30"
              }`,
            },
            React.createElement("span", {
              className: `w-2 h-2 rounded-full ${
                connectionStatus === "connected" ? "bg-emerald-400 animate-pulse shadow-[0_0_8px_rgba(52,211,153,0.8)]" : "bg-rose-400"
              }`,
            }),
            `SignalR`
          ),

          React.createElement(
            "button",
            {
              onClick: clearLogs,
              className:
                "p-2 text-slate-400 hover:text-white bg-slate-800/50 hover:bg-slate-700/50 border border-slate-700/50 rounded-xl transition shadow-lg",
              title: "Logları Temizle",
            },
            React.createElement(Trash2, { className: "w-4 h-4" })
          )
        )
      ),

      // --- 2. METRİK KARTLARI (Derinlik ve İkonlu) ---
      React.createElement(
        "div",
        { className: "grid grid-cols-2 md:grid-cols-4 gap-4" },
        [
          { label: "Başarılı İşlem", count: successCount, color: "emerald", icon: CheckCircle2 },
          { label: "DLQ İzolasyonu", count: dlqCount, color: "rose", icon: ServerCrash },
          { label: "Engellenen Çift Çekim", count: duplicateBlockedCount, color: "amber", icon: ShieldAlert },
          { label: "Polly Retry Denemesi", count: totalRetries, color: "orange", icon: Activity },
        ].map((stat, i) => 
          React.createElement(
            "div",
            { 
              key: i,
              className: `relative overflow-hidden bg-slate-900/40 backdrop-blur-md border border-slate-800/60 p-5 rounded-2xl shadow-lg border-t-2 border-t-${stat.color}-500/50 group hover:bg-slate-800/40 transition-colors`
            },
            React.createElement(stat.icon, { className: `absolute -right-4 -bottom-4 w-24 h-24 text-${stat.color}-500/5 transform -rotate-12 group-hover:scale-110 transition-transform` }),
            React.createElement("div", { className: "relative z-10" },
              React.createElement("div", { className: "text-[11px] uppercase tracking-wider text-slate-400 font-semibold mb-1" }, stat.label),
              React.createElement("div", { className: `text-3xl font-black text-${stat.color}-400 drop-shadow-md` }, stat.count)
            )
          )
        )
      ),

      // --- 3. PIPELINE BİLEŞENİ ---
      React.createElement(PipelineFlow, { activeStage }),

      // --- 4. AKSİYON KARTLARI (Senaryo Simülatörü) ---
      React.createElement(
        "div",
        { className: "bg-slate-900/40 backdrop-blur-xl border border-slate-800/60 p-6 rounded-2xl shadow-xl space-y-5" },
        React.createElement(
          "div",
          { className: "flex items-center justify-between border-b border-slate-800/60 pb-4" },
          React.createElement(
            "h2",
            { className: "text-sm font-bold uppercase tracking-widest text-slate-300 flex items-center gap-2" },
            React.createElement(TerminalSquare, { className: "w-4 h-4 text-indigo-400" }),
            "Simülasyon Senaryoları"
          ),
          React.createElement(
            "button",
            {
              onClick: handleReplayDlq,
              disabled: loadingScenario !== null,
              className:
                "flex items-center gap-2 px-4 py-2 bg-indigo-500/10 hover:bg-indigo-500/20 border border-indigo-500/30 disabled:opacity-50 text-indigo-300 text-xs font-bold uppercase tracking-wider rounded-lg shadow-lg transition-all",
            },
            React.createElement(RotateCcw, { className: "w-4 h-4" }),
            "DLQ Replay"
          )
        ),
        React.createElement(
          "div",
          { className: "grid grid-cols-1 md:grid-cols-3 gap-4" },
          // Buton 1
          React.createElement(
            "button",
            {
              onClick: () => triggerScenario("success"),
              disabled: loadingScenario !== null,
              className: "group relative overflow-hidden flex flex-col items-start p-4 bg-slate-950/50 hover:bg-emerald-950/30 border border-slate-800 hover:border-emerald-500/30 disabled:opacity-50 rounded-xl transition-all text-left",
            },
            React.createElement("div", { className: "flex items-center gap-2 mb-1" },
              React.createElement(CheckCircle2, { className: "w-5 h-5 text-emerald-500" }),
              React.createElement("span", { className: "font-bold text-slate-200 group-hover:text-emerald-400 transition-colors" }, "1. Happy Path")
            ),
            React.createElement("span", { className: "text-xs text-slate-500 leading-relaxed" }, "Boru hattı üzerinden normal bir siparişin 200 OK ile işlenmesi.")
          ),
          // Buton 2
          React.createElement(
            "button",
            {
              onClick: () => triggerScenario("timeout"),
              disabled: loadingScenario !== null,
              className: "group relative overflow-hidden flex flex-col items-start p-4 bg-slate-950/50 hover:bg-rose-950/30 border border-slate-800 hover:border-rose-500/30 disabled:opacity-50 rounded-xl transition-all text-left",
            },
            React.createElement("div", { className: "flex items-center gap-2 mb-1" },
              React.createElement(AlertTriangle, { className: "w-5 h-5 text-rose-500" }),
              React.createElement("span", { className: "font-bold text-slate-200 group-hover:text-rose-400 transition-colors" }, "2. Timeout & Devre Kesici")
            ),
            React.createElement("span", { className: "text-xs text-slate-500 leading-relaxed" }, "5s gecikme yaratır. Polly Retry ve Circuit Breaker (Fail-Fast) tetikler.")
          ),
          // Buton 3
          React.createElement(
            "button",
            {
              onClick: () => triggerScenario("duplicate"),
              disabled: loadingScenario !== null,
              className: "group relative overflow-hidden flex flex-col items-start p-4 bg-slate-950/50 hover:bg-amber-950/30 border border-slate-800 hover:border-amber-500/30 disabled:opacity-50 rounded-xl transition-all text-left",
            },
            React.createElement("div", { className: "flex items-center gap-2 mb-1" },
              React.createElement(ShieldAlert, { className: "w-5 h-5 text-amber-500" }),
              React.createElement("span", { className: "font-bold text-slate-200 group-hover:text-amber-400 transition-colors" }, "3. Idempotency (SETNX)")
            ),
            React.createElement("span", { className: "text-xs text-slate-500 leading-relaxed" }, "Aynı siparişi gönderir. Redis seviyesinde çift çekim engellenir.")
          )
        )
      ),

      React.createElement(
        "div",
        { className: "grid grid-cols-1 lg:grid-cols-2 gap-6" },

        // Terminal Penceresi
        React.createElement(
          "div",
          { className: "bg-slate-950/80 backdrop-blur-xl border border-slate-800/60 rounded-2xl overflow-hidden shadow-2xl flex flex-col ring-1 ring-white/5" },
          React.createElement(
            "div",
            { className: "bg-slate-900/80 px-4 py-3 border-b border-slate-800/60 flex justify-between items-center" },
            React.createElement(
              "div", 
              { className: "flex items-center gap-2" },
              // Mac Window Dots
              React.createElement("div", { className: "flex gap-1.5 mr-2" },
                React.createElement("div", { className: "w-3 h-3 rounded-full bg-rose-500/80" }),
                React.createElement("div", { className: "w-3 h-3 rounded-full bg-amber-500/80" }),
                React.createElement("div", { className: "w-3 h-3 rounded-full bg-emerald-500/80" })
              ),
              React.createElement("span", { className: "text-[11px] font-mono text-slate-400" }, "bash: kafka-consumer.log")
            ),
            React.createElement("span", { className: "flex items-center gap-2 text-[10px] text-emerald-500/70 font-mono uppercase tracking-wider" }, 
              React.createElement("span", { className: "w-1.5 h-1.5 bg-emerald-500 rounded-full animate-pulse" }),
              "Live Stream"
            )
          ),
          React.createElement(
            "div",
            { className: "p-5 h-[380px] overflow-y-auto font-mono text-[11px] space-y-2.5 bg-black/40 flex-1" },
            logs.length === 0
              ? React.createElement(
                  "div",
                  { className: "text-slate-600/70 italic py-16 flex flex-col items-center justify-center h-full gap-2" },
                  React.createElement(TerminalSquare, { className: "w-8 h-8 opacity-20" }),
                  "Aksiyon bekleniyor..."
                )
              : logs.map((log: any, index: number) =>
                  React.createElement(
                    "div",
                    {
                      key: index,
                      className: `flex items-start gap-3 p-2.5 rounded-md border border-transparent transition-colors ${
                        log.status === "ERROR"
                          ? "bg-rose-950/20 text-rose-300/90 border-rose-900/30"
                          : log.status === "WARNING"
                          ? "bg-amber-950/20 text-amber-300/90 border-amber-900/30"
                          : log.status === "SUCCESS"
                          ? "bg-emerald-950/10 text-emerald-300/90 border-emerald-900/30"
                          : "text-slate-300/80 hover:bg-slate-900/50"
                      }`,
                    },
                    React.createElement("span", { className: "text-slate-500 shrink-0 select-none" }, `[${log.timestamp}]`),
                    React.createElement("span", { className: "font-bold shrink-0 opacity-80" }, `${log.stage}`),
                    React.createElement("span", { className: "flex-1 break-words leading-relaxed" }, log.message)
                  )
                )
          )
        ),

        // PostgreSQL Penceresi
        React.createElement(
          "div",
          { className: "bg-slate-950/80 backdrop-blur-xl border border-slate-800/60 rounded-2xl overflow-hidden shadow-2xl flex flex-col ring-1 ring-white/5" },
          React.createElement(
            "div",
            { className: "bg-slate-900/80 px-4 py-3 border-b border-slate-800/60 flex justify-between items-center" },
            React.createElement(
              "div",
              { className: "flex items-center gap-2.5" },
              React.createElement(Database, { className: "w-4 h-4 text-blue-400" }),
              React.createElement("span", { className: "text-xs font-bold uppercase tracking-wider text-slate-300" }, "Veritabanı: Orders")
            ),
            React.createElement(
              "button",
              { onClick: refreshOrders, className: "text-slate-400 hover:text-white p-1 rounded hover:bg-slate-800 transition", title: "Tabloyu Yenile" },
              React.createElement(RefreshCw, { className: "w-4 h-4" })
            )
          ),
          React.createElement(
            "div",
            { className: "h-[380px] overflow-y-auto font-mono text-xs bg-black/40 flex-1" },
            dbOrders.length === 0
              ? React.createElement(
                  "div",
                  { className: "text-slate-600/70 italic flex items-center justify-center h-full" },
                  "Tabloda henüz kayıt yok."
                )
              : React.createElement(
                  "table",
                  { className: "w-full text-left border-collapse" },
                  React.createElement(
                    "thead",
                    { className: "sticky top-0 bg-slate-900/95 backdrop-blur z-10 shadow-sm" },
                    React.createElement(
                      "tr",
                      { className: "border-b border-slate-800/60 text-slate-400 text-[10px] uppercase tracking-widest" },
                      React.createElement("th", { className: "py-3 px-4" }, "Sipariş ID"),
                      React.createElement("th", { className: "py-3 px-4" }, "Tutar"),
                      React.createElement("th", { className: "py-3 px-4" }, "Statü"),
                      React.createElement("th", { className: "py-3 px-4 text-right" }, "Zaman")
                    )
                  ),
                  React.createElement(
                    "tbody",
                    { className: "divide-y divide-slate-800/30" },
                    dbOrders.map((o: any) =>
                      React.createElement(
                        "tr",
                        { key: o.id, className: "hover:bg-slate-800/40 transition-colors group" },
                        React.createElement("td", { className: "py-3 px-4 text-slate-300 font-medium group-hover:text-white transition-colors" }, o.orderId),
                        React.createElement("td", { className: "py-3 px-4 text-slate-400" }, `${o.amount} ₺`),
                        React.createElement(
                          "td",
                          { className: "py-3 px-4" },
                          React.createElement(
                            "span",
                            {
                              className: `px-2 py-1 rounded-md text-[9px] font-black uppercase tracking-widest ${
                                o.status === "COMPLETED"
                                  ? "bg-emerald-500/10 text-emerald-400 border border-emerald-500/20"
                                  : "bg-rose-500/10 text-rose-400 border border-rose-500/20"
                              }`,
                            },
                            o.status
                          )
                        ),
                        React.createElement(
                          "td",
                          { className: "py-3 px-4 text-right text-slate-500 text-[10px]" },
                          new Date(o.processedAt).toLocaleTimeString("tr-TR", { hour12: false })
                        )
                      )
                    )
                  )
                )
          )
        )
      )
    )
  );
}