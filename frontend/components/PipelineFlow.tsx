"use client";

import React from "react";
import {
  ArrowRight,
  Server,
  ShieldCheck,
  RefreshCw,
  AlertOctagon,
  CheckCircle2,
  Inbox,
} from "lucide-react";

interface PipelineFlowProps {
  activeStage: string | null;
}

export default function PipelineFlow({ activeStage }: PipelineFlowProps) {
  const nodes = [
    { key: "KAFKA_RECEIVED", label: "1. Kafka Topic", icon: Inbox },
    { key: "IDEMPOTENCY_CHECK", label: "2. Redis (SETNX)", icon: ShieldCheck },
    { key: "DOWNSTREAM_ATTEMPT", label: "3. Ödeme Gateway", icon: Server },
    { key: "RETRY", label: "4. Polly Retry", icon: RefreshCw },
    { key: "COMPLETED", label: "5. Başarılı İşlem", icon: CheckCircle2 },
    { key: "DLQ", label: "6. Dead Letter Queue", icon: AlertOctagon, isDlq: true },
  ];

  return React.createElement(
    "div",
    { className: "bg-slate-900 border border-slate-800 rounded-xl p-6 shadow-xl" },
    React.createElement(
      "div",
      { className: "flex items-center justify-between mb-4" },
      React.createElement(
        "span",
        { className: "text-xs font-semibold uppercase tracking-wider text-slate-400" },
        "Canlı Boru Hattı Akışı (Live Pipeline Execution)"
      ),
      activeStage
        ? React.createElement(
            "span",
            {
              className:
                "text-xs text-amber-400 font-mono bg-amber-950/60 px-2 py-1 rounded border border-amber-800/60",
            },
            `İşleniyor: ${activeStage}`
          )
        : null
    ),
    React.createElement(
      "div",
      { className: "flex flex-wrap items-center gap-3" },
      nodes.map((node, index) => {
        const Icon = node.icon;
        const isActive = activeStage === node.key;

        let boxColor = "bg-slate-950 border-slate-800 text-slate-400";
        let iconColor = "text-slate-500";
        let textColor = "text-slate-300";

        if (isActive) {
          if (node.isDlq) {
            boxColor = "bg-rose-950 border-rose-500 scale-105 shadow-lg shadow-rose-900/50";
            iconColor = "text-rose-400 animate-bounce";
            textColor = "text-white font-bold";
          } else if (node.key === "COMPLETED") {
            boxColor = "bg-emerald-950 border-emerald-500 scale-105 shadow-lg shadow-emerald-900/50";
            iconColor = "text-emerald-400";
            textColor = "text-white font-bold";
          } else {
            boxColor = "bg-orange-950 border-orange-500 scale-105 shadow-lg shadow-orange-900/50";
            iconColor = "text-orange-400 animate-spin";
            textColor = "text-white font-bold";
          }
        }

        return React.createElement(
          React.Fragment,
          { key: node.key },
          React.createElement(
            "div",
            {
              className: `flex items-center gap-3 px-4 py-3 rounded-lg border transition-all duration-300 ${boxColor}`,
            },
            React.createElement(Icon, { className: `w-5 h-5 ${iconColor}` }),
            React.createElement("span", { className: `text-sm ${textColor}` }, node.label)
          ),
          index < nodes.length - 1 && index !== 4
            ? React.createElement(ArrowRight, {
                className: "w-4 h-4 text-slate-600 hidden lg:block",
              })
            : null
        );
      })
    )
  );
}