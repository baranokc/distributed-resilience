"use client";

import { useEffect, useState, useRef, useCallback } from "react";
import * as signalR from "@microsoft/signalr";

export interface PipelineStep {
  orderId: string;
  stage: "KAFKA_RECEIVED" | "IDEMPOTENCY_CHECK" | "DOWNSTREAM_ATTEMPT" | "RETRY" | "DLQ" | "COMPLETED";
  status: "INFO" | "SUCCESS" | "WARNING" | "ERROR";
  message: string;
  retryAttempt?: number | null;
  latencyMs?: number | null;
  timestamp: string;
}

export interface DbOrder {
  id: string;
  orderId: string;
  amount: number;
  customerId: string;
  status: string;
  failureReason?: string | null;
  createdAt: string;
  processedAt: string;
}

export function useResilienceHub(baseUrl: string = "http://localhost:5000") {
  const [logs, setLogs] = useState([] as PipelineStep[]);
  const [dbOrders, setDbOrders] = useState([] as DbOrder[]);
  const [connectionStatus, setConnectionStatus] = useState("connecting");
  const [activeStage, setActiveStage] = useState(null as string | null);
  const [circuitState, setCircuitState] = useState("CLOSED"); // CLOSED, OPEN, HALF_OPEN
  
  const connectionRef = useRef(null as signalR.HubConnection | null);

  const fetchOrders = useCallback(async () => {
    try {
      const res = await fetch(`${baseUrl}/api/orders`);
      if (res.ok) {
        const data = (await res.json()) as DbOrder[];
        setDbOrders(data);
      }
    } catch (err) {
      console.error("PostgreSQL siparişleri çekilemedi:", err);
    }
  }, [baseUrl]);

  const replayDlq = async () => {
    try {
      const res = await fetch(`${baseUrl}/api/orders/replay-dlq`, { method: "POST" });
      const data = await res.json();
      return data;
    } catch (err) {
      console.error("DLQ Replay hatası:", err);
      return null;
    }
  };

  useEffect(() => {
    fetchOrders();

    const connection = new signalR.HubConnectionBuilder()
      .withUrl(`${baseUrl}/hubs/resilience`, {
        transport: signalR.HttpTransportType.WebSockets | signalR.HttpTransportType.LongPolling,
      })
      .withAutomaticReconnect([0, 2000, 5000, 10000])
      .configureLogging(signalR.LogLevel.Warning)
      .build();

    connectionRef.current = connection;

    connection.on("ReceivePipelineStep", (step: any) => {
      const stepWithTime: PipelineStep = {
        ...step,
        timestamp: new Date().toLocaleTimeString("tr-TR", { hour12: false }),
      };

      setActiveStage(step.stage);
      setLogs((prev) => [stepWithTime, ...prev.slice(0, 49)]);

      if (step.stage === "COMPLETED" || step.stage === "DLQ") {
        setTimeout(fetchOrders, 400);
      }
    });

    connection.on("ReceiveCircuitBreakerState", (state: string) => {
      setCircuitState(state);
    });

    connection
      .start()
      .then(() => setConnectionStatus("connected"))
      .catch(() => setConnectionStatus("disconnected"));

    connection.onreconnecting(() => setConnectionStatus("connecting"));
    connection.onreconnected(() => {
      setConnectionStatus("connected");
      fetchOrders();
    });
    connection.onclose(() => setConnectionStatus("disconnected"));

    return () => {
      connection.stop();
    };
  }, [baseUrl, fetchOrders]);

  return {
    logs,
    dbOrders,
    connectionStatus,
    activeStage,
    circuitState,
    replayDlq,
    clearLogs: () => setLogs([] as PipelineStep[]),
    refreshOrders: fetchOrders,
  };
}