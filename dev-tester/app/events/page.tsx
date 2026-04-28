"use client";

import { useMemo } from "react";
import { useRustZap } from "@/components/rustzap-provider";

function formatTime(value: string) {
  return new Intl.DateTimeFormat("pt-BR", {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit"
  }).format(new Date(value));
}

export default function EventsPage() {
  const { state, actions, averageLatencyMs } = useRustZap();
  const latestPayload = useMemo(
    () => (state.events[0] ? JSON.stringify(state.events[0].payload, null, 2) : ""),
    [state.events]
  );

  return (
    <section className="page">
      <div className="page-header">
        <div>
          <h1>Events</h1>
          <p>WebSocket/mock event stream, reconnect state, latency, and payload inspection.</p>
        </div>
        <div className="toolbar">
          <button type="button" onClick={actions.connectWebSocket}>
            Connect
          </button>
          <button type="button" onClick={actions.disconnectWebSocket}>
            Disconnect
          </button>
        </div>
      </div>

      <div className="metric-grid compact">
        <article className="metric-card">
          <span>Status</span>
          <strong>{state.ws.status}</strong>
          <small>{state.ws.lastError ?? "no socket error"}</small>
        </article>
        <article className="metric-card">
          <span>Attempts</span>
          <strong>{state.ws.attempts}</strong>
          <small>reconnect counter</small>
        </article>
        <article className="metric-card">
          <span>Average Latency</span>
          <strong>{averageLatencyMs} ms</strong>
          <small>{state.events.length} events sampled</small>
        </article>
      </div>

      <section className="split-grid">
        <article className="surface">
          <h2>Stream</h2>
          <div className="event-stream">
            {state.events.map((event) => (
              <button type="button" key={event.id}>
                <span>
                  <b>{event.type}</b>
                  <small>{formatTime(event.receivedAt)} · seq {event.eventSeq}</small>
                </span>
                <strong>{event.latencyMs} ms</strong>
              </button>
            ))}
          </div>
        </article>
        <article className="surface">
          <h2>Latest Payload</h2>
          {latestPayload ? (
            <pre className="payload-viewer">{latestPayload}</pre>
          ) : (
            <p className="empty-state">No payload captured yet.</p>
          )}
        </article>
      </section>
    </section>
  );
}
