"use client";

import { useRustZap } from "@/components/rustzap-provider";
import { StatusPill } from "@/components/status-pill";

function formatDate(value?: string) {
  if (!value) {
    return "n/a";
  }
  return new Intl.DateTimeFormat("pt-BR", {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit"
  }).format(new Date(value));
}

export default function DashboardPage() {
  const { state, actions, averageLatencyMs } = useRustZap();
  const latestEvent = state.events[0];
  const pendingMedia = state.media.filter((media) => media.storageStatus !== "permanent").length;
  const unreadTotal = state.conversations.reduce((sum, conversation) => sum + conversation.unreadCount, 0);

  return (
    <section className="page">
      <div className="page-header">
        <div>
          <h1>Dashboard</h1>
          <p>Local control surface for RustZap development state.</p>
        </div>
        <div className="toolbar">
          <button type="button" onClick={actions.ensureDevTenant}>
            Ensure Tenant
          </button>
          <button type="button" onClick={actions.refreshReal}>
            Refresh
          </button>
        </div>
      </div>

      {state.error ? <div className="notice danger">{state.error}</div> : null}

      <div className="metric-grid">
        <article className="metric-card">
          <span>Mode</span>
          <strong>{state.mode}</strong>
          <small>{state.devTenantReady ? "tenant ready" : "tenant not initialized"}</small>
        </article>
        <article className="metric-card">
          <span>Channel</span>
          <strong>
            <StatusPill value={state.channel.status} />
          </strong>
          <small>{state.channel.id}</small>
        </article>
        <article className="metric-card">
          <span>Conversations</span>
          <strong>{state.conversations.length}</strong>
          <small>{unreadTotal} unread</small>
        </article>
        <article className="metric-card">
          <span>Media</span>
          <strong>{state.media.length}</strong>
          <small>{pendingMedia} temporary</small>
        </article>
        <article className="metric-card">
          <span>Events</span>
          <strong>{state.events.length}</strong>
          <small>{averageLatencyMs} ms average latency</small>
        </article>
        <article className="metric-card">
          <span>WebSocket</span>
          <strong>{state.ws.status}</strong>
          <small>{state.ws.attempts} attempts</small>
        </article>
      </div>

      <section className="split-grid">
        <article className="surface">
          <h2>Latest Event</h2>
          {latestEvent ? (
            <dl className="details-list">
              <div>
                <dt>Type</dt>
                <dd>{latestEvent.type}</dd>
              </div>
              <div>
                <dt>Received</dt>
                <dd>{formatDate(latestEvent.receivedAt)}</dd>
              </div>
              <div>
                <dt>Latency</dt>
                <dd>{latestEvent.latencyMs} ms</dd>
              </div>
            </dl>
          ) : (
            <p className="empty-state">No events received yet.</p>
          )}
        </article>
        <article className="surface">
          <h2>Last Real Response</h2>
          {state.lastRealResponse ? (
            <pre className="payload-viewer">{JSON.stringify(state.lastRealResponse, null, 2)}</pre>
          ) : (
            <p className="empty-state">Real mode responses appear here after requests.</p>
          )}
        </article>
      </section>
    </section>
  );
}
