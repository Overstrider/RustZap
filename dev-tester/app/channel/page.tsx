"use client";

import { useMemo } from "react";
import { useRustZap } from "@/components/rustzap-provider";
import { StatusPill } from "@/components/status-pill";

function secondsUntil(value: string) {
  const diff = Date.parse(value) - Date.now();
  if (!Number.isFinite(diff) || diff <= 0) {
    return "expired";
  }
  return `${Math.ceil(diff / 1000)}s`;
}

export default function ChannelPage() {
  const { state, actions } = useRustZap();
  const capabilities = useMemo(
    () => Object.entries(state.channel.capabilities).sort(([left], [right]) => left.localeCompare(right)),
    [state.channel.capabilities]
  );

  return (
    <section className="page">
      <div className="page-header">
        <div>
          <h1>Channel</h1>
          <p>WhatsApp account setup, QR rotation, and capability visibility.</p>
        </div>
        <StatusPill value={state.channel.status} />
      </div>

      <section className="split-grid">
        <article className="surface">
          <h2>Account</h2>
          <dl className="details-list">
            <div>
              <dt>Project</dt>
              <dd>{state.projects[0]?.id ?? "tetoz"}</dd>
            </div>
            <div>
              <dt>Company</dt>
              <dd>{state.companies[0]?.id ?? "company_dev"}</dd>
            </div>
            <div>
              <dt>Channel</dt>
              <dd>{state.channel.id}</dd>
            </div>
            <div>
              <dt>Phone</dt>
              <dd>{state.channel.phoneE164}</dd>
            </div>
          </dl>
          <div className="toolbar wrap">
            <button type="button" onClick={actions.ensureDevTenant}>
              Ensure Tenant
            </button>
            <button type="button" onClick={actions.createChannel}>
              Create Channel
            </button>
            <button type="button" onClick={actions.connectChannel}>
              Connect
            </button>
            <button type="button" onClick={actions.cycleChannelStatus}>
              Cycle Status
            </button>
          </div>
        </article>

        <article className="surface">
          <h2>QR Pairing</h2>
          <div className="qr-text-frame">
            <code>{state.channel.qrCodeText || "QR not requested"}</code>
          </div>
          <dl className="details-list">
            <div>
              <dt>Expires</dt>
              <dd>{state.channel.qrExpiresAt}</dd>
            </div>
            <div>
              <dt>Countdown</dt>
              <dd>{secondsUntil(state.channel.qrExpiresAt)}</dd>
            </div>
            <div>
              <dt>Generation</dt>
              <dd>{state.channel.qrGeneration}</dd>
            </div>
          </dl>
          <div className="toolbar wrap">
            <button type="button" onClick={actions.fetchQr}>
              Fetch QR
            </button>
            <button type="button" onClick={actions.rotateQr}>
              Rotate QR
            </button>
          </div>
        </article>
      </section>

      <section className="surface">
        <h2>Capabilities</h2>
        <div className="capability-grid">
          {capabilities.map(([capability, enabled]) => (
            <span className={enabled ? "capability enabled" : "capability disabled"} key={capability}>
              <b>{capability}</b>
              <small>{enabled ? "enabled" : "not supported"}</small>
            </span>
          ))}
        </div>
      </section>
    </section>
  );
}
