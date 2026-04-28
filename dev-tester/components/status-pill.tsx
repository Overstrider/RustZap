import type { ChannelStatus, MessageStatus, WsStatus } from "@/lib/types";

type StatusPillProps = {
  value: ChannelStatus | MessageStatus | WsStatus | string;
};

function toneFor(value: string) {
  if (["connected", "read", "played", "completed"].includes(value)) {
    return "ok";
  }
  if (["waiting_qr", "connecting", "queued", "server_ack", "reconnecting"].includes(value)) {
    return "warn";
  }
  if (["disconnected", "failed", "error", "closed"].includes(value)) {
    return "bad";
  }
  return "info";
}

export function StatusPill({ value }: StatusPillProps) {
  return <span className={`pill ${toneFor(value)}`}>{value}</span>;
}
