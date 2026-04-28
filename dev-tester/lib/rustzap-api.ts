import { COMPANY_ID, PROJECT_ID } from "@/lib/mock-data";
import type { RustZapHttpResponse } from "@/lib/types";

export const DEFAULT_CHANNEL_ID = "ch_dev_whatsapp";

export function initialMode() {
  return process.env.NEXT_PUBLIC_RUSTZAP_MOCK === "false" ? "real" : "mock";
}

export function rustZapWsUrl() {
  const configured = process.env.NEXT_PUBLIC_RUSTZAP_WS_URL ?? "ws://localhost:8167/ws/v1";
  if (typeof window === "undefined") {
    return configured;
  }
  try {
    const url = new URL(configured);
    if (["localhost", "127.0.0.1", "0.0.0.0"].includes(url.hostname)) {
      url.hostname = window.location.hostname;
      url.port = "8167";
    }
    return url.toString();
  } catch {
    return `ws://${window.location.hostname}:8167/ws/v1`;
  }
}

export function tenantPath(path: string) {
  return `/v1/projects/${PROJECT_ID}/companies/${COMPANY_ID}${path}`;
}

export function devPath(path: string) {
  return `/v1/dev/projects/${PROJECT_ID}/companies/${COMPANY_ID}${path}`;
}

export function idempotencyKey(prefix: string) {
  const random = crypto.randomUUID?.() ?? Math.random().toString(36).slice(2);
  return `${prefix}-${Date.now()}-${random}`;
}

export async function requestRustZap(
  method: string,
  path: string,
  body?: unknown,
  key?: string
): Promise<RustZapHttpResponse> {
  const headers = new Headers({
    accept: "application/json"
  });

  if (body !== undefined) {
    headers.set("content-type", "application/json");
  }
  if (key) {
    headers.set("idempotency-key", key);
  }

  const response = await fetch(`/api/rustzap${path}`, {
    method,
    headers,
    body: body === undefined ? undefined : JSON.stringify(body),
    cache: "no-store"
  });
  const contentType = response.headers.get("content-type") ?? "";
  const parsedBody = contentType.includes("application/json")
    ? await response.json()
    : await response.text();

  return {
    status: response.status,
    body: parsedBody
  };
}
