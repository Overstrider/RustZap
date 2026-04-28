import { NextRequest, NextResponse } from "next/server";

type RouteContext = {
  params: Promise<{
    path: string[];
  }>;
};

export const dynamic = "force-dynamic";
export const runtime = "nodejs";

async function proxyRustZap(request: NextRequest, context: RouteContext) {
  const { path } = await context.params;
  const baseUrl = process.env.RUSTZAP_BASE_URL ?? "http://127.0.0.1:8167";
  const normalizedBase = baseUrl.endsWith("/") ? baseUrl : `${baseUrl}/`;
  const target = new URL(path.map(encodeURIComponent).join("/"), normalizedBase);
  target.search = request.nextUrl.search;

  const headers = new Headers();
  const contentType = request.headers.get("content-type");
  const accept = request.headers.get("accept");
  const idempotencyKey = request.headers.get("idempotency-key");

  if (contentType) {
    headers.set("content-type", contentType);
  }
  headers.set("accept", accept ?? "application/json");
  headers.set(
    "authorization",
    `Bearer ${process.env.RUSTZAP_PROJECT_API_KEY ?? "dev_project_key"}`
  );
  headers.set("x-rustzap-project-id", process.env.RUSTZAP_PROJECT_ID ?? "tetoz");
  headers.set("x-rustzap-company-id", process.env.RUSTZAP_COMPANY_ID ?? "company_dev");
  if (idempotencyKey) {
    headers.set("idempotency-key", idempotencyKey);
  }

  const init: RequestInit = {
    method: request.method,
    headers,
    cache: "no-store"
  };

  if (request.method !== "GET" && request.method !== "HEAD") {
    init.body = await request.arrayBuffer();
  }

  try {
    const response = await fetch(target, init);
    const body = await response.arrayBuffer();
    const responseHeaders = new Headers();
    const responseType = response.headers.get("content-type");

    if (responseType) {
      responseHeaders.set("content-type", responseType);
    }
    responseHeaders.set("cache-control", "no-store");

    return new NextResponse(body, {
      status: response.status,
      statusText: response.statusText,
      headers: responseHeaders
    });
  } catch (error) {
    const upstreamBaseUrl = normalizedBase.replace(/\/$/, "");
    return NextResponse.json(
      {
        error: {
          code: "RUSTZAP_PROXY_ERROR",
          message: `RustZap backend unavailable at ${upstreamBaseUrl}: ${
            error instanceof Error ? error.message : "request failed"
          }`,
          upstream_base_url: upstreamBaseUrl
        }
      },
      { status: 502 }
    );
  }
}

export {
  proxyRustZap as DELETE,
  proxyRustZap as GET,
  proxyRustZap as PATCH,
  proxyRustZap as POST,
  proxyRustZap as PUT
};
