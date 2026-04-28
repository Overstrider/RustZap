"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import type { ReactNode } from "react";
import { useRustZap } from "@/components/rustzap-provider";
import { StatusPill } from "@/components/status-pill";

const links = [
  { href: "/dashboard", label: "Dashboard" },
  { href: "/channel", label: "Channel" },
  { href: "/chat", label: "Chat" },
  { href: "/groups", label: "Groups" },
  { href: "/media", label: "Media" },
  { href: "/events", label: "Events" }
];

export function AppShell({ children }: { children: ReactNode }) {
  const pathname = usePathname();
  const { state, actions } = useRustZap();

  return (
    <div className="app-shell">
      <header className="topbar">
        <div className="brand">
          <strong>RustZap Tester</strong>
          <span>
            {state.projects[0]?.id ?? "tetoz"} / {state.companies[0]?.id ?? "company_dev"}
          </span>
        </div>
        <nav className="nav" aria-label="Primary navigation">
          {links.map((link) => (
            <Link
              className={pathname === link.href ? "active" : undefined}
              href={link.href}
              key={link.href}
            >
              {link.label}
            </Link>
          ))}
        </nav>
        <div className="mode-toggle">
          <StatusPill value={state.channel.status} />
          <select
            aria-label="API mode"
            value={state.mode}
            onChange={(event) => actions.setMode(event.target.value === "real" ? "real" : "mock")}
          >
            <option value="mock">mock</option>
            <option value="real">real</option>
          </select>
        </div>
      </header>
      <main>{children}</main>
    </div>
  );
}
