import type { Metadata } from "next";
import { AppShell } from "@/components/app-shell";
import { RustZapProvider } from "@/components/rustzap-provider";
import "./globals.css";

export const metadata: Metadata = {
  title: "RustZap WhatsApp Tester",
  description: "Local WhatsApp Web style tester for RustZap"
};

export default function RootLayout({
  children
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body>
        <RustZapProvider>
          <AppShell>{children}</AppShell>
        </RustZapProvider>
      </body>
    </html>
  );
}
