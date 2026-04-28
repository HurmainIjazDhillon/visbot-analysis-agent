import "./globals.css";
import type { Metadata } from "next";

export const metadata: Metadata = {
  title: "VisBot Analysis",
  description: "Chat UI for intelligent asset analysis",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
