import type { PendingToolCall } from "./tool-approval";

// Store on `window` so data survives Turbopack/webpack HMR module re-evaluations.
// Module-level variables are re-initialized when HMR replaces this module;
// `window` properties persist for the entire page lifetime.
// Single-slot design: only one HIL approval is pending at a time per page.
declare global {
  interface Window {
    __deerflow_pending_approvals?: PendingToolCall[] | null;
  }
}

export const approvalStore = {
  get(): PendingToolCall[] | null {
    if (typeof window === "undefined") return null;
    return window.__deerflow_pending_approvals ?? null;
  },
  set(approvals: PendingToolCall[]): void {
    if (typeof window !== "undefined") {
      window.__deerflow_pending_approvals = approvals;
    }
  },
  clear(): void {
    if (typeof window !== "undefined") {
      window.__deerflow_pending_approvals = null;
    }
  },
};
