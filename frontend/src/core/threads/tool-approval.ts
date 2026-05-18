export interface PendingToolCall {
  id: string;
  name: string;
  args: Record<string, unknown>;
}

export interface ToolApprovalDecision {
  status: "approved" | "rejected";
  args?: Record<string, unknown>;
  reason?: string;
}

export type ToolApprovals = Record<string, ToolApprovalDecision>;
