"use client";

import { CheckIcon, XIcon } from "lucide-react";
import { useCallback, useState } from "react";

import { Button } from "@/components/ui/button";
import type {
  PendingToolCall,
  ToolApprovalDecision,
  ToolApprovals,
} from "@/core/threads/tool-approval";
import { cn } from "@/lib/utils";

interface ToolCallCardState {
  decision: "approved" | "rejected" | null;
  argsJson: string;
  reason: string;
  argsError: boolean;
}

function parseArgsJson(
  json: string,
): Record<string, unknown> | null {
  try {
    const parsed = JSON.parse(json) as unknown;
    if (typeof parsed === "object" && parsed !== null && !Array.isArray(parsed)) {
      return parsed as Record<string, unknown>;
    }
    return null;
  } catch {
    return null;
  }
}

function ToolCallCard({
  toolCall,
  state,
  onChange,
}: {
  toolCall: PendingToolCall;
  state: ToolCallCardState;
  onChange: (state: ToolCallCardState) => void;
}) {
  return (
    <div
      className={cn(
        "rounded-xl border p-3 transition-colors",
        state.decision === "approved" && "border-green-500/40 bg-green-500/5",
        state.decision === "rejected" && "border-red-500/40 bg-red-500/5",
        state.decision === null && "border-border bg-background/50",
      )}
    >
      <div className="mb-2 flex items-center justify-between gap-2">
        <span className="font-mono text-xs font-semibold text-foreground/80">
          {toolCall.name}
        </span>
        <div className="flex gap-1.5">
          <Button
            size="sm"
            variant={state.decision === "approved" ? "default" : "outline"}
            className={cn(
              "h-6 gap-1 px-2 text-xs",
              state.decision === "approved" &&
                "bg-green-600 hover:bg-green-700",
            )}
            onClick={() =>
              onChange({ ...state, decision: "approved", argsError: false })
            }
          >
            <CheckIcon className="size-3" />
            批准
          </Button>
          <Button
            size="sm"
            variant={state.decision === "rejected" ? "destructive" : "outline"}
            className="h-6 gap-1 px-2 text-xs"
            onClick={() =>
              onChange({ ...state, decision: "rejected", argsError: false })
            }
          >
            <XIcon className="size-3" />
            拒绝
          </Button>
        </div>
      </div>

      <textarea
        className={cn(
          "w-full resize-y rounded-lg border bg-background/80 p-2 font-mono text-xs leading-relaxed outline-none",
          "focus:ring-1 focus:ring-ring",
          state.argsError ? "border-red-500" : "border-border/60",
          state.decision === "rejected" && "opacity-50",
        )}
        rows={Math.min(8, state.argsJson.split("\n").length + 1)}
        value={state.argsJson}
        disabled={state.decision === "rejected"}
        onChange={(e) => {
          const newJson = e.target.value;
          onChange({
            ...state,
            argsJson: newJson,
            argsError: parseArgsJson(newJson) === null,
          });
        }}
      />
      {state.argsError && (
        <p className="mt-1 text-xs text-red-500">JSON 格式错误</p>
      )}

      {state.decision === "rejected" && (
        <input
          className="mt-2 w-full rounded-lg border border-border/60 bg-background/80 px-2 py-1.5 text-xs outline-none focus:ring-1 focus:ring-ring"
          placeholder="拒绝原因（可选）"
          value={state.reason}
          onChange={(e) => onChange({ ...state, reason: e.target.value })}
        />
      )}
    </div>
  );
}

export function ToolApprovalPanel({
  toolCalls,
  onSubmit,
}: {
  toolCalls: PendingToolCall[];
  onSubmit: (approvals: ToolApprovals) => void;
}) {
  const [cardStates, setCardStates] = useState<ToolCallCardState[]>(() =>
    toolCalls.map((tc) => ({
      decision: null,
      argsJson: JSON.stringify(tc.args, null, 2),
      reason: "",
      argsError: false,
    })),
  );

  const decidedCount = cardStates.filter((s) => s.decision !== null).length;
  const allDecided = decidedCount === toolCalls.length;
  const hasErrors = cardStates.some((s) => s.argsError);

  const handleApproveAll = useCallback(() => {
    setCardStates((prev) =>
      prev.map((s) => ({ ...s, decision: "approved" as const, argsError: false })),
    );
  }, []);

  const handleSubmit = useCallback(() => {
    const approvals: ToolApprovals = {};
    for (let i = 0; i < toolCalls.length; i++) {
      const tc = toolCalls[i]!;
      const s = cardStates[i]!;
      if (s.decision === null) continue;

      const decision: ToolApprovalDecision =
        s.decision === "approved"
          ? {
              status: "approved",
              args: parseArgsJson(s.argsJson) ?? tc.args,
            }
          : {
              status: "rejected",
              ...(s.reason.trim() ? { reason: s.reason.trim() } : {}),
            };
      approvals[tc.id] = decision;
    }
    onSubmit(approvals);
  }, [toolCalls, cardStates, onSubmit]);

  return (
    <div className="rounded-2xl border bg-background/80 backdrop-blur-sm">
      <div className="flex items-center justify-between border-b px-4 py-2.5">
        <div>
          <p className="text-sm font-medium">工具调用待审批</p>
          <p className="text-muted-foreground text-xs">
            AI 请求执行以下工具，请确认后继续
          </p>
        </div>
        <Button
          size="sm"
          variant="outline"
          className="h-7 px-2.5 text-xs"
          onClick={handleApproveAll}
        >
          全部批准
        </Button>
      </div>

      <div className="flex flex-col gap-2 p-3">
        {toolCalls.map((tc, i) => (
          <ToolCallCard
            key={tc.id}
            toolCall={tc}
            state={cardStates[i]!}
            onChange={(s) =>
              setCardStates((prev) => prev.map((x, j) => (j === i ? s : x)))
            }
          />
        ))}
      </div>

      <div className="flex items-center justify-between border-t px-4 py-2.5">
        <span className="text-muted-foreground text-xs">
          {decidedCount}/{toolCalls.length} 已决定
        </span>
        <Button
          size="sm"
          disabled={!allDecided || hasErrors}
          onClick={handleSubmit}
        >
          提交决策
        </Button>
      </div>
    </div>
  );
}
