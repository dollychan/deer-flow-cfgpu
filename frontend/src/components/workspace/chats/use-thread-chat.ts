"use client";

import { useParams, usePathname, useSearchParams } from "next/navigation";
import { useEffect, useState } from "react";

import { uuid } from "@/core/utils/uuid";

export function useThreadChat() {
  const { thread_id: threadIdFromPath } = useParams<{ thread_id: string }>();
  const pathname = usePathname();

  const searchParams = useSearchParams();
  const [threadId, setThreadId] = useState(() => {
    return threadIdFromPath === "new" ? uuid() : threadIdFromPath;
  });

  const [isNewThread, setIsNewThread] = useState(
    () => threadIdFromPath === "new",
  );

  // history.replaceState (used in onStart after thread creation) updates
  // window.location but does NOT trigger Next.js router updates — useParams()
  // returns a stale "new" until a real navigation occurs.  After Fast Refresh
  // the component remounts and re-reads the stale param, losing the real thread
  // ID.  Recover it from window.location once on mount (client-side only, so
  // no hydration mismatch).
  useEffect(() => {
    if (threadIdFromPath !== "new") return;
    const urlMatch = window.location.pathname.match(/\/chats\/([^/]+)$/);
    const urlId = urlMatch?.[1];
    if (urlId && urlId !== "new") {
      setThreadId(urlId);
      setIsNewThread(false);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []); // intentionally run once on mount only

  useEffect(() => {
    if (pathname.endsWith("/new")) {
      setIsNewThread(true);
      setThreadId(uuid());
      return;
    }
    // Guard: after history.replaceState updates the URL from /chats/new to
    // /chats/{UUID}, Next.js useParams may still return the stale "new" value
    // because replaceState does not trigger router updates.  Avoid propagating
    // this invalid thread ID to downstream hooks (e.g. useStream), which would
    // cause a 422 from LangGraph Server.
    if (threadIdFromPath === "new") {
      return;
    }
    setIsNewThread(false);
    setThreadId(threadIdFromPath);
  }, [pathname, threadIdFromPath]);
  const isMock = searchParams.get("mock") === "true";
  return { threadId, setThreadId, isNewThread, setIsNewThread, isMock };
}
