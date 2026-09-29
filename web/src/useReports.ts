import { useEffect, useState } from "react";
import { parseSnapshots, parseReport } from "./reports";
import { labels } from "./i18n";
import type { Snapshot } from "./types";

// 実験ループの10Hz記録に対し、ブラウザの読み込みは1Hzにする。
const POLL_MS = 1000;
export function useReports() {
  const [snapshots, setSnapshots] = useState<Snapshot[]>([]);
  const [paused, setPaused] = useState(false);
  const [error, setError] = useState("");
  useEffect(() => {
    if (paused) return;
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout>;
    async function refresh() {
      try {
        const response = await fetch("/api/reports", {
          signal: controller.signal,
          cache: "no-store",
        });
        if (!response.ok) throw new Error(labels.error);
        const next = parseSnapshots(await response.json());
        if (!controller.signal.aborted) {
          setSnapshots(next);
          setError("");
        }
      } catch (failure) {
        if (!controller.signal.aborted)
          setError(failure instanceof Error ? failure.message : labels.error);
      } finally {
        if (!controller.signal.aborted) timer = setTimeout(refresh, POLL_MS);
      }
    }
    void refresh();
    return () => {
      controller.abort();
      clearTimeout(timer);
    };
  }, [paused]);
  async function importFile(file: File) {
    try {
      if (file.size > 4 * 1024 * 1024) throw new Error(labels.invalid);
      const report = parseReport(JSON.parse(await file.text()));
      setSnapshots([{ name: file.name, modified: Date.now() / 1000, report }]);
      setPaused(true);
      setError("");
    } catch {
      setError(labels.invalid);
    }
  }
  return { snapshots, paused, setPaused, error, importFile };
}
