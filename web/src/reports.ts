import type { Channel, Report, Snapshot } from "./types";
import { labels } from "./i18n";

export function channels(report: Report): Channel[] {
  return report.channels ?? report.reliable_benchmark?.channels ?? [];
}
function object(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
function numbers(value: unknown, names: string[]): boolean {
  return (
    object(value) &&
    names.every(
      (name) =>
        typeof value[name] === "number" &&
        Number.isFinite(value[name]) &&
        (value[name] as number) >= 0,
    )
  );
}
function signal(value: unknown): boolean {
  return numbers(value, [
    "node",
    "queue_us",
    "available_bytes_per_second",
    "capacity_bytes_per_second",
  ]);
}
function validChannel(value: unknown): boolean {
  if (
    !object(value) ||
    !numbers(value.metrics, [
      "submitted",
      "acknowledged",
      "retransmissions",
      "nacks",
    ]) ||
    !object(value.fabric)
  )
    return false;
  const fabric = value.fabric;
  return (
    numbers(fabric, ["window_bytes", "in_flight_bytes", "events_evicted"]) &&
    object(fabric.settings) &&
    Array.isArray(fabric.paths) &&
    fabric.paths.length <= 8 &&
    fabric.paths.every(
      (path) =>
        numbers(path, [
          "path",
          "sent",
          "acknowledged",
          "timeouts",
          "nacks",
          "rtt_us",
          "in_flight_bytes",
          "disabled_until_us",
        ]) && signal(path.signal),
    ) &&
    Array.isArray(fabric.events) &&
    fabric.events.length <= 128 &&
    fabric.events.every(
      (event) =>
        numbers(event, ["at_us", "path", "window_bytes"]) &&
        typeof event.reason === "string" &&
        signal(event.signal),
    )
  );
}
export function parseReport(value: unknown): Report {
  const report = value as Partial<Report> | null;
  if (
    !report ||
    !Number.isInteger(report.node) ||
    !numbers(report, ["observed_us"]) ||
    !numbers(report.network, ["sent", "received", "trimmed", "send_errors"])
  )
    throw new Error(labels.invalid);
  const entries = report.channels ?? report.reliable_benchmark?.channels ?? [];
  if (
    !Array.isArray(entries) ||
    entries.length > 16 ||
    !entries.every(validChannel)
  )
    throw new Error(labels.invalid);
  return report as Report;
}
export function parseSnapshots(value: unknown): Snapshot[] {
  if (!Array.isArray(value) || value.length > 64)
    throw new Error(labels.invalid);
  return value.map((item) => {
    if (typeof item?.name !== "string" || typeof item?.modified !== "number")
      throw new Error(labels.invalid);
    return {
      name: item.name,
      modified: item.modified,
      report: parseReport(item.report),
    };
  });
}
export function number(value: number): string {
  return new Intl.NumberFormat("ja-JP").format(value);
}
export function milliseconds(value: number): string {
  return `${(value / 1000).toFixed(2)} ms`;
}

export function channelKey(channel: Channel, index: number): string {
  return `${channel.destination ?? 0}:${channel.channel ?? index + 1}`;
}
export function channelLabel(channel: Channel, index: number): string {
  const name =
    (channel.class ?? (index === 0 ? "short" : "bulk")) === "short"
      ? labels.short
      : labels.bulk;
  return channel.destination
    ? `${name} → ${labels.node} ${channel.destination}`
    : name;
}
