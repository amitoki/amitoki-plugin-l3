import { useState } from "react";
import { ChannelView } from "./ChannelView";
import { useReports } from "./useReports";
import { channels, number, channelKey, channelLabel } from "./reports";
import { labels } from "./i18n";

export default function App() {
  const { snapshots, paused, setPaused, error, importFile } = useReports();
  const [selected, setSelected] = useState("");
  const [selectedChannel, setSelectedChannel] = useState("");
  const snapshot =
    snapshots.find((item) => item.name === selected) ??
    snapshots.find((item) => channels(item.report).length > 0) ??
    snapshots[0];
  const entries = snapshot ? channels(snapshot.report) : [];
  const channel =
    entries.find(
      (entry, index) => channelKey(entry, index) === selectedChannel,
    ) ?? entries[0];
  const state = paused
    ? labels.paused
    : snapshot?.name.endsWith(".report.json")
      ? labels.saved
      : snapshot && Date.now() / 1000 - snapshot.modified > 3
        ? labels.stale
        : labels.live;
  return (
    <>
      <header className="flex h-12 items-center justify-between border-b border-slate-200 px-5 md:px-9">
        <div className="flex items-center gap-3">
          <span className="font-semibold tracking-tight">{labels.title}</span>
          <span className="text-slate-300">/</span>
          <span className="text-sm text-slate-500">{labels.section}</span>
        </div>
        <span className="text-xs text-slate-500">{state}</span>
      </header>
      <main className="mx-auto max-w-6xl px-5 py-7 md:px-9">
        <div className="mb-6 flex flex-wrap items-center justify-between gap-3">
          <div className="flex flex-wrap gap-2">
            <select
              aria-label={labels.node}
              value={snapshot?.name ?? ""}
              onChange={(event) => {
                setSelected(event.target.value);
                setSelectedChannel("");
              }}
            >
              {snapshots.map((item) => (
                <option key={item.name} value={item.name}>
                  {labels.node} {item.report.node} · {item.name}
                </option>
              ))}
            </select>
            {entries.map((entry, index) => (
              <button
                aria-pressed={entry === channel}
                key={channelKey(entry, index)}
                onClick={() => setSelectedChannel(channelKey(entry, index))}
              >
                {channelLabel(entry, index)}
              </button>
            ))}
          </div>
          <div className="flex gap-2">
            <label className="button cursor-pointer">
              {labels.load}
              <input
                aria-label={labels.load}
                type="file"
                accept=".json,application/json"
                className="sr-only"
                onChange={(event) => {
                  const file = event.target.files?.[0];
                  if (file) void importFile(file);
                }}
              />
            </label>
            <button onClick={() => setPaused(!paused)}>
              {paused ? labels.resume : labels.pause}
            </button>
          </div>
        </div>
        {error && (
          <p
            role="alert"
            className="mb-4 rounded-lg bg-amber-50 p-3 text-sm text-amber-800"
          >
            {error}
          </p>
        )}
        {channel && snapshot ? (
          <ChannelView
            channel={channel}
            observed={snapshot.report.observed_us}
          />
        ) : snapshot ? (
          <div className="panel flex gap-8">
            {[
              [labels.sent, snapshot.report.network.sent],
              [labels.trimmed, snapshot.report.network.trimmed],
            ].map(([name, value]) => (
              <p key={name}>
                {name} <strong className="ml-3">{number(Number(value))}</strong>
              </p>
            ))}
          </div>
        ) : (
          <p className="py-24 text-center text-slate-500">{labels.empty}</p>
        )}
      </main>
    </>
  );
}
