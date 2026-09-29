import type { Channel } from "./types";
import { labels, reasons, settingNames } from "./i18n";
import { milliseconds, number } from "./reports";

export function ChannelView({
  channel,
  observed,
}: {
  channel: Channel;
  observed: number;
}) {
  const { metrics, fabric } = channel;
  return (
    <>
      <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
        {[
          [labels.confirmed, number(metrics.acknowledged)],
          [labels.retries, number(metrics.retransmissions)],
          [labels.window, `${number(fabric.window_bytes)} B`],
          [labels.pending, `${number(fabric.in_flight_bytes)} B`],
        ].map(([name, value]) => (
          <div key={name} className="panel">
            <p className="caption">{name}</p>
            <p className="mt-2 text-2xl font-medium tabular-nums">{value}</p>
          </div>
        ))}
      </div>
      <section className="mt-7">
        <h2>{labels.paths}</h2>
        <div className="mt-3 grid gap-3 md:grid-cols-2">
          {fabric.paths.map((path) => (
            <article className="panel" key={path.path}>
              <div className="flex items-center justify-between">
                <h3 className="font-medium">
                  {labels.path} {path.path}
                </h3>
                <span
                  className={
                    path.disabled_until_us > observed
                      ? "badge warning"
                      : "badge"
                  }
                >
                  {number(path.sent)} {labels.sent}
                </span>
              </div>
              <div className="my-5 flex items-center gap-3 text-xs text-slate-500">
                <span>{labels.source}</span>
                <div className="h-px grow bg-indigo-200" />
                <span className="rounded-md border border-indigo-200 bg-indigo-50 px-4 py-2 text-indigo-700">
                  {path.signal.node
                    ? `${labels.bottleneck} ${path.signal.node}`
                    : `${labels.path} ${path.path}`}
                </span>
                <div className="h-px grow bg-indigo-200" />
                <span>{labels.destination}</span>
              </div>
              <dl className="grid grid-cols-2 gap-x-5 gap-y-3 text-sm">
                {[
                  [labels.rtt, milliseconds(path.rtt_us)],
                  [labels.queue, milliseconds(path.signal.queue_us)],
                  [
                    labels.capacity,
                    path.signal.capacity_bytes_per_second
                      ? `${((path.signal.capacity_bytes_per_second * 8) / 1e6).toFixed(2)} Mbps`
                      : labels.unavailable,
                  ],
                  [labels.nacks, number(path.nacks)],
                  [labels.timeout, number(path.timeouts)],
                  [labels.confirmed, number(path.acknowledged)],
                ].map(([name, value]) => (
                  <div key={name} className="flex justify-between gap-2">
                    <dt className="text-slate-500">{name}</dt>
                    <dd className="tabular-nums">{value}</dd>
                  </div>
                ))}
              </dl>
            </article>
          ))}
        </div>
      </section>
      <section className="mt-7">
        <h2>{labels.settings}</h2>
        <div className="mt-3 flex flex-wrap gap-2">
          {Object.entries(settingNames).map(([key, name]) => (
            <span
              className={`badge ${fabric.settings[key] ? "" : "muted"}`}
              key={key}
            >
              {name} · {fabric.settings[key] ? labels.enabled : labels.disabled}
            </span>
          ))}
        </div>
      </section>
      <section className="mt-7">
        <h2>{labels.events}</h2>
        <div className="mt-3 overflow-x-auto rounded-xl border border-slate-200">
          <table className="w-full text-left text-sm">
            <thead>
              <tr>
                {[
                  labels.time,
                  labels.path,
                  labels.change,
                  labels.window,
                  labels.queue,
                ].map((name) => (
                  <th key={name}>{name}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {[...fabric.events]
                .reverse()
                .slice(0, 40)
                .map((event, index) => (
                  <tr key={`${event.at_us}-${index}`}>
                    <td className="font-mono text-xs">
                      {(event.at_us / 1e6).toFixed(3)} s
                    </td>
                    <td>{event.path}</td>
                    <td>{reasons[event.reason] ?? event.reason}</td>
                    <td>{number(event.window_bytes)} B</td>
                    <td>{milliseconds(event.signal.queue_us)}</td>
                  </tr>
                ))}
            </tbody>
          </table>
          {fabric.events.length === 0 && (
            <p className="p-4 text-slate-500">{labels.noEvents}</p>
          )}
        </div>
      </section>
    </>
  );
}
