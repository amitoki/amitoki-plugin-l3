export interface Signal {
  node: number;
  queue_us: number;
  available_bytes_per_second: number;
  capacity_bytes_per_second: number;
}
export interface PathState {
  path: number;
  sent: number;
  acknowledged: number;
  timeouts: number;
  nacks: number;
  rtt_us: number;
  in_flight_bytes: number;
  disabled_until_us: number;
  signal: Signal;
}
export interface FabricEvent {
  at_us: number;
  path: number;
  reason: string;
  window_bytes: number;
  signal: Signal;
}
export interface Channel {
  channel?: number;
  class?: "short" | "bulk";
  destination?: number;
  state: string;
  metrics: {
    submitted: number;
    acknowledged: number;
    retransmissions: number;
    nacks: number;
  };
  fabric: {
    window_bytes: number;
    in_flight_bytes: number;
    settings: Record<string, boolean | number>;
    paths: PathState[];
    events: FabricEvent[];
    events_evicted: number;
  };
}
export interface Report {
  node: number;
  observed_us: number;
  channels?: Channel[];
  reliable_benchmark?: { channels: Channel[]; complete: boolean };
  network: {
    sent: number;
    received: number;
    trimmed: number;
    send_errors: number;
  };
}
export interface Snapshot {
  name: string;
  modified: number;
  report: Report;
}
