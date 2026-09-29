import { chromium } from "playwright";
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { mkdtemp, writeFile, rm, mkdir, copyFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { createInterface } from "node:readline";
import { once } from "node:events";
import { get } from "node:http";

const directory = await mkdtemp(join(tmpdir(), "amitoki-fabric-web-"));
const signal = {
  node: 11,
  queue_us: 2000,
  available_bytes_per_second: 0,
  capacity_bytes_per_second: 125000,
};
const channel = {
  state: "active",
  metrics: { submitted: 10, acknowledged: 9, retransmissions: 1, nacks: 1 },
  fabric: {
    window_bytes: 6040,
    in_flight_bytes: 190,
    settings: {
      adaptive_paths: true,
      telemetry: true,
      trimming: true,
      congestion_control: true,
      clock_independent: true,
    },
    paths: [1, 2].map((path) => ({
      path,
      sent: 5,
      acknowledged: 4,
      timeouts: 0,
      nacks: 1,
      rtt_us: 1000,
      in_flight_bytes: 190,
      disabled_until_us: 0,
      signal,
    })),
    events: [
      { at_us: 1000, path: 1, reason: "trimmed", window_bytes: 6040, signal },
    ],
    events_evicted: 0,
  },
};
const report = {
  node: 1,
  observed_us: 1000,
  network: { sent: 10, received: 10, trimmed: 0, send_errors: 0 },
  reliable_benchmark: { complete: false, channels: [channel, channel] },
};
await writeFile(join(directory, "a.report.json"), JSON.stringify(report));
const server = spawn(
  "python3",
  ["../scripts/observe.py", "--reports", directory, "--port", "0"],
  { stdio: ["ignore", "pipe", "inherit"] },
);
const lines = createInterface({ input: server.stdout });
let browser;
try {
  const [url] = await once(lines, "line");
  assert.equal(
    await new Promise((resolve, reject) =>
      get(
        url + "/api/reports",
        { headers: { Host: "untrusted.example" } },
        (response) => {
          response.resume();
          resolve(response.statusCode);
        },
      ).on("error", reject),
    ),
    403,
  );
  assert.equal(
    (
      await fetch(url + "/api/reports", {
        headers: { Origin: "https://untrusted.example" },
      })
    ).status,
    403,
  );
  assert.equal((await fetch(url + "/%2e%2e/package.json")).status, 404);
  browser = await chromium.launch({ headless: true });
  const page = await browser.newPage({
    viewport: { width: 1280, height: 1100 },
  });
  const failures = [];
  page.on("pageerror", (error) => failures.push(error.message));
  await page.goto(url);
  await page.getByRole("heading", { name: "経路", exact: true }).waitFor();
  assert.equal(
    await page.getByRole("heading", { name: /経路 [12]/ }).count(),
    2,
  );
  await page.getByRole("button", { name: "Bulk", exact: true }).click();
  assert.equal(
    await page
      .getByRole("button", { name: "Bulk", exact: true })
      .getAttribute("aria-pressed"),
    "true",
  );
  await page.getByRole("button", { name: "一時停止" }).click();
  await page.getByRole("button", { name: "更新を再開" }).waitFor();
  const invalid = join(directory, "invalid.json");
  await writeFile(
    invalid,
    JSON.stringify({
      ...report,
      reliable_benchmark: { channels: [{ fabric: { paths: [{}] } }] },
    }),
  );
  await page.getByLabel("JSONを開く").setInputFiles(invalid);
  await page.getByRole("alert").filter({ hasText: "形式が不正" }).waitFor();
  const screenshotReport = process.env.AMITOKI_FABRIC_REPORT;
  if (screenshotReport)
    await copyFile(resolve(screenshotReport), join(directory, "a.report.json"));
  await page
    .getByLabel("JSONを開く")
    .setInputFiles(join(directory, "a.report.json"));
  await page.getByRole("heading", { name: "経路", exact: true }).waitFor();
  const artifacts = resolve("../artifacts/web/2026-09-28");
  await mkdir(artifacts, { recursive: true });
  await page.screenshot({
    path: join(artifacts, "fabric.png"),
    fullPage: true,
  });
  await page.setViewportSize({ width: 390, height: 844 });
  assert.equal(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= window.innerWidth,
    ),
    true,
  );
  assert.deepEqual(failures, []);
  console.log(
    "Web: 経路表示・切替・停止・JSON入力・エラー・mobile・localhost制限を確認",
  );
} finally {
  await browser?.close();
  lines.close();
  server.kill("SIGTERM");
  await once(server, "exit");
  await rm(directory, { recursive: true, force: true });
}
