#!/usr/bin/env node
// Final two-request Garmin capability probe. Credentials are never persisted.

import fs from "node:fs";
import { spawnSync } from "node:child_process";

const WS_URL =
  process.argv.find((arg) => arg.startsWith("--ws-url="))?.split("=")[1] ??
  "ws://192.168.4.22:8123/api/websocket";
const LEDGER_PATH =
  process.argv.find((arg) => arg.startsWith("--ledger="))?.split("=")[1] ??
  "/home/js/garmin-capability-audit-ledger.json";
const SSH_HOST =
  process.argv.find((arg) => arg.startsWith("--ssh-host="))?.split("=")[1] ??
  "k2s";
const DELAY_MS = Number.parseInt(
  process.argv.find((arg) => arg.startsWith("--delay-ms="))?.split("=")[1] ??
    "60000",
  10,
);
const REFRESH_TIMEOUT_MS = Number.parseInt(
  process.argv
    .find((arg) => arg.startsWith("--refresh-timeout-ms="))
    ?.split("=")[1] ?? "360000",
  10,
);
const DRY_RUN = process.argv.includes("--dry-run");
const SKIP_REFRESH_WAIT = process.argv.includes("--skip-refresh-wait");

const REFRESH_MARKER_ENTITIES = [
  "sensor.garmin_connect_last_synced",
  "sensor.garmin_connect_last_activity",
  "sensor.garmin_connect_training_status",
];

let nextMessageId = 1;

function taipeiDate() {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: "Asia/Taipei",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts(new Date());
  const values = Object.fromEntries(parts.map(({ type, value }) => [type, value]));
  return `${values.year}-${values.month}-${values.day}`;
}

function wait(milliseconds) {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

function readLedger() {
  if (!fs.existsSync(LEDGER_PATH)) {
    return { version: 2, requests: {}, decisions: {}, local_evidence: {} };
  }
  const ledger = JSON.parse(fs.readFileSync(LEDGER_PATH, "utf8"));
  ledger.version = Math.max(ledger.version ?? 1, 2);
  ledger.requests ??= {};
  ledger.decisions ??= {};
  ledger.local_evidence ??= {};
  return ledger;
}

function saveLedger(ledger) {
  const temporaryPath = `${LEDGER_PATH}.tmp`;
  fs.writeFileSync(temporaryPath, `${JSON.stringify(ledger, null, 2)}\n`, {
    mode: 0o600,
  });
  fs.chmodSync(temporaryPath, 0o600);
  fs.renameSync(temporaryPath, LEDGER_PATH);
  fs.chmodSync(LEDGER_PATH, 0o600);
}

function readSecret(prompt) {
  return new Promise((resolve, reject) => {
    process.stderr.write(prompt);
    const input = process.stdin;
    let value = "";
    input.setEncoding("utf8");
    if (input.isTTY) input.setRawMode(true);
    input.resume();
    const onData = (chunk) => {
      for (const char of chunk) {
        if (char === "\u0003") {
          cleanup();
          reject(new Error("已取消"));
          return;
        }
        if (char === "\r" || char === "\n") {
          cleanup();
          process.stderr.write("\n");
          resolve(value);
          return;
        }
        if (char === "\u007f" || char === "\b") {
          value = value.slice(0, -1);
        } else {
          value += char;
        }
      }
    };
    const cleanup = () => {
      input.off("data", onData);
      if (input.isTTY) input.setRawMode(false);
      input.pause();
    };
    input.on("data", onData);
  });
}

function connect(token) {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(WS_URL);
    const timeout = setTimeout(() => {
      ws.close();
      reject(new Error("连接超时"));
    }, 30000);
    ws.addEventListener("message", (event) => {
      const message = JSON.parse(event.data);
      if (message.type === "auth_required") {
        ws.send(JSON.stringify({ type: "auth", access_token: token }));
      } else if (message.type === "auth_ok") {
        clearTimeout(timeout);
        resolve(ws);
      } else if (message.type === "auth_invalid") {
        clearTimeout(timeout);
        reject(new Error("认证失败"));
      }
    });
    ws.addEventListener("error", () => reject(new Error("WebSocket 连接失败")));
  });
}

function callWebSocket(ws, payload, label, timeoutMs = 60000) {
  return new Promise((resolve, reject) => {
    const id = nextMessageId++;
    const timeout = setTimeout(() => {
      ws.removeEventListener("message", onMessage);
      reject(new Error(`${label} 响应超时`));
    }, timeoutMs);
    const onMessage = (event) => {
      const message = JSON.parse(event.data);
      if (message.id !== id) return;
      clearTimeout(timeout);
      ws.removeEventListener("message", onMessage);
      if (!message.success) {
        reject(new Error(`${label}: ${JSON.stringify(message.error || {})}`));
        return;
      }
      resolve(message.result?.response ?? message.result);
    };
    ws.addEventListener("message", onMessage);
    ws.send(JSON.stringify({ id, ...payload }));
  });
}

function getStates(ws) {
  return callWebSocket(ws, { type: "get_states" }, "读取 HA 状态", 30000);
}

function callService(ws, service, serviceData) {
  return callWebSocket(
    ws,
    {
      type: "call_service",
      domain: "garmin_connect",
      service,
      service_data: serviceData,
      return_response: true,
    },
    `${service}`,
  );
}

function stateMap(states) {
  return new Map(states.map((state) => [state.entity_id, state]));
}

function reportMarker(state) {
  return state?.last_reported ?? state?.last_updated ?? null;
}

async function waitForRegularRefresh(ws) {
  if (SKIP_REFRESH_WAIT) return;

  const baseline = stateMap(await getStates(ws));
  const baselineMarkers = new Map(
    REFRESH_MARKER_ENTITIES.map((entityId) => [
      entityId,
      reportMarker(baseline.get(entityId)),
    ]),
  );
  if ([...baselineMarkers.values()].some((value) => value === null)) {
    throw new Error("缺少 Garmin 刷新标记实体；未发送任何 Garmin 请求");
  }

  process.stderr.write("等待 Garmin 核心、活动和训练实体完成下一轮常规刷新…\n");
  const deadline = Date.now() + REFRESH_TIMEOUT_MS;
  while (Date.now() < deadline) {
    await wait(5000);
    const current = stateMap(await getStates(ws));
    const refreshed = REFRESH_MARKER_ENTITIES.every(
      (entityId) =>
        reportMarker(current.get(entityId)) !== baselineMarkers.get(entityId),
    );
    if (refreshed) {
      process.stderr.write("检测到常规刷新完成；等待 10 秒后进入探测窗口。\n");
      await wait(10000);
      return;
    }
  }
  throw new Error("等待常规刷新超时；未发送任何 Garmin 请求");
}

function activityId(activity) {
  const value = activity?.activityId ?? activity?.activity_id;
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) ? parsed : null;
}

function activityType(activity) {
  const value =
    activity?.activityType?.typeKey ??
    activity?.activityType?.type_key ??
    activity?.activityType ??
    activity?.activity_type;
  return typeof value === "string" ? value : "unknown";
}

function activityDate(activity) {
  const value =
    activity?.startTimeLocal ??
    activity?.startTimeGMT ??
    activity?.start_time_local ??
    activity?.start_time_gmt;
  return typeof value === "string" && /^\d{4}-\d{2}-\d{2}/.test(value)
    ? value.slice(0, 10)
    : null;
}

function scoreActivity(activity) {
  const keys = new Set(Object.keys(activity ?? {}));
  const type = activityType(activity);
  const preferredType = /(running|cycling|walking|hiking)/i.test(type) ? 20 : 0;
  const richFieldPatterns = [
    /heart/i,
    /temperature/i,
    /training.*effect/i,
    /training.*load/i,
    /cadence/i,
    /speed/i,
    /power/i,
    /latitude|longitude|polyline/i,
    /recovery/i,
  ];
  const richness = richFieldPatterns.filter((pattern) =>
    [...keys].some((key) => pattern.test(key)),
  ).length;
  return preferredType + richness;
}

function selectActivity(states) {
  const statesById = stateMap(states);
  const recent =
    statesById.get("sensor.garmin_connect_last_activities")?.attributes
      ?.last_activities ?? [];
  const last =
    statesById.get("sensor.garmin_connect_last_activity")?.attributes ?? {};
  const candidates = [...recent, last]
    .filter((activity) => activityId(activity) !== null)
    .sort(
      (left, right) =>
        scoreActivity(right) - scoreActivity(left) ||
        activityId(right) - activityId(left),
    );
  if (candidates.length === 0) {
    throw new Error("HA 本地状态没有可用活动 ID；未请求 Garmin 活动列表");
  }

  const selected = candidates[0];
  const fieldNames = Object.keys(selected).sort();
  return {
    id: activityId(selected),
    date: activityDate(selected),
    type: activityType(selected),
    field_names: fieldNames,
    recovery_field_names: fieldNames.filter((name) => /recovery/i.test(name)),
    training_field_names: fieldNames.filter((name) =>
      /training|vo2|max.*load/i.test(name),
    ),
  };
}

function secureFitFile(filePath) {
  if (!/^\/config\/garmin_activities\/activity_\d+\.fit$/.test(filePath)) {
    throw new Error(`拒绝修改非预期 FIT 路径：${filePath}`);
  }
  const chmod = spawnSync(
    "ssh",
    [
      SSH_HOST,
      "docker",
      "exec",
      "homeassistant",
      "chmod",
      "600",
      filePath,
    ],
    { encoding: "utf8" },
  );
  if (chmod.status !== 0) {
    throw new Error(`设置 FIT 权限失败：${chmod.stderr.trim()}`);
  }
  const statResult = spawnSync(
    "ssh",
    [
      SSH_HOST,
      "docker",
      "exec",
      "homeassistant",
      "stat",
      "-c",
      "%a",
      filePath,
    ],
    { encoding: "utf8" },
  );
  if (statResult.status !== 0 || statResult.stdout.trim() !== "600") {
    throw new Error("FIT 文件权限验证失败");
  }
}

function requestMustStop(response) {
  const error = response?.result?.error ?? "";
  const errorType = response?.result?.error_type ?? "";
  return /429|rate.?limit|auth|401|403|server error|HTTP 5\d\d/i.test(
    `${errorType} ${error}`,
  );
}

function readinessIsEmpty(response) {
  const result = response?.result;
  if (result?.ok !== true) return null;
  const shape = result.shape ?? {};
  if (shape.type === "object") return shape.key_count === 0;
  if (shape.type === "array") return shape.length === 0;
  return shape.non_null === false;
}

function safeFailure(error) {
  return {
    occurred_at: new Date().toISOString(),
    error_type: error?.constructor?.name ?? "Error",
    message: String(error?.message ?? error).slice(0, 500),
  };
}

const targetDate = taipeiDate();
if (DRY_RUN) {
  process.stdout.write(
    `${JSON.stringify(
      {
        phase: "final-first-batch",
        hard_request_limit: 2,
        changes_scan_interval: false,
        reloads_integration: false,
        requests: [
          {
            service: "garmin_connect.download_activity",
            source: "HA local last_activity/last_activities",
            format: "fit",
          },
          {
            service: "garmin_connect.probe_capability",
            probe: "training_readiness",
            date: targetDate,
            final_attempt: true,
          },
        ],
        delay_seconds: DELAY_MS / 1000,
      },
      null,
      2,
    )}\n`,
  );
  process.exit(0);
}

const ledger = readLedger();
const token = (await readSecret("新的临时 Home Assistant Token（输入不回显）: ")).trim();
if (!token) throw new Error("Token 不能为空");

const ws = await connect(token);
let completedRequests = 0;
try {
  const states = await getStates(ws);
  const selectedActivity = selectActivity(states);
  ledger.local_evidence[`activity_${selectedActivity.id}`] = {
    observed_at: new Date().toISOString(),
    source: "home_assistant_state",
    ...selectedActivity,
  };
  saveLedger(ledger);

  const fitKey = `download_fit|${selectedActivity.id}|fit`;
  const readinessKey = `training_readiness_final|${targetDate}`;
  const fitPending =
    !ledger.decisions.fit_sample && !Object.hasOwn(ledger.requests, fitKey);
  const readinessPending =
    !ledger.decisions.training_readiness &&
    !Object.hasOwn(ledger.requests, readinessKey);

  process.stderr.write(
    `最终阶段待执行 ${Number(fitPending) + Number(readinessPending)} 项，硬上限 2 项。\n`,
  );
  if (!fitPending && !readinessPending) {
    process.stderr.write("所有最终探测均已完成；本次 Garmin 请求为 0。\n");
  } else {
    await waitForRegularRefresh(ws);

    if (fitPending) {
      process.stderr.write(
        `[1/2] 下载一份 FIT：activity_id=${selectedActivity.id}，type=${selectedActivity.type}\n`,
      );
      const response = await callService(ws, "download_activity", {
        activity_id: selectedActivity.id,
        file_format: "fit",
      });
      ledger.requests[fitKey] = {
        requested_at: new Date().toISOString(),
        request: {
          service: "download_activity",
          activity_id: selectedActivity.id,
          file_format: "fit",
        },
        response,
      };
      saveLedger(ledger);
      completedRequests += 1;

      if (requestMustStop(response)) {
        throw new Error("FIT 请求返回认证、限流或服务端异常");
      }
      secureFitFile(response.file_path);
      ledger.decisions.fit_sample = {
        status: "retained",
        activity_id: selectedActivity.id,
        activity_date: selectedActivity.date,
        activity_type: selectedActivity.type,
        file_path: response.file_path,
        size_bytes: response.size_bytes,
        mode: "0600",
        request_key: fitKey,
      };
      saveLedger(ledger);
    }

    if (readinessPending) {
      if (completedRequests > 0) await wait(DELAY_MS);
      process.stderr.write(`[2/2] 最终训练准备度验证：${targetDate}\n`);
      const response = await callService(ws, "probe_capability", {
        probe: "training_readiness",
        date: targetDate,
      });
      ledger.requests[readinessKey] = {
        requested_at: new Date().toISOString(),
        request: {
          service: "probe_capability",
          probe: "training_readiness",
          date: targetDate,
          final_attempt: true,
        },
        response,
      };
      saveLedger(ledger);
      completedRequests += 1;

      if (requestMustStop(response)) {
        throw new Error("训练准备度请求返回认证、限流或服务端异常");
      }
      const isEmpty = readinessIsEmpty(response);
      ledger.decisions.training_readiness = {
        status:
          isEmpty === true
            ? "excluded_fr255_unsupported"
            : isEmpty === false
              ? "included"
              : "inconclusive_no_automatic_retry",
        final_probe_date: targetDate,
        request_key: readinessKey,
      };
      saveLedger(ledger);
    }
  }
} catch (error) {
  ledger.failures ??= [];
  ledger.failures.push(safeFailure(error));
  saveLedger(ledger);
  process.stderr.write(`已停止：${error.message}\n`);
  process.exitCode = 2;
} finally {
  ws.close();
}

process.stderr.write(
  `本次 Garmin 请求 ${completedRequests} 项；账本：${LEDGER_PATH}\n`,
);
