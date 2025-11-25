import { createHash } from "node:crypto";
import { hostname } from "node:os";

const IPFS_BASE = process.env.IPFS_BASE || "http://127.0.0.1:5001";
const TOPIC = process.env.PUBSUB_TOPIC || "SDT_Broadcast";
const PEER_ID = process.env.PEER_ID || hostname();
const HEARTBEAT_TIMEOUT_MS = Number(
  process.env.PEER_HEARTBEAT_TIMEOUT_MS || 15000
);
const HEARTBEAT_CHECK_MS = Number(
  process.env.PEER_HEARTBEAT_CHECK_MS || 2000
);

function encodeTopic(topic) {
  const txt = String(topic ?? "");
  const base64 = Buffer.from(txt, "utf8").toString("base64");
  const urlSafe = base64.replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
  return `u${urlSafe}`;
}

async function publishToTopic(message) {
  const payload = typeof message === "string" ? message : JSON.stringify(message);
  const form = new FormData();
  form.append("file", new Blob([payload], { type: "application/octet-stream" }));

  const pubUrl = `${IPFS_BASE}/api/v0/pubsub/pub?arg=${encodeTopic(TOPIC)}`;
  const resp = await fetch(pubUrl, { method: "POST", body: form });
  if (!resp.ok) {
    const body = await resp.text().catch(() => "");
    throw new Error(`Falha no pubsub pub: HTTP ${resp.status} ${body}`);
  }
  return payload;
}

const subUrl = new URL(`${IPFS_BASE}/api/v0/pubsub/sub`);
subUrl.searchParams.set("arg", encodeTopic(TOPIC));

console.log(`[peer] subscrever "${TOPIC}" em ${IPFS_BASE}`);
const resp = await fetch(subUrl, { method: "POST" });
if (!resp.ok) {
  const txt = await resp.text().catch(() => "");
  console.error("Falha no sub:", resp.status, txt);
  process.exit(1);
}

const reader = resp.body?.getReader?.();
if (!reader) {
  console.error("Stream do pubsub sem reader, encerrando.");
  process.exit(1);
}

startHeartbeatMonitor();

const decoder = new TextDecoder();
let buf = "";

const vectorVersions = [];
const pendingEmbeddings = new Map();
const faissIndex = new Map(); // simulacao em memoria
let lastHeartbeatAt = Date.now();
let leaderAlive = true;

function hashVector(cids = []) {
  const h = createHash("sha256");
  for (const cid of cids) {
    h.update(String(cid));
  }
  return h.digest("hex");
}

function summarizeEmbeddings(embeddings) {
  if (!Array.isArray(embeddings) || embeddings.length === 0) return null;
  const preview = embeddings.slice(0, 5).map((n) => Number(n.toFixed(4)));
  return { dimension: embeddings.length, preview };
}

function detectConflict(incomingVersion) {
  const latest = vectorVersions.at(-1);
  if (!incomingVersion || Number.isNaN(incomingVersion)) return "versao invalida";
  if (!latest) return null;
  if (incomingVersion <= latest.version) return `versao ${incomingVersion} <= local ${latest.version}`;
  if (incomingVersion !== latest.version + 1) return `esperava versao ${latest.version + 1}`;
  return null;
}

function buildVector(payload, latest) {
  if (Array.isArray(payload?.vector) && payload.vector.length) {
    return payload.vector.map((x) => String(x));
  }
  const v = [...(latest?.cids ?? [])];
  const cid = payload?.document?.cid ? String(payload.document.cid) : null;
  if (cid && !v.includes(cid)) v.push(cid);
  return v;
}

function storeIncomingVersion(payload, sender) {
  const incomingVersion = Number(payload?.vectorVersion ?? 0);
  const conflict = detectConflict(incomingVersion);
  if (conflict) return { status: "conflict", reason: conflict };

  const latest = vectorVersions.at(-1);
  const vector = buildVector(payload, latest);
  const documentCid = payload?.document?.cid ? String(payload.document.cid) : null;
  const entry = {
    version: incomingVersion,
    cids: vector,
    confirmed: false,
    documentCid,
    createdAt: payload?.createdAt || new Date().toISOString(),
    receivedFrom: sender,
  };
  vectorVersions.push(entry);

  const embeddings = Array.isArray(payload?.embeddings) ? payload.embeddings : [];
  pendingEmbeddings.set(incomingVersion, embeddings);
  const vectorHash = hashVector(vector);
  const info = summarizeEmbeddings(embeddings);
  console.log(
    `[peer] preparada versao ${incomingVersion} com ${vector.length} CIDs (cid novo: ${documentCid ?? "?"}).` +
    ` Embeddings: ${info ? `${info.dimension} dims` : "nenhum"}.`
  );
  return { status: "stored", entry, hash: vectorHash };
}

async function sendConfirmation(entry, vectorHash) {
  const confirmation = {
    type: "vector-confirmation",
    vectorVersion: entry.version,
    vectorHash,
    vectorLength: entry.cids.length,
    documentCid: entry.documentCid ?? null,
    peerId: PEER_ID,
    receivedFrom: entry.receivedFrom ?? null,
    createdAt: entry.createdAt,
  };
  await publishToTopic(confirmation);
  console.log(
    `[peer] confirmacao enviada da versao ${entry.version} (hash ${vectorHash.slice(0, 8)}...).`
  );
}

async function handleDocumentUpdate(payload, sender) {
  touchHeartbeat(sender ?? "leader");
  const result = storeIncomingVersion(payload, sender);
  if (result.status === "conflict") {
    console.warn(
      `[peer] conflito para versao ${payload?.vectorVersion ?? "?"}: ${result.reason}. Ignorando.`
    );
    return;
  }
  try {
    await sendConfirmation(result.entry, result.hash);
  } catch (err) {
    console.error(
      `[peer] falha ao enviar confirmacao da versao ${result.entry.version}:`,
      err
    );
  }
}

async function handleLeaderBlock(payload, sender) {
  touchHeartbeat(payload?.leaderId ?? sender ?? "leader");
  const updates = Array.isArray(payload?.updates) ? payload.updates : [];
  for (const update of updates) {
    await handleDocumentUpdate(update, payload?.leaderId ?? sender);
  }
}

function applyCommit(payload, sender) {
  touchHeartbeat(payload?.leaderId ?? sender ?? "leader");
  const version = Number(payload?.vectorVersion ?? 0);
  const vectorHash = payload?.vectorHash ? String(payload.vectorHash) : null;
  const entry = vectorVersions.find((v) => v.version === version);
  if (!entry) {
    console.warn(
      `[peer] commit recebido para versao ${version}, mas versao nao existe. De ${sender ?? "?"}`
    );
    return;
  }
  entry.confirmed = true;
  const embeddings = pendingEmbeddings.get(version) ?? [];
  faissIndex.set(version, {
    embeddings,
    vectorHash,
    updatedAt: new Date().toISOString(),
  });
  pendingEmbeddings.delete(version);
  console.log(
    `[peer] commit aplicado para versao ${version} (hash ${vectorHash?.slice?.(0, 8) ?? "n/a"}...), index FAISS em memoria atualizado.`
  );
}

function logConfirmation(message, sender) {
  const from = message?.peerId || sender || "peer";
  console.log(
    `[peer] confirmacao recebida de ${from} para versao ${message?.vectorVersion ?? "?"}` +
    `${message?.vectorHash ? ` (hash ${String(message.vectorHash).slice(0, 8)}...)` : ""}`
  );
}

function touchHeartbeat(source = "leader") {
  lastHeartbeatAt = Date.now();
  if (!leaderAlive) {
    leaderAlive = true;
    console.log(`[peer] heartbeat retomado do ${source}.`);
  }
}

function startHeartbeatMonitor() {
  setInterval(() => {
    const delta = Date.now() - lastHeartbeatAt;
    if (leaderAlive && delta > HEARTBEAT_TIMEOUT_MS) {
      leaderAlive = false;
      console.warn(
        `[peer] possivel falha do lider: ${Math.round(delta / 1000)}s sem heartbeat.`
      );
    }
  }, HEARTBEAT_CHECK_MS);
}

while (true) {
  const { value, done } = await reader.read();
  if (done) break;
  buf += decoder.decode(value, { stream: true });
  let i;
  while ((i = buf.indexOf("\n")) >= 0) {
    const line = buf.slice(0, i).trim();
    buf = buf.slice(i + 1);
    if (!line) continue;
    try {
      const obj = JSON.parse(line);
      let encoded = obj.data;
      if (typeof encoded === "string" && encoded.startsWith("u")) {
        encoded = encoded.slice(1);
      }
      const msg = Buffer.from(encoded, "base64").toString("utf8");
      let parsed;
      try {
        parsed = JSON.parse(msg);
      } catch {
        parsed = null;
      }
      if (parsed?.type === "document-update") {
        await handleDocumentUpdate(parsed, obj.from);
        continue;
      }
      if (parsed?.type === "vector-confirmation") {
        logConfirmation(parsed, obj.from);
        continue;
      }
      if (parsed?.type === "vector-commit") {
        applyCommit(parsed, obj.from);
        continue;
      }
      if (parsed?.type === "leader-block") {
        await handleLeaderBlock(parsed, obj.from);
        continue;
      }
      if (parsed?.type === "leader-heartbeat") {
        touchHeartbeat(parsed?.leaderId ?? obj.from ?? "leader");
        continue;
      }
      console.log(`[peer] [${TOPIC}] ${obj.from ?? "unknown"}: ${msg}`);
    } catch (err) {
      console.error("Falha ao processar mensagem:", line, err);
    }
  }
}
