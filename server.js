import http from "node:http";
import { createHash, randomBytes, randomUUID } from "node:crypto";
import { hostname } from "node:os";
import { pipeline } from "@xenova/transformers";

const PORT = Number(process.env.PORT || 7070);
const IPFS_BASE = process.env.IPFS_BASE || "http://127.0.0.1:5001";

const isLeader = process.argv.includes("--leader");
const PUBSUB_TOPIC = process.env.PUBSUB_TOPIC || "SDT_Broadcast";
const documentVectors = [];
const confirmationLog = new Map();
const peerCountHint = Number(process.env.PEER_COUNT_HINT || 0);
const peerQuorumOverride = Number(process.env.PEER_QUORUM || 0);
const leaderId = process.env.LEADER_ID || hostname();

const HEARTBEAT_INTERVAL_MS = Number(
  process.env.LEADER_HEARTBEAT_INTERVAL_MS || 5000
);
const BLOCK_FLUSH_MS = HEARTBEAT_INTERVAL_MS;

const PROMPT_REBROADCAST_MS = Number(
  process.env.PROMPT_REBROADCAST_MS || 8000
);
const PROMPT_PROCESSING_TIMEOUT_MS = Number(
  process.env.PROMPT_PROCESSING_TIMEOUT_MS || 120000
);
const PROMPT_RETENTION_MS = Number(
  process.env.PROMPT_RETENTION_MS || 900000
);

const pendingLeaderUpdates = [];
const promptRequests = new Map();
let heartbeatTimer = null;
let promptMaintenanceTimer = null;
let embeddingsPipelinePromise = null;

function encodeTopic(topic) {
  const txt = String(topic ?? "");
  const base64 = Buffer.from(txt, "utf8").toString("base64");
  const urlSafe = base64
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/, "");
  return `u${urlSafe}`;
}

function hashVector(cids = []) {
  const h = createHash("sha256");
  for (const cid of cids) {
    h.update(String(cid));
  }
  return h.digest("hex");
}

function sendJson(res, status, obj) {
  const data = Buffer.from(JSON.stringify(obj));
  res.writeHead(status, {
    "content-type": "application/json; charset=utf-8",
    "content-length": data.length,
  });
  res.end(data);
}

function notFound(res) {
  res.writeHead(404, { "content-type": "text/plain; charset=utf-8" });
  res.end("Not Found");
}

async function readRequestBody(req) {
  const chunks = [];
  for await (const c of req) chunks.push(c);
  return Buffer.concat(chunks);
}

async function ipfsAlive() {
  try {
    const r = await fetch(`${IPFS_BASE}/api/v0/version`, { method: "POST" });
    return r.ok;
  } catch {
    return false;
  }
}

async function getEmbeddingPipeline() {
  if (!embeddingsPipelinePromise) {
    embeddingsPipelinePromise = pipeline(
      "feature-extraction",
      "Xenova/all-MiniLM-L6-v2"
    );
  }
  return embeddingsPipelinePromise;
}

function bufferToEmbeddableText(buffer) {
  const asText = buffer.toString("utf8");
  const controlMatches = asText.match(/[\x00-\x08\x0E-\x1F]/g) ?? [];
  if (controlMatches.length > asText.length * 0.1) {
    return buffer.toString("base64");
  }
  return asText;
}

function fallbackEmbeddings(buffer, dimension = 64) {
  const acc = new Array(dimension).fill(0);
  for (let i = 0; i < buffer.length; i += 1) {
    acc[i % dimension] += buffer[i];
  }
  const norm =
    Math.sqrt(acc.reduce((sum, value) => sum + value * value, 0)) || 1;
  return acc.map((value) => value / norm);
}

async function generateEmbeddings(buffer) {
  const text = bufferToEmbeddableText(buffer);
  try {
    const embedder = await getEmbeddingPipeline();
    const output = await embedder(text, { pooling: "mean", normalize: true });
    return Array.from(output.data);
  } catch (error) {
    console.error(
      "Falha ao gerar embeddings com transformer, usando fallback:",
      error
    );
    return fallbackEmbeddings(buffer);
  }
}

async function flushLeaderBlock(reason = "heartbeat") {
  const updates = pendingLeaderUpdates.splice(0);
  const latest = documentVectors.at(-1);
  const payload = {
    type: "leader-block",
    leaderId,
    sentAt: new Date().toISOString(),
    reason,
    latestVersion: latest?.version ?? 0,
    latestHash: latest ? hashVector(latest.cids) : null,
    updates,
  };
  try {
    await publishToTopic(payload);
    const updateCount = updates.length;
    const label = updateCount > 0 ? `${updateCount} update(s)` : "heartbeat";
    console.log(`[leader] bloco enviado (${label})`);
  } catch (err) {
    console.error("Falha ao enviar bloco/heartbeat do lider:", err);
  }
}

function startLeaderHeartbeat() {
  if (!isLeader) return;
  if (heartbeatTimer) clearInterval(heartbeatTimer);
  heartbeatTimer = setInterval(() => {
    flushLeaderBlock("interval");
  }, BLOCK_FLUSH_MS);
}

function createDocumentVectorVersion(cid, metadata = {}) {
  const lastVersion = documentVectors.at(-1);
  const vector = [...(lastVersion?.cids ?? [])];
  vector.push(cid);
  const version = lastVersion ? lastVersion.version + 1 : 1;
  const entry = {
    version,
    cids: vector,
    confirmed: false,
    metadata,
    createdAt: new Date().toISOString(),
  };
  documentVectors.push(entry);
  return entry;
}

async function publishToTopic(message) {
  const payload =
    typeof message === "string" ? message : JSON.stringify(message);

  const form = new FormData();
  form.append(
    "file",
    new Blob([payload], { type: "application/octet-stream" })
  );

  const resp = await fetch(
    `${IPFS_BASE}/api/v0/pubsub/pub?arg=${encodeTopic(PUBSUB_TOPIC)}`,
    {
      method: "POST",
      body: form,
    }
  );

  if (!resp.ok) {
    const body = await resp.text().catch(() => "");
    throw new Error(`Falha IPFS pubsub pub: HTTP ${resp.status} ${body}`);
  }

  return payload;
}

function createPromptRequest(promptText, mode = "faiss") {
  const id = randomUUID();
  const token = randomBytes(12).toString("hex");
  const now = new Date();
  const entry = {
    id,
    token,
    prompt: String(promptText || "").trim(),
    mode: String(mode || "faiss"),
    status: "pending",
    assignedPeer: null,
    createdAt: now.toISOString(),
    updatedAt: now.toISOString(),
    response: null,
    lastBroadcastAt: 0,
  };
  promptRequests.set(id, entry);
  return entry;
}

async function publishPromptRequest(entry, reason = "new") {
  if (!entry) return;
  const payload = {
    type: "prompt-request",
    requestId: entry.id,
    token: entry.token,
    prompt: entry.prompt,
    mode: entry.mode,
    createdAt: entry.createdAt,
    reason,
  };
  await publishToTopic(payload);
  entry.lastBroadcastAt = Date.now();
}

async function broadcastPromptAssignment(entry, reason = "assign") {
  if (!entry) return;
  const payload = {
    type: "prompt-claim-ack",
    requestId: entry.id,
    token: entry.token,
    assignedPeer: entry.assignedPeer,
    prompt: entry.prompt,
    mode: entry.mode,
    createdAt: entry.createdAt,
    reason,
  };
  await publishToTopic(payload);
}

function handlePromptClaim(message, sender) {
  const id = message?.requestId;
  const token = message?.token ? String(message.token) : null;
  if (!id || !token) return;
  const entry = promptRequests.get(id);
  if (!entry || entry.token !== token) return;
  if (entry.status === "done" || entry.status === "error") return;

  const peerId = message?.peerId || sender || "peer";
  if (!entry.assignedPeer) {
    entry.assignedPeer = peerId;
    entry.status = "processing";
    entry.updatedAt = new Date().toISOString();
    broadcastPromptAssignment(entry).catch((err) =>
      console.error("Falha ao confirmar atribuicao de prompt:", err)
    );
    return;
  }

  if (entry.assignedPeer !== peerId) {
    broadcastPromptAssignment(entry, "already-assigned").catch((err) =>
      console.error("Falha ao reenviar atribuicao de prompt:", err)
    );
  }
}

function handlePromptResponse(message, sender) {
  const id = message?.requestId;
  const token = message?.token ? String(message.token) : null;
  if (!id || !token) return;
  const entry = promptRequests.get(id);
  if (!entry || entry.token !== token) return;

  const peerId = message?.peerId || sender || entry.assignedPeer || "peer";
  entry.assignedPeer = entry.assignedPeer || peerId;
  entry.status = message?.error ? "error" : "done";
  entry.updatedAt = new Date().toISOString();
  entry.response = {
    peerId,
    output: message?.output ?? null,
    matches: Array.isArray(message?.matches) ? message.matches : [],
    mode: message?.mode || entry.mode,
    error: message?.error ? String(message.error) : null,
    receivedAt: new Date().toISOString(),
  };
}

function queueLeaderUpdate(updatePayload) {
  pendingLeaderUpdates.push(updatePayload);
}

function recordVectorConfirmation(message, sender) {
  const version = Number(message?.vectorVersion ?? 0);
  const hash = message?.vectorHash ? String(message.vectorHash) : null;
  const from = message?.peerId || sender || "peer";
  const vectorLength = message?.vectorLength ?? null;

  const expected = documentVectors.find((v) => v.version === version);
  const expectedHash = expected ? hashVector(expected.cids) : null;
  const matches = expectedHash && hash ? expectedHash === hash : null;

  const entry = confirmationLog.get(version) ?? {
    confirmations: [],
    expectedHash,
  };
  entry.expectedHash = expectedHash ?? entry.expectedHash ?? null;
  entry.confirmations.push({
    peerId: from,
    hash,
    vectorLength,
    matches,
    receivedAt: new Date().toISOString(),
  });
  confirmationLog.set(version, entry);

  console.log(
    `[leader] confirmacao recebida da versao ${version} de ${from}:` +
      ` hash ${hash ?? "n/a"}` +
      `${matches === false ? " (mismatch)" : ""}` +
      `${matches === null ? " (sem hash esperado)" : ""}`
  );

  maybeCommitVersion(version);
}

function computeQuorum(entry) {
  if (peerQuorumOverride > 0) return peerQuorumOverride;
  const observedPeers = new Set(
    (entry?.confirmations ?? []).map((c) => c.peerId || "peer")
  );
  const dynamic = observedPeers.size
    ? Math.floor(observedPeers.size / 2) + 1
    : 1;
  const hinted = peerCountHint > 0 ? Math.floor(peerCountHint / 2) + 1 : 1;
  return Math.max(1, dynamic, hinted);
}

async function commitVector(version, entry) {
  const vectorHash = hashVector(entry.cids);
  const commitPayload = {
    type: "vector-commit",
    vectorVersion: version,
    vectorHash,
    vectorLength: entry.cids.length,
    cids: entry.cids,
    document: entry.metadata,
    createdAt: entry.createdAt,
  };
  await publishToTopic(commitPayload);
  entry.confirmed = true;
  const logEntry = confirmationLog.get(version) ?? {};
  logEntry.committed = true;
  logEntry.committedAt = new Date().toISOString();
  logEntry.expectedHash = logEntry.expectedHash ?? vectorHash;
  confirmationLog.set(version, logEntry);
  console.log(
    `[leader] commit enviado da versao ${version} (hash ${vectorHash.slice(
      0,
      8
    )}...), quorum atingido.`
  );
}

async function maybeCommitVersion(version) {
  const vectorEntry = documentVectors.find((v) => v.version === version);
  if (!vectorEntry || vectorEntry.confirmed) return;

  const logEntry = confirmationLog.get(version);
  const expectedHash = hashVector(vectorEntry.cids);
  if (!logEntry || !Array.isArray(logEntry.confirmations)) return;
  const matching = logEntry.confirmations.filter(
    (c) => c.hash && c.hash === expectedHash
  );
  const quorum = computeQuorum(logEntry);
  if (matching.length >= quorum) {
    try {
      await commitVector(version, vectorEntry);
    } catch (err) {
      console.error("Falha ao enviar commit:", err);
    }
  }
}

function startPromptMaintenance() {
  if (!isLeader) return;
  if (promptMaintenanceTimer) clearInterval(promptMaintenanceTimer);
  promptMaintenanceTimer = setInterval(() => {
    const now = Date.now();
    for (const [id, entry] of promptRequests.entries()) {
      const createdMs = Date.parse(entry.createdAt || "") || now;
      const updatedMs = Date.parse(entry.updatedAt || "") || createdMs;
      const age = now - createdMs;
      const finished = entry.status === "done" || entry.status === "error";

      if (finished && age > PROMPT_RETENTION_MS) {
        promptRequests.delete(id);
        continue;
      }

      if (!finished) {
        const sinceBroadcast = now - (entry.lastBroadcastAt || 0);
        if (sinceBroadcast > PROMPT_REBROADCAST_MS) {
          publishPromptRequest(entry, "rebroadcast").catch((err) =>
            console.error("Falha ao republicar prompt pendente:", err)
          );
        }
        const inFlightTooLong =
          entry.status === "processing" &&
          now - updatedMs > PROMPT_PROCESSING_TIMEOUT_MS;
        if (inFlightTooLong) {
          entry.status = "pending";
          entry.assignedPeer = null;
          entry.updatedAt = new Date().toISOString();
        }
      }
    }
  }, Math.max(4000, PROMPT_REBROADCAST_MS));
}

async function startLeaderConfirmationListener() {
  const subUrl = new URL(`${IPFS_BASE}/api/v0/pubsub/sub`);
  subUrl.searchParams.set("arg", encodeTopic(PUBSUB_TOPIC));
  console.log(
    `[leader] ouvindo confirmacoes no topico "${PUBSUB_TOPIC}" via pubsub...`
  );

  try {
    const resp = await fetch(subUrl, { method: "POST" });
    if (!resp.ok) {
      const txt = await resp.text().catch(() => "");
      console.error(
        "Falha ao subscrever o topico de confirmacoes:",
        resp.status,
        txt
      );
      return;
    }

    const reader = resp.body?.getReader?.();
    if (!reader) {
      console.error("Subscricao de pubsub sem reader disponivel.");
      return;
    }
    const decoder = new TextDecoder();
    let buf = "";

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
          if (parsed?.type === "vector-confirmation") {
            recordVectorConfirmation(parsed, obj.from);
          } else if (parsed?.type === "prompt-claim") {
            handlePromptClaim(parsed, obj.from);
          } else if (parsed?.type === "prompt-response") {
            handlePromptResponse(parsed, obj.from);
          }
        } catch (err) {
          console.error("Falha ao processar mensagem do pubsub:", err);
        }
      }
    }
  } catch (err) {
    console.error("Erro ao escutar confirmacoes via pubsub:", err);
  }
}

async function handleUpload(req, res) {
  if (req.method !== "POST") {
    res.writeHead(405, { allow: "POST" });
    return res.end();
  }
  const filename = req.headers["filename"];
  if (!filename || String(filename).trim() === "") {
    return sendJson(res, 400, {
      error: "Cabecalho 'Filename' e obrigatorio.",
    });
  }
  try {
    const chunks = [];
    for await (const chunk of req) chunks.push(chunk);
    const buffer = Buffer.concat(chunks);

    const form = new FormData();
    form.append("file", new Blob([buffer]), String(filename));

    const addUrl = new URL(`${IPFS_BASE}/api/v0/add`);
    addUrl.searchParams.set("pin", "true");
    addUrl.searchParams.set("progress", "false");

    const ipfsResp = await fetch(addUrl, { method: "POST", body: form });
    if (!ipfsResp.ok) {
      const errText = await ipfsResp.text().catch(() => "");
      return sendJson(res, 502, {
        error: "Falha ao adicionar ao IPFS",
        detail: `HTTP ${ipfsResp.status} ${ipfsResp.statusText}`,
        body: errText.slice(0, 500),
      });
    }
    const json = await ipfsResp.json();
    const cid = json?.Hash ?? null;
    if (!cid) {
      return sendJson(res, 502, {
        error: "Resposta IPFS invalida",
      });
    }

    const versionEntry = createDocumentVectorVersion(cid, {
      cid,
      filename: String(filename),
      size: json?.Size ? Number(json.Size) : buffer.length,
    });
    const embeddings = await generateEmbeddings(buffer);
    const payload = {
      type: "document-update",
      vectorVersion: versionEntry.version,
      vector: versionEntry.cids,
      document: {
        cid,
        filename: String(filename),
        size: versionEntry.metadata.size,
      },
      embeddings,
      createdAt: versionEntry.createdAt,
    };

    let propagation = { ok: false, error: "Lider nao configurado" };
    if (isLeader) {
      try {
        queueLeaderUpdate(payload);
        propagation = { ok: true, mode: "queued" };
      } catch (error) {
        console.error("Erro ao propagar atualizacao para os peers:", error);
        propagation = { ok: false, error: String(error?.message || error) };
      }
    }

    return sendJson(res, 201, {
      name: json?.Name ?? String(filename),
      cid,
      size: versionEntry.metadata.size,
      pinned: true,
      vectorVersion: versionEntry.version,
      currentVector: versionEntry.cids,
      embeddingsDimensions: embeddings.length,
      propagation,
    });
  } catch (e) {
    return sendJson(res, 500, {
      error: "Erro interno",
      detail: String(e?.message || e),
    });
  }
}

async function handleHealth(_req, res) {
  const alive = await ipfsAlive();
  const body = {
    status: "ok",
    ipfs: alive ? "up" : "down",
  };
  body.pubsub = { topic: PUBSUB_TOPIC };
  body.vector = {
    latestVersion: documentVectors.at(-1)?.version ?? null,
    totalVersions: documentVectors.length,
    latestSize: documentVectors.at(-1)?.cids.length ?? 0,
  };
  const pendingPrompts = Array.from(promptRequests.values()).filter(
    (p) => p.status !== "done" && p.status !== "error"
  ).length;
  body.prompts = {
    total: promptRequests.size,
    pending: pendingPrompts,
  };
  return sendJson(res, 200, body);
}

async function handleBroadcast(req, res) {
  if (req.method !== "POST") {
    res.writeHead(405, { allow: "POST" });
    return res.end();
  }
  try {
    const chunks = [];
    for await (const c of req) chunks.push(c);
    const raw = Buffer.concat(chunks).toString("utf8");
    let msg;
    try {
      msg = JSON.parse(raw)?.msg;
    } catch {
      msg = raw.trim();
    }
    if (!msg) return sendJson(res, 400, { error: "Mensagem vazia." });

    const x = await publishToTopic(msg);
    return sendJson(res, 200, {
      ok: true,
      topic: PUBSUB_TOPIC,
      published: x.toString(),
    });
  } catch (e) {
    return sendJson(res, 500, {
      error: "Erro no broadcast",
      detail: String(e?.message || e),
    });
  }
}

async function handlePromptSubmit(req, res) {
  if (req.method !== "POST") {
    res.writeHead(405, { allow: "POST" });
    return res.end();
  }
  const body = await readRequestBody(req);
  let parsed;
  try {
    parsed = JSON.parse(body.toString("utf8"));
  } catch {
    parsed = null;
  }
  const promptText = parsed?.prompt || body.toString("utf8").trim();
  const mode = parsed?.mode || "faiss";
  if (!promptText || String(promptText).trim() === "") {
    return sendJson(res, 400, { error: "Campo 'prompt' obrigatorio." });
  }
  if (!isLeader) {
    return sendJson(res, 503, {
      error: "Este processo nao e o lider ativo.",
    });
  }
  const entry = createPromptRequest(promptText, mode);
  let propagation = { ok: false };
  try {
    await publishPromptRequest(entry, "new");
    propagation = { ok: true };
  } catch (err) {
    console.error("Falha ao publicar prompt-request:", err);
    propagation = { ok: false, error: String(err?.message || err) };
  }
  return sendJson(res, 202, {
    id: entry.id,
    token: entry.token,
    status: entry.status,
    assignedPeer: entry.assignedPeer,
    mode: entry.mode,
    propagation,
  });
}

async function handlePromptStatus(promptId, res) {
  const entry = promptRequests.get(promptId);
  if (!entry) return sendJson(res, 404, { error: "Prompt nao encontrada." });
  return sendJson(res, 200, {
    id: entry.id,
    status: entry.status,
    assignedPeer: entry.assignedPeer,
    mode: entry.mode,
    createdAt: entry.createdAt,
    updatedAt: entry.updatedAt,
    response: entry.response,
  });
}

const server = http.createServer(async (req, res) => {
  try {
    const { pathname } = new URL(req.url, "http://localhost");
    if (pathname === "/health" && req.method === "GET")
      return handleHealth(req, res);
    if (pathname === "/files") return handleUpload(req, res);
    if (pathname === "/broadcast") return handleBroadcast(req, res);
    if (pathname === "/prompts") return handlePromptSubmit(req, res);
    const promptMatch = pathname.match(/^\/prompts\/([^/]+)$/);
    if (promptMatch && req.method === "GET")
      return handlePromptStatus(promptMatch[1], res);
    res.writeHead(404, { "content-type": "text/plain; charset=utf-8" });
    res.end("Not Found");
  } catch (e) {
    return sendJson(res, 500, {
      error: "Erro inesperado",
      detail: String(e?.message || e),
    });
  }
});

if (isLeader) {
  startLeaderConfirmationListener().catch((err) =>
    console.error("Erro ao iniciar listener de confirmacoes:", err)
  );
  startLeaderHeartbeat();
  startPromptMaintenance();
  flushLeaderBlock("startup");
}

server.listen(PORT, () => {
  console.log(
    `Leader API em http://localhost:${PORT} ${isLeader ? "(lider)" : ""}`
  );
  if (isLeader) {
    console.log(`Propagacao via pubsub topico "${PUBSUB_TOPIC}"`);
  }
});
