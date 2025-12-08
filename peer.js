import { createHash } from "node:crypto";
import { hostname } from "node:os";
import { spawn } from "node:child_process";
import { pipeline } from "@xenova/transformers";

const IPFS_BASE = process.env.IPFS_BASE || "http://127.0.0.1:5001";
const TOPIC = process.env.PUBSUB_TOPIC || "SDT_Broadcast";
const PEER_ID = process.env.PEER_ID || hostname();
const HEARTBEAT_TIMEOUT_MS = Number(
  process.env.PEER_HEARTBEAT_TIMEOUT_MS || 15000
);
const HEARTBEAT_CHECK_MS = Number(
  process.env.PEER_HEARTBEAT_CHECK_MS || 2000
);
const ELECTION_TIMEOUT_BASE_MS = Number(
  process.env.PEER_ELECTION_TIMEOUT_BASE_MS || 3000
);
const ELECTION_TIMEOUT_JITTER_MS = Number(
  process.env.PEER_ELECTION_TIMEOUT_JITTER_MS || 2000
);
const VICTORY_DELAY_MS = Number(process.env.PEER_VICTORY_DELAY_MS || 1200);
const LEADER_HEARTBEAT_MS = Number(
  process.env.PEER_LEADER_HEARTBEAT_MS || 4000
);
const PROMPT_CLAIM_JITTER_MS = Number(
  process.env.PROMPT_CLAIM_JITTER_MS || 800
);
const PROMPT_TOP_K = Number(process.env.PROMPT_TOP_K || 3);
const PROMPT_DOC_BYTES = Number(process.env.PROMPT_DOC_BYTES || 2048);

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
let currentLeaderId = null;
let actingLeader = false;
let currentTerm = 0;
let highestCandidateId = PEER_ID;
let electionTimer = null;
let victoryTimer = null;
let leaderHeartbeatTimer = null;
let leaderServerProcess = null;
const promptClaims = new Map();
const promptResults = new Map();
let promptPipelinePromise = null;

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

async function getPromptPipeline() {
  if (!promptPipelinePromise) {
    promptPipelinePromise = pipeline(
      "feature-extraction",
      "Xenova/all-MiniLM-L6-v2"
    );
  }
  return promptPipelinePromise;
}

function fallbackPromptEmbeddings(text, dimension = 64) {
  const buf = Buffer.from(String(text ?? ""), "utf8");
  const acc = new Array(dimension).fill(0);
  for (let i = 0; i < buf.length; i += 1) {
    acc[i % dimension] += buf[i];
  }
  const norm =
    Math.sqrt(acc.reduce((sum, value) => sum + value * value, 0)) || 1;
  return acc.map((value) => value / norm);
}

async function embedPrompt(text) {
  try {
    const embedder = await getPromptPipeline();
    const output = await embedder(String(text ?? ""), {
      pooling: "mean",
      normalize: true,
    });
    return Array.from(output.data);
  } catch (err) {
    console.error("Falha ao gerar embedding da prompt, usando fallback:", err);
    return fallbackPromptEmbeddings(text);
  }
}

function cosineSimilarity(a = [], b = []) {
  const len = Math.min(a.length, b.length);
  if (!len) return 0;
  let dot = 0;
  let normA = 0;
  let normB = 0;
  for (let i = 0; i < len; i += 1) {
    const va = Number(a[i]) || 0;
    const vb = Number(b[i]) || 0;
    dot += va * vb;
    normA += va * va;
    normB += vb * vb;
  }
  if (!normA || !normB) return 0;
  return dot / (Math.sqrt(normA) * Math.sqrt(normB));
}

function rankFaissMatches(promptEmbedding, topK = PROMPT_TOP_K) {
  const entries = [];
  for (const [version, data] of faissIndex.entries()) {
    if (!data?.embeddings || data.embeddings.length === 0) continue;
    const similarity = cosineSimilarity(promptEmbedding, data.embeddings);
    entries.push({
      version,
      similarity: Number(similarity.toFixed(4)),
      vectorHash: data.vectorHash ?? null,
      document: data.document ?? null,
      updatedAt: data.updatedAt ?? null,
    });
  }
  entries.sort((a, b) => Number(b.similarity) - Number(a.similarity));
  return entries.slice(0, topK);
}

function randomPromptDelay() {
  return Math.floor(Math.random() * PROMPT_CLAIM_JITTER_MS);
}

function randomElectionTimeout() {
  const jitter = Math.floor(Math.random() * ELECTION_TIMEOUT_JITTER_MS);
  return ELECTION_TIMEOUT_BASE_MS + jitter;
}

async function fetchDocumentPreview(cid, maxBytes = PROMPT_DOC_BYTES) {
  if (!cid) return null;
  try {
    const catUrl = new URL(`${IPFS_BASE}/api/v0/cat`);
    catUrl.searchParams.set("arg", cid);
    const resp = await fetch(catUrl, { method: "POST" });
    if (!resp.ok) return null;
    const buf = await resp.arrayBuffer();
    const slice = buf.byteLength > maxBytes ? buf.slice(0, maxBytes) : buf;
    return new TextDecoder().decode(slice);
  } catch (err) {
    console.error(`Falha ao obter conteudo do CID ${cid}:`, err);
    return null;
  }
}

async function buildAnswerFromMatches(promptText, matches) {
  const previews = [];
  for (const m of matches) {
    const cid = m?.document?.cid ?? null;
    if (!cid) continue;
    const text = await fetchDocumentPreview(cid, PROMPT_DOC_BYTES);
    if (text) {
      previews.push({ cid, text: text.trim() });
    }
    if (previews.length >= PROMPT_TOP_K) break;
  }
  if (!previews.length) return null;
  const joined = previews
    .map((p) => `CID ${p.cid}: ${p.text.slice(0, 400)}`)
    .join("\n---\n");
  return `Pergunta: ${promptText}\nFontes selecionadas:\n${joined}`;
}

function stopLeaderHeartbeats() {
  if (leaderHeartbeatTimer) {
    clearInterval(leaderHeartbeatTimer);
    leaderHeartbeatTimer = null;
  }
}

function stopLeaderServer() {
  if (!leaderServerProcess) return;
  console.log("[peer] parando server.js (perdemos lideranca).");
  leaderServerProcess.kill();
  leaderServerProcess = null;
}

function startLeaderServer() {
  if (leaderServerProcess) return;
  console.log("[peer] iniciando server.js em modo lider...");
  leaderServerProcess = spawn(process.execPath, ["server.js", "--leader"], {
    stdio: "inherit",
    env: { ...process.env, LEADER_ID: PEER_ID },
  });
  leaderServerProcess.on("exit", (code, signal) => {
    console.warn(
      `[peer] server.js terminou (code=${code ?? "null"} signal=${signal ?? "null"}).`
    );
    leaderServerProcess = null;
    if (actingLeader) {
      console.warn("[peer] ainda sou lider mas server.js nao esta rodando.");
    }
  });
}

async function sendLeaderHeartbeat(reason = "heartbeat") {
  const latest = vectorVersions.at(-1);
  const payload = {
    type: "leader-heartbeat",
    leaderId: PEER_ID,
    term: currentTerm,
    sentAt: new Date().toISOString(),
    reason,
    latestVersion: latest?.version ?? 0,
    latestHash: latest ? hashVector(latest.cids) : null,
  };
  try {
    await publishToTopic(payload);
  } catch (err) {
    console.error("Falha ao enviar heartbeat do peer-lider:", err);
  }
}

function startLeaderHeartbeats() {
  if (leaderHeartbeatTimer) return;
  leaderHeartbeatTimer = setInterval(() => {
    sendLeaderHeartbeat("interval");
  }, LEADER_HEARTBEAT_MS);
}

function isBetterLeader(candidateId, candidateTerm) {
  if (!candidateId) return false;
  if (candidateTerm > currentTerm) return true;
  if (candidateTerm < currentTerm) return false;
  if (!currentLeaderId) return true;
  if (candidateId === currentLeaderId) return true;
  return candidateId > currentLeaderId;
}

function setLeader(id, term = currentTerm) {
  const candidateId = id || currentLeaderId || null;
  const candidateTerm = Number(term ?? 0);
  const accept = isBetterLeader(candidateId, candidateTerm);
  if (!accept) {
    return;
  }

  const steppingDown = actingLeader && candidateId !== PEER_ID;
  currentLeaderId = candidateId;
  highestCandidateId = candidateId;
  if (candidateTerm > currentTerm) currentTerm = candidateTerm;
  const wasDown = !leaderAlive;
  leaderAlive = true;
  lastHeartbeatAt = Date.now();
  if (wasDown) cancelElectionTimers();
  if (steppingDown) {
    actingLeader = false;
    stopLeaderHeartbeats();
    stopLeaderServer();
    return;
  }

  const isSelfLeader = candidateId === PEER_ID;
  const becameLeader = isSelfLeader && !actingLeader;
  if (!isSelfLeader) {
    actingLeader = false;
    stopLeaderHeartbeats();
    stopLeaderServer();
    return;
  }

  actingLeader = true;
  cancelElectionTimers();
  startLeaderHeartbeats();
  startLeaderServer();
  if (becameLeader) {
    sendLeaderHeartbeat("assumed-leader").catch((err) =>
      console.error("Falha ao enviar heartbeat imediato do novo lider:", err)
    );
  }
}

function detectConflict(incomingVersion) {
  const latest = vectorVersions.at(-1);
  if (!incomingVersion || Number.isNaN(incomingVersion)) return "versao invalida";
  if (!latest) return null;
  if (incomingVersion <= latest.version) return `versao ${incomingVersion} <= local ${latest.version}`;
  if (incomingVersion !== latest.version + 1) return `esperava versao ${latest.version + 1}`;
  return null;
}

function cancelElectionTimers() {
  if (electionTimer) {
    clearTimeout(electionTimer);
    electionTimer = null;
  }
  if (victoryTimer) {
    clearTimeout(victoryTimer);
    victoryTimer = null;
  }
}

async function announceVictory(reason = "win") {
  cancelElectionTimers();
  highestCandidateId = PEER_ID;
  actingLeader = true;
  setLeader(PEER_ID, currentTerm);
  const snapshot = buildStateSnapshot();
  const payload = {
    type: "leader-announce",
    leaderId: PEER_ID,
    term: currentTerm,
    sentAt: new Date().toISOString(),
    reason,
    state: snapshot,
  };
  try {
    await publishToTopic(payload);
    console.log("[peer] anunciei-me como novo lider.");
  } catch (err) {
    console.error("Falha ao anunciar novo lider:", err);
  }
}

function beginElection() {
  electionTimer = null;
  if (actingLeader) return;
  currentTerm += 1;
  highestCandidateId = PEER_ID;
  currentLeaderId = null;
  leaderAlive = false;
  stopLeaderHeartbeats();
  const payload = {
    type: "leader-election",
    candidateId: PEER_ID,
    term: currentTerm,
    latestVersion: vectorVersions.at(-1)?.version ?? 0,
    sentAt: new Date().toISOString(),
  };
  publishToTopic(payload).catch((err) =>
    console.error("Falha ao publicar candidatura a lider:", err)
  );
  victoryTimer = setTimeout(() => {
    if (highestCandidateId === PEER_ID && !leaderAlive) {
      announceVictory("no-higher-candidate");
    }
  }, VICTORY_DELAY_MS);
}

function scheduleElection(reason = "timeout") {
  if (actingLeader || leaderAlive) return;
  if (electionTimer) return;
  const delay = randomElectionTimeout();
  console.warn(`[peer] iniciando eleicao em ${delay}ms (motivo: ${reason})`);
  electionTimer = setTimeout(beginElection, delay);
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
    metadata: payload?.document ?? null,
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
  setLeader(payload?.leaderId ?? sender ?? "leader", Number(payload?.term ?? currentTerm));
  const updates = Array.isArray(payload?.updates) ? payload.updates : [];
  for (const update of updates) {
    await handleDocumentUpdate(update, payload?.leaderId ?? sender);
  }
}

function buildStateSnapshot() {
  return {
    vectorVersions,
    pendingEmbeddings: Array.from(pendingEmbeddings.entries()),
    faissIndex: Array.from(faissIndex.entries()),
  };
}

function mergeState(snapshot) {
  if (!snapshot) return;
  const incomingVectors = snapshot.vectorVersions ?? [];
  for (const v of incomingVectors) {
    const existing = vectorVersions.find((x) => x.version === v.version);
    if (!existing) {
      vectorVersions.push(v);
      continue;
    }
    if (!existing.confirmed && v.confirmed) {
      existing.confirmed = true;
      existing.cids = v.cids;
      existing.documentCid = v.documentCid;
      existing.metadata = v.metadata;
      existing.createdAt = v.createdAt;
    }
  }
  const incomingPending = snapshot.pendingEmbeddings ?? [];
  for (const [version, emb] of incomingPending) {
    if (!pendingEmbeddings.has(version)) {
      pendingEmbeddings.set(version, emb);
    }
  }
  const incomingFaiss = snapshot.faissIndex ?? [];
  for (const [version, data] of incomingFaiss) {
    if (!faissIndex.has(version)) {
      faissIndex.set(version, data);
    }
  }
}

function handleLeaderAnnounce(payload, sender) {
  const leaderId = payload?.leaderId ?? sender ?? "peer";
  const term = Number(payload?.term ?? 0);
  setLeader(leaderId, term);
  mergeState(payload?.state);
  cancelElectionTimers();
  console.log(`[peer] novo lider anunciado: ${leaderId} (term ${term}).`);
}

function handleLeaderElection(payload, sender) {
  const candidateId = payload?.candidateId;
  const term = Number(payload?.term ?? 0);
  if (!candidateId) return;
  if (term > currentTerm) {
    currentTerm = term;
    highestCandidateId = candidateId;
    leaderAlive = false;
    actingLeader = false;
    stopLeaderHeartbeats();
    cancelElectionTimers();
  } else if (term < currentTerm) {
    return;
  } else {
    if (candidateId > highestCandidateId) highestCandidateId = candidateId;
  }

  if (candidateId > PEER_ID) {
    currentLeaderId = candidateId;
    cancelElectionTimers();
  } else if (candidateId < PEER_ID && !electionTimer && !victoryTimer && !leaderAlive) {
    scheduleElection("higher-self");
  }
}

function applyCommit(payload, sender) {
  setLeader(payload?.leaderId ?? sender ?? "leader", Number(payload?.term ?? currentTerm));
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
  const document = payload?.document ?? entry?.metadata ?? null;
  faissIndex.set(version, {
    embeddings,
    vectorHash,
    updatedAt: new Date().toISOString(),
    document,
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

async function sendPromptResponse(payload) {
  try {
    await publishToTopic(payload);
  } catch (err) {
    console.error("Falha ao publicar resposta da prompt:", err);
  }
  promptResults.set(payload.requestId, {
    payload,
    status: payload.error ? "error" : "done",
    sentAt: new Date().toISOString(),
  });
  promptClaims.delete(payload.requestId);
}

async function processPromptJob(job) {
  const promptEmbedding = await embedPrompt(job.prompt);
  const matches = rankFaissMatches(promptEmbedding, PROMPT_TOP_K);
  let answer = null;
  if ((job.mode || "").toLowerCase() === "generate") {
    answer = await buildAnswerFromMatches(job.prompt, matches);
  }
  const output =
    answer ??
    `Top ${matches.length} matches: ${matches
      .map((m) => `${m.document?.cid ?? `v${m.version}`}:${m.similarity.toFixed(3)}`)
      .join(", ")}`;
  const payload = {
    type: "prompt-response",
    requestId: job.id,
    token: job.token,
    peerId: PEER_ID,
    mode: job.mode,
    matches,
    output,
  };
  await sendPromptResponse(payload);
  console.log(`[peer] resposta enviada para prompt ${job.id} (${job.mode}).`);
}

async function replayPromptResult(requestId) {
  const cached = promptResults.get(requestId);
  if (!cached?.payload) return;
  try {
    await publishToTopic(cached.payload);
  } catch (err) {
    console.error(`Falha ao reenviar resposta cacheada para ${requestId}:`, err);
  }
}

async function sendPromptClaim(claim) {
  const payload = {
    type: "prompt-claim",
    requestId: claim.id,
    token: claim.token,
    peerId: PEER_ID,
    mode: claim.mode,
  };
  await publishToTopic(payload);
  claim.lastClaimAt = Date.now();
}

async function handlePromptRequest(payload, sender) {
  const requestId = payload?.requestId;
  const token = payload?.token ? String(payload.token) : null;
  if (!requestId || !token) return;
  touchHeartbeat(sender ?? "leader");
  if (promptResults.has(requestId)) {
    await replayPromptResult(requestId);
    return;
  }
  const existing = promptClaims.get(requestId);
  if (existing && existing.token === token) {
    const stale =
      !existing.lastClaimAt ||
      Date.now() - existing.lastClaimAt > Math.max(5000, PROMPT_CLAIM_JITTER_MS * 4);
    if (stale) {
      sendPromptClaim(existing).catch((err) =>
        console.error("Falha ao reenviar prompt-claim:", err)
      );
    }
    return;
  }
  const claim = {
    id: requestId,
    token,
    prompt: payload?.prompt ?? "",
    mode: payload?.mode || "faiss",
    createdAt: payload?.createdAt || new Date().toISOString(),
    receivedFrom: sender ?? null,
  };
  promptClaims.set(requestId, claim);
  const delay = randomPromptDelay();
  setTimeout(() => {
    const stillPending = promptClaims.get(requestId);
    if (!stillPending || stillPending.token !== token) return;
    sendPromptClaim(stillPending).catch((err) =>
      console.error("Falha ao enviar prompt-claim:", err)
    );
  }, delay);
}

async function handlePromptAssignment(payload, sender) {
  const requestId = payload?.requestId;
  const token = payload?.token ? String(payload.token) : null;
  if (!requestId || !token) return;
  touchHeartbeat(sender ?? "leader");
  if (promptResults.has(requestId)) {
    await replayPromptResult(requestId);
    return;
  }
  const assignedPeer = payload?.assignedPeer;
  if (assignedPeer && assignedPeer !== PEER_ID) {
    promptClaims.delete(requestId);
    return;
  }
  const claim = promptClaims.get(requestId);
  if (claim && claim.token !== token) return;
  if (assignedPeer !== PEER_ID) return;
  const job = {
    id: requestId,
    token,
    prompt: payload?.prompt ?? claim?.prompt ?? "",
    mode: payload?.mode || claim?.mode || "faiss",
    createdAt: payload?.createdAt || claim?.createdAt || new Date().toISOString(),
  };
  promptClaims.set(requestId, { ...job, assigned: true });
  try {
    await processPromptJob(job);
  } catch (err) {
    console.error(`Falha ao processar prompt ${requestId}:`, err);
    const errorPayload = {
      type: "prompt-response",
      requestId,
      token,
      peerId: PEER_ID,
      mode: job.mode,
      matches: [],
      output: null,
      error: String(err?.message || err),
    };
    await sendPromptResponse(errorPayload);
  }
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
      scheduleElection("leader-timeout");
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
      if (parsed?.type === "prompt-request") {
        await handlePromptRequest(parsed, obj.from);
        continue;
      }
      if (parsed?.type === "prompt-claim-ack") {
        await handlePromptAssignment(parsed, obj.from);
        continue;
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
        setLeader(parsed?.leaderId ?? obj.from ?? "leader", Number(parsed?.term ?? currentTerm));
        continue;
      }
      if (parsed?.type === "leader-announce") {
        handleLeaderAnnounce(parsed, obj.from);
        continue;
      }
      if (parsed?.type === "leader-election") {
        handleLeaderElection(parsed, obj.from);
        continue;
      }
      console.log(`[peer] [${TOPIC}] ${obj.from ?? "unknown"}: ${msg}`);
    } catch (err) {
      console.error("Falha ao processar mensagem:", line, err);
    }
  }
}
