import http from "node:http";
import { createHash } from "node:crypto";
import { pipeline } from "@xenova/transformers";

const PORT = Number(process.env.PORT || 7070);
const IPFS_BASE = process.env.IPFS_BASE || "http://127.0.0.1:5001";

const isLeader = process.argv.includes("--leader");
const PUBSUB_TOPIC = process.env.PUBSUB_TOPIC || "SDT_Broadcast";
const documentVectors = [];
const confirmationLog = new Map();
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
      "sentence-transformers/all-MiniLM-L6-v2"
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
      error: "Cabeçalho 'Filename' é obrigatório.",
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
        error: "Resposta IPFS inválida",
      });
    }

    const versionEntry = createDocumentVectorVersion(cid, {
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

    let propagation = { ok: false, error: "Líder não configurado" };
    if (isLeader) {
      try {
        await publishToTopic(payload);
        propagation = { ok: true };
      } catch (error) {
        console.error("Erro ao propagar atualização para os peers:", error);
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

const server = http.createServer(async (req, res) => {
  try {
    const { pathname } = new URL(req.url, "http://localhost");
    if (pathname === "/health" && req.method === "GET")
      return handleHealth(req, res);
    if (pathname === "/files") return handleUpload(req, res); // aceita /files e /files/
    if (pathname === "/broadcast") return handleBroadcast(req, res);
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
}

server.listen(PORT, () => {
  console.log(
    `Leader API em http://localhost:${PORT} ${isLeader ? "(líder)" : ""}`
  );
  if (isLeader) {
    console.log(`Propagação via pubsub tópico "${PUBSUB_TOPIC}"`);
  }
});
