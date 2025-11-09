import http from "node:http";
import { startP2P } from "./p2p.js";

const PORT = Number(process.env.PORT || 7070);
const IPFS_BASE = process.env.IPFS_BASE || "http://127.0.0.1:5001";

const isLeader = process.argv.includes("--leader");
const p2pPortArg = process.argv
  .find((a) => a.startsWith("--p2p-port="))
  ?.split("=")[1];
const connectArg = process.argv
  .find((a) => a.startsWith("--connect="))
  ?.split("=")[1];
const p2pPort = p2pPortArg ? parseInt(p2pPortArg, 10) : 15000;
let p2p = null;

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

if (isLeader) {
  p2p = await startP2P({ p2pPort, connect: connectArg }); // líder também pode “dial” peers, se precisares
  console.log("P2P do Líder:");
  console.log("peerId:", p2p.peerId);
  console.log("addrs :", p2p.addrs.join(" | "));
  console.log("topic :", p2p.topic);
}

async function handleUpload(req, res) {
  if (req.method !== "POST") {
    res.writeHead(405, { allow: "POST" });
    return res.end();
  }
  const filename = req.headers["x-filename"];
  if (!filename || String(filename).trim() === "") {
    return sendJson(res, 400, {
      error: "Cabeçalho 'X-Filename' é obrigatório.",
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
    return sendJson(res, 201, {
      name: json?.Name ?? String(filename),
      cid: json?.Hash ?? null,
      size: json?.Size ? Number(json.Size) : buffer.length,
      pinned: true,
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
  if (isLeader && p2p) {
    body.p2p = {
      topic: p2p.topic,
      peerId: p2p.peerId,
      addrs: p2p.addrs,
    };
  }
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

    const TOPIC = process.env.PUBSUB_TOPIC || "mestres-broadcast";
    const url = new URL(`${IPFS_BASE}/api/v0/pubsub/pub`);
    url.searchParams.set("arg", TOPIC);
    url.searchParams.set("arg", msg);

    const r = await fetch(url, { method: "POST" });
    if (!r.ok) {
      const t = await r.text().catch(() => "");
      return sendJson(res, 502, {
        error: "Falha IPFS pubsub pub",
        detail: `HTTP ${r.status}`,
        body: t.slice(0, 500),
      });
    }
    return sendJson(res, 200, { ok: true, topic: TOPIC, published: msg });
  } catch (e) {
    return sendJson(res, 500, {
      error: "Erro no broadcast",
      detail: String(e?.message || e),
    });
  }
}

const server = http.createServer(async (req, res) => {
  try {
    if (req.url === "/health" && req.method === "GET")
      return handleHealth(req, res);
    if (req.url === "/files") return handleUpload(req, res);
    if (req.url === "/broadcast") return handleBroadcast(req, res);
    res.writeHead(404, { "content-type": "text/plain; charset=utf-8" });
    res.end("Not Found");
  } catch (e) {
    return sendJson(res, 500, {
      error: "Erro inesperado",
      detail: String(e?.message || e),
    });
  }
});

server.listen(PORT, () => {
  console.log(
    `Leader API em http://localhost:${PORT} ${isLeader ? "(líder + P2P)" : ""}`
  );
});
