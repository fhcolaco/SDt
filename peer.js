const IPFS_BASE = process.env.IPFS_BASE || "http://127.0.0.1:5001";
const TOPIC = process.env.PUBSUB_TOPIC || "mestres-broadcast";

function encodeTopic(topic) {
  const txt = String(topic ?? "");
  const base64 = Buffer.from(txt, "utf8").toString("base64");
  const urlSafe = base64.replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
  return `u${urlSafe}`;
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

const reader = resp.body.getReader();
const decoder = new TextDecoder();
let buf = "";
function summarizeEmbeddings(embeddings) {
  if (!Array.isArray(embeddings) || embeddings.length === 0) return null;
  const preview = embeddings.slice(0, 5).map((n) => Number(n.toFixed(4)));
  return { dimension: embeddings.length, preview };
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
      const obj = JSON.parse(line); // NDJSON com { from, data }
      const msg = Buffer.from(obj.data, "base64").toString("utf8");
      try {
        const parsed = JSON.parse(msg);
        if (parsed?.type === "document-update") {
          const info = summarizeEmbeddings(parsed.embeddings);
          console.log(
            `📩 [${TOPIC}] versão ${parsed.vectorVersion} com ${parsed.vector?.length ?? 0} CIDs. CID do documento: ${parsed.document?.cid}. Embeddings: ${info?.dimension ?? 0} dimensões${
              info ? ` (preview ${info.preview.join(", ")})` : ""
            }.`
          );
          continue;
        }
      } catch {}
      console.log(`📩 [${TOPIC}] ${obj.from}: ${msg}`);
    } catch {}
  }
}
