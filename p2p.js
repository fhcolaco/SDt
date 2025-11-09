// p2p.mjs
import { createLibp2p } from "libp2p";
import { tcp } from "@libp2p/tcp";
import { webSockets } from "@libp2p/websockets";
import { gossipsub } from "@libp2p/gossipsub";
import { kadDHT } from "@libp2p/kad-dht";
import * as uint8arrays from "uint8arrays";

const DEFAULT_TOPIC = "mestres-broadcast";

export async function startP2P({
  p2pPort = 15000,
  topic = DEFAULT_TOPIC,
  connect = null,
} = {}) {
  const node = await createLibp2p({
    addresses: {
      listen: [
        `/ip4/0.0.0.0/tcp/${p2pPort}`,
        `/ip4/127.0.0.1/tcp/${p2pPort}/ws`,
      ],
    },
    transports: [tcp(), webSockets()],
    pubsub: gossipsub(),
    dht: kadDHT(),
  });

  await node.start();

  node.pubsub.subscribe(topic);
  node.pubsub.addEventListener("message", (evt) => {
    const txt = uint8arrays.toString(evt.detail.data);
    console.log(`📩 [${topic}] ${evt.detail.from}: ${txt}`);
  });

  if (connect) {
    try {
      await node.dial(connect);
      console.log(`Ligado a bootstrap: ${connect}`);
    } catch (e) {
      console.error("Falha a ligar ao bootstrap:", e.message);
    }
  }

  async function publish(message) {
    const data = uint8arrays.fromString(String(message));
    await node.pubsub.publish(topic, data);
  }

  return {
    node,
    publish,
    topic,
    peerId: node.peerId.toString(),
    addrs: node.getMultiaddrs().map((a) => a.toString()),
  };
}

// Permite correr como “apenas peer” para testes: `node p2p.mjs --port=15001 --connect=<multiaddr>`
if (import.meta.main) {
  const portArg = process.argv
    .find((a) => a.startsWith("--port="))
    ?.split("=")[1];
  const connect = process.argv
    .find((a) => a.startsWith("--connect="))
    ?.split("=")[1];
  const topic =
    process.argv.find((a) => a.startsWith("--topic="))?.split("=")[1] ??
    DEFAULT_TOPIC;
  const p2pPort = portArg ? parseInt(portArg, 10) : 15001;

  const { peerId, addrs } = await startP2P({ p2pPort, topic, connect });
  console.log("Peer a ouvir:");
  console.log("peerId:", peerId);
  console.log("addrs :", addrs.join(" | "));
  console.log(`Subscrito no tópico: ${topic}`);
}
