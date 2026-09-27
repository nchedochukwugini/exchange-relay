const UPSTREAMS = {
  okx: "https://ws.okx.com:8443/ws/v5/public",
  kraken: "https://ws.kraken.com/v2",
  kraken_futures: "https://futures.kraken.com/ws/v1",
  coinbase_futures: "https://advanced-trade-ws.coinbase.com",
  kucoin_spot: "https://x-push-spot.kucoin.com",
  kucoin_futures: "https://x-push-futures.kucoin.com"
};

export default {
  async fetch(request) {
    if (request.headers.get("Upgrade") !== "websocket") {
      return new Response("Expected WebSocket", { status: 426 });
    }

    const url = new URL(request.url);
    const exchange = url.searchParams.get("exchange");
    const upstreamUrl = UPSTREAMS[exchange];

    if (!upstreamUrl) {
      return new Response("Invalid exchange", { status: 400 });
    }

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);

    server.binaryType = "arraybuffer";
    server.accept({ allowHalfOpen: true });

    let upstreamResponse;

    try {
      upstreamResponse = await fetch(upstreamUrl, {
        headers: {
          Upgrade: "websocket"
        }
      });
    } catch (error) {
      server.close(1011, "Upstream connection failed");
      return new Response(
        "Could not connect to upstream: " + error.message,
        { status: 502 }
      );
    }

    const upstream = upstreamResponse.webSocket;

    if (!upstream) {
      server.close(1011, "Upstream did not accept WebSocket");
      return new Response(
        "Upstream did not accept WebSocket",
        { status: 502 }
      );
    }

    upstream.binaryType = "arraybuffer";
    upstream.accept({ allowHalfOpen: true });

    async function forwardMessage(socket, data) {
      try {
        if (socket.readyState !== WebSocket.OPEN) return;

        if (typeof data === "string") {
          socket.send(data);
          return;
        }

        if (data instanceof ArrayBuffer) {
          socket.send(data);
          return;
        }

        if (data instanceof Blob) {
          socket.send(await data.arrayBuffer());
          return;
        }

        socket.send(data);
      } catch {}
    }

    server.addEventListener("message", event => {
      forwardMessage(upstream, event.data);
    });

    upstream.addEventListener("message", event => {
      forwardMessage(server, event.data);
    });

    server.addEventListener("close", event => {
      try {
        if (
          upstream.readyState === WebSocket.OPEN ||
          upstream.readyState === WebSocket.CLOSING
        ) {
          upstream.close(
            event.code || 1000,
            event.reason || ""
          );
        }
      } catch {}
    });

    upstream.addEventListener("close", event => {
      try {
        if (
          server.readyState === WebSocket.OPEN ||
          server.readyState === WebSocket.CLOSING
        ) {
          server.close(
            event.code || 1000,
            event.reason || ""
          );
        }
      } catch {}
    });

    server.addEventListener("error", () => {
      try {
        upstream.close(1011, "Client socket error");
      } catch {}
    });

    upstream.addEventListener("error", () => {
      try {
        server.close(1011, "Upstream socket error");
      } catch {}
    });

    return new Response(null, {
      status: 101,
      webSocket: client
    });
  }
};
