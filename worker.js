const UPSTREAMS = {
  okx: "https://ws.okx.com:8443/ws/v5/public",
  kraken: "https://ws.kraken.com/v2"
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
      return new Response("Invalid exchange. Use ?exchange=okx or ?exchange=kraken", {
        status: 400
      });
    }

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);

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
      return new Response("Could not connect to upstream: " + error.message, {
        status: 502
      });
    }

    const upstream = upstreamResponse.webSocket;

    if (!upstream) {
      server.close(1011, "Upstream did not accept WebSocket");
      return new Response("Upstream did not accept WebSocket", {
        status: 502
      });
    }

    upstream.accept({ allowHalfOpen: true });

    server.addEventListener("message", event => {
      try {
        if (upstream.readyState === WebSocket.OPEN) {
          upstream.send(event.data);
        }
      } catch {}
    });

    upstream.addEventListener("message", event => {
      try {
        if (server.readyState === WebSocket.OPEN) {
          server.send(event.data);
        }
      } catch {}
    });

    server.addEventListener("close", event => {
      try {
        if (
          upstream.readyState === WebSocket.OPEN ||
          upstream.readyState === WebSocket.CLOSING
        ) {
          upstream.close(event.code || 1000, event.reason || "");
        }
      } catch {}
    });

    upstream.addEventListener("close", event => {
      try {
        if (
          server.readyState === WebSocket.OPEN ||
          server.readyState === WebSocket.CLOSING
        ) {
          server.close(event.code || 1000, event.reason || "");
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
