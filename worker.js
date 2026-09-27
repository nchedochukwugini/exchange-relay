const UPSTREAMS = {
  okx: "https://ws.okx.com:8443/ws/v5/public",
  kraken: "https://ws.kraken.com/v2",
  kraken_futures: "https://futures.kraken.com/ws/v1",
  coinbase_futures: "https://advanced-trade-ws.coinbase.com"
};

async function getKucoinWebSocketUrl(type) {
  const endpoint =
    type === "spot"
      ? "https://api.kucoin.com/api/v1/bullet-public"
      : "https://api-futures.kucoin.com/api/v1/bullet-public";

  const response = await fetch(endpoint, {
    method: "POST"
  });

  if (!response.ok) {
    throw new Error(`KuCoin token HTTP ${response.status}`);
  }

  const json = await response.json();
  const data = json?.data;
  const server = data?.instanceServers?.[0];

  if (!data?.token || !server?.endpoint) {
    throw new Error("KuCoin token response missing server");
  }

  return `${server.endpoint}?token=${encodeURIComponent(data.token)}`;
}

export default {
  async fetch(request) {
    if (request.headers.get("Upgrade") !== "websocket") {
      return new Response("Expected WebSocket", { status: 426 });
    }

    const url = new URL(request.url);
    const exchange = url.searchParams.get("exchange");

    let upstreamUrl;

    if (exchange === "kucoin_spot") {
      try {
        upstreamUrl = await getKucoinWebSocketUrl("spot");
      } catch (error) {
        return new Response(
          "KuCoin spot bootstrap failed: " + error.message,
          { status: 502 }
        );
      }
    } else if (exchange === "kucoin_futures") {
      try {
        upstreamUrl = await getKucoinWebSocketUrl("futures");
      } catch (error) {
        return new Response(
          "KuCoin futures bootstrap failed: " + error.message,
          { status: 502 }
        );
      }
    } else {
      upstreamUrl = UPSTREAMS[exchange];
    }

    if (!upstreamUrl) {
      return new Response(
        "Invalid exchange",
        { status: 400 }
      );
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
