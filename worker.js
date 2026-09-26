const UPSTREAMS = {
  okx_spot: "wss://ws.okx.com:8443/ws/v5/public",
  okx_futures: "wss://ws.okx.com:8443/ws/v5/public",
  kraken_spot: "wss://ws.kraken.com/v2",
  kraken_futures: "wss://futures.kraken.com/ws/v1"
};

const FETCH_TIMEOUT_MS = 15000;

export default {
  async fetch(request) {
    if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket") {
      return new Response("Expected WebSocket", {
        status: 426
      });
    }

    const url = new URL(request.url);
    const exchange = url.searchParams.get("exchange");
    const upstreamUrl = UPSTREAMS[exchange];

    if (!upstreamUrl) {
      return new Response(
        "Invalid exchange: " + exchange,
        { status: 400 }
      );
    }

    console.log(
      `[RELAY] ${exchange} -> ${upstreamUrl}`
    );

    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];

    server.accept({ allowHalfOpen: true });

    let upstreamResponse;

    try {
      const controller = new AbortController();

      const timeout = setTimeout(() => {
        controller.abort();
      }, FETCH_TIMEOUT_MS);

      try {
        upstreamResponse = await fetch(upstreamUrl, {
          method: "GET",
          headers: {
            "Upgrade": "websocket"
          },
          signal: controller.signal
        });
      } finally {
        clearTimeout(timeout);
      }

      console.log(
        `[RELAY] ${exchange} upstream status: ${upstreamResponse.status}`
      );

    } catch (error) {
      console.error(
        `[RELAY] ${exchange} upstream fetch failed:`,
        error?.name,
        error?.message
      );

      try {
        server.close(1011, "Upstream connection failed");
      } catch {}

      return new Response(
        `Upstream connection failed: ${error?.name}: ${error?.message}`,
        { status: 502 }
      );
    }

    const upstream = upstreamResponse.webSocket;

    if (!upstream) {
      console.error(
        `[RELAY] ${exchange} upstream did not provide WebSocket`
      );

      try {
        server.close(1011, "No upstream WebSocket");
      } catch {}

      return new Response(
        `Upstream did not provide WebSocket. HTTP status: ${upstreamResponse.status}`,
        { status: 502 }
      );
    }

    console.log(
      `[RELAY] ${exchange} upstream WebSocket accepted`
    );

    upstream.accept({ allowHalfOpen: true });

    server.addEventListener("message", event => {
      try {
        if (upstream.readyState === WebSocket.OPEN) {
          upstream.send(event.data);
        }
      } catch (error) {
        console.error(
          `[RELAY] ${exchange} client->upstream error:`,
          error.message
        );
      }
    });

    upstream.addEventListener("message", event => {
      try {
        if (server.readyState === WebSocket.OPEN) {
          server.send(event.data);
        }
      } catch (error) {
        console.error(
          `[RELAY] ${exchange} upstream->client error:`,
          error.message
        );
      }
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
