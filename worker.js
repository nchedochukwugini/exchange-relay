const UPSTREAMS = {
  okx: "https://ws.okx.com:8443/ws/v5/public",
  kraken: "https://ws.kraken.com/v2"
};

const CONNECT_TIMEOUT_MS = 10000;

export default {
  async fetch(request) {
    const upgrade = request.headers.get("Upgrade");

    console.log("Incoming request", {
      method: request.method,
      upgrade,
      url: request.url
    });

    if (upgrade !== "websocket") {
      return new Response("Expected WebSocket", {
        status: 426
      });
    }

    const requestUrl = new URL(request.url);
    const exchange = requestUrl.searchParams.get("exchange");

    console.log("Requested exchange:", exchange);

    const upstreamUrl = UPSTREAMS[exchange];

    if (!upstreamUrl) {
      return new Response(
        "Invalid exchange. Use ?exchange=okx or ?exchange=kraken",
        { status: 400 }
      );
    }

    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];

    server.accept({
      allowHalfOpen: true
    });

    console.log("Client WebSocket accepted");

    const controller = new AbortController();

    const timeout = setTimeout(() => {
      console.log(
        `Upstream ${exchange} connection timed out after ${CONNECT_TIMEOUT_MS}ms`
      );

      controller.abort();
    }, CONNECT_TIMEOUT_MS);

    let response;

    try {
      console.log("Connecting to upstream:", upstreamUrl);

      response = await fetch(upstreamUrl, {
        method: "GET",
        headers: {
          Upgrade: "websocket"
        },
        signal: controller.signal
      });

      clearTimeout(timeout);

      console.log("Upstream response:", response.status);

    } catch (error) {
      clearTimeout(timeout);

      console.log("Upstream connection error:", {
        name: error?.name,
        message: error?.message
      });

      try {
        server.close(1011, "Upstream connection failed");
      } catch {}

      return new Response(
        `Upstream ${exchange} connection failed: ${error?.message ?? "unknown error"}`,
        {
          status: 502
        }
      );
    }

    const upstream = response.webSocket;

    if (!upstream) {
      console.log(
        "Upstream did not provide a WebSocket. HTTP status:",
        response.status
      );

      try {
        server.close(1011, "Upstream did not accept WebSocket");
      } catch {}

      return new Response(
        `Upstream did not accept WebSocket. Status: ${response.status}`,
        {
          status: 502
        }
      );
    }

    console.log("Upstream WebSocket obtained");

    upstream.accept({
      allowHalfOpen: true
    });

    console.log("Upstream WebSocket accepted");

    server.addEventListener("message", event => {
      console.log("Client -> upstream message");

      try {
        if (upstream.readyState === WebSocket.OPEN) {
          upstream.send(event.data);
        }
      } catch (error) {
        console.log("Client -> upstream error:", error.message);
      }
    });

    upstream.addEventListener("message", event => {
      console.log("Upstream -> client message");

      try {
        if (server.readyState === WebSocket.OPEN) {
          server.send(event.data);
        }
      } catch (error) {
        console.log("Upstream -> client error:", error.message);
      }
    });

    server.addEventListener("close", event => {
      console.log(
        "Client closed:",
        event.code,
        event.reason || ""
      );

      try {
        upstream.close(
          event.code || 1000,
          event.reason || ""
        );
      } catch {}
    });

    upstream.addEventListener("close", event => {
      console.log(
        "Upstream closed:",
        event.code,
        event.reason || ""
      );

      try {
        server.close(
          event.code || 1000,
          event.reason || ""
        );
      } catch {}
    });

    server.addEventListener("error", event => {
      console.log("Client WebSocket error");

      try {
        upstream.close(1011, "Client socket error");
      } catch {}
    });

    upstream.addEventListener("error", event => {
      console.log("Upstream WebSocket error");

      try {
        server.close(1011, "Upstream socket error");
      } catch {}
    });

    console.log("Relay established successfully");

    return new Response(null, {
      status: 101,
      webSocket: client
    });
  }
};
