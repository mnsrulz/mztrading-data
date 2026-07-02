import type { Context } from "@netlify/edge-functions";

export default async (request: Request, context: Context) => {
  // Check if the request is an upgrade request
  if (request.headers.get("upgrade") === "websocket") {
    const { socket, response } = Deno.upgradeWebSocket(request);
    
    socket.onopen = () => console.log("WebSocket connected at the edge");
    socket.onmessage = (event) => {
      console.log(`Received: ${event.data}`);
      socket.send(`Echo from edge: ${event.data}`);
    };
    socket.onerror = (error) => console.error("Socket error:", error);
    socket.onclose = () => console.log("WebSocket closed");

    return response;
  }

  return new Response("Not a WebSocket request", { status: 400 });
};

export const config = { path: "/ws" };