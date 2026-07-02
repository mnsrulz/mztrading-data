import type { Context } from "@netlify/edge-functions";

export default async (request: Request, context: Context) => {
  return new Response("hello from edge function", { status: 200 });
};

export const config = { path: "/api4" };