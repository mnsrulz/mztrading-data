import type { Context } from "@netlify/edge-functions";

export default async (request: Request, context: Context) => {
  const files = Deno.readDirSync("./");
  return Response.json({
    files: files
  }, { status: 200 });
};

export const config = { path: "/api4" };