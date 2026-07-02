import type { Context } from "@netlify/edge-functions";

export default async (request: Request, context: Context) => {
  const d = new URL(request.url).searchParams.get("d");
  const files = Deno.readDirSync(d ?? "./");//.map(({name, isDirectory, isFile, isSymlink}) => ({ name, isDirectory, isFile, isSymlink }));
  return Response.json({
    files: Array.from(files)
  }, { status: 200 });
};

export const config = { path: "/api4" };