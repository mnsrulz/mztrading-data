import type { Context } from "@netlify/edge-functions";
import { walk } from "https://deno.land/std@0.170.0/fs/walk.ts";

export default async (request: Request, context: Context) => {
  const files = Deno.readDirSync("./");//.map(({name, isDirectory, isFile, isSymlink}) => ({ name, isDirectory, isFile, isSymlink }));
  const fileList = []
  try {
    for await (const walkEntry of walk("./data")) {
      const type = walkEntry.isSymlink
      ? "symlink"
      : walkEntry.isFile
      ? "file"
      : "directory";
      
      fileList.push({type, path: walkEntry.path});
    }
  } catch (error) {
    console.error("Error walking the directory:", error);
  }
  return Response.json({
    files: Array.from(files), 
    fileList,
  }, { status: 200 });
};

export const config = { path: "/api4" };