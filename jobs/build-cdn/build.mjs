import { join } from "node:path";
import { mkdir } from "node:fs/promises";
import { createWriteStream } from "node:fs";
import { pipeline } from "node:stream/promises";
import { Readable } from "node:stream";
import optionsSnapshotSummary from "../../data/options-snapshot.summary.json" with {
    type: "json",
};

if(!process.env.BRANCH.startsWith("NF_")) {
  throw new Error("Invalid branch name");
}
const bname = process.env.BRANCH.substring(3); //netlify branch name

const url = optionsSnapshotSummary[bname].zipAssetUrl;
const response = await fetch(url);

if (!response.ok || !response.body) {
  throw new Error(`Download failed: ${response.status}`);
}

await mkdir("temp", { recursive: true });
const filePath = join("temp", "snapshot.zip");

await pipeline(
  Readable.fromWeb(response.body),
  createWriteStream(filePath)
);

console.log(`File saved to: ${filePath}`);