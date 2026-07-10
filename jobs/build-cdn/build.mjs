import { join } from "node:path";
import { mkdir } from "node:fs/promises";
import { createWriteStream } from "node:fs";
import { pipeline } from "node:stream/promises";
import { Readable } from "node:stream";
import optionsSnapshotSummary from "../../data/options-snapshot.summary.json" with {
    type: "json",
};

const realBranchName = process.env.BRANCH || process.env.CF_PAGES_BRANCH;
let bname = '';
if(!realBranchName.startsWith("NF_")) {
  bname = Object.keys(optionsSnapshotSummary).at(-1);
} else {
  bname = realBranchName.substring(3); //netlify or CF branch name
}

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
