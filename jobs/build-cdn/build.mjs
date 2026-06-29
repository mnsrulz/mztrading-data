import { join } from "node:path";
import { createWriteStream } from "node:fs";
import { pipeline } from "node:stream/promises";
import { Readable } from "node:stream";
import optionsSnapshotSummary from "../../data/options-snapshot.summary.json" with {
    type: "json",
};

const bname = 'DATA_2026-06-26';

const url = optionsSnapshotSummary[bname].zipAssetUrl;
const response = await fetch(url);

if (!response.ok || !response.body) {
  throw new Error(`Download failed: ${response.status}`);
}

const filePath = join("temp", "snapshot.zip");

await pipeline(
  Readable.fromWeb(response.body),
  createWriteStream(filePath)
);

console.log(`File saved to: ${filePath}`);