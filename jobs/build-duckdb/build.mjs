import { join } from "node:path";
import { mkdir } from "node:fs/promises";
import { createWriteStream } from "node:fs";
import { pipeline } from "node:stream/promises";
import { Readable } from "node:stream";
import summary from "../../data/cboe-options-rolling.json" with {
    type: "json",
};

const { assetUrl } = summary;
const response = await fetch(assetUrl);

if (!response.ok || !response.body) {
  throw new Error(`Download failed: ${response.status}`);
}

await mkdir("public", { recursive: true });
const filePath = join("public", "options_data.parquet");

await pipeline(
  Readable.fromWeb(response.body),
  createWriteStream(filePath)
);

console.log(`File saved to: ${filePath}`);