import { getOptionsSnapshotSummary, ghRepoBaseUrl } from "../../lib/data.ts";
import { format } from "https://deno.land/std@0.224.0/datetime/format.ts";

const data = getOptionsSnapshotSummary();


const releaseName = Deno.env.get("RELEASE_NAME") ||
    `DEX_GEX_SNAPSHOT_${format(new Date(), "yyyy-MM-dd")}`;
const displayName = Deno.env.get("DISPLAY_NAME")

displayName && console.log(`Display name for this release: ${displayName}`);

console.log(`🔄 Generating options snapshot for release: ${releaseName}`);

const allSymbols = JSON.parse(Deno.readTextFileSync(`temp/all-symbols.json`)) as string[];

data[releaseName] = {
    displayName: displayName || format(new Date(), "yyyy-MM-dd"),
    created: new Date(),
    zipAssetUrl: `${ghRepoBaseUrl}/${releaseName}/options-snapshots.zip`,
    releasesBaseUrl: `https://github.com/mnsrulz/mztrading-data/releases`,
    sdResolution: "620",
    hdResolution: "1240",
    tickers: allSymbols
};

Deno.writeTextFileSync(
    "./data/options-snapshot.summary.json",
    JSON.stringify(data, null, 2),
);

console.log(`🟢 Summary file generated successfully!`);