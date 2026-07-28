import { classifyExpirations } from "../lib/utils.ts";
import expirationStrikes from "./../data/options-expirations-strikes.json" with {
    type: "json",
};

const uniqueExpirations = new Set<string>(
  Object.values(expirationStrikes).flatMap(k => Object.keys(k as any))
);

const data = classifyExpirations(uniqueExpirations);

Deno.writeTextFileSync(
    "./data/options-expirations-summary.json",
    JSON.stringify(data, null, 4),
);

console.log(`expirations successfully!`);