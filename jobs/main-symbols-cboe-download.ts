import ky from "https://esm.sh/ky@1.8.2";
import { parse } from "jsr:@std/csv";

console.log(`Downloading symbols from cboe.`);
//the below feed is also available.
//https://marketdata.theocc.com/delo-download?prodType=EU&downloadFields=OS;US&format=txt
const symobolsText = await ky(
    "https://www.cboe.com/us/options/symboldir/equity_index_options/?download=csv",
).text();

console.log(`Parsing csv...`);

const parseData = parse(symobolsText, {
    trimLeadingSpace: true,
    columns: ["name", "symbol", "description", "temp", "temp2"],
    skipFirstRow: true,
    strip: true,
});

const data = parseData.map(({ name, symbol }) => ({ name, symbol })).sort((a, b) => a.symbol.localeCompare(b.symbol) || a.name.localeCompare(b.name));

Deno.writeTextFileSync(
    "./data/symbols.json",
    JSON.stringify(data, null, 4),
);

console.log(`Symbols downloaded successfully!`);