import dayjs from "https://esm.sh/dayjs@1.11.13";
import { ensureDir } from "https://deno.land/std@0.224.0/fs/ensure_dir.ts";
import optionsExpirationStrikesMap from "../../data/options-expirations-strikes.json" with {
    type: "json",
};
import { getWeekOfMonth } from "../../lib/utils.ts";

type OptionsExpirationStrikesType = Record<string, Record<string, string>>
const dataFolder = `dist/expirations`;
await ensureDir(dataFolder);

const expirationMap = optionsExpirationStrikesMap as OptionsExpirationStrikesType;
//Build expirations per symbol
Object.keys(expirationMap).forEach(symbol => {
    const expirations = Object.keys(expirationMap[symbol]).toSorted().map(k => {
        return {
            expiration: k,
            strikes: JSON.parse(expirationMap[symbol][k]) as number[]
        }
    });

    const monthlyExpiryMap = new Map<string, string>();
    for (const { expiration } of expirations) {
        const expirationDayjs = dayjs(expiration, 'YYYY-MM-DD', true);
        if (expirationDayjs.date() >= 15 && expirationDayjs.date() <= 21 && getWeekOfMonth(expirationDayjs.date(), expirationDayjs.month(), expirationDayjs.year()) == 3) { //third week of the month
            const k = `${expirationDayjs.year()}-${expirationDayjs.month()}`;
            if (monthlyExpiryMap.get(k)! > expiration) continue;
            monthlyExpiryMap.set(k, expiration);
        }
    }

    const monthlyExpirations = new Set([...monthlyExpiryMap.values()]);
    const data = expirations.map(k => ({ ...k, isMonthly: monthlyExpirations.has(k.expiration) }));

    Deno.writeTextFileSync(`${dataFolder}/${symbol}.json`, JSON.stringify(data));
})