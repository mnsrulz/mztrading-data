import Fuse from "https://esm.sh/fuse.js@7.0.0";
import dayjs from "https://esm.sh/dayjs@1.11.13";

// import optionsDataSummary from "./../data/options-data.summary.json" with {
//     type: "json",
// };

import optionsSnapshotSummary from "./../data/options-snapshot.summary.json" with {
    type: "json",
};

import cboeOptionsSummary from "./../data/cboe-options-summary.json" with {
    type: "json",
};

import optionsExpirationStrikesMap from "./../data/options-expirations-strikes.json" with {
    type: "json",
};

import optionsRollingSummary from "./../data/cboe-options-rolling.json" with {
    type: "json",
};

import symbols from "./../data/symbols.json" with {
    type: "json",
};
import { getWeekOfMonth } from "./utils.ts";

// type OptionsDataSummary = Record<string, {
//     displayName: string;
//     created: Date | string;
//     symbols: Record<string, {
//         fileName: string;
//         assetUrl: string;
//     }>;
// }>;

type OptionsSnapshotSummaryFileType = {
    hdFileName: string;
    hdAssetUrl?: string;
    sdFileName: string;
    sdAssetUrl?: string;
};
type OptionsSnapshotSummary = Record<string, {
    displayName: string;
    created: Date | string;
    zipAssetUrl?: string;
    releasesBaseUrl: string;
    sdResolution: string;
    hdResolution: string;
    // symbols: Record<string, {
    //     "gex": OptionsSnapshotSummaryFileType;
    //     "dex": OptionsSnapshotSummaryFileType;
    // }>;
    tickers: string[];
}>;

type CboeOptionSummaryType = {
    name: string,
    optionsAssetUrl: string,
    stocksAssetUrl: string
}

type TickerSymbol = { name: string, symbol: string }

type OptionsExpirationStrikesType = Record<string, Record<string, string>>


// export const getOptionsDataSummary = () => {
//     return optionsDataSummary as OptionsDataSummary;
// };

export const getOptionsSnapshotSummary = () => {
    return optionsSnapshotSummary as OptionsSnapshotSummary;
};

export const OptionsSnapshotSummary = (optionsSnapshotSummary as OptionsSnapshotSummary);

export const OptionsExpirationStrikes = (optionsExpirationStrikesMap as OptionsExpirationStrikesType);

export const AvailableSnapshotDates = Object.values(OptionsSnapshotSummary).map(k => ({ dt: k.displayName }));

//export const OptionsSnapshotSummaryLegacy = Object.fromEntries(Object.keys(OptionsSnapshotSummary).map(j => [OptionsSnapshotSummary[j].displayName, { zipAssetUrl: OptionsSnapshotSummary[j].zipAssetUrl, symbols: OptionsSnapshotSummary[j].symbols }]));

export const zipServiceUrl = 'https://zipservice-deno.deno.dev/download';//?f=AAOI_GEX_620.png&q=https://github.com/mnsrulz/mztrading-data/releases/download/DEX_GEX_SNAPSHOT_2025-07-08/options-snapshots.zip';
const snapshotCdnUrl = 'https://mztradingsnapshotcdn.deno.dev/api/snapshots'; //?dt=2025-08-14&f=AAPL_DEX_620.png&symbol=AAPL'

const getFileName = (symbol: string, type: 'dex' | 'gex', resolution: string) => `${symbol.toUpperCase()}_${type.toUpperCase()}_${resolution}.png`;

export const getSnapshotsAvailableForDate = (dt: string) => {
    const result = Object.values(OptionsSnapshotSummary).find(k => k.displayName == dt);
    const releaseName = Object.keys(OptionsSnapshotSummary).find(k => OptionsSnapshotSummary[k].displayName == dt);

    if (result) {
        //FLR_DEX_620.png
        return result.tickers.map(k => {
            const dexHdFileName = getFileName(k, 'dex', result.hdResolution);
            const dexSdFileName = getFileName(k, 'dex', result.sdResolution);
            const gexHdFileName = getFileName(k, 'gex', result.hdResolution);
            const gexSdFileName = getFileName(k, 'gex', result.sdResolution);
            return {
                symbol: k,
                dex: {
                    hdAssetUrl: result.zipAssetUrl ? `${snapshotCdnUrl}?f=${dexHdFileName}&dt=${dt}&symbol=${k}` : `${result.releasesBaseUrl}/download/${releaseName}/${dexHdFileName}`,
                    sdAssetUrl: result.zipAssetUrl ? `${snapshotCdnUrl}?f=${dexSdFileName}&dt=${dt}&symbol=${k}` : `${result.releasesBaseUrl}/download/${releaseName}/${dexSdFileName}`
                },
                gex: {
                    hdAssetUrl: result.zipAssetUrl ? `${snapshotCdnUrl}?f=${gexHdFileName}&dt=${dt}&symbol=${k}` : `${result.releasesBaseUrl}/download/${releaseName}/${gexHdFileName}`,
                    sdAssetUrl: result.zipAssetUrl ? `${snapshotCdnUrl}?f=${gexSdFileName}&dt=${dt}&symbol=${k}` : `${result.releasesBaseUrl}/download/${releaseName}/${gexSdFileName}`
                },
            }
        });
    }
    throw new Error('No data found for this date');
}

export const getZipAssetUrlForSymbol = (symbol: string, dt: string) => {
    const result = Object.values(OptionsSnapshotSummary).find(k => k.displayName == dt);
    if (result) {
        if (result.tickers.includes(symbol)) {
            return result.zipAssetUrl;
        } else {
            throw new Error(`No data found for symbol ${symbol} on date ${dt}`);
        }
    }
    throw new Error(`No data found for date ${dt}`);
}

export const getZipAssetInfoByDate = (dt: string) => {
    const result = Object.values(OptionsSnapshotSummary).find(k => k.displayName == dt);
    if (result) {
        return {
            zipAssetUrl: result.zipAssetUrl,
            fileNames: result.tickers.flatMap(symbol => ([
                getFileName(symbol, 'dex', result.hdResolution),
                getFileName(symbol, 'dex', result.sdResolution),
                getFileName(symbol, 'gex', result.hdResolution),
                getFileName(symbol, 'gex', result.sdResolution),
            ]))
        };
    }
}

export const getSnapshotsAvailableForSymbol = (symbol: string) => {
    const result = Object.keys(OptionsSnapshotSummary)
        .filter((j) =>
            OptionsSnapshotSummary[j].tickers.includes(symbol)
        )
        .map((releaseName) => {
            const { zipAssetUrl, displayName: date, hdResolution, sdResolution, releasesBaseUrl } = OptionsSnapshotSummary[releaseName];
            const dexHdFileName = getFileName(symbol, 'dex', hdResolution);
            const dexSdFileName = getFileName(symbol, 'dex', sdResolution);
            const gexHdFileName = getFileName(symbol, 'gex', hdResolution);
            const gexSdFileName = getFileName(symbol, 'gex', sdResolution);

            return {
                date,
                dex: {
                    hdAssetUrl: zipAssetUrl ? `${snapshotCdnUrl}?f=${dexHdFileName}&dt=${date}&symbol=${symbol}` : `${releasesBaseUrl}/download/${releaseName}/${dexHdFileName}`,
                    sdAssetUrl: zipAssetUrl ? `${snapshotCdnUrl}?f=${dexSdFileName}&dt=${date}&symbol=${symbol}` : `${releasesBaseUrl}/download/${releaseName}/${dexSdFileName}`
                },
                gex: {
                    hdAssetUrl: zipAssetUrl ? `${snapshotCdnUrl}?f=${gexHdFileName}&dt=${date}&symbol=${symbol}` : `${releasesBaseUrl}/download/${releaseName}/${gexHdFileName}`,
                    sdAssetUrl: zipAssetUrl ? `${snapshotCdnUrl}?f=${gexSdFileName}&dt=${date}&symbol=${symbol}` : `${releasesBaseUrl}/download/${releaseName}/${gexSdFileName}`
                },
            }
        });
    return result;
}

// export const mapDataToLegacy = () => {
//     const intermediateData = getOptionsDataSummary();
//     return Object.keys(intermediateData).flatMap((j) => {
//         return Object.keys(intermediateData[j].symbols).map((k) => ({
//             symbol: k,
//             dt: intermediateData[j].displayName,
//         }));
//     });
// };

export const ghRepoBaseUrl = 'https://github.com/mnsrulz/mztrading-data/releases/download';

export const cleanSymbol = (symbol: string) => decodeURIComponent(symbol).replace(/\W/g, '');

export const allTickerSymbols = symbols as TickerSymbol[]

const fuse = new Fuse(allTickerSymbols, {
    keys: ["symbol", "name"],
    threshold: 0.2,
});

export const searchTicker = (q: string) => {
    const filtered = fuse.search(q, { limit: 25 }).map((x) => x.item);
    return filtered;
}

export const CboeOptionsRawSummary = (cboeOptionsSummary as CboeOptionSummaryType[]).map(({ name, optionsAssetUrl, stocksAssetUrl }) => ({ name, optionsAssetUrl, stocksAssetUrl, dt: name.substring((name.lastIndexOf('_') + 1)).substring(0, 10) }));

export const getCboeLatestDateAndSymbols = (forceDayId?: string) => {
    if (forceDayId) {
        if (optionsRollingSummary.symbolsSummary.some(k => k.dt == forceDayId)) {
            return {
                latestDate: forceDayId,
                symbols: optionsRollingSummary.symbolsSummary.filter(k => k.dt == forceDayId).map(k => k.symbol)    //.slice(0, 30) // Limit to 30 symbols for testing
            }
        } else {
            return null;
        }
    }

    const latestDate = optionsRollingSummary.symbolsSummary.map(k => k.dt).sort().pop();
    if (latestDate) {
        return {
            latestDate,
            symbols: optionsRollingSummary.symbolsSummary.filter(k => k.dt == latestDate).map(k => k.symbol)
        }
    }
    return null;
}


export const getSymbolExpirations = (symbol: string) => {
    const symbolExpirations = OptionsExpirationStrikes[symbol];
    if (!symbolExpirations) return [];
    const expirations = Object.keys(symbolExpirations).toSorted().map(k => {
        return {
            expiration: k,
            strikes: JSON.parse(symbolExpirations[k])
        }
    })
    //dup code. TODO: make it centralized
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
    return expirations.map(k => ({ ...k, isMonthly: monthlyExpirations.has(k.expiration) }));
}