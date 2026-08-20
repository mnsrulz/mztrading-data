import ky from 'https://esm.sh/ky@1.8.2';

type WorkerOhlcRow = { date: string; open: number; high: number; low: number; close: number; volume: number; adjclose?: number };
export type OptionOhlcRow = { dt: string; open: number; high: number; low: number; close: number; volume: number };

export const mapWorkerOhlcRow = (row: WorkerOhlcRow): OptionOhlcRow => ({
    dt: row.date,
    open: row.open,
    high: row.high,
    low: row.low,
    close: row.close,
    volume: row.volume,
});

export const getOptionHistoricalOhlc = async (contractId: string, n?: number) => {
    const rows = await ky('https://live-quotes.mztrading.workers.dev/ohlc', {
        searchParams: { s: contractId, ...(n ? { n: String(n) } : {}) }
    }).json<WorkerOhlcRow[]>();
    return rows.map(mapWorkerOhlcRow);
}
export async function getPriceAtDate(
    symbol: string,
    dt: string,
    fallbackToPreviousDayWhenNoPriceFound: boolean,
    keepOriginalValue: true,
): Promise<number | undefined | null>;

export async function getPriceAtDate(
    symbol: string,
    dt: string,
    fallbackToPreviousDayWhenNoPriceFound: boolean,
    keepOriginalValue?: false
): Promise<string | undefined | null>;

export async function getPriceAtDate(symbol: string, dt: string, fallbackToPreviousDayWhenNoPriceFound: boolean, keepOriginalValue = true) {
    const { price } = await ky('https://live-quotes.mztrading.workers.dev/price', {
        searchParams: {
            s: symbol,
            dt: dt,
            f: fallbackToPreviousDayWhenNoPriceFound ? '1' : '0',
            o: keepOriginalValue ? '1' : '0'
        }
    }).json<{ price?: number | string | null }>();
    return price;
}

export const getLastNPrices = async (symbol: string, lastN: number, interval: 'd' | 'h') => {
    const { prices } = await ky('https://live-quotes.mztrading.workers.dev/prices', {
        searchParams: {
            s: symbol,
            n: lastN,
            i: interval
        }
    }).json<{
        prices: {
            date: Date,
            close: number
        }[]
    }>();

    return prices;
}