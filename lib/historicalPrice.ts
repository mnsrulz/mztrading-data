import ky from 'https://esm.sh/ky@1.8.2';
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