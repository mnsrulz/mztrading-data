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
    const { price } = await ky('https://live-quotes.mztrading.workers.dev/price?s=AAPL&dt=2026-07-16', {
        searchParams: {
            s: symbol,
            dt: dt,
            f: fallbackToPreviousDayWhenNoPriceFound ? '1' : '0',
            o: keepOriginalValue ? '1' : '0'
        }
    }).json<{ price?: number | string | null }>();
    return price;
}

// export const getLastNPrices = async (symbol: string, lastN: number, interval: 'd' | 'h') => {
//     const t = Math.ceil(interval == 'h' ? Math.ceil((lastN * 1.2) / 40) : Math.ceil((lastN * 1.2) / 5));       //take extra couple days of data just to be sure we have enough data
//     const start = dayjs().format('YYYY-MM-DD');
//     const resp = await yf.chart(EXCEPTION_SYMBOLS[symbol.toUpperCase()] || symbol, {
//         interval: interval == 'd' ? '1d' : '1h',
//         period1: dayjs(start).add(-t, 'week').toDate(),
//         period2: dayjs(start).toDate()
//     })
//     return resp.quotes.map(({ close, date }) => ({ close, date })).filter(k => k.close != null).map(({ close, date }) => ({ date, close: Number(close) })).slice(-lastN);
// }