import ky from "https://esm.sh/ky@1.8.2";

const client = ky.create({
    headers: {
        'Referer': 'https://www.cboe.com/',
        'Accept': 'application/json'
    },
    throwHttpErrors: false,
    retry: {
        statusCodes: [429],
        limit: 3
    },
    cache: 'no-cache'
});

const kvcboeanalytics = 'cboe-options-analytics';

const fetchOptionChainFromCboe = async (symbol: string) => {
    const url = `https://www-api.cboe.com/delayed_quote/api/v2/options/${symbol}`;

    let response = await client(url);
    
    if (!response.ok) throw new Error('error fetching options data');

    return await response.json<{
        data: {
            options: {
                option: string,
                open_interest: number,
                delta: number,
                volume: number,
                gamma: number,
                last_trade_price: number,
                ask: number,
                bid: number
            }[],
            close: number
        },
        timestamp: string
    }>();
}
const optionRegex = /(\w+)(\d{6})([CP])(\d+)/;

export const getOptionsChain = async (symbol: string) => {
    symbol = symbol.toUpperCase();
    const optionChain = await fetchOptionChainFromCboe(symbol);
    const currentPrice = optionChain.data.close;    //TODO: is this the close price which remains same if the market is open??
    const ts = new Date(optionChain.timestamp + 'Z');  //convert to UTC
    console.time(`getOptionsChain-mapping-${symbol}`)
    const mappedOptions = optionChain.data.options.map(({ option, open_interest, volume, delta, gamma, last_trade_price, ask, bid }) => {
        //implement mem cache for regex match??
        const rxMatch = optionRegex.exec(option);
        if (!rxMatch) throw new Error('error parsing option')

        return {
            strike: Number(`${rxMatch[4]}`) / 1000,
            expiration: `20${rxMatch[2].substring(0, 2)}-${rxMatch[2].substring(2, 4)}-${rxMatch[2].substring(4, 6)}`,
            open_interest,
            option_type: (rxMatch[3] == 'C' ? 'C' : 'P') as 'C' | 'P',
            volume,
            delta,
            gamma,
            last_trade_price,
            ask,
            bid
        }
    });
    console.timeEnd(`getOptionsChain-mapping-${symbol}`)
    return { data: mappedOptions, currentPrice, timestamp: ts };
}