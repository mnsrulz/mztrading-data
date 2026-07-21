import { getOIAnomalConnection } from "./historicalOptions.ts";

const baseCte = `
WITH T AS(
            SELECT CAST(dt as STRING) as dt,
            option, option_symbol,
            CAST(expiration as STRING) as expiration,
            dte, option_type,
            strike,
            delta,
            gamma,
            open_interest,
            volume,
            prev_open_interest,
            oi_change,
            anomaly_score 
                    FROM 'oianomaly.parquet'
        )
`;

type OIAnomalySearchRequestParams = {
    facets: string | string[],
    facetFilters: [string[]],
    query: string,
    page: number,
    hitsPerPage: number,
    numericFilters: string[]
}

type OIAnomalyFacetSearchRequestParams = OIAnomalySearchRequestParams & {
    facetName: string,
    facetQuery: string
};

export type OIAnomalySearchRequest = {
    indexName: string,
    params: OIAnomalySearchRequestParams
}

export type OIAnomalyFacetSearchRequestType = Omit<OIAnomalySearchRequest, "params"> & {
    params: OIAnomalyFacetSearchRequestParams
};

export type OIAnomalySearchResponse = {
    results: {
        hits: {}[],
        nbHits: number,
        nbPages: number,
        page: number,
        hitsPerPage: number,
        processingTimeMS: number,
        facets: Record<string, { name: string, count: number }[]>,
    }[]
}

export const queryOIAnomalySearch = async (request: OIAnomalySearchRequest[]): Promise<OIAnomalySearchResponse> => {
    const mainRequest = request.find(k => Array.isArray(k.params.facets));
    if (!mainRequest) throw new Error(`no main request found!`);
    const perPage = mainRequest.params.hitsPerPage || 12;
    const offset = (mainRequest.params.page || 0) * perPage;

    const { items, count } = await executeMainQuery(mainRequest, perPage, offset);
    const facetValues = await executeFacet(request, mainRequest);
    return {
        results: [
            {
                hits: items,
                facets: facetValues,
                page: 0,
                nbHits: count,
                nbPages: Math.ceil((count / perPage)),
                hitsPerPage: perPage,
                processingTimeMS: 100
            }
        ]
    }
}

export const queryOIAnomalyFacetSearch = async (request: OIAnomalyFacetSearchRequestType) => {
    const result = await executeFacetSearch(request);
    return {
        facetHits: result.map(({ value, count }) => ({ value, count, highlighted: `${value}` })),
        exhaustiveFacetsCount: true,
        processingTimeMS: 100,
        "exhaustive": {
            "facetsCount": true
        }
    }
}

async function executeMainQuery(request: OIAnomalySearchRequest, perPage: number, offset: number) {
    const conn = await getOIAnomalConnection();
    const { params, indexName } = request;
    const query = buildFacetQuery(params);
    console.log(`executing main query: ${query} `);
    const baseQuery = `
    ${baseCte},
    T1 AS (
        SELECT * FROM T
        ${query && 'WHERE ' + query}
    )
    `
    const countsResult = await conn.send(`
        ${baseQuery}
        
        SELECT COUNT(1) as count FROM T1
            `);
    const counts = countsResult.readAll().flatMap(k => k.toArray().map((row) => row.toJSON())) as { count: number }[];

    const arrowResult = await conn.send(`
                ${baseQuery}

                SELECT * FROM T1
                ORDER BY ${indexName} desc
                LIMIT ${perPage} OFFSET ${offset}
            `);
    const items = arrowResult.readAll().flatMap(k => k.toArray().map((row) => row.toJSON())) as {
        dt: string, option: string, option_symbol: string, expiration: string, dte: number, strike: number, delta: number, gamma: number,
        open_interest: number, volume: number, prev_open_interest: number, oi_change: number, anomaly_score: number
    }[];

    return {
        items,
        count: counts[0].count
    };
}

async function executeFacet(request: OIAnomalySearchRequest[], mainRequest: OIAnomalySearchRequest) {
    const facetValues: Record<string, any> = {};
    const conn = await getOIAnomalConnection();
    const availableFacets = mainRequest.params.facets as string[];
    const processedFacets: string[] = [];
    for (const req of request) {
        const { params } = req;
        if (typeof params.facets === 'string') {
            const query = buildFacetQuery(params);
            const result = await conn.send(`
                ${baseCte}
                SELECT ${params.facets}, COUNT(1) cnt FROM T
                ${query && 'WHERE ' + query} 
                GROUP BY ${params.facets}`);
            facetValues[params.facets] = result.readAll().flatMap(k => k.toArray().map((row) => row.toJSON())).reduce((r, c) => { r[c[params.facets as string]] = c['cnt']; return r; }, {})
            processedFacets.push(params.facets);
        }
    }

    for (const facet of availableFacets) {
        if (processedFacets.includes(facet)) continue;
        const { params } = mainRequest;
        const query = buildFacetQuery(params);
        console.log(`executing query for facet: ${facet}: ${query} `);

        const result = await conn.send(`
            ${baseCte}
            SELECT ${facet}, COUNT(1) cnt FROM T
            ${query && 'WHERE ' + query} 
            GROUP BY ${facet} `);
        facetValues[facet] = result.readAll().flatMap(k => k.toArray().map((row) => row.toJSON())).reduce((r, c) => { r[c[facet]] = c['cnt']; return r; }, {})
    }

    return facetValues;
}

function buildFacetQuery(params: OIAnomalySearchRequestParams | OIAnomalyFacetSearchRequestParams) {
    let query = params.facetFilters && params.facetFilters.map(f => {
        const innerk = f.map(k => {
            const [key, value] = k.split(':');
            return `${key} = '${value}'`;
        }).join(' OR ');
        if (innerk) {
            return `(${innerk})`;
        }
    }).join(' AND ');
    if (params.numericFilters && params.numericFilters.length > 0) {
        const numericQuery = params.numericFilters.join(' AND ');
        query = query ? `${query} AND ${numericQuery}` : numericQuery;
    }

    const t = params as OIAnomalyFacetSearchRequestParams;
    if (t && t.facetName && t.facetQuery) {
        const facetQuery = `${t.facetName} ILIKE '%${t.facetQuery}%'`;
        query = query ? `${query} AND ${facetQuery}` : facetQuery;
    }

    return query;
}

async function executeFacetSearch(request: OIAnomalyFacetSearchRequestType) {
    const conn = await getOIAnomalConnection();
    const { params } = request;
    const query = buildFacetQuery(params);
    const result = await conn.send(`
                ${baseCte}
                SELECT ${params.facetName} AS value, COUNT(1) AS count FROM T
                ${query && 'WHERE ' + query} 
                GROUP BY ${params.facetName} `);
    const resultSet = result.readAll().flatMap(k => k.toArray().map((row) => row.toJSON())) as { value: string, count: number }[];
    return resultSet;
}