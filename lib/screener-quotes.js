const { yahooFinance, YAHOO_OPTS } = require('./yahoo-finance');

const QUOTE_BATCH_SIZE = 500;

function mapQuoteToRow(quote, listing) {
  const price = quote.regularMarketPrice ?? 0;
  const bookValue = quote.bookValue ?? 0;
  const cmpBv =
    quote.priceToBook != null && quote.priceToBook > 0
      ? Number(quote.priceToBook.toFixed(2))
      : bookValue > 0
        ? Number((price / bookValue).toFixed(2))
        : 0;
  const rawDivYield = quote.dividendYield ?? quote.trailingAnnualDividendYield ?? 0;

  return {
    symbol: (quote.symbol || listing.symbol || '').toUpperCase(),
    name: quote.shortName || quote.longName || listing.name || '',
    exchange: listing.exchange,
    series: listing.series || '',
    price,
    change: quote.regularMarketChange ?? 0,
    changePercent: (quote.regularMarketChangePercent ?? 0) * 100,
    marketCap: quote.marketCap ?? 0,
    volume: quote.regularMarketVolume ?? 0,
    pe: quote.trailingPE ?? quote.forwardPE ?? 0,
    eps: quote.epsTrailingTwelveMonths ?? quote.epsForward ?? 0,
    cmpBv,
    divYield: Number((rawDivYield * 100).toFixed(2)),
    promHold: 0,
    profitGrowth: 0,
    salesGrowth: 0,
    roe: null,
    roa: null,
  };
}

function normalizeQuotes(raw) {
  if (!raw) return [];
  if (Array.isArray(raw)) return raw;
  if (typeof raw === 'object') return Object.values(raw);
  return [];
}

async function fetchQuoteBatchOnce(tickers, listingBySymbol) {
  if (tickers.length === 0) return [];

  const raw = await yahooFinance.quote(tickers, { return: 'array' }, YAHOO_OPTS);
  const quotes = normalizeQuotes(raw);
  const rows = [];

  for (const quote of quotes) {
    if (!quote?.symbol || quote.quoteType === 'NONE') continue;
    if (!quote.symbol.endsWith('.NS') && !quote.symbol.endsWith('.BO')) continue;
    const listing = listingBySymbol.get(quote.symbol.toUpperCase()) || {
      symbol: quote.symbol,
      name: '',
      exchange: quote.symbol.endsWith('.BO') ? 'BSE' : 'NSE',
      series: '',
    };
    rows.push(mapQuoteToRow(quote, listing));
  }

  return rows;
}

async function fetchQuoteBatch(tickers, listingBySymbol) {
  try {
    return await fetchQuoteBatchOnce(tickers, listingBySymbol);
  } catch (err) {
    if (tickers.length <= 1) {
      console.warn(`[ScreenerSync] Quote skip ${tickers[0]}: ${err.message}`);
      return [];
    }
    const mid = Math.ceil(tickers.length / 2);
    const left = await fetchQuoteBatch(tickers.slice(0, mid), listingBySymbol);
    const right = await fetchQuoteBatch(tickers.slice(mid), listingBySymbol);
    return [...left, ...right];
  }
}

async function fetchAllMarketQuotes(universe, onProgress) {
  const listingBySymbol = new Map(universe.map((s) => [s.symbol.toUpperCase(), s]));
  const tickers = universe.map((s) => s.symbol);
  const allRows = [];

  for (let i = 0; i < tickers.length; i += QUOTE_BATCH_SIZE) {
    const batch = tickers.slice(i, i + QUOTE_BATCH_SIZE);
    const rows = await fetchQuoteBatch(batch, listingBySymbol);
    allRows.push(...rows);
    if (onProgress) onProgress(allRows.length, tickers.length);
  }

  return allRows;
}

module.exports = {
  QUOTE_BATCH_SIZE,
  mapQuoteToRow,
  fetchQuoteBatch,
  fetchAllMarketQuotes,
};
