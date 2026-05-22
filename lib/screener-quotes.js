const {
  yahooFinance,
  YAHOO_MODULE_OPTS,
  QUOTE_ARRAY_OPTS,
  isYahooRateLimitError,
} = require('./yahoo-finance');

/** Small batches + long pauses — Yahoo rate-limits Render/datacenter IPs quickly */
const QUOTE_BATCH_SIZE = parseInt(process.env.SCREENER_BATCH_SIZE || '20', 10);
const BATCH_DELAY_MS = parseInt(process.env.SCREENER_BATCH_DELAY_MS || '4000', 10);
const RATE_LIMIT_PAUSE_MS = parseInt(process.env.SCREENER_RATE_LIMIT_PAUSE_MS || '90000', 10);
const MAX_RETRIES = 5;
const BACKFILL_MAX = parseInt(process.env.SCREENER_BACKFILL_MAX || '0', 10);

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

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

function rowFromQuote(quote, listingBySymbol) {
  if (!quote?.symbol || quote.quoteType === 'NONE') return null;
  if (!quote.symbol.endsWith('.NS') && !quote.symbol.endsWith('.BO')) return null;
  const sym = quote.symbol.toUpperCase();
  const listing = listingBySymbol.get(sym) || {
    symbol: sym,
    name: '',
    exchange: sym.endsWith('.BO') ? 'BSE' : 'NSE',
    series: '',
  };
  return mapQuoteToRow(quote, listing);
}

async function fetchQuoteBatchOnce(tickers, listingBySymbol) {
  if (tickers.length === 0) return [];

  const raw = await yahooFinance.quote(tickers, QUOTE_ARRAY_OPTS, YAHOO_MODULE_OPTS);
  const quotes = normalizeQuotes(raw);
  const rows = [];

  for (const quote of quotes) {
    const row = rowFromQuote(quote, listingBySymbol);
    if (row) rows.push(row);
  }

  return rows;
}

async function fetchQuoteSingle(ticker, listingBySymbol) {
  const raw = await yahooFinance.quote(ticker, {}, YAHOO_MODULE_OPTS);
  const quote = Array.isArray(raw) ? raw[0] : raw;
  const row = rowFromQuote(quote, listingBySymbol);
  return row ? [row] : [];
}

async function fetchQuoteBatchWithRetry(tickers, listingBySymbol) {
  if (tickers.length === 0) return [];

  let lastErr = null;
  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      const rows = await fetchQuoteBatchOnce(tickers, listingBySymbol);
      if (rows.length > 0 || tickers.length <= 2) return rows;
      lastErr = new Error('Empty batch response');
    } catch (err) {
      lastErr = err;
      if (isYahooRateLimitError(err)) {
        console.warn(
          `[ScreenerSync] Yahoo 429/rate limit (batch ${tickers.length}), waiting ${RATE_LIMIT_PAUSE_MS / 1000}s…`
        );
        await sleep(RATE_LIMIT_PAUSE_MS);
      } else if (attempt < MAX_RETRIES) {
        await sleep(2000 * attempt);
      }
    }
  }

  if (tickers.length <= 1) {
    try {
      return await fetchQuoteSingle(tickers[0], listingBySymbol);
    } catch (err) {
      if (!isYahooRateLimitError(err)) {
        console.warn(`[ScreenerSync] Quote skip ${tickers[0]}: ${err.message}`);
      }
      return [];
    }
  }

  if (isYahooRateLimitError(lastErr)) {
    await sleep(RATE_LIMIT_PAUSE_MS);
  }

  const mid = Math.ceil(tickers.length / 2);
  const left = await fetchQuoteBatchWithRetry(tickers.slice(0, mid), listingBySymbol);
  const right = await fetchQuoteBatchWithRetry(tickers.slice(mid), listingBySymbol);
  const merged = [...left, ...right];

  if (merged.length > 0) return merged;

  if (lastErr && !isYahooRateLimitError(lastErr)) {
    console.warn(`[ScreenerSync] Batch failed (${tickers.length}): ${lastErr.message}`);
  }
  return [];
}

async function backfillMissingQuotes(tickers, listingBySymbol, existingRows) {
  if (BACKFILL_MAX <= 0) return existingRows;

  const got = new Set(existingRows.map((r) => r.symbol));
  const missing = tickers.filter((t) => !got.has(t.toUpperCase()));
  if (missing.length === 0) return existingRows;

  const toFetch = missing.slice(0, BACKFILL_MAX);
  console.log(`[ScreenerSync] Backfilling ${toFetch.length}/${missing.length} missing quotes…`);

  const extra = [];
  for (let i = 0; i < toFetch.length; i++) {
    try {
      const rows = await fetchQuoteSingle(toFetch[i], listingBySymbol);
      extra.push(...rows);
    } catch (err) {
      if (isYahooRateLimitError(err)) {
        console.warn('[ScreenerSync] Backfill paused — rate limited');
        break;
      }
    }
    await sleep(3000);
  }

  return [...existingRows, ...extra];
}

async function fetchAllMarketQuotes(universe, onProgress) {
  const listingBySymbol = new Map(universe.map((s) => [s.symbol.toUpperCase(), s]));
  const tickers = universe.map((s) => s.symbol);
  const allRows = [];
  const batchCount = Math.ceil(tickers.length / QUOTE_BATCH_SIZE);

  console.log(
    `[ScreenerSync] Fetching quotes: ${tickers.length} symbols, batch=${QUOTE_BATCH_SIZE}, delay=${BATCH_DELAY_MS}ms`
  );

  for (let i = 0; i < tickers.length; i += QUOTE_BATCH_SIZE) {
    const batchNum = Math.floor(i / QUOTE_BATCH_SIZE) + 1;
    const batch = tickers.slice(i, i + QUOTE_BATCH_SIZE);
    const rows = await fetchQuoteBatchWithRetry(batch, listingBySymbol);
    allRows.push(...rows);
    if (onProgress) onProgress(allRows.length, tickers.length);
    if (batchNum % 10 === 0 || batchNum === batchCount) {
      console.log(`[ScreenerSync] Progress ${batchNum}/${batchCount} — ${allRows.length} quotes so far`);
    }
    if (i + QUOTE_BATCH_SIZE < tickers.length) await sleep(BATCH_DELAY_MS);
  }

  const filled = await backfillMissingQuotes(tickers, listingBySymbol, allRows);
  console.log(`[ScreenerSync] Fetched ${filled.length}/${tickers.length} quotes`);
  return filled;
}

module.exports = {
  QUOTE_BATCH_SIZE,
  mapQuoteToRow,
  fetchQuoteBatchWithRetry,
  fetchAllMarketQuotes,
};
