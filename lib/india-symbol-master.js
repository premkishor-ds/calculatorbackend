const NSE_CSV_URL = 'https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv';
const BSE_CSV_URL =
  'https://raw.githubusercontent.com/kanwalpreet18/canslimTechnical/master/DATA/bseSymbols.csv';

const FETCH_HEADERS = {
  'User-Agent': 'Mozilla/5.0 (compatible; VisionWealth/1.0)',
  Accept: 'text/csv,text/plain,*/*',
};

const universeCache = new Map();
const UNIVERSE_CACHE_MS = 6 * 60 * 60 * 1000;

function parseCsvLine(line) {
  const out = [];
  let cur = '';
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') {
      inQuotes = !inQuotes;
      continue;
    }
    if (ch === ',' && !inQuotes) {
      out.push(cur.trim());
      cur = '';
      continue;
    }
    cur += ch;
  }
  out.push(cur.trim());
  return out;
}

async function fetchNseSymbols() {
  const res = await fetch(NSE_CSV_URL, { headers: FETCH_HEADERS });
  if (!res.ok) throw new Error(`NSE symbol list failed (${res.status})`);

  const lines = (await res.text()).trim().split(/\r?\n/).slice(1);
  const symbols = [];

  for (const line of lines) {
    if (!line) continue;
    const cols = parseCsvLine(line);
    const ticker = cols[0]?.trim().toUpperCase();
    const name = cols[1]?.trim();
    const series = cols[2]?.trim();
    if (!ticker || !name) continue;
    symbols.push({
      symbol: `${ticker}.NS`,
      name,
      exchange: 'NSE',
      series: series || '',
    });
  }

  return symbols;
}

async function fetchBseSymbols() {
  const res = await fetch(BSE_CSV_URL, { headers: FETCH_HEADERS });
  if (!res.ok) throw new Error(`BSE symbol list failed (${res.status})`);

  const lines = (await res.text()).trim().split(/\r?\n/).slice(1);
  const symbols = [];

  for (const line of lines) {
    if (!line) continue;
    const cols = parseCsvLine(line);
    const status = cols[3]?.trim();
    if (status && status.toLowerCase() !== 'active') continue;
    const ticker = cols[1]?.trim().toUpperCase();
    const name = cols[2]?.trim();
    if (!ticker || !name) continue;
    symbols.push({
      symbol: `${ticker}.BO`,
      name,
      exchange: 'BSE',
      series: '',
    });
  }

  return symbols;
}

async function getIndiaSymbolUniverse(exchange = 'all') {
  const cacheKey = exchange;
  const cached = universeCache.get(cacheKey);
  if (cached && Date.now() - cached.fetchedAt < UNIVERSE_CACHE_MS) {
    return cached.symbols;
  }

  const [nse, bse] = await Promise.all([
    exchange === 'bse' ? Promise.resolve([]) : fetchNseSymbols(),
    exchange === 'nse' ? Promise.resolve([]) : fetchBseSymbols(),
  ]);

  let symbols;
  if (exchange === 'nse') symbols = nse;
  else if (exchange === 'bse') symbols = bse;
  else {
    const seen = new Set();
    symbols = [];
    for (const s of [...nse, ...bse]) {
      if (seen.has(s.symbol)) continue;
      seen.add(s.symbol);
      symbols.push(s);
    }
  }

  universeCache.set(cacheKey, { symbols, fetchedAt: Date.now() });
  return symbols;
}

function getIstDateString(date = new Date()) {
  return date.toLocaleDateString('en-CA', { timeZone: 'Asia/Kolkata' });
}

module.exports = {
  fetchNseSymbols,
  fetchBseSymbols,
  getIndiaSymbolUniverse,
  getIstDateString,
};
