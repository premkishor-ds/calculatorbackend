/**
 * Build MongoDB query from screener HTTP query params (matches frontend FilterSidebar ids).
 */
function num(val) {
  if (val === undefined || val === null || val === '') return undefined;
  const n = Number(val);
  return Number.isFinite(n) ? n : undefined;
}

function rangeClause(min, max) {
  const clause = {};
  if (min !== undefined) clause.$gte = min;
  if (max !== undefined) clause.$lte = max;
  return Object.keys(clause).length ? clause : null;
}

function buildScreenerQuery(query, asOfDate) {
  const mongo = { asOfDate };

  const exchange = (query.exchange || 'all').toLowerCase();
  if (exchange === 'nse') mongo.exchange = 'NSE';
  else if (exchange === 'bse') mongo.exchange = 'BSE';

  const fieldMap = {
    pe: 'pe',
    forwardPe: 'pe',
    pb: 'cmpBv',
    divYield: 'divYield',
    roe: 'roe',
    roa: 'roa',
    revenueGrowth: 'salesGrowth',
    profitGrowth: 'profitGrowth',
    salesGrowth: 'salesGrowth',
    promoterHolding: 'promHold',
    changePercent: 'changePercent',
    price: 'price',
  };

  for (const [param, field] of Object.entries(fieldMap)) {
    const min = num(query[`${param}Min`]);
    const max = num(query[`${param}Max`]);
    if (min === undefined && max === undefined) continue;

    let minVal = min;
    let maxVal = max;

    if (param === 'marketCap') {
      if (min !== undefined) minVal = min * 10000000;
      if (max !== undefined) maxVal = max * 10000000;
    }

    const clause = rangeClause(minVal, maxVal);
    if (clause) mongo[field] = clause;
  }

  const mcapMin = num(query.marketCapMin);
  const mcapMax = num(query.marketCapMax);
  if (mcapMin !== undefined || mcapMax !== undefined) {
    const clause = rangeClause(
      mcapMin !== undefined ? mcapMin * 10000000 : undefined,
      mcapMax !== undefined ? mcapMax * 10000000 : undefined
    );
    if (clause) mongo.marketCap = clause;
  }

  const q = (query.q || query.search || '').trim();
  if (q) {
    const re = new RegExp(q.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'i');
    mongo.$or = [{ symbol: re }, { name: re }];
  }

  return mongo;
}

function buildSort(query) {
  const sortField = query.sort || 'marketCap';
  const sortDir = query.sortDir === 'asc' ? 1 : -1;
  const allowed = [
    'symbol',
    'name',
    'price',
    'changePercent',
    'marketCap',
    'pe',
    'eps',
    'cmpBv',
    'divYield',
    'promHold',
    'profitGrowth',
    'salesGrowth',
  ];
  const field = allowed.includes(sortField) ? sortField : 'marketCap';
  return { [field]: sortDir };
}

module.exports = { buildScreenerQuery, buildSort, num };
