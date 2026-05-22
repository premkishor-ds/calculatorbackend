const MarketStock = require('../models/MarketStock');
const ScreenerSync = require('../models/ScreenerSync');
const { getIndiaSymbolUniverse, getIstDateString } = require('../lib/india-symbol-master');
const { fetchAllMarketQuotes } = require('../lib/screener-quotes');

let syncInProgress = false;

async function runScreenerSync(options = {}) {
  const { force = false } = options;

  if (syncInProgress) {
    return { skipped: true, message: 'Sync already in progress' };
  }

  const asOfDate = getIstDateString();

  if (!force) {
    const stockCount = await MarketStock.countDocuments({ asOfDate });
    if (stockCount > 1000) {
      const existing = await ScreenerSync.findOne({ asOfDate }).lean();
      return {
        skipped: true,
        asOfDate,
        savedCount: existing?.savedCount || stockCount,
        message: 'Snapshot for today already exists',
      };
    }
  }

  syncInProgress = true;

  await ScreenerSync.findOneAndUpdate(
    { asOfDate },
    {
      asOfDate,
      status: 'running',
      startedAt: new Date(),
      completedAt: null,
      errorMessage: '',
    },
    { upsert: true, new: true }
  );

  try {
    console.log(`[ScreenerSync] Starting daily sync for ${asOfDate}...`);
    const universe = await getIndiaSymbolUniverse('all');
    const nseCount = universe.filter((s) => s.exchange === 'NSE').length;
    const bseCount = universe.filter((s) => s.exchange === 'BSE').length;

    const rows = await fetchAllMarketQuotes(universe, (done, total) => {
      if (done % 1000 === 0 || done === total) {
        console.log(`[ScreenerSync] Quotes ${done}/${total}`);
      }
    });

    const bulkOps = rows.map((row) => ({
      updateOne: {
        filter: { symbol: row.symbol, asOfDate },
        update: {
          $set: {
            ...row,
            asOfDate,
          },
        },
        upsert: true,
      },
    }));

    const BULK_CHUNK = 1000;
    for (let i = 0; i < bulkOps.length; i += BULK_CHUNK) {
      const chunk = bulkOps.slice(i, i + BULK_CHUNK);
      await MarketStock.bulkWrite(chunk, { ordered: false });
    }
    const savedCount = rows.length;

    // Remove stale rows for this date not in latest pull (optional cleanup)
    const validSymbols = rows.map((r) => r.symbol);
    await MarketStock.deleteMany({
      asOfDate,
      symbol: { $nin: validSymbols },
    });

    await ScreenerSync.findOneAndUpdate(
      { asOfDate },
      {
        status: 'completed',
        completedAt: new Date(),
        universeSize: universe.length,
        savedCount: rows.length,
        nseCount,
        bseCount,
        errorMessage: '',
      }
    );

    console.log(`[ScreenerSync] Completed ${asOfDate}: ${rows.length} stocks saved to MongoDB`);

    return {
      skipped: false,
      asOfDate,
      universeSize: universe.length,
      savedCount: rows.length,
      nseCount,
      bseCount,
    };
  } catch (err) {
    console.error('[ScreenerSync] Failed:', err);
    await ScreenerSync.findOneAndUpdate(
      { asOfDate },
      {
        status: 'failed',
        completedAt: new Date(),
        errorMessage: err.message || 'Unknown error',
      }
    );
    throw err;
  } finally {
    syncInProgress = false;
  }
}

async function getLatestAsOfDate() {
  const latest = await ScreenerSync.findOne({ status: 'completed' })
    .sort({ asOfDate: -1 })
    .lean();
  if (latest?.asOfDate) return latest.asOfDate;

  const stock = await MarketStock.findOne().sort({ asOfDate: -1 }).select('asOfDate').lean();
  return stock?.asOfDate || null;
}

async function ensureTodaySnapshot() {
  try {
    const asOfDate = getIstDateString();
    const count = await MarketStock.countDocuments({ asOfDate });
    if (count > 1000) {
      console.log(`[ScreenerSync] Today (${asOfDate}) already has ${count} stocks in DB`);
      return;
    }
    console.log(`[ScreenerSync] No snapshot for ${asOfDate} (${count} rows). Running initial sync...`);
    await runScreenerSync({ force: true });
  } catch (err) {
    console.error('[ScreenerSync] Startup sync failed:', err.message);
  }
}

module.exports = {
  runScreenerSync,
  getLatestAsOfDate,
  ensureTodaySnapshot,
  isSyncInProgress: () => syncInProgress,
};
