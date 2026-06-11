const { yahooFinance, YAHOO_MODULE_OPTS } = require('../lib/yahoo-finance');
const Stock = require('../models/Stock');
const StockDetails = require('../models/StockDetails');
const StockSyncLog = require('../models/StockSyncLog');
const cron = require('node-cron');

/**
 * Sync details for a single symbol.
 * If cache is valid (< 24 hours), skips API request unless force=true.
 */
async function syncSingleStock(symbol, watchlistStockId, options = {}) {
  const { force = false } = options;
  const startedAt = new Date();
  symbol = symbol.trim().toUpperCase();

  try {
    // 1. Check Cache
    if (!force) {
      const existing = await StockDetails.findOne({ symbol });
      if (existing && existing.last_synced_at) {
        const diffMs = Date.now() - new Date(existing.last_synced_at).getTime();
        const diffHours = diffMs / (1000 * 60 * 60);
        if (diffHours < 24) {
          return {
            status: 'skipped',
            message: 'Cache valid (< 24h old)',
            data: existing,
          };
        }
      }
    }

    console.log(`[StockSync] Fetching Yahoo Finance data for ${symbol}...`);

    // 2. Fetch from Yahoo Finance
    let quote;
    try {
      quote = await yahooFinance.quote(symbol, {}, YAHOO_MODULE_OPTS);
    } catch (err) {
      if (err.message && (err.message.includes('Not Found') || err.message.includes('404'))) {
        throw new Error('Invalid stock symbol.');
      }
      throw new Error('Unable to connect to Yahoo Finance.');
    }

    if (!quote || quote.quoteType === 'NONE') {
      throw new Error('Invalid stock symbol.');
    }

    let summary = {};
    try {
      summary = await yahooFinance.quoteSummary(
        symbol,
        {
          modules: ['summaryProfile', 'financialData', 'defaultKeyStatistics', 'calendarEvents'],
        },
        YAHOO_MODULE_OPTS
      );
    } catch (err) {
      console.warn(`[StockSync] QuoteSummary failed for ${symbol}: ${err.message}. Proceeding with quote data only.`);
    }

    const summaryProfile = summary?.summaryProfile || {};
    const financialData = summary?.financialData || {};
    const defaultKeyStatistics = summary?.defaultKeyStatistics || {};
    const calendarEvents = summary?.calendarEvents || {};

    // Map fields
    const updateData = {
      watchlist_stock_id: watchlistStockId,
      symbol,
      company_name: quote.longName || quote.shortName || symbol,
      exchange: quote.exchange || '',
      sector: summaryProfile.sector || '',
      industry: summaryProfile.industry || '',
      market_cap: quote.marketCap ?? 0,
      enterprise_value: defaultKeyStatistics.enterpriseValue ?? 0,
      current_price: quote.regularMarketPrice ?? 0,
      previous_close: quote.regularMarketPreviousClose ?? 0,
      open_price: quote.regularMarketOpen ?? 0,
      day_high: quote.regularMarketDayHigh ?? 0,
      day_low: quote.regularMarketDayLow ?? 0,
      fifty_two_week_high: quote.fiftyTwoWeekHigh ?? 0,
      fifty_two_week_low: quote.fiftyTwoWeekLow ?? 0,
      volume: quote.regularMarketVolume ?? 0,
      average_volume: quote.averageDailyVolume3Month ?? quote.averageVolume ?? 0,
      pe_ratio: quote.trailingPE ?? 0,
      forward_pe: quote.forwardPE ?? 0,
      peg_ratio: defaultKeyStatistics.pegRatio ?? 0,
      price_to_book: quote.priceToBook ?? defaultKeyStatistics.priceToBook ?? 0,
      eps: quote.epsTrailingTwelveMonths ?? 0,
      dividend_yield: quote.dividendYield ?? (quote.trailingAnnualDividendYield ? quote.trailingAnnualDividendYield * 100 : 0),
      beta: defaultKeyStatistics.beta ?? 0,
      shares_outstanding: defaultKeyStatistics.sharesOutstanding ?? 0,
      float_shares: defaultKeyStatistics.floatShares ?? 0,
      revenue: financialData.totalRevenue ?? 0,
      gross_profit: financialData.grossMargins ? (financialData.grossMargins * (financialData.totalRevenue || 0)) : 0,
      operating_income: financialData.operatingCashflow ?? 0, // Fallback to operatingCashflow if operatingIncome is missing
      net_income: financialData.netIncomeToCommon ?? 0,
      free_cash_flow: financialData.freeCashflow ?? 0,
      total_assets: financialData.totalAssets ?? 0,
      total_liabilities: financialData.totalLiabilities ?? 0,
      cash: financialData.totalCash ?? 0,
      debt: financialData.totalDebt ?? 0,
      book_value: defaultKeyStatistics.bookValue ?? 0,
      roe: financialData.returnOnEquity ?? 0,
      roa: financialData.returnOnAssets ?? 0,
      profit_margin: financialData.profitMargins ?? 0,
      earnings_date: calendarEvents.earnings?.earningsDate?.[0] || null,
      analyst_target_price: financialData.targetMeanPrice ?? 0,
      recommendation: financialData.recommendationKey ?? '',
      last_synced_at: new Date(),
    };

    // 3. Update or Insert details
    const updatedDetails = await StockDetails.findOneAndUpdate(
      { symbol },
      { $set: updateData },
      { upsert: true, new: true }
    );

    // 3.5 Calculate and store advanced metrics
    try {
      const StockCalculationService = require('./stock-calculation');
      await StockCalculationService.calculateAndStore(symbol, watchlistStockId);
    } catch (calcErr) {
      console.error(`[StockSync] Advanced calculations failed for ${symbol}:`, calcErr.message);
    }

    // 4. Log success
    await StockSyncLog.create({
      stock_id: watchlistStockId,
      symbol,
      status: 'success',
      error_message: '',
      started_at: startedAt,
      completed_at: new Date(),
    });

    return {
      status: 'success',
      data: updatedDetails,
    };
  } catch (err) {
    console.error(`[StockSync] Failed sync for ${symbol}:`, err.message);

    // Log failure
    await StockSyncLog.create({
      stock_id: watchlistStockId,
      symbol,
      status: 'failed',
      error_message: err.message || 'Unknown error',
      started_at: startedAt,
      completed_at: new Date(),
    });

    throw err;
  }
}

/**
 * Sync all stocks in the watchlist
 */
async function syncAllStocks(options = {}) {
  const { force = false } = options;
  console.log(`[StockSync] Starting sync for all stocks (force=${force})...`);

  const stocks = await Stock.find({});
  let successCount = 0;
  let failCount = 0;
  let skippedCount = 0;

  for (const stock of stocks) {
    try {
      const res = await syncSingleStock(stock.symbol, stock._id, { force });
      if (res.status === 'skipped') {
        skippedCount++;
      } else {
        successCount++;
      }
    } catch (err) {
      failCount++;
      // Continue processing other stocks
    }
  }

  console.log(`[StockSync] Sync completed. Success: ${successCount}, Failed: ${failCount}, Skipped: ${skippedCount}`);
  return { successCount, failCount, skippedCount };
}

/**
 * Setup daily cron job
 */
function scheduleStockDetailsCron() {
  // Run daily after market close at 22:00 (10 PM) server time.
  const cronExpr = process.env.STOCK_DETAILS_CRON || '0 22 * * *';
  cron.schedule(cronExpr, async () => {
    console.log(`[StockSync] Starting scheduled daily sync...`);
    try {
      await syncAllStocks({ force: true });
    } catch (err) {
      console.error(`[StockSync] Daily cron failed:`, err);
    }
  });
  console.log(`[StockSync] Daily cron scheduled at: ${cronExpr}`);
}

module.exports = {
  syncSingleStock,
  syncAllStocks,
  scheduleStockDetailsCron,
};
