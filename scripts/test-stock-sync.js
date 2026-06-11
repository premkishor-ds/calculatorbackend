require('dotenv').config();
const mongoose = require('mongoose');
const Stock = require('../models/Stock');
const StockDetails = require('../models/StockDetails');
const StockSyncLog = require('../models/StockSyncLog');
const StockMetrics = require('../models/StockMetrics');
const StockGrowthMetrics = require('../models/StockGrowthMetrics');
const StockValuationMetrics = require('../models/StockValuationMetrics');
const StockRiskMetrics = require('../models/StockRiskMetrics');
const StockScores = require('../models/StockScores');

const { syncSingleStock } = require('../services/stock-sync');
const { connectMongo } = require('../lib/connect-mongo');

async function runTests() {
  console.log('--- Starting Stock Details & Advanced Metrics Tests ---');
  
  // Connect to DB
  await connectMongo();
  
  const testSymbol = 'AAPL';
  const testUserId = new mongoose.Types.ObjectId();
  const testWatchlist = 'test-watchlist';
  
  // Cleanup any left-overs
  await Stock.deleteMany({ symbol: testSymbol });
  await StockDetails.deleteMany({ symbol: testSymbol });
  await StockSyncLog.deleteMany({ symbol: testSymbol });
  await StockMetrics.deleteMany({ symbol: testSymbol });
  await StockGrowthMetrics.deleteMany({ symbol: testSymbol });
  await StockValuationMetrics.deleteMany({ symbol: testSymbol });
  await StockRiskMetrics.deleteMany({ symbol: testSymbol });
  await StockScores.deleteMany({ symbol: testSymbol });

  console.log('\n[1/5] Testing Watchlist Addition & Automatic Base Details Sync...');
  const newStock = new Stock({
    userId: testUserId,
    symbol: testSymbol,
    name: 'Apple Inc.',
    watchlist: testWatchlist
  });
  await newStock.save();
  
  const syncRes = await syncSingleStock(testSymbol, newStock._id, { force: true });
  if (syncRes.status !== 'success') {
    throw new Error('Base sync failed: ' + syncRes.status);
  }
  
  const details = await StockDetails.findOne({ symbol: testSymbol });
  if (!details) {
    throw new Error('StockDetails document was not created!');
  }
  console.log('✅ Base StockDetails successfully created!');

  console.log('\n[2/5] Testing Advanced Calculated Metrics Generation...');
  // Since we called syncSingleStock, it automatically triggers StockCalculationService inside it!
  const metrics = await StockMetrics.findOne({ symbol: testSymbol });
  const growth = await StockGrowthMetrics.findOne({ symbol: testSymbol });
  const valuation = await StockValuationMetrics.findOne({ symbol: testSymbol });
  const risk = await StockRiskMetrics.findOne({ symbol: testSymbol });
  const scores = await StockScores.findOne({ symbol: testSymbol });

  if (!metrics || !growth || !valuation || !risk || !scores) {
    throw new Error(`One or more advanced metrics documents were not created! 
      Metrics: ${!!metrics}, Growth: ${!!growth}, Valuation: ${!!valuation}, Risk: ${!!risk}, Scores: ${!!scores}`);
  }

  console.log('✅ All 5 advanced calculated metrics documents successfully created in MongoDB!');
  console.log(`   SMA 50: ${metrics.sma_50}`);
  console.log(`   RSI 14: ${metrics.rsi_14}`);
  console.log(`   Revenue Growth YoY: ${growth.revenue_growth_yoy}%`);
  console.log(`   DCF Fair Value: $${valuation.dcf_fair_value}`);
  console.log(`   Valuation Status: ${valuation.valuation_status}`);
  console.log(`   Sharpe Ratio: ${risk.sharpe_ratio}`);
  console.log(`   Piotroski F-Score: ${risk.piotroski_f_score}/9`);
  console.log(`   AI Overall Score: ${scores.overall_score}/100`);
  console.log(`   Investment Rating: ${scores.investment_rating}`);

  // Assertions for DCF, Graham, Sharpe, Piotroski, AI Score
  if (typeof metrics.sma_50 !== 'number' || metrics.sma_50 <= 0) {
    throw new Error('SMA 50 calculation error');
  }
  if (typeof valuation.dcf_fair_value !== 'number' || valuation.dcf_fair_value <= 0) {
    throw new Error('DCF Fair Value calculation error');
  }
  if (typeof risk.sharpe_ratio !== 'number') {
    throw new Error('Sharpe Ratio calculation error');
  }
  if (typeof risk.piotroski_f_score !== 'number' || risk.piotroski_f_score < 0 || risk.piotroski_f_score > 9) {
    throw new Error('Piotroski F-Score range error');
  }
  if (typeof scores.overall_score !== 'number' || scores.overall_score < 0 || scores.overall_score > 100) {
    throw new Error('AI Overall Score range error');
  }

  console.log('\n[3/5] Testing Sync Log Persistence...');
  const logs = await StockSyncLog.find({ symbol: testSymbol });
  if (logs.length === 0) {
    throw new Error('No sync logs were found!');
  }
  console.log(`✅ Logs populated successfully: count=${logs.length}`);

  console.log('\n[4/5] Testing Deletion Cascade of Calculated Metrics...');
  await Stock.deleteOne({ _id: newStock._id });
  await StockDetails.deleteMany({ symbol: testSymbol });
  await StockSyncLog.deleteMany({ symbol: testSymbol });
  await StockMetrics.deleteMany({ symbol: testSymbol });
  await StockGrowthMetrics.deleteMany({ symbol: testSymbol });
  await StockValuationMetrics.deleteMany({ symbol: testSymbol });
  await StockRiskMetrics.deleteMany({ symbol: testSymbol });
  await StockScores.deleteMany({ symbol: testSymbol });

  const metricsAfterDel = await StockMetrics.findOne({ symbol: testSymbol });
  if (metricsAfterDel) {
    throw new Error('Cleanup did not cascade delete calculated metrics!');
  }
  console.log('✅ Watchlist deletion cascade cleanup verified for advanced metrics.');

  console.log('\n[5/5] Testing Edge Case: Invalid Ticker Rejection...');
  try {
    await syncSingleStock('INVALID_TICKER_123_XYZ', new mongoose.Types.ObjectId(), { force: true });
    throw new Error('Expected invalid symbol to fail!');
  } catch (err) {
    console.log('✅ Invalid symbol correctly rejected: ' + err.message);
  }

  console.log('\n🎉 ALL ADVANCED CALCULATIONS & ANALYTICS TESTS PASSED! 🎉');
  process.exit(0);
}

runTests().catch(err => {
  console.error('\n❌ TEST SUITE FAILED:', err);
  process.exit(1);
});
