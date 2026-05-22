const mongoose = require('mongoose');

const marketStockSchema = new mongoose.Schema(
  {
    symbol: { type: String, required: true, trim: true, uppercase: true },
    name: { type: String, required: true, trim: true },
    exchange: { type: String, enum: ['NSE', 'BSE'], required: true },
    series: { type: String, default: '' },
    asOfDate: { type: String, required: true }, // YYYY-MM-DD (IST trading day snapshot)

    price: { type: Number, default: 0 },
    change: { type: Number, default: 0 },
    changePercent: { type: Number, default: 0 },
    marketCap: { type: Number, default: 0 },
    volume: { type: Number, default: 0 },
    pe: { type: Number, default: 0 },
    eps: { type: Number, default: 0 },
    cmpBv: { type: Number, default: 0 },
    divYield: { type: Number, default: 0 },
    promHold: { type: Number, default: 0 },
    profitGrowth: { type: Number, default: 0 },
    salesGrowth: { type: Number, default: 0 },
    roe: { type: Number, default: null },
    roa: { type: Number, default: null },
  },
  { timestamps: true }
);

marketStockSchema.index({ symbol: 1, asOfDate: 1 }, { unique: true });
marketStockSchema.index({ asOfDate: 1, exchange: 1 });
marketStockSchema.index({ asOfDate: 1, marketCap: -1 });
marketStockSchema.index({ asOfDate: 1, pe: 1 });
marketStockSchema.index({ asOfDate: 1, changePercent: 1 });

module.exports = mongoose.model('MarketStock', marketStockSchema);
