const mongoose = require('mongoose');

const stockMetricsSchema = new mongoose.Schema(
  {
    stock_id: {
      type: mongoose.Schema.Types.ObjectId,
      ref: 'Stock',
      required: true,
      index: true,
    },
    symbol: {
      type: String,
      required: true,
      unique: true,
      trim: true,
      uppercase: true,
      index: true,
    },
    sma_20: { type: Number, default: 0 },
    sma_50: { type: Number, default: 0 },
    sma_100: { type: Number, default: 0 },
    sma_200: { type: Number, default: 0 },
    ema_9: { type: Number, default: 0 },
    ema_20: { type: Number, default: 0 },
    ema_50: { type: Number, default: 0 },
    ema_200: { type: Number, default: 0 },
    rsi_14: { type: Number, default: 0 },
    macd: { type: Number, default: 0 },
    macd_signal: { type: Number, default: 0 },
    macd_histogram: { type: Number, default: 0 },
    atr: { type: Number, default: 0 },
    bollinger_upper: { type: Number, default: 0 },
    bollinger_middle: { type: Number, default: 0 },
    bollinger_lower: { type: Number, default: 0 },
    volatility: { type: Number, default: 0 },
    vwap: { type: Number, default: 0 },
    obv: { type: Number, default: 0 },
    stochastic_rsi: { type: Number, default: 0 },
    williams_r: { type: Number, default: 0 },
    cci: { type: Number, default: 0 },
  },
  {
    timestamps: {
      createdAt: 'created_at',
      updatedAt: 'updated_at',
    },
  }
);

module.exports = mongoose.model('StockMetrics', stockMetricsSchema);
