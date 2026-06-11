const mongoose = require('mongoose');

const stockGrowthMetricsSchema = new mongoose.Schema(
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
    revenue_growth_qoq: { type: Number, default: 0 },
    revenue_growth_yoy: { type: Number, default: 0 },
    earnings_growth_qoq: { type: Number, default: 0 },
    earnings_growth_yoy: { type: Number, default: 0 },
    profit_growth_qoq: { type: Number, default: 0 },
    profit_growth_yoy: { type: Number, default: 0 },
    free_cash_flow_growth: { type: Number, default: 0 },
    roe_growth: { type: Number, default: 0 },
    book_value_growth: { type: Number, default: 0 },
  },
  {
    timestamps: {
      createdAt: 'created_at',
      updatedAt: 'updated_at',
    },
  }
);

module.exports = mongoose.model('StockGrowthMetrics', stockGrowthMetricsSchema);
