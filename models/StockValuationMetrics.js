const mongoose = require('mongoose');

const stockValuationMetricsSchema = new mongoose.Schema(
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
    dcf_fair_value: { type: Number, default: 0 },
    graham_number: { type: Number, default: 0 },
    peter_lynch_value: { type: Number, default: 0 },
    margin_of_safety: { type: Number, default: 0 },
    upside_percentage: { type: Number, default: 0 },
    valuation_status: {
      type: String,
      enum: ['Undervalued', 'Fairly Valued', 'Overvalued'],
      default: 'Fairly Valued',
    },
  },
  {
    timestamps: {
      createdAt: 'created_at',
      updatedAt: 'updated_at',
    },
  }
);

module.exports = mongoose.model('StockValuationMetrics', stockValuationMetricsSchema);
