const mongoose = require('mongoose');

const stockRiskMetricsSchema = new mongoose.Schema(
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
    annualized_volatility: { type: Number, default: 0 },
    sharpe_ratio: { type: Number, default: 0 },
    sortino_ratio: { type: Number, default: 0 },
    max_drawdown: { type: Number, default: 0 },
    calmar_ratio: { type: Number, default: 0 },
    beta_calculated: { type: Number, default: 0 },
    altman_z_score: { type: Number, default: 0 },
    piotroski_f_score: { type: Number, default: 0 },
    beneish_m_score: { type: Number, default: 0 },
    risk_score: { type: Number, default: 0 },
    risk_level: {
      type: String,
      enum: ['Low', 'Medium', 'High'],
      default: 'Medium',
    },
  },
  {
    timestamps: {
      createdAt: 'created_at',
      updatedAt: 'updated_at',
    },
  }
);

module.exports = mongoose.model('StockRiskMetrics', stockRiskMetricsSchema);
