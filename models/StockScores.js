const mongoose = require('mongoose');

const stockScoresSchema = new mongoose.Schema(
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
    technical_score: { type: Number, default: 0 },
    fundamental_score: { type: Number, default: 0 },
    value_score: { type: Number, default: 0 },
    risk_score: { type: Number, default: 0 },
    overall_score: { type: Number, default: 0 },
    investment_rating: {
      type: String,
      enum: ['Strong Buy', 'Buy', 'Hold', 'Sell', 'Strong Sell'],
      default: 'Hold',
    },
  },
  {
    timestamps: {
      createdAt: 'created_at',
      updatedAt: 'updated_at',
    },
  }
);

module.exports = mongoose.model('StockScores', stockScoresSchema);
