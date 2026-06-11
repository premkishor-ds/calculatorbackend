const mongoose = require('mongoose');

const stockSyncLogSchema = new mongoose.Schema(
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
      trim: true,
      uppercase: true,
      index: true,
    },
    status: {
      type: String,
      required: true,
      enum: ['success', 'failed'],
      index: true,
    },
    error_message: {
      type: String,
      default: '',
    },
    started_at: {
      type: Date,
      required: true,
    },
    completed_at: {
      type: Date,
      required: true,
    },
    created_at: {
      type: Date,
      default: Date.now,
    },
  },
  {
    // Disable automatic timestamps since we manually set started_at, completed_at, and created_at
    timestamps: false,
  }
);

module.exports = mongoose.model('StockSyncLog', stockSyncLogSchema);
