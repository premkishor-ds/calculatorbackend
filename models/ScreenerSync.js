const mongoose = require('mongoose');

const screenerSyncSchema = new mongoose.Schema(
  {
    asOfDate: { type: String, required: true, unique: true },
    status: {
      type: String,
      enum: ['running', 'completed', 'failed'],
      default: 'running',
    },
    startedAt: { type: Date, default: Date.now },
    completedAt: { type: Date },
    universeSize: { type: Number, default: 0 },
    savedCount: { type: Number, default: 0 },
    nseCount: { type: Number, default: 0 },
    bseCount: { type: Number, default: 0 },
    errorMessage: { type: String, default: '' },
  },
  { timestamps: true }
);

module.exports = mongoose.model('ScreenerSync', screenerSyncSchema);
