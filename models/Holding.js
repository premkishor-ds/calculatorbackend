const mongoose = require('mongoose');

const HoldingSchema = new mongoose.Schema({
  userId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: 'User',
    required: false,
    index: true
  },
  symbol: {
    type: String,
    required: true,
    uppercase: true,
    trim: true,
  },
  name: {
    type: String,
    required: true,
    trim: true,
  },
  buyPrice: {
    type: Number,
    required: true,
    min: 0,
  },
  quantity: {
    type: Number,
    required: true,
    min: 1,
  },
  purchaseDate: {
    type: Date,
    default: Date.now,
  },
  watchlist: {
    type: String,
    default: 'default',
    trim: true,
  },
  transactionType: {
    type: String,
    enum: ['buy', 'sell', 'dividend', 'bonus', 'split'],
    default: 'buy',
    lowercase: true
  },
  brokerageFees: {
    type: Number,
    default: 0
  },
  standardTaxes: {
    type: Number,
    default: 0
  },
  realizedPnL: {
    type: Number,
    default: 0
  }
}, {
  timestamps: true
});

module.exports = mongoose.model('Holding', HoldingSchema);
