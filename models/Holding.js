const mongoose = require('mongoose');

const HoldingSchema = new mongoose.Schema({
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
  }
}, {
  timestamps: true
});

module.exports = mongoose.model('Holding', HoldingSchema);
