const mongoose = require('mongoose');

const positionSchema = new mongoose.Schema({
  symbol: {
    type: String,
    required: true,
    trim: true,
    uppercase: true,
    unique: true
  },
  side: {
    type: String,
    enum: ['buy', 'sell'],
    required: true
  },
  averagePrice: {
    type: Number,
    required: true
  },
  quantity: {
    type: Number,
    required: true
  },
  realizedPnL: {
    type: Number,
    default: 0
  }
}, {
  timestamps: true
});

module.exports = mongoose.model('Position', positionSchema);
