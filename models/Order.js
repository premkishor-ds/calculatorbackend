const mongoose = require('mongoose');

const orderSchema = new mongoose.Schema({
  symbol: {
    type: String,
    required: true,
    trim: true,
    uppercase: true
  },
  side: {
    type: String,
    enum: ['buy', 'sell'],
    required: true
  },
  type: {
    type: String,
    enum: ['market', 'limit'],
    required: true
  },
  price: {
    type: Number,
    required: true
  },
  quantity: {
    type: Number,
    required: true
  },
  status: {
    type: String,
    enum: ['pending', 'filled', 'cancelled'],
    default: 'pending'
  },
  stopLoss: {
    type: Number
  },
  takeProfit: {
    type: Number
  },
  filledAt: {
    type: Date
  }
}, {
  timestamps: true
});

orderSchema.index({ symbol: 1, status: 1 });

module.exports = mongoose.model('Order', orderSchema);
