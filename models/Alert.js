const mongoose = require('mongoose');

const alertSchema = new mongoose.Schema({
  symbol: {
    type: String,
    required: true,
    trim: true,
    uppercase: true
  },
  condition: {
    type: String,
    enum: ['price_crosses', 'price_above', 'price_below', 'volume_spike', 'rsi_above', 'rsi_below'],
    required: true
  },
  value: {
    type: Number,
    required: true
  },
  isTriggered: {
    type: Boolean,
    default: false
  },
  isActive: {
    type: Boolean,
    default: true
  },
  triggeredAt: {
    type: Date
  },
  delivery: {
    type: String,
    enum: ['in_app', 'email', 'webhook', 'sms'],
    default: 'in_app'
  }
}, {
  timestamps: true
});

alertSchema.index({ symbol: 1, isTriggered: 1, isActive: 1 });

module.exports = mongoose.model('Alert', alertSchema);
