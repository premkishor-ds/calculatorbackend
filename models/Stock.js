const mongoose = require('mongoose');

const stockSchema = new mongoose.Schema({
  symbol: {
    type: String,
    required: [true, 'Stock symbol is required'],
    trim: true,
    uppercase: true
  },
  watchlist: {
    type: String,
    default: 'default',
    trim: true
  },
  name: {
    type: String,
    required: [true, 'Stock name is required'],
    trim: true
  },
  isFavourite: {
    type: Boolean,
    default: false
  },
  tags: {
    type: [String],
    default: [],
    validate: {
      validator: (arr) =>
        Array.isArray(arr) && arr.length <= 20 && arr.every(t => typeof t === 'string' && t.length <= 50),
      message: 'Tags must be an array of up to 20 strings (max 50 chars each)'
    }
  }
}, {
  timestamps: true
});

// Compound unique index — a symbol can only exist once per watchlist
stockSchema.index({ watchlist: 1, symbol: 1 }, { unique: true });
// Text index for server-side search
stockSchema.index({ symbol: 'text', name: 'text' });

module.exports = mongoose.model('Stock', stockSchema);
