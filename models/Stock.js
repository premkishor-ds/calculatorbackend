const mongoose = require('mongoose');

const VALID_TAGS = [
  'favourite', 'nextbuy', 'bullish', 'currentlyinvested',
  'watchclosely', 'highconviction', 'swingplay', 'longterm',
  'avoid', 'researching', 'takingprofit', 'undervalued'
];

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
      validator: (arr) => arr.every(t => VALID_TAGS.includes(t)),
      message: 'Invalid tag value'
    }
  }
}, {
  timestamps: true
});

// Set compound unique index so a symbol can only exist once per watchlist
stockSchema.index({ watchlist: 1, symbol: 1 }, { unique: true });

module.exports = mongoose.model('Stock', stockSchema);
