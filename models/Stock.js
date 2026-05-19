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
    unique: true,
    trim: true,
    uppercase: true
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

module.exports = mongoose.model('Stock', stockSchema);
