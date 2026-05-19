const mongoose = require('mongoose');

const watchlistSchema = new mongoose.Schema({
  name: {
    type: String,
    required: [true, 'Watchlist name is required'],
    trim: true,
    unique: true
  },
  isDefault: {
    type: Boolean,
    default: false
  }
}, {
  timestamps: true
});

module.exports = mongoose.model('Watchlist', watchlistSchema);
