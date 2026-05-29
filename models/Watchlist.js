const mongoose = require('mongoose');

const watchlistSchema = new mongoose.Schema({
  userId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: 'User',
    required: false,
    index: true
  },
  name: {
    type: String,
    required: [true, 'Watchlist name is required'],
    trim: true
  },
  description: {
    type: String,
    trim: true,
    default: ''
  },
  isDefault: {
    type: Boolean,
    default: false
  }
}, {
  timestamps: true
});

module.exports = mongoose.model('Watchlist', watchlistSchema);
