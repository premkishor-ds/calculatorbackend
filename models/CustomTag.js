const mongoose = require('mongoose');

const customTagSchema = new mongoose.Schema({
  tagId: {
    type: String,
    required: true,
    unique: true,
    enum: ['watchlist1', 'watchlist2', 'watchlist3', 'watchlist4', 'watchlist5']
  },
  label: { type: String, required: true, trim: true, maxlength: 24 },
  color: { type: String, required: true }, // hex color e.g. #f97316
}, { timestamps: true });

module.exports = mongoose.model('CustomTag', customTagSchema);
