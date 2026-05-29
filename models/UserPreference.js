const mongoose = require('mongoose');

const userPreferenceSchema = new mongoose.Schema({
  userId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: 'User',
    required: true,
    unique: true,
  },
  theme: {
    type: String,
    enum: ['dark', 'light'],
    default: 'dark',
  },
  timezone: {
    type: String,
    default: 'Asia/Kolkata',
  },
  dashboardSettings: {
    type: mongoose.Schema.Types.Mixed,
    default: {},
  },
  chartSettings: {
    type: mongoose.Schema.Types.Mixed,
    default: {},
  },
}, {
  timestamps: true,
});

module.exports = mongoose.model('UserPreference', userPreferenceSchema);
