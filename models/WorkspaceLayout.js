const mongoose = require('mongoose');

const workspaceLayoutSchema = new mongoose.Schema({
  name: {
    type: String,
    required: true,
    trim: true,
    unique: true
  },
  layout: {
    type: mongoose.Schema.Types.Mixed, // stores dynamic grid configuration, indicators, intervals, drawings
    required: true
  }
}, {
  timestamps: true
});

module.exports = mongoose.model('WorkspaceLayout', workspaceLayoutSchema);
