const mongoose = require('mongoose');

const drawingSchema = new mongoose.Schema({
  symbol: {
    type: String,
    required: [true, 'Symbol is required'],
    trim: true,
    uppercase: true
  },
  chartMode: {
    type: String,
    enum: ['price', 'pe'],
    required: [true, 'Chart mode is required']
  },
  type: {
    type: String,
    required: [true, 'Drawing type is required']
  },
  points: [{
    time: { type: Number, required: true },
    price: { type: Number, required: true }
  }],
  price: {
    type: Number
  },
  time: {
    type: Number
  },
  color: {
    type: String,
    default: '#22c55e'
  },
  config: {
    type: mongoose.Schema.Types.Mixed,
    default: {}
  }
}, {
  timestamps: true
});

// Create a compound index for fast retrievals when loading charts
drawingSchema.index({ symbol: 1, chartMode: 1 });

module.exports = mongoose.model('Drawing', drawingSchema);
