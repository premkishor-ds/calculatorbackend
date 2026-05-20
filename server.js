require('dotenv').config();
const express = require('express');
const mongoose = require('mongoose');
const cors = require('cors');
const Stock = require('./models/Stock');
const CustomTag = require('./models/CustomTag');
const Watchlist = require('./models/Watchlist');
const Drawing = require('./models/Drawing');

const app = express();
const PORT = process.env.PORT || 5001;

// 1. Custom Secure Response Headers Middleware (Helmet-equivalent security posture)
app.use((req, res, next) => {
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('X-Frame-Options', 'DENY');
  res.setHeader('X-XSS-Protection', '1; mode=block');
  res.setHeader('Strict-Transport-Security', 'max-age=31536000; includeSubDomains; preload');
  res.setHeader('Referrer-Policy', 'strict-origin-when-cross-origin');
  res.setHeader('Permissions-Policy', 'camera=(), microphone=(), geolocation=()');
  // Content Security Policy for API requests
  res.setHeader('Content-Security-Policy', "default-src 'self'; frame-ancestors 'none';");
  next();
});

// 2. High-Performance In-Memory Rate Limiting with proactive garbage collection
const rateLimitWindowMs = 15 * 60 * 1000; // 15 minutes
const rateLimitMaxRequests = 500; // 500 requests per 15 minutes per IP
const ipRequests = new Map();

// Periodic sweeping of stale cache values to prevent memory leaks
setInterval(() => {
  const now = Date.now();
  for (const [ip, data] of ipRequests.entries()) {
    if (now - data.startTime > rateLimitWindowMs) {
      ipRequests.delete(ip);
    }
  }
}, 5 * 60 * 1000);

app.use((req, res, next) => {
  const ip = req.headers['x-forwarded-for'] || req.socket.remoteAddress;
  const now = Date.now();
  
  if (!ipRequests.has(ip)) {
    ipRequests.set(ip, { startTime: now, count: 1 });
    next();
    return;
  }
  
  const data = ipRequests.get(ip);
  if (now - data.startTime > rateLimitWindowMs) {
    data.startTime = now;
    data.count = 1;
    next();
    return;
  }
  
  data.count += 1;
  if (data.count > rateLimitMaxRequests) {
    res.status(429).json({
      error: 'Too Many Requests',
      message: 'Rate limit exceeded. Please try again later.'
    });
    return;
  }
  
  next();
});

// Middleware
const allowedOrigins = process.env.ALLOWED_ORIGINS
  ? process.env.ALLOWED_ORIGINS.split(',')
  : ['http://localhost:3000'];

app.use(cors({
  origin: function (origin, callback) {
    if (!origin) {
      callback(null, true);
      return;
    }
    // Check if origin is explicitly allowed
    if (allowedOrigins.indexOf(origin) !== -1) {
      callback(null, true);
      return;
    }
    // Self-healing: Dynamically allow any netlify.app origins for seamless front-backend integration
    if (origin.endsWith('netlify.app') || origin.endsWith('.netlify.app')) {
      callback(null, true);
      return;
    }
    callback(new Error('Blocked by CORS security policy'));
  },
  credentials: true
}));
app.use(express.json());

// MongoDB Connection with Enterprise connection pooling
mongoose.connect(process.env.MONGODB_URI, {
  maxPoolSize: 100,
  minPoolSize: 10,
  socketTimeoutMS: 45000,
  serverSelectionTimeoutMS: 5000,
})
  .then(() => {
    console.log('Successfully connected to MongoDB.');
    seedDefaultStocks();
  })
  .catch((err) => {
    console.error('MongoDB connection error:', err);
  });

// Predefined list of default symbols and their correct corporate names
const DEFAULT_STOCKS_SEED = [
  { symbol: 'VOLTAMP.NS', name: 'Voltamp Transformers Ltd.' },
  { symbol: 'TDPOWERSYS.NS', name: 'TD Power Systems Ltd.' },
  { symbol: 'TARIL.NS', name: 'Transformers & Rectifiers (India) Ltd.' },
  { symbol: 'PRECWIRE.NS', name: 'Precision Wires India Ltd.' },
  { symbol: 'MAZDOCK.NS', name: 'Mazagon Dock Shipbuilders Ltd.' },
  { symbol: 'KIRLOSENG.NS', name: 'Kirloskar Oil Engines Ltd.' },
  { symbol: 'HSCL.NS', name: 'Himadri Speciality Chemical Ltd.' },
  { symbol: 'HFCL.NS', name: 'HFCL Ltd.' },
  { symbol: 'E2E.NS', name: 'E2E Networks Ltd.' },
  { symbol: 'BECTORFOOD.NS', name: 'Mrs. Bectors Food Specialities Ltd.' },
  { symbol: 'AURIONPRO.NS', name: 'Aurionpro Solutions Ltd.' },
  { symbol: 'KEI.NS', name: 'KEI Industries Ltd.' },
  { symbol: 'COFORGE.NS', name: 'Coforge Ltd.' },
  { symbol: 'MANORAMA.NS', name: 'Manorama Industries Ltd.' },
  { symbol: 'ZENTEC.NS', name: 'Zen Technologies Ltd.' },
  { symbol: 'APARINDS.NS', name: 'Apar Industries Ltd.' },
  { symbol: 'SHILCTECH.NS', name: 'Shilpa Medicare Ltd.' },
  { symbol: 'INOXINDIA.NS', name: 'Inox India Ltd.' },
  { symbol: 'KRN.NS', name: 'KRN Heat Exchanger and Refrigeration Ltd.' },
  { symbol: 'IDEAFORGE.NS', name: 'ideaForge Technology Ltd.' },
  { symbol: 'GRSE.NS', name: 'Garden Reach Shipbuilders & Engineers Ltd.' },
  { symbol: 'PARAS.NS', name: 'Paras Defence and Space Technologies Ltd.' },
  { symbol: 'ASTRAMICRO.NS', name: 'Astra Microwave Products Ltd.' },
  { symbol: 'SYRMA.NS', name: 'Syrma SGS Technology Ltd.' },
  { symbol: 'KAYNES.NS', name: 'Kaynes Technology India Ltd.' },
  { symbol: 'AEROFLEX.NS', name: 'Aeroflex Industries Ltd.' },
  { symbol: 'KMEW.NS', name: 'Knowledge Marine & Export Works Ltd.' },
  { symbol: 'GVT&D.NS', name: 'GE Vernova T&D India Ltd.' },
  { symbol: 'CGPOWER.NS', name: 'CG Power & Industrial Solutions Ltd.' },
  { symbol: 'APOLLO.NS', name: 'Apollo Hospitals Enterprise Ltd.' },
  { symbol: 'UNIMECH.NS', name: 'Unimech Aerospace and Manufacture Ltd.' },
  { symbol: 'DATAPATTNS.NS', name: 'Data Patterns (India) Ltd.' },
  { symbol: 'MTARTECH.NS', name: 'MTAR Technologies Ltd.' },
  { symbol: 'NETWEB.NS', name: 'Netweb Technologies India Ltd.' }
];

// Seed DB if it's empty
async function seedDefaultStocks() {
  try {
    // Ensure default watchlist exists
    let defaultWatchlist = await Watchlist.findOne({ name: 'default' });
    if (!defaultWatchlist) {
      defaultWatchlist = new Watchlist({ name: 'default', isDefault: true });
      await defaultWatchlist.save();
      console.log('Seeded default watchlist');
    }

    const count = await Stock.countDocuments({ watchlist: 'default' });
    if (count === 0) {
      console.log('Stock collection for default watchlist is empty. Seeding default stock list...');
      await Stock.insertMany(DEFAULT_STOCKS_SEED.map(item => ({
        symbol: item.symbol,
        name: item.name,
        isFavourite: false,
        watchlist: 'default'
      })));
      console.log('Seeding completed successfully!');
    } else {
      console.log(`Database already contains ${count} stocks for default watchlist. Skipping seeding.`);
    }
  } catch (error) {
    console.error('Error seeding default stocks:', error);
  }
}

/* ── API Routes ────────────────────────────────────────────── */

// GET /api/watchlists - Fetch all watchlists
app.get('/api/watchlists', async (req, res) => {
  try {
    let lists = await Watchlist.find({}).sort({ isDefault: -1, createdAt: 1 });
    if (lists.length === 0) {
      const def = new Watchlist({ name: 'default', isDefault: true });
      await def.save();
      lists = [def];
    }
    res.json(lists);
  } catch (err) {
    res.status(500).json({ error: 'Failed to retrieve watchlists' });
  }
});

// POST /api/watchlists - Create a custom watchlist
app.post('/api/watchlists', async (req, res) => {
  try {
    const { name } = req.body;
    if (!name || !name.trim()) {
      return res.status(400).json({ error: 'Watchlist name is required' });
    }
    const cleanName = name.trim();
    
    // Escape regex characters to prevent Regular Expression Denial of Service (ReDoS) NoSQL Injection
    const escapedName = cleanName.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const existing = await Watchlist.findOne({ name: { $regex: new RegExp(`^${escapedName}$`, 'i') } });
    if (existing) {
      return res.status(400).json({ error: 'A watchlist with this name already exists' });
    }

    const wl = new Watchlist({ name: cleanName, isDefault: false });
    await wl.save();
    res.status(201).json(wl);
  } catch (err) {
    res.status(500).json({ error: 'Failed to create watchlist', details: err.message });
  }
});

// DELETE /api/watchlists/:name - Delete a custom watchlist by name
app.delete('/api/watchlists/:name', async (req, res) => {
  try {
    const nameParam = req.params.name.trim();
    
    // Find watchlist
    const wl = await Watchlist.findOne({ name: nameParam });
    if (!wl) {
      return res.status(404).json({ error: 'Watchlist not found' });
    }
    
    if (wl.isDefault || wl.name.toLowerCase() === 'default') {
      return res.status(400).json({ error: 'The default watchlist cannot be deleted' });
    }

    // Get all symbols in this watchlist first to cascade delete drawings
    const stocksInWatchlist = await Stock.find({ watchlist: wl.name });
    const symbols = stocksInWatchlist.map(s => s.symbol);

    // Cascade delete all stocks in this watchlist
    await Stock.deleteMany({ watchlist: wl.name });

    // Cascade delete drawings for these symbols
    if (symbols.length > 0) {
      await Drawing.deleteMany({ symbol: { $in: symbols } });
    }
    
    // Delete watchlist
    await Watchlist.findByIdAndDelete(wl._id);
    
    res.json({ message: `Watchlist '${wl.name}', associated stocks, and drawings successfully deleted` });
  } catch (err) {
    res.status(500).json({ error: 'Failed to delete watchlist', details: err.message });
  }
});

// GET /api/stocks - Fetch all stocks for a specific watchlist
app.get('/api/stocks', async (req, res) => {
  try {
    const watchlistName = req.query.watchlist || 'default';
    const stocks = await Stock.find({ watchlist: watchlistName }).sort({ updatedAt: -1 });
    res.json(stocks);
  } catch (error) {
    console.error('GET /api/stocks error:', error);
    res.status(500).json({ error: 'Failed to retrieve stocks from database' });
  }
});

// POST /api/stocks - Add a new stock to a specific watchlist
app.post('/api/stocks', async (req, res) => {
  try {
    let { symbol, name, isFavourite, isfavoute, watchlist } = req.body;
    
    if (!symbol) {
      return res.status(400).json({ error: 'Stock symbol is required' });
    }
    if (!name) {
      return res.status(400).json({ error: 'Stock name is required' });
    }

    const formattedSymbol = symbol.trim().toUpperCase();
    const formattedName = name.trim();
    const wlName = (watchlist || 'default').trim();
    
    // Normalize isFavourite
    const favStatus = isFavourite !== undefined ? isFavourite : (isfavoute !== undefined ? isfavoute : false);

    // Check if stock already exists in this watchlist
    let existingStock = await Stock.findOne({ symbol: formattedSymbol, watchlist: wlName });
    if (existingStock) {
      existingStock.name = formattedName;
      existingStock.isFavourite = favStatus;
      await existingStock.save();
      return res.status(200).json(existingStock);
    }

    const newStock = new Stock({
      symbol: formattedSymbol,
      name: formattedName,
      isFavourite: favStatus,
      watchlist: wlName
    });

    await newStock.save();
    console.log(`Added stock: ${formattedSymbol} (${formattedName}) to watchlist: ${wlName}`);
    res.status(201).json(newStock);
  } catch (error) {
    console.error('POST /api/stocks error:', error);
    if (error.code === 11000) {
      return res.status(409).json({ error: 'Stock symbol already exists in this watchlist' });
    }
    res.status(500).json({ error: 'Failed to add stock to database', details: error.message });
  }
});

// PATCH /api/stocks/:symbol - Update favorite status or tags for a stock inside a specific watchlist
app.patch('/api/stocks/:symbol', async (req, res) => {
  try {
    const symbolParam = req.params.symbol.trim().toUpperCase();
    const wlName = (req.body.watchlist || req.query.watchlist || 'default').trim();
    const { isFavourite, isfavoute, tags } = req.body;

    const stock = await Stock.findOne({ symbol: symbolParam, watchlist: wlName });
    if (!stock) {
      return res.status(404).json({ error: `Stock with symbol ${symbolParam} not found in watchlist ${wlName}` });
    }

    if (isFavourite !== undefined || isfavoute !== undefined) {
      stock.isFavourite = isFavourite !== undefined ? isFavourite : isfavoute;
    }
    if (Array.isArray(tags)) {
      stock.tags = tags;
    }

    await stock.save();
    res.json(stock);
  } catch (error) {
    console.error('PATCH /api/stocks/:symbol error:', error);
    res.status(500).json({ error: 'Failed to update stock' });
  }
});

// DELETE /api/stocks/:symbol - Delete a stock from a specific watchlist
app.delete('/api/stocks/:symbol', async (req, res) => {
  try {
    const symbolParam = req.params.symbol.trim().toUpperCase();
    const wlName = (req.query.watchlist || req.body.watchlist || 'default').trim();
    const result = await Stock.findOneAndDelete({ symbol: symbolParam, watchlist: wlName });
    
    if (!result) {
      return res.status(404).json({ error: `Stock with symbol ${symbolParam} not found in watchlist ${wlName}` });
    }

    // Cascade delete drawings for this symbol
    await Drawing.deleteMany({ symbol: symbolParam });

    console.log(`Deleted stock: ${symbolParam} from watchlist ${wlName} and cascade deleted its drawings.`);
    res.json({ message: `Stock ${symbolParam} and its drawings successfully deleted from watchlist ${wlName}`, deletedStock: result });
  } catch (error) {
    console.error('DELETE /api/stocks/:symbol error:', error);
    res.status(500).json({ error: 'Failed to delete stock from database' });
  }
});

// Error handling middleware
app.use((err, req, res, next) => {
  console.error('Unhandled server error:', err);
  res.status(500).json({ error: 'Internal server error occurred' });
});

/* ── Custom Tag Routes ─────────────────────────────────────── */

const DEFAULT_CUSTOM_TAGS = [
  { tagId: 'watchlist1', label: 'Watchlist 1', color: '#f97316' },
  { tagId: 'watchlist2', label: 'Watchlist 2', color: '#8b5cf6' },
  { tagId: 'watchlist3', label: 'Watchlist 3', color: '#06b6d4' },
  { tagId: 'watchlist4', label: 'Watchlist 4', color: '#ec4899' },
  { tagId: 'watchlist5', label: 'Watchlist 5', color: '#84cc16' },
];

// GET /api/custom-tags
app.get('/api/custom-tags', async (req, res) => {
  try {
    const saved = await CustomTag.find({});
    const result = DEFAULT_CUSTOM_TAGS.map(def => {
      const found = saved.find(s => s.tagId === def.tagId);
      return found ? { tagId: found.tagId, label: found.label, color: found.color } : def;
    });
    res.json(result);
  } catch (err) {
    res.status(500).json({ error: 'Failed to fetch custom tags' });
  }
});

// PUT /api/custom-tags/:tagId
app.put('/api/custom-tags/:tagId', async (req, res) => {
  try {
    const { tagId } = req.params;
    const { label, color } = req.body;
    if (!label || !color) return res.status(400).json({ error: 'label and color required' });
    const tag = await CustomTag.findOneAndUpdate(
      { tagId },
      { label: label.trim().slice(0, 24), color },
      { upsert: true, new: true }
    );
    res.json({ tagId: tag.tagId, label: tag.label, color: tag.color });
  } catch (err) {
    res.status(500).json({ error: 'Failed to update custom tag' });
  }
});

/* ── Drawings API Routes ────────────────────────────────────── */

// GET /api/drawings - Fetch drawings for a specific symbol
app.get('/api/drawings', async (req, res) => {
  try {
    const { symbol, chartMode } = req.query;
    if (!symbol) {
      return res.status(400).json({ error: 'Symbol query parameter is required' });
    }
    const filter = { symbol: symbol.trim().toUpperCase() };
    if (chartMode) {
      filter.chartMode = chartMode.trim().toLowerCase();
    }
    const drawings = await Drawing.find(filter);
    res.json(drawings);
  } catch (err) {
    console.error('GET /api/drawings error:', err);
    res.status(500).json({ error: 'Failed to retrieve drawings' });
  }
});

// POST /api/drawings/sync - Synchronize drawings for a symbol + mode
app.post('/api/drawings/sync', async (req, res) => {
  try {
    const { symbol, chartMode, drawings } = req.body;
    if (!symbol) {
      return res.status(400).json({ error: 'Symbol is required' });
    }
    if (!chartMode) {
      return res.status(400).json({ error: 'Chart mode is required' });
    }
    if (!Array.isArray(drawings)) {
      return res.status(400).json({ error: 'Drawings must be an array' });
    }

    const cleanSymbol = symbol.trim().toUpperCase();
    const cleanMode = chartMode.trim().toLowerCase();

    // 1. Delete all existing drawings for this symbol + mode
    await Drawing.deleteMany({ symbol: cleanSymbol, chartMode: cleanMode });

    // 2. Insert new drawings
    const drawingsToSave = drawings.map(d => ({
      symbol: cleanSymbol,
      chartMode: cleanMode,
      type: d.type,
      points: d.points || [],
      price: d.price,
      time: d.time,
      color: d.color || '#22c55e'
    }));

    const savedDrawings = await Drawing.insertMany(drawingsToSave);
    res.status(200).json(savedDrawings);
  } catch (err) {
    console.error('POST /api/drawings/sync error:', err);
    res.status(500).json({ error: 'Failed to synchronize drawings', details: err.message });
  }
});

// Start the server
app.listen(PORT, () => {
  console.log(`Vision backend server is running on http://localhost:${PORT}`);
});
