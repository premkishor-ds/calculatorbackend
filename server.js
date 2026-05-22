require('dotenv').config();
const express = require('express');
const http = require('http');
const { WebSocketServer } = require('ws');
const mongoose = require('mongoose');
const cors = require('cors');

// Import Schemas
const Stock = require('./models/Stock');
const CustomTag = require('./models/CustomTag');
const Watchlist = require('./models/Watchlist');
const Drawing = require('./models/Drawing');
const Alert = require('./models/Alert');
const Order = require('./models/Order');
const Position = require('./models/Position');
const WorkspaceLayout = require('./models/WorkspaceLayout');
const MarketStock = require('./models/MarketStock');
const ScreenerSync = require('./models/ScreenerSync');
const { buildScreenerQuery, buildSort } = require('./lib/screener-query');
const {
  runScreenerSync,
  getLatestAsOfDate,
  ensureTodaySnapshot,
  isSyncInProgress,
} = require('./services/screener-sync');
const { connectMongo } = require('./lib/connect-mongo');
const cron = require('node-cron');

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

// CORS Config
const allowedOrigins = process.env.ALLOWED_ORIGINS
  ? process.env.ALLOWED_ORIGINS.split(',')
  : ['http://localhost:3000'];

app.use(cors({
  origin: function (origin, callback) {
    if (!origin) {
      callback(null, true);
      return;
    }
    if (allowedOrigins.indexOf(origin) !== -1) {
      callback(null, true);
      return;
    }
    if (origin.endsWith('netlify.app') || origin.endsWith('.netlify.app')) {
      callback(null, true);
      return;
    }
    callback(new Error('Blocked by CORS security policy'));
  },
  credentials: true
}));
app.use(express.json());

// MongoDB + daily screener snapshot (auto-sync if today's data is missing)
async function initDatabase() {
  await connectMongo();
  await seedDefaultStocks();
  await syncSimulatorSymbolsFromDb();
  scheduleScreenerCron();
  await ensureTodaySnapshot();
}

/** Register every watchlist symbol for live tick / alert / order simulation */
async function syncSimulatorSymbolsFromDb() {
  try {
    const stocks = await Stock.find({}).select('symbol').lean();
    let added = 0;
    for (const { symbol } of stocks) {
      if (!symbol || stockPrices[symbol]) continue;
      const base = stockBaselines[symbol] ?? 400 + Math.random() * 1600;
      stockPrices[symbol] = {
        price: base,
        change: 0,
        changePercent: 0,
        open: base,
      };
      added++;
    }
    if (added > 0) {
      console.log(`[Simulator] Registered ${added} symbols from MongoDB (${Object.keys(stockPrices).length} total)`);
    }
  } catch (err) {
    console.error('[Simulator] Failed to sync symbols from DB:', err.message);
  }
}

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

/* ── Live Market Simulated Feeds State ─────────────────────── */
const stockBaselines = {
  'VOLTAMP.NS': 10250.0,
  'TDPOWERSYS.NS': 450.0,
  'TARIL.NS': 780.0,
  'PRECWIRE.NS': 150.0,
  'MAZDOCK.NS': 2400.0,
  'KIRLOSENG.NS': 920.0,
  'HSCL.NS': 380.0,
  'HFCL.NS': 115.0,
  'E2E.NS': 1200.0,
  'BECTORFOOD.NS': 1400.0,
  'AURIONPRO.NS': 1650.0,
  'KEI.NS': 3200.0,
  'COFORGE.NS': 5200.0,
  'MANORAMA.NS': 650.0,
  'ZENTEC.NS': 850.0,
  'APARINDS.NS': 6400.0,
  'SHILCTECH.NS': 720.0,
  'INOXINDIA.NS': 1100.0,
  'KRN.NS': 350.0,
  'IDEAFORGE.NS': 680.0,
  'GRSE.NS': 1800.0,
  'PARAS.NS': 980.0,
  'ASTRAMICRO.NS': 650.0,
  'SYRMA.NS': 490.0,
  'KAYNES.NS': 2800.0,
  'AEROFLEX.NS': 160.0,
  'KMEW.NS': 180.0,
  'GVT&D.NS': 850.0,
  'CGPOWER.NS': 680.0,
  'APOLLO.NS': 6100.0,
  'UNIMECH.NS': 420.0,
  'DATAPATTNS.NS': 2200.0,
  'MTARTECH.NS': 1900.0,
  'NETWEB.NS': 1250.0
};

const stockPrices = {};
for (const symbol in stockBaselines) {
  const base = stockBaselines[symbol];
  stockPrices[symbol] = {
    price: base,
    change: 0,
    changePercent: 0,
    open: base * (1 - 0.01 * (Math.random() - 0.5))
  };
}

// Paper Trading Account global state
let virtualBalance = 1000000.0;

/* ── WebSocket Setup & Simulator Loop ──────────────────────── */
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

wss.on('connection', (ws) => {
  console.log('Client connected to WebSocket stream');
  
  // Send active balance upon connecting
  ws.send(JSON.stringify({ type: 'portfolio_update', balance: virtualBalance }));
  
  ws.on('message', (message) => {
    try {
      const parsed = JSON.parse(message);
      if (parsed.type === 'ping') {
        ws.send(JSON.stringify({ type: 'pong' }));
      }
    } catch {}
  });

  ws.on('close', () => {
    console.log('Client disconnected from stream');
  });
});

// Periodic Random Walk Market Simulator Ticker Loop (Every 1 second)
setInterval(async () => {
  const clients = wss.clients;
  if (clients.size === 0) return;

  for (const symbol in stockPrices) {
    const data = stockPrices[symbol];
    const volatility = 0.0012; // 0.12% max move per second
    const percentChange = (Math.random() - 0.5) * 2 * volatility;
    const delta = data.price * percentChange;
    
    data.price = parseFloat((data.price + delta).toFixed(2));
    data.change = parseFloat((data.price - data.open).toFixed(2));
    data.changePercent = parseFloat(((data.change / data.open) * 100).toFixed(2));

    const bids = [];
    const asks = [];
    const spreadSteps = [0.05, 0.10, 0.15, 0.20, 0.25];
    for (let i = 0; i < 5; i++) {
      bids.push({
        price: parseFloat((data.price - spreadSteps[i]).toFixed(2)),
        size: Math.floor(Math.random() * 800) + 100,
        count: Math.floor(Math.random() * 15) + 1
      });
      asks.push({
        price: parseFloat((data.price + spreadSteps[i]).toFixed(2)),
        size: Math.floor(Math.random() * 800) + 100,
        count: Math.floor(Math.random() * 15) + 1
      });
    }

    const tick = {
      type: 'tick',
      symbol,
      price: data.price,
      change: data.change,
      changePercent: data.changePercent,
      volume: Math.floor(Math.random() * 80) + 5,
      time: Math.floor(Date.now() / 1000),
      bids,
      asks
    };

    const msg = JSON.stringify(tick);
    clients.forEach(client => {
      if (client.readyState === 1) { // OPEN
        client.send(msg);
      }
    });

    // Check matches for Alerts & Pending Paper Orders
    await checkAlertsForSymbol(symbol, data.price);
    await checkOrdersForSymbol(symbol, data.price);
  }
}, 1000);

async function checkAlertsForSymbol(symbol, currentPrice) {
  try {
    const activeAlerts = await Alert.find({ symbol, isTriggered: false, isActive: true });
    for (const alert of activeAlerts) {
      let triggered = false;
      if (alert.condition === 'price_crosses' && Math.abs(currentPrice - alert.value) / alert.value < 0.0015) {
        triggered = true;
      } else if (alert.condition === 'price_above' && currentPrice >= alert.value) {
        triggered = true;
      } else if (alert.condition === 'price_below' && currentPrice <= alert.value) {
        triggered = true;
      }

      if (triggered) {
        alert.isTriggered = true;
        alert.triggeredAt = new Date();
        await alert.save();

        const alertMsg = JSON.stringify({
          type: 'alert_triggered',
          alert: {
            _id: alert._id,
            symbol: alert.symbol,
            condition: alert.condition,
            value: alert.value,
            triggeredAt: alert.triggeredAt
          }
        });
        wss.clients.forEach(c => {
          if (c.readyState === 1) c.send(alertMsg);
        });
      }
    }
  } catch (err) {
    console.error('Alert checker loop failure:', err);
  }
}

async function checkOrdersForSymbol(symbol, currentPrice) {
  try {
    const pending = await Order.find({ symbol, status: 'pending' });
    for (const order of pending) {
      let fill = false;
      if (order.type === 'limit') {
        if (order.side === 'buy' && currentPrice <= order.price) fill = true;
        if (order.side === 'sell' && currentPrice >= order.price) fill = true;
      }

      if (fill) {
        order.status = 'filled';
        order.filledAt = new Date();
        await order.save();

        let pos = await Position.findOne({ symbol });
        if (order.side === 'buy') {
          const totalCost = (pos ? (pos.averagePrice * pos.quantity) : 0) + (order.price * order.quantity);
          const totalQty = (pos ? pos.quantity : 0) + order.quantity;
          if (!pos) {
            pos = new Position({
              symbol,
              side: 'buy',
              averagePrice: order.price,
              quantity: order.quantity
            });
          } else {
            pos.averagePrice = parseFloat((totalCost / totalQty).toFixed(2));
            pos.quantity = totalQty;
          }
          virtualBalance = parseFloat((virtualBalance - (order.price * order.quantity)).toFixed(2));
        } else { // sell
          if (pos) {
            const qtyFilled = Math.min(pos.quantity, order.quantity);
            const profit = parseFloat(((order.price - pos.averagePrice) * qtyFilled).toFixed(2));
            pos.quantity -= qtyFilled;
            pos.realizedPnL = parseFloat((pos.realizedPnL + profit).toFixed(2));
            virtualBalance = parseFloat((virtualBalance + (order.price * order.quantity)).toFixed(2));
            if (pos.quantity === 0) {
              await Position.findByIdAndDelete(pos._id);
              pos = null;
            }
          }
        }

        if (pos) await pos.save();

        const execMsg = JSON.stringify({
          type: 'order_filled',
          order,
          position: pos,
          balance: virtualBalance
        });
        wss.clients.forEach(c => {
          if (c.readyState === 1) c.send(execMsg);
        });
      }
    }
  } catch (err) {
    console.error('Order matching engine execution failure:', err);
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

// DELETE /api/watchlists/:name - Delete a custom watchlist
app.delete('/api/watchlists/:name', async (req, res) => {
  try {
    const nameParam = req.params.name.trim();
    const wl = await Watchlist.findOne({ name: nameParam });
    if (!wl) return res.status(404).json({ error: 'Watchlist not found' });
    
    if (wl.isDefault || wl.name.toLowerCase() === 'default') {
      return res.status(400).json({ error: 'The default watchlist cannot be deleted' });
    }

    const stocksInWatchlist = await Stock.find({ watchlist: wl.name });
    const symbols = stocksInWatchlist.map(s => s.symbol);
    await Stock.deleteMany({ watchlist: wl.name });
    if (symbols.length > 0) {
      await Drawing.deleteMany({ symbol: { $in: symbols } });
    }
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
    res.status(500).json({ error: 'Failed to retrieve stocks from database' });
  }
});

// POST /api/stocks - Add a new stock to a specific watchlist
app.post('/api/stocks', async (req, res) => {
  try {
    let { symbol, name, isFavourite, isfavoute, watchlist } = req.body;
    if (!symbol) return res.status(400).json({ error: 'Stock symbol is required' });
    if (!name) return res.status(400).json({ error: 'Stock name is required' });

    const formattedSymbol = symbol.trim().toUpperCase();
    const formattedName = name.trim();
    const wlName = (watchlist || 'default').trim();
    const favStatus = isFavourite !== undefined ? isFavourite : (isfavoute !== undefined ? isfavoute : false);

    if (!stockPrices[formattedSymbol]) {
      let seedPrice = stockBaselines[formattedSymbol] ?? 500.0;
      try {
        const { yahooFinance, YAHOO_OPTS } = require('./lib/yahoo-finance');
        const q = await yahooFinance.quote(formattedSymbol, YAHOO_OPTS);
        const live = q?.regularMarketPrice;
        if (live && live > 0) seedPrice = live;
      } catch {
        /* use baseline fallback */
      }
      stockPrices[formattedSymbol] = {
        price: seedPrice,
        change: 0,
        changePercent: 0,
        open: seedPrice,
      };
    }

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
    scheduleSimulatorSync();
    res.status(201).json(newStock);
  } catch (error) {
    if (error.code === 11000) {
      return res.status(409).json({ error: 'Stock symbol already exists in this watchlist' });
    }
    res.status(500).json({ error: 'Failed to add stock to database', details: error.message });
  }
});

function scheduleSimulatorSync() {
  syncSimulatorSymbolsFromDb().catch(() => {});
}

// PATCH /api/stocks/:symbol - Update favourite or tags
app.patch('/api/stocks/:symbol', async (req, res) => {
  try {
    const symbolParam = req.params.symbol.trim().toUpperCase();
    const wlName = (req.body.watchlist || req.query.watchlist || 'default').trim();
    const { isFavourite, isfavoute, tags } = req.body;

    const stock = await Stock.findOne({ symbol: symbolParam, watchlist: wlName });
    if (!stock) return res.status(404).json({ error: `Stock with symbol ${symbolParam} not found` });

    if (isFavourite !== undefined || isfavoute !== undefined) {
      stock.isFavourite = isFavourite !== undefined ? isFavourite : isfavoute;
    }
    if (Array.isArray(tags)) stock.tags = tags;

    await stock.save();
    res.json(stock);
  } catch (error) {
    if (error.name === 'ValidationError') {
      return res.status(400).json({ error: error.message || 'Invalid stock data' });
    }
    res.status(500).json({ error: 'Failed to update stock' });
  }
});

// DELETE /api/stocks/:symbol - Delete stock and its drawings
app.delete('/api/stocks/:symbol', async (req, res) => {
  try {
    const symbolParam = req.params.symbol.trim().toUpperCase();
    const wlName = (req.query.watchlist || req.body.watchlist || 'default').trim();
    const result = await Stock.findOneAndDelete({ symbol: symbolParam, watchlist: wlName });
    if (!result) return res.status(404).json({ error: `Stock not found` });

    await Drawing.deleteMany({ symbol: symbolParam });
    res.json({ message: `Stock ${symbolParam} deleted successfully`, deletedStock: result });
  } catch (error) {
    res.status(500).json({ error: 'Failed to delete stock from database' });
  }
});

/* ── Custom Tag Routes ─────────────────────────────────────── */
const DEFAULT_CUSTOM_TAGS = [
  { tagId: 'watchlist1', label: 'Watchlist 1', color: '#f97316' },
  { tagId: 'watchlist2', label: 'Watchlist 2', color: '#8b5cf6' },
  { tagId: 'watchlist3', label: 'Watchlist 3', color: '#06b6d4' },
  { tagId: 'watchlist4', label: 'Watchlist 4', color: '#ec4899' },
  { tagId: 'watchlist5', label: 'Watchlist 5', color: '#84cc16' },
];

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
app.get('/api/drawings', async (req, res) => {
  try {
    const { symbol, chartMode } = req.query;
    if (!symbol) return res.status(400).json({ error: 'Symbol query parameter is required' });
    const filter = { symbol: symbol.trim().toUpperCase() };
    if (chartMode) filter.chartMode = chartMode.trim().toLowerCase();
    const drawings = await Drawing.find(filter);
    res.json(drawings);
  } catch (err) {
    res.status(500).json({ error: 'Failed to retrieve drawings' });
  }
});

app.post('/api/drawings/sync', async (req, res) => {
  try {
    const { symbol, chartMode, drawings } = req.body;
    if (!symbol) return res.status(400).json({ error: 'Symbol is required' });
    if (!chartMode) return res.status(400).json({ error: 'Chart mode is required' });
    if (!Array.isArray(drawings)) return res.status(400).json({ error: 'Drawings must be an array' });

    const cleanSymbol = symbol.trim().toUpperCase();
    const cleanMode = chartMode.trim().toLowerCase();

    await Drawing.deleteMany({ symbol: cleanSymbol, chartMode: cleanMode });
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
    res.status(500).json({ error: 'Failed to synchronize drawings', details: err.message });
  }
});

/* ── Paper Trading Panel API Endpoints ─────────────────────── */

// GET /api/trading/portfolio - Retrieve account balance
app.get('/api/trading/portfolio', (req, res) => {
  res.json({ balance: virtualBalance });
});

// POST /api/trading/portfolio/reset - Reset paper trading account balance & clear ledger
app.post('/api/trading/portfolio/reset', async (req, res) => {
  try {
    virtualBalance = 1000000.0;
    await Position.deleteMany({});
    await Order.deleteMany({});
    
    // Broadcast reset to connected sockets
    wss.clients.forEach(c => {
      if (c.readyState === 1) {
        c.send(JSON.stringify({ type: 'portfolio_update', balance: virtualBalance }));
      }
    });
    
    res.json({ message: 'Paper trading account reset successfully', balance: virtualBalance });
  } catch (err) {
    res.status(500).json({ error: 'Failed to reset paper account' });
  }
});

// GET /api/trading/positions - Retrieve all open virtual positions
app.get('/api/trading/positions', async (req, res) => {
  try {
    const positions = await Position.find({});
    // Attach current pricing dynamically
    const updated = positions.map(pos => {
      const live = stockPrices[pos.symbol]?.price || pos.averagePrice;
      const profit = parseFloat(((live - pos.averagePrice) * pos.quantity * (pos.side === 'buy' ? 1 : -1)).toFixed(2));
      return {
        ...pos.toObject(),
        currentPrice: live,
        unrealizedPnL: profit
      };
    });
    res.json(updated);
  } catch (err) {
    res.status(500).json({ error: 'Failed to retrieve holdings' });
  }
});

// GET /api/trading/orders - Retrieve complete order history log
app.get('/api/trading/orders', async (req, res) => {
  try {
    const orders = await Order.find({}).sort({ createdAt: -1 });
    res.json(orders);
  } catch (err) {
    res.status(500).json({ error: 'Failed to retrieve orders' });
  }
});

// POST /api/trading/orders - Create trade orders (Market/Limit execution)
app.post('/api/trading/orders', async (req, res) => {
  try {
    const { symbol, side, type, price, quantity } = req.body;
    if (!symbol) return res.status(400).json({ error: 'Symbol is required' });
    if (!side || !['buy', 'sell'].includes(side)) return res.status(400).json({ error: 'Side must be buy/sell' });
    if (!type || !['market', 'limit'].includes(type)) return res.status(400).json({ error: 'Type must be market/limit' });
    if (!quantity || quantity <= 0) return res.status(400).json({ error: 'Invalid quantity' });

    const symUpper = symbol.trim().toUpperCase();
    const livePrice = stockPrices[symUpper]?.price || 500.0;
    const executionPrice = type === 'market' ? livePrice : parseFloat(Number(price).toFixed(2));

    if (type === 'market') {
      // Execute fill instantly!
      if (side === 'buy') {
        const cost = executionPrice * quantity;
        if (cost > virtualBalance) return res.status(400).json({ error: 'Insufficient virtual cash balance' });
        
        let pos = await Position.findOne({ symbol: symUpper });
        if (!pos) {
          pos = new Position({ symbol: symUpper, side: 'buy', averagePrice: executionPrice, quantity });
        } else {
          const totalCost = (pos.averagePrice * pos.quantity) + cost;
          pos.quantity += quantity;
          pos.averagePrice = parseFloat((totalCost / pos.quantity).toFixed(2));
        }
        await pos.save();
        virtualBalance = parseFloat((virtualBalance - cost).toFixed(2));
      } else { // sell
        let pos = await Position.findOne({ symbol: symUpper });
        if (!pos || pos.quantity < quantity) return res.status(400).json({ error: 'Holdings insufficient for sell order' });
        
        const profit = parseFloat(((executionPrice - pos.averagePrice) * quantity).toFixed(2));
        pos.quantity -= quantity;
        pos.realizedPnL = parseFloat((pos.realizedPnL + profit).toFixed(2));
        virtualBalance = parseFloat((virtualBalance + (executionPrice * quantity)).toFixed(2));
        
        if (pos.quantity === 0) {
          await Position.findByIdAndDelete(pos._id);
        } else {
          await pos.save();
        }
      }

      const completedOrder = new Order({
        symbol: symUpper,
        side,
        type,
        price: executionPrice,
        quantity,
        status: 'filled',
        filledAt: new Date()
      });
      await completedOrder.save();

      // Broadcast update
      wss.clients.forEach(c => {
        if (c.readyState === 1) {
          c.send(JSON.stringify({ type: 'portfolio_update', balance: virtualBalance }));
        }
      });

      return res.status(201).json({ order: completedOrder, balance: virtualBalance });
    } else {
      // Limit order - post to pending order book
      const pendingOrder = new Order({
        symbol: symUpper,
        side,
        type,
        price: executionPrice,
        quantity,
        status: 'pending'
      });
      await pendingOrder.save();
      return res.status(201).json({ order: pendingOrder, balance: virtualBalance });
    }
  } catch (err) {
    res.status(500).json({ error: 'Order placement failed', details: err.message });
  }
});

// DELETE /api/trading/orders/:id - Cancel working limit order
app.delete('/api/trading/orders/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const order = await Order.findOne({ _id: id, status: 'pending' });
    if (!order) return res.status(404).json({ error: 'Pending order not found' });
    order.status = 'cancelled';
    await order.save();
    res.json({ message: 'Order successfully cancelled', order });
  } catch (err) {
    res.status(500).json({ error: 'Failed to cancel order' });
  }
});

/* ── Alert System API Endpoints ────────────────────────────── */

app.get('/api/alerts', async (req, res) => {
  try {
    const list = await Alert.find({}).sort({ createdAt: -1 });
    res.json(list);
  } catch (err) {
    res.status(500).json({ error: 'Failed to load alerts' });
  }
});

app.post('/api/alerts', async (req, res) => {
  try {
    const { symbol, condition, value } = req.body;
    if (!symbol || !condition || value === undefined) {
      return res.status(400).json({ error: 'Symbol, condition, and price level are required' });
    }
    const newAlert = new Alert({
      symbol: symbol.trim().toUpperCase(),
      condition,
      value: parseFloat(value)
    });
    await newAlert.save();
    res.status(201).json(newAlert);
  } catch (err) {
    res.status(500).json({ error: 'Failed to create alert' });
  }
});

app.delete('/api/alerts/:id', async (req, res) => {
  try {
    const { id } = req.params;
    await Alert.findByIdAndDelete(id);
    res.json({ message: 'Alert deleted successfully' });
  } catch (err) {
    res.status(500).json({ error: 'Failed to delete alert' });
  }
});

/* ── Workspace Templates API Endpoints ─────────────────────── */

app.get('/api/workspace/layouts', async (req, res) => {
  try {
    const layouts = await WorkspaceLayout.find({}).sort({ updatedAt: -1 });
    res.json(layouts);
  } catch (err) {
    res.status(500).json({ error: 'Failed to load workspace layouts' });
  }
});

app.get('/api/workspace/layouts/:name', async (req, res) => {
  try {
    const { name } = req.params;
    const item = await WorkspaceLayout.findOne({ name });
    if (!item) return res.status(404).json({ error: 'Layout not found' });
    res.json(item);
  } catch (err) {
    res.status(500).json({ error: 'Failed to fetch layout details' });
  }
});

app.post('/api/workspace/layouts', async (req, res) => {
  try {
    const { name, layout } = req.body;
    if (!name || !layout) return res.status(400).json({ error: 'Name and layout parameters required' });
    const item = await WorkspaceLayout.findOneAndUpdate(
      { name: name.trim() },
      { layout },
      { upsert: true, new: true }
    );
    res.status(200).json(item);
  } catch (err) {
    res.status(500).json({ error: 'Failed to save workspace layout' });
  }
});

app.delete('/api/workspace/layouts/:name', async (req, res) => {
  try {
    const { name } = req.params;
    await WorkspaceLayout.findOneAndDelete({ name });
    res.json({ message: 'Workspace layout deleted successfully' });
  } catch (err) {
    res.status(500).json({ error: 'Failed to delete layout' });
  }
});

/* ── Screener (MongoDB daily snapshot) ─────────────────────── */

function scheduleScreenerCron() {
  // 4:30 PM IST Mon–Fri (after NSE close) — cron uses server TZ; adjust via SCREENER_CRON env
  const expr = process.env.SCREENER_CRON || '30 11 * * 1-5';
  cron.schedule(expr, () => {
    runScreenerSync({ force: true }).catch((err) => {
      console.error('[ScreenerCron] Daily sync failed:', err.message);
    });
  });
  console.log(`Screener daily cron scheduled: ${expr}`);
}

function formatMarketStock(doc) {
  return {
    symbol: doc.symbol,
    name: doc.name,
    price: doc.price,
    change: doc.change,
    changePercent: doc.changePercent,
    marketCap: doc.marketCap,
    pe: doc.pe,
    eps: doc.eps,
    cmpBv: doc.cmpBv,
    divYield: doc.divYield,
    promHold: doc.promHold,
    profitGrowth: doc.profitGrowth,
    salesGrowth: doc.salesGrowth,
    roe: doc.roe ?? undefined,
    roa: doc.roa ?? undefined,
    exchange: doc.exchange,
    asOfDate: doc.asOfDate,
  };
}

app.get('/api/screener/meta', async (req, res) => {
  try {
    const asOfDate = (await getLatestAsOfDate()) || null;
    const sync = asOfDate
      ? await ScreenerSync.findOne({ asOfDate }).lean()
      : null;
    const count = asOfDate ? await MarketStock.countDocuments({ asOfDate }) : 0;
    const nseCount = asOfDate
      ? await MarketStock.countDocuments({ asOfDate, exchange: 'NSE' })
      : 0;
    const bseCount = asOfDate
      ? await MarketStock.countDocuments({ asOfDate, exchange: 'BSE' })
      : 0;

    res.json({
      asOfDate,
      syncing: isSyncInProgress(),
      status:
        sync?.status ||
        (count > 100 ? 'completed' : count > 0 ? 'partial' : 'pending'),
      universeSize: sync?.universeSize ?? 0,
      savedCount: count > 0 ? count : (sync?.savedCount ?? 0),
      nseCount: sync?.nseCount || nseCount,
      bseCount: sync?.bseCount || bseCount,
      completedAt: sync?.completedAt || null,
      errorMessage: sync?.errorMessage || '',
    });
  } catch (err) {
    res.status(500).json({ error: 'Failed to read screener metadata' });
  }
});

app.get('/api/screener', async (req, res) => {
  try {
    const asOfDate = (await getLatestAsOfDate()) || null;
    if (!asOfDate) {
      return res.json({
        asOfDate: null,
        total: 0,
        count: 0,
        stocks: [],
        message: 'No screener snapshot yet. Run sync or wait for cron.',
      });
    }

    const mongoQuery = buildScreenerQuery(req.query, asOfDate);
    const sort = buildSort(req.query);
    const offset = Math.max(0, parseInt(req.query.offset || '0', 10));
    const limit = Math.min(
      2000,
      Math.max(1, parseInt(req.query.limit || '5000', 10))
    );

    const [total, docs] = await Promise.all([
      MarketStock.countDocuments(mongoQuery),
      MarketStock.find(mongoQuery)
        .sort(sort)
        .skip(offset)
        .limit(limit)
        .lean(),
    ]);

    res.json({
      asOfDate,
      exchange: req.query.exchange || 'all',
      total,
      offset,
      limit,
      count: docs.length,
      stocks: docs.map(formatMarketStock),
    });
  } catch (err) {
    console.error('Screener query failed:', err);
    res.status(500).json({ error: 'Failed to query screener data' });
  }
});

app.post('/api/screener/sync', async (req, res) => {
  try {
    const force = req.query.force === 'true' || req.body?.force === true;
    if (isSyncInProgress()) {
      return res.status(409).json({ error: 'Sync already in progress' });
    }
    const result = await runScreenerSync({ force });
    res.json(result);
  } catch (err) {
    res.status(500).json({ error: err.message || 'Screener sync failed' });
  }
});

// Error handling middleware
app.use((err, req, res, next) => {
  console.error('Unhandled server error:', err);
  res.status(500).json({ error: 'Internal server error occurred' });
});

async function boot() {
  try {
    await initDatabase();
  } catch (err) {
    console.error('Database init failed:', err.message);
    console.warn('API running without MongoDB — screener endpoints will be empty until sync succeeds.');
  }

  server.listen(PORT, () => {
    console.log(`Vision backend server is running on http://localhost:${PORT}`);
  });
}

boot();
