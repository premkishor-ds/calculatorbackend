require('dotenv').config();
const express = require('express');
const mongoose = require('mongoose');
const cors = require('cors');
const Stock = require('./models/Stock');
const CustomTag = require('./models/CustomTag');

const app = express();
const PORT = process.env.PORT || 5001;

// Middleware
app.use(cors());
app.use(express.json());

// MongoDB Connection
mongoose.connect(process.env.MONGODB_URI)
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
    const count = await Stock.countDocuments();
    if (count === 0) {
      console.log('Stock collection is empty. Seeding default stock list...');
      await Stock.insertMany(DEFAULT_STOCKS_SEED.map(item => ({
        symbol: item.symbol,
        name: item.name,
        isFavourite: false
      })));
      console.log('Seeding completed successfully!');
    } else {
      console.log(`Database already contains ${count} stocks. Skipping seeding.`);
    }
  } catch (error) {
    console.error('Error seeding default stocks:', error);
  }
}

/* ── API Routes ────────────────────────────────────────────── */

// GET /api/stocks - Fetch all stocks
app.get('/api/stocks', async (req, res) => {
  try {
    const stocks = await Stock.find({}).sort({ updatedAt: -1 });
    res.json(stocks);
  } catch (error) {
    console.error('GET /api/stocks error:', error);
    res.status(500).json({ error: 'Failed to retrieve stocks from database' });
  }
});

// POST /api/stocks - Add a new stock
app.post('/api/stocks', async (req, res) => {
  try {
    let { symbol, name, isFavourite, isfavoute } = req.body;
    
    if (!symbol) {
      return res.status(400).json({ error: 'Stock symbol is required' });
    }
    if (!name) {
      return res.status(400).json({ error: 'Stock name is required' });
    }

    const formattedSymbol = symbol.trim().toUpperCase();
    const formattedName = name.trim();
    
    // Normalize isFavourite (handles alternate spellings like "isfavoute")
    const favStatus = isFavourite !== undefined ? isFavourite : (isfavoute !== undefined ? isfavoute : false);

    // Check if stock already exists
    let existingStock = await Stock.findOne({ symbol: formattedSymbol });
    if (existingStock) {
      // If it exists, let's update its favorite status or name if they changed, or just return it
      existingStock.name = formattedName;
      existingStock.isFavourite = favStatus;
      await existingStock.save();
      return res.status(200).json(existingStock);
    }

    const newStock = new Stock({
      symbol: formattedSymbol,
      name: formattedName,
      isFavourite: favStatus
    });

    await newStock.save();
    console.log(`Added stock: ${formattedSymbol} (${formattedName})`);
    res.status(201).json(newStock);
  } catch (error) {
    console.error('POST /api/stocks error:', error);
    if (error.code === 11000) {
      return res.status(409).json({ error: 'Stock symbol already exists in database' });
    }
    res.status(500).json({ error: 'Failed to add stock to database', details: error.message });
  }
});

// PATCH /api/stocks/:symbol - Toggle or set favorite status for a stock by symbol
app.patch('/api/stocks/:symbol', async (req, res) => {
  try {
    const symbolParam = req.params.symbol.trim().toUpperCase();
    const { isFavourite, isfavoute, tags } = req.body;

    const stock = await Stock.findOne({ symbol: symbolParam });
    if (!stock) {
      return res.status(404).json({ error: `Stock with symbol ${symbolParam} not found` });
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

// DELETE /api/stocks/:symbol - Delete a stock by symbol
app.delete('/api/stocks/:symbol', async (req, res) => {
  try {
    const symbolParam = req.params.symbol.trim().toUpperCase();
    const result = await Stock.findOneAndDelete({ symbol: symbolParam });
    
    if (!result) {
      return res.status(404).json({ error: `Stock with symbol ${symbolParam} not found` });
    }

    console.log(`Deleted stock: ${symbolParam}`);
    res.json({ message: `Stock ${symbolParam} successfully deleted`, deletedStock: result });
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
    // Merge defaults with saved — saved values override defaults
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

// Start the server
app.listen(PORT, () => {
  console.log(`Vision backend server is running on http://localhost:${PORT}`);
});
