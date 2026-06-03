require('dotenv').config();
const express = require('express');
const crypto = require('crypto');
const http = require('http');
const { WebSocketServer } = require('ws');
const _mongoose = require('mongoose');
const cors = require('cors');
const { setupSwagger } = require('./swagger');

// Import Schemas
const Stock = require('./models/Stock');
const CustomTag = require('./models/CustomTag');
const Watchlist = require('./models/Watchlist');
const Drawing = require('./models/Drawing');
const Alert = require('./models/Alert');
const WorkspaceLayout = require('./models/WorkspaceLayout');
const MarketStock = require('./models/MarketStock');
const ScreenerSync = require('./models/ScreenerSync');
const Holding = require('./models/Holding');
const Notification = require('./models/Notification');

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
  if (!req.path.startsWith('/api-docs') && req.path !== '/swagger-custom.js') {
    res.setHeader('Content-Security-Policy', "default-src 'self'; frame-ancestors 'none';");
  } else {
    // Relaxed Content-Security-Policy so that Swagger UI styles, scripts, and fonts load correctly
    res.setHeader('Content-Security-Policy', "default-src 'self' 'unsafe-inline' 'unsafe-eval' data: https://*; img-src 'self' data: https://*; style-src 'self' 'unsafe-inline' https://*; script-src 'self' 'unsafe-inline' 'unsafe-eval' https://*;");
  }
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
setupSwagger(app);
app.get('/swagger-custom.js', (req, res) => res.sendFile(__dirname + '/swagger-custom.js'));

// MongoDB + daily screener snapshot (auto-sync if today's data is missing)
async function initDatabase() {
  await connectMongo();
  await seedDefaultStocks();
  await syncSimulatorSymbolsFromDb();
  scheduleScreenerCron();
  // Do not block HTTP — full sync can take 30–60+ min on Render
  ensureTodaySnapshot().catch((err) => {
    console.error('[ScreenerSync] Background startup sync failed:', err.message);
  });
}

/** Register a symbol in the simulator with a real-time price seed from Yahoo Finance */
async function registerSymbolInSimulator(symbol) {
  if (!symbol || stockPrices[symbol]) return;
  let seedPrice = 500.0;
  try {
    const { yahooFinance, YAHOO_MODULE_OPTS } = require('./lib/yahoo-finance');
    const q = await yahooFinance.quote(symbol, {}, YAHOO_MODULE_OPTS);
    const live = q?.regularMarketPrice;
    if (live && live > 0) seedPrice = live;
  } catch { /* use fallback */ }
  stockPrices[symbol] = { price: seedPrice, change: 0, changePercent: 0, open: seedPrice };
}

/** No-op kept for compatibility — symbols are now registered on-demand */
async function syncSimulatorSymbolsFromDb() {}

// Predefined list of default symbols and their correct corporate names
const DEFAULT_STOCKS_SEED = [
  { symbol: '20MICRONS.NS', name: '20 Microns Limited' },
  { symbol: '21STCENMGM.NS', name: '21st Century Management Services Limited' },
  { symbol: '360ONE.NS', name: '360 ONE WAM LIMITED' },
  { symbol: '3BBLACKBIO.NS', name: '3B Blackbio Dx Limited' },
  { symbol: '3IINFOLTD.NS', name: '3i Infotech Limited' },
  { symbol: '3MINDIA.NS', name: '3M India Limited' },
  { symbol: '3PLAND.NS', name: '3P Land Holdings Limited' },
  { symbol: '5PAISA.NS', name: '5Paisa Capital Limited' },
  { symbol: '63MOONS.NS', name: '63 moons technologies limited' },
  { symbol: 'A2ZINFRA.NS', name: 'A2Z Infra Engineering Limited' },
  { symbol: 'AAATECH.NS', name: 'AAA Technologies Limited' },
  { symbol: 'AADHARHFC.NS', name: 'Aadhar Housing Finance Limited' },
  { symbol: 'AAKASH.NS', name: 'Aakash Exploration Services Limited' },
  { symbol: 'AAREYDRUGS.NS', name: 'Aarey Drugs & Pharmaceuticals Limited' },
  { symbol: 'AARNAV.NS', name: 'Aarnav Fashions Limited' },
  { symbol: 'AARON.NS', name: 'Aaron Industries Limited' },
  { symbol: 'AARTECH.NS', name: 'Aartech Solonics Limited' },
  { symbol: 'AARTIDRUGS.NS', name: 'Aarti Drugs Limited' },
  { symbol: 'AARTIIND.NS', name: 'Aarti Industries Limited' },
  { symbol: 'AARTIPHARM.NS', name: 'Aarti Pharmalabs Limited' },
  { symbol: 'AARTISURF.NS', name: 'Aarti Surfactants Limited' },
  { symbol: 'AARVI.NS', name: 'Aarvi Encon Limited' },
  { symbol: 'AAVAS.NS', name: 'Aavas Financiers Limited' },
  { symbol: 'ABANSENT.NS', name: 'Abans Enterprises Limited' },
  { symbol: 'ABB.NS', name: 'ABB India Limited' },
  { symbol: 'ABBOTINDIA.NS', name: 'Abbott India Limited' },
  { symbol: 'ABCAPITAL.NS', name: 'Aditya Birla Capital Limited' },
  { symbol: 'ABCOTS.NS', name: 'A B Cotspin India Limited' },
  { symbol: 'ABDL.NS', name: 'Allied Blenders and Distillers Limited' },
  { symbol: 'ABFRL.NS', name: 'Aditya Birla Fashion and Retail Limited' },
  { symbol: 'ABINFRA.NS', name: 'A B Infrabuild Limited' },
  { symbol: 'ABLBL.NS', name: 'Aditya Birla Lifestyle Brands Limited' },
  { symbol: 'ABMINTLLTD.NS', name: 'ABM International Limited' },
  { symbol: 'ABMKNO.NS', name: 'ABM Knowledgeware Limited' },
  { symbol: 'ABREL.NS', name: 'Aditya Birla Real Estate Limited' },
  { symbol: 'ABSLAMC.NS', name: 'Aditya Birla Sun Life AMC Limited' },
  { symbol: 'ACC.NS', name: 'ACC Limited' },
  { symbol: 'ACCELYA.NS', name: 'Accelya Solutions India Limited' },
  { symbol: 'ACCURACY.NS', name: 'Accuracy Shipping Limited' },
  { symbol: 'ACE.NS', name: 'Action Construction Equipment Limited' },
  { symbol: 'ACEINTEG.NS', name: 'Ace Integrated Solutions Limited' },
  { symbol: 'ACI.NS', name: 'Archean Chemical Industries Limited' },
  { symbol: 'ACL.NS', name: 'Andhra Cements Limited' },
  { symbol: 'ACMESOLAR.NS', name: 'Acme Solar Holdings Limited' },
  { symbol: 'ACSTECH.NS', name: 'ACS Technologies Limited' },
  { symbol: 'ACUTAAS.NS', name: 'Acutaas Chemicals Limited' },
  { symbol: 'ADANIENSOL.NS', name: 'Adani Energy Solutions Limited' },
  { symbol: 'ADANIENT.NS', name: 'Adani Enterprises Limited' },
  { symbol: 'ADANIGREEN.NS', name: 'Adani Green Energy Limited' },
  { symbol: 'ADANIPORTS.NS', name: 'Adani Ports and Special Economic Zone Limited' },
  { symbol: 'ADANIPOWER.NS', name: 'Adani Power Limited' },
  { symbol: 'ADFFOODS.NS', name: 'ADF Foods Limited' },
  { symbol: 'ADL.NS', name: 'Archidply Decor Limited' },
  { symbol: 'ADOR.NS', name: 'Ador Welding Limited' },
  { symbol: 'ADROITINFO.NS', name: 'Adroit Infotech Limited' },
  { symbol: 'ADSL.NS', name: 'Allied Digital Services Limited' },
  { symbol: 'ADVAIT.NS', name: 'Advait Energy Transitions Limited' },
  { symbol: 'ADVANCE.NS', name: 'Advance Agrolife Limited' },
  { symbol: 'ADVANIHOTR.NS', name: 'Advani Hotels & Resorts (India) Limited' },
  { symbol: 'ADVENTHTL.NS', name: 'Advent Hotels International Limited' },
  { symbol: 'ADVENZYMES.NS', name: 'Advanced Enzyme Technologies Limited' },
  { symbol: 'AEGISLOG.NS', name: 'Aegis Logistics Limited' },
  { symbol: 'AEGISVOPAK.NS', name: 'Aegis Vopak Terminals Limited' },
  { symbol: 'AEPL.NS', name: 'Artemis Electricals and Projects Limited' },
  { symbol: 'AEQUS.NS', name: 'Aequs Limited' },
  { symbol: 'AEROENTER.NS', name: 'Aeroflex Enterprises Limited' },
  { symbol: 'AEROFLEX.NS', name: 'Aeroflex Industries Limited' },
  { symbol: 'AERONEU.NS', name: 'Aeroflex Neu Limited' },
  { symbol: 'AETHER.NS', name: 'Aether Industries Limited' },
  { symbol: 'AFCONS.NS', name: 'Afcons Infrastructure Limited' },
  { symbol: 'AFFLE.NS', name: 'Affle 3i Limited' },
  { symbol: 'AFFORDABLE.NS', name: 'Affordable Robotic & Automation Limited' },
  { symbol: 'AFIL.NS', name: 'Akme Fintrade (India) Limited' },
  { symbol: 'AFSL.NS', name: 'Abans Financial Services Limited' },
  { symbol: 'AGARIND.NS', name: 'Agarwal Industrial Corporation Limited' },
  { symbol: 'AGARWALEYE.NS', name: 'Dr. Agarwal\'s Health Care Limited' },
  { symbol: 'AGI.NS', name: 'AGI Greenpac Limited' },
  { symbol: 'AGIIL.NS', name: 'Agi Infra Limited' },
  { symbol: 'AGRITECH.NS', name: 'Agri-Tech (India) Limited' },
  { symbol: 'AGROPHOS.NS', name: 'Agro Phos India Limited' },
  { symbol: 'AHCL.NS', name: 'Anlon Healthcare Limited' },
  { symbol: 'AHLADA.NS', name: 'Ahlada Engineers Limited' },
  { symbol: 'AHLEAST.NS', name: 'Asian Hotels (East) Limited' },
  { symbol: 'AHLUCONT.NS', name: 'Ahluwalia Contracts (India) Limited' },
  { symbol: 'AHLWEST.NS', name: 'Asian Hotels (West) Limited' },
  { symbol: 'AIAENG.NS', name: 'AIA Engineering Limited' },
  { symbol: 'AIIL.NS', name: 'Authum Investment & Infrastructure Limited' },
  { symbol: 'AIRAN.NS', name: 'Airan Limited' },
  { symbol: 'AIROLAM.NS', name: 'Airo Lam limited' },
  { symbol: 'AJANTPHARM.NS', name: 'Ajanta Pharma Limited' },
  { symbol: 'AJAXENGG.NS', name: 'Ajax Engineering Limited' },
  { symbol: 'AJMERA.NS', name: 'Ajmera Realty & Infra India Limited' },
  { symbol: 'AJOONI.NS', name: 'Ajooni Biotech Limited' },
  { symbol: 'AKASH.NS', name: 'Akash Infra-Projects Limited' },
  { symbol: 'AKCAPIT.NS', name: 'AK Capital Services Limited' },
  { symbol: 'AKG.NS', name: 'Akg Exim Limited' },
  { symbol: 'AKI.NS', name: 'AKI India Limited' },
  { symbol: 'AKSHAR.NS', name: 'Akshar Spintex Limited' },
  { symbol: 'AKSHARCHEM.NS', name: 'AksharChem India Limited' },
  { symbol: 'AKSHOPTFBR.NS', name: 'Aksh Optifibre Limited' },
  { symbol: 'AKUMS.NS', name: 'Akums Drugs and Pharmaceuticals Limited' },
  { symbol: 'ALANKIT.NS', name: 'Alankit Limited' },
  { symbol: 'ALBERTDAVD.NS', name: 'Albert David Limited' },
  { symbol: 'ALEMBICLTD.NS', name: 'Alembic Limited' },
  { symbol: 'ALGOQUANT.NS', name: 'Algoquant Fintech Limited' },
  { symbol: 'ALICON.NS', name: 'Alicon Castalloy Limited' },
  { symbol: 'ALIVUS.NS', name: 'Alivus Life Sciences Limited' },
  { symbol: 'ALKALI.NS', name: 'Alkali Metals Limited' },
  { symbol: 'ALKEM.NS', name: 'Alkem Laboratories Limited' },
  { symbol: 'ALKYLAMINE.NS', name: 'Alkyl Amines Chemicals Limited' },
  { symbol: 'ALLCARGO.NS', name: 'Allcargo Logistics Limited' },
  { symbol: 'ALLDIGI.NS', name: 'Alldigi Tech Limited' },
  { symbol: 'ALLTIME.NS', name: 'All Time Plastics Limited' },
  { symbol: 'ALMONDZ.NS', name: 'Almondz Global Securities Limited' },
  { symbol: 'ALOKINDS.NS', name: 'Alok Industries Limited' },
  { symbol: 'ALPA.NS', name: 'Alpa Laboratories Limited' },
  { symbol: 'ALPHAGEO.NS', name: 'Alphageo (India) Limited' },
  { symbol: 'AMAGI.NS', name: 'Amagi Media Labs Limited' },
  { symbol: 'AMANTA.NS', name: 'Amanta Healthcare Limited' },
  { symbol: 'AMBALALSA.NS', name: 'Ambalal Sarabhai Enterprises Limited' },
  { symbol: 'AMBER.NS', name: 'Amber Enterprises India Limited' },
  { symbol: 'AMBICAAGAR.NS', name: 'Ambica Agarbathies & Aroma industries Limited' },
  { symbol: 'AMBIKCO.NS', name: 'Ambika Cotton Mills Limited' },
  { symbol: 'AMBUJACEM.NS', name: 'Ambuja Cements Limited' },
  { symbol: 'AMDIND.NS', name: 'AMD Industries Limited' },
  { symbol: 'AMIRCHAND.NS', name: 'Amir Chand Jagdish Kumar (Exports) Limited' },
  { symbol: 'AMJLAND.NS', name: 'Amj Land Holdings Limited' },
  { symbol: 'AMNPLST.NS', name: 'Amines & Plasticizers Limited' },
  { symbol: 'AMRUTANJAN.NS', name: 'Amrutanjan Health Care Limited' },
  { symbol: 'ANANDRATHI.NS', name: 'Anand Rathi Wealth Limited' },
  { symbol: 'ANANTRAJ.NS', name: 'Anant Raj Limited' },
  { symbol: 'ANDHRAPAP.NS', name: 'ANDHRA PAPER LIMITED' },
  { symbol: 'ANDHRSUGAR.NS', name: 'The Andhra Sugars Limited' },
  { symbol: 'ANGELONE.NS', name: 'Angel One Limited' },
  { symbol: 'ANIKINDS.NS', name: 'Anik Industries Limited' },
  { symbol: 'ANKITMETAL.NS', name: 'Ankit Metal & Power Limited' },
  { symbol: 'ANMOL.NS', name: 'Anmol India Limited' },
  { symbol: 'ANSALAPI.NS', name: 'Ansal Properties & Infrastructure Limited' },
  { symbol: 'ANTELOPUS.NS', name: 'Antelopus Selan Energy Limited' },
  { symbol: 'ANTGRAPHIC.NS', name: 'Antarctica Limited' },
  { symbol: 'ANTHEM.NS', name: 'Anthem Biosciences Limited' },
  { symbol: 'ANUHPHR.NS', name: 'Anuh Pharma Limited' },
  { symbol: 'ANUP.NS', name: 'The Anup Engineering Limited' },
  { symbol: 'ANURAS.NS', name: 'Anupam Rasayan India Limited' },
  { symbol: 'APARINDS.NS', name: 'Apar Industries Limited' },
  { symbol: 'APCL.NS', name: 'Anjani Portland Cement Limited' },
  { symbol: 'APCOTEXIND.NS', name: 'Apcotex Industries Limited' },
  { symbol: 'APEX.NS', name: 'Apex Frozen Foods Limited' },
  { symbol: 'APLAPOLLO.NS', name: 'APL Apollo Tubes Limited' },
  { symbol: 'APLLTD.NS', name: 'Alembic Pharmaceuticals Limited' },
  { symbol: 'APOLLO.NS', name: 'Apollo Micro Systems Limited' },
  { symbol: 'APOLLOHOSP.NS', name: 'Apollo Hospitals Enterprise Limited' },
  { symbol: 'APOLLOPIPE.NS', name: 'Apollo Pipes Limited' },
  { symbol: 'APOLLOTYRE.NS', name: 'Apollo Tyres Limited' },
  { symbol: 'APOLSINHOT.NS', name: 'Apollo Sindoori Hotels Limited' },
  { symbol: 'APTECHT.NS', name: 'Aptech Limited' },
  { symbol: 'APTUS.NS', name: 'Aptus Value Housing Finance India Limited' },
  { symbol: 'AQYLON.NS', name: 'Aqylon Nexus Limited' },
  { symbol: 'ARCHIDPLY.NS', name: 'Archidply Industries Limited' },
  { symbol: 'ARCHIES.NS', name: 'Archies Limited' },
  { symbol: 'ARE&M.NS', name: 'Amara Raja Energy & Mobility Limited' },
  { symbol: 'ARENTERP.NS', name: 'Rajdarshan Industries Limited' },
  { symbol: 'ARFIN.NS', name: 'Arfin India Limited' },
  { symbol: 'ARIES.NS', name: 'Aries Agro Limited' },
  { symbol: 'ARIHANT.NS', name: 'Arihant Foundations & Housing Limited' },
  { symbol: 'ARIHANTCAP.NS', name: 'Arihant Capital Markets Limited' },
  { symbol: 'ARIHANTSUP.NS', name: 'Arihant Superstructures Limited' },
  { symbol: 'ARIS.NS', name: 'Arisinfra Solutions Limited' },
  { symbol: 'ARKADE.NS', name: 'Arkade Developers Limited' },
  { symbol: 'ARMANFIN.NS', name: 'Arman Financial Services Limited' },
  { symbol: 'AROGRANITE.NS', name: 'Aro Granite Industries Limited' },
  { symbol: 'ARROWGREEN.NS', name: 'Arrow Greentech Limited' },
  { symbol: 'ARSHIYA.NS', name: 'Arshiya Limited' },
  { symbol: 'ARSSBL.NS', name: 'Anand Rathi Share and Stock Brokers Limited' },
  { symbol: 'ARTEMISMED.NS', name: 'Artemis Medicare Services Limited' },
  { symbol: 'ARTNIRMAN.NS', name: 'Art Nirman Limited' },
  { symbol: 'ARVEE.NS', name: 'Arvee Laboratories (India) Limited' },
  { symbol: 'ARVIND.NS', name: 'Arvind Limited' },
  { symbol: 'ARVINDFASN.NS', name: 'Arvind Fashions Limited' },
  { symbol: 'ARVSMART.NS', name: 'Arvind SmartSpaces Limited' },
  { symbol: 'ASAHIINDIA.NS', name: 'Asahi India Glass Limited' },
  { symbol: 'ASAHISONG.NS', name: 'Asahi Songwon Colors Limited' },
  { symbol: 'ASAL.NS', name: 'Automotive Stampings and Assemblies Limited' },
  { symbol: 'ASALCBR.NS', name: 'Associated Alcohols & Breweries Ltd.' },
  { symbol: 'ASHAPURMIN.NS', name: 'Ashapura Minechem Limited' },
  { symbol: 'ASHIANA.NS', name: 'Ashiana Housing Limited' },
  { symbol: 'ASHIKA.NS', name: 'Ashika Credit Capital Limited' },
  { symbol: 'ASHIMASYN.NS', name: 'Ashima Limited' },
  { symbol: 'ASHOKA.NS', name: 'Ashoka Buildcon Limited' },
  { symbol: 'ASHOKAMET.NS', name: 'Ashoka Metcast Limited' },
  { symbol: 'ASHOKLEY.NS', name: 'Ashok Leyland Limited' },
  { symbol: 'ASIANENE.NS', name: 'Asian Energy Services Limited' },
  { symbol: 'ASIANHOTNR.NS', name: 'Asian Hotels (North) Limited' },
  { symbol: 'ASIANPAINT.NS', name: 'Asian Paints Limited' },
  { symbol: 'ASIANTILES.NS', name: 'Asian Granito India Limited' },
  { symbol: 'ASKAUTOLTD.NS', name: 'ASK Automotive Limited' },
  { symbol: 'ASMS.NS', name: 'Bartronics India Limited' },
  { symbol: 'ASPINWALL.NS', name: 'Aspinwall and Company Limited' },
  { symbol: 'ASTAR.NS', name: 'Asian Star Company Limited' },
  { symbol: 'ASTEC.NS', name: 'Astec LifeSciences Limited' },
  { symbol: 'ASTERDM.NS', name: 'Aster DM Healthcare Limited' },
  { symbol: 'ASTRAL.NS', name: 'Astral Limited' },
  { symbol: 'ASTRAMICRO.NS', name: 'Astra Microwave Products Limited' },
  { symbol: 'ASTRAZEN.NS', name: 'AstraZeneca Pharma India Limited' },
  { symbol: 'ASTRON.NS', name: 'Astron Paper & Board Mill Limited' },
  { symbol: 'ATALREAL.NS', name: 'Atal Realtech Limited' },
  { symbol: 'ATAM.NS', name: 'Atam Valves Limited' },
  { symbol: 'ATGL.NS', name: 'Adani Total Gas Limited' },
  { symbol: 'ATHERENERG.NS', name: 'Ather Energy Limited' },
  { symbol: 'ATL.NS', name: 'Allcargo Terminals Limited' },
  { symbol: 'ATLANTAA.NS', name: 'ATLANTAA LIMITED' },
  { symbol: 'ATLANTAELE.NS', name: 'Atlanta Electricals Limited' },
  { symbol: 'ATLASCYCLE.NS', name: 'Atlas Cycles (Haryana) Limited' },
  { symbol: 'ATUL.NS', name: 'Atul Limited' },
  { symbol: 'ATULAUTO.NS', name: 'Atul Auto Limited' },
  { symbol: 'AUBANK.NS', name: 'AU Small Finance Bank Limited' },
  { symbol: 'AURIGROW.NS', name: 'Auri Grow India Limited' },
  { symbol: 'AURIONPRO.NS', name: 'Aurionpro Solutions Limited' },
  { symbol: 'AUROPHARMA.NS', name: 'Aurobindo Pharma Limited' },
  { symbol: 'AURUM.NS', name: 'Aurum PropTech Limited' },
  { symbol: 'AUSOMENT.NS', name: 'Ausom Enterprise Limited' },
  { symbol: 'AUTOAXLES.NS', name: 'Automotive Axles Limited' },
  { symbol: 'AUTOIND.NS', name: 'Autoline Industries Limited' },
  { symbol: 'AVADHSUGAR.NS', name: 'Avadh Sugar & Energy Limited' },
  { symbol: 'AVAILFC.NS', name: 'Available Finance Limited' },
  { symbol: 'AVALON.NS', name: 'Avalon Technologies Limited' },
  { symbol: 'AVANTEL.NS', name: 'Avantel Limited' },
  { symbol: 'AVANTIFEED.NS', name: 'Avanti Feeds Limited' },
  { symbol: 'AVG.NS', name: 'AVG Logistics Limited' },
  { symbol: 'AVL.NS', name: 'Aditya Vision Limited' },
  { symbol: 'AVONMORE.NS', name: 'Avonmore Capital & Management Services Limited' },
  { symbol: 'AVROIND.NS', name: 'AVRO INDIA LIMITED' },
  { symbol: 'AVTNPL.NS', name: 'AVT Natural Products Limited' },
  { symbol: 'AWFIS.NS', name: 'Awfis Space Solutions Limited' },
  { symbol: 'AWHCL.NS', name: 'Antony Waste Handling Cell Limited' },
  { symbol: 'AWL.NS', name: 'AWL Agri Business Limited' },
  { symbol: 'AXISBANK.NS', name: 'Axis Bank Limited' },
  { symbol: 'AXISCADES.NS', name: 'AXISCADES Technologies Limited' },
  { symbol: 'AXITA.NS', name: 'Axita Cotton Limited' },
  { symbol: 'AYE.NS', name: 'Aye Finance Limited' },
  { symbol: 'AYMSYNTEX.NS', name: 'AYM Syntex Limited' },
  { symbol: 'AZAD.NS', name: 'Azad Engineering Limited' },
  { symbol: 'BAFNAPH.NS', name: 'Bafna Pharmaceuticals Limited' },
  { symbol: 'BAGFILMS.NS', name: 'B.A.G Films and Media Limited' },
  { symbol: 'BAIDFIN.NS', name: 'Baid Finserv Limited' },
  { symbol: 'BAJAJ-AUTO.NS', name: 'Bajaj Auto Limited' },
  { symbol: 'BAJAJCON.NS', name: 'Bajaj Consumer Care Limited' },
  { symbol: 'BAJAJELEC.NS', name: 'Bajaj Electricals Limited' },
  { symbol: 'BAJAJFINSV.NS', name: 'Bajaj Finserv Limited' },
  { symbol: 'BAJAJHCARE.NS', name: 'Bajaj Healthcare Limited' },
  { symbol: 'BAJAJHFL.NS', name: 'Bajaj Housing Finance Limited' },
  { symbol: 'BAJAJHIND.NS', name: 'Bajaj Hindusthan Sugar Limited' },
  { symbol: 'BAJAJHLDNG.NS', name: 'Bajaj Holdings & Investment Limited' },
  { symbol: 'BAJAJINDEF.NS', name: 'Indef Manufacturing Limited' },
  { symbol: 'BAJAJST.NS', name: 'Bajaj Steel Industries Limited' },
  { symbol: 'BAJEL.NS', name: 'Bajel Projects Limited' },
  { symbol: 'BAJFINANCE.NS', name: 'Bajaj Finance Limited' },
  { symbol: 'BALAJEE.NS', name: 'Shree Tirupati Balajee Agro Trading Company Limited' },
  { symbol: 'BALAJITELE.NS', name: 'Balaji Telefilms Limited' },
  { symbol: 'BALAMINES.NS', name: 'Balaji Amines Limited' },
  { symbol: 'BALAXI.NS', name: 'BALAXI PHARMACEUTICALS LIMITED' },
  { symbol: 'BALKRISHNA.NS', name: 'Balkrishna Paper Mills Limited' },
  { symbol: 'BALKRISIND.NS', name: 'Balkrishna Industries Limited' },
  { symbol: 'BALMLAWRIE.NS', name: 'Balmer Lawrie & Company Limited' },
  { symbol: 'BALPHARMA.NS', name: 'Bal Pharma Limited' },
  { symbol: 'BALRAMCHIN.NS', name: 'Balrampur Chini Mills Limited' },
  { symbol: 'BALUFORGE.NS', name: 'Balu Forge Industries Limited' },
  { symbol: 'BANARBEADS.NS', name: 'Banaras Beads Limited' },
  { symbol: 'BANARISUG.NS', name: 'Bannari Amman Sugars Limited' },
  { symbol: 'BANCOINDIA.NS', name: 'Banco Products (I) Limited' },
  { symbol: 'BANDHANBNK.NS', name: 'Bandhan Bank Limited' },
  { symbol: 'BANG.NS', name: 'Bang Overseas Limited' },
  { symbol: 'BANKA.NS', name: 'Banka BioLoo Limited' },
  { symbol: 'BANKBARODA.NS', name: 'Bank of Baroda' },
  { symbol: 'BANKINDIA.NS', name: 'Bank of India' },
  { symbol: 'BANSALWIRE.NS', name: 'Bansal Wire Industries Limited' },
  { symbol: 'BANSWRAS.NS', name: 'Banswara Syntex Limited' },
  { symbol: 'BASF.NS', name: 'BASF India Limited' },
  { symbol: 'BASML.NS', name: 'Bannari Amman Spinning Mills Limited' },
  { symbol: 'BATAINDIA.NS', name: 'Bata India Limited' },
  { symbol: 'BATLIBOI.NS', name: 'Batliboi Limited' },
  { symbol: 'BAYERCROP.NS', name: 'Bayer Cropscience Limited' },
  { symbol: 'BBL.NS', name: 'Bharat Bijlee Limited' },
  { symbol: 'BBOX.NS', name: 'Black Box Limited' },
  { symbol: 'BBTC.NS', name: 'The Bombay Burmah Trading Corporation Limited' },
  { symbol: 'BBTCL.NS', name: 'B&B Triplewall Containers Limited' },
  { symbol: 'BCG.NS', name: 'Brightcom Group Limited' },
  { symbol: 'BCLIND.NS', name: 'Bcl Industries Limited' },
  { symbol: 'BCONCEPTS.NS', name: 'Brand Concepts Limited' },
  { symbol: 'BCPL.NS', name: 'BCPL Railway Infrastructure Limited' },
  { symbol: 'BDL.NS', name: 'Bharat Dynamics Limited' },
  { symbol: 'BEARDSELL.NS', name: 'Beardsell Limited' },
  { symbol: 'BECTORFOOD.NS', name: 'Mrs. Bectors Food Specialities Limited' },
  { symbol: 'BEDMUTHA.NS', name: 'Bedmutha Industries Limited' },
  { symbol: 'BEEKAY.NS', name: 'Beekay Steel Industries Limited' },
  { symbol: 'BEL.NS', name: 'Bharat Electronics Limited' },
  { symbol: 'BELLACASA.NS', name: 'Bella Casa Fashion & Retail Limited' },
  { symbol: 'BELRISE.NS', name: 'Belrise Industries Limited' },
  { symbol: 'BEML.NS', name: 'BEML Limited' },
  { symbol: 'BENGALASM.NS', name: 'Bengal & Assam Company Limited' },
  { symbol: 'BEPL.NS', name: 'Bhansali Engineering Polymers Limited' },
  { symbol: 'BERGEPAINT.NS', name: 'Berger Paints (I) Limited' },
  { symbol: 'BESTAGRO.NS', name: 'Best Agrolife Limited' },
  { symbol: 'BETA.NS', name: 'Beta Drugs Limited' },
  { symbol: 'BFINVEST.NS', name: 'BF Investment Limited' },
  { symbol: 'BFUTILITIE.NS', name: 'BF Utilities Limited' },
  { symbol: 'BGRENERGY.NS', name: 'BGR Energy Systems Limited' },
  { symbol: 'BHAGCHEM.NS', name: 'Bhagiradha Chemicals & Industries Limited' },
  { symbol: 'BHAGERIA.NS', name: 'Bhageria Industries Limited' },
  { symbol: 'BHAGYANGR.NS', name: 'Bhagyanagar India Limited' },
  { symbol: 'BHANDARI.NS', name: 'Bhandari Hosiery Exports Limited' },
  { symbol: 'BHARATCOAL.NS', name: 'Bharat Coking Coal Limited' },
  { symbol: 'BHARATFORG.NS', name: 'Bharat Forge Limited' },
  { symbol: 'BHARATGEAR.NS', name: 'Bharat Gears Limited' },
  { symbol: 'BHARATRAS.NS', name: 'Bharat Rasayan Limited' },
  { symbol: 'BHARATSE.NS', name: 'Bharat Seats Limited' },
  { symbol: 'BHARATWIRE.NS', name: 'Bharat Wire Ropes Limited' },
  { symbol: 'BHARTIARTL.NS', name: 'Bharti Airtel Limited' },
  { symbol: 'BHARTIHEXA.NS', name: 'Bharti Hexacom Limited' },
  { symbol: 'BHEL.NS', name: 'Bharat Heavy Electricals Limited' },
  { symbol: 'BI.NS', name: 'Bilcare Limited' },
  { symbol: 'BIGBLOC.NS', name: 'Bigbloc Construction Limited' },
  { symbol: 'BIKAJI.NS', name: 'Bikaji Foods International Limited' },
  { symbol: 'BIL.NS', name: 'Bhartiya International Limited' },
  { symbol: 'BIMETAL.NS', name: 'Bimetal Bearings Limited' },
  { symbol: 'BIOCON.NS', name: 'Biocon Limited' },
  { symbol: 'BIOFILCHEM.NS', name: 'Biofil Chemicals & Pharmaceuticals Limited' },
  { symbol: 'BIRLACABLE.NS', name: 'Birla Cable Limited' },
  { symbol: 'BIRLACORPN.NS', name: 'Birla Corporation Limited' },
  { symbol: 'BIRLAMONEY.NS', name: 'Aditya Birla Money Limited' },
  { symbol: 'BIRLANU.NS', name: 'BirlaNu Limited' },
  { symbol: 'BIRLAPREC.NS', name: 'Birla Precision Technologies Limited' },
  { symbol: 'BLACKBUCK.NS', name: 'BLACKBUCK LIMITED' },
  { symbol: 'BLACKROSE.NS', name: 'Black Rose Inds. Limited' },
  { symbol: 'BLAL.NS', name: 'BEML Land Assets Limited' },
  { symbol: 'BLBLIMITED.NS', name: 'BLB Limited' },
  { symbol: 'BLIL.NS', name: 'Balmer Lawrie Investments Limited' },
  { symbol: 'BLISSGVS.NS', name: 'Bliss GVS Pharma Limited' },
  { symbol: 'BLKASHYAP.NS', name: 'B. L. Kashyap and Sons Limited' },
  { symbol: 'BLS.NS', name: 'BLS International Services Limited' },
  { symbol: 'BLSE.NS', name: 'BLS E-Services Limited' },
  { symbol: 'BLUECHIP.NS', name: 'Blue Chip India Limited' },
  { symbol: 'BLUECOAST.NS', name: 'Blue Coast Hotels Limited' },
  { symbol: 'BLUEDART.NS', name: 'Blue Dart Express Limited' },
  { symbol: 'BLUEJET.NS', name: 'Blue Jet Healthcare Limited' },
  { symbol: 'BLUESTARCO.NS', name: 'Blue Star Limited' },
  { symbol: 'BLUESTONE.NS', name: 'BlueStone Jewellery and Lifestyle Limited' },
  { symbol: 'BLUSPRING.NS', name: 'Bluspring Enterprises Limited' },
  { symbol: 'BMWVENTLTD.NS', name: 'BMW Ventures Limited' },
  { symbol: 'BNAGROCHEM.NS', name: 'BN Agrochem Limited' },
  { symbol: 'BNALTD.NS', name: 'B & A Limited' },
  { symbol: 'BODALCHEM.NS', name: 'Bodal Chemicals Limited' },
  { symbol: 'BOHRAIND.NS', name: 'Bohra Industries Limited' },
  { symbol: 'BOMDYEING.NS', name: 'Bombay Dyeing & Mfg Company Limited' },
  { symbol: 'BONLON.NS', name: 'Bonlon Industries Limited' },
  { symbol: 'BORANA.NS', name: 'Borana Weaves Limited' },
  { symbol: 'BOROLTD.NS', name: 'Borosil Limited' },
  { symbol: 'BORORENEW.NS', name: 'BOROSIL RENEWABLES LIMITED' },
  { symbol: 'BOROSCI.NS', name: 'Borosil Scientific Limited' },
  { symbol: 'BOSCH-HCIL.NS', name: 'BOSCH HOME COMFORT INDIA LIMITED' },
  { symbol: 'BOSCHLTD.NS', name: 'Bosch Limited' },
  { symbol: 'BPCL.NS', name: 'Bharat Petroleum Corporation Limited' },
  { symbol: 'BPL.NS', name: 'BPL Limited' },
  { symbol: 'BRIGADE.NS', name: 'Brigade Enterprises Limited' },
  { symbol: 'BRIGHOTEL.NS', name: 'Brigade Hotel Ventures Limited' },
  { symbol: 'BRITANNIA.NS', name: 'Britannia Industries Limited' },
  { symbol: 'BRNL.NS', name: 'Bharat Road Network Limited' },
  { symbol: 'BROOKS.NS', name: 'Brooks Laboratories Limited' },
  { symbol: 'BSE.NS', name: 'BSE Limited' },
  { symbol: 'BSHSL.NS', name: 'Bombay Super Hybrid Seeds Limited' },
  { symbol: 'BSL.NS', name: 'BSL Limited' },
  { symbol: 'BSOFT.NS', name: 'BIRLASOFT LIMITED' },
  { symbol: 'BTML.NS', name: 'Bodhi Tree Multimedia Limited' },
  { symbol: 'BTTL.NS', name: 'Bhilwara Technical Textiles Limited' },
  { symbol: 'BUILDPRO.NS', name: 'Shankara Buildpro Limited' },
  { symbol: 'BUTTERFLY.NS', name: 'Butterfly Gandhimathi Appliances Limited' },
  { symbol: 'BVCL.NS', name: 'Barak Valley Cements Limited' },
  { symbol: 'BYKE.NS', name: 'The Byke Hospitality Ltd' },
  { symbol: 'CALSOFT.NS', name: 'California Software Company Limited' },
  { symbol: 'CAMLINFINE.NS', name: 'Camlin Fine Sciences Limited' },
  { symbol: 'CAMPUS.NS', name: 'Campus Activewear Limited' },
  { symbol: 'CAMS.NS', name: 'Computer Age Management Services Limited' },
  { symbol: 'CANBK.NS', name: 'Canara Bank' },
  { symbol: 'CANFINHOME.NS', name: 'Can Fin Homes Limited' },
  { symbol: 'CANHLIFE.NS', name: 'Canara HSBC Life Insurance Company Limited' },
  { symbol: 'CANTABIL.NS', name: 'Cantabil Retail India Limited' },
  { symbol: 'CAPACITE.NS', name: 'Capacit\'e Infraprojects Limited' },
  { symbol: 'CAPILLARY.NS', name: 'Capillary Technologies India Limited' },
  { symbol: 'CAPITALSFB.NS', name: 'Capital Small Finance Bank Limited' },
  { symbol: 'CAPLIPOINT.NS', name: 'Caplin Point Laboratories Limited' },
  { symbol: 'CAPTRUST.NS', name: 'Capital Trust Limited' },
  { symbol: 'CARBORUNIV.NS', name: 'Carborundum Universal Limited' },
  { symbol: 'CARERATING.NS', name: 'CARE Ratings Limited' },
  { symbol: 'CARRARO.NS', name: 'Carraro India Limited' },
  { symbol: 'CARTRADE.NS', name: 'Cartrade Tech Limited' },
  { symbol: 'CARYSIL.NS', name: 'CARYSIL LIMITED' },
  { symbol: 'CASTROLIND.NS', name: 'Castrol India Limited' },
  { symbol: 'CCAVENUE.NS', name: 'AvenuesAI Limited' },
  { symbol: 'CCCL.NS', name: 'Consolidated Construction Consortium Limited' },
  { symbol: 'CCHHL.NS', name: 'Country Club Hospitality & Holidays Limited' },
  { symbol: 'CCL.NS', name: 'CCL Products (India) Limited' },
  { symbol: 'CDSL.NS', name: 'Central Depository Services (India) Limited' },
  { symbol: 'CEATLTD.NS', name: 'CEAT Limited' },
  { symbol: 'CEIGALL.NS', name: 'Ceigall India Limited' },
  { symbol: 'CEINSYS.NS', name: 'Ceinsys Tech Limited' },
  { symbol: 'CELEBRITY.NS', name: 'Celebrity Fashions Limited' },
  { symbol: 'CELLO.NS', name: 'Cello World Limited' },
  { symbol: 'CEMPRO.NS', name: 'Cemindia Projects Limited' },
  { symbol: 'CENTENKA.NS', name: 'Century Enka Limited' },
  { symbol: 'CENTEXT.NS', name: 'Century Extrusions Limited' },
  { symbol: 'CENTRALBK.NS', name: 'Central Bank of India' },
  { symbol: 'CENTRUM.NS', name: 'Centrum Capital Limited' },
  { symbol: 'CENTUM.NS', name: 'Centum Electronics Limited' },
  { symbol: 'CENTURYPLY.NS', name: 'Century Plyboards (India) Limited' },
  { symbol: 'CERA.NS', name: 'Cera Sanitaryware Limited' },
  { symbol: 'CEREBRAINT.NS', name: 'Cerebra Integrated Technologies Limited' },
  { symbol: 'CESC.NS', name: 'CESC Limited' },
  { symbol: 'CEWATER.NS', name: 'Concord Enviro Systems Limited' },
  { symbol: 'CGCL.NS', name: 'Capri Global Capital Limited' },
  { symbol: 'CGPOWER.NS', name: 'CG Power and Industrial Solutions Limited' },
  { symbol: 'CHALET.NS', name: 'Chalet Hotels Limited' },
  { symbol: 'CHAMBLFERT.NS', name: 'Chambal Fertilizers & Chemicals Limited' },
  { symbol: 'CHEMBOND.NS', name: 'Chembond Material Technologies Limited' },
  { symbol: 'CHEMBONDCH.NS', name: 'Chembond Chemicals Limited' },
  { symbol: 'CHEMCON.NS', name: 'Chemcon Speciality Chemicals Limited' },
  { symbol: 'CHEMFAB.NS', name: 'Chemfab Alkalis Limited' },
  { symbol: 'CHEMPLASTS.NS', name: 'Chemplast Sanmar Limited' },
  { symbol: 'CHENNPETRO.NS', name: 'Chennai Petroleum Corporation Limited' },
  { symbol: 'CHEVIOT.NS', name: 'Cheviot Company Limited' },
  { symbol: 'CHOICEIN.NS', name: 'Choice International Limited' },
  { symbol: 'CHOLAFIN.NS', name: 'Cholamandalam Investment and Finance Company Limited' },
  { symbol: 'CHOLAHLDNG.NS', name: 'Cholamandalam Financial Holdings Limited' },
  { symbol: 'CIEINDIA.NS', name: 'CIE Automotive India Limited' },
  { symbol: 'CIFL.NS', name: 'Capital India Finance Limited' },
  { symbol: 'CINELINE.NS', name: 'Cineline India Limited' },
  { symbol: 'CINEVISTA.NS', name: 'Cinevista Limited' },
  { symbol: 'CIPLA.NS', name: 'Cipla Limited' },
  { symbol: 'CLEAN.NS', name: 'Clean Science and Technology Limited' },
  { symbol: 'CLEANMAX.NS', name: 'Clean Max Enviro Energy Solutions Limited' },
  { symbol: 'CLEDUCATE.NS', name: 'CL Educate Limited' },
  { symbol: 'CLSEL.NS', name: 'Chaman Lal Setia Exports Limited' },
  { symbol: 'CMPDI.NS', name: 'Central Mine Planning & Design Institute Limited' },
  { symbol: 'CMSINFO.NS', name: 'CMS Info Systems Limited' },
  { symbol: 'CNL.NS', name: 'Creative Newtech Limited' },
  { symbol: 'COALINDIA.NS', name: 'Coal India Limited' },
  { symbol: 'COASTCORP.NS', name: 'Coastal Corporation Limited' },
  { symbol: 'COCHINSHIP.NS', name: 'Cochin Shipyard Limited' },
  { symbol: 'COCKERILL.NS', name: 'John Cockerill India Limited' },
  { symbol: 'COFFEEDAY.NS', name: 'Coffee Day Enterprises Limited' },
  { symbol: 'COFORGE.NS', name: 'Coforge Limited' },
  { symbol: 'COHANCE.NS', name: 'Cohance Lifesciences Limited' },
  { symbol: 'COLPAL.NS', name: 'Colgate Palmolive (India) Limited' },
  { symbol: 'COMFINTE.NS', name: 'Comfort Intech Limited' },
  { symbol: 'COMPINFO.NS', name: 'Compuage Infocom Limited' },
  { symbol: 'COMPUSOFT.NS', name: 'Compucom Software Limited' },
  { symbol: 'COMSYN.NS', name: 'Commercial Syn Bags Limited' },
  { symbol: 'CONCOR.NS', name: 'Container Corporation of India Limited' },
  { symbol: 'CONCORDBIO.NS', name: 'Concord Biotech Limited' },
  { symbol: 'CONFIPET.NS', name: 'Confidence Petroleum India Limited' },
  { symbol: 'CONSOFINVT.NS', name: 'Consolidated Finvest & Holdings Limited' },
  { symbol: 'CONTROLPR.NS', name: 'Control Print Limited' },
  { symbol: 'CORALFINAC.NS', name: 'Coral India Finance & Housing Limited' },
  { symbol: 'CORDSCABLE.NS', name: 'Cords Cable Industries Limited' },
  { symbol: 'COROMANDEL.NS', name: 'Coromandel International Limited' },
  { symbol: 'CORONA.NS', name: 'CORONA Remedies Limited' },
  { symbol: 'COSMOFIRST.NS', name: 'COSMO FIRST LIMITED' },
  { symbol: 'COUNCODOS.NS', name: 'Country Condo\'s Limited' },
  { symbol: 'CPCAP.NS', name: 'CP Capital Limited' },
  { symbol: 'CPEDU.NS', name: 'Career Point Edutech Limited' },
  { symbol: 'CPPLUS.NS', name: 'Aditya Infotech Limited' },
  { symbol: 'CRAFTSMAN.NS', name: 'Craftsman Automation Limited' },
  { symbol: 'CRAMC.NS', name: 'Canara Robeco Asset Management Company Limited' },
  { symbol: 'CREATIVEYE.NS', name: 'Creative Eye Limited' },
  { symbol: 'CREDITACC.NS', name: 'CREDITACCESS GRAMEEN LIMITED' },
  { symbol: 'CREST.NS', name: 'Crest Ventures Limited' },
  { symbol: 'CRISIL.NS', name: 'CRISIL Limited' },
  { symbol: 'CRIZAC.NS', name: 'Crizac Limited' },
  { symbol: 'CROMPTON.NS', name: 'Crompton Greaves Consumer Electricals Limited' },
  { symbol: 'CROWN.NS', name: 'Crown Lifters Limited' },
  { symbol: 'CSBBANK.NS', name: 'CSB Bank Limited' },
  { symbol: 'CSLFINANCE.NS', name: 'CSL Finance Limited' },
  { symbol: 'CTE.NS', name: 'Cambridge Technology Enterprises Limited' },
  { symbol: 'CUB.NS', name: 'City Union Bank Limited' },
  { symbol: 'CUBEXTUB.NS', name: 'Cubex Tubings Limited' },
  { symbol: 'CUMMINSIND.NS', name: 'Cummins India Limited' },
  { symbol: 'CUPID.NS', name: 'Cupid Limited' },
  { symbol: 'CURAA.NS', name: 'Cura Technologies Limited' },
  { symbol: 'CYBERMEDIA.NS', name: 'Cyber Media (India) Limited' },
  { symbol: 'CYBERTECH.NS', name: 'Cybertech Systems And Software Limited' },
  { symbol: 'CYIENT.NS', name: 'Cyient Limited' },
  { symbol: 'CYIENTDLM.NS', name: 'Cyient DLM Limited' },
  { symbol: 'DABUR.NS', name: 'Dabur India Limited' },
  { symbol: 'DAICHI.NS', name: 'Dai-Ichi Karkaria Limited' },
  { symbol: 'DALBHARAT.NS', name: 'Dalmia Bharat Limited' },
  { symbol: 'DALMIASUG.NS', name: 'Dalmia Bharat Sugar and Industries Limited' },
  { symbol: 'DAMCAPITAL.NS', name: 'Dam Capital Advisors Limited' },
  { symbol: 'DAMODARIND.NS', name: 'Damodar Industries Limited' },
  { symbol: 'DANGEE.NS', name: 'Dangee Dums Limited' },
  { symbol: 'DATAMATICS.NS', name: 'Datamatics Global Services Limited' },
  { symbol: 'DATAPATTNS.NS', name: 'Data Patterns (India) Limited' },
  { symbol: 'DAVANGERE.NS', name: 'Davangere Sugar Company Limited' },
  { symbol: 'DBCORP.NS', name: 'D.B.Corp Limited' },
  { symbol: 'DBEIL.NS', name: 'Deepak Builders & Engineers India Limited' },
  { symbol: 'DBL.NS', name: 'Dilip Buildcon Limited' },
  { symbol: 'DBOL.NS', name: 'Dhampur Bio Organics Limited' },
  { symbol: 'DBREALTY.NS', name: 'Valor Estate Limited' },
  { symbol: 'DBSTOCKBRO.NS', name: 'DB (International) Stock Brokers Limited' },
  { symbol: 'DCAL.NS', name: 'Dishman Carbogen Amcis Limited' },
  { symbol: 'DCBBANK.NS', name: 'DCB Bank Limited' },
  { symbol: 'DCI.NS', name: 'Dc Infotech And Communication Limited' },
  { symbol: 'DCM.NS', name: 'DCM  Limited' },
  { symbol: 'DCMFINSERV.NS', name: 'DCM Financial Services Limited' },
  { symbol: 'DCMNVL.NS', name: 'DCM Nouvelle Limited' },
  { symbol: 'DCMSHRIRAM.NS', name: 'DCM Shriram Limited' },
  { symbol: 'DCMSIL.NS', name: 'DCM Shriram International Limited' },
  { symbol: 'DCMSRIND.NS', name: 'DCM Shriram Industries Limited' },
  { symbol: 'DCW.NS', name: 'DCW Limited' },
  { symbol: 'DCXINDIA.NS', name: 'DCX Systems Limited' },
  { symbol: 'DDEVPLSTIK.NS', name: 'Ddev Plastiks Industries Limited' },
  { symbol: 'DECCANCE.NS', name: 'Deccan Cements Limited' },
  { symbol: 'DECNGOLD.NS', name: 'Deccan Gold Mines Limited' },
  { symbol: 'DEEDEV.NS', name: 'DEE Development Engineers Limited' },
  { symbol: 'DEEPAKFERT.NS', name: 'Deepak Fertilizers and Petrochemicals Corporation Limited' },
  { symbol: 'DEEPAKNTR.NS', name: 'Deepak Nitrite Limited' },
  { symbol: 'DEEPINDS.NS', name: 'Deep Industries Limited' },
  { symbol: 'DELHIVERY.NS', name: 'Delhivery Limited' },
  { symbol: 'DELPHIFX.NS', name: 'DELPHI WORLD MONEY LIMITED' },
  { symbol: 'DELTACORP.NS', name: 'Delta Corp Limited' },
  { symbol: 'DELTAMAGNT.NS', name: 'Delta Manufacturing Limited' },
  { symbol: 'DEN.NS', name: 'Den Networks Limited' },
  { symbol: 'DENORA.NS', name: 'De Nora India Limited' },
  { symbol: 'DENTA.NS', name: 'Denta Water and Infra Solutions Limited' },
  { symbol: 'DEVIT.NS', name: 'Dev Information Technology Limited' },
  { symbol: 'DEVX.NS', name: 'Dev Accelerator Limited' },
  { symbol: 'DEVYANI.NS', name: 'Devyani International Limited' },
  { symbol: 'DGCONTENT.NS', name: 'Digicontent Limited' },
  { symbol: 'DHAMPURSUG.NS', name: 'Dhampur Sugar Mills Limited' },
  { symbol: 'DHANBANK.NS', name: 'Dhanlaxmi Bank Limited' },
  { symbol: 'DHANUKA.NS', name: 'Dhanuka Agritech Limited' },
  { symbol: 'DHARMAJ.NS', name: 'Dharmaj Crop Guard Limited' },
  { symbol: 'DHRUV.NS', name: 'Dhruv Consultancy Services Limited' },
  { symbol: 'DHUNINV.NS', name: 'Dhunseri Investments Limited' },
  { symbol: 'DIACABS.NS', name: 'Diamond Power Infrastructure Limited' },
  { symbol: 'DIAMINESQ.NS', name: 'Diamines & Chemicals Limited' },
  { symbol: 'DIAMONDYD.NS', name: 'Prataap Snacks Limited' },
  { symbol: 'DICIND.NS', name: 'DIC India Limited' },
  { symbol: 'DIFFNKG.NS', name: 'Diffusion Engineers Limited' },
  { symbol: 'DIGIDRIVE.NS', name: 'Digidrive Distributors Limited' },
  { symbol: 'DIGISPICE.NS', name: 'DiGiSPICE Technologies Limited' },
  { symbol: 'DIGITIDE.NS', name: 'Digitide Solutions Limited' },
  { symbol: 'DIGJAMLMTD.NS', name: 'Digjam Limited' },
  { symbol: 'DISAQ.NS', name: 'Disa India Limited' },
  { symbol: 'DISHTV.NS', name: 'Dish TV India Limited' },
  { symbol: 'DIVGIITTS.NS', name: 'Divgi Torqtransfer Systems Limited' },
  { symbol: 'DIVISLAB.NS', name: 'Divi\'s Laboratories Limited' },
  { symbol: 'DIXON.NS', name: 'Dixon Technologies (India) Limited' },
  { symbol: 'DJML.NS', name: 'DJ Mediaprint & Logistics Limited' },
  { symbol: 'DLF.NS', name: 'DLF Limited' },
  { symbol: 'DLINKINDIA.NS', name: 'D-Link (India) Limited' },
  { symbol: 'DMART.NS', name: 'Avenue Supermarts Limited' },
  { symbol: 'DMCC.NS', name: 'DMCC SPECIALITY CHEMICALS LIMITED' },
  { symbol: 'DNAMEDIA.NS', name: 'Diligent Media Corporation Limited' },
  { symbol: 'DODLA.NS', name: 'Dodla Dairy Limited' },
  { symbol: 'DOLATALGO.NS', name: 'Dolat Algotech Limited' },
  { symbol: 'DOLLAR.NS', name: 'Dollar Industries Limited' },
  { symbol: 'DOLPHIN.NS', name: 'Dolphin Offshore Enterprises (India) Limited' },
  { symbol: 'DOMS.NS', name: 'DOMS Industries Limited' },
  { symbol: 'DONEAR.NS', name: 'Donear Industries Limited' },
  { symbol: 'DPABHUSHAN.NS', name: 'D. P. Abhushan Limited' },
  { symbol: 'DPSCLTD.NS', name: 'DPSC Limited' },
  { symbol: 'DPWIRES.NS', name: 'D P Wires Limited' },
  { symbol: 'DRAGARWQ.NS', name: 'Dr Agarwals Eye Hospital Limited' },
  { symbol: 'DRCSYSTEMS.NS', name: 'DRC Systems India Limited' },
  { symbol: 'DREAMFOLKS.NS', name: 'Dreamfolks Services Limited' },
  { symbol: 'DREDGECORP.NS', name: 'Dredging Corporation of India Limited' },
  { symbol: 'DRREDDY.NS', name: 'Dr. Reddy\'s Laboratories Limited' },
  { symbol: 'DSFCL.NS', name: 'DCM Shriram Fine Chemicals Limited' },
  { symbol: 'DSSL.NS', name: 'Dynacons Systems & Solutions Limited' },
  { symbol: 'DTIL.NS', name: 'Dhunseri Tea & Industries Limited' },
  { symbol: 'DUCON.NS', name: 'Ducon Infratechnologies Limited' },
  { symbol: 'DVL.NS', name: 'Dhunseri Ventures Limited' },
  { symbol: 'DWARKESH.NS', name: 'Dwarikesh Sugar Industries Limited' },
  { symbol: 'DYCL.NS', name: 'Dynamic Cables Limited' },
  { symbol: 'DYNAMATECH.NS', name: 'Dynamatic Technologies Limited' },
  { symbol: 'DYNPRO.NS', name: 'Dynemic Products Limited' },
  { symbol: 'E2E.NS', name: 'E2E Networks Limited' },
  { symbol: 'EASEMYTRIP.NS', name: 'Easy Trip Planners Limited' },
  { symbol: 'EASTSILK.NS', name: 'Eastern Silk Industries Limited' },
  { symbol: 'EBGNG.NS', name: 'GNG Electronics Limited' },
  { symbol: 'ECLERX.NS', name: 'eClerx Services Limited' },
  { symbol: 'ECOSMOBLTY.NS', name: 'Ecos (India) Mobility & Hospitality Limited' },
  { symbol: 'EDELWEISS.NS', name: 'Edelweiss Financial Services Limited' },
  { symbol: 'EFCIL.NS', name: 'EFC (I) Limited' },
  { symbol: 'EICHERMOT.NS', name: 'Eicher Motors Limited' },
  { symbol: 'EIDPARRY.NS', name: 'EID Parry India Limited' },
  { symbol: 'EIEL.NS', name: 'Enviro Infra Engineers Limited' },
  { symbol: 'EIFFL.NS', name: 'Euro India Fresh Foods Limited' },
  { symbol: 'EIHAHOTELS.NS', name: 'EIH Associated Hotels Limited' },
  { symbol: 'EIHOTEL.NS', name: 'EIH Limited' },
  { symbol: 'EIMCOELECO.NS', name: 'Eimco Elecon (India) Limited' },
  { symbol: 'EKC.NS', name: 'Everest Kanto Cylinder Limited' },
  { symbol: 'ELANTAS.NS', name: 'Elantas Beck India Limited' },
  { symbol: 'ELCIDIN.NS', name: 'EL CID Investments Limited' },
  { symbol: 'ELDEHSG.NS', name: 'Eldeco Housing And Industries Limited' },
  { symbol: 'ELECON.NS', name: 'Elecon Engineering Company Limited' },
  { symbol: 'ELECTCAST.NS', name: 'Electrosteel Castings Limited' },
  { symbol: 'ELECTHERM.NS', name: 'Electrotherm (India) Limited' },
  { symbol: 'ELGIEQUIP.NS', name: 'Elgi Equipments Limited' },
  { symbol: 'ELGIRUBCO.NS', name: 'Elgi Rubber Company Limited' },
  { symbol: 'ELIN.NS', name: 'Elin Electronics Limited' },
  { symbol: 'ELITECON.NS', name: 'Elitecon International Limited' },
  { symbol: 'ELLEN.NS', name: 'Ellenbarrie Industrial Gases Limited' },
  { symbol: 'ELPROINTL.NS', name: 'Elpro International Limited' },
  { symbol: 'EMAMILTD.NS', name: 'Emami Limited' },
  { symbol: 'EMAMIPAP.NS', name: 'Emami Paper Mills Limited' },
  { symbol: 'EMAMIREAL.NS', name: 'Emami Realty Limited' },
  { symbol: 'EMBDL.NS', name: 'Embassy Developments Limited' },
  { symbol: 'EMCURE.NS', name: 'Emcure Pharmaceuticals Limited' },
  { symbol: 'EMIL.NS', name: 'Electronics Mart India Limited' },
  { symbol: 'EMKAY.NS', name: 'Emkay Global Financial Services Limited' },
  { symbol: 'EMMBI.NS', name: 'Emmbi Industries Limited' },
  { symbol: 'EMMVEE.NS', name: 'Emmvee Photovoltaic Power Limited' },
  { symbol: 'EMPOWER.NS', name: 'Empower India Limited' },
  { symbol: 'EMSLIMITED.NS', name: 'EMS Limited' },
  { symbol: 'EMUDHRA.NS', name: 'eMudhra Limited' },
  { symbol: 'ENDURANCE.NS', name: 'Endurance Technologies Limited' },
  { symbol: 'ENERGYDEV.NS', name: 'Energy Development Company Limited' },
  { symbol: 'ENGINERSIN.NS', name: 'Engineers India Limited' },
  { symbol: 'ENIL.NS', name: 'Entertainment Network (India) Limited' },
  { symbol: 'ENRIN.NS', name: 'Siemens Energy India Limited' },
  { symbol: 'ENTERO.NS', name: 'Entero Healthcare Solutions Limited' },
  { symbol: 'EPACK.NS', name: 'EPACK Durable Limited' },
  { symbol: 'EPACKPEB.NS', name: 'EPack Prefab Technologies Limited' },
  { symbol: 'EPIGRAL.NS', name: 'Epigral Limited' },
  { symbol: 'EPL.NS', name: 'EPL Limited' },
  { symbol: 'EQUIPPP.NS', name: 'Equippp Social Impact Technologies Limited' },
  { symbol: 'EQUITASBNK.NS', name: 'Equitas Small Finance Bank Limited' },
  { symbol: 'ERIS.NS', name: 'Eris Lifesciences Limited' },
  { symbol: 'ESABINDIA.NS', name: 'Esab India Limited' },
  { symbol: 'ESAFSFB.NS', name: 'ESAF Small Finance Bank Limited' },
  { symbol: 'ESCORTS.NS', name: 'Escorts Kubota Limited' },
  { symbol: 'ESSARSHPNG.NS', name: 'Essar Shipping Limited' },
  { symbol: 'ESSEN-RE3.NS', name: 'Integra Essentia Limited-RE' },
  { symbol: 'ESSENTIA.NS', name: 'Integra Essentia Limited' },
  { symbol: 'ESTER.NS', name: 'Ester Industries Limited' },
  { symbol: 'ETERNAL.NS', name: 'ETERNAL LIMITED' },
  { symbol: 'ETHOSLTD.NS', name: 'Ethos Limited' },
  { symbol: 'EUREKAFORB.NS', name: 'Eureka Forbes Limited' },
  { symbol: 'EUROBOND.NS', name: 'Euro Panel Products Limited' },
  { symbol: 'EUROPRATIK.NS', name: 'Euro Pratik Sales Limited' },
  { symbol: 'EUROTEXIND.NS', name: 'Eurotex Industries and Exports Limited' },
  { symbol: 'EVEREADY.NS', name: 'Eveready Industries India Limited' },
  { symbol: 'EVERESTIND.NS', name: 'Everest Industries Limited' },
  { symbol: 'EXCELINDUS.NS', name: 'Excel Industries Limited' },
  { symbol: 'EXCELSOFT.NS', name: 'Excelsoft Technologies Limited' },
  { symbol: 'EXICOM.NS', name: 'Exicom Tele-Systems Limited' },
  { symbol: 'EXIDEIND.NS', name: 'Exide Industries Limited' },
  { symbol: 'EXPLEOSOL.NS', name: 'Expleo Solutions Limited' },
  { symbol: 'EXXARO.NS', name: 'Exxaro Tiles Limited' },
  { symbol: 'FABTECH.NS', name: 'Fabtech Technologies Limited' },
  { symbol: 'FACT.NS', name: 'Fertilizers and Chemicals Travancore Limited' },
  { symbol: 'FAIRCHEMOR.NS', name: 'Fairchem Organics Limited' },
  { symbol: 'FAZE3Q.NS', name: 'Faze Three Limited' },
  { symbol: 'FCL.NS', name: 'Fineotex Chemical Limited' },
  { symbol: 'FCSSOFT.NS', name: 'FCS Software Solutions Limited' },
  { symbol: 'FDC.NS', name: 'FDC Limited' },
  { symbol: 'FEDDERSHOL.NS', name: 'Fedders Holding Limited' },
  { symbol: 'FEDERALBNK.NS', name: 'The Federal Bank  Limited' },
  { symbol: 'FEDFINA.NS', name: 'Fedbank Financial Services Limited' },
  { symbol: 'FEL.NS', name: 'Future Enterprises Limited' },
  { symbol: 'FELDVR.NS', name: 'Future Enterprises Limited' },
  { symbol: 'FERMENTA.NS', name: 'Fermenta Biotech Limited' },
  { symbol: 'FIBERWEB.NS', name: 'Fiberweb (India) Limited' },
  { symbol: 'FIEMIND.NS', name: 'Fiem Industries Limited' },
  { symbol: 'FILATEX.NS', name: 'Filatex India Limited' },
  { symbol: 'FILATFASH.NS', name: 'Filatex Fashions Limited' },
  { symbol: 'FINCABLES.NS', name: 'Finolex Cables Limited' },
  { symbol: 'FINEORG.NS', name: 'Fine Organic Industries Limited' },
  { symbol: 'FINKURVE.NS', name: 'Finkurve Financial Services Limited' },
  { symbol: 'FINOPB.NS', name: 'Fino Payments Bank Limited' },
  { symbol: 'FINPIPE.NS', name: 'Finolex Industries Limited' },
  { symbol: 'FIRSTCRY.NS', name: 'Brainbees Solutions Limited' },
  { symbol: 'FISCHER.NS', name: 'Fischer Medical Ventures Limited' },
  { symbol: 'FIVESTAR.NS', name: 'Five-Star Business Finance Limited' },
  { symbol: 'FLAIR.NS', name: 'Flair Writing Industries Limited' },
  { symbol: 'FLEXITUFF.NS', name: 'Flexituff Ventures International Limited' },
  { symbol: 'FLFL.NS', name: 'Future Lifestyle Fashions Limited' },
  { symbol: 'FLUOROCHEM.NS', name: 'Gujarat Fluorochemicals Limited' },
  { symbol: 'FMGOETZE.NS', name: 'Federal-Mogul Goetze (India) Limited.' },
  { symbol: 'FMNL.NS', name: 'Future Market Networks Limited' },
  { symbol: 'FOCUS.NS', name: 'Focus Lighting and Fixtures Limited' },
  { symbol: 'FOODSIN.NS', name: 'Foods & Inns Limited' },
  { symbol: 'FORCEMOT.NS', name: 'FORCE MOTORS LTD' },
  { symbol: 'FORTIS.NS', name: 'Fortis Healthcare Limited' },
  { symbol: 'FOSECOIND.NS', name: 'Foseco India Limited' },
  { symbol: 'FRACTAL.NS', name: 'Fractal Analytics Limited' },
  { symbol: 'FRONTSP.NS', name: 'Frontier Springs Limited' },
  { symbol: 'FSL.NS', name: 'Firstsource Solutions Limited' },
  { symbol: 'FUSION.NS', name: 'Fusion Finance Limited' },
  { symbol: 'GABRIEL.NS', name: 'Gabriel India Limited' },
  { symbol: 'GAEL.NS', name: 'Gujarat Ambuja Exports Limited' },
  { symbol: 'GAIL.NS', name: 'GAIL (India) Limited' },
  { symbol: 'GALAPREC.NS', name: 'Gala Precision Engineering Limited' },
  { symbol: 'GALAXYSURF.NS', name: 'Galaxy Surfactants Limited' },
  { symbol: 'GALLANTT.NS', name: 'Gallantt Ispat Limited' },
  { symbol: 'GANDHAR.NS', name: 'Gandhar Oil Refinery (India) Limited' },
  { symbol: 'GANDHITUBE.NS', name: 'Gandhi Special Tubes Limited' },
  { symbol: 'GANECOS.NS', name: 'Ganesha Ecosphere Limited' },
  { symbol: 'GANESHBE.NS', name: 'Ganesh Benzoplast Limited' },
  { symbol: 'GANESHCP.NS', name: 'Ganesh Consumer Products Limited' },
  { symbol: 'GANESHHOU.NS', name: 'GANESH HOUSING LIMITED' },
  { symbol: 'GANGAFORGE.NS', name: 'Ganga Forging Limited' },
  { symbol: 'GANGESSECU.NS', name: 'Ganges Securities Limited' },
  { symbol: 'GARFIBRES.NS', name: 'Garware Technical Fibres Limited' },
  { symbol: 'GARUDA.NS', name: 'Garuda Construction and Engineering Limited' },
  { symbol: 'GATECH.NS', name: 'GACM Technologies Limited' },
  { symbol: 'GATECHDVR.NS', name: 'GACM Technologies Limited' },
  { symbol: 'GATEWAY.NS', name: 'Gateway Distriparks Limited' },
  { symbol: 'GAUDIUMIVF.NS', name: 'Gaudium IVF and Women Health Limited' },
  { symbol: 'GAYAHWS.NS', name: 'Gayatri Highways Limited' },
  { symbol: 'GAYAPROJ.NS', name: 'Gayatri Projects Limited' },
  { symbol: 'GCSL.NS', name: 'Gretex Corporate Services Limited' },
  { symbol: 'GEECEE.NS', name: 'GeeCee Ventures Limited' },
  { symbol: 'GEEKAYWIRE.NS', name: 'Geekay Wires Limited' },
  { symbol: 'GEMAROMA.NS', name: 'Gem Aromatics Limited' },
  { symbol: 'GENCON.NS', name: 'Generic Engineering Construction and Projects Limited' },
  { symbol: 'GENESYS.NS', name: 'Genesys International Corporation Limited' },
  { symbol: 'GENUSPAPER.NS', name: 'Genus Paper & Boards Limited' },
  { symbol: 'GENUSPOWER.NS', name: 'Genus Power Infrastructures Limited' },
  { symbol: 'GEOJITFSL.NS', name: 'Geojit Financial Services Limited' },
  { symbol: 'GESHIP.NS', name: 'The Great Eastern Shipping Company Limited' },
  { symbol: 'GFLLIMITED.NS', name: 'GFL Limited' },
  { symbol: 'GHCL.NS', name: 'GHCL Limited' },
  { symbol: 'GHCLTEXTIL.NS', name: 'GHCL Textiles Limited' },
  { symbol: 'GICHSGFIN.NS', name: 'GIC Housing Finance Limited' },
  { symbol: 'GICL.NS', name: 'Globe International Carriers Limited' },
  { symbol: 'GICRE.NS', name: 'General Insurance Corporation of India' },
  { symbol: 'GILLANDERS.NS', name: 'Gillanders Arbuthnot & Company Limited' },
  { symbol: 'GILLETTE.NS', name: 'Gillette India Limited' },
  { symbol: 'GINNIFILA.NS', name: 'Ginni Filaments Limited' },
  { symbol: 'GIPCL.NS', name: 'Gujarat Industries Power Company Limited' },
  { symbol: 'GKENERGY.NS', name: 'GK Energy Limited' },
  { symbol: 'GKSL.NS', name: 'Gujarat Kidney And Super Speciality Limited' },
  { symbol: 'GKWLIMITED.NS', name: 'GKW Limited' },
  { symbol: 'GLAND.NS', name: 'Gland Pharma Limited' },
  { symbol: 'GLAXO.NS', name: 'GlaxoSmithKline Pharmaceuticals Limited' },
  { symbol: 'GLENMARK.NS', name: 'Glenmark Pharmaceuticals Limited' },
  { symbol: 'GLFL.NS', name: 'Gujarat Lease Financing Limited' },
  { symbol: 'GLOBAL.NS', name: 'Global Education Limited' },
  { symbol: 'GLOBALE.NS', name: 'Globale Tessile Limited' },
  { symbol: 'GLOBALVECT.NS', name: 'Global Vectra Helicorp Limited' },
  { symbol: 'GLOBE.NS', name: 'GLOBE ENTERPRISES (INDIA) LIMITED' },
  { symbol: 'GLOBECIVIL.NS', name: 'Globe Civil Projects Limited' },
  { symbol: 'GLOBUSSPR.NS', name: 'Globus Spirits Limited' },
  { symbol: 'GLOSTERLTD.NS', name: 'Gloster Limited' },
  { symbol: 'GLOTTIS.NS', name: 'Glottis Limited' },
  { symbol: 'GMBREW.NS', name: 'GM Breweries Limited' },
  { symbol: 'GMDCLTD.NS', name: 'Gujarat Mineral Development Corporation Limited' },
  { symbol: 'GMMPFAUDLR.NS', name: 'GMM Pfaudler Limited' },
  { symbol: 'GMRAIRPORT.NS', name: 'GMR AIRPORTS LIMITED' },
  { symbol: 'GMRP&UI.NS', name: 'GMR Power and Urban Infra Limited' },
  { symbol: 'GNA.NS', name: 'GNA Axles Limited' },
  { symbol: 'GNFC.NS', name: 'Gujarat Narmada Valley Fertilizers and Chemicals Limited' },
  { symbol: 'GNRL.NS', name: 'Gujarat Natural Resources Limited' },
  { symbol: 'GOACARBON.NS', name: 'Goa Carbon Limited' },
  { symbol: 'GOCLCORP.NS', name: 'GOCL Corporation Limited' },
  { symbol: 'GOCOLORS.NS', name: 'Go Fashion (India) Limited' },
  { symbol: 'GODAVARIB.NS', name: 'Godavari Biorefineries Limited' },
  { symbol: 'GODFRYPHLP.NS', name: 'Godfrey Phillips India Limited' },
  { symbol: 'GODIGIT.NS', name: 'Go Digit General Insurance Limited' },
  { symbol: 'GODREJAGRO.NS', name: 'Godrej Agrovet Limited' },
  { symbol: 'GODREJCP.NS', name: 'Godrej Consumer Products Limited' },
  { symbol: 'GODREJIND.NS', name: 'Godrej Industries Limited' },
  { symbol: 'GODREJPROP.NS', name: 'Godrej Properties Limited' },
  { symbol: 'GOKEX.NS', name: 'Gokaldas Exports Limited' },
  { symbol: 'GOKUL.NS', name: 'Gokul Refoils and Solvent Limited' },
  { symbol: 'GOKULAGRO.NS', name: 'Gokul Agro Resources Limited' },
  { symbol: 'GOLDENTOBC.NS', name: 'Golden Tobacco Limited' },
  { symbol: 'GOLDIAM.NS', name: 'Goldiam International Limited' },
  { symbol: 'GOLDTECH.NS', name: 'AION-TECH SOLUTIONS LIMITED' },
  { symbol: 'GOODLUCK.NS', name: 'Goodluck India Limited' },
  { symbol: 'GOODYEAR.NS', name: 'Goodyear India Limited' },
  { symbol: 'GOPAL.NS', name: 'Gopal Snacks Limited' },
  { symbol: 'GOYALALUM.NS', name: 'Goyal Aluminiums Limited' },
  { symbol: 'GPIL.NS', name: 'Godawari Power And Ispat limited' },
  { symbol: 'GPPL.NS', name: 'Gujarat Pipavav Port Limited' },
  { symbol: 'GPTHEALTH.NS', name: 'GPT Healthcare Limited' },
  { symbol: 'GPTINFRA.NS', name: 'GPT Infraprojects Limited' },
  { symbol: 'GRADIENTE.NS', name: 'Gradiente Infotainment Limited' },
  { symbol: 'GRANDOAK.NS', name: 'Grand Oak Canyons Distillery Limited' },
  { symbol: 'GRANULES.NS', name: 'Granules India Limited' },
  { symbol: 'GRAPHITE.NS', name: 'Graphite India Limited' },
  { symbol: 'GRASIM.NS', name: 'Grasim Industries Limited' },
  { symbol: 'GRAUWEIL.NS', name: 'Grauer & Weil India Limited' },
  { symbol: 'GRAVISSHO.NS', name: 'Graviss Hospitality Limited' },
  { symbol: 'GRAVITA.NS', name: 'Gravita India Limited' },
  { symbol: 'GREAVESCOT.NS', name: 'Greaves Cotton Limited' },
  { symbol: 'GREENLAM.NS', name: 'Greenlam Industries Limited' },
  { symbol: 'GREENPANEL.NS', name: 'Greenpanel Industries Limited' },
  { symbol: 'GREENPLY.NS', name: 'Greenply Industries Limited' },
  { symbol: 'GREENPOWER.NS', name: 'Orient Green Power Company Limited' },
  { symbol: 'GRINDWELL.NS', name: 'Grindwell Norton Limited' },
  { symbol: 'GRINFRA.NS', name: 'G R Infraprojects Limited' },
  { symbol: 'GRMOVER.NS', name: 'GRM Overseas Limited' },
  { symbol: 'GROBTEA.NS', name: 'The Grob Tea Company Limited' },
  { symbol: 'GROWW.NS', name: 'Billionbrains Garage Ventures Limited' },
  { symbol: 'GRPLTD.NS', name: 'GRP Limited' },
  { symbol: 'GRSE.NS', name: 'Garden Reach Shipbuilders & Engineers Limited' },
  { symbol: 'GRWRHITECH.NS', name: 'Garware Hi-Tech Films Limited' },
  { symbol: 'GSFC.NS', name: 'Gujarat State Fertilizers & Chemicals Limited' },
  { symbol: 'GSLSU.NS', name: 'Global Surfaces Limited' },
  { symbol: 'GSPCROP.NS', name: 'GSP Crop Science Limited' },
  { symbol: 'GSS.NS', name: 'GSS Infotech Limited' },
  { symbol: 'GTECJAINX.NS', name: 'G-TEC JAINX EDUCATION LIMITED' },
  { symbol: 'GTL.NS', name: 'GTL Limited' },
  { symbol: 'GTLINFRA.NS', name: 'GTL Infrastructure Limited' },
  { symbol: 'GTPL.NS', name: 'GTPL Hathway Limited' },
  { symbol: 'GUFICBIO.NS', name: 'Gufic Biosciences Limited' },
  { symbol: 'GUJALKALI.NS', name: 'Gujarat Alkalies and Chemicals Limited' },
  { symbol: 'GUJAPOLLO.NS', name: 'Gujarat Apollo Industries Limited' },
  { symbol: 'GUJGASLTD.NS', name: 'Gujarat Gas Limited' },
  { symbol: 'GUJRAFFIA.NS', name: 'Gujarat Raffia Industries Limited' },
  { symbol: 'GUJTHEM.NS', name: 'Gujarat Themis Biosyn Limited' },
  { symbol: 'GULFOILLUB.NS', name: 'Gulf Oil Lubricants India Limited' },
  { symbol: 'GULFPETRO.NS', name: 'GP Petroleums Limited' },
  { symbol: 'GULPOLY.NS', name: 'Gulshan Polyols Limited' },
  { symbol: 'GVKPIL.NS', name: 'GVK Power & Infrastructure Limited' },
  { symbol: 'GVPIL.NS', name: 'GE Power India Limited' },
  { symbol: 'GVPTECH.NS', name: 'GVP Infotech Limited' },
  { symbol: 'GVT&D.NS', name: 'GE Vernova T&D India Limited' },
  { symbol: 'HAL.NS', name: 'Hindustan Aeronautics Limited' },
  { symbol: 'HALDER.NS', name: 'Halder Venture Limited' },
  { symbol: 'HALDYNGL.NS', name: 'Haldyn Glass Limited' },
  { symbol: 'HALEOSLABS.NS', name: 'HALEOS LABS LIMITED' },
  { symbol: 'HAPPSTMNDS.NS', name: 'Happiest Minds Technologies Limited' },
  { symbol: 'HAPPYFORGE.NS', name: 'Happy Forgings Limited' },
  { symbol: 'HARDWYN.NS', name: 'Hardwyn India Limited' },
  { symbol: 'HARIOMPIPE.NS', name: 'Hariom Pipe Industries Limited' },
  { symbol: 'HARRMALAYA.NS', name: 'Harrisons  Malayalam Limited' },
  { symbol: 'HARSHA.NS', name: 'Harsha Engineers International Limited' },
  { symbol: 'HATHWAY.NS', name: 'Hathway Cable & Datacom Limited' },
  { symbol: 'HATSUN.NS', name: 'Hatsun Agro Product Limited' },
  { symbol: 'HAVELLS.NS', name: 'Havells India Limited' },
  { symbol: 'HAVISHA.NS', name: 'Sri Havisha Hospitality and Infrastructure Limited' },
  { symbol: 'HAWKINCOOK.NS', name: 'Hawkins Cookers Limited' },
  { symbol: 'HBESD.NS', name: 'HB Estate Developers Limited' },
  { symbol: 'HBLENGINE.NS', name: 'HBL Engineering Limited' },
  { symbol: 'HBSL.NS', name: 'HB Stockholdings Limited' },
  { symbol: 'HCC.NS', name: 'Hindustan Construction Company Limited' },
  { symbol: 'HCG.NS', name: 'Healthcare Global Enterprises Limited' },
  { symbol: 'HCL-INSYS.NS', name: 'HCL Infosystems Limited' },
  { symbol: 'HCLTECH.NS', name: 'HCL Technologies Limited' },
  { symbol: 'HDBFS.NS', name: 'HDB Financial Services Limited' },
  { symbol: 'HDFCAMC.NS', name: 'HDFC Asset Management Company Limited' },
  { symbol: 'HDFCBANK.NS', name: 'HDFC Bank Limited' },
  { symbol: 'HDFCLIFE.NS', name: 'HDFC Life Insurance Company Limited' },
  { symbol: 'HDIL.NS', name: 'Housing Development and Infrastructure Limited' },
  { symbol: 'HEADSUP.NS', name: 'Heads UP Ventures Limited' },
  { symbol: 'HEALTHX.NS', name: 'Health X Platform Limited' },
  { symbol: 'HECPROJECT.NS', name: 'HEC Infra Projects Limited' },
  { symbol: 'HEG.NS', name: 'HEG Limited' },
  { symbol: 'HEIDELBERG.NS', name: 'HeidelbergCement India Limited' },
  { symbol: 'HEMIPROP.NS', name: 'Hemisphere Properties India Limited' },
  { symbol: 'HERANBA.NS', name: 'Heranba Industries Limited' },
  { symbol: 'HERITGFOOD.NS', name: 'Heritage Foods Limited' },
  { symbol: 'HEROMOTOCO.NS', name: 'Hero MotoCorp Limited' },
  { symbol: 'HESTERBIO.NS', name: 'Hester Biosciences Limited' },
  { symbol: 'HEXATRADEX.NS', name: 'Hexa Tradex Limited' },
  { symbol: 'HEXT.NS', name: 'Hexaware Technologies Limited' },
  { symbol: 'HFCL.NS', name: 'HFCL Limited' },
  { symbol: 'HGINFRA.NS', name: 'H.G. Infra Engineering Limited' },
  { symbol: 'HGM.NS', name: 'HandsOn Global Management (HGM) Limited' },
  { symbol: 'HGS.NS', name: 'Hinduja Global Solutions Limited' },
  { symbol: 'HIKAL.NS', name: 'Hikal Limited' },
  { symbol: 'HILINFRA.NS', name: 'Highway Infrastructure Limited' },
  { symbol: 'HILTON.NS', name: 'Hilton Metal Forging Limited' },
  { symbol: 'HIMATSEIDE.NS', name: 'Himatsingka Seide Limited' },
  { symbol: 'HINDALCO.NS', name: 'Hindalco Industries Limited' },
  { symbol: 'HINDCOMPOS.NS', name: 'Hindustan Composites Limited' },
  { symbol: 'HINDCON.NS', name: 'Hindcon Chemicals Limited' },
  { symbol: 'HINDCOPPER.NS', name: 'Hindustan Copper Limited' },
  { symbol: 'HINDOILEXP.NS', name: 'Hindustan Oil Exploration Company Limited' },
  { symbol: 'HINDPETRO.NS', name: 'Hindustan Petroleum Corporation Limited' },
  { symbol: 'HINDUNILVR.NS', name: 'Hindustan Unilever Limited' },
  { symbol: 'HINDWAREAP.NS', name: 'Hindware Home Innovation Limited' },
  { symbol: 'HINDZINC.NS', name: 'Hindustan Zinc Limited' },
  { symbol: 'HIRECT.NS', name: 'Hind Rectifiers Limited' },
  { symbol: 'HISARMETAL.NS', name: 'Hisar Metal Industries Limited' },
  { symbol: 'HITECH.NS', name: 'Hi-Tech Pipes Limited' },
  { symbol: 'HITECHCORP.NS', name: 'Hitech Corporation Limited' },
  { symbol: 'HITECHGEAR.NS', name: 'The Hi-Tech Gears Limited' },
  { symbol: 'HLEGLAS.NS', name: 'HLE Glascoat Limited' },
  { symbol: 'HLVLTD.NS', name: 'HLV LIMITED' },
  { symbol: 'HMAAGRO.NS', name: 'HMA Agro Industries Limited' },
  { symbol: 'HMT.NS', name: 'HMT Limited' },
  { symbol: 'HMVL.NS', name: 'Hindustan Media Ventures Limited' },
  { symbol: 'HNDFDS.NS', name: 'Hindustan Foods Limited' },
  { symbol: 'HOMEFIRST.NS', name: 'Home First Finance Company India Limited' },
  { symbol: 'HONASA.NS', name: 'Honasa Consumer Limited' },
  { symbol: 'HONAUT.NS', name: 'Honeywell Automation India Limited' },
  { symbol: 'HONDAPOWER.NS', name: 'Honda India Power Products Limited' },
  { symbol: 'HPAL.NS', name: 'HP Adhesives Limited' },
  { symbol: 'HPIL.NS', name: 'Hindprakash Industries Limited' },
  { symbol: 'HPL.NS', name: 'HPL Electric & Power Limited' },
  { symbol: 'HSCL.NS', name: 'Himadri Speciality Chemical Limited' },
  { symbol: 'HTMEDIA.NS', name: 'HT Media Limited' },
  { symbol: 'HUBTOWN.NS', name: 'Hubtown Limited' },
  { symbol: 'HUDCO.NS', name: 'Housing & Urban Development Corporation Limited' },
  { symbol: 'HUHTAMAKI.NS', name: 'Huhtamaki India Limited' },
  { symbol: 'HYBRIDFIN.NS', name: 'Hybrid Financial Services Limited' },
  { symbol: 'HYUNDAI.NS', name: 'Hyundai Motor India Limited' },
  { symbol: 'IBULLSLTD.NS', name: 'Indiabulls Limited' },
  { symbol: 'ICDSLTD.NS', name: 'ICDS Limited' },
  { symbol: 'ICEMAKE.NS', name: 'Ice Make Refrigeration Limited' },
  { symbol: 'ICICIAMC.NS', name: 'ICICI Prudential Asset Management Company Limited' },
  { symbol: 'ICICIBANK.NS', name: 'ICICI Bank Limited' },
  { symbol: 'ICICIGI.NS', name: 'ICICI Lombard General Insurance Company Limited' },
  { symbol: 'ICICIPRULI.NS', name: 'ICICI Prudential Life Insurance Company Limited' },
  { symbol: 'ICIL.NS', name: 'Indo Count Industries Limited' },
  { symbol: 'ICRA.NS', name: 'ICRA Limited' },
  { symbol: 'IDBI.NS', name: 'IDBI Bank Limited' },
  { symbol: 'IDEA.NS', name: 'Vodafone Idea Limited' },
  { symbol: 'IDEAFORGE.NS', name: 'Ideaforge Technology Limited' },
  { symbol: 'IDFCFIRSTB.NS', name: 'IDFC First Bank Limited' },
  { symbol: 'IEX.NS', name: 'Indian Energy Exchange Limited' },
  { symbol: 'IFBAGRO.NS', name: 'IFB Agro Industries Limited' },
  { symbol: 'IFBIND.NS', name: 'IFB Industries Limited' },
  { symbol: 'IFCI.NS', name: 'IFCI Limited' },
  { symbol: 'IFGLEXPOR.NS', name: 'IFGL Refractories Limited' },
  { symbol: 'IGARASHI.NS', name: 'Igarashi Motors India Limited' },
  { symbol: 'IGCL.NS', name: 'Indogulf Cropsciences Limited' },
  { symbol: 'IGIL.NS', name: 'International Gemological Institute Limited' },
  { symbol: 'IGL.NS', name: 'Indraprastha Gas Limited' },
  { symbol: 'IGPL.NS', name: 'IG Petrochemicals Limited' },
  { symbol: 'IIFL.NS', name: 'IIFL Finance Limited' },
  { symbol: 'IIFLCAPS.NS', name: 'IIFL Capital Services Limited' },
  { symbol: 'IITL.NS', name: 'Industrial Investment Trust Limited' },
  { symbol: 'IKIO.NS', name: 'IKIO Technologies Limited' },
  { symbol: 'IKS.NS', name: 'Inventurus Knowledge Solutions Limited' },
  { symbol: 'IL&FSENGG.NS', name: 'IL&FS Engineering and Construction Company Limited' },
  { symbol: 'IL&FSTRANS.NS', name: 'IL&FS Transportation Networks Limited' },
  { symbol: 'IMAGICAA.NS', name: 'Imagicaaworld Entertainment Limited' },
  { symbol: 'IMFA.NS', name: 'Indian Metals & Ferro Alloys Limited' },
  { symbol: 'IMPAL.NS', name: 'India Motor Parts and Accessories Limited' },
  { symbol: 'IMPEXFERRO.NS', name: 'Impex Ferro Tech Limited' },
  { symbol: 'INA.NS', name: 'Insolation Energy Limited' },
  { symbol: 'INCREDIBLE.NS', name: 'INCREDIBLE INDUSTRIES LIMITED' },
  { symbol: 'INDBANK.NS', name: 'Indbank Merchant Banking Services Limited' },
  { symbol: 'INDGN.NS', name: 'Indegene Limited' },
  { symbol: 'INDHOTEL.NS', name: 'The Indian Hotels Company Limited' },
  { symbol: 'INDIACEM.NS', name: 'The India Cements Limited' },
  { symbol: 'INDIAGLYCO.NS', name: 'India Glycols Limited' },
  { symbol: 'INDIAMART.NS', name: 'Indiamart Intermesh Limited' },
  { symbol: 'INDIANB.NS', name: 'Indian Bank' },
  { symbol: 'INDIANCARD.NS', name: 'Indian Card Clothing Company Limited' },
  { symbol: 'INDIANHUME.NS', name: 'Indian Hume Pipe Company Limited' },
  { symbol: 'INDIASHLTR.NS', name: 'India Shelter Finance Corporation Limited' },
  { symbol: 'INDIGO.NS', name: 'InterGlobe Aviation Limited' },
  { symbol: 'INDIGOPNTS.NS', name: 'Indigo Paints Limited' },
  { symbol: 'INDIQUBE.NS', name: 'Indiqube Spaces Limited' },
  { symbol: 'INDNIPPON.NS', name: 'India Nippon Electricals Limited' },
  { symbol: 'INDOAMIN.NS', name: 'Indo Amines Limited' },
  { symbol: 'INDOBORAX.NS', name: 'Indo Borax & Chemicals Limited' },
  { symbol: 'INDOCO.NS', name: 'Indoco Remedies Limited' },
  { symbol: 'INDOFARM.NS', name: 'Indo Farm Equipment Limited' },
  { symbol: 'INDORAMA.NS', name: 'Indo Rama Synthetics (India) Limited' },
  { symbol: 'INDOSTAR.NS', name: 'IndoStar Capital Finance Limited' },
  { symbol: 'INDOTECH.NS', name: 'Indo Tech Transformers Limited' },
  { symbol: 'INDOTHAI.NS', name: 'Indo Thai Securities Limited' },
  { symbol: 'INDOUS.NS', name: 'Indo Us Biotech Limited' },
  { symbol: 'INDOWIND.NS', name: 'Indowind Energy Limited' },
  { symbol: 'INDPRUD.NS', name: 'Industrial & Prudential Investment Company Limited' },
  { symbol: 'INDRAMEDCO.NS', name: 'Indraprastha Medical Corporation Limited' },
  { symbol: 'INDSWFTLAB.NS', name: 'Ind-Swift Laboratories Limited' },
  { symbol: 'INDTERRAIN.NS', name: 'Indian Terrain Fashions Limited' },
  { symbol: 'INDUSINDBK.NS', name: 'IndusInd Bank Limited' },
  { symbol: 'INDUSTOWER.NS', name: 'Indus Towers Limited' },
  { symbol: 'INFOBEAN.NS', name: 'InfoBeans Technologies Limited' },
  { symbol: 'INFOMEDIA.NS', name: 'Infomedia Press Limited' },
  { symbol: 'INFY.NS', name: 'Infosys Limited' },
  { symbol: 'INGERRAND.NS', name: 'Ingersoll Rand (India) Limited' },
  { symbol: 'INNOVACAP.NS', name: 'Innova Captab Limited' },
  { symbol: 'INNOVANA.NS', name: 'Innovana Thinklabs Limited' },
  { symbol: 'INNOVISION.NS', name: 'Innovision Limited' },
  { symbol: 'INOXGREEN.NS', name: 'Inox Green Energy Services Limited' },
  { symbol: 'INOXINDIA.NS', name: 'INOX India Limited' },
  { symbol: 'INOXWIND.NS', name: 'Inox Wind Limited' },
  { symbol: 'INSECTICID.NS', name: 'Insecticides (India) Limited' },
  { symbol: 'INSPIRISYS.NS', name: 'Inspirisys Solutions Limited' },
  { symbol: 'INTELLECT.NS', name: 'Intellect Design Arena Limited' },
  { symbol: 'INTENTECH.NS', name: 'Intense Technologies Limited' },
  { symbol: 'INTERARCH.NS', name: 'Interarch Building Solutions Limited' },
  { symbol: 'INTLCONV.NS', name: 'International Conveyors Limited' },
  { symbol: 'INVENTURE.NS', name: 'Inventure Growth & Securities Limited' },
  { symbol: 'INVPRECQ.NS', name: 'Investment & Precision Castings Limited' },
  { symbol: 'IOB.NS', name: 'Indian Overseas Bank' },
  { symbol: 'IOC.NS', name: 'Indian Oil Corporation Limited' },
  { symbol: 'IOLCP.NS', name: 'IOL Chemicals and Pharmaceuticals Limited' },
  { symbol: 'IONEXCHANG.NS', name: 'ION Exchange (India) Limited' },
  { symbol: 'IPCALAB.NS', name: 'IPCA Laboratories Limited' },
  { symbol: 'IPL.NS', name: 'India Pesticides Limited' },
  { symbol: 'IRB.NS', name: 'IRB Infrastructure Developers Limited' },
  { symbol: 'IRCON.NS', name: 'Ircon International Limited' },
  { symbol: 'IRCTC.NS', name: 'Indian Railway Catering And Tourism Corporation Limited' },
  { symbol: 'IREDA.NS', name: 'Indian Renewable Energy Development Agency Limited' },
  { symbol: 'IRFC.NS', name: 'Indian Railway Finance Corporation Limited' },
  { symbol: 'IRIS.NS', name: 'IRIS RegTech Solutions Limited' },
  { symbol: 'IRISDOREME.NS', name: 'Iris Clothings Limited' },
  { symbol: 'IRMENERGY.NS', name: 'IRM Energy Limited' },
  { symbol: 'ISFT.NS', name: 'Intrasoft Technologies Limited' },
  { symbol: 'ISGEC.NS', name: 'Isgec Heavy Engineering Limited' },
  { symbol: 'ISHANCH.NS', name: 'Ishan Dyes and Chemicals Limited' },
  { symbol: 'ITC.NS', name: 'ITC Limited' },
  { symbol: 'ITCHOTELS.NS', name: 'ITC Hotels Limited' },
  { symbol: 'ITDC.NS', name: 'India Tourism Development Corporation Limited' },
  { symbol: 'ITI.NS', name: 'ITI Limited' },
  { symbol: 'IVALUE.NS', name: 'Ivalue Infosolutions Limited' },
  { symbol: 'IVC.NS', name: 'IL&FS Investment Managers Limited' },
  { symbol: 'IVP.NS', name: 'IVP Limited' },
  { symbol: 'IWP.NS', name: 'The Indian Wood Products Company Limited' },
  { symbol: 'IXIGO.NS', name: 'Le Travenues Technology Limited' },
  { symbol: 'IZMO.NS', name: 'IZMO Limited' },
  { symbol: 'J&KBANK.NS', name: 'The Jammu & Kashmir Bank Limited' },
  { symbol: 'JAGRAN.NS', name: 'Jagran Prakashan Limited' },
  { symbol: 'JAGSNPHARM.NS', name: 'Jagsonpal Pharmaceuticals Limited' },
  { symbol: 'JAIBALAJI.NS', name: 'Jai Balaji Industries Limited' },
  { symbol: 'JAICORPLTD.NS', name: 'Jai Corp Limited' },
  { symbol: 'JAINREC.NS', name: 'Jain Resource Recycling Limited' },
  { symbol: 'JAIPURKURT.NS', name: 'Nandani Creation Limited' },
  { symbol: 'JAMNAAUTO.NS', name: 'Jamna Auto Industries Limited' },
  { symbol: 'JARO.NS', name: 'Jaro Institute of Technology Management and Research Limited' },
  { symbol: 'JASH.NS', name: 'Jash Engineering Limited' },
  { symbol: 'JAYAGROGN.NS', name: 'Jayant Agro Organics Limited' },
  { symbol: 'JAYBARMARU.NS', name: 'Jay Bharat Maruti Limited' },
  { symbol: 'JAYKAY.NS', name: 'Jaykay Enterprises Limited' },
  { symbol: 'JAYNECOIND.NS', name: 'Jayaswal Neco Industries Limited' },
  { symbol: 'JAYSREETEA.NS', name: 'Jayshree Tea & Industries Limited' },
  { symbol: 'JBCHEPHARM.NS', name: 'JB Chemicals & Pharmaceuticals Limited' },
  { symbol: 'JBMA.NS', name: 'JBM Auto Limited' },
  { symbol: 'JETFREIGHT.NS', name: 'Jet Freight Logistics Limited' },
  { symbol: 'JGCHEM.NS', name: 'J.G.Chemicals Limited' },
  { symbol: 'JHS.NS', name: 'JHS Svendgaard Laboratories Limited' },
  { symbol: 'JINDALPHOT.NS', name: 'Jindal Photo Limited' },
  { symbol: 'JINDALPOLY.NS', name: 'Jindal Poly Films Limited' },
  { symbol: 'JINDALSAW.NS', name: 'Jindal Saw Limited' },
  { symbol: 'JINDALSTEL.NS', name: 'JINDAL STEEL LIMITED' },
  { symbol: 'JINDRILL.NS', name: 'Jindal Drilling And Industries Limited' },
  { symbol: 'JINDWORLD.NS', name: 'Jindal Worldwide Limited' },
  { symbol: 'JIOFIN.NS', name: 'Jio Financial Services Limited' },
  { symbol: 'JISLDVREQS.NS', name: 'Jain Irrigation Systems Limited' },
  { symbol: 'JISLJALEQS.NS', name: 'Jain Irrigation Systems Limited' },
  { symbol: 'JITFINFRA.NS', name: 'JITF Infralogistics Limited' },
  { symbol: 'JKCEMENT.NS', name: 'JK Cement Limited' },
  { symbol: 'JKIL.NS', name: 'J.Kumar Infraprojects Limited' },
  { symbol: 'JKIPL.NS', name: 'Jinkushal Industries Limited' },
  { symbol: 'JKLAKSHMI.NS', name: 'JK Lakshmi Cement Limited' },
  { symbol: 'JKPAPER.NS', name: 'JK Paper Limited' },
  { symbol: 'JKTYRE.NS', name: 'JK Tyre & Industries Limited' },
  { symbol: 'JLHL.NS', name: 'Jupiter Life Line Hospitals Limited' },
  { symbol: 'JMA.NS', name: 'Jullundur Motor Agency (Delhi) Limited' },
  { symbol: 'JMFINANCIL.NS', name: 'JM Financial Limited' },
  { symbol: 'JNKINDIA.NS', name: 'JNK India Limited' },
  { symbol: 'JOCIL.NS', name: 'Jocil Limited' },
  { symbol: 'JPOLYINVST.NS', name: 'Jindal Poly Investment and Finance Company Limited' },
  { symbol: 'JPPOWER.NS', name: 'Jaiprakash Power Ventures Limited' },
  { symbol: 'JSFB.NS', name: 'Jana Small Finance Bank Limited' },
  { symbol: 'JSL.NS', name: 'Jindal Stainless Limited' },
  { symbol: 'JSLL.NS', name: 'Jeena Sikho Lifecare Limited' },
  { symbol: 'JSWCEMENT.NS', name: 'JSW Cement Limited' },
  { symbol: 'JSWDULUX.NS', name: 'JSW Dulux Limited' },
  { symbol: 'JSWENERGY.NS', name: 'JSW Energy Limited' },
  { symbol: 'JSWHL.NS', name: 'JSW Holdings Limited' },
  { symbol: 'JSWINFRA.NS', name: 'JSW Infrastructure Limited' },
  { symbol: 'JSWSTEEL.NS', name: 'JSW Steel Limited' },
  { symbol: 'JTEKTINDIA.NS', name: 'Jtekt India Limited' },
  { symbol: 'JTLIND.NS', name: 'JTL INDUSTRIES LIMITED' },
  { symbol: 'JUBLCPL.NS', name: 'Jubilant Agri and Consumer Products Limited' },
  { symbol: 'JUBLFOOD.NS', name: 'Jubilant Foodworks Limited' },
  { symbol: 'JUBLINGREA.NS', name: 'Jubilant Ingrevia Limited' },
  { symbol: 'JUBLPHARMA.NS', name: 'Jubilant Pharmova Limited' },
  { symbol: 'JUNIPER.NS', name: 'Juniper Hotels Limited' },
  { symbol: 'JUSTDIAL.NS', name: 'Just Dial Limited' },
  { symbol: 'JWL.NS', name: 'Jupiter Wagons Limited' },
  { symbol: 'JYOTHYLAB.NS', name: 'Jyothy Labs Limited' },
  { symbol: 'JYOTICNC.NS', name: 'Jyoti CNC Automation Limited' },
  { symbol: 'JYOTISTRUC.NS', name: 'Jyoti Structures Limited' },
  { symbol: 'KABRAEXTRU.NS', name: 'Kabra Extrusion Technik Limited' },
  { symbol: 'KAJARIACER.NS', name: 'Kajaria Ceramics Limited' },
  { symbol: 'KAKATCEM.NS', name: 'Kakatiya Cement Sugar & Industries Limited' },
  { symbol: 'KALAMANDIR.NS', name: 'Sai Silks (Kalamandir) Limited' },
  { symbol: 'KALPATARU.NS', name: 'Kalpataru Limited' },
  { symbol: 'KALYANI.NS', name: 'Kalyani Commercials Limited' },
  { symbol: 'KALYANIFRG.NS', name: 'Kalyani Forge Limited' },
  { symbol: 'KALYANKJIL.NS', name: 'Kalyan Jewellers India Limited' },
  { symbol: 'KAMAHOLD.NS', name: 'Kama Holdings Limited' },
  { symbol: 'KAMATHOTEL.NS', name: 'Kamat Hotels (I) Limited' },
  { symbol: 'KAMDHENU.NS', name: 'Kamdhenu Limited' },
  { symbol: 'KAMOPAINTS.NS', name: 'Kamdhenu Ventures Limited' },
  { symbol: 'KANANIIND.NS', name: 'Kanani Industries Limited' },
  { symbol: 'KANCHI.NS', name: 'Kanchi Karpooram Limited' },
  { symbol: 'KANORICHEM.NS', name: 'Kanoria Chemicals & Industries Limited' },
  { symbol: 'KANPRPLA.NS', name: 'Kanpur Plastipack Limited' },
  { symbol: 'KANSAINER.NS', name: 'Kansai Nerolac Paints Limited' },
  { symbol: 'KAPSTON.NS', name: 'Kapston Services Limited' },
  { symbol: 'KARMAENG.NS', name: 'Karma Energy Limited' },
  { symbol: 'KARURVYSYA.NS', name: 'Karur Vysya Bank Limited' },
  { symbol: 'KAUSHALYA.NS', name: 'Kaushalya Infrastructure Development Corporation Limited' },
  { symbol: 'KAVDEFENCE.NS', name: 'Kavveri Defence & Wireless Technologies Limited' },
  { symbol: 'KAYA.NS', name: 'Kaya Limited' },
  { symbol: 'KAYNES.NS', name: 'Kaynes Technology India Limited' },
  { symbol: 'KCP.NS', name: 'KCP Limited' },
  { symbol: 'KCPSUGIND.NS', name: 'KCP Sugar and Industries Corporation Limited' },
  { symbol: 'KDDL.NS', name: 'KDDL Limited' },
  { symbol: 'KEC.NS', name: 'KEC International Limited' },
  { symbol: 'KECL.NS', name: 'Kirloskar Electric Company Limited' },
  { symbol: 'KEEPLEARN.NS', name: 'DSJ Keep Learning Limited' },
  { symbol: 'KEI.NS', name: 'KEI Industries Limited' },
  { symbol: 'KELLTONTEC.NS', name: 'Kellton Tech Solutions Limited' },
  { symbol: 'KENNAMET.NS', name: 'Kennametal India Limited' },
  { symbol: 'KERNEX.NS', name: 'Kernex Microsystems (India) Limited' },
  { symbol: 'KESORAMIND.NS', name: 'Kesoram Industries Limited' },
  { symbol: 'KEYFINSERV.NS', name: 'Keynote Financial Services Limited' },
  { symbol: 'KFINTECH.NS', name: 'Kfin Technologies Limited' },
  { symbol: 'KHADIM.NS', name: 'Khadim India Limited' },
  { symbol: 'KHAICHEM.NS', name: 'Khaitan Chemicals & Fertilizers Limited' },
  { symbol: 'KHAITANLTD.NS', name: 'Khaitan (India) Limited' },
  { symbol: 'KHANDSE.NS', name: 'Khandwala Securities Limited' },
  { symbol: 'KICL.NS', name: 'Kalyani Investment Company Limited' },
  { symbol: 'KILITCH.NS', name: 'Kilitch Drugs (India) Limited' },
  { symbol: 'KIMS.NS', name: 'Krishna Institute of Medical Sciences Limited' },
  { symbol: 'KINGFA.NS', name: 'Kingfa Science & Technology (India) Limited' },
  { symbol: 'KIOCL.NS', name: 'KIOCL Limited' },
  { symbol: 'KIRANVYPAR.NS', name: 'Kiran Vyapar Limited' },
  { symbol: 'KIRIINDUS.NS', name: 'Kiri Industries Limited' },
  { symbol: 'KIRLFER.NS', name: 'Kirloskar Ferrous Industries Limited' },
  { symbol: 'KIRLOSBROS.NS', name: 'Kirloskar Brothers Limited' },
  { symbol: 'KIRLOSENG.NS', name: 'Kirloskar Oil Engines Limited' },
  { symbol: 'KIRLOSIND.NS', name: 'Kirloskar Industries Limited' },
  { symbol: 'KIRLPNU.NS', name: 'Kirloskar Pneumatic Company Limited' },
  { symbol: 'KISSHT.NS', name: 'OnEMI Technology Solutions Limited' },
  { symbol: 'KITEX.NS', name: 'Kitex Garments Limited' },
  { symbol: 'KKCL.NS', name: 'Kewal Kiran Clothing Limited' },
  { symbol: 'KLBRENG-B.NS', name: 'Kilburn Engineering Limited' },
  { symbol: 'KMEW.NS', name: 'Knowledge Marine & Engineering Works Limited' },
  { symbol: 'KMSUGAR.NS', name: 'K.M.Sugar Mills Limited' },
  { symbol: 'KNAGRI.NS', name: 'KN Agri Resources Limited' },
  { symbol: 'KNRCON.NS', name: 'KNR Constructions Limited' },
  { symbol: 'KOHINOOR.NS', name: 'Kohinoor Foods Limited' },
  { symbol: 'KOKUYOCMLN.NS', name: 'Kokuyo Camlin Limited' },
  { symbol: 'KOLTEPATIL.NS', name: 'Kolte - Patil Developers Limited' },
  { symbol: 'KOPRAN.NS', name: 'Kopran Limited' },
  { symbol: 'KOTAKBANK.NS', name: 'Kotak Mahindra Bank Limited' },
  { symbol: 'KOTARISUG.NS', name: 'Kothari Sugars And Chemicals Limited' },
  { symbol: 'KOTHARIPET.NS', name: 'Kothari Petrochemicals Limited' },
  { symbol: 'KOTHARIPRO.NS', name: 'Kothari Products Limited' },
  { symbol: 'KOTIC.NS', name: 'Kothari Industrial Corporation Limited' },
  { symbol: 'KOTYARK.NS', name: 'Kotyark Industries Limited' },
  { symbol: 'KOVAI.NS', name: 'Kovai Medical Center & Hospital Limited' },
  { symbol: 'KPEL.NS', name: 'K.P. Energy Limited' },
  { symbol: 'KPIGREEN.NS', name: 'KPI Green Energy Limited' },
  { symbol: 'KPIL.NS', name: 'Kalpataru Projects International Limited' },
  { symbol: 'KPITTECH.NS', name: 'KPIT Technologies Limited' },
  { symbol: 'KPL.NS', name: 'Kwality Pharmaceuticals Limited' },
  { symbol: 'KPRMILL.NS', name: 'K.P.R. Mill Limited' },
  { symbol: 'KRBL.NS', name: 'KRBL Limited' },
  { symbol: 'KREBSBIO.NS', name: 'Krebs Biochemicals and Industries Limited' },
  { symbol: 'KRIDHANINF.NS', name: 'Kridhan Infra Limited' },
  { symbol: 'KRISHANA.NS', name: 'Krishana Phoschem Limited' },
  { symbol: 'KRISHIVAL.NS', name: 'Krishival Foods Limited' },
  { symbol: 'KRISHNADEF.NS', name: 'Krishna Defence And Allied Industries Limited' },
  { symbol: 'KRITI.NS', name: 'Kriti Industries (India) Limited' },
  { symbol: 'KRITIKA.NS', name: 'Kritika Wires Limited' },
  { symbol: 'KRITINUT.NS', name: 'Kriti Nutrients Limited' },
  { symbol: 'KRN.NS', name: 'KRN Heat Exchanger and Refrigeration Limited' },
  { symbol: 'KRONOX.NS', name: 'Kronox Lab Sciences Limited' },
  { symbol: 'KROSS.NS', name: 'Kross Limited' },
  { symbol: 'KRSNAA.NS', name: 'Krsnaa Diagnostics Limited' },
  { symbol: 'KRYSTAL.NS', name: 'Krystal Integrated Services Limited' },
  { symbol: 'KSB.NS', name: 'Ksb Limited' },
  { symbol: 'KSCL.NS', name: 'Kaveri Seed Company Limited' },
  { symbol: 'KSHINTL.NS', name: 'KSH International Limited' },
  { symbol: 'KSHITIJPOL.NS', name: 'Kshitij Polyline Limited' },
  { symbol: 'KSL.NS', name: 'Kalyani Steels Limited' },
  { symbol: 'KSOLVES.NS', name: 'Ksolves India Limited' },
  { symbol: 'KSR.NS', name: 'KSR Footwear Limited' },
  { symbol: 'KTKBANK.NS', name: 'The Karnataka Bank Limited' },
  { symbol: 'KUANTUM.NS', name: 'Kuantum Papers Limited' },
  { symbol: 'KWIL.NS', name: 'Kwality Wall\'s (India) Limited' },
  { symbol: 'LAGNAM.NS', name: 'Lagnam Spintex Limited' },
  { symbol: 'LAHOTIOV.NS', name: 'Lahoti Overseas Limited' },
  { symbol: 'LAKPRE.NS', name: 'Lakshmi Precision Screws Limited' },
  { symbol: 'LAL.NS', name: 'Lorenzini Apparels Limited' },
  { symbol: 'LALPATHLAB.NS', name: 'Dr. Lal Path Labs Ltd.' },
  { symbol: 'LAMBODHARA.NS', name: 'Lambodhara Textiles Limited' },
  { symbol: 'LANCORHOL.NS', name: 'Lancor Holdings Limited' },
  { symbol: 'LANDMARK.NS', name: 'Landmark Cars Limited' },
  { symbol: 'LANDSMILL.NS', name: 'Landsmill Green Limited' },
  { symbol: 'LAOPALA.NS', name: 'La Opala RG Limited' },
  { symbol: 'LASA.NS', name: 'Lasa Supergenerics Limited' },
  { symbol: 'LATENTVIEW.NS', name: 'Latent View Analytics Limited' },
  { symbol: 'LATTEYS.NS', name: 'Latteys Industries Limited' },
  { symbol: 'LAURUSLABS.NS', name: 'Laurus Labs Limited' },
  { symbol: 'LAXMICOT.NS', name: 'Laxmi Cotspin Limited' },
  { symbol: 'LAXMIDENTL.NS', name: 'Laxmi Dental Limited' },
  { symbol: 'LAXMIINDIA.NS', name: 'Laxmi India Finance Limited' },
  { symbol: 'LCCINFOTEC.NS', name: 'LCC Infotech Limited' },
  { symbol: 'LEMERITE.NS', name: 'Le Merite Exports Limited' },
  { symbol: 'LEMONTREE.NS', name: 'Lemon Tree Hotels Limited' },
  { symbol: 'LENSKART.NS', name: 'Lenskart Solutions Limited' },
  { symbol: 'LEXUS.NS', name: 'Lexus Granito (India) Limited' },
  { symbol: 'LFIC.NS', name: 'Lakshmi Finance & Industrial Corporation Limited' },
  { symbol: 'LGBBROSLTD.NS', name: 'LG Balakrishnan & Bros Limited' },
  { symbol: 'LGEINDIA.NS', name: 'LG Electronics India Limited' },
  { symbol: 'LGHL.NS', name: 'Laxmi Goldorna House Limited' },
  { symbol: 'LIBAS.NS', name: 'Libas Consumer Products Limited' },
  { symbol: 'LIBERTSHOE.NS', name: 'Liberty Shoes Limited' },
  { symbol: 'LICHSGFIN.NS', name: 'LIC Housing Finance Limited' },
  { symbol: 'LICI.NS', name: 'Life Insurance Corporation Of India' },
  { symbol: 'LIKHITHA.NS', name: 'Likhitha Infrastructure Limited' },
  { symbol: 'LINC.NS', name: 'Linc Limited' },
  { symbol: 'LINCOLN.NS', name: 'Lincoln Pharmaceuticals Limited' },
  { symbol: 'LINDEINDIA.NS', name: 'Linde India Limited' },
  { symbol: 'LLOYDSENGG.NS', name: 'LLOYDS ENGINEERING WORKS LIMITED' },
  { symbol: 'LLOYDSENT.NS', name: 'Lloyds Enterprises Limited' },
  { symbol: 'LLOYDSME.NS', name: 'Lloyds Metals And Energy Limited' },
  { symbol: 'LMW.NS', name: 'LMW Limited' },
  { symbol: 'LODHA.NS', name: 'Lodha Developers Limited' },
  { symbol: 'LOKESHMACH.NS', name: 'Lokesh Machines Limited' },
  { symbol: 'LORDSCHLO.NS', name: 'Lords Chloro Alkali Limited' },
  { symbol: 'LOTUSDEV.NS', name: 'Sri Lotus Developers and Realty Limited' },
  { symbol: 'LOTUSEYE.NS', name: 'Lotus Eye Hospital and Institute Limited' },
  { symbol: 'LOVABLE.NS', name: 'Lovable Lingerie Limited' },
  { symbol: 'LOYALTEX.NS', name: 'Loyal Textile Mills Limited' },
  { symbol: 'LPDC.NS', name: 'Landmark Property Development Company Limited' },
  { symbol: 'LT.NS', name: 'Larsen & Toubro Limited' },
  { symbol: 'LTF.NS', name: 'L&T Finance Limited' },
  { symbol: 'LTFOODS.NS', name: 'LT Foods Limited' },
  { symbol: 'LTM.NS', name: 'LTM Limited' },
  { symbol: 'LTTS.NS', name: 'L&T Technology Services Limited' },
  { symbol: 'LUMAXIND.NS', name: 'Lumax Industries Limited' },
  { symbol: 'LUMAXTECH.NS', name: 'Lumax Auto Technologies Limited' },
  { symbol: 'LUPIN.NS', name: 'Lupin Limited' },
  { symbol: 'LUXIND.NS', name: 'Lux Industries Limited' },
  { symbol: 'LXCHEM.NS', name: 'Laxmi Organic Industries Limited' },
  { symbol: 'LYKALABS.NS', name: 'Lyka Labs Limited' },
  { symbol: 'LYPSAGEMS.NS', name: 'Lypsa Gems & Jewellery Limited' },
  { symbol: 'M&M.NS', name: 'Mahindra & Mahindra Limited' },
  { symbol: 'M&MFIN.NS', name: 'Mahindra & Mahindra Financial Services Limited' },
  { symbol: 'MAANALU.NS', name: 'Maan Aluminium Limited' },
  { symbol: 'MACPOWER.NS', name: 'Macpower CNC Machines Limited' },
  { symbol: 'MADHAV.NS', name: 'Madhav Marbles and Granites Limited' },
  { symbol: 'MADHAVIPL.NS', name: 'Madhav Infra Projects Limited' },
  { symbol: 'MADHUCON.NS', name: 'Madhucon Projects Limited' },
  { symbol: 'MADRASFERT.NS', name: 'Madras Fertilizers Limited' },
  { symbol: 'MAFATIND.NS', name: 'Mafatlal Industries Limited' },
  { symbol: 'MAGADSUGAR.NS', name: 'Magadh Sugar & Energy Limited' },
  { symbol: 'MAGNUM.NS', name: 'Magnum Ventures Limited' },
  { symbol: 'MAHABANK.NS', name: 'Bank of Maharashtra' },
  { symbol: 'MAHAPEXLTD.NS', name: 'Maha Rashtra Apex Corporation Limited' },
  { symbol: 'MAHASTEEL.NS', name: 'Mahamaya Steel Industries Limited' },
  { symbol: 'MAHEPC.NS', name: 'Mahindra EPC Irrigation Limited' },
  { symbol: 'MAHESHWARI.NS', name: 'Maheshwari Logistics Limited' },
  { symbol: 'MAHLIFE.NS', name: 'Mahindra Lifespace Developers Limited' },
  { symbol: 'MAHLOG.NS', name: 'Mahindra Logistics Limited' },
  { symbol: 'MAHSCOOTER.NS', name: 'Maharashtra Scooters Limited' },
  { symbol: 'MAHSEAMLES.NS', name: 'Maharashtra Seamless Limited' },
  { symbol: 'MAITHANALL.NS', name: 'Maithan Alloys Limited' },
  { symbol: 'MAJESAUT.NS', name: 'Majestic Auto Limited' },
  { symbol: 'MALLCOM.NS', name: 'Mallcom (India) Limited' },
  { symbol: 'MALUPAPER.NS', name: 'Malu Paper Mills Limited' },
  { symbol: 'MAMATA.NS', name: 'Mamata Machinery Limited' },
  { symbol: 'MANAKALUCO.NS', name: 'Manaksia Aluminium Company Limited' },
  { symbol: 'MANAKCOAT.NS', name: 'Manaksia Coated Metals & Industries Limited' },
  { symbol: 'MANAKSIA.NS', name: 'Manaksia Limited' },
  { symbol: 'MANAKSTEEL.NS', name: 'Manaksia Steels Limited' },
  { symbol: 'MANALIPETC.NS', name: 'Manali Petrochemicals Limited' },
  { symbol: 'MANAPPURAM.NS', name: 'Manappuram Finance Limited' },
  { symbol: 'MANBA.NS', name: 'Manba Finance Limited' },
  { symbol: 'MANCREDIT.NS', name: 'Mangal Credit and Fincorp Limited' },
  { symbol: 'MANGALAM.NS', name: 'Mangalam Drugs And Organics Limited' },
  { symbol: 'MANGLMCEM.NS', name: 'Mangalam Cement Limited' },
  { symbol: 'MANINDS.NS', name: 'Man Industries (India) Limited' },
  { symbol: 'MANINFRA.NS', name: 'Man Infraconstruction Limited' },
  { symbol: 'MANKIND.NS', name: 'Mankind Pharma Limited' },
  { symbol: 'MANOMAY.NS', name: 'Manomay Tex India Limited' },
  { symbol: 'MANORAMA.NS', name: 'Manorama Industries Limited' },
  { symbol: 'MANORG.NS', name: 'Mangalam Organics Limited' },
  { symbol: 'MANUGRAPH.NS', name: 'Manugraph India Limited' },
  { symbol: 'MANYAVAR.NS', name: 'Vedant Fashions Limited' },
  { symbol: 'MAPMYINDIA.NS', name: 'C.E. Info Systems Limited' },
  { symbol: 'MARALOVER.NS', name: 'Maral Overseas Limited' },
  { symbol: 'MARATHON.NS', name: 'Marathon Nextgen Realty Limited' },
  { symbol: 'MARICO.NS', name: 'Marico Limited' },
  { symbol: 'MARINE.NS', name: 'Marine Electricals (India) Limited' },
  { symbol: 'MARKOLINES.NS', name: 'Markolines Pavement Technologies Limited' },
  { symbol: 'MARKSANS.NS', name: 'Marksans Pharma Limited' },
  { symbol: 'MARSONS.NS', name: 'Marsons Limited' },
  { symbol: 'MARUTI.NS', name: 'Maruti Suzuki India Limited' },
  { symbol: 'MASFIN.NS', name: 'MAS Financial Services Limited' },
  { symbol: 'MASKINVEST.NS', name: 'Mask Investments Limited' },
  { symbol: 'MASTEK.NS', name: 'Mastek Limited' },
  { symbol: 'MASTERTR.NS', name: 'Master Trust Limited' },
  { symbol: 'MATRIMONY.NS', name: 'Matrimony.Com Limited' },
  { symbol: 'MAWANASUG.NS', name: 'Mawana Sugars Limited' },
  { symbol: 'MAXESTATES.NS', name: 'Max Estates Limited' },
  { symbol: 'MAXHEALTH.NS', name: 'Max Healthcare Institute Limited' },
  { symbol: 'MAXIND.NS', name: 'Max India Limited' },
  { symbol: 'MAYURUNIQ.NS', name: 'Mayur Uniquoters Ltd' },
  { symbol: 'MAZDA.NS', name: 'Mazda Limited' },
  { symbol: 'MAZDOCK.NS', name: 'Mazagon Dock Shipbuilders Limited' },
  { symbol: 'MBAPL.NS', name: 'Madhya Bharat Agro Products Limited' },
  { symbol: 'MBEL.NS', name: 'M & B Engineering Limited' },
  { symbol: 'MBLINFRA.NS', name: 'MBL Infrastructure Limited' },
  { symbol: 'MCCHRLS-B.NS', name: 'Mac Charles India Limited' },
  { symbol: 'MCL.NS', name: 'Madhav Copper Limited' },
  { symbol: 'MCLEODRUSS.NS', name: 'Mcleod Russel India Limited' },
  { symbol: 'MCLOUD.NS', name: 'Magellanic Cloud Limited' },
  { symbol: 'MCX.NS', name: 'Multi Commodity Exchange of India Limited' },
  { symbol: 'MEDANTA.NS', name: 'Global Health Limited' },
  { symbol: 'MEDIASSIST.NS', name: 'Medi Assist Healthcare Services Limited' },
  { symbol: 'MEDICAMEQ.NS', name: 'Medicamen Biotech Limited' },
  { symbol: 'MEDICO.NS', name: 'Medico Remedies Limited' },
  { symbol: 'MEDPLUS.NS', name: 'Medplus Health Services Limited' },
  { symbol: 'MEESHO.NS', name: 'Meesho Limited' },
  { symbol: 'MEGASTAR.NS', name: 'Megastar Foods Limited' },
  { symbol: 'MEIL.NS', name: 'Mangal Electrical Industries Limited' },
  { symbol: 'MENNPIS.NS', name: 'Menon Pistons Limited' },
  { symbol: 'MENONBE.NS', name: 'Menon Bearings Limited' },
  { symbol: 'MEP.NS', name: 'MEP Infrastructure Developers Limited' },
  { symbol: 'MERCANTILE.NS', name: 'Mercantile Ventures Limited' },
  { symbol: 'METROBRAND.NS', name: 'Metro Brands Limited' },
  { symbol: 'METROGLOBL.NS', name: 'Metroglobal Limited' },
  { symbol: 'METROPOLIS.NS', name: 'Metropolis Healthcare Limited' },
  { symbol: 'MFML.NS', name: 'Mahalaxmi Fabric Mills Limited' },
  { symbol: 'MFSL.NS', name: 'Max Financial Services Limited' },
  { symbol: 'MGEL.NS', name: 'Mangalam Global Enterprise Limited' },
  { symbol: 'MGL.NS', name: 'Mahanagar Gas Limited' },
  { symbol: 'MHLXMIRU.NS', name: 'Mahalaxmi Rubtech Limited' },
  { symbol: 'MHRIL.NS', name: 'Mahindra Holidays & Resorts India Limited' },
  { symbol: 'MICEL.NS', name: 'MIC Electronics Limited' },
  { symbol: 'MIDHANI.NS', name: 'Mishra Dhatu Nigam Limited' },
  { symbol: 'MIDWESTLTD.NS', name: 'Midwest Limited' },
  { symbol: 'MINDACORP.NS', name: 'Minda Corporation Limited' },
  { symbol: 'MINDTECK.NS', name: 'Mindteck (India) Limited' },
  { symbol: 'MIRCELECTR.NS', name: 'MIRC Electronics Limited' },
  { symbol: 'MIRZAINT.NS', name: 'Mirza International Limited' },
  { symbol: 'MITCON.NS', name: 'MITCON Consultancy & Engineering Services Limited' },
  { symbol: 'MITTAL.NS', name: 'Mittal Life Style Limited' },
  { symbol: 'MKPL.NS', name: 'M K Proteins Limited' },
  { symbol: 'MMFL.NS', name: 'MM Forgings Limited' },
  { symbol: 'MMP.NS', name: 'MMP Industries Limited' },
  { symbol: 'MMTC.NS', name: 'MMTC Limited' },
  { symbol: 'MMWL.NS', name: 'Media Matrix Worldwide Limited' },
  { symbol: 'MOBIKWIK.NS', name: 'One Mobikwik Systems Limited' },
  { symbol: 'MODINATUR.NS', name: 'Modi Naturals Limited' },
  { symbol: 'MODIRUBBER.NS', name: 'Modi Rubber Limited' },
  { symbol: 'MODIS.NS', name: 'Modis Navnirman Limited' },
  { symbol: 'MODISONLTD.NS', name: 'MODISON LIMITED' },
  { symbol: 'MODTHREAD.NS', name: 'Modern Threads (India) Limited' },
  { symbol: 'MOHITIND.NS', name: 'Mohit Industries Limited' },
  { symbol: 'MOIL.NS', name: 'MOIL Limited' },
  { symbol: 'MOKSH.NS', name: 'Moksh Ornaments Limited' },
  { symbol: 'MOL.NS', name: 'Meghmani Organics Limited' },
  { symbol: 'MOLDTECH.NS', name: 'Mold-Tek Technologies Limited' },
  { symbol: 'MOLDTKPAC.NS', name: 'Mold-Tek Packaging Limited' },
  { symbol: 'MONARCH.NS', name: 'Monarch Networth Capital Limited' },
  { symbol: 'MONEYBOXX.NS', name: 'Moneyboxx Finance Limited' },
  { symbol: 'MONTECARLO.NS', name: 'Monte Carlo Fashions Limited' },
  { symbol: 'MORARJEE.NS', name: 'Morarjee Textiles Limited' },
  { symbol: 'MOREPENLAB.NS', name: 'Morepen Laboratories Limited' },
  { symbol: 'MOSCHIP.NS', name: 'Moschip Technologies Limited' },
  { symbol: 'MOTHERSON.NS', name: 'Samvardhana Motherson International Limited' },
  { symbol: 'MOTILALOFS.NS', name: 'Motilal Oswal Financial Services Limited' },
  { symbol: 'MOTISONS.NS', name: 'Motisons Jewellers Limited' },
  { symbol: 'MOTOGENFIN.NS', name: 'The Motor & General Finance Limited' },
  { symbol: 'MPHASIS.NS', name: 'MphasiS Limited' },
  { symbol: 'MPSLTD.NS', name: 'MPS Limited' },
  { symbol: 'MRF.NS', name: 'MRF Limited' },
  { symbol: 'MRPL.NS', name: 'Mangalore Refinery and Petrochemicals Limited' },
  { symbol: 'MSPL.NS', name: 'MSP Steel & Power Limited' },
  { symbol: 'MSTCLTD.NS', name: 'Mstc Limited' },
  { symbol: 'MSUMI.NS', name: 'Motherson Sumi Wiring India Limited' },
  { symbol: 'MTARTECH.NS', name: 'Mtar Technologies Limited' },
  { symbol: 'MTEDUCARE.NS', name: 'MT Educare Limited' },
  { symbol: 'MTNL.NS', name: 'Mahanagar Telephone Nigam Limited' },
  { symbol: 'MUFIN.NS', name: 'Mufin Green Finance Limited' },
  { symbol: 'MUFTI.NS', name: 'Credo Brands Marketing Limited' },
  { symbol: 'MUKANDLTD.NS', name: 'Mukand Limited' },
  { symbol: 'MUKKA.NS', name: 'Mukka Proteins Limited' },
  { symbol: 'MUKTAARTS.NS', name: 'Mukta Arts Limited' },
  { symbol: 'MUNJALAU.NS', name: 'Munjal Auto Industries Limited' },
  { symbol: 'MUNJALSHOW.NS', name: 'Munjal Showa Limited' },
  { symbol: 'MURUDCERA.NS', name: 'Murudeshwar Ceramics Limited' },
  { symbol: 'MUTHOOTCAP.NS', name: 'Muthoot Capital Services Limited' },
  { symbol: 'MUTHOOTFIN.NS', name: 'Muthoot Finance Limited' },
  { symbol: 'MUTHOOTMF.NS', name: 'Muthoot Microfin Limited' },
  { symbol: 'MVGJL.NS', name: 'Manoj Vaibhav Gems N Jewellers Limited' },
  { symbol: 'MWL.NS', name: 'Mangalam Worldwide Limited' },
  { symbol: 'NACLIND.NS', name: 'NACL Industries Limited' },
  { symbol: 'NAGAFERT.NS', name: 'Nagarjuna Fertilizers and Chemicals Limited' },
  { symbol: 'NAGREEKCAP.NS', name: 'Nagreeka Capital & Infrastructure Limited' },
  { symbol: 'NAGREEKEXP.NS', name: 'Nagreeka Exports Limited' },
  { symbol: 'NAHARCAP.NS', name: 'Nahar Capital and Financial Services Limited' },
  { symbol: 'NAHARINDUS.NS', name: 'Nahar Industrial Enterprises Limited' },
  { symbol: 'NAHARPOLY.NS', name: 'Nahar Poly Films Limited' },
  { symbol: 'NAHARSPING.NS', name: 'Nahar Spinning Mills Limited' },
  { symbol: 'NAM-INDIA.NS', name: 'Nippon Life India Asset Management Limited' },
  { symbol: 'NARMADA.NS', name: 'Narmada Agrobase Limited' },
  { symbol: 'NATCAPSUQ.NS', name: 'Natural Capsules Limited' },
  { symbol: 'NATCOPHARM.NS', name: 'Natco Pharma Limited' },
  { symbol: 'NATHBIOGEN.NS', name: 'Nath Bio-Genes (India) Limited' },
  { symbol: 'NATIONALUM.NS', name: 'National Aluminium Company Limited' },
  { symbol: 'NATIONSTD.NS', name: 'National Standard (India) Limited' },
  { symbol: 'NAUKRI.NS', name: 'Info Edge (India) Limited' },
  { symbol: 'NAVA.NS', name: 'NAVA LIMITED' },
  { symbol: 'NAVINFLUOR.NS', name: 'Navin Fluorine International Limited' },
  { symbol: 'NAVKARCORP.NS', name: 'Navkar Corporation Limited' },
  { symbol: 'NAVKARURB.NS', name: 'Navkar Urbanstructure Limited' },
  { symbol: 'NAVNETEDUL.NS', name: 'Navneet Education Limited' },
  { symbol: 'NAZARA.NS', name: 'Nazara Technologies Limited' },
  { symbol: 'NBCC.NS', name: 'NBCC (India) Limited' },
  { symbol: 'NBIFIN.NS', name: 'N. B. I. Industrial Finance Company Limited' },
  { symbol: 'NCC.NS', name: 'NCC Limited' },
  { symbol: 'NCLIND.NS', name: 'NCL Industries Limited' },
  { symbol: 'NDGL.NS', name: 'Naga Dhunseri Group Limited' },
  { symbol: 'NDL.NS', name: 'Nandan Denim Limited' },
  { symbol: 'NDLVENTURE.NS', name: 'NDL Ventures Limited' },
  { symbol: 'NDRAUTO.NS', name: 'Ndr Auto Components Limited' },
  { symbol: 'NDTV.NS', name: 'New Delhi Television Limited' },
  { symbol: 'NEAGI.NS', name: 'Neelamalai Agro Industries Limited' },
  { symbol: 'NECCLTD.NS', name: 'North Eastern Carrying Corporation Limited' },
  { symbol: 'NECLIFE.NS', name: 'Nectar Lifesciences Limited' },
  { symbol: 'NELCAST.NS', name: 'Nelcast Limited' },
  { symbol: 'NELCO.NS', name: 'NELCO Limited' },
  { symbol: 'NEOGEN.NS', name: 'Neogen Chemicals Limited' },
  { symbol: 'NEPHROPLUS.NS', name: 'Nephrocare Health Services Limited' },
  { symbol: 'NESCO.NS', name: 'Nesco Limited' },
  { symbol: 'NESTLEIND.NS', name: 'Nestle India Limited' },
  { symbol: 'NETWEB.NS', name: 'Netweb Technologies India Limited' },
  { symbol: 'NETWORK18.NS', name: 'Network18 Media & Investments Limited' },
  { symbol: 'NEULANDLAB.NS', name: 'Neuland Laboratories Limited' },
  { symbol: 'NEWGEN.NS', name: 'Newgen Software Technologies Limited' },
  { symbol: 'NEXTMEDIA.NS', name: 'Next Mediaworks Limited' },
  { symbol: 'NFL.NS', name: 'National Fertilizers Limited' },
  { symbol: 'NGIL.NS', name: 'Nakoda Group of Industries Limited' },
  { symbol: 'NGLFINE.NS', name: 'NGL Fine-Chem Limited' },
  { symbol: 'NH.NS', name: 'Narayana Hrudayalaya Ltd.' },
  { symbol: 'NHPC.NS', name: 'NHPC Limited' },
  { symbol: 'NIACL.NS', name: 'The New India Assurance Company Limited' },
  { symbol: 'NIBE.NS', name: 'NIBE Limited' },
  { symbol: 'NIBL.NS', name: 'NRB Industrial Bearings Limited' },
  { symbol: 'NIITLTD.NS', name: 'NIIT Limited' },
  { symbol: 'NIITMTS.NS', name: 'NIIT Learning Systems Limited' },
  { symbol: 'NILAINFRA.NS', name: 'Nila Infrastructures Limited' },
  { symbol: 'NILASPACES.NS', name: 'Nila Spaces Limited' },
  { symbol: 'NILE.NS', name: 'Nile Limited' },
  { symbol: 'NILKAMAL.NS', name: 'Nilkamal Limited' },
  { symbol: 'NIMBSPROJ.NS', name: 'Nimbus Projects Limited' },
  { symbol: 'NINSYS.NS', name: 'NINtec Systems Limited' },
  { symbol: 'NIPPOBATRY.NS', name: 'Indo-National Limited' },
  { symbol: 'NIRAJ.NS', name: 'Niraj Cement Structurals Limited' },
  { symbol: 'NIRAJISPAT.NS', name: 'Niraj Ispat Industries Limited' },
  { symbol: 'NIRLON.NS', name: 'Nirlon Limited' },
  { symbol: 'NITCO.NS', name: 'Nitco Limited' },
  { symbol: 'NITINSPIN.NS', name: 'Nitin Spinners Limited' },
  { symbol: 'NITIRAJ.NS', name: 'Nitiraj Engineers Limited' },
  { symbol: 'NITTAGELA.NS', name: 'Nitta Gelatin India Limited' },
  { symbol: 'NIVABUPA.NS', name: 'Niva Bupa Health Insurance Company Limited' },
  { symbol: 'NKIND.NS', name: 'NK Industries Limited' },
  { symbol: 'NLCINDIA.NS', name: 'NLC India Limited' },
  { symbol: 'NMDC.NS', name: 'NMDC Limited' },
  { symbol: 'NOCIL.NS', name: 'NOCIL Limited' },
  { symbol: 'NOIDATOLL.NS', name: 'Noida Toll Bridge Company Limited' },
  { symbol: 'NORBTEAEXP.NS', name: 'Norben Tea & Exports Limited' },
  { symbol: 'NORTHARC.NS', name: 'Northern Arc Capital Limited' },
  { symbol: 'NOVAAGRI.NS', name: 'Nova Agritech Limited' },
  { symbol: 'NOVARTIND.NS', name: 'Novartis India Limited' },
  { symbol: 'NPST.NS', name: 'Network People Services Technologies Limited' },
  { symbol: 'NRAIL.NS', name: 'N R Agarwal Industries Limited' },
  { symbol: 'NRBBEARING.NS', name: 'NRB Bearing Limited' },
  { symbol: 'NRL.NS', name: 'Nupur Recyclers Limited' },
  { symbol: 'NSIL.NS', name: 'Nalwa Sons Investments Limited' },
  { symbol: 'NSLNISP.NS', name: 'NMDC Steel Limited' },
  { symbol: 'NTPC.NS', name: 'NTPC Limited' },
  { symbol: 'NTPCGREEN.NS', name: 'NTPC Green Energy Limited' },
  { symbol: 'NUCLEUS.NS', name: 'Nucleus Software Exports Limited' },
  { symbol: 'NURECA.NS', name: 'Nureca Limited' },
  { symbol: 'NUVAMA.NS', name: 'Nuvama Wealth Management Limited' },
  { symbol: 'NUVOCO.NS', name: 'Nuvoco Vistas Corporation Limited' },
  { symbol: 'NYKAA.NS', name: 'FSN E-Commerce Ventures Limited' },
  { symbol: 'OAL.NS', name: 'Oriental Aromatics Limited' },
  { symbol: 'OBCL.NS', name: 'Orissa Bengal Carrier Limited' },
  { symbol: 'OBEROIRLTY.NS', name: 'Oberoi Realty Limited' },
  { symbol: 'OCCLLTD.NS', name: 'OCCL Limited' },
  { symbol: 'ODIGMA.NS', name: 'Odigma Consultancy Solutions Limited' },
  { symbol: 'OFSS.NS', name: 'Oracle Financial Services Software Limited' },
  { symbol: 'OIL.NS', name: 'Oil India Limited' },
  { symbol: 'OILCOUNTUB.NS', name: 'Oil Country Tubular Limited' },
  { symbol: 'OLAELEC.NS', name: 'Ola Electric Mobility Limited' },
  { symbol: 'OLECTRA.NS', name: 'Olectra Greentech Limited' },
  { symbol: 'OMAXAUTO.NS', name: 'Omax Autos Limited' },
  { symbol: 'OMAXE.NS', name: 'Omaxe Limited' },
  { symbol: 'OMFREIGHT.NS', name: 'Om Freight Forwarders Limited' },
  { symbol: 'OMINFRAL.NS', name: 'OM INFRA LIMITED' },
  { symbol: 'OMNI.NS', name: 'Omnitech Engineering Limited' },
  { symbol: 'OMPOWER.NS', name: 'Om Power Transmission Limited' },
  { symbol: 'ONELIFECAP.NS', name: 'Onelife Capital Advisors Limited' },
  { symbol: 'ONEPOINT.NS', name: 'One Point One Solutions Limited' },
  { symbol: 'ONESOURCE.NS', name: 'Onesource Specialty Pharma Limited' },
  { symbol: 'ONGC.NS', name: 'Oil & Natural Gas Corporation Limited' },
  { symbol: 'ONMOBILE.NS', name: 'OnMobile Global Limited' },
  { symbol: 'ONWARDTEC.NS', name: 'Onward Technologies Limited' },
  { symbol: 'OPTIEMUS.NS', name: 'Optiemus Infracom Limited' },
  { symbol: 'ORBTEXP.NS', name: 'Orbit Exports Limited' },
  { symbol: 'ORCHASP.NS', name: 'Orchasp Limited' },
  { symbol: 'ORCHPHARMA.NS', name: 'Orchid Pharma Limited' },
  { symbol: 'ORICONENT.NS', name: 'Oricon Enterprises Limited' },
  { symbol: 'ORIENTALTL.NS', name: 'Oriental Trimex Limited' },
  { symbol: 'ORIENTBELL.NS', name: 'Orient Bell Limited' },
  { symbol: 'ORIENTCEM.NS', name: 'Orient Cement Limited' },
  { symbol: 'ORIENTCER.NS', name: 'ORIENT CERATECH LIMITED' },
  { symbol: 'ORIENTELEC.NS', name: 'Orient Electric Limited' },
  { symbol: 'ORIENTHOT.NS', name: 'Oriental Hotels Limited' },
  { symbol: 'ORIENTLTD.NS', name: 'Orient Press Limited' },
  { symbol: 'ORIENTPPR.NS', name: 'Orient Paper & Industries Limited' },
  { symbol: 'ORIENTTECH.NS', name: 'Orient Technologies Limited' },
  { symbol: 'ORISSAMINE.NS', name: 'The Orissa Minerals Development Company Limited' },
  { symbol: 'ORKLAINDIA.NS', name: 'Orkla India Limited' },
  { symbol: 'ORTEL.NS', name: 'Ortel Communications Limited' },
  { symbol: 'ORTINGLOBE.NS', name: 'ORTIN GLOBAL LIMITED' },
  { symbol: 'OSIAHYPER.NS', name: 'Osia Hyper Retail Limited' },
  { symbol: 'OSWALAGRO.NS', name: 'Oswal Agro Mills Limited' },
  { symbol: 'OSWALGREEN.NS', name: 'Oswal Greentech Limited' },
  { symbol: 'OSWALPUMPS.NS', name: 'Oswal Pumps Limited' },
  { symbol: 'OSWALSEEDS.NS', name: 'ShreeOswal Seeds And Chemicals Limited' },
  { symbol: 'PACEDIGITK.NS', name: 'Pace Digitek Limited' },
  { symbol: 'PAGEIND.NS', name: 'Page Industries Limited' },
  { symbol: 'PAISALO.NS', name: 'Paisalo Digital Limited' },
  { symbol: 'PAKKA.NS', name: 'PAKKA LIMITED' },
  { symbol: 'PALASHSECU.NS', name: 'Palash Securities Limited' },
  { symbol: 'PALREDTEC.NS', name: 'Palred Technologies Limited' },
  { symbol: 'PANACEABIO.NS', name: 'Panacea Biotec Limited' },
  { symbol: 'PANACHE.NS', name: 'Panache Digilife Limited' },
  { symbol: 'PANAMAPET.NS', name: 'Panama Petrochem Limited' },
  { symbol: 'PANSARI.NS', name: 'Pansari Developers Limited' },
  { symbol: 'PAR.NS', name: 'Par Drugs And Chemicals Limited' },
  { symbol: 'PARACABLES.NS', name: 'Paramount Communications Limited' },
  { symbol: 'PARADEEP.NS', name: 'Paradeep Phosphates Limited' },
  { symbol: 'PARAGMILK.NS', name: 'Parag Milk Foods Limited' },
  { symbol: 'PARAS.NS', name: 'Paras Defence and Space Technologies Limited' },
  { symbol: 'PARASPETRO.NS', name: 'Paras Petrofils Limited' },
  { symbol: 'PARKHOSPS.NS', name: 'Park Medi World Limited' },
  { symbol: 'PARKHOTELS.NS', name: 'Apeejay Surrendra Park Hotels Limited' },
  { symbol: 'PARSVNATH.NS', name: 'Parsvnath Developers Limited' },
  { symbol: 'PASHUPATI.NS', name: 'Pashupati Cotspin Limited' },
  { symbol: 'PASUPTAC.NS', name: 'Pasupati Acrylon Limited' },
  { symbol: 'PATANJALI.NS', name: 'Patanjali Foods Limited' },
  { symbol: 'PATELENG.NS', name: 'Patel Engineering Limited' },
  { symbol: 'PATELRMART.NS', name: 'Patel Retail Limited' },
  { symbol: 'PATINTLOG.NS', name: 'Patel Integrated Logistics Limited' },
  { symbol: 'PAUSHAKLTD.NS', name: 'Paushak Limited' },
  { symbol: 'PAVNAIND.NS', name: 'Pavna Industries Limited' },
  { symbol: 'PAYTM.NS', name: 'One 97 Communications Limited' },
  { symbol: 'PCBL.NS', name: 'PCBL Chemical Limited' },
  { symbol: 'PCJEWELLER.NS', name: 'PC Jeweller Limited' },
  { symbol: 'PDMJEPAPER.NS', name: 'Pudumjee Paper Products Limited' },
  { symbol: 'PDSL.NS', name: 'PDS Limited' },
  { symbol: 'PEARLPOLY.NS', name: 'Pearl Polymers Limited' },
  { symbol: 'PENIND.NS', name: 'Pennar Industries Limited' },
  { symbol: 'PENINLAND.NS', name: 'Peninsula Land Limited' },
  { symbol: 'PERSISTENT.NS', name: 'Persistent Systems Limited' },
  { symbol: 'PETRONET.NS', name: 'Petronet LNG Limited' },
  { symbol: 'PFC.NS', name: 'Power Finance Corporation Limited' },
  { symbol: 'PFIZER.NS', name: 'Pfizer Limited' },
  { symbol: 'PFOCUS.NS', name: 'Prime Focus Limited' },
  { symbol: 'PFS.NS', name: 'PTC India Financial Services Limited' },
  { symbol: 'PGEL.NS', name: 'PG Electroplast Limited' },
  { symbol: 'PGHH.NS', name: 'Procter & Gamble Hygiene and Health Care Limited' },
  { symbol: 'PGHL.NS', name: 'Procter & Gamble Health Limited' },
  { symbol: 'PGIL.NS', name: 'Pearl Global Industries Limited' },
  { symbol: 'PHOENIXLTD.NS', name: 'The Phoenix Mills Limited' },
  { symbol: 'PICCADIL.NS', name: 'Piccadily Agro Industries Limited' },
  { symbol: 'PIDILITIND.NS', name: 'Pidilite Industries Limited' },
  { symbol: 'PIGL.NS', name: 'Power & Instrumentation (Gujarat) Limited' },
  { symbol: 'PIIND.NS', name: 'PI Industries Limited' },
  { symbol: 'PILANIINVS.NS', name: 'Pilani Investment and Industries Corporation Limited' },
  { symbol: 'PILITA.NS', name: 'PIL ITALICA LIFESTYLE LIMITED' },
  { symbol: 'PINELABS.NS', name: 'Pine Labs Limited' },
  { symbol: 'PIONEEREMB.NS', name: 'Pioneer Embroideries Limited' },
  { symbol: 'PIONRINV.NS', name: 'Pioneer Investcorp Limited' },
  { symbol: 'PIRAMALFIN.NS', name: 'Piramal Finance Limited' },
  { symbol: 'PITTIENG.NS', name: 'Pitti Engineering Limited' },
  { symbol: 'PIXTRANS.NS', name: 'Pix Transmissions Limited' },
  { symbol: 'PKTEA.NS', name: 'The Peria Karamalai Tea & Produce Company Limited' },
  { symbol: 'PLASTIBLEN.NS', name: 'Plastiblends India Limited' },
  { symbol: 'PLATIND.NS', name: 'Platinum Industries Limited' },
  { symbol: 'PLAZACABLE.NS', name: 'Plaza Wires Limited' },
  { symbol: 'PML.NS', name: 'Paul Merchants Limited' },
  { symbol: 'PNB.NS', name: 'Punjab National Bank' },
  { symbol: 'PNBGILTS.NS', name: 'PNB Gilts Limited' },
  { symbol: 'PNBHOUSING.NS', name: 'PNB Housing Finance Limited' },
  { symbol: 'PNC.NS', name: 'Pritish Nandy Communications Limited' },
  { symbol: 'PNCINFRA.NS', name: 'PNC Infratech Limited' },
  { symbol: 'PNGJL.NS', name: 'P N Gadgil Jewellers Limited' },
  { symbol: 'PNGSREVA.NS', name: 'PNGS Reva Diamond Jewellery Limited' },
  { symbol: 'POCL.NS', name: 'Pondy Oxides & Chemicals Limited' },
  { symbol: 'PODDARMENT.NS', name: 'Poddar Pigments Limited' },
  { symbol: 'POKARNA.NS', name: 'Pokarna Limited' },
  { symbol: 'POLICYBZR.NS', name: 'PB Fintech Limited' },
  { symbol: 'POLYCAB.NS', name: 'Polycab India Limited' },
  { symbol: 'POLYMED.NS', name: 'Poly Medicure Limited' },
  { symbol: 'POLYPLEX.NS', name: 'Polyplex Corporation Limited' },
  { symbol: 'PONNIERODE.NS', name: 'Ponni Sugars (Erode) Limited' },
  { symbol: 'POONAWALLA.NS', name: 'Poonawalla Fincorp Limited' },
  { symbol: 'POWERGRID.NS', name: 'Power Grid Corporation of India Limited' },
  { symbol: 'POWERICA.NS', name: 'Powerica Limited' },
  { symbol: 'POWERINDIA.NS', name: 'Hitachi Energy India Limited' },
  { symbol: 'POWERMECH.NS', name: 'Power Mech Projects Limited' },
  { symbol: 'PPAP.NS', name: 'PPAP Automotive Limited' },
  { symbol: 'PPL.NS', name: 'Prakash Pipes Limited' },
  { symbol: 'PPLPHARMA.NS', name: 'Piramal Pharma Limited' },
  { symbol: 'PRABHA.NS', name: 'Prabha Energy Limited' },
  { symbol: 'PRADPME.NS', name: 'Pradeep Metals Limited' },
  { symbol: 'PRAENG.NS', name: 'Prajay Engineers Syndicate Limited' },
  { symbol: 'PRAJIND.NS', name: 'Praj Industries Limited' },
  { symbol: 'PRAKASH.NS', name: 'Prakash Industries Limited' },
  { symbol: 'PRAKASHSTL.NS', name: 'Prakash Steelage Limited' },
  { symbol: 'PRAVEG.NS', name: 'Praveg Limited' },
  { symbol: 'PRAXIS.NS', name: 'Praxis Home Retail Limited' },
  { symbol: 'PRECAM.NS', name: 'Precision Camshafts Limited' },
  { symbol: 'PRECOT.NS', name: 'Precot Limited' },
  { symbol: 'PRECWIRE.NS', name: 'Precision Wires India Limited' },
  { symbol: 'PREMCO.NS', name: 'Premco Global Limited' },
  { symbol: 'PREMEXPLN.NS', name: 'Premier Explosives Limited' },
  { symbol: 'PREMIER.NS', name: 'Premier Limited' },
  { symbol: 'PREMIERENE.NS', name: 'Premier Energies Limited' },
  { symbol: 'PREMIERPOL.NS', name: 'Premier Polyfilm Limited' },
  { symbol: 'PRESTIGE.NS', name: 'Prestige Estates Projects Limited' },
  { symbol: 'PRICOLLTD.NS', name: 'Pricol Limited' },
  { symbol: 'PRIMESECU.NS', name: 'Prime Securities Limited' },
  { symbol: 'PRIMO.NS', name: 'Primo Chemicals Limited' },
  { symbol: 'PRINCEPIPE.NS', name: 'Prince Pipes And Fittings Limited' },
  { symbol: 'PRITI.NS', name: 'Priti International Limited' },
  { symbol: 'PRITIKAUTO.NS', name: 'Pritika Auto Industries Limited' },
  { symbol: 'PRIVISCL.NS', name: 'Privi Speciality Chemicals Limited' },
  { symbol: 'PROSTARM.NS', name: 'Prostarm Info Systems Limited' },
  { symbol: 'PROTEAN.NS', name: 'Protean eGov Technologies Limited' },
  { symbol: 'PROZONER.NS', name: 'Prozone Realty Limited' },
  { symbol: 'PRSMJOHNSN.NS', name: 'Prism Johnson Limited' },
  { symbol: 'PRUDENT.NS', name: 'Prudent Corporate Advisory Services Limited' },
  { symbol: 'PRUDMOULI.NS', name: 'Prudential Sugar Corporation Limited' },
  { symbol: 'PSB.NS', name: 'Punjab & Sind Bank' },
  { symbol: 'PSPPROJECT.NS', name: 'PSP Projects Limited' },
  { symbol: 'PTC.NS', name: 'PTC India Limited' },
  { symbol: 'PTCIL.NS', name: 'PTC Industries Limited' },
  { symbol: 'PTL.NS', name: 'PTL Enterprises Limited' },
  { symbol: 'PUNJABCHEM.NS', name: 'Punjab Chemicals & Crop Protection Limited' },
  { symbol: 'PURVA.NS', name: 'Puravankara Limited' },
  { symbol: 'PVP.NS', name: 'PVP Ventures Limited' },
  { symbol: 'PVRINOX.NS', name: 'PVR INOX Limited' },
  { symbol: 'PVSL.NS', name: 'Popular Vehicles and Services Limited' },
  { symbol: 'PWL.NS', name: 'Physicswallah Limited' },
  { symbol: 'PYRAMID.NS', name: 'Pyramid Technoplast Limited' },
  { symbol: 'QPOWER.NS', name: 'Quality Power Electrical Equipments Limited' },
  { symbol: 'QUADFUTURE.NS', name: 'Quadrant Future Tek Limited' },
  { symbol: 'QUESS.NS', name: 'Quess Corp Limited' },
  { symbol: 'QUICKHEAL.NS', name: 'Quick Heal Technologies Limited' },
  { symbol: 'QUINT.NS', name: 'Quint Digital Limited' },
  { symbol: 'RACE.NS', name: 'Race Eco Chain Limited' },
  { symbol: 'RACLGEAR.NS', name: 'RACL Geartech Limited' },
  { symbol: 'RADAAN.NS', name: 'Radaan Mediaworks India Limited' },
  { symbol: 'RADHIKAJWE.NS', name: 'Radhika Jeweltech Limited' },
  { symbol: 'RADIANTCMS.NS', name: 'Radiant Cash Management Services Limited' },
  { symbol: 'RADICO.NS', name: 'Radico Khaitan Limited' },
  { symbol: 'RADIOCITY.NS', name: 'Music Broadcast Limited' },
  { symbol: 'RAILTEL.NS', name: 'Railtel Corporation Of India Limited' },
  { symbol: 'RAIN.NS', name: 'Rain Industries Limited' },
  { symbol: 'RAINBOW.NS', name: 'Rainbow Childrens Medicare Limited' },
  { symbol: 'RAJESHEXPO.NS', name: 'Rajesh Exports Limited' },
  { symbol: 'RAJMET.NS', name: 'Rajnandini Metal Limited' },
  { symbol: 'RAJOOENG.NS', name: 'Rajoo Engineers Limited' },
  { symbol: 'RAJPALAYAM.NS', name: 'Rajapalayam Mills Limited' },
  { symbol: 'RAJRATAN.NS', name: 'Rajratan Global Wire Limited' },
  { symbol: 'RAJRILTD.NS', name: 'Raj Rayon Industries Limited' },
  { symbol: 'RAJSREESUG.NS', name: 'Rajshree Sugars & Chemicals Limited' },
  { symbol: 'RAJTV.NS', name: 'Raj Television Network Limited' },
  { symbol: 'RALLIS.NS', name: 'Rallis India Limited' },
  { symbol: 'RAMANEWS.NS', name: 'Shree Rama Newsprint Limited' },
  { symbol: 'RAMAPHO.NS', name: 'Rama Phosphates Limited' },
  { symbol: 'RAMASTEEL.NS', name: 'Rama Steel Tubes Limited' },
  { symbol: 'RAMCOCEM.NS', name: 'The Ramco Cements Limited' },
  { symbol: 'RAMCOIND.NS', name: 'Ramco Industries Limited' },
  { symbol: 'RAMCOSYS.NS', name: 'Ramco Systems Limited' },
  { symbol: 'RAMKY.NS', name: 'Ramky Infrastructure Limited' },
  { symbol: 'RAMRAT.NS', name: 'Ram Ratna Wires Limited' },
  { symbol: 'RANASUG.NS', name: 'Rana Sugars Limited' },
  { symbol: 'RANEHOLDIN.NS', name: 'Rane Holdings Limited' },
  { symbol: 'RATEGAIN.NS', name: 'Rategain Travel Technologies Limited' },
  { symbol: 'RATNAMANI.NS', name: 'Ratnamani Metals & Tubes Limited' },
  { symbol: 'RATNAVEER.NS', name: 'Ratnaveer Precision Engineering Limited' },
  { symbol: 'RAYMOND.NS', name: 'Raymond Limited' },
  { symbol: 'RAYMONDLSL.NS', name: 'Raymond Lifestyle Limited' },
  { symbol: 'RAYMONDREL.NS', name: 'Raymond Realty Limited' },
  { symbol: 'RBA.NS', name: 'Restaurant Brands Asia Limited' },
  { symbol: 'RBLBANK.NS', name: 'RBL Bank Limited' },
  { symbol: 'RBZJEWEL.NS', name: 'RBZ Jewellers Limited' },
  { symbol: 'RCF.NS', name: 'Rashtriya Chemicals and Fertilizers Limited' },
  { symbol: 'RCOM.NS', name: 'Reliance Communications Limited' },
  { symbol: 'RECLTD.NS', name: 'REC Limited' },
  { symbol: 'REDINGTON.NS', name: 'Redington Limited' },
  { symbol: 'REDTAPE.NS', name: 'Redtape Limited' },
  { symbol: 'REFEX.NS', name: 'Refex Industries Limited' },
  { symbol: 'REGAAL.NS', name: 'Regaal Resources Limited' },
  { symbol: 'REGENCERAM.NS', name: 'Regency Ceramics Limited' },
  { symbol: 'RELAXO.NS', name: 'Relaxo Footwears Limited' },
  { symbol: 'RELCHEMQ.NS', name: 'Reliance Chemotex Industries Limited' },
  { symbol: 'RELIABLE.NS', name: 'Reliable Data Services Limited' },
  { symbol: 'RELIANCE.NS', name: 'Reliance Industries Limited' },
  { symbol: 'RELIGARE.NS', name: 'Religare Enterprises Limited' },
  { symbol: 'RELTD.NS', name: 'Ravindra Energy Limited' },
  { symbol: 'REMSONSIND.NS', name: 'Remsons Industries Limited' },
  { symbol: 'RENUKA.NS', name: 'Shree Renuka Sugars Limited' },
  { symbol: 'REPCOHOME.NS', name: 'Repco Home Finance Limited' },
  { symbol: 'REPL.NS', name: 'Rudrabhishek Enterprises Limited' },
  { symbol: 'REPRO.NS', name: 'Repro India Limited' },
  { symbol: 'RESPONIND.NS', name: 'Responsive Industries Limited' },
  { symbol: 'RETAIL.NS', name: 'JHS Svendgaard Retail Ventures Limited' },
  { symbol: 'RGL.NS', name: 'Renaissance Global Limited' },
  { symbol: 'RHETAN.NS', name: 'Rhetan TMT Limited' },
  { symbol: 'RHFL.NS', name: 'Reliance Home Finance Limited' },
  { symbol: 'RHIM.NS', name: 'RHI MAGNESITA INDIA LIMITED' },
  { symbol: 'RHL.NS', name: 'Robust Hotels Limited' },
  { symbol: 'RICOAUTO.NS', name: 'Rico Auto Industries Limited' },
  { symbol: 'RIIL.NS', name: 'Reliance Industrial Infrastructure Limited' },
  { symbol: 'RISHABH.NS', name: 'Rishabh Instruments Limited' },
  { symbol: 'RITCO.NS', name: 'Ritco Logistics Limited' },
  { symbol: 'RITES.NS', name: 'RITES Limited' },
  { symbol: 'RKDL.NS', name: 'Ravi Kumar Distilleries Limited' },
  { symbol: 'RKEC.NS', name: 'RKEC Projects Limited' },
  { symbol: 'RKFORGE.NS', name: 'Ramkrishna Forgings Limited' },
  { symbol: 'RKSWAMY.NS', name: 'R K Swamy Limited' },
  { symbol: 'RMC.NS', name: 'RMC Switchgears Limited' },
  { symbol: 'RMDRIP.NS', name: 'R M Drip and Sprinklers Systems Limited' },
  { symbol: 'RML.NS', name: 'Rane (Madras) Limited' },
  { symbol: 'RNBDENIMS.NS', name: 'R&B Denims Limited' },
  { symbol: 'ROHLTD.NS', name: 'Royal Orchid Hotels Limited' },
  { symbol: 'ROLEXRINGS.NS', name: 'Rolex Rings Limited' },
  { symbol: 'ROLLT.NS', name: 'Rollatainers Limited' },
  { symbol: 'ROML.NS', name: 'Raj Oil Mills Limited' },
  { symbol: 'ROSSARI.NS', name: 'Rossari Biotech Limited' },
  { symbol: 'ROSSELLIND.NS', name: 'Rossell India Limited' },
  { symbol: 'ROSSTECH.NS', name: 'Rossell Techsys Limited' },
  { symbol: 'ROTO.NS', name: 'Roto Pumps Limited' },
  { symbol: 'ROUTE.NS', name: 'ROUTE MOBILE LIMITED' },
  { symbol: 'RPEL.NS', name: 'Raghav Productivity Enhancers Limited' },
  { symbol: 'RPGLIFE.NS', name: 'RPG Life Sciences Limited' },
  { symbol: 'RPOWER.NS', name: 'Reliance Power Limited' },
  { symbol: 'RPPINFRA.NS', name: 'R.P.P. Infra Projects Limited' },
  { symbol: 'RPPL.NS', name: 'Rajshree Polypack Limited' },
  { symbol: 'RPSGVENT.NS', name: 'RPSG VENTURES LIMITED' },
  { symbol: 'RPTECH.NS', name: 'Rashi Peripherals Limited' },
  { symbol: 'RRIL.NS', name: 'RRIL Limited' },
  { symbol: 'RRKABEL.NS', name: 'R R Kabel Limited' },
  { symbol: 'RSDFIN.NS', name: 'RSD Finance Limited' },
  { symbol: 'RSL.NS', name: 'Rajputana Stainless Limited' },
  { symbol: 'RSSOFTWARE.NS', name: 'R. S. Software (India) Limited' },
  { symbol: 'RSWM.NS', name: 'RSWM Limited' },
  { symbol: 'RSYSTEMS.NS', name: 'R Systems International Limited' },
  { symbol: 'RTNINDIA.NS', name: 'RattanIndia Enterprises Limited' },
  { symbol: 'RTNPOWER.NS', name: 'RattanIndia Power Limited' },
  { symbol: 'RUBFILA.NS', name: 'Rubfila International Limited' },
  { symbol: 'RUBICON.NS', name: 'Rubicon Research Limited' },
  { symbol: 'RUBYMILLS.NS', name: 'The Ruby Mills Limited' },
  { symbol: 'RUCHINFRA.NS', name: 'Ruchi Infrastructure Limited' },
  { symbol: 'RUCHIRA.NS', name: 'Ruchira Papers Limited' },
  { symbol: 'RUDRA.NS', name: 'Rudra Global Infra Products Limited' },
  { symbol: 'RUPA.NS', name: 'Rupa & Company Limited' },
  { symbol: 'RUSHIL.NS', name: 'Rushil Decor Limited' },
  { symbol: 'RUSTOMJEE.NS', name: 'Keystone Realtors Limited' },
  { symbol: 'RVHL.NS', name: 'Ravinder Heights Limited' },
  { symbol: 'RVNL.NS', name: 'Rail Vikas Nigam Limited' },
  { symbol: 'RVTH.NS', name: 'Revathi Equipment India Limited' },
  { symbol: 'S&SPOWER.NS', name: 'S&S Power Switchgears Limited' },
  { symbol: 'SAATVIKGL.NS', name: 'Saatvik Green Energy Limited' },
  { symbol: 'SADBHAV.NS', name: 'Sadbhav Engineering Limited' },
  { symbol: 'SADBHIN.NS', name: 'Sadbhav Infrastructure Project Limited' },
  { symbol: 'SADHNANIQ.NS', name: 'Sadhana Nitrochem Limited' },
  { symbol: 'SAFARI.NS', name: 'Safari Industries (India) Limited' },
  { symbol: 'SAGARDEEP.NS', name: 'Sagardeep Alloys Limited' },
  { symbol: 'SAGCEM.NS', name: 'Sagar Cements Limited' },
  { symbol: 'SAGILITY.NS', name: 'SAGILITY LIMITED' },
  { symbol: 'SAHLIBHFI.NS', name: 'Shalibhadra Finance Limited' },
  { symbol: 'SAHYADRI.NS', name: 'Sahyadri Industries Limited' },
  { symbol: 'SAIL.NS', name: 'Steel Authority of India Limited' },
  { symbol: 'SAILIFE.NS', name: 'Sai Life Sciences Limited' },
  { symbol: 'SAIPARENT.NS', name: 'Sai Parenterals Limited' },
  { symbol: 'SAKAR.NS', name: 'Sakar Healthcare Limited' },
  { symbol: 'SAKHTISUG.NS', name: 'Sakthi Sugars Limited' },
  { symbol: 'SAKSOFT.NS', name: 'Saksoft Limited' },
  { symbol: 'SAKUMA.NS', name: 'Sakuma Exports Limited' },
  { symbol: 'SALASAR.NS', name: 'Salasar Techno Engineering Limited' },
  { symbol: 'SALONA.NS', name: 'Salona Cotspin Limited' },
  { symbol: 'SALSTEEL.NS', name: 'S.A.L. Steel Limited' },
  { symbol: 'SALZERELEC.NS', name: 'Salzer Electronics Limited' },
  { symbol: 'SAMBHAAV.NS', name: 'Sambhaav Media Limited' },
  { symbol: 'SAMBHV.NS', name: 'Sambhv Steel Tubes Limited' },
  { symbol: 'SAMHI.NS', name: 'Samhi Hotels Limited' },
  { symbol: 'SAMMAANCAP.NS', name: 'Sammaan Capital Limited' },
  { symbol: 'SAMPANN.NS', name: 'Sampann Utpadan India Limited' },
  { symbol: 'SANATHAN.NS', name: 'Sanathan Textiles Limited' },
  { symbol: 'SANCO.NS', name: 'Sanco Industries Limited' },
  { symbol: 'SANDESH.NS', name: 'The Sandesh Limited' },
  { symbol: 'SANDHAR.NS', name: 'Sandhar Technologies Limited' },
  { symbol: 'SANDUMA.NS', name: 'Sandur Manganese & Iron Ores Limited' },
  { symbol: 'SANGAMIND.NS', name: 'Sangam (India) Limited' },
  { symbol: 'SANGHVIMOV.NS', name: 'Sanghvi Movers Limited' },
  { symbol: 'SANGINITA.NS', name: 'Sanginita Chemicals Limited' },
  { symbol: 'SANOFI.NS', name: 'Sanofi India Limited' },
  { symbol: 'SANOFICONR.NS', name: 'Sanofi Consumer Healthcare India Limited' },
  { symbol: 'SANSERA.NS', name: 'Sansera Engineering Limited' },
  { symbol: 'SANSTAR.NS', name: 'Sanstar Limited' },
  { symbol: 'SANWARIA.NS', name: 'Sanwaria Consumer Limited' },
  { symbol: 'SAPPHIRE.NS', name: 'Sapphire Foods India Limited' },
  { symbol: 'SAPPL.NS', name: 'Shree Ajit Pulp & Paper Limited' },
  { symbol: 'SARDAEN.NS', name: 'Sarda Energy & Minerals Limited' },
  { symbol: 'SAREGAMA.NS', name: 'Saregama India Limited' },
  { symbol: 'SARLAPOLY.NS', name: 'Sarla Performance Fibers Limited' },
  { symbol: 'SARVESHWAR.NS', name: 'Sarveshwar Foods Limited' },
  { symbol: 'SASKEN.NS', name: 'Sasken Technologies Limited' },
  { symbol: 'SATIA.NS', name: 'Satia Industries Limited' },
  { symbol: 'SATIN.NS', name: 'Satin Creditcare Network Limited' },
  { symbol: 'SAURASHCEM.NS', name: 'Saurashtra Cement Limited' },
  { symbol: 'SAYAJIHOTL.NS', name: 'Sayaji Hotels Limited' },
  { symbol: 'SBC.NS', name: 'SBC Exports Limited' },
  { symbol: 'SBCL.NS', name: 'Shivalik Bimetal Controls Limited' },
  { symbol: 'SBFC.NS', name: 'SBFC Finance Limited' },
  { symbol: 'SBGLP.NS', name: 'Suratwwala Business Group Limited' },
  { symbol: 'SBICARD.NS', name: 'SBI Cards and Payment Services Limited' },
  { symbol: 'SBILIFE.NS', name: 'SBI Life Insurance Company Limited' },
  { symbol: 'SBIN.NS', name: 'State Bank of India' },
  { symbol: 'SCANSTL.NS', name: 'Scan Steels Limited' },
  { symbol: 'SCHAEFFLER.NS', name: 'Schaeffler India Limited' },
  { symbol: 'SCHAND.NS', name: 'S Chand And Company Limited' },
  { symbol: 'SCHNEIDER.NS', name: 'Schneider Electric Infrastructure Limited' },
  { symbol: 'SCI.NS', name: 'Shipping Corporation Of India Limited' },
  { symbol: 'SCILAL.NS', name: 'Shipping Corporation of India Land and Assets Limited' },
  { symbol: 'SCODATUBES.NS', name: 'Scoda Tubes Limited' },
  { symbol: 'SCPL.NS', name: 'Sheetal Cool Products Limited' },
  { symbol: 'SDBL.NS', name: 'Som Distilleries & Breweries Limited' },
  { symbol: 'SEAMECLTD.NS', name: 'Seamec Limited' },
  { symbol: 'SECMARK.NS', name: 'SecMark Consultancy Limited' },
  { symbol: 'SECURKLOUD.NS', name: 'SECUREKLOUD TECHNOLOGIES LIMITED' },
  { symbol: 'SEDEMAC.NS', name: 'SEDEMAC Mechatronics Limited' },
  { symbol: 'SEIL.NS', name: 'Shanti Educational Initiatives Limited' },
  { symbol: 'SEJALLTD.NS', name: 'Sejal Glass Limited' },
  { symbol: 'SELMC.NS', name: 'SEL Manufacturing Company Limited' },
  { symbol: 'SEMAC.NS', name: 'Semac Construction Limited' },
  { symbol: 'SENCO.NS', name: 'Senco Gold Limited' },
  { symbol: 'SENORES.NS', name: 'Senores Pharmaceuticals Limited' },
  { symbol: 'SEPC.NS', name: 'SEPC Limited' },
  { symbol: 'SERVOTECH.NS', name: 'Servotech Renewable Power System Limited' },
  { symbol: 'SESHAPAPER.NS', name: 'Seshasayee Paper and Boards Limited' },
  { symbol: 'SETCO.NS', name: 'Setco Automotive Limited' },
  { symbol: 'SETL.NS', name: 'Standard Engineering Technology Limited' },
  { symbol: 'SEYAIND.NS', name: 'Seya Industries Limited' },
  { symbol: 'SFL.NS', name: 'Sheela Foam Limited' },
  { symbol: 'SGFIN.NS', name: 'SG Finserve Limited' },
  { symbol: 'SGIL.NS', name: 'Synergy Green Industries Limited' },
  { symbol: 'SGL.NS', name: 'STL Global Limited' },
  { symbol: 'SGMART.NS', name: 'SG Mart Limited' },
  { symbol: 'SHADOWFAX.NS', name: 'Shadowfax Technologies Limited' },
  { symbol: 'SHAH.NS', name: 'Shah Metacorp Limited' },
  { symbol: 'SHAHALLOYS.NS', name: 'Shah Alloys Limited' },
  { symbol: 'SHAILY.NS', name: 'Shaily Engineering Plastics Limited' },
  { symbol: 'SHAKTIPUMP.NS', name: 'Shakti Pumps (India) Limited' },
  { symbol: 'SHALBY.NS', name: 'Shalby Limited' },
  { symbol: 'SHALPAINTS.NS', name: 'Shalimar Paints Limited' },
  { symbol: 'SHANKARA.NS', name: 'Shankara Building Products Limited' },
  { symbol: 'SHANTI.NS', name: 'Shanti Overseas (India) Limited' },
  { symbol: 'SHANTIGEAR.NS', name: 'Shanthi Gears Limited' },
  { symbol: 'SHANTIGOLD.NS', name: 'Shanti Gold International Limited' },
  { symbol: 'SHARDACROP.NS', name: 'Sharda Cropchem Limited' },
  { symbol: 'SHARDAMOTR.NS', name: 'Sharda Motor Industries Limited' },
  { symbol: 'SHARDUL.NS', name: 'Shardul Securities Limited' },
  { symbol: 'SHAREINDIA.NS', name: 'Share India Securities Limited' },
  { symbol: 'SHBAJRG.NS', name: 'Shri Bajrang Alliance Limited' },
  { symbol: 'SHEKHAWATI.NS', name: 'Shekhawati Industries Limited' },
  { symbol: 'SHEMAROO.NS', name: 'Shemaroo Entertainment Limited' },
  { symbol: 'SHILCTECH.NS', name: 'Shilchar Technologies Limited' },
  { symbol: 'SHILPAMED.NS', name: 'Shilpa Medicare Limited' },
  { symbol: 'SHINDL.NS', name: 'Sharat Industries Limited' },
  { symbol: 'SHIVALIK.NS', name: 'Shivalik Rasayan Limited' },
  { symbol: 'SHIVAMAUTO.NS', name: 'Shivam Autotech Limited' },
  { symbol: 'SHIVAMILLS.NS', name: 'Shiva Mills Limited' },
  { symbol: 'SHIVATEX.NS', name: 'Shiva Texyarn Limited' },
  { symbol: 'SHIVAUM.NS', name: 'Shiv Aum Steels Limited' },
  { symbol: 'SHK.NS', name: 'S H Kelkar and Company Limited' },
  { symbol: 'SHOPERSTOP.NS', name: 'Shoppers Stop Limited' },
  { symbol: 'SHRADHA.NS', name: 'Shradha Realty Limited' },
  { symbol: 'SHREDIGCEM.NS', name: 'Shree Digvijay Cement Co.Ltd' },
  { symbol: 'SHREECEM.NS', name: 'SHREE CEMENT LIMITED' },
  { symbol: 'SHREEJISPG.NS', name: 'Shreeji Shipping Global Limited' },
  { symbol: 'SHREEPUSHK.NS', name: 'Shree Pushkar Chemicals & Fertilisers Limited' },
  { symbol: 'SHREERAMA.NS', name: 'Shree Rama Multi-Tech Limited' },
  { symbol: 'SHRENIK.NS', name: 'Shrenik Limited' },
  { symbol: 'SHREYANIND.NS', name: 'Shreyans Industries Limited' },
  { symbol: 'SHRIKRISH.NS', name: 'Shri Krishna Devcon Limited' },
  { symbol: 'SHRINGARMS.NS', name: 'Shringar House of Mangalsutra Limited' },
  { symbol: 'SHRIPISTON.NS', name: 'Shriram Pistons & Rings Limited' },
  { symbol: 'SHRIRAMFIN.NS', name: 'Shriram Finance Limited' },
  { symbol: 'SHRIRAMPPS.NS', name: 'Shriram Properties Limited' },
  { symbol: 'SHYAMCENT.NS', name: 'Shyam Century Ferrous Limited' },
  { symbol: 'SHYAMMETL.NS', name: 'Shyam Metalics and Energy Limited' },
  { symbol: 'SHYAMTEL.NS', name: 'Shyam Telecom Limited' },
  { symbol: 'SICAGEN.NS', name: 'Sicagen India Limited' },
  { symbol: 'SICALLOG.NS', name: 'Sical Logistics Limited' },
  { symbol: 'SIEMENS.NS', name: 'Siemens Limited' },
  { symbol: 'SIGACHI.NS', name: 'Sigachi Industries Limited' },
  { symbol: 'SIGIND.NS', name: 'Signet Industries Limited' },
  { symbol: 'SIGMA.NS', name: 'Sigma Solve Limited' },
  { symbol: 'SIGMAADV.NS', name: 'SIGMA ADVANCED SYSTEMS LIMITED' },
  { symbol: 'SIGNATURE.NS', name: 'Signatureglobal (India) Limited' },
  { symbol: 'SIGNPOST.NS', name: 'Signpost India Limited' },
  { symbol: 'SIKA.NS', name: 'Sika Interplant Systems Limited' },
  { symbol: 'SIKKO.NS', name: 'Sikko Industries Limited' },
  { symbol: 'SIL.NS', name: 'Standard Industries Limited' },
  { symbol: 'SILGO.NS', name: 'Silgo Retail Limited' },
  { symbol: 'SILINV.NS', name: 'SIL Investments Limited' },
  { symbol: 'SILLYMONKS.NS', name: 'Silly Monks Entertainment Limited' },
  { symbol: 'SILVERTUC.NS', name: 'Silver Touch Technologies Limited' },
  { symbol: 'SIMPLEXINF.NS', name: 'Simplex Infrastructures Limited' },
  { symbol: 'SINCLAIR.NS', name: 'Sinclairs Hotels Limited' },
  { symbol: 'SINDHUTRAD.NS', name: 'Sindhu Trade Links Limited' },
  { symbol: 'SINGERIND.NS', name: 'Singer India Limited' },
  { symbol: 'SINTERCOM.NS', name: 'Sintercom India Limited' },
  { symbol: 'SIRCA.NS', name: 'Sirca Paints India Limited' },
  { symbol: 'SIS.NS', name: 'SIS LIMITED' },
  { symbol: 'SITINET.NS', name: 'Siti Networks Limited' },
  { symbol: 'SIYSIL.NS', name: 'Siyaram Silk Mills Limited' },
  { symbol: 'SJS.NS', name: 'S.J.S. Enterprises Limited' },
  { symbol: 'SJVN.NS', name: 'SJVN Limited' },
  { symbol: 'SKFINDIA.NS', name: 'SKF India Limited' },
  { symbol: 'SKFINDUS.NS', name: 'SKF India (Industrial) Limited' },
  { symbol: 'SKIPPER.NS', name: 'Skipper Limited' },
  { symbol: 'SKMEGGPROD.NS', name: 'SKM Egg Products Export (India) Limited' },
  { symbol: 'SKYGOLD.NS', name: 'SKY GOLD AND DIAMONDS LIMITED' },
  { symbol: 'SMARTLINK.NS', name: 'Smartlink Holdings Limited' },
  { symbol: 'SMARTWORKS.NS', name: 'Smartworks Coworking Spaces Limited' },
  { symbol: 'SMCGLOBAL.NS', name: 'SMC Global Securities Limited' },
  { symbol: 'SMLMAH.NS', name: 'SML Mahindra Limited' },
  { symbol: 'SMLT.NS', name: 'Sarthak Metals Limited' },
  { symbol: 'SMSPHARMA.NS', name: 'SMS Pharmaceuticals Limited' },
  { symbol: 'SNOWMAN.NS', name: 'Snowman Logistics Limited' },
  { symbol: 'SOBHA.NS', name: 'Sobha Limited' },
  { symbol: 'SOFTTECH.NS', name: 'Softtech Engineers Limited' },
  { symbol: 'SOLARA.NS', name: 'Solara Active Pharma Sciences Limited' },
  { symbol: 'SOLARINDS.NS', name: 'Solar Industries India Limited' },
  { symbol: 'SOLARWORLD.NS', name: 'Solarworld Energy Solutions Limited' },
  { symbol: 'SOLEX.NS', name: 'Solex Energy Limited' },
  { symbol: 'SOMANYCERA.NS', name: 'Somany Ceramics Limited' },
  { symbol: 'SOMATEX.NS', name: 'Soma Textiles & Industries Limited' },
  { symbol: 'SOMICONVEY.NS', name: 'Somi Conveyor Beltings Limited' },
  { symbol: 'SONACOMS.NS', name: 'Sona BLW Precision Forgings Limited' },
  { symbol: 'SONAL.NS', name: 'Sonal Mercantile Limited' },
  { symbol: 'SONAMLTD.NS', name: 'SONAM LIMITED' },
  { symbol: 'SONATSOFTW.NS', name: 'Sonata Software Limited' },
  { symbol: 'SOTL.NS', name: 'Savita Oil Technologies Limited' },
  { symbol: 'SOUTHBANK.NS', name: 'The South Indian Bank Limited' },
  { symbol: 'SOUTHWEST.NS', name: 'South West Pinnacle Exploration Limited' },
  { symbol: 'SPAL.NS', name: 'S. P. Apparels Limited' },
  { symbol: 'SPANDANA.NS', name: 'Spandana Sphoorty Financial Limited' },
  { symbol: 'SPARC.NS', name: 'Sun Pharma Advanced Research Company Limited' },
  { symbol: 'SPCENET.NS', name: 'Spacenet Enterprises India Limited' },
  { symbol: 'SPECIALITY.NS', name: 'Speciality Restaurants Limited' },
  { symbol: 'SPECTRUM.NS', name: 'Spectrum Electrical Industries Limited' },
  { symbol: 'SPENCERS.NS', name: 'Spencer\'s Retail Limited' },
  { symbol: 'SPIC.NS', name: 'Southern Petrochemicals Industries Corporation  Limited' },
  { symbol: 'SPLIL.NS', name: 'SPL Industries Limited' },
  { symbol: 'SPLPETRO.NS', name: 'Supreme Petrochem Limited' },
  { symbol: 'SPMLINFRA.NS', name: 'SPML Infra Limited' },
  { symbol: 'SPORTKING.NS', name: 'Sportking India Limited' },
  { symbol: 'SRD.NS', name: 'Shankar Lal Rampal Dye-Chem Limited' },
  { symbol: 'SREEL.NS', name: 'Sreeleathers Limited' },
  { symbol: 'SRF.NS', name: 'SRF Limited' },
  { symbol: 'SRGHFL.NS', name: 'SRG Housing Finance Limited' },
  { symbol: 'SRHHYPOLTD.NS', name: 'Sree Rayalaseema Hi-Strength Hypo Limited' },
  { symbol: 'SRM.NS', name: 'SRM Contractors Limited' },
  { symbol: 'SRTL.NS', name: 'Shree Ram Twistex Limited' },
  { symbol: 'SSDL.NS', name: 'Saraswati Saree Depot Limited' },
  { symbol: 'SSWL.NS', name: 'Steel Strips Wheels Limited' },
  { symbol: 'STALLION.NS', name: 'Stallion India Fluorochemicals Limited' },
  { symbol: 'STANLEY.NS', name: 'Stanley Lifestyles Limited' },
  { symbol: 'STAR.NS', name: 'Strides Pharma Science Limited' },
  { symbol: 'STARCEMENT.NS', name: 'Star Cement Limited' },
  { symbol: 'STARHEALTH.NS', name: 'Star Health and Allied Insurance Company Limited' },
  { symbol: 'STARPAPER.NS', name: 'Star Paper Mills Limited' },
  { symbol: 'STARTECK.NS', name: 'Starteck Finance Limited' },
  { symbol: 'STCINDIA.NS', name: 'The State Trading Corporation of India Limited' },
  { symbol: 'STEELCAS.NS', name: 'Steelcast Limited' },
  { symbol: 'STEELCITY.NS', name: 'Steel City Securities Limited' },
  { symbol: 'STEELXIND.NS', name: 'STEEL EXCHANGE INDIA LIMITED' },
  { symbol: 'STEL.NS', name: 'Stel Holdings Limited' },
  { symbol: 'STERTOOLS.NS', name: 'Sterling Tools Limited' },
  { symbol: 'STLNETWORK.NS', name: 'STL Networks Limited' },
  { symbol: 'STLTECH.NS', name: 'Sterlite Technologies Limited' },
  { symbol: 'STOVEKRAFT.NS', name: 'Stove Kraft Limited' },
  { symbol: 'STUDDS.NS', name: 'Studds Accessories Limited' },
  { symbol: 'STYL.NS', name: 'Seshaasai Technologies Limited' },
  { symbol: 'STYLAMIND.NS', name: 'Stylam Industries Limited' },
  { symbol: 'STYLEBAAZA.NS', name: 'Baazar Style Retail Limited' },
  { symbol: 'STYRENIX.NS', name: 'Styrenix Performance Materials Limited' },
  { symbol: 'SUBEXLTD.NS', name: 'Subex Limited' },
  { symbol: 'SUBROS.NS', name: 'Subros Limited' },
  { symbol: 'SUDARCOLOR.NS', name: 'Sudarshan Colorants India Limited' },
  { symbol: 'SUDARSCHEM.NS', name: 'Sudarshan Chemical Industries Limited' },
  { symbol: 'SUDEEPPHRM.NS', name: 'Sudeep Pharma Limited' },
  { symbol: 'SUKHJITS.NS', name: 'Sukhjit Starch & Chemicals Limited' },
  { symbol: 'SULA.NS', name: 'Sula Vineyards Limited' },
  { symbol: 'SUMEETINDS.NS', name: 'Sumeet Industries Limited' },
  { symbol: 'SUMICHEM.NS', name: 'Sumitomo Chemical India Limited' },
  { symbol: 'SUMIT.NS', name: 'Sumit Woods Limited' },
  { symbol: 'SUMMITSEC.NS', name: 'Summit Securities Limited' },
  { symbol: 'SUNCLAY.NS', name: 'Sundaram Clayton Limited' },
  { symbol: 'SUNDARAM.NS', name: 'Sundaram Multi Pap Limited' },
  { symbol: 'SUNDARMFIN.NS', name: 'Sundaram Finance Limited' },
  { symbol: 'SUNDRMBRAK.NS', name: 'Sundaram Brake Linings Limited' },
  { symbol: 'SUNDRMFAST.NS', name: 'Sundram Fasteners Limited' },
  { symbol: 'SUNDROP.NS', name: 'Sundrop Brands Limited' },
  { symbol: 'SUNFLAG.NS', name: 'Sunflag Iron And Steel Company Limited' },
  { symbol: 'SUNPHARMA.NS', name: 'Sun Pharmaceutical Industries Limited' },
  { symbol: 'SUNTECK.NS', name: 'Sunteck Realty Limited' },
  { symbol: 'SUNTV.NS', name: 'Sun TV Network Limited' },
  { symbol: 'SUPERHOUSE.NS', name: 'Superhouse Limited' },
  { symbol: 'SUPERSPIN.NS', name: 'Super Spinning Mills Limited' },
  { symbol: 'SUPRAJIT.NS', name: 'Suprajit Engineering Limited' },
  { symbol: 'SUPREME.NS', name: 'Supreme Holdings & Hospitality (India) Limited' },
  { symbol: 'SUPREMEENG.NS', name: 'Supreme Engineering Limited' },
  { symbol: 'SUPREMEIND.NS', name: 'Supreme Industries Limited' },
  { symbol: 'SUPREMEINF.NS', name: 'Supreme Infrastructure India Limited' },
  { symbol: 'SUPRIYA.NS', name: 'Supriya Lifescience Limited' },
  { symbol: 'SURAJEST.NS', name: 'Suraj Estate Developers Limited' },
  { symbol: 'SURAJLTD.NS', name: 'Suraj Limited' },
  { symbol: 'SURAKSHA.NS', name: 'Suraksha Diagnostic Limited' },
  { symbol: 'SURANASOL.NS', name: 'Surana Solar Limited' },
  { symbol: 'SURANAT&P.NS', name: 'Surana Telecom and Power Limited' },
  { symbol: 'SURYALA.NS', name: 'Suryalata Spinning Mills Limited' },
  { symbol: 'SURYALAXMI.NS', name: 'Suryalakshmi Cotton Mills Limited' },
  { symbol: 'SURYAROSNI.NS', name: 'Surya Roshni Limited' },
  { symbol: 'SURYODAY.NS', name: 'Suryoday Small Finance Bank Limited' },
  { symbol: 'SUTLEJTEX.NS', name: 'Sutlej Textiles and Industries Limited' },
  { symbol: 'SUVEN.NS', name: 'Suven Life Sciences Limited' },
  { symbol: 'SUVIDHAA.NS', name: 'Suvidhaa Infoserve Limited' },
  { symbol: 'SUYOG.NS', name: 'Suyog Telematics Limited' },
  { symbol: 'SUZLON.NS', name: 'Suzlon Energy Limited' },
  { symbol: 'SVLL.NS', name: 'Shree Vasu Logistics Limited' },
  { symbol: 'SVPGLOB.NS', name: 'SVP GLOBAL TEXTILES LIMITED' },
  { symbol: 'SWANCORP.NS', name: 'SWAN CORP LIMITED' },
  { symbol: 'SWANDEF.NS', name: 'Swan Defence and Heavy Industries Limited' },
  { symbol: 'SWARAJENG.NS', name: 'Swaraj Engines Limited' },
  { symbol: 'SWELECTES.NS', name: 'Swelect Energy Systems Limited' },
  { symbol: 'SWIGGY.NS', name: 'Swiggy Limited' },
  { symbol: 'SWSOLAR.NS', name: 'Sterling and Wilson Renewable Energy Limited' },
  { symbol: 'SYMPHONY.NS', name: 'Symphony Limited' },
  { symbol: 'SYNCOMF.NS', name: 'Syncom Formulations (India) Limited' },
  { symbol: 'SYNGENE.NS', name: 'Syngene International Limited' },
  { symbol: 'SYRMA.NS', name: 'Syrma SGS Technology Limited' },
  { symbol: 'SYSTMTXC.NS', name: 'Systematix Corporate Services Limited' },
  { symbol: 'TAALTECH.NS', name: 'Taal Tech Limited' },
  { symbol: 'TAINWALCHM.NS', name: 'Tainwala Chemical and Plastic (I) Limited' },
  { symbol: 'TAJGVK.NS', name: 'Taj GVK Hotels & Resorts Limited' },
  { symbol: 'TAKE.NS', name: 'Take Solutions Limited' },
  { symbol: 'TALBROAUTO.NS', name: 'Talbros Automotive Components Limited' },
  { symbol: 'TAMBOLIIN.NS', name: 'Tamboli Industries Limited' },
  { symbol: 'TANLA.NS', name: 'Tanla Platforms Limited' },
  { symbol: 'TARACHAND.NS', name: 'Tara Chand InfraLogistic Solutions Limited' },
  { symbol: 'TARAPUR.NS', name: 'Tarapur Transformers Limited' },
  { symbol: 'TARC.NS', name: 'TARC Limited' },
  { symbol: 'TARIL.NS', name: 'Transformers And Rectifiers (India) Limited' },
  { symbol: 'TARMAT.NS', name: 'Tarmat Limited' },
  { symbol: 'TARSONS.NS', name: 'Tarsons Products Limited' },
  { symbol: 'TASTYBITE.NS', name: 'Tasty Bite Eatables Limited' },
  { symbol: 'TATACAP.NS', name: 'Tata Capital Limited' },
  { symbol: 'TATACHEM.NS', name: 'Tata Chemicals Limited' },
  { symbol: 'TATACOMM.NS', name: 'Tata Communications Limited' },
  { symbol: 'TATACONSUM.NS', name: 'TATA CONSUMER PRODUCTS LIMITED' },
  { symbol: 'TATAELXSI.NS', name: 'Tata Elxsi Limited' },
  { symbol: 'TATAINVEST.NS', name: 'Tata Investment Corporation Limited' },
  { symbol: 'TATAPOWER.NS', name: 'Tata Power Company Limited' },
  { symbol: 'TATASTEEL.NS', name: 'Tata Steel Limited' },
  { symbol: 'TATATECH.NS', name: 'Tata Technologies Limited' },
  { symbol: 'TATVA.NS', name: 'Tatva Chintan Pharma Chem Limited' },
  { symbol: 'TBOTEK.NS', name: 'TBO Tek Limited' },
  { symbol: 'TBZ.NS', name: 'Tribhovandas Bhimji Zaveri Limited' },
  { symbol: 'TCC.NS', name: 'TCC Concept Limited' },
  { symbol: 'TCI.NS', name: 'Transport Corporation of India Limited' },
  { symbol: 'TCIEXP.NS', name: 'TCI Express Limited' },
  { symbol: 'TCIFINANCE.NS', name: 'TCI Finance Limited' },
  { symbol: 'TCPLPACK.NS', name: 'TCPL Packaging Limited' },
  { symbol: 'TCS.NS', name: 'Tata Consultancy Services Limited' },
  { symbol: 'TDPOWERSYS.NS', name: 'TD Power Systems Limited' },
  { symbol: 'TEAMGTY.NS', name: 'Team India Guaranty Limited' },
  { symbol: 'TEAMLEASE.NS', name: 'Teamlease Services Limited' },
  { symbol: 'TECHM.NS', name: 'Tech Mahindra Limited' },
  { symbol: 'TECHNOE.NS', name: 'Techno Electric & Engineering Company Limited' },
  { symbol: 'TECHNVISN.NS', name: 'TechNVision Ventures Limited' },
  { symbol: 'TECILCHEM.NS', name: 'TECIL Chemicals and Hydro Power Limited' },
  { symbol: 'TEGA.NS', name: 'Tega Industries Limited' },
  { symbol: 'TEJASNET.NS', name: 'Tejas Networks Limited' },
  { symbol: 'TEMBO.NS', name: 'Tembo Global Industries Limited' },
  { symbol: 'TENNIND.NS', name: 'Tenneco Clean Air India Limited' },
  { symbol: 'TERASOFT.NS', name: 'Tera Software Limited' },
  { symbol: 'TEXINFRA.NS', name: 'Texmaco Infrastructure & Holdings Limited' },
  { symbol: 'TEXMOPIPES.NS', name: 'Texmo Pipes and Products Limited' },
  { symbol: 'TEXRAIL.NS', name: 'Texmaco Rail & Engineering Limited' },
  { symbol: 'TFCILTD.NS', name: 'Tourism Finance Corporation of India Limited' },
  { symbol: 'TFL.NS', name: 'Transwarranty Finance Limited' },
  { symbol: 'TGBHOTELS.NS', name: 'TGB Banquets And Hotels Limited' },
  { symbol: 'THACKER.NS', name: 'Thacker & Company Limited' },
  { symbol: 'THAKDEV.NS', name: 'Thakkers Developers Limited' },
  { symbol: 'THANGAMAYL.NS', name: 'Thangamayil Jewellery Limited' },
  { symbol: 'THEINVEST.NS', name: 'The Investment Trust Of India Limited' },
  { symbol: 'THEJO.NS', name: 'Thejo Engineering Limited' },
  { symbol: 'THELEELA.NS', name: 'Leela Palaces Hotels & Resorts Limited' },
  { symbol: 'THEMISMED.NS', name: 'Themis Medicare Limited' },
  { symbol: 'THERMAX.NS', name: 'Thermax Limited' },
  { symbol: 'THOMASCOOK.NS', name: 'Thomas Cook  (India)  Limited' },
  { symbol: 'THOMASCOTT.NS', name: 'Thomas Scott (India) Limited' },
  { symbol: 'THYROCARE.NS', name: 'Thyrocare Technologies Limited' },
  { symbol: 'TI.NS', name: 'Tilaknagar Industries Limited' },
  { symbol: 'TICL.NS', name: 'Twamev Construction and Infrastructure Limited' },
  { symbol: 'TIGERLOGS.NS', name: 'Tiger Logistics (India) Limited' },
  { symbol: 'TIIL.NS', name: 'Technocraft Industries (India) Limited' },
  { symbol: 'TIINDIA.NS', name: 'Tube Investments of India Limited' },
  { symbol: 'TIJARIA.NS', name: 'Tijaria Polypipes Limited' },
  { symbol: 'TIL.NS', name: 'TIL Limited' },
  { symbol: 'TIMETECHNO.NS', name: 'Time Technoplast Limited' },
  { symbol: 'TIMEX.NS', name: 'Timex Group India Limited' },
  { symbol: 'TIMKEN.NS', name: 'Timken India Limited' },
  { symbol: 'TINNARUBR.NS', name: 'Tinna Rubber and Infrastructure Limited' },
  { symbol: 'TIPSFILMS.NS', name: 'Tips Films Limited' },
  { symbol: 'TIPSMUSIC.NS', name: 'Tips Music Limited' },
  { symbol: 'TIRUMALCHM.NS', name: 'Thirumalai Chemicals Limited' },
  { symbol: 'TIRUPATIFL.NS', name: 'Tirupati Forge Limited' },
  { symbol: 'TITAGARH.NS', name: 'TITAGARH RAIL SYSTEMS LIMITED' },
  { symbol: 'TITAN.NS', name: 'Titan Company Limited' },
  { symbol: 'TMB.NS', name: 'Tamilnad Mercantile Bank Limited' },
  { symbol: 'TMCV.NS', name: 'Tata Motors Limited' },
  { symbol: 'TMPV.NS', name: 'Tata Motors Passenger Vehicles Limited' },
  { symbol: 'TNPETRO.NS', name: 'Tamilnadu PetroProducts Limited' },
  { symbol: 'TNPL.NS', name: 'Tamil Nadu Newsprint & Papers Limited' },
  { symbol: 'TNTELE.NS', name: 'Tamilnadu Telecommunication Limited' },
  { symbol: 'TOKYOPLAST.NS', name: 'Tokyo Plast International Limited' },
  { symbol: 'TOLINS.NS', name: 'Tolins Tyres Limited' },
  { symbol: 'TORNTPHARM.NS', name: 'Torrent Pharmaceuticals Limited' },
  { symbol: 'TORNTPOWER.NS', name: 'Torrent Power Limited' },
  { symbol: 'TOTAL.NS', name: 'Total Transport Systems Limited' },
  { symbol: 'TOUCHWOOD.NS', name: 'Touchwood Entertainment Limited' },
  { symbol: 'TPHQ.NS', name: 'Teamo Productions HQ Limited' },
  { symbol: 'TPLPLASTEH.NS', name: 'TPL Plastech Limited' },
  { symbol: 'TRACXN.NS', name: 'Tracxn Technologies Limited' },
  { symbol: 'TRANSPEK.NS', name: 'Transpek Industry Limited' },
  { symbol: 'TRANSRAILL.NS', name: 'Transrail Lighting Limited' },
  { symbol: 'TRANSWORLD.NS', name: 'TRANSWORLD SHIPPING LINES LIMITED' },
  { symbol: 'TRAVELFOOD.NS', name: 'Travel Food Services Limited' },
  { symbol: 'TREEHOUSE.NS', name: 'Tree House Education & Accessories Limited' },
  { symbol: 'TREJHARA.NS', name: 'TREJHARA SOLUTIONS LIMITED' },
  { symbol: 'TREL.NS', name: 'Transindia Real Estate Limited' },
  { symbol: 'TRENT.NS', name: 'Trent Limited' },
  { symbol: 'TRF.NS', name: 'TRF Limited' },
  { symbol: 'TRIDENT.NS', name: 'Trident Limited' },
  { symbol: 'TRIGYN.NS', name: 'Trigyn Technologies Limited' },
  { symbol: 'TRITURBINE.NS', name: 'Triveni Turbine Limited' },
  { symbol: 'TRIVENI.NS', name: 'Triveni Engineering & Industries Limited' },
  { symbol: 'TRU.NS', name: 'TruCap Finance Limited' },
  { symbol: 'TRUALT.NS', name: 'TruAlt Bioenergy Limited' },
  { symbol: 'TSFINV.NS', name: 'TSF INVESTMENTS LIMITED' },
  { symbol: 'TTKHLTCARE.NS', name: 'TTK Healthcare Limited' },
  { symbol: 'TTKPRESTIG.NS', name: 'TTK Prestige Limited' },
  { symbol: 'TTL.NS', name: 'T T Limited' },
  { symbol: 'TTML.NS', name: 'Tata Teleservices (Maharashtra) Limited' },
  { symbol: 'TVSELECT.NS', name: 'TVS Electronics Limited' },
  { symbol: 'TVSHLTD.NS', name: 'TVS Holdings Limited' },
  { symbol: 'TVSMOTOR.NS', name: 'TVS Motor Company Limited' },
  { symbol: 'TVSSCS.NS', name: 'TVS Supply Chain Solutions Limited' },
  { symbol: 'TVSSRICHAK.NS', name: 'TVS Srichakra Limited' },
  { symbol: 'TVTODAY.NS', name: 'TV Today Network Limited' },
  { symbol: 'TVVISION.NS', name: 'TV Vision Limited' },
  { symbol: 'UBL.NS', name: 'United Breweries Limited' },
  { symbol: 'UCAL.NS', name: 'UCAL LIMITED' },
  { symbol: 'UCOBANK.NS', name: 'UCO Bank' },
  { symbol: 'UDS.NS', name: 'Updater Services Limited' },
  { symbol: 'UEL.NS', name: 'Ujaas Energy Limited' },
  { symbol: 'UFBL.NS', name: 'United Foodbrands Limited' },
  { symbol: 'UFLEX.NS', name: 'UFLEX Limited' },
  { symbol: 'UFO.NS', name: 'UFO Moviez India Limited' },
  { symbol: 'UGARSUGAR.NS', name: 'The Ugar Sugar Works Limited' },
  { symbol: 'UGROCAP.NS', name: 'Ugro Capital Limited' },
  { symbol: 'UJJIVANSFB.NS', name: 'Ujjivan Small Finance Bank Limited' },
  { symbol: 'ULTRACEMCO.NS', name: 'UltraTech Cement Limited' },
  { symbol: 'ULTRAMAR.NS', name: 'Ultramarine & Pigments Limited' },
  { symbol: 'UMAEXPORTS.NS', name: 'Uma Exports Limited' },
  { symbol: 'UMESLTD.NS', name: 'Usha Martin Education & Solutions Limited' },
  { symbol: 'UMIYA-MRO.NS', name: 'UMIYA BUILDCON LIMITED' },
  { symbol: 'UNICHEMLAB.NS', name: 'Unichem Laboratories Limited' },
  { symbol: 'UNIDT.NS', name: 'United Drilling Tools Limited' },
  { symbol: 'UNIECOM.NS', name: 'Unicommerce Esolutions Limited' },
  { symbol: 'UNIENTER.NS', name: 'Uniphos Enterprises Limited' },
  { symbol: 'UNIINFO.NS', name: 'Uniinfo Telecom Services Limited' },
  { symbol: 'UNIMECH.NS', name: 'Unimech Aerospace and Manufacturing Limited' },
  { symbol: 'UNIONBANK.NS', name: 'Union Bank of India' },
  { symbol: 'UNIPARTS.NS', name: 'Uniparts India Limited' },
  { symbol: 'UNITDSPR.NS', name: 'United Spirits Limited' },
  { symbol: 'UNITECH.NS', name: 'Unitech Limited' },
  { symbol: 'UNITEDPOLY.NS', name: 'United Polyfab Gujarat Limited' },
  { symbol: 'UNITEDTEA.NS', name: 'The United Nilgiri Tea Estates Company Limited' },
  { symbol: 'UNIVASTU.NS', name: 'Univastu India Limited' },
  { symbol: 'UNIVCABLES.NS', name: 'Universal Cables Limited' },
  { symbol: 'UNIVPHOTO.NS', name: 'Universus Photo Imagings Limited' },
  { symbol: 'UNOMINDA.NS', name: 'UNO Minda Limited' },
  { symbol: 'UPL.NS', name: 'UPL Limited' },
  { symbol: 'URAVIDEF.NS', name: 'Uravi Defence and Technology Limited' },
  { symbol: 'URBANCO.NS', name: 'Urban Company Limited' },
  { symbol: 'URJA.NS', name: 'Urja Global Limited' },
  { symbol: 'USHAMART.NS', name: 'Usha Martin Limited' },
  { symbol: 'USK.NS', name: 'Udayshivakumar Infra Limited' },
  { symbol: 'UTIAMC.NS', name: 'UTI Asset Management Company Limited' },
  { symbol: 'UTKARSHBNK.NS', name: 'Utkarsh Small Finance Bank Limited' },
  { symbol: 'UTLSOLAR.NS', name: 'Fujiyama Power Systems Limited' },
  { symbol: 'UTTAMSUGAR.NS', name: 'Uttam Sugar Mills Limited' },
  { symbol: 'UYFINCORP.NS', name: 'U. Y. Fincorp Limited' },
  { symbol: 'V2RETAIL.NS', name: 'V2 Retail Limited' },
  { symbol: 'VADILALIND.NS', name: 'Vadilal Industries Limited' },
  { symbol: 'VAIBHAVGBL.NS', name: 'Vaibhav Global Limited' },
  { symbol: 'VAISHALI.NS', name: 'Vaishali Pharma Limited' },
  { symbol: 'VAKRANGEE.NS', name: 'Vakrangee Limited' },
  { symbol: 'VALIANTLAB.NS', name: 'Valiant Laboratories Limited' },
  { symbol: 'VALIANTORG.NS', name: 'Valiant Organics Limited' },
  { symbol: 'VARDHACRLC.NS', name: 'Vardhman Acrylics Limited' },
  { symbol: 'VARDMNPOLY.NS', name: 'Vardhman Polytex Limited' },
  { symbol: 'VARROC.NS', name: 'Varroc Engineering Limited' },
  { symbol: 'VASCONEQ.NS', name: 'Vascon Engineers Limited' },
  { symbol: 'VASWANI.NS', name: 'Vaswani Industries Limited' },
  { symbol: 'VBL.NS', name: 'Varun Beverages Limited' },
  { symbol: 'VCL.NS', name: 'Vaxtex Cotfab Limited' },
  { symbol: 'VEDL.NS', name: 'Vedanta Limited' },
  { symbol: 'VEEDOL.NS', name: 'Veedol Corporation Limited' },
  { symbol: 'VELJAN.NS', name: 'Veljan Denison Limited' },
  { symbol: 'VENKEYS.NS', name: 'Venky\'s (India) Limited' },
  { symbol: 'VENTIVE.NS', name: 'Ventive Hospitality Limited' },
  { symbol: 'VENUSPIPES.NS', name: 'Venus Pipes & Tubes Limited' },
  { symbol: 'VENUSREM.NS', name: 'Venus Remedies Limited' },
  { symbol: 'VERANDA.NS', name: 'Veranda Learning Solutions Limited' },
  { symbol: 'VERTOZ.NS', name: 'Vertoz Limited' },
  { symbol: 'VESUVIUS.NS', name: 'Vesuvius India Limited' },
  { symbol: 'VETO.NS', name: 'Veto Switchgears And Cables Limited' },
  { symbol: 'VGL.NS', name: 'VARVEE GLOBAL LIMITED' },
  { symbol: 'VGUARD.NS', name: 'V-Guard Industries Limited' },
  { symbol: 'VHL.NS', name: 'Vardhman Holdings Limited' },
  { symbol: 'VHLTD.NS', name: 'Viceroy Hotels Limited' },
  { symbol: 'VIDHIING.NS', name: 'Vidhi Specialty Food Ingredients Limited' },
  { symbol: 'VIDYAWIRES.NS', name: 'Vidya Wires Limited' },
  { symbol: 'VIJAYA.NS', name: 'Vijaya Diagnostic Centre Limited' },
  { symbol: 'VIJIFIN.NS', name: 'Viji Finance Limited' },
  { symbol: 'VIKASECO.NS', name: 'Vikas EcoTech Limited' },
  { symbol: 'VIKASLIFE.NS', name: 'Vikas Lifecare Limited' },
  { symbol: 'VIKRAMSOLR.NS', name: 'Vikram Solar Limited' },
  { symbol: 'VIKRAN.NS', name: 'Vikran Engineering Limited' },
  { symbol: 'VIMTALABS.NS', name: 'Vimta Labs Limited' },
  { symbol: 'VINATIORGA.NS', name: 'Vinati Organics Limited' },
  { symbol: 'VINCOFE.NS', name: 'Vintage Coffee And Beverages Limited' },
  { symbol: 'VINDHYATEL.NS', name: 'Vindhya Telelinks Limited' },
  { symbol: 'VINEETLAB.NS', name: 'Vineet Laboratories Limited' },
  { symbol: 'VINNY.NS', name: 'Vinny Overseas Limited' },
  { symbol: 'VINYLINDIA.NS', name: 'Vinyl Chemicals (India) Limited' },
  { symbol: 'VIPCLOTHNG.NS', name: 'VIP Clothing Limited' },
  { symbol: 'VIPIND.NS', name: 'VIP Industries Limited' },
  { symbol: 'VIPULLTD.NS', name: 'Vipul Limited' },
  { symbol: 'VIRINCHI.NS', name: 'Virinchi Limited' },
  { symbol: 'VISACHROME.NS', name: 'VISA Chrome Limited' },
  { symbol: 'VISAKAIND.NS', name: 'Visaka Industries Limited' },
  { symbol: 'VISHNU.NS', name: 'Vishnu Chemicals Limited' },
  { symbol: 'VISHWARAJ.NS', name: 'Vishwaraj Sugar Industries Limited' },
  { symbol: 'VITAL.NS', name: 'Vital Chemtech Limited' },
  { symbol: 'VIVIDHA.NS', name: 'Visagar Polytex Limited' },
  { symbol: 'VIVIMEDLAB.NS', name: 'Vivimed Labs Limited' },
  { symbol: 'VIYASH.NS', name: 'Viyash Scientific Limited' },
  { symbol: 'VLEGOV.NS', name: 'VL E-Governance & IT Solutions Limited' },
  { symbol: 'VLSFINANCE.NS', name: 'VLS Finance Limited' },
  { symbol: 'VMART.NS', name: 'V-Mart Retail Limited' },
  { symbol: 'VMM.NS', name: 'Vishal Mega Mart Limited' },
  { symbol: 'VMSTMT.NS', name: 'VMS TMT Limited' },
  { symbol: 'VOLTAMP.NS', name: 'Voltamp Transformers Limited' },
  { symbol: 'VOLTAS.NS', name: 'Voltas Limited' },
  { symbol: 'VPRPL.NS', name: 'Vishnu Prakash R Punglia Limited' },
  { symbol: 'VRAJ.NS', name: 'Vraj Iron and Steel Limited' },
  { symbol: 'VRLLOG.NS', name: 'VRL Logistics Limited' },
  { symbol: 'VSSL.NS', name: 'Vardhman Special Steels Limited' },
  { symbol: 'VSTIND.NS', name: 'VST Industries Limited' },
  { symbol: 'VSTL.NS', name: 'Vibhor Steel Tubes Limited' },
  { symbol: 'VSTTILLERS.NS', name: 'V.S.T Tillers Tractors Limited' },
  { symbol: 'VTL.NS', name: 'Vardhman Textiles Limited' },
  { symbol: 'WAAREEENER.NS', name: 'Waaree Energies Limited' },
  { symbol: 'WAAREEINDO.NS', name: 'Indosolar Limited' },
  { symbol: 'WAAREERTL.NS', name: 'Waaree Renewable Technologies Limited' },
  { symbol: 'WABAG.NS', name: 'VA Tech Wabag Limited' },
  { symbol: 'WAKEFIT.NS', name: 'Wakefit Innovations Limited' },
  { symbol: 'WALCHANNAG.NS', name: 'Walchandnagar Industries Limited' },
  { symbol: 'WANBURY.NS', name: 'Wanbury Limited' },
  { symbol: 'WCIL.NS', name: 'Western Carriers (India) Limited' },
  { symbol: 'WEALTH.NS', name: 'Wealth First Portfolio Managers Limited' },
  { symbol: 'WEBELSOLAR.NS', name: 'Websol Energy System Limited' },
  { symbol: 'WEIZMANIND.NS', name: 'Weizmann Limited' },
  { symbol: 'WEL.NS', name: 'Wonder Electricals Limited' },
  { symbol: 'WELCORP.NS', name: 'Welspun Corp Limited' },
  { symbol: 'WELENT.NS', name: 'Welspun Enterprises Limited' },
  { symbol: 'WELINV.NS', name: 'Welspun Investments and Commercials Limited' },
  { symbol: 'WELSPLSOL.NS', name: 'Welspun Specialty Solutions Limited' },
  { symbol: 'WELSPUNLIV.NS', name: 'Welspun Living Limited' },
  { symbol: 'WENDT.NS', name: 'Wendt (India) Limited' },
  { symbol: 'WESTLIFE.NS', name: 'WESTLIFE FOODWORLD LIMITED' },
  { symbol: 'WEWIN.NS', name: 'WE WIN LIMITED' },
  { symbol: 'WEWORK.NS', name: 'WeWork India Management Limited' },
  { symbol: 'WHEELS.NS', name: 'Wheels India Limited' },
  { symbol: 'WHIRLPOOL.NS', name: 'Whirlpool of India Limited' },
  { symbol: 'WILLAMAGOR.NS', name: 'Williamson Magor & Company Limited' },
  { symbol: 'WIMPLAST.NS', name: 'Wim Plast Limited' },
  { symbol: 'WINDLAS.NS', name: 'Windlas Biotech Limited' },
  { symbol: 'WINDMACHIN.NS', name: 'Windsor Machines Limited' },
  { symbol: 'WINSOME.NS', name: 'Winsome Yarns Limited' },
  { symbol: 'WIPL.NS', name: 'The Western India Plywoods Limited' },
  { symbol: 'WIPRO.NS', name: 'Wipro Limited' },
  { symbol: 'WOCKPHARMA.NS', name: 'Wockhardt Limited' },
  { symbol: 'WONDERLA.NS', name: 'Wonderla Holidays Limited' },
  { symbol: 'WORTHPERI.NS', name: 'Worth Peripherals Limited' },
  { symbol: 'WPIL.NS', name: 'WPIL Limited' },
  { symbol: 'WSI.NS', name: 'W S Industries (I) Limited' },
  { symbol: 'WSTCSTPAPR.NS', name: 'West Coast Paper Mills Limited' },
  { symbol: 'XCHANGING.NS', name: 'Xchanging Solutions Limited' },
  { symbol: 'XELPMOC.NS', name: 'Xelpmoc Design And Tech Limited' },
  { symbol: 'XPROINDIA.NS', name: 'Xpro India Limited' },
  { symbol: 'XTGLOBAL.NS', name: 'Xtglobal Infotech Limited' },
  { symbol: 'YASHO.NS', name: 'Yasho Industries Limited' },
  { symbol: 'YATHARTH.NS', name: 'Yatharth Hospital & Trauma Care Services Limited' },
  { symbol: 'YATRA.NS', name: 'Yatra Online Limited' },
  { symbol: 'YESBANK.NS', name: 'Yes Bank Limited' },
  { symbol: 'YUKEN.NS', name: 'Yuken India Limited' },
  { symbol: 'ZAGGLE.NS', name: 'Zaggle Prepaid Ocean Services Limited' },
  { symbol: 'ZEEL.NS', name: 'Zee Entertainment Enterprises Limited' },
  { symbol: 'ZEELEARN.NS', name: 'Zee Learn Limited' },
  { symbol: 'ZEEMEDIA.NS', name: 'Zee Media Corporation Limited' },
  { symbol: 'ZENITHEXPO.NS', name: 'Zenith Exports Limited' },
  { symbol: 'ZENITHSTL.NS', name: 'Zenith Steel Pipes & Industries Limited' },
  { symbol: 'ZENSARTECH.NS', name: 'Zensar Technologies Limited' },
  { symbol: 'ZENTEC.NS', name: 'Zen Technologies Limited' },
  { symbol: 'ZFCVINDIA.NS', name: 'ZF Commercial Vehicle Control Systems India Limited' },
  { symbol: 'ZFSTEERING.NS', name: 'ZF Steering Gear (India) Limited' },
  { symbol: 'ZIMLAB.NS', name: 'Zim Laboratories Limited' },
  { symbol: 'ZODIAC.NS', name: 'Zodiac Energy Limited' },
  { symbol: 'ZODIACLOTH.NS', name: 'Zodiac Clothing Company Limited' },
  { symbol: 'ZOTA.NS', name: 'Zota Health Care LImited' },
  { symbol: 'ZSARACOM.NS', name: 'Saraswati Commercial India Limited' },
  { symbol: 'ZUARI.NS', name: 'Zuari Agro Chemicals Limited' },
  { symbol: 'ZUARIIND.NS', name: 'ZUARI INDUSTRIES LIMITED' },
  { symbol: 'ZYDUSLIFE.NS', name: 'Zydus Lifesciences Limited' },
  { symbol: 'ZYDUSWELL.NS', name: 'Zydus Wellness Limited' },
  { symbol: 'AMARAJABAT.BO', name: 'AMARA RAJA BATTERIES LTD.' },
  { symbol: 'HDFC.BO', name: 'HOUSING DEVELOPMENT FINANCE CORP.LTD.' },
  { symbol: 'ANDHRAPET.BO', name: 'ANDHRA PETROCHEMICALS LTD.' },
  { symbol: 'APPLEFIN.BO', name: 'APPLE FINANCE LTD.' },
  { symbol: 'ASSAMCO.BO', name: 'Assam Company (India) Limited' },
  { symbol: 'ATVPR.BO', name: 'ATV PROJECTS INDIA LTD.' },
  { symbol: 'AUTOLITIND.BO', name: 'AUTOLITE (INDIA) LTD.' },
  { symbol: 'AUTORIDFIN.BO', name: 'AUTORIDERS FINANCE LTD.' },
  { symbol: 'CENTURYTEX.BO', name: 'CENTURY TEXTILES & INDUSTRIES LTD.' },
  { symbol: 'BHAGGAS.BO', name: 'Bhagawati Gas Limited' },
  { symbol: 'BHUSANSTL.BO', name: 'BHUSHAN STEEL LTD.' },
  { symbol: 'ABCIL.BO', name: 'ADITYA BIRLA CHEMICALS (INDIA) LTD.' },
  { symbol: 'BIHSPONG.BO', name: 'BIHAR SPONGE IRON LTD.' },
  { symbol: 'BINANIIND.BO', name: 'BINANI INDUSTRIES LTD.' },
  { symbol: 'BIRLAERIC.BO', name: 'BIRLA ERICSSON OPTICAL LTD.' },
  { symbol: 'BNKCAP.BO', name: 'BNK CAPITAL MARKETS LTD.' },
  { symbol: 'CAMPHOR.BO', name: 'CAMPHOR & ALLIED PRODUCTS LTD.-$' },
  { symbol: 'CROMPGREAV.BO', name: 'CROMPTON GREAVES LTD.' },
  { symbol: 'BALLARPUR.BO', name: 'BALLARPUR INDUSTRIES LTD.' },
  { symbol: 'RELCAPITAL.BO', name: 'RELIANCE CAPITAL LTD.' },
  { symbol: 'MERCK.BO', name: 'MERCK LTD.' },
  { symbol: 'EMPEESUG.BO', name: 'EMPEE SUGARS & CHEMICALS LTD.' },
  { symbol: 'ESSAROIL.BO', name: 'ESSAR OIL LTD.' },
  { symbol: 'ESSELPRO.BO', name: 'ESSEL PROPACK LTD.' },
  { symbol: 'FEDDERLOYD.BO', name: 'FEDDERS LLOYD CORPORATION LTD.' },
  { symbol: 'FERROALL.BO', name: 'FERRO ALLOYS CORPORATION LTD.' },
  { symbol: 'FGP.BO', name: 'FGP LTD.' },
  { symbol: 'PHCAP.BO', name: 'PH CAPITAL LTD.' },
  { symbol: 'CMIFPE.BO', name: 'CMI FPE LTD.-$' },
  { symbol: 'GARDENSILK.BO', name: 'GARDEN SILK MILLS LTD.' },
  { symbol: 'PARRYSUGAR.BO', name: 'PARRYS SUGAR INDUSTRIES LTD.' },
  { symbol: 'GOODRICKE.BO', name: 'GOODRICKE GROUP LTD.' },
  { symbol: 'GTNINDS.BO', name: 'GTN INDUSTRIES LTD.' },
  { symbol: 'GUJFLUORO.BO', name: 'GUJARAT FLUOROCHEMICALS LTD.' },
  { symbol: 'HCIL.BO', name: 'HIMADRI CHEMICALS & INDUSTRIES LTD.' },
  { symbol: 'HSIL.BO', name: 'HSIL LTD.' },
  { symbol: 'HINDUJAVEN.BO', name: 'HINDUJA VENTURES LTD.' },
  { symbol: 'PRAGBOS.BO', name: 'PRAG BOSIMI SYNTHETICS LTD.' },
  { symbol: 'HOTELEELA.BO', name: 'HOTEL LEELAVENTURE LTD.' },
  { symbol: 'INDLEASE.BO', name: 'INDIA LEASE DEVELOPMENT LTD.' },
  { symbol: 'INSILCO.BO', name: 'INSILCO LTD.' },
  { symbol: 'IFSL.BO', name: 'INTEGRATED FINANCIAL SERVICES LTD.' },
  { symbol: 'ITHL.BO', name: 'INTERNATIONAL TRAVEL HOUSE LTD.-$' },
  { symbol: 'ATFL.BO', name: 'AGRO TECH FOODS LTD.' },
  { symbol: 'JASCH.BO', name: 'JASCH INDUSTRIES LTD.' },
  { symbol: 'JCTEL.BO', name: 'JCT ELECTRONICS LTD.' },
  { symbol: 'JCTLTD.BO', name: 'JCT LTD.' },
  { symbol: 'UMANGDAIR.BO', name: 'UMANG DAIRIES LTD.' },
  { symbol: 'KANELIND.BO', name: 'Kanel Industries Limited' },
  { symbol: 'KGDENIM.BO', name: 'KG DENIM LTD.' },
  { symbol: 'KINETICENG.BO', name: 'KINETIC ENGINEERING LTD.' },
  { symbol: 'ENVAIREL.BO', name: 'ENVAIR ELECTRODYNE LTD.' },
  { symbol: 'MAVIIND.BO', name: 'MAVI INDUSTRIES LTD.' },
  { symbol: 'KSBPUMPS.BO', name: 'KSB PUMPS LTD.' },
  { symbol: 'LAXMIMACH.BO', name: 'LAKSHMI MACHINE WORKS LTD.' },
  { symbol: 'UTTAMVALUE.BO', name: 'UTTAM VALUE STEELS LTD.' },
  { symbol: 'LML.BO', name: 'LML LTD.' },
  { symbol: 'LOKHSG.BO', name: 'LOK HOUSING & CONSTRUCTIONS LTD.-$' },
  { symbol: 'MAX.BO', name: 'MAX INDIA LTD.' },
  { symbol: 'MIDINDIA.BO', name: 'MID INDIA INDUSTRIES LTD.' },
  { symbol: 'SPICEJET.BO', name: 'SPICEJET LTD.' },
  { symbol: 'NATPEROX.BO', name: 'NATIONAL PEROXIDE LTD.' },
  { symbol: 'NEPCMICON.BO', name: 'NEPC INDIA LTD.' },
  { symbol: 'PEL.BO', name: 'PIRAMAL ENTERPRISES LTD.' },
  { symbol: 'ABIRLANUVO.BO', name: 'ADITYA BIRLA NUVO LTD.' },
  { symbol: 'ORIENTBANK.BO', name: 'ORIENTAL BANK OF COMMERCE' },
  { symbol: 'INDSUCR.BO', name: 'INDIAN SUCROSE LTD.' },
  { symbol: 'PANCM.BO', name: 'PANYAM CEMENTS & MINERAL INDUSTRIES LTD.' },
  { symbol: 'PENTAGRAPH.BO', name: 'PENTAMEDIA GRAPHICS LTD.' },
  { symbol: 'PRISMCEM.BO', name: 'PRISM CEMENT LTD.' },
  { symbol: 'PDUMJEPULP.BO', name: 'PUDUMJEE PULP & PAPER MILLS LTD.' },
  { symbol: 'PUNJCOMMU.BO', name: 'PUNJAB COMMUNICATIONS LTD.-$' },
  { symbol: 'RAMAPPR-B.BO', name: 'RAMA PAPER MILLS LTD.' },
  { symbol: 'RAMAPETRO.BO', name: 'RAMA PETROCHEMICALS LTD.' },
  { symbol: 'RAPICUT.BO', name: 'RAPICUT CARBIDES LTD.' },
  { symbol: 'RMGALLOY.BO', name: 'RMG Alloy Steel Limited' },
  { symbol: 'ROLTA.BO', name: 'ROLTA INDIA LTD.' },
  { symbol: 'RUCHISOYA.BO', name: 'RUCHI SOYA INDUSTRIES LTD.' },
  { symbol: 'SALORAINTL.BO', name: 'SALORA INTERNATIONAL LTD.' },
  { symbol: 'SAMTELIN.BO', name: 'SAMTEL INDIA LTD.-$' },
  { symbol: 'SAMTEL.BO', name: 'SAMTEL COLOR LTD.' },
  { symbol: 'SKPMIL.BO', name: 'SHREE KRISHNA PAPER MILLS & INDUSTRIES LTD.' },
  { symbol: 'RELINFRA.BO', name: 'RELIANCE INFRASTRUCTURE LTD.' },
  { symbol: 'SOLCT.BO', name: 'SOLID CARBIDE TOOLS LTD.' },
  { symbol: 'STEELCO.BO', name: 'STEELCO GUJARAT LTD.' },
  { symbol: 'SUPPETRO.BO', name: 'SUPREME PETROCHEM LTD.' },
  { symbol: 'TRANSCHEM.BO', name: 'TRANSCHEM LTD.-$' },
  { symbol: 'UTLINDS.BO', name: 'UTL Industries Limited' },
  { symbol: 'UNIPHOS.BO', name: 'UNIPHOS ENTERPRISES LTD.' },
  { symbol: 'HOCL.BO', name: 'HINDUSTAN ORGANIC CHEMICALS LTD.' },
  { symbol: 'MPILCORPL.BO', name: 'MPIL CORPORATION LTD.' },
  { symbol: 'KORE.BO', name: 'Kore Foods Ltd' },
  { symbol: 'AGCNET.BO', name: 'AGC Networks Limited' },
  { symbol: 'UCALFUEL.BO', name: 'UCAL FUEL SYSTEMS LTD.' },
  { symbol: 'HINDMOTORS.BO', name: 'HINDUSTAN MOTORS LTD.' },
  { symbol: 'M&AMP;M.BO', name: 'MAHINDRA & MAHINDRA LTD.' },
  { symbol: 'TATAMOTORS.BO', name: 'TATA MOTORS LTD.' },
  { symbol: 'GARWARPOLY.BO', name: 'GARWARE POLYESTER LTD.' },
  { symbol: 'GSKCONS.BO', name: 'GLAXOSMITHKLINE CONSUMER HEALTHCARE LTD.' },
  { symbol: 'AKZOINDIA.BO', name: 'Akzo Nobel India Limited' },
  { symbol: 'ZUARIGLOB.BO', name: 'ZUARI GLOBAL LTD.' },
  { symbol: 'TATAGLOBAL.BO', name: 'Tata Global Beverages Limited' },
  { symbol: 'FINOLEXIND.BO', name: 'FINOLEX INDUSTRIES LTD.' },
  { symbol: 'VALUEIND.BO', name: 'VALUE INDUSTRIES LTD.' },
  { symbol: 'SBBJ.BO', name: 'STATE BANK OF BIKANER & JAIPUR' },
  { symbol: 'SUNRINV.BO', name: 'SUNRISE INDUSTRIAL TRADERS LTD.' },
  { symbol: 'ZGOLDINV.BO', name: 'GOLD ROCK INVESTMENTS LTD.' },
  { symbol: 'PEOPLIN.BO', name: 'PEOPLES INVESTMENTS LTD.' },
  { symbol: 'DSINVEST.BO', name: 'DALAL STREET INVESTMENTS LTD.' },
  { symbol: 'KARTKIN.BO', name: 'KARTIK INVESTMENTS TRUST LTD.' },
  { symbol: 'ROSEI.BO', name: 'ROSE INVESTMENTS LTD.' },
  { symbol: 'ZSURYODI.BO', name: 'SURYODAYA INVESTMENT & TRADING COMPANY LTD.' },
  { symbol: 'OSCAR.BO', name: 'OSCAR INVESTMENTS LTD.' },
  { symbol: 'BIDL.BO', name: 'Bhagyodaya Infrastructure Development Ltd' },
  { symbol: 'KRATOSENER.BO', name: 'KRATOS ENERGY & INFRASTRUCTURE LTD.' },
  { symbol: 'ZCHANAIN.BO', name: 'CHANAKYA INVESTMENTS LTD.' },
  { symbol: 'JAYBHCR.BO', name: 'JAYABHARAT CREDIT LTD.-$' },
  { symbol: 'RAPIDIN.BO', name: 'RAPID INVESTMENTS LTD.' },
  { symbol: 'WALCHPF.BO', name: 'WALCHAND PEOPLEFIRST LTD.' },
  { symbol: 'ZSWASTSA.BO', name: 'SWASTIK SAFE DEPOSIT & INVESTMENTS LTD.' },
  { symbol: 'WHBRADY.BO', name: 'W.H.BRADY & CO.LTD.' },
  { symbol: 'BOMBCYC.BO', name: 'BOMBAY CYCLE & MOTOR AGENCY LTD.' },
  { symbol: 'MACK.BO', name: 'MACK TRADING CO.LTD.' },
  { symbol: 'MALTC.BO', name: 'MALABAR TRADING CO.LTD.' },
  { symbol: 'MULLER.BO', name: 'MULLER & PHIPPS (INDIA) LTD.' },
  { symbol: 'ZNEWSAGA.BO', name: 'NEW SAGAR TRADING CO.LTD.' },
  { symbol: 'ANANDPROJ.BO', name: 'Anand Projects Ltd' },
  { symbol: 'INDIANVSH.BO', name: 'INDIANIVESH LTD.' },
  { symbol: 'CHOWGULSTM.BO', name: 'CHOWGULE STEAMSHIPS LTD.' },
  { symbol: 'GLOBOFFS.BO', name: 'GLOBAL OFFSHORE SERVICES LTD.-$' },
  { symbol: 'DHENUBUILD.BO', name: 'DHENU BUILDCON INFRA LTD.' },
  { symbol: 'ASSOSTNB.BO', name: 'ASSOCIATED STONE INDUSTRIES (KOTAH) LTD.' },
  { symbol: 'KLYNCEM.BO', name: 'KALYANPUR CEMENTS LTD.' },
  { symbol: 'OCL.BO', name: 'OCL INDIA LTD.' },
  { symbol: 'BOMBPOT.BO', name: 'BOMBAY POTTERIES & TILES LTD.' },
  { symbol: 'BOROSIL.BO', name: 'BOROSIL GLASS WORKS LTD.' },
  { symbol: 'MARATHR.BO', name: 'MARATHWADA REFRACTORIES LTD.' },
  { symbol: 'RASSIREF.BO', name: 'RAASI REFRACTORIES LTD.-$' },
  { symbol: 'TRIVENIGQ.BO', name: 'TRIVENI GLASS LTD.-$' },
  { symbol: 'IPAPPM.BO', name: 'International Paper APPM Limited' },
  { symbol: 'MYSPAPE.BO', name: 'MYSORE PAPER MILLS LTD.' },
  { symbol: 'NATHPULP.BO', name: 'NATH PULP & PAPER MILLS LTD.' },
  { symbol: 'CITADEL.BO', name: 'CITADEL REALTY AND DEVELOPERS LTD.' },
  { symbol: 'SOLIDCO.BO', name: 'SOLID CONTAINERS LTD.' },
  { symbol: 'SPECIAPP.BO', name: 'SPECIALITY PAPERS LTD.' },
  { symbol: 'RELSIND.BO', name: 'RELSON INDIA LTD.' },
  { symbol: 'SHBHAWPA.BO', name: 'SHREE BHAWANI PAPER MILLS LTD.' },
  { symbol: 'RAMAPULP.BO', name: 'RAMA PULP & PAPERS LTD.' },
  { symbol: 'VAPIPPR.BO', name: 'VAPI PAPER MILLS LTD.' },
  { symbol: 'SINTEX.BO', name: 'SINTEX INDUSTRIES LTD.' },
  { symbol: 'BLUBLND-B.BO', name: 'BLUE BLENDS (INDIA) LTD.' },
  { symbol: 'ZGAEKWAR.BO', name: 'GAEKWAR MILLS LTD.' },
  { symbol: 'FORBESCO.BO', name: 'FORBES & COMPANY LTD.-$' },
  { symbol: 'HPCOTTON.BO', name: 'H.P.COTTON TEXTILE MILLS LTD.' },
  { symbol: 'UNITEDINT.BO', name: 'UNITED INTERACTIVE LTD.' },
  { symbol: 'JAMSHRI.BO', name: 'JAMSHRI RANJITSINGHJI SPG. & WVG. MILLS CO.LTD.-$' },
  { symbol: 'KATRSPG.BO', name: 'KATARE SPINNING MILLS LTD.' },
  { symbol: 'LAKSHMIMIL.BO', name: 'LAKSHMI MILLS COMPANY LTD.-$' },
  { symbol: 'MALWACOTT.BO', name: 'MALWA COTTON SPINNING MILLS LTD.' },
  { symbol: 'MODERN.BO', name: 'MODERN INDIA LTD.-$' },
  { symbol: 'PASUSPG.BO', name: 'PASUPATI SPG.& WVG.MILLS LTD.' },
  { symbol: 'RAJABAH.BO', name: 'RAJA BAHADUR INTERNATIONAL LTD.' },
  { symbol: 'SHREERAM.BO', name: 'SHREE RAM URBAN INFRASTRUCTURE LTD.' },
  { symbol: 'SIMPLXREA.BO', name: 'SIMPLEX REALTY LTD.' },
  { symbol: 'SWANENERGY.BO', name: 'SWAN ENERGY LTD.' },
  { symbol: 'VICTMILL.BO', name: 'VICTORIA MILLS LTD.' },
  { symbol: 'ZSVTRADI.BO', name: 'S.V.TRADING & AGENCIES LTD.' },
  { symbol: 'ZSVARAJT.BO', name: 'SVARAJ TRADING & AGENCIES LTD.' },
  { symbol: 'KSHITIJ.BO', name: 'KSHITIZ INVESTMENT LTD.' },
  { symbol: 'SALSAIN.BO', name: 'SHREE SALASAR INVESTMENT LTD.' },
  { symbol: 'DHANLEELA.BO', name: 'DHANLEELA INVESTMENTS & TRADING COMPANY LTD.' },
  { symbol: 'INDSOYA.BO', name: 'INDSOYA LTD.' },
  { symbol: 'VEERENRGY.BO', name: 'VEER ENERGY & INFRASTRUCTURE LTD.' },
  { symbol: 'TILAKFIN.BO', name: 'Tilak Finance Limited' },
  { symbol: 'KKFIN.BO', name: 'K K Fincorp Limited' },
  { symbol: 'UNIJOLL.BO', name: 'UNIJOLLY INVESTMENTS CO.LTD.' },
  { symbol: 'WAGEND.BO', name: 'Wagend Infra Venture Limited' },
  { symbol: 'HEALINV.BO', name: 'HEALTHY INVESTMENTS LTD.' },
  { symbol: 'SAHARA.BO', name: 'SAHARA ONE MEDIA & ENTERTAINMENT LTD.-$' },
  { symbol: 'MODWOOL.BO', name: 'MODELLA WOOLLENS LTD.' },
  { symbol: 'MODIPON.BO', name: 'MODIPON LTD.' },
  { symbol: 'DIGJAM.BO', name: 'DIGJAM LTD.' },
  { symbol: 'SHRIDINE.BO', name: 'SHRI DINESH MILLS LTD.-$' },
  { symbol: 'SWADPOL.BO', name: 'SWADESHI POLYTEX LTD.' },
  { symbol: 'BIRLATR.BO', name: 'BIRLA TRANSASIA CARPETS LTD.' },
  { symbol: 'FOMEHOT.BO', name: 'FOMENTO RESORTS & HOTELS LTD.' },
  { symbol: 'SHRAJSYNQ.BO', name: 'SHREE RAJASTHAN SYNTEX LTD.-$' },
  { symbol: 'SHRMFGC.BO', name: 'SHREE MANUFACTURING CO.LTD.' },
  { symbol: 'HINDSYNTEX.BO', name: 'HIND SYNTEX LTD.' },
  { symbol: 'ZSATYASL.BO', name: 'SATYAM SILK MILLS LTD.' },
  { symbol: 'ZDIGIELE.BO', name: 'DIGITAL ELECTRONICS LTD.' },
  { symbol: 'EMCO.BO', name: 'EMCO LTD.-$' },
  { symbol: 'GEE.BO', name: 'GEE LTD.' },
  { symbol: 'JYOTI.BO', name: 'JYOTI LTD.-$' },
  { symbol: 'JSLINDL.BO', name: 'JSL INDUSTRIES LTD.' },
  { symbol: 'KAYCEEI.BO', name: 'KAYCEE INDUSTRIES LTD.' },
  { symbol: 'INDOKEM.BO', name: 'INDOKEM LTD.' },
  { symbol: 'PANAENERG.BO', name: 'PANASONIC ENERGY INDIA COMPANY LTD.-$' },
  { symbol: 'PERMAGN.BO', name: 'PERMANENT MAGNETS LTD.-$' },
  { symbol: 'HIGHENE.BO', name: 'HIGH ENERGY BATTERIES (INDIA) LTD.' },
  { symbol: 'STDBAT.BO', name: 'STANDARD BATTERIES LTD.' },
  { symbol: 'WSIND.BO', name: 'W.S.INDUSTRIES (INDIA) LTD.-$' },
  { symbol: 'DLTNCBL.BO', name: 'DELTON CABLES LTD.' },
  { symbol: 'DELTRON.BO', name: 'DELTRON LTD.' },
  { symbol: 'LAKSELEC.BO', name: 'LAKSHMI ELECTRICAL CONTROL SYSTEMS LTD.' },
  { symbol: 'KHAITANELE.BO', name: 'KHAITAN ELECTRICALS LTD.' },
  { symbol: 'TUMUSEL.BO', name: 'TUMUS ELECTRIC CORPORATION LTD.' },
  { symbol: 'CONFINT.BO', name: 'Confidence Finance And Trading Limited' },
  { symbol: 'RAVINDT.BO', name: 'RAVINDRA TRADING & AGENCIES LTD.' },
  { symbol: 'GDTRAGN.BO', name: 'G.D.TRADING & AGENCIES LTD.' },
  { symbol: 'ZMULTIPU.BO', name: 'MULTIPURPOSE TRADING & AGENCIES LTD.' },
  { symbol: 'REMISIN.BO', name: 'REMI SALES & ENGINEERING LTD.' },
  { symbol: 'RIDHISYN.BO', name: 'RIDHI SYNTHETICS LTD.' },
  { symbol: 'GRANDMA.BO', name: 'GRANDMA TRADING & AGENCIES LTD.' },
  { symbol: 'ZARDIINV.BO', name: 'ARDI INVESTMENT & TRADING LTD.' },
  { symbol: 'SOFTBPO.BO', name: 'SOFTBPO GLOBAL SERVICES LTD.' },
  { symbol: 'NYSSACORP.BO', name: 'Nyssa Corporation Limited' },
  { symbol: 'ZVINADTR.BO', name: 'VINADITYA TRADING CO.LTD.' },
  { symbol: 'ELFTRDG.BO', name: 'ELF TRADING & CHEMICALS MANUFACTURING LTD.' },
  { symbol: 'ARVRTRI.BO', name: 'ARUN VARUN TRADE & INVESTMENT LTD.' },
  { symbol: 'KRISHNA.BO', name: 'Krishna Ventures Limited' },
  { symbol: 'GANHOLD.BO', name: 'GANESH HOLDINGS LTD.' },
  { symbol: 'SJCORP.BO', name: 'SJ CORPORATION LTD.' },
  { symbol: 'UNIABEXAL.BO', name: 'UNI ABEX ALLOY PRODUCTS LTD.' },
  { symbol: 'ANILSPL.BO', name: 'ANIL SPECIAL STEEL INDUSTRIES LTD.-$' },
  { symbol: 'BWLLTD.BO', name: 'BWL LTD.' },
  { symbol: 'BGWTATO.BO', name: 'BHAGWATI AUTOCAST LTD.' },
  { symbol: 'CHASBRT.BO', name: 'CHASE BRIGHT STEEL LTD.' },
  { symbol: 'UNIVPRIM.BO', name: 'UNIVERSAL PRIME ALUMINIUM LTD.' },
  { symbol: 'GALADA.BO', name: 'GALADA POWER & TELECOMMUNICATION LTD.' },
  { symbol: 'GONTER.BO', name: 'GONTERMANN-PEIPERS (INDIA) LTD.' },
  { symbol: 'HINDWRS.BO', name: 'HINDUSTAN WIRES LTD.' },
  { symbol: 'IBRIGST.BO', name: 'INDIAN BRIGHT STEEL CO.LTD.' },
  { symbol: 'INLCM.BO', name: 'INDIAN LINK CHAIN MANUFACTURES LTD.' },
  { symbol: 'INFORTEC.BO', name: 'INFORMED TECHNOLOGIES INDIA LTD.' },
  { symbol: 'KAIRA.BO', name: 'KAIRA CAN CO.LTD.' },
  { symbol: 'ORISSASP.BO', name: 'ORISSA SPONGE IRON & STEEL LTD.-$' },
  { symbol: 'ORIENTABRA.BO', name: 'ORIENT ABRASIVES LTD.-$' },
  { symbol: 'RATHIST.BO', name: 'RATHI STEEL & POWER LTD.-$' },
  { symbol: 'SSDUNC.BO', name: 'SCHRADER DUNCAN LTD.-$' },
  { symbol: 'STOVACQ.BO', name: 'STOVEC INDUSTRIES LTD.' },
  { symbol: 'STEWARTQ.BO', name: 'STEWARTS & LLOYDS OF INDIA LTD.-$' },
  { symbol: 'TATAYODOGA.BO', name: 'TAYO ROLLS LTD.-$' },
  { symbol: 'TINPLATE.BO', name: 'TINPLATE COMPANY OF INDIA LTD.' },
  { symbol: 'TUBEINVEST.BO', name: 'TUBE INVESTMENTS OF INDIA LTD.' },
  { symbol: 'ZWELCAST.BO', name: 'WELCAST STEELS LTD.' },
  { symbol: 'VIDIRMT.BO', name: 'VIDARBHA IRON & STEEL CORPORATION LTD.' },
  { symbol: 'ACGL.BO', name: 'AUTOMOBILE CORPORATION OF GOA LTD.-$' },
  { symbol: 'SCOOTER.BO', name: 'SCOOTERS INDIA LTD.' },
  { symbol: 'SMLISUZU.BO', name: 'SML ISUZU LIMITED' },
  { symbol: 'JAINEX.BO', name: 'JAINEX AAMCOL LTD.' },
  { symbol: 'ALFREDHE.BO', name: 'ALFRED HERBERT (INDIA) LTD.' },
  { symbol: 'CIMMCO.BO', name: 'CIMMCO LTD.' },
  { symbol: 'GGDANDE.BO', name: 'G.G.DANDEKAR MACHINE WORKS LTD.' },
  { symbol: 'GMM.BO', name: 'GMM PFAUDLER LTD.' },
  { symbol: 'TULIVE.BO', name: 'Tulive Developers Limited' },
  { symbol: 'KULKPOWT.BO', name: 'KULKARNI POWER & TOOLS LTD.' },
  { symbol: 'LXMIATO.BO', name: 'LAKSHMI AUTOMATIC LOOM WORKS LTD.' },
  { symbol: 'LYNMC.BO', name: 'LYNX MACHINERY & COMMERCIALS LTD.' },
  { symbol: 'MIRCH.BO', name: 'Mirch Technologies (India) Ltd' },
  { symbol: 'MONOT.BO', name: 'MONOTYPE INDIA LTD.' },
  { symbol: 'INTEGRAEN.BO', name: 'INTEGRA ENGINEERING INDIA LTD.' },
  { symbol: 'REVATHI.BO', name: 'REVATHI EQUIPMENT LTD.-$' },
  { symbol: 'PSITINFRA.BO', name: 'PS IT Infrastructure & Services Limited' },
  { symbol: 'SINDUVA.BO', name: 'SINDU VALLEY TECHNOLOGIES LTD.' },
  { symbol: 'AXONVL.BO', name: 'Axon Ventures Limited' },
  { symbol: 'SHYMINV.BO', name: 'SHYAMKAMAL INVESTMENTS LTD.' },
  { symbol: 'ZHEMHOLD.BO', name: 'HEM HOLDINGS & TRADING LTD.' },
  { symbol: 'MAHACORP.BO', name: 'MAHARASHTRA CORPORATION LTD.' },
  { symbol: 'DOLAT.BO', name: 'DOLAT INVESTMENTS LTD.' },
  { symbol: 'SHYAMHO.BO', name: 'SHYAMAL HOLDINGS & TRADING LTD.' },
  { symbol: 'PRISMINFO.BO', name: 'PRISM INFORMATICS LTD.' },
  { symbol: 'IMCFINA.BO', name: 'IMC FINANCE LTD.' },
  { symbol: 'GOLDCORP.BO', name: 'Goldcrest Corporation Limited' },
  { symbol: 'WWTECHHOL.BO', name: 'W W TECHNOLOGY HOLDINGS LTD.' },
  { symbol: 'ZKOVALIN.BO', name: 'KOVALAM INVESTMENT & TRADING CO.LTD.' },
  { symbol: 'MULTIIN.BO', name: 'MULTIPLUS HOLDINGS LTD.' },
  { symbol: 'SKYLMILAR.BO', name: 'SKYLINE MILLARS LTD.' },
  { symbol: 'REMIPRO.BO', name: 'REMI PROCESS PLANT & MACHINERY LTD.' },
  { symbol: 'ABCBEARS.BO', name: 'ABC BEARINGS LTD.-$' },
  { symbol: 'TAPARIA.BO', name: 'TAPARIA TOOLS LTD.' },
  { symbol: 'BRADYM.BO', name: 'BRADY & MORRIS ENGINEERING CO.LTD.' },
  { symbol: 'DRLCOME.BO', name: 'DRILLCO METAL CARBIDES LTD.' },
  { symbol: 'DECANBRG.BO', name: 'DECCAN BEARINGS LTD.' },
  { symbol: 'GAJRA.BO', name: 'GAJRA BEVEL GEARS LTD.-$' },
  { symbol: 'GUJAUTO.BO', name: 'GUJARAT AUTOMOTIVE GEARS LTD.' },
  { symbol: 'HERCULES.BO', name: 'HERCULES HOISTS LTD.' },
  { symbol: 'HINDEVER.BO', name: 'HINDUSTAN EVEREST TOOLS LTD.' },
  { symbol: 'SINGER.BO', name: 'SINGER INDIA LTD.' },
  { symbol: 'INTLCOMBQ.BO', name: 'INTERNATIONAL COMBUSTION (INDIA) LTD.' },
  { symbol: 'JOSTS.BO', name: 'JOST\'S ENGINEERING CO.LTD.' },
  { symbol: 'FAGBEARING.BO', name: 'FAG BEARINGS INDIA LTD.' },
  { symbol: 'MPCOSEMB.BO', name: 'MIPCO SEAMLESS RINGS (GUJARAT) LTD.' },
  { symbol: 'ROLCOEN.BO', name: 'ROLCON ENGINEERING CO.LTD.' },
  { symbol: 'REILELEC.BO', name: 'REIL ELECTRICALS INDIA LTD.' },
  { symbol: 'SNL.BO', name: 'SNL BEARINGS LTD.' },
  { symbol: 'JAIPAN.BO', name: 'JAIPAN INDUSTRIES LTD.' },
  { symbol: 'HINDHARD.BO', name: 'HINDUSTAN HARDY SPICER LTD.' },
  { symbol: 'VISHMEL.BO', name: 'VISHAL MALLEABLES LTD.' },
  { symbol: 'TRITONV.BO', name: 'TRITON VALVES LTD.' },
  { symbol: 'HINDUJAFO.BO', name: 'HINDUJA FOUNDRIES LTD.-$' },
  { symbol: 'SUDAI.BO', name: 'SUDAL INDUSTRIES LTD.' },
  { symbol: 'JRIIIL.BO', name: 'JRI Industries & Infrastructure Limited' },
  { symbol: 'BHRKALM.BO', name: 'BHORUKA ALUMINIUM LTD.' },
  { symbol: 'MAISF.BO', name: 'MAHESH AGRICULTURE IMPLEMENTS & STEEL FORGE LTD.' },
  { symbol: 'ANUPMAL.BO', name: 'ANUP MALLEABLE LTD.' },
  { symbol: 'STANROS.BO', name: 'STANROSE MAFATLAL INVESTMENTS AND FINANCE LTD.' },
  { symbol: 'PRECTRA.BO', name: 'PRECIOUS TRADING & INVESTMENTS LTD.' },
  { symbol: 'ALNATRD.BO', name: 'ALNA TRADING & EXPORTS LTD.' },
  { symbol: 'PANKAJPIYUS.BO', name: 'Pankaj Piyush Trade & Investment Ltd' },
  { symbol: 'PARNAXLAB.BO', name: 'PARNAX LAB LTD.' },
  { symbol: 'INDIACO.BO', name: 'INDIACO VENTURES LTD.' },
  { symbol: 'INTELLCAP.BO', name: 'INTELLIVATE CAPITAL VENTURES LTD.' },
  { symbol: 'VYAPAR.BO', name: 'VYAPAR INDUSTRIES LTD.' },
  { symbol: 'ASISL.BO', name: 'ASIS LOGISTICS LIMITED' },
  { symbol: 'ISHWATR.BO', name: 'ISHWARSHAKTI HOLDINGS & TRADERS LTD.' },
  { symbol: 'TERRAFORM.BO', name: 'TERRAFORM MAGNUM LTD.' },
  { symbol: 'APIS.BO', name: 'APIS INDIA LTD.' },
  { symbol: 'ZKHATAUE.BO', name: 'KHATAU EXIM LTD.' },
  { symbol: 'SRIOMTR.BO', name: 'SHREE OM TRADES LTD.' },
  { symbol: 'GLXYENT.BO', name: 'GALAXY ENTERTAINMENT CORPORATION LTD.-$' },
  { symbol: 'ZSARVAMA.BO', name: 'SARVAMANGAL MERCANTILE CO.LTD.' },
  { symbol: 'ASL.BO', name: 'Arihant Superstructures Limited' },
  { symbol: 'VIKSHEN.BO', name: 'VIKSIT ENGINEERING LTD.' },
  { symbol: 'STYABS.BO', name: 'STYROLUTION ABS (INDIA) LTD.' },
  { symbol: 'MODISNME.BO', name: 'MODISON METALS LTD.-$' },
  { symbol: 'SASHWAT.BO', name: 'Sashwat Technocrats Limited' },
  { symbol: 'BORAX.BO', name: 'BORAX MORARJI LTD.' },
  { symbol: 'CHEMOPH.BO', name: 'CHEMO PHARMA LABORATORIES LTD.' },
  { symbol: 'CLNINDIA.BO', name: 'CLARIANT CHEMICALS (INDIA) LTD.' },
  { symbol: 'DEEPAKNI.BO', name: 'DEEPAK NITRITE LTD.-$' },
  { symbol: 'DHARAMSI.BO', name: 'DHARAMSI MORARJI CHEMICAL CO.LTD.' },
  { symbol: 'DIL.BO', name: 'DIL LTD.' },
  { symbol: 'GUJCARB.BO', name: 'GUJARAT CARBON & INDUSTRIES LTD.' },
  { symbol: 'JAYCH.BO', name: 'JAYSHREE CHEMICALS LTD.' },
  { symbol: 'JLMORI.BO', name: 'J.L.MORISON (INDIA) LTD.' },
  { symbol: 'KELENRG.BO', name: 'KELTECH ENERGIES LTD.' },
  { symbol: 'KEMP.BO', name: 'KEMP & COMPANY LTD.' },
  { symbol: 'MPAGI.BO', name: 'M.P.AGRO INDUSTRIES LTD.' },
  { symbol: 'ORIENTCQ.BO', name: 'ORIENTAL CARBON & CHEMICALS LTD.-$' },
  { symbol: 'PHILIPCARB.BO', name: 'PHILLIPS CARBON BLACK LTD.' },
  { symbol: 'AMAL.BO', name: 'AMAL LTD.' },
  { symbol: 'POLYCHEM.BO', name: 'POLYCHEM LTD.' },
  { symbol: 'SUNASIAN.BO', name: 'Sunrise Asian Limited' },
  { symbol: 'ZANDUREALT.BO', name: 'Zandu Realty Limited' },
  { symbol: 'MYSORPETRO.BO', name: 'MYSORE PETRO CHEMICALS LTD.-$' },
  { symbol: 'TUTIALKA.BO', name: 'TUTICORIN ALKALI CHEMICALS & FERTILISERS LTD.' },
  { symbol: 'PACL.BO', name: 'PUNJAB ALKALIES & CHEMICALS LTD.' },
  { symbol: 'TANFACIND.BO', name: 'TANFAC INDUSTRIES LTD.-$' },
  { symbol: 'GUJPETR.BO', name: 'GUJARAT PETROSYNTHESE LTD.' },
  { symbol: 'SWADEIN.BO', name: 'SWADESHI INDUSTRIES LEASING CO.LTD.' },
  { symbol: 'MASCH.BO', name: 'MASTER CHEMICALS LTD.' },
  { symbol: 'SHREEJAL.BO', name: 'SHREEJAL INFO HUBS LTD.' },
  { symbol: 'CHEMFALKAL.BO', name: 'CHEMFAB ALKALIS LTD.' },
  { symbol: 'JAYSYN.BO', name: 'JAYSYNTH DYESTUFF (INDIA) LTD.' },
  { symbol: 'MAKERSL.BO', name: 'MAKERS LABORATORIES LTD.-$' },
  { symbol: 'CONTCHM.BO', name: 'CONTINENTAL CHEMICALS LTD.' },
  { symbol: 'ADVPETR-B.BO', name: 'ADVANCE PETROCHEMICALS LTD.' },
  { symbol: 'BBREALTY.BO', name: 'B&B Realty Limited' },
  { symbol: 'RAJSPTR.BO', name: 'RAJASTHAN PETRO SYNTHETICS LTD.' },
  { symbol: 'BLUECHIPT.BO', name: 'BLUE CHIP TEX INDUSTRIES LTD.' },
  { symbol: 'NOBLEXP.BO', name: 'NOBLE EXPLOCHEM LTD.-$' },
  { symbol: 'JAGAJITIND.BO', name: 'JAGATJIT INDUSTRIES LTD.' },
  { symbol: 'KESARENT.BO', name: 'KESAR ENTERPRISES LTD.-$' },
  { symbol: 'OUDHSUG.BO', name: 'OUDH SUGAR MILLS LTD.' },
  { symbol: 'APTEAML.BO', name: 'APTE AMALGAMATIONS LTD.' },
  { symbol: 'RAVALSUGAR.BO', name: 'RAVALGAON SUGAR FARM LTD.' },
  { symbol: 'KHODAY.BO', name: 'KHODAY INDIA LTD.-$' },
  { symbol: 'DHARSUGAR.BO', name: 'DHARANI SUGARS & CHEMICALS LTD.' },
  { symbol: 'THIRUSUGAR.BO', name: 'THIRU AROORAN SUGARS LTD.' },
  { symbol: 'UBHOLDINGS.BO', name: 'UNITED BREWERIES (HOLDINGS) LTD.' },
  { symbol: 'KFBL.BO', name: 'KOTHARI FERMENTATION & BIOCHEM LTD.' },
  { symbol: 'CAPRO.BO', name: 'CAPROLACTAM CHEMICALS LTD.' },
  { symbol: 'PICCASUG.BO', name: 'PICCADILY SUGAR & ALLIED INDUSTRIES LTD.' },
  { symbol: 'GIRDSGA.BO', name: 'GIRDHARILAL SUGAR & ALLIED INDUSTRIES LTD.' },
  { symbol: 'RIGASUG.BO', name: 'RIGA SUGAR COMPANY LTD.-$' },
  { symbol: 'CJGEL.BO', name: 'C.J.GELATINE PRODUCTS LTD.' },
  { symbol: 'MOUNTSHIQ.BO', name: 'MOUNT SHIVALIK INDUSTRIES LTD.-$' },
  { symbol: 'AMRITCORP.BO', name: 'AMRIT CORP.LTD.-$' },
  { symbol: 'CORAGRO.BO', name: 'COROMANDEL AGRO PRODUCTS & OILS LTD.' },
  { symbol: 'KLRF.BO', name: 'KLRF LTD.' },
  { symbol: 'OLYOI.BO', name: 'OLYMPIC OIL INDUSTRIES LTD.' },
  { symbol: 'MLKFOOD.BO', name: 'MILKFOOD LTD.' },
  { symbol: 'POLSON.BO', name: 'POLSON LTD.' },
  { symbol: 'RASOI.BO', name: 'RASOI LTD.' },
  { symbol: 'RATNAMAGRO.BO', name: 'Ratnamani Agro Industries Ltd' },
  { symbol: 'SAGRSOY-B.BO', name: 'SAGAR SOYA PRODUCTS LTD.' },
  { symbol: 'ORIBEVER.BO', name: 'ORIENT BEVERAGES LTD.' },
  { symbol: 'TTKHEALTH.BO', name: 'TTK HEALTHCARE LTD.-$' },
  { symbol: 'SREERAYA.BO', name: 'SREE RAYALASEEMA ALKALIES & ALLIED CHEMICALS LTD.' },
  { symbol: 'LIMECHM.BO', name: 'LIME CHEMICALS LTD.' },
  { symbol: 'NOL.BO', name: 'NATIONAL OXYGEN LTD.' },
  { symbol: 'WIREFABR.BO', name: 'WIRES & FABRIKS (SA) LTD.' },
  { symbol: 'ANSALHSG.BO', name: 'ANSAL HOUSING & CONSTRUCTION LTD.-$' },
  { symbol: 'COMPUPN.BO', name: 'COMPUTER POINT LTD.' },
  { symbol: 'ADDIND.BO', name: 'ADDI INDUSTRIES LTD.-$' },
  { symbol: 'ASHNOOR.BO', name: 'ASHNOOR TEXTILE MILLS LTD.' },
  { symbol: 'DHRUVES.BO', name: 'DHRUV ESTATES LTD.' },
  { symbol: 'WINSOMEDJ.BO', name: 'WINSOME DIAMONDS AND JEWELLERY LTD.' },
  { symbol: 'SCHABLON.BO', name: 'SCHABLONA INDIA LTD.' },
  { symbol: 'LKPFIN.BO', name: 'LKP Finance Limited' },
  { symbol: 'EASTBUILD.BO', name: 'EAST BUILDTECH LTD.' },
  { symbol: 'ARUMUGA.BO', name: 'Sri Arumuga Enterprise Limited' },
  { symbol: 'MNPLFIN.BO', name: 'MANIPAL FINANCE CORPORATION LTD.' },
  { symbol: 'KIDUJA.BO', name: 'KIDUJA INDIA LTD.' },
  { symbol: 'KEYCORP.BO', name: 'KEY CORP LTD.' },
  { symbol: 'SHIKHARLETR.BO', name: 'SHIKHAR LEASING & TRADING LTD.' },
  { symbol: 'GUJHOTE.BO', name: 'GUJARAT HOTELS LTD.' },
  { symbol: 'RAJATH.BO', name: 'Rajath Finance Limited' },
  { symbol: 'RASRESOR.BO', name: 'RAS RESORTS & APART HOTELS LTD.' },
  { symbol: 'PARMCOS-B.BO', name: 'PARAMOUNT COSMETICS (INDIA) LTD.' },
  { symbol: 'JINDHOT.BO', name: 'JINDAL HOTELS LTD.' },
  { symbol: 'SERIND.BO', name: 'SER INDUSTRIES LTD.' },
  { symbol: 'JPTRLES.BO', name: 'JUPITER INDUSTRIES & LEASING LTD.' },
  { symbol: 'SIMMOND.BO', name: 'SIMMONDS MARSHALL LTD.' },
  { symbol: 'LEDOTEA.BO', name: 'LEDO TEA CO.LTD.' },
  { symbol: 'WARRENTEA.BO', name: 'WARREN TEA LTD.-$' },
  { symbol: 'COCHMAL.BO', name: 'COCHIN MALABAR ESTATES & INDUSTRIES LTD.' },
  { symbol: 'BESTEAST.BO', name: 'BEST EASTERN HOTELS LTD.' },
  { symbol: 'ISTLTD.BO', name: 'IST LTD.' },
  { symbol: 'COSMOFILMS.BO', name: 'COSMO FILMS LTD.' },
  { symbol: 'DIAMANT.BO', name: 'Diamant Infrastructure Limited' },
  { symbol: 'NEWMKTADV.BO', name: 'NEW MARKETS ADVISORY LTD.' },
  { symbol: 'NITINALOY.BO', name: 'NITIN ALLOYS GLOBAL LTD.' },
  { symbol: 'SMIFS.BO', name: 'SMIFS CAPITAL MARKETS LTD.' },
  { symbol: 'GREYCELLS.BO', name: 'Greycells Education Limited' },
  { symbol: 'MSRINDIA.BO', name: 'MSR INDIA LTD.' },
  { symbol: 'JOYREALTY.BO', name: 'Joy Realty Limited' },
  { symbol: 'WELSPSY.BO', name: 'WELSPUN SYNTEX LTD.' },
  { symbol: 'BLCISER.BO', name: 'BLUE CIRCLE SERVICES LTD.' },
  { symbol: 'PANCARBON.BO', name: 'PANASONIC CARBON INDIA CO.LTD.-$' },
  { symbol: 'HBLEAS.BO', name: 'HB LEASING & FINANCE CO.LTD.' },
  { symbol: 'SHRICON.BO', name: 'SHRICON INDUSTRIES LTD.' },
  { symbol: 'STRLGUA.BO', name: 'STERLING GUARANTY & FINANCE LTD.' },
  { symbol: 'FRONTCAP.BO', name: 'Frontier Capital Ltd' },
  { symbol: 'KEDIACN.BO', name: 'KEDIA CONSTRUCTION CO.LTD.' },
  { symbol: 'SATRAPROP.BO', name: 'SATRA PROPERTIES (INDIA) LTD.' },
  { symbol: 'STEERINTER.BO', name: 'STERLING INTERNATIONAL ENTERPRISES LTD.-$' },
  { symbol: 'TRANOCE.BO', name: 'TRANSOCEANIC PROPERTIES LTD.' },
  { symbol: 'THAKRAL.BO', name: 'THAKRAL SERVICES (INDIA) LTD.' },
  { symbol: 'VJTFEDU.BO', name: 'VJTF EDUSERVICES LTD.' },
  { symbol: 'VOLLF.BO', name: 'VOLTAIRE LEASING & FINANCE LTD.' },
  { symbol: 'NETLINK.BO', name: 'NETLINK SOLUTIONS (INDIA) LTD.' },
  { symbol: 'ZLEENCON.BO', name: 'LEENA CONSULTANCY LTD.' },
  { symbol: 'INDINFO.BO', name: 'INDIAN INFOTECH & SOFTWARE LTD.' },
  { symbol: 'BANASFN.BO', name: 'BANAS FINANCE LTD.' },
  { symbol: 'HATHWAYB.BO', name: 'HATHWAY BHAWANI CABLETEL & DATACOM LTD.' },
  { symbol: 'PRESSMN.BO', name: 'Pressman Advertising Limited' },
  { symbol: 'PHOTON.BO', name: 'PHOTON CAPITAL ADVISORS LTD.' },
  { symbol: 'OSWALEA.BO', name: 'OSWAL LEASING LTD.' },
  { symbol: 'GOVINDRU.BO', name: 'GOVIND RUBBER LTD.-$' },
  { symbol: 'INDAG.BO', name: 'INDAG RUBBER LTD.-$' },
  { symbol: 'MMRUBBR-B.BO', name: 'MM RUBBER COMPANY LTD.' },
  { symbol: 'BENARAS.BO', name: 'BENARES HOTELS LTD.' },
  { symbol: 'BHAGWOX.BO', name: 'BHAGWATI OXYGEN LTD.' },
  { symbol: 'BOMOXY-B1.BO', name: 'BOMBAY OXYGEN CORPORATION LTD.' },
  { symbol: 'CRAVATEX.BO', name: 'CRAVATEX LTD.' },
  { symbol: 'CAPRIHANS.BO', name: 'CAPRIHANS INDIA LTD.-$' },
  { symbol: 'ITDCEM.BO', name: 'ITD CEMENTATION INDIA LTD.' },
  { symbol: 'EMPIND.BO', name: 'EMPIRE INDUSTRIES LTD.' },
  { symbol: 'GAMMONIND.BO', name: 'GAMMON INDIA LTD.' },
  { symbol: 'GARWALLROP.BO', name: 'GARWARE-WALL ROPES LTD.' },
  { symbol: 'GARWAMAR.BO', name: 'GARWARE MARINE INDUSTRIES LTD.' },
  { symbol: 'HARDCAS.BO', name: 'HARDCASTLE & WAUD MFG.CO.LTD.' },
  { symbol: 'HINDDORROL.BO', name: 'HINDUSTAN DORR-OLIVER LTD.' },
  { symbol: 'ZHINDHSG.BO', name: 'HINDUSTAN HOUSING CO.LTD.' },
  { symbol: 'HIL.BO', name: 'HIL LTD.' },
  { symbol: 'JAYSHREETEA.BO', name: 'JAY SHREE TEA & INDUSTRIES LTD.' },
  { symbol: 'MODRNSH.BO', name: 'MODERN SHARES & STOCKBROKERS LTD.' },
  { symbol: 'PAPERPROD.BO', name: 'Huhtamaki PPL Limited' },
  { symbol: 'PREMSYN.BO', name: 'PREMIER SYNTHETICS LTD.' },
  { symbol: 'RJSHAH.BO', name: 'R.J.SHAH & CO.LTD.' },
  { symbol: 'SHAHCON.BO', name: 'SHAH CONSTRUCTION CO.LTD.' },
  { symbol: 'SINNAR.BO', name: 'SINNAR BIDI UDYOG LTD.' },
  { symbol: 'HINDMILL.BO', name: 'HINDOOSTAN MILLS LTD.' },
  { symbol: 'ZSOUTGAS.BO', name: 'SOUTHERN GAS LTD.' },
  { symbol: 'TRADWIN.BO', name: 'TRADE WINGS LTD.' },
  { symbol: 'SWASTIVI.BO', name: 'SWASTI VINAYAKA SYNTHETICS LTD.' },
  { symbol: 'MADHUSE.BO', name: 'MADHUSUDAN SECURITIES LTD.' },
  { symbol: 'YAMNINV.BO', name: 'YAMINI INVESTMENTS COMPANY LTD.' },
  { symbol: 'ZSUBWAYF.BO', name: 'SUBWAY FINANCE & INVESTMENT CO.LTD.' },
  { symbol: 'ZARCOLEA.BO', name: 'ARCO LEASING LTD.' },
  { symbol: 'KUSUMEL.BO', name: 'KUSAM ELECTRICAL INDUSTRIES LTD.' },
  { symbol: 'JUMBFNL.BO', name: 'JUMBO FINANCE LTD.' },
  { symbol: 'SAKTHIFIN.BO', name: 'SAKTHI FINANCE LTD.' },
  { symbol: 'DHFL.BO', name: 'DEWAN HOUSING FINANCE CORPORATION LTD.' },
  { symbol: 'WEIZFIN.BO', name: 'WEIZMANN FINCORP LTD.' },
  { symbol: 'SATINDLTD.BO', name: 'SAT INDUSTRIES LTD.-$' },
  { symbol: 'JMDTELEFILM.BO', name: 'JMD TELEFILMS INDUSTRIES LTD.' },
  { symbol: 'TVOLCON.BO', name: 'TIVOLI CONSTRUCTION LTD.' },
  { symbol: 'SHIVTEX.BO', name: 'SHIVA TEXYARN LTD.' },
  { symbol: 'VBDESAI.BO', name: 'V.B.DESAI FINANCIAL SERVICES LTD.' },
  { symbol: 'QUADRANT.BO', name: 'Quadrant Televentures Limited-$' },
  { symbol: '1STCUS.BO', name: 'FIRST CUSTODIAN FUND (INDIA) LTD.' },
  { symbol: 'GALAXCP.BO', name: 'GALAXY CONSOLIDATED FINANCE LTD.' },
  { symbol: 'KAMANWALA.BO', name: 'KAMANWALA HOUSING CONSTRUCTION LTD.' },
  { symbol: 'KOTHARIFIN.BO', name: 'KOTHARI WORLD FINANCE LTD.' },
  { symbol: 'BAJRFIN.BO', name: 'BAJRANG FINANCE LTD.' },
  { symbol: 'ASYAINFO.BO', name: 'Asya Infosoft Limited' },
  { symbol: 'WSFIN.BO', name: 'WALL STREET FINANCE LTD.' },
  { symbol: 'REMITR.BO', name: 'REMI SECURITIES LTD.' },
  { symbol: 'RAASIENT.BO', name: 'RAASI ENTERPRISES LTD.' },
  { symbol: 'HIMALFD.BO', name: 'HIMALCHULI FOOD PRODUCTS LTD.' },
  { symbol: 'PARSHWANA.BO', name: 'PARSHWANATH CORPORATION LTD.' },
  { symbol: 'SURYAKR.BO', name: 'SURYAKRIPA FINANCE LTD.' },
  { symbol: 'MAHAINV.BO', name: 'MAHAMAYA INVESTMENTS LTD.' },
  { symbol: 'MUNCAPM.BO', name: 'MUNOTH CAPITAL MARKET LTD.' },
  { symbol: 'SRTRANSFIN.BO', name: 'SHRIRAM TRANSPORT FINANCE CO.LTD.' },
  { symbol: 'WHITELIO.BO', name: 'WHITE LION ASIA LTD.' },
  { symbol: 'SAGARSYST.BO', name: 'SAGAR SYSTECH LTD.' },
  { symbol: 'GRUH.BO', name: 'GRUH FINANCE LTD.' },
  { symbol: 'INDCEMCAP.BO', name: 'INDIA CEMENTS CAPITAL LTD.' },
  { symbol: 'KAILASH.BO', name: 'KAILASH AUTO FINANCE LTD.' },
  { symbol: 'ADMANUM.BO', name: 'AD-MANUM FINANCE LTD.' },
  { symbol: 'VISHWAFIN.BO', name: 'Vishwamitra Financial Services Limited' },
  { symbol: 'MEGLON.BO', name: 'MEGLON INFRA-REAL (INDIA) LTD.' },
  { symbol: 'MEHIF.BO', name: 'MEHTA INTEGRATED FINANCE LTD.' },
  { symbol: 'VIDEOIND.BO', name: 'VIDEOCON INDUSTRIES LTD.' },
  { symbol: 'INTRGLB.BO', name: 'INTER GLOBE FINANCE LTD.' },
  { symbol: 'DFLINFRA.BO', name: 'DFL INFRASTRUCTURE FINANCE LTD.' },
  { symbol: 'BALATECGL.BO', name: 'BALA TECHNO GLOBAL LTD.' },
  { symbol: 'MUNOTHI.BO', name: 'MUNOTH INVESTMENTS LTD.' },
  { symbol: 'SHRISTI.BO', name: 'SHRISTI INFRASTRUCTURE DEVELOPMENT CORPORATION LTD.' },
  { symbol: 'ATNINTER.BO', name: 'ATN INTERNATIONAL LTD.' },
  { symbol: 'DHARFIN.BO', name: 'DHARANI FINANCE LTD.' },
  { symbol: 'VCKCAP.BO', name: 'VCK CAPITAL MARKET SERVICES LTD.' },
  { symbol: 'BHARAT.BO', name: 'BHARAT BHUSHAN SHARE & COMMODITY BROKERS LTD.' },
  { symbol: 'CAPITALT.BO', name: 'CAPITAL TRUST LTD.' },
  { symbol: 'USHAKIRA.BO', name: 'USHAKIRAN FINANCE LTD.' },
  { symbol: 'VIVOBIOT.BO', name: 'VIVO BIO TECH LTD.' },
  { symbol: 'VEERHEALTH.BO', name: 'Veerhealth Care Limited' },
  { symbol: 'PANINDIAC.BO', name: 'PAN INDIA CORPORATION LTD.' },
  { symbol: 'SAHARAHOUS.BO', name: 'SAHARA HOUSINGFINA CORPORATION LTD.' },
  { symbol: 'NDASEC.BO', name: 'NDA SECURITIES LTD.' },
  { symbol: 'SUPRATRE.BO', name: 'Supra Trends Limited' },
  { symbol: 'GSBFIN.BO', name: 'GSB FINANCE LTD.' },
  { symbol: 'MORARKFI.BO', name: 'MORARKA FINANCE LTD.' },
  { symbol: 'PROFINC.BO', name: 'PRO FIN CAPITAL SERVICES LTD.' },
  { symbol: 'TIMESGTY.BO', name: 'TIMES GUARANTY LTD.' },
  { symbol: 'SODFC.BO', name: 'SOM DATT FINANCE CORPORATION LTD.' },
  { symbol: 'SAVFI.BO', name: 'SAVANI FINANCIALS LTD.' },
  { symbol: 'REGTRUS.BO', name: 'REGENCY TRUST LTD.' },
  { symbol: 'LIBORDFIN.BO', name: 'Libord Finance Ltd' },
  { symbol: 'PALSOFT.BO', name: 'PALSOFT INFOSYSTEMS LTD.' },
  { symbol: 'YASHMGM.BO', name: 'YASH MANAGEMENT & SATELLITE LTD.' },
  { symbol: 'ARIHCAPM.BO', name: 'ARIHANT CAPITAL MARKETS LTD.' },
  { symbol: 'BIRSHLEDU.BO', name: 'BIRLA SHLOKA EDUTECH LTD.' },
  { symbol: 'ISLCONSUL.BO', name: 'ISL CONSULTING LTD.' },
  { symbol: 'JIKIND.BO', name: 'JIK INDUSTRIES LTD.' },
  { symbol: 'RRFIN.BO', name: 'RR FINANCIAL CONSULTANTS LTD.' },
  { symbol: 'IMCAP.BO', name: 'IM+ Capitals Limited' },
  { symbol: 'MADHURC.BO', name: 'MADHUR CAPITAL & FINANCE LTD.' },
  { symbol: 'SANGHCO.BO', name: 'SANGHI CORPORATE SERVICES LTD.' },
  { symbol: 'WISEC.BO', name: 'WISEC GLOBAL LTD.' },
  { symbol: 'GEMOIL.BO', name: 'GEMMIA OILTECH (INDIA) LTD.' },
  { symbol: 'SUGALDAM.BO', name: 'SUGAL & DAMANI SHARE BROKERS LTD.' },
  { symbol: 'NETTLINX.BO', name: 'NETTLINX LTD.' },
  { symbol: 'BGIL.BO', name: 'BGIL FILMS & TECHNOLOGIES LTD.' },
  { symbol: 'IFLPROMOT.BO', name: 'IFL PROMOTERS LTD.' },
  { symbol: 'MATHEWE.BO', name: 'MATHEW EASOW RESEARCH SECURITIES LTD.' },
  { symbol: 'WARNER.BO', name: 'WARNER MULTIMEDIA LTD.' },
  { symbol: 'AJCON.BO', name: 'AJCON GLOBAL SERVICES LTD.' },
  { symbol: 'CHRTEDCA.BO', name: 'CHARTERED CAPITAL & INVESTMENT LTD.' },
  { symbol: 'BHAGYFN.BO', name: 'BHAGYASHREE LEASING & FINANCE LTD.' },
  { symbol: 'STANCAP.BO', name: 'STANDARD CAPITAL MARKETS LTD.' },
  { symbol: 'PARSHINV.BO', name: 'PARSHARTI INVESTMENT LTD.' },
  { symbol: 'ACTIONFI.BO', name: 'ACTION FINANCIAL SERVICES (INDIA) LTD.' },
  { symbol: 'CUBIFIN.BO', name: 'CUBICAL FINANCIAL SERVICES LTD.' },
  { symbol: 'RELICTEC.BO', name: 'RELIC TECHNOLOGIES LTD.' },
  { symbol: 'ESCORTSFIN.BO', name: 'ESCORTS FINANCE LTD.' },
  { symbol: 'CAPMANFI.BO', name: 'CAPMAN FINANCIALS LTD.' },
  { symbol: 'BALFC.BO', name: 'BAID LEASING AND FINANCE CO.LTD.' },
  { symbol: 'VIPUL.BO', name: 'VIPUL LTD.' },
  { symbol: 'KZLFIN.BO', name: 'K.Z.LEASING & FINANCE LTD.' },
  { symbol: 'TRCFIN.BO', name: 'TRC FINANCIAL SERVICES LTD.' },
  { symbol: 'PASUFIN.BO', name: 'PASUPATI FINCAP LTD.' },
  { symbol: 'USHDI.BO', name: 'USHDEV INTERNATIONAL LTD.' },
  { symbol: 'MEHSECU.BO', name: 'MEHTA SECURITIES LTD.' },
  { symbol: 'MEHTAHG.BO', name: 'MEHTA HOUSING FINANCE LTD.' },
  { symbol: 'CHOKSEC.BO', name: 'CHOKHANI SECURITIES LTD.' },
  { symbol: 'ABIRAFN.BO', name: 'ABIRAMI FINANCIAL SERVICES (INDIA) LTD.' },
  { symbol: 'MANSIFIN.BO', name: 'MANSI FINANCE (CHENNAI) LTD.' },
  { symbol: 'SEVENHILL.BO', name: 'Seven Hill Industries Limited' },
  { symbol: 'UPASAFN.BO', name: 'UPASANA FINANCE LTD.' },
  { symbol: 'MUTHTFN.BO', name: 'MUTHOOT CAPITAL SERVICES LTD.' },
  { symbol: 'AMANITRA.BO', name: 'AMANI TRADING & EXPORTS LTD.' },
  { symbol: 'SOBME.BO', name: 'SOBHAGYA MERCHANTILE LTD.' },
  { symbol: 'CNIRESLTD.BO', name: 'CNI RESEARCH LTD.' },
  { symbol: 'WINROC.BO', name: 'WINRO COMMERCIAL (INDIA) LTD.' },
  { symbol: 'NDMETAL.BO', name: 'N.D.METAL INDUSTRIES LTD.' },
  { symbol: 'INERTIAST.BO', name: 'INERTIA STEEL LTD.' },
  { symbol: 'PHTRADING.BO', name: 'PH TRADING LTD.' },
  { symbol: 'ASWTR.BO', name: 'AASWA TRADING & EXPORTS LTD.' },
  { symbol: 'ROYALIND.BO', name: 'Royal India Corporation Limited' },
  { symbol: 'SMIL.BO', name: 'SPLASH MEDIA & INFRA LTD.' },
  { symbol: 'ZSANMCOM.BO', name: 'SANMITRA COMMERCIAL LTD.' },
  { symbol: 'AYOME.BO', name: 'AYOKI MERCANTILE LTD.' },
  { symbol: 'VISTR.BO', name: 'VISHVPRABHA TRADING LTD.' },
  { symbol: 'MRUTR.BO', name: 'MRUGESH TRADING LTD.' },
  { symbol: 'ANSHNCO.BO', name: 'ANSHUNI COMMERCIALS LTD.' },
  { symbol: 'CRANESSOFT.BO', name: 'CRANES SOFTWARE INTERNATIONAL LTD.' },
  { symbol: 'PUNITCO.BO', name: 'PUNIT COMMERCIALS LTD.' },
  { symbol: 'TRIPR.BO', name: 'TRIOCHEM PRODUCTS LTD.' },
  { symbol: 'NIDHGRN.BO', name: 'NIDHI GRANITES LTD.' },
  { symbol: 'PROAIMENT.BO', name: 'Proaim Enterprises Ltd' },
  { symbol: 'AVIVA.BO', name: 'AVIVA INDUSTRIES LTD.' },
  { symbol: 'TWIROST.BO', name: 'TWIN ROSES TRADES & AGENCIES LTD.' },
  { symbol: 'DELINFRA.BO', name: 'Delma Infrastructure Limited' },
  { symbol: 'SIGNETIND.BO', name: 'Signet Industries Limited' },
  { symbol: 'ENSSI.BO', name: 'ENSA STEEL INDUSTRIES LTD.' },
  { symbol: 'AVANCE.BO', name: 'AVANCE TECHNOLOGIES LTD.' },
  { symbol: 'SPECMKT.BO', name: 'SPECULAR MARKETING & FINANCING LTD.' },
  { symbol: 'TERRAREAL.BO', name: 'Terraform Realstate Limited' },
  { symbol: '8KMILES.BO', name: '8K MILES SOFTWARE SERVICES LTD.' },
  { symbol: 'EXTCO.BO', name: 'EXTOL COMMERCIAL LTD.' },
  { symbol: 'MATRUTR.BO', name: 'MATRU-SMRITI TRADERS LTD.' },
  { symbol: 'MATRAREAL.BO', name: 'MATRA REALTY LTD.' },
  { symbol: 'CAPRICORN.BO', name: 'CAPRICORN SYSTEMS GLOBAL SOLUTIONS LTD.' },
  { symbol: 'VAMA.BO', name: 'VAMA INDUSTRIES LTD.' },
  { symbol: 'BENTCOM.BO', name: 'BENTLEY COMMERCIAL ENTERPRISES LTD.' },
  { symbol: 'SILVERO.BO', name: 'SILVER OAK COMMERCIAL LTD.' },
  { symbol: 'CLASELE.BO', name: 'CLASSIC ELECTRICALS LTD.' },
  { symbol: 'VORACON.BO', name: 'VORA CONSTRUCTIONS LTD.' },
  { symbol: 'FINAVENT.BO', name: 'FINAVENTURE CAPITAL LTD.' },
  { symbol: 'TARCF.BO', name: 'TARRIF CINE & FINANCE LTD.' },
  { symbol: 'VERITAS.BO', name: 'VERITAS (INDIA) LTD.' },
  { symbol: 'JAYTEX.BO', name: 'JAYBHARAT TEXTILES & REAL ESTATE LTD.' },
  { symbol: 'ZNIVITRD.BO', name: 'NIVI TRADING LTD.' },
  { symbol: 'ASHCAP.BO', name: 'ASHIRWAD CAPITAL LTD.' },
  { symbol: 'SVARTCORP.BO', name: 'Swasti Vinayaka Art And Heritage Corporation Ltd' },
  { symbol: 'BAJGLOB.BO', name: 'BAJAJ GLOBAL LTD.' },
  { symbol: 'TASHIND.BO', name: 'TASHI INDIA LTD.' },
  { symbol: 'ARONICOMM.BO', name: 'ARONI COMMERCIALS LTD.-$' },
  { symbol: 'NNTL.BO', name: 'N2N Technologies Limited' },
  { symbol: 'SHIRPUR-G.BO', name: 'SHIRPUR GOLD REFINERY LTD.' },
  { symbol: 'ZSPEEDCO.BO', name: 'SPEEDAGE COMMERCIALS LTD.' },
  { symbol: 'BHAGYNAGAR.BO', name: 'BHAGYANAGAR INDIA LTD.' },
  { symbol: 'WHELTEX.BO', name: 'WHEEL & AXLE TEXTILES LTD.' },
  { symbol: 'STERLINBIO.BO', name: 'STERLING BIOTECH LTD.' },
  { symbol: 'CHMBBRW.BO', name: 'CHAMBAL BREWERIES & DISTILLERIES LTD.' },
  { symbol: 'TYPHOON.BO', name: 'TYPHOON HOLDINGS LTD.' },
  { symbol: 'ROSETEX.BO', name: 'ROSEKAMAL TEXTILES LTD.' },
  { symbol: 'BIJLTEX.BO', name: 'BIJLEE TEXTILES LTD.' },
  { symbol: 'JARITEX.BO', name: 'JARIGOLD TEXTILES LTD.' },
  { symbol: 'BIRLACAP.BO', name: 'BIRLA CAPITAL & FINANCIAL SERVICES LTD.' },
  { symbol: 'MAHSHRE.BO', name: 'MAHASHREE TRADING LTD.' },
  { symbol: 'CESL.BO', name: 'CES Limited' },
  { symbol: 'ARAVALIS.BO', name: 'ARAVALI SECURITIES & FINANCE LTD.' },
  { symbol: 'SWORDEDGE.BO', name: 'Sword-Edge Commercials Limited' },
  { symbol: 'ZSHERAPR.BO', name: 'SHERATON PROPERTIES & FINANCE LTD.' },
  { symbol: 'ENNORE.BO', name: 'ENNORE COKE LTD.' },
  { symbol: 'INNOVENT.BO', name: 'INNOVENTIVE VENTURE LTD.' },
  { symbol: 'MAGANTR.BO', name: 'MAGNANIMOUS TRADE & FINANCE LTD.' },
  { symbol: 'CRESSAN.BO', name: 'CRESSANDA SOLUTIONS LTD.' },
  { symbol: 'ZNIVEMER.BO', name: 'NIVEDITA MERCANTILE & FINANCING LTD.' },
  { symbol: 'KAPASHI.BO', name: 'KAPASHI COMMERCIALS LTD.' },
  { symbol: 'IPOWER.BO', name: 'I-POWER SOLUTIONS INDIA LTD.' },
  { symbol: 'UNIWSEC.BO', name: 'UNIWORTH SECURITIES LTD.' },
  { symbol: 'RAJSAN.BO', name: 'Rajsanket Realty Limited' },
  { symbol: 'SPECTACLE.BO', name: 'Spectacle Ventures Ltd' },
  { symbol: 'MERCTRD.BO', name: 'MERCURY TRADE LINKS LTD.' },
  { symbol: 'TRITRADE.BO', name: 'TRINITY TRADELINK LIMITED' },
  { symbol: 'NIRAVCOM.BO', name: 'NIRAV COMMERCIALS LTD.' },
  { symbol: 'APOLLOFI.BO', name: 'APOLLO FINVEST (INDIA) LTD.' },
  { symbol: 'ESQRMON.BO', name: 'ESQUIRE MONEY GUARANTEES LTD.' },
  { symbol: 'ENBETRD.BO', name: 'ENBEE TRADE & FINANCE LTD.' },
  { symbol: 'GANONTR.BO', name: 'GANON TRADING FINANCE CO.LTD.' },
  { symbol: 'DEVITRD.BO', name: 'DEVINSU TRADING LTD.' },
  { symbol: 'SHRJAGP.BO', name: 'SHRI JAGDAMBA POLYMERS LTD.' },
  { symbol: 'PUNCTRD.BO', name: 'PUNCTUAL TRADING LTD.' },
  { symbol: 'SHRGLTR.BO', name: 'SHREE GLOBAL TRADEFIN LTD.' },
  { symbol: 'SANTOWIN.BO', name: 'SANTOWIN CORPORATION LTD.' },
  { symbol: 'BETXIND.BO', name: 'BETEX INDIA LTD.' },
  { symbol: 'GYTRIPA.BO', name: 'GAYATRI TISSUE & PAPERS LTD.' },
  { symbol: 'POLYTEX.BO', name: 'POLYTEX INDIA LTD.' },
  { symbol: 'DHANCOT.BO', name: 'DHANLAXMI COTEX LTD.' },
  { symbol: 'REMIELEK.BO', name: 'Remi Elektrotechnik Limited' },
  { symbol: 'OASISEC.BO', name: 'OASIS SECURITIES LTD.' },
  { symbol: 'GARNETINT.BO', name: 'GARNET INTERNATIONAL LTD.' },
  { symbol: 'SHALPRO.BO', name: 'SHALIMAR PRODUCTIONS LTD.' },
  { symbol: 'MEENST.BO', name: 'MEENAKSHI STEEL INDUSTRIES LTD.' },
  { symbol: 'VARUNME.BO', name: 'VARUN MERCANTILE LTD.' },
  { symbol: 'SUPER.BO', name: 'SUPER SALES INDIA LTD.-$' },
  { symbol: 'SEQUENT.BO', name: 'SEQUENT SCIENTIFIC LTD.' },
  { symbol: 'ASAHINFRA.BO', name: 'ASAHI INFRASTRUCTURE & PROJECTS LTD.' },
  { symbol: 'RTEXPO.BO', name: 'R.T.EXPORTS LTD.-$' },
  { symbol: 'AVANTI.BO', name: 'AVANTI FEEDS LTD.-$' },
  { symbol: 'GUJNRECOKE.BO', name: 'GUJARAT NRE COKE LTD.' },
  { symbol: 'ZODJRDMKJ.BO', name: 'ZODIAC-JRD-MKJ LTD.' },
  { symbol: 'SITAENT.BO', name: 'SITA ENTERPRISES LTD.' },
  { symbol: 'PULSRIN.BO', name: 'PULSAR INTERNATIONAL LTD.' },
  { symbol: 'UNIMOVR.BO', name: 'UNIMODE OVERSEAS LTD.' },
  { symbol: 'KEYCORPSER.BO', name: 'KEYNOTE CORPORATE SERVICES LTD.' },
  { symbol: 'MACINTR.BO', name: 'MACRO (INTERNATIONAL) EXPORTS LTD.' },
  { symbol: 'HARIAEXPO.BO', name: 'HARIA EXPORTS LTD.-$' },
  { symbol: 'BHANDHOS.BO', name: 'BHANDARI HOSIERY EXPORTS LTD.' },
  { symbol: 'RLF.BO', name: 'RLF LTD.' },
  { symbol: 'REGENTRP.BO', name: 'Regent Enterprises Ltd' },
  { symbol: 'SAVERA.BO', name: 'SAVERA INDUSTRIES LTD.' },
  { symbol: 'VBCFERROQ.BO', name: 'VBC FERRO ALLOYS LTD.-$' },
  { symbol: 'TATASPONGE.BO', name: 'TATA SPONGE IRON LTD.' },
  { symbol: 'ZJEETMAC.BO', name: 'JEET MACHINE TOOLS LTD.' },
  { symbol: 'NBVENTURES.BO', name: 'NAVA BHARAT VENTURES LTD.' },
  { symbol: 'ZHINUDYP.BO', name: 'HINDUSTHAN UDYOG LTD.' },
  { symbol: 'REMIEDEL.BO', name: 'Remi Edelstahl Tubulars Limited' },
  { symbol: 'GSAUTO.BO', name: 'G.S.AUTO INTERNATIONAL LTD.' },
  { symbol: 'TRANSFRE.BO', name: 'TRANS-FREIGHT CONTAINERS LTD.' },
  { symbol: 'SHBCLQ.BO', name: 'SHIVALIK BIMETAL CONTROLS LTD.' },
  { symbol: 'AMFORG.BO', name: 'AMFORGE INDUSTRIES LTD.' },
  { symbol: 'ABCGAS.BO', name: 'ABC GAS (INTERNATIONAL) LTD.' },
  { symbol: 'ORICON.BO', name: 'ORICON ENTERPRISES LTD.-$' },
  { symbol: 'BALASORE.BO', name: 'BALASORE ALLOYS LTD.' },
  { symbol: 'ACROW.BO', name: 'ACROW INDIA LTD.' },
  { symbol: 'STINDIA.BO', name: 'STI INDIA LTD.' },
  { symbol: 'STLSTRINF.BO', name: 'STEEL STRIPS INFRASTRUCTURES LTD.' },
  { symbol: 'NATNLSTEEL.BO', name: 'NATIONAL STEEL & AGRO INDUSTRIES LTD.' },
  { symbol: 'UTTAMSTL.BO', name: 'UTTAM GALVA STEELS LTD.' },
  { symbol: 'JAYUSH.BO', name: 'JAY USHIN LTD.' },
  { symbol: 'MUKESTL.BO', name: 'MUKESH STEELS LTD.' },
  { symbol: 'PREMPIPES.BO', name: 'PREMIER PIPES LTD.' },
  { symbol: 'RUCHISTR.BO', name: 'RUCHI STRIPS & ALLOYS LTD.' },
  { symbol: 'MDRNSTL.BO', name: 'MODERN STEELS LTD.-$' },
  { symbol: 'SYNTHFO.BO', name: 'SYNTHIKO FOILS LTD.' },
  { symbol: 'GOLKONDA.BO', name: 'Golkonda Aluminium Extrusions Ltd-$' },
  { symbol: 'BHUWALST.BO', name: 'BHUWALKA STEEL INDUSTRIES LTD.-$' },
  { symbol: 'METALFORGE.BO', name: 'Metalyst Forgings Limited' },
  { symbol: 'GUJTLRM.BO', name: 'GUJARAT TOOLROOM LTD.' },
  { symbol: 'COCHINM.BO', name: 'COCHIN MINERALS & RUTILE LTD.-$' },
  { symbol: 'PARINFRA.BO', name: 'Parab Infra Limited' },
  { symbol: 'ISWL.BO', name: 'INDIA STEEL WORKS LTD.' },
  { symbol: 'RJKMRFR.BO', name: 'RAJKUMAR FORGE LTD.' },
  { symbol: 'VALLABHSQ.BO', name: 'VALLABH STEELS LTD.-$' },
  { symbol: 'ASHIS.BO', name: 'ASHIANA ISPAT LTD.' },
  { symbol: 'PMTELELIN.BO', name: 'P.M.TELELINNKS LTD.' },
  { symbol: 'PENNARALUM.BO', name: 'PENNAR ALUMINIUM CO.LTD.' },
  { symbol: 'SMPL.BO', name: 'SUJANA METAL PRODUCTS LTD.' },
  { symbol: 'SMFIL.BO', name: 'Smiths & Founders (India) Limited' },
  { symbol: 'BLOIN.BO', name: 'BLOOM INDUSTRIES LTD.' },
  { symbol: 'TRINETRA.BO', name: 'TRINETRA CEMENT LTD.' },
  { symbol: 'PARTHAL.BO', name: 'PARTH ALUMINIUM LTD.' },
  { symbol: 'TATAMETALI.BO', name: 'TATA METALIKS LTD.' },
  { symbol: 'MONNETISPA.BO', name: 'MONNET ISPAT & ENERGY LTD.' },
  { symbol: 'ELANGO.BO', name: 'ELANGO INDUSTRIES LTD.' },
  { symbol: 'KANSHST.BO', name: 'KANISHK STEEL INDUSTRIES LTD.' },
  { symbol: 'MAHALXSE.BO', name: 'MAHALAXMI SEAMLESS LTD.-$' },
  { symbol: 'SIMPLEXCAS.BO', name: 'SIMPLEX CASTINGS LTD.' },
  { symbol: 'SSWRL.BO', name: 'SHREE STEEL WIRE ROPES LTD.' },
  { symbol: 'SOUTHMG.BO', name: 'SOUTHERN MAGNESIUM & CHEMICALS LTD.' },
  { symbol: 'BAROEXT.BO', name: 'BARODA EXTRUSION LTD.' },
  { symbol: 'GUJCONT.BO', name: 'GUJARAT CONTAINERS LTD.' },
  { symbol: 'PANCHMAHQ.BO', name: 'PANCHMAHAL STEEL LTD.' },
  { symbol: 'ADITYA.BO', name: 'ADITYA ISPAT LTD.' },
  { symbol: 'SRIND.BO', name: 'S.R.INDUSTRIES LTD.' },
  { symbol: 'PITTILAM.BO', name: 'PITTI LAMINATIONS LTD.-$' },
  { symbol: 'GLITTEKG.BO', name: 'GLITTEK GRANITES LTD.' },
  { symbol: 'VARDHINDQ.BO', name: 'VARDHMAN INDUSTRIES LTD.-$' },
  { symbol: 'TNSTLTU.BO', name: 'TAMILNADU STEEL TUBES LTD.' },
  { symbol: 'MSCTC.BO', name: 'MARDIA SAMYOUNG CAPILLARY TUBES COMPANY LTD.' },
  { symbol: 'SHRDAIS.BO', name: 'SHARDA ISPAT LTD.' },
  { symbol: 'REALSTR.BO', name: 'REAL STRIPS LTD.-$' },
  { symbol: 'NOVIS.BO', name: 'NOVA IRON & STEEL LTD.' },
  { symbol: 'STERPOW.BO', name: 'Sterling Powergensys Limited' },
  { symbol: 'FFPL.BO', name: 'FOUNDRY FUEL PRODUCTS LTD.' },
  { symbol: 'SB&AMP;TINTL.BO', name: 'SB & T INTERNATIONAL LTD.' },
  { symbol: 'SURANAIND.BO', name: 'SURANA INDUSTRIES LTD.' },
  { symbol: 'SRIPIPES.BO', name: 'Srikalahasthi Pipes Limited' },
  { symbol: 'PRESHAMET.BO', name: 'Presha Metallurgical Ltd.' },
  { symbol: 'TULSYAN.BO', name: 'TULSYAN NEC LTD.-$' },
  { symbol: 'AXELPOLY.BO', name: 'AXEL POLYMERS LTD.' },
  { symbol: 'MARGPROIN.BO', name: 'MARG PROJECTS AND INFRASTRUCTURE LTD.' },
  { symbol: 'NEYVELILIG.BO', name: 'NEYVELI LIGNITE CORPORATION LTD.' },
  { symbol: 'SPECTRA.BO', name: 'SPECTRA INDUSTRIES LTD.' },
  { symbol: 'JMTAUTOLTD.BO', name: 'JMT AUTO LTD.' },
  { symbol: 'KAJARIR.BO', name: 'KIC METALIKS LTD.' },
  { symbol: 'SOLIDSTON.BO', name: 'SOLID STONE COMPANY LTD.' },
  { symbol: 'KUMARWI.BO', name: 'KUMAR WIRE CLOTH MANUFACTURING COMPANY LTD.' },
  { symbol: 'SHILGRAVQ.BO', name: 'SHILP GRAVURES LTD.-$' },
  { symbol: 'WHITEDIA.BO', name: 'WHITE DIAMOND INDUSTRIES LTD.' },
  { symbol: 'MFSINTRCRP.BO', name: 'MFS INTERCORP LTD.' },
  { symbol: 'HIMGRANI.BO', name: 'HIMALAYA GRANITES LTD.-$' },
  { symbol: 'RANJEEV.BO', name: 'RANJEEV ALLOYS LTD.' },
  { symbol: 'HIMFIBP.BO', name: 'HIMACHAL FIBRES LTD.' },
  { symbol: 'RAJKSYN.BO', name: 'RAJKAMAL SYNTHETICS LTD.' },
  { symbol: 'DEEPAKSP.BO', name: 'DEEPAK SPINNERS LTD.-$' },
  { symbol: 'JBFIND.BO', name: 'JBF INDUSTRIES LTD.' },
  { symbol: 'EVERTEX.BO', name: 'Evergreen Textiles Limited' },
  { symbol: 'PBMPOLY.BO', name: 'PBM POLYTEX LTD.-$' },
  { symbol: 'ADINATH.BO', name: 'ADINATH TEXTILES LTD.' },
  { symbol: 'GUPTSYN.BO', name: 'GUPTA SYNTHETICS LTD.' },
  { symbol: 'ESKAY.BO', name: 'ESKAY K\'N\'IT (INDIA) LTD.' },
  { symbol: 'KONARKSY.BO', name: 'KONARK SYNTHETIC LTD.' },
  { symbol: 'SURYVANSP.BO', name: 'SURYAVANSHI SPINNING MILLS LTD.-$' },
  { symbol: 'UNIWORTH.BO', name: 'UNIWORTH LTD.' },
  { symbol: 'WELSPUNIND.BO', name: 'WELSPUN INDIA LTD.' },
  { symbol: 'INDIANACRY.BO', name: 'INDIAN ACRYLICS LTD.' },
  { symbol: 'CEETAIN.BO', name: 'CEETA INDUSTRIES LTD.' },
  { symbol: 'RISHYRN.BO', name: 'RISHAB SPECIAL YARNS LTD.' },
  { symbol: 'STCORP.BO', name: 'S&T CORPORATION LTD.' },
  { symbol: 'BALATECIN.BO', name: 'BALA TECHNO INDUSTRIES LTD.' },
  { symbol: 'BINNY.BO', name: 'BINNY LTD.' },
  { symbol: 'KLIFESTYL.BO', name: 'K-Lifestyle & Industries Limited' },
  { symbol: 'IKAB.BO', name: 'IKAB SECURITIES & INVESTMENT LTD.' },
  { symbol: 'SNSTEXTIL.BO', name: 'SNS TEXTILES LTD.' },
  { symbol: 'SEASONST.BO', name: 'SEASONS TEXTILES LTD.-$' },
  { symbol: 'ZENIFIB.BO', name: 'ZENITH FIBRES LTD.-$' },
  { symbol: 'BHILSPIN.BO', name: 'BHILWARA SPINNERS LTD.' },
  { symbol: 'AARVEEDEN.BO', name: 'AARVEE DENIMS & EXPORTS LTD.' },
  { symbol: 'HARYANATEX.BO', name: 'Haryana Texprints (Overseas) Limited' },
  { symbol: 'VIPPYSP.BO', name: 'VIPPY SPINPRO LTD.' },
  { symbol: 'JAIHINDS.BO', name: 'JAIHIND SYNTHETICS LTD.' },
  { symbol: 'RAGHUSYN.BO', name: 'RAGHUVIR SYNTHETICS LTD.' },
  { symbol: 'JATTAINDUS.BO', name: 'JATTASHANKAR INDUSTIES LTD.' },
  { symbol: 'KAMADGIRI.BO', name: 'KAMADGIRI FASHION LTD.-$' },
  { symbol: 'OMNITEX.BO', name: 'OMNITEX INDUSTRIES (INDIA) LTD.' },
  { symbol: 'PATSPINLTD.BO', name: 'PATSPIN INDIA LTD.' },
  { symbol: 'OBRSESY.BO', name: 'OVERSEAS SYNTHETICS LTD.' },
  { symbol: 'NEOINFRA.BO', name: 'NEO INFRACON LTD.' },
  { symbol: 'SSK.BO', name: 'SSK Lifestyles Limited' },
  { symbol: 'EVERLON.BO', name: 'EVERLON SYNTHETICS LTD.' },
  { symbol: 'CITIZYN.BO', name: 'CITIZEN YARNS LTD.' },
  { symbol: 'YARNSYN.BO', name: 'YARN SYNDICATE LTD.' },
  { symbol: 'GUJCOTEX.BO', name: 'GUJARAT COTEX LTD.' },
  { symbol: 'AJIL.BO', name: 'Atlas Jewellery India Limited' },
  { symbol: 'GARWSYN.BO', name: 'GARWARE SYNTHETICS LTD.' },
  { symbol: 'SHARDFI.BO', name: 'SHARAD FIBRES & YARN PROCESSORS LTD.' },
  { symbol: 'SARUPINDUS.BO', name: 'SARUP INDUSTRIES LTD.-$' },
  { symbol: 'OXFORDIN.BO', name: 'OXFORD INDUSTRIES LTD.' },
  { symbol: 'HINDADH.BO', name: 'HINDUSTAN ADHESIVES LTD.' },
  { symbol: 'BPTEX.BO', name: 'Blue Pearl Texspin Limited' },
  { symbol: 'SRIKPRIND.BO', name: 'SRI KPR INDUSTRIES LTD.' },
  { symbol: 'JYOTIRES.BO', name: 'JYOTI RESINS & ADHESIVES LTD.' },
  { symbol: 'SOUTLAT.BO', name: 'SOUTHERN LATEX LTD.' },
  { symbol: 'OSWAYRN.BO', name: 'OSWAL YARNS LTD.' },
  { symbol: 'WINSOMTX.BO', name: 'WINSOME TEXTILE INDUSTRIES LTD.-$' },
  { symbol: 'FAIRDSY.BO', name: 'FAIR DEAL FILAMENTS LTD.-$' },
  { symbol: 'ASAHIIND.BO', name: 'Asahi Industries Limited' },
  { symbol: 'THAMBBI.BO', name: 'THAMBBI MODERN SPINNING MILLS LTD.' },
  { symbol: 'POLTC.BO', name: 'POLYGENTA TECHNOLOGIES LTD.' },
  { symbol: 'SAINTGOBAIN.BO', name: 'SAINT-GOBAIN SEKURIT INDIA LTD.' },
  { symbol: 'MADHUDIN.BO', name: 'MADHUSUDAN INDUSTRIES LTD.' },
  { symbol: 'SRIVAJRA.BO', name: 'SRI VAJRA GRANITES LTD.' },
  { symbol: 'RESTILE.BO', name: 'RESTILE CERAMICS LTD.' },
  { symbol: 'VERTICLIND.BO', name: 'Vertical Industries Ltd' },
  { symbol: 'RAMMA.BO', name: 'RAMMAICA (INDIA) LTD.' },
  { symbol: 'HINDNATGLS.BO', name: 'HINDUSTHAN NATIONAL GLASS & INDUSTRIES LTD.-$' },
  { symbol: 'SARDAPLY.BO', name: 'SARDA PLYWOOD INDUSTRIES LTD.' },
  { symbol: 'MANGTIMBER.BO', name: 'MANGALAM TIMBER PRODUCTS LTD.' },
  { symbol: 'AGIOPAPER.BO', name: 'AGIO PAPER & INDUSTRIES LTD.-$' },
  { symbol: 'YASHPPR.BO', name: 'YASH PAPERS LTD.-$' },
  { symbol: 'SARDAPPR.BO', name: 'SARDA PAPERS LTD.' },
  { symbol: 'SOMAPPR.BO', name: 'SOMA PAPERS & INDUSTRIES LTD.' },
  { symbol: 'ARCPR.BO', name: 'ARROW COATED PRODUCTS LTD.' },
  { symbol: 'JUMBO.BO', name: 'JUMBO BAG LTD.' },
  { symbol: 'NRAGRINDQ.BO', name: 'N.R.AGARWAL INDUSTRIES LTD.' },
  { symbol: 'SRPML.BO', name: 'SHREE RAJESHWARANAND PAPER MILLS LTD.' },
  { symbol: 'PDUMJEIND.BO', name: 'PUDUMJEE INDUSTRIES LTD.' },
  { symbol: 'SANPA.BO', name: 'SANGAL PAPERS LTD.' },
  { symbol: 'VENTURA.BO', name: 'VENTURA TEXTILES LTD.' },
  { symbol: 'SHKARTP.BO', name: 'SHREE KARTHIK PAPERS LTD.' },
  { symbol: 'STHINPA.BO', name: 'SOUTH INDIA PAPER MILLS LTD.' },
  { symbol: 'SCANDENT.BO', name: 'Scandent Imaging Limited' },
  { symbol: 'RIR.BO', name: 'RUTTONSHA INTERNATIONAL RECTIFIER LTD.' },
  { symbol: 'ADORWELD.BO', name: 'ADOR WELDING LTD.' },
  { symbol: 'IDM.BO', name: 'INTERNATIONAL DATA MANAGEMENT LTD.' },
  { symbol: 'SALZER.BO', name: 'SALZER ELECTRONICS LTD.-$' },
  { symbol: 'JETKINGQ.BO', name: 'JETKING INFOTRAIN LTD.' },
  { symbol: 'INDAGIV.BO', name: 'IND-AGIV COMMERCE LTD.' },
  { symbol: 'HIGHGROUND.BO', name: 'High Ground Enterprise Ltd' },
  { symbol: 'APLAB.BO', name: 'APLAB LTD.-$' },
  { symbol: 'PCS.BO', name: 'PCS TECHNOLOGY LTD.' },
  { symbol: 'MOSERBAER.BO', name: 'MOSER BAER INDIA LTD.' },
  { symbol: 'ZENITHCOMP.BO', name: 'ZENITH COMPUTERS LTD.' },
  { symbol: 'SPELS.BO', name: 'SPEL SEMICONDUCTOR LTD.' },
  { symbol: 'KLKELEC.BO', name: 'KLK Electrical Ltd' },
  { symbol: 'SWITCHTE.BO', name: 'SWITCHING TECHNOLOGIES GUNTHER LTD.' },
  { symbol: 'SPICEMOBI.BO', name: 'Spice Mobility Limited-$' },
  { symbol: 'SUJANAUNI.BO', name: 'SUJANA UNIVERSAL INDUSTRIES LTD.-$' },
  { symbol: 'TRENDELEC.BO', name: 'TREND ELECTRONICS LTD.-$' },
  { symbol: 'PAEL.BO', name: 'PAE LTD.' },
  { symbol: 'CALCOM.BO', name: 'CALCOM VISION LTD.' },
  { symbol: 'DYNAVSN.BO', name: 'DYNAVISION LTD.' },
  { symbol: 'BCCFUBA.BO', name: 'BCC FUBA INDIA LTD.' },
  { symbol: 'PRECISIO.BO', name: 'PRECISION ELECTRONICS LTD.' },
  { symbol: 'FINELINE.BO', name: 'FINE-LINE CIRCUITS LTD.' },
  { symbol: 'HBLPOWER.BO', name: 'HBL POWER SYSTEMS LTD.-$' },
  { symbol: 'GUJPOLYA.BO', name: 'GUJARAT POLY-AVX ELECTRONICS LTD.' },
  { symbol: 'PHOENIXLL.BO', name: 'Phoenix Lamps Limited' },
  { symbol: 'SAVINFOCO.BO', name: 'SAVANT INFOCOMM LTD.' },
  { symbol: 'CMI.BO', name: 'CMI LTD.' },
  { symbol: 'MOTHERSUMI.BO', name: 'MOTHERSON SUMI SYSTEMS LTD.' },
  { symbol: 'ACIIN.BO', name: 'ACI INFOCOM LTD.' },
  { symbol: 'INCAP.BO', name: 'INCAP LTD.' },
  { symbol: 'GUJINTRX.BO', name: 'GUJARAT INTRUX LTD.-$' },
  { symbol: 'MUKSTRI.BO', name: 'MUKESH STRIPS LTD.' },
  { symbol: 'VINTRON.BO', name: 'VINTRON INFORMATICS LTD.' },
  { symbol: 'PANELEC.BO', name: 'PAN ELECTRONICS INDIA LTD.' },
  { symbol: 'VXLINSTR.BO', name: 'VXL INSTRUMENTS LTD.' },
  { symbol: 'SUNSOUI.BO', name: 'SUN SOURCE (INDIA) LTD.' },
  { symbol: 'LEENEE.BO', name: 'LEE & NEE SOFTWARES (EXPORTS) LTD.' },
  { symbol: 'PATELSAI.BO', name: 'PATELS AIRTEMP (INDIA) LTD.' },
  { symbol: 'INTEGSW.BO', name: 'INTEGRA SWITCHGEAR LTD.' },
  { symbol: 'ATHENAGLO.BO', name: 'Athena Global Technologies Ltd-$' },
  { symbol: 'INSOE.BO', name: 'INNOVATION SOFTWARE EXPORTS LTD.' },
  { symbol: 'DUTRON.BO', name: 'DUTRON POLYMERS LTD.-$' },
  { symbol: 'MAGNAELQ.BO', name: 'MAGNA ELECTRO CASTINGS LTD.-$' },
  { symbol: 'LINAKS.BO', name: 'LINAKS MICROELECTRONICS LTD.' },
  { symbol: 'KOATOOLIN.BO', name: 'KOA TOOLS INDIA LTD.' },
  { symbol: 'ELNET.BO', name: 'ELNET TECHNOLOGIES LTD.-$' },
  { symbol: 'ARTKPOW.BO', name: 'ARTECH POWER PRODUCTS LTD.' },
  { symbol: 'ACCEL.BO', name: 'ACCEL TRANSMATIC LTD.' },
  { symbol: 'RICOHQ.BO', name: 'RICOH INDIA LTD.' },
  { symbol: 'DHINDIA.BO', name: 'D&H India Ltd-$' },
  { symbol: 'LLOYDELENG.BO', name: 'LLOYD ELECTRIC & ENGINEERING LTD.' },
  { symbol: 'RAJGLOWIR.BO', name: 'RAJRATAN GLOBAL WIRE LTD.' },
  { symbol: 'SURANAT&AMP;P.BO', name: 'Surana Telecom And Power Limited' },
  { symbol: 'ALFATRAN.BO', name: 'ALFA TRANSFORMERS LTD.' },
  { symbol: 'STARLITE.BO', name: 'STARLITE COMPONENTS LTD.' },
  { symbol: 'ADVNCMIC.BO', name: 'ADVANCED MICRONIC DEVICES LTD.-$' },
  { symbol: 'NHCFOODS.BO', name: 'NHC FOODS LTD.' },
  { symbol: 'GRCABLE.BO', name: 'GR CABLES LTD.' },
  { symbol: 'INDLMETER.BO', name: 'IMP POWERS LTD.' },
  { symbol: 'DMCEDU.BO', name: 'DMC Education Ltd' },
  { symbol: 'KEERTHI.BO', name: 'KEERTHI INDUSTRIES LTD.' },
  { symbol: 'GSCLCEMENT.BO', name: 'GUJARAT SIDHEE CEMENT LTD.' },
  { symbol: 'SURAJ.BO', name: 'SURAJ PRODUCTS LTD.' },
  { symbol: 'PRSNTIN.BO', name: 'PRASHANT INDIA LTD.' },
  { symbol: 'SHAHFOOD.BO', name: 'SHAH FOODS LTD.' },
  { symbol: 'ZKHANDEN.BO', name: 'KHANDELWAL EXTRACTION LTD.' },
  { symbol: 'TASTYBIT.BO', name: 'TASTY BITE EATABLES LTD.' },
  { symbol: 'RITESHIN.BO', name: 'RITESH INTERNATIONAL LTD.' },
  { symbol: 'VADILENT.BO', name: 'VADILAL ENTERPRISES LTD.' },
  { symbol: 'ASHAI.BO', name: 'ASHIANA AGRO INDUSTRIES LTD.' },
  { symbol: 'NVCMIND.BO', name: 'NAVCOM INDUSTRIES LTD.' },
  { symbol: 'WILLIMFI.BO', name: 'WILLIAMSON FINANCIAL SERVICES LTD.' },
  { symbol: 'AJANTSOY.BO', name: 'AJANTA SOYA LTD.' },
  { symbol: 'RICHIRICH.BO', name: 'Richirich Inventures Limited' },
  { symbol: 'SIEL.BO', name: 'Superior Industrial Enterprises Limited' },
  { symbol: 'SPTRSHI.BO', name: 'SAPTARISHI AGRO INDUSTRIES LTD.' },
  { symbol: 'SRDAPRT.BO', name: 'SARDA PROTEINS LTD.' },
  { symbol: 'JVLAGRO.BO', name: 'JVL AGRO INDUSTRIES LTD.' },
  { symbol: 'PRIMAGR.BO', name: 'PRIMA AGRO LTD.' },
  { symbol: 'UNOINDL.BO', name: 'UNNO INDUSTRIES LTD.' },
  { symbol: 'MADHURIND.BO', name: 'MADHUR INDUSTRIES LTD.' },
  { symbol: 'AGRODUTCH.BO', name: 'AGRO DUTCH INDUSTRIES LTD.' },
  { symbol: 'TARAI.BO', name: 'TARAI FOODS LTD.' },
  { symbol: 'MODAIRY.BO', name: 'MODERN DAIRIES LTD.' },
  { symbol: 'BAMBINO.BO', name: 'BAMBINO AGRO INDUSTRIES LTD.' },
  { symbol: 'PRIMIND.BO', name: 'PRIME INDUSTRIES LTD.' },
  { symbol: 'RJNIEXT.BO', name: 'RAJANI EXTRACTIONS LTD.' },
  { symbol: 'VIKASWSP.BO', name: 'VIKAS WSP LTD.' },
  { symbol: 'AASHEE.BO', name: 'AASHEE INFOTECH LTD.' },
  { symbol: 'MURLIIND.BO', name: 'MURLI INDUSTRIES LTD.-$' },
  { symbol: 'VSFPROJ.BO', name: 'VSF PROJECTS LTD.' },
  { symbol: 'POONADAL.BO', name: 'POONA DAL & OIL INDUSTRIES LTD.' },
  { symbol: 'TRANSFD.BO', name: 'TRANSGLOBE FOODS LTD.' },
  { symbol: 'VIMALOIL.BO', name: 'VIMAL OIL & FOODS LTD.-$' },
  { symbol: 'KMGMILK.BO', name: 'KMG MILK FOOD LTD.' },
  { symbol: 'KSE.BO', name: 'KSE LTD.-$' },
  { symbol: 'PIONAGR.BO', name: 'PIONEER AGRO EXTRACTS LTD.' },
  { symbol: 'NARBADA.BO', name: 'NARBADA GEMS AND JEWELLERY LTD.' },
  { symbol: 'VIRATCRA.BO', name: 'VIRAT CRANE INDUSTRIES LTD.' },
  { symbol: 'CHORDIA.BO', name: 'CHORDIA FOOD PRODUCTS LTD.' },
  { symbol: 'OMEAG.BO', name: 'OMEGA AG-SEEDS (PUNJAB) LTD.' },
  { symbol: 'TAIIND.BO', name: 'TAI INDUSTRIES LTD.-$' },
  { symbol: 'KOHINOORT.BO', name: 'Kohinoor Techno Engineers Limited' },
  { symbol: 'OCEAGRO.BO', name: 'OCEAN AGRO (INDIA) LTD.' },
  { symbol: 'BKV.BO', name: 'BKV INDUSTRIES LTD.' },
  { symbol: 'ASIANTNE.BO', name: 'ASIAN TEA & EXPORTS LTD.-$' },
  { symbol: 'NEHAINT.BO', name: 'NEHA INTERNATIONAL LTD.' },
  { symbol: 'SIMRAN.BO', name: 'SIMRAN FARMS LTD.' },
  { symbol: 'LAKSHMIO.BO', name: 'LAKSHMI OVERSEAS INDUSTRIES LTD.' },
  { symbol: 'SMILAX.BO', name: 'Smilax Industries Limited' },
  { symbol: 'DFM.BO', name: 'DFM FOODS LTD.' },
  { symbol: 'SURFI.BO', name: 'SURYO FOODS & INDUSTRIES LTD.' },
  { symbol: 'INTEGFD.BO', name: 'INTEGRATED PROTEINS LTD.' },
  { symbol: 'MAHAANF.BO', name: 'MAHAAN FOODS LTD.' },
  { symbol: 'SUNCLAYLTD.BO', name: 'SUNDARAM-CLAYTON LTD.' },
  { symbol: 'SONASTEER.BO', name: 'SONA KOYO STEERING SYSTEMS LTD.' },
  { symbol: 'SAMKRG.BO', name: 'SAMKRG PISTONS & RINGS LTD.-$' },
  { symbol: 'AMTEKAUTO.BO', name: 'AMTEK AUTO LTD.-$' },
  { symbol: 'ECSTSTL.BO', name: 'EAST COAST STEEL LTD.' },
  { symbol: 'SICAL.BO', name: 'SICAL LOGISTICS LTD.' },
  { symbol: 'ARCEEIN.BO', name: 'ARCEE INDUSTRIES LTD.' },
  { symbol: 'ABCINDQ.BO', name: 'ABC INDIA LTD.-$' },
  { symbol: 'BALTE.BO', name: 'BALURGHAT TECHNOLOGIES LTD.' },
  { symbol: 'COARO.BO', name: 'COASTAL ROADWAYS LTD.' },
  { symbol: 'JAGSONAI.BO', name: 'JAGSON AIRLINES LTD.' },
  { symbol: 'SIBARAUT.BO', name: 'SIBAR AUTO PARTS LTD.' },
  { symbol: 'SHREYAS.BO', name: 'SHREYAS SHIPPING & LOGISTICS LTD.' },
  { symbol: 'ABG.BO', name: 'ABG INFRALOGISTICS LTD.' },
  { symbol: 'SHVSUIT.BO', name: 'SHIVA SUITINGS LTD.' },
  { symbol: 'INDOVATION.BO', name: 'INDOVATION TECHNOLOGIES LTD.' },
  { symbol: 'SURYAJYOTI.BO', name: 'SURYAJYOTI SPINNING MILLS LTD.-$' },
  { symbol: 'NAKODA.BO', name: 'Nakoda Limited-$' },
  { symbol: 'SOURCEIND.BO', name: 'SOURCE INDUSTRIES (INDIA) LTD.' },
  { symbol: 'TAMJAIM.BO', name: 'TAMILNADU JAIBHARAT MILLS LTD.' },
  { symbol: 'ADVLIFE.BO', name: 'ADVANCE LIFESTYLES LTD.' },
  { symbol: 'KAKTEX.BO', name: 'KAKATIYA TEXTILES LTD.' },
  { symbol: 'OCTAVE.BO', name: 'Perfect-Octave Media Projects Ltd' },
  { symbol: 'HISARSP.BO', name: 'HISAR SPINNING MILLS LTD.' },
  { symbol: 'ALOKTEXT.BO', name: 'ALOK INDUSTRIES LTD.' },
  { symbol: 'ASIL.BO', name: 'AMIT SPINNING INDUSTRIES LTD.' },
  { symbol: 'PASARI.BO', name: 'PASARI SPINNING MILLS LTD.' },
  { symbol: 'SPENTEX.BO', name: 'SPENTEX INDUSTRIES LTD.' },
  { symbol: 'AMARJOTHI.BO', name: 'AMARJOTHI SPINNING MILLS LTD.' },
  { symbol: 'OLYMPTX.BO', name: 'OLYMPIA INDUSTRIES LTD.' },
  { symbol: 'SUDTIND-B.BO', name: 'SUDITI INDUSTRIES LTD.' },
  { symbol: 'KHTRFIB.BO', name: 'KHATOR FIBRE & FABRICS LTD.' },
  { symbol: 'SBFL.BO', name: 'Shree Bhavya Fabrics Ltd' },
  { symbol: 'GEMSPIN.BO', name: 'GEM SPINNERS INDIA LTD.' },
  { symbol: 'EUREKAI.BO', name: 'EUREKA INDUSTRIES LTD.' },
  { symbol: 'ADITYASP.BO', name: 'ADITYA SPINNERS LTD.' },
  { symbol: 'PRIMEURB.BO', name: 'PRIME URBAN DEVELOPMENT INDIA LTD.' },
  { symbol: 'DHANFAB.BO', name: 'DHANLAXMI FABRICS LTD.' },
  { symbol: 'SLSTLQ.BO', name: 'SRI LAKSHMI SARASWATHI TEXTILES (ARNI) LTD.-$' },
  { symbol: 'FRONTBUSS.BO', name: 'FRONTLINE BUSINESS SOLUTIONS LTD.' },
  { symbol: 'ARORAFIB.BO', name: 'ARORA FIBRES LTD.' },
  { symbol: 'GANGOTRI.BO', name: 'GANGOTRI TEXTILES LTD.' },
  { symbol: 'SRMCL.BO', name: 'SRI RAMAKRISHNA MILLS (COIMBATORE) LTD.-$' },
  { symbol: 'SEASONF.BO', name: 'SEASONS FURNISHINGS LTD.' },
  { symbol: 'UNITEDTE.BO', name: 'UNITED TEXTILES LTD.' },
  { symbol: 'SAMTEX.BO', name: 'SAMTEX FASHIONS LTD.-$' },
  { symbol: 'CITYMAN.BO', name: 'CITYMAN LTD.' },
  { symbol: 'DHANROTO.BO', name: 'DHANALAXMI ROTO SPINNERS LTD.' },
  { symbol: 'DAMOINDUS.BO', name: 'DAMODAR INDUSTRIES LTD.-$' },
  { symbol: 'SANBLUE.BO', name: 'SANBLUE CORPORATION LTD.' },
  { symbol: 'UNIROYAL.BO', name: 'UNIROYAL INDUSTRIES LTD.' },
  { symbol: 'TATIAGLOB.BO', name: 'TATIA GLOBAL VENNTURE LTD.' },
  { symbol: 'SUNILTX.BO', name: 'SUNIL INDUSTRIES LTD.' },
  { symbol: 'SRINACHA.BO', name: 'SRI NACHAMMAI COTTON MILLS LTD.-$' },
  { symbol: 'SEQUELE.BO', name: 'SEQUEL E-ROUTERS LTD.' },
  { symbol: 'SAMBANDAM.BO', name: 'SAMBANDAM SPINNING MILLS LTD.-$' },
  { symbol: 'KANDAGIRI.BO', name: 'KANDAGIRI SPINNING MILLS LTD.-$' },
  { symbol: 'CRANEX.BO', name: 'CRANEX LTD.' },
  { symbol: 'AUSTENG.BO', name: 'AUSTIN ENGINEERING CO.LTD.' },
  { symbol: 'VCCLLTD.BO', name: 'VCCL LTD.' },
  { symbol: 'FLUIDOM.BO', name: 'FLUIDOMAT LTD.' },
  { symbol: 'EMAINDIA.BO', name: 'EMA INDIA LTD.' },
  { symbol: 'MIVENMACH.BO', name: 'MIVEN MACHINE TOOLS LTD.' },
  { symbol: 'STONEIN.BO', name: 'STONE INDIA LTD.-$' },
  { symbol: 'VOITHPAPR.BO', name: 'VOITH PAPER FABRICS INDIA LTD.-$' },
  { symbol: 'ARTSONEN.BO', name: 'ARTSON ENGINEERING LTD.' },
  { symbol: 'SOLIMAC.BO', name: 'SOLITAIRE MACHINE TOOLS LTD.' },
  { symbol: 'DIAPOWER.BO', name: 'Diamond Power Infrastructure Limited-$' },
  { symbol: 'INDSILHYD.BO', name: 'INDSIL HYDRO POWER AND MANGANESE LTD.-$' },
  { symbol: 'SVOGL.BO', name: 'SVOGL Oil Gas And Energy Ltd-$' },
  { symbol: 'ITL.BO', name: 'ITL INDUSTRIES LTD.-$' },
  { symbol: 'RASANDIK.BO', name: 'RASANDIK ENGINEERING INDUSTRIES INDIA LTD.-$' },
  { symbol: 'YOGISUNG.BO', name: 'YOGI SUNG-WON (INDIA) LTD.' },
  { symbol: 'SWISSGLA.BO', name: 'SWISS GLASCOAT EQUIPMENTS LTD.' },
  { symbol: 'GUJAPOLLO*.BO', name: 'GUJARAT APOLLO INDUSTRIES LTD.' },
  { symbol: 'TANAA.BO', name: 'TANEJA AEROSPACE & AVIATION LTD.-$' },
  { symbol: 'CONART.BO', name: 'CONART ENGINEERS LTD.-$' },
  { symbol: 'VHCLINDUS.BO', name: 'VHCL INDUSTRIES LTD.' },
  { symbol: 'SHIVAGR.BO', name: 'SHIVAGRICO IMPLEMENTS LTD.' },
  { symbol: 'IYKOTHITE.BO', name: 'IYKOT HITECH TOOLROOM LTD.' },
  { symbol: 'CENLUB.BO', name: 'CENLUB INDUSTRIES LTD.' },
  { symbol: 'KALINDEE.BO', name: 'Kalindee Rail Nirman (Engineers) Ltd-$' },
  { symbol: 'DOLPHINOFF.BO', name: 'DOLPHIN OFFSHORE ENTERPRISES (INDIA) LTD.-$' },
  { symbol: 'JMCPROJECT.BO', name: 'JMC PROJECTS (INDIA) LTD.-$' },
  { symbol: 'VJLAXMIE.BO', name: 'VEEJAY LAKSHMI ENGINEERING WORKS LTD.-$' },
  { symbol: 'AHMDSTE.BO', name: 'AHMEDABAD STEELCRAFT LTD.' },
  { symbol: 'ALSTOMT&AMP;D.BO', name: 'ALSTOM T&D INDIA LTD.' },
  { symbol: 'KALPATPOWR.BO', name: 'KALPATARU POWER TRANSMISSION LTD.' },
  { symbol: 'IFMIMPX.BO', name: 'IFM IMPEX GLOBAL LTD.' },
  { symbol: 'CHANDNI.BO', name: 'Chandni Textiles Engineering Ind. Ltd' },
  { symbol: 'TIGLOB.BO', name: 'T & I GLOBAL LTD.' },
  { symbol: 'BEMHY.BO', name: 'BEMCO HYDRAULICS LTD.' },
  { symbol: 'PROFDIA.BO', name: 'PROFESSIONAL DIAMONDS LTD.' },
  { symbol: 'ANSALBU.BO', name: 'ANSAL BUILDWELL LTD.-$' },
  { symbol: 'BNRSEC.BO', name: 'B.N.RATHI SECURITIES LTD.' },
  { symbol: 'RISHITECH.BO', name: 'RISHI TECHTEX LTD.' },
  { symbol: 'SAFARIND.BO', name: 'SAFARI INDUSTRIES (INDIA) LTD.' },
  { symbol: 'RAJDHNIL.BO', name: 'RAJDHANI LEASING & INDUSTRIES LTD.' },
  { symbol: 'ADSDIAG.BO', name: 'ADS DIAGNOSTIC LTD.' },
  { symbol: 'ZBINTXPP.BO', name: 'BINAYAK TEX PROCESSORS LTD.' },
  { symbol: 'JJFINCOR.BO', name: 'J.J.FINANCE CORPORATION LTD.' },
  { symbol: 'COSMOFE.BO', name: 'COSMO FERRITES LTD.-$' },
  { symbol: 'ZPPOLYSA.BO', name: 'PLANTER POLYSACKS LTD.' },
  { symbol: 'FUTURSEC.BO', name: 'FUTURISTIC SECURITIES LTD.' },
  { symbol: 'SANCTRN.BO', name: 'SANCO TRANS LTD.' },
  { symbol: 'ADORMUL.BO', name: 'ADOR MULTIPRODUCTS LTD.' },
  { symbol: 'BONINDL.BO', name: 'BONANZA INDUSTRIES LTD.' },
  { symbol: 'MEDICAPQ.BO', name: 'MEDI-CAPS LTD.-$' },
  { symbol: 'OTCO.BO', name: 'OTCO INTERNATIONAL LTD.' },
  { symbol: 'MORGANITE.BO', name: 'MORGANITE CRUCIBLE (INDIA) LTD.' },
  { symbol: 'SIPIND.BO', name: 'SIP INDUSTRIES LTD.' },
  { symbol: 'BAPACK.BO', name: 'B&A Packaging India Limited' },
  { symbol: 'ABAN.BO', name: 'ABAN OFFSHORE LTD.' },
  { symbol: 'KILBURN.BO', name: 'KILBURN OFFICE AUTOMATION LTD.' },
  { symbol: 'MCSLTD.BO', name: 'MCS LTD.' },
  { symbol: 'SRMENERGY.BO', name: 'SRM ENERGY LTD.' },
  { symbol: 'CONTPTR.BO', name: 'CONTINENTAL PETROLEUMS LTD.' },
  { symbol: 'SHRENUJ.BO', name: 'SHRENUJ & CO.LTD.' },
  { symbol: 'NBFOOT.BO', name: 'NB FOOTWEAR LTD.' },
  { symbol: 'MACPLASQ.BO', name: 'MACHINO PLASTICS LTD.-$' },
  { symbol: 'VENKYS.BO', name: 'VENKY\'S (INDIA) LTD.' },
  { symbol: 'GVFILM.BO', name: 'GV FILMS LTD.' },
  { symbol: 'RAMAVISION.BO', name: 'RAMA VISION LTD.' },
  { symbol: 'PURITY.BO', name: 'PURITY FLEX PACK LTD.' },
  { symbol: 'MICROSE.BO', name: 'MICROSE INDIA LTD.' },
  { symbol: 'STDSHOE.BO', name: 'STANDARD SHOE SOLE AND MOULD (INDIA) LTD.' },
  { symbol: 'DCMSRMIND.BO', name: 'DCM SHRIRAM INDUSTRIES LTD.-$' },
  { symbol: 'TRITON.BO', name: 'TRITON CORP.LTD.' },
  { symbol: 'HITACHIHOM.BO', name: 'HITACHI HOME AND LIFE SOLUTIONS (INDIA) LTD.' },
  { symbol: 'ADCINDIA.BO', name: 'ADC India Communications Limited-$' },
  { symbol: 'COVENTRY.BO', name: 'COVENTRY COIL-O-MATIC (HARYANA) LTD.' },
  { symbol: 'SUNRAJDI.BO', name: 'SUNRAJ DIAMOND EXPORTS LTD.' },
  { symbol: 'SHARP.BO', name: 'SHARP INDIA LTD.' },
  { symbol: 'TECPO.BO', name: 'TECHTRAN POLYLENSES LTD.' },
  { symbol: 'INDBNK.BO', name: 'IND BANK HOUSING LTD.' },
  { symbol: 'JAIMATAG.BO', name: 'JAI MATA GLASS LTD.' },
  { symbol: 'LOTUSCHO.BO', name: 'LOTUS CHOCOLATE CO.LTD.' },
  { symbol: 'PACIFICI.BO', name: 'PACIFIC INDUSTRIES LTD.' },
  { symbol: 'CMMHOSP.BO', name: 'CHENNAI MEENAKSHI MULTISPECIALITY HOSPITAL LTD.-$' },
  { symbol: 'UNIOFFICE.BO', name: 'UNIVERSAL OFFICE AUTOMATION LTD.' },
  { symbol: 'RAINBOWPAP.BO', name: 'RAINBOW PAPERS LTD.-$' },
  { symbol: 'APMIN.BO', name: 'APM INDUSTRIES LTD.-$' },
  { symbol: 'KRYPTONQ.BO', name: 'KRYPTON INDUSTRIES LTD.' },
  { symbol: 'NETWORK.BO', name: 'NETWORK LTD.' },
  { symbol: 'MARBU.BO', name: 'MARTIN BURN LTD.' },
  { symbol: 'FRL.BO', name: 'FUTURE RETAIL LTD.' },
  { symbol: 'INDTONER.BO', name: 'INDIAN TONERS & DEVELOPERS LTD.-$' },
  { symbol: 'JENSONICOL.BO', name: 'JENSON & NICHOLSON (INDIA) LTD.' },
  { symbol: 'KUNSTOFF.BO', name: 'KUNSTSTOFFE INDUSTRIES LTD.' },
  { symbol: 'PHRMASI.BO', name: 'PHAARMASIA LTD.' },
  { symbol: 'PODDARDEV.BO', name: 'Poddar Developers Ltd' },
  { symbol: 'IPRINGLTD.BO', name: 'IP RINGS LTD.-$' },
  { symbol: 'REDEXPR.BO', name: 'REDEX PROTECH LTD.' },
  { symbol: 'WATERBASE.BO', name: 'WATERBASE LTD.' },
  { symbol: 'FLEXFO.BO', name: 'FLEX FOODS LTD.-$' },
  { symbol: 'GOLKUNDIA.BO', name: 'GOLKUNDA DIAMONDS & JEWELLERY LTD.' },
  { symbol: 'FORTISMLR.BO', name: 'Fortis Malar Hospitals Limited' },
  { symbol: 'JMGCORP.BO', name: 'JMG CORPORATION LTD.' },
  { symbol: 'SVAMSOF.BO', name: 'SVAM SOFTWARE LTD.' },
  { symbol: 'VIJSHAN.BO', name: 'VIJAY SHANTHI BUILDERS LTD.' },
  { symbol: 'SIDDHATUBE.BO', name: 'SIDDHARTHA TUBES LTD.' },
  { symbol: 'ECOBOAR.BO', name: 'ECOBOARD INDUSTRIES LTD.' },
  { symbol: 'DPL.BO', name: 'Dhunseri Petrochem Limited-$' },
  { symbol: 'IOSYSTEM.BO', name: 'IO SYSTEM LTD.' },
  { symbol: 'EPCIN.BO', name: 'EPC INDUSTRIE LTD.' },
  { symbol: 'SREINFRA.BO', name: 'SREI INFRASTRUCTURE FINANCE LTD.' },
  { symbol: 'GUJBOROS.BO', name: 'GUJARAT BOROSIL LTD.' },
  { symbol: 'DERPC.BO', name: 'DERA PAINTS & CHEMICALS LTD.' },
  { symbol: 'SHUKJEW.BO', name: 'SHUKRA JEWELLERS LTD.' },
  { symbol: 'MAZDALTD.BO', name: 'MAZDA LTD.-$' },
  { symbol: 'VICEROY.BO', name: 'VICEROY HOTELS LTD.' },
  { symbol: 'DIVINE.BO', name: 'DIVINE MULTIMEDIA (INDIA) LTD.' },
  { symbol: 'NEOCORP.BO', name: 'NEO CORP INTERNATIONAL LTD.' },
  { symbol: 'SOVERDIA.BO', name: 'SOVEREIGN DIAMONDS LTD.' },
  { symbol: 'GUJRAFIA.BO', name: 'GUJARAT RAFFIA INDUSTRIES LTD.' },
  { symbol: 'INNOVTEC.BO', name: 'INNOVATIVE TECH PACK LTD.' },
  { symbol: 'SUPTANERY.BO', name: 'Super Tannery Limited' },
  { symbol: 'INVICTA.BO', name: 'INVICTA MEDITEK LTD.' },
  { symbol: 'SKYSS.BO', name: 'SKYPAK SERVICE SPECIALIST LTD.' },
  { symbol: 'AXTEL.BO', name: 'AXTEL INDUSTRIES LTD.' },
  { symbol: 'DHOOTIND.BO', name: 'DHOOT INDUSTRIES LTD.' },
  { symbol: 'MAGNA.BO', name: 'MAGNA INDUSTRIES & EXPORTS LTD.' },
  { symbol: 'PRECISION.BO', name: 'PRECISION CONTAINEURS LTD.' },
  { symbol: 'JYOTIOVR.BO', name: 'JYOTI OVERSEAS LTD.' },
  { symbol: 'TOTEX.BO', name: 'TOTAL EXPORTS LTD.' },
  { symbol: 'VRWODAR.BO', name: 'V R WOODART LTD.' },
  { symbol: 'DSKULKARNI.BO', name: 'D.S.KULKARNI DEVELOPERS LTD.' },
  { symbol: 'AVIPHOT.BO', name: 'AVI PHOTOCHEM LTD.' },
  { symbol: 'MAGMA.BO', name: 'MAGMA FINCORP LTD.' },
  { symbol: 'HINFLUR.BO', name: 'HINDUSTAN FLUOROCARBONS LTD.' },
  { symbol: 'HYDROS&AMP;S.BO', name: 'HYDRO S & S INDUSTRIES LTD.-$' },
  { symbol: 'PATIDAR.BO', name: 'Patidar Buildcon Limited' },
  { symbol: 'VENLONENT.BO', name: 'VENLON ENTERPRISES LTD.' },
  { symbol: 'PETPLST.BO', name: 'PET PLASTICS LTD.' },
  { symbol: 'PANIDPR.BO', name: 'PANTHER INDUSTRIAL PRODUCTS LTD.' },
  { symbol: 'ALBERTDA.BO', name: 'ALBERT DAVID LTD.-$' },
  { symbol: 'HARLETH.BO', name: 'HARYANA LEATHER CHEMICALS LTD.' },
  { symbol: 'MONSANTO.BO', name: 'MONSANTO INDIA LTD.' },
  { symbol: 'ACRYSIL.BO', name: 'ACRYSIL LTD.' },
  { symbol: 'LINEARPO.BO', name: 'LINEAR POLYMERS LTD.' },
  { symbol: 'PCCOSMA.BO', name: 'PEE CEE COSMA SOPE LTD.' },
  { symbol: 'TCMLMTD.BO', name: 'TCM LTD.' },
  { symbol: 'KESARPE.BO', name: 'KESAR PETROPRODUCTS LTD.' },
  { symbol: 'LACTOSE.BO', name: 'LACTOSE (INDIA) LTD.' },
  { symbol: 'TEEAI.BO', name: 'TEESTA AGRO INDUSTRIES LTD.' },
  { symbol: 'PNTKYOR.BO', name: 'PENTOKEY ORGANY (INDIA) LTD.' },
  { symbol: 'RESONANCE.BO', name: 'RESONANCE SPECIALTIES LTD.-$' },
  { symbol: 'MAHAPOL.BO', name: 'MAHARASHTRA POLYBUTENES LTD.' },
  { symbol: 'UNIMERQ.BO', name: 'UNIMERS INDIA LTD.' },
  { symbol: 'AIMCOPEST.BO', name: 'AIMCO PESTICIDES LTD.' },
  { symbol: 'GUJTERC.BO', name: 'GUJARAT TERCE LABORATORIES LTD.' },
  { symbol: 'KABRADG.BO', name: 'KABRA DRUGS LTD.' },
  { symbol: 'BCL.BO', name: 'BCL Industries and Infrastructures Ltd-$' },
  { symbol: 'SHHARICH.BO', name: 'SHREE HARI CHEMICALS EXPORT LTD.' },
  { symbol: 'ORCHIDPHAR.BO', name: 'Orchid Pharma Ltd' },
  { symbol: 'CRAZYINF.BO', name: 'CRAZY INFOTECH LTD.' },
  { symbol: 'ISHITADR.BO', name: 'ISHITA DRUGS & INDUSTRIES LTD.' },
  { symbol: 'UNIVSTAR.BO', name: 'UNIVERSAL STARCH-CHEM ALLIED LTD.' },
  { symbol: 'AREYDRG.BO', name: 'AAREY DRUGS & PHARMACEUTICALS LTD.' },
  { symbol: 'ASINPET.BO', name: 'ASIAN PETROPRODUCTS & EXPORTS LTD.' },
  { symbol: 'CAMEXLTD.BO', name: 'CAMEX LTD.' },
  { symbol: 'KAVITIND.BO', name: 'Kavit Industries Limited' },
  { symbol: 'INDOEURO.BO', name: 'INDO EURO INDCHEM LTD.' },
  { symbol: 'RIDDHI.BO', name: 'RIDDHI SIDDHI GLUCO BIOLS LTD.-$' },
  { symbol: 'TRAMEDI.BO', name: 'TRANS MEDICARE LTD.' },
  { symbol: 'SVCSUPE.BO', name: 'SVC SUPERCHEM LTD.' },
  { symbol: 'RAAJMEDI.BO', name: 'RAAJ MEDISAFE INDIA LTD.' },
  { symbol: 'CORALAB.BO', name: 'CORAL LABORATORIES LTD.' },
  { symbol: 'TULASEEBIOE.BO', name: 'TULASEE BIO-ETHANOL LTD.' },
  { symbol: 'BACPHAR.BO', name: 'BACIL PHARMA LTD.' },
  { symbol: 'KMCSHIL.BO', name: 'KMC SPECIALITY HOSPITALS (INDIA) LTD.' },
  { symbol: 'LAFFANSQ.BO', name: 'LAFFANS PETROCHEMICALS LTD.-$' },
  { symbol: 'SECHE.BO', name: 'SECUNDERABAD HEALTHCARE LTD.' },
  { symbol: 'SHABCHM.BO', name: 'SHABA CHEMICALS LTD.' },
  { symbol: 'GAYATRIBI.BO', name: 'GAYATRI BIOORGANICS LTD.' },
  { symbol: 'PODARPIGQ.BO', name: 'PODDAR PIGMENTS LTD.-$' },
  { symbol: 'PHARMAID.BO', name: 'PHARMAIDS PHARMACEUTICALS LTD.' },
  { symbol: 'VIVIDIND.BO', name: 'VIVID GLOBAL INDUSTRIES LTD.' },
  { symbol: 'PRIYALT.BO', name: 'PRIYA LTD.-$' },
  { symbol: 'TIRUSTA.BO', name: 'TIRUPATI STARCH & CHEMICALS LTD.' },
  { symbol: 'EMEDTECH.BO', name: 'Emed.com Technologies Ltd' },
  { symbol: 'HEMORGANIC.BO', name: 'Hemo Organic Limited' },
  { symbol: 'JDORGOCHEM.BO', name: 'JD ORGOCHEM LTD.' },
  { symbol: 'ASHOKALC.BO', name: 'ASHOK ALCO-CHEM LTD.' },
  { symbol: 'AKSCHEM.BO', name: 'AKSHARCHEM (INDIA) LTD.-$' },
  { symbol: 'BERLDRG.BO', name: 'BERYL DRUGS LTD.' },
  { symbol: 'RATHIGRA.BO', name: 'RATHI GRAPHIC TECHNOLOGIES LTD.' },
  { symbol: 'INDXTRA.BO', name: 'INDIAN EXTRACTIONS LTD.' },
  { symbol: 'ISTRNETWK.BO', name: 'iStreet Network Limited' },
  { symbol: 'GAGAN.BO', name: 'GAGAN GASES LTD.' },
  { symbol: 'PARKERAC.BO', name: 'PARKER AGROCHEM EXPORTS LTD.-$' },
  { symbol: 'RELISH.BO', name: 'RELISH PHARMACEUTICALS LTD.' },
  { symbol: 'ALUFLUOR.BO', name: 'ALUFLUORIDE LTD.' },
  { symbol: 'SSORGS.BO', name: 'SS ORGANICS LTD.' },
  { symbol: 'ARCHITORG.BO', name: 'ARCHIT ORGANOSYS LTD.' },
  { symbol: 'SIKOZY.BO', name: 'Sikozy Realtors Limited' },
  { symbol: 'INDSWFTLTD.BO', name: 'IND-SWIFT LTD.' },
  { symbol: 'WELCURE.BO', name: 'WELCURE DRUGS & PHARMACEUTICALS LTD.' },
  { symbol: 'BIBCL.BO', name: 'BHARAT IMMUNOLOGICALS & BIOLOGICALS CORPORATION LTD.-$' },
  { symbol: 'BASANTGL.BO', name: 'BASANT AGRO TECH (INDIA) LTD.-$' },
  { symbol: 'PARENTLD.BO', name: 'PARENTERAL DRUGS (INDIA) LTD.-$' },
  { symbol: 'KILBURNC.BO', name: 'KILBURN CHEMICALS LTD.-$' },
  { symbol: 'SANDUPHQ.BO', name: 'SANDU PHARMACEUTICALS LTD.' },
  { symbol: 'NAGAAGRI.BO', name: 'NAGARJUNA AGRICHEM LTD.' },
  { symbol: 'TITANBIO.BO', name: 'TITAN BIOTECH LTD.' },
  { symbol: 'BIJHANS.BO', name: 'BIJOY HANS LTD.' },
  { symbol: 'SPANDIAQ.BO', name: 'SPAN DIAGNOSTICS LTD.-$' },
  { symbol: 'JENBURPH.BO', name: 'JENBURKT PHARMACEUTICALS LTD.' },
  { symbol: 'CAPPL.BO', name: 'CAPLIN POINT LABORATORIES LTD.' },
  { symbol: 'LINKPH.BO', name: 'LINK PHARMA CHEM LTD.' },
  { symbol: 'GUJMEDI.BO', name: 'GUJARAT MEDITECH LTD.' },
  { symbol: 'WINTAC.BO', name: 'WINTAC LTD.' },
  { symbol: 'NUTRA.BO', name: 'Nutraplus India Limited' },
  { symbol: 'EMMESSA.BO', name: 'EMMESSAR BIOTECH & NUTRITION LTD.' },
  { symbol: 'ELDERPG.BO', name: 'ELDER PROJECTS LTD.' },
  { symbol: 'VARDHCH.BO', name: 'VARDHAMAN LABORATORIES LTD.' },
  { symbol: 'PHYTO.BO', name: 'PHYTO CHEM (INDIA) LTD.' },
  { symbol: 'DYNAMIND.BO', name: 'DYNAMIC INDUSTRIES LTD.-$' },
  { symbol: 'BDH.BO', name: 'BDH INDUSTRIES LTD.' },
  { symbol: 'ELDERHCL.BO', name: 'ELDER HEALTH CARE LTD.' },
  { symbol: 'JAUSPOL.BO', name: 'JAUSS POLYMERS LTD.' },
  { symbol: 'PROCAL.BO', name: 'PROCAL ELECTRONICS INDIA LTD.' },
  { symbol: 'KEMROCK.BO', name: 'KEMROCK INDUSTRIES & EXPORTS LTD.' },
  { symbol: 'GLOBUSCON.BO', name: 'Globus Constructors & Developers Limited' },
  { symbol: 'POLYCHMP.BO', name: 'POLYMECHPLAST MACHINES LTD.' },
  { symbol: 'LUMITECH.BO', name: 'LUMINAIRE TECHNOLOGIES LTD.' },
  { symbol: 'KCCLPLASTC.BO', name: 'KCCL PLASTIC LTD.' },
  { symbol: 'STELLANT.BO', name: 'STELLANT SECURITIES (INDIA) LTD.' },
  { symbol: 'GALXBRG.BO', name: 'GALAXY BEARINGS LTD.' },
  { symbol: 'VINRKLB.BO', name: 'REKVINA LABORATORIES LTD.' },
  { symbol: 'SCAGRO.BO', name: 'SC Agrotech Ltd' },
  { symbol: 'SGARRES.BO', name: 'SAGAR TOURIST RESORTS LTD.' },
  { symbol: 'SATHAISPAT.BO', name: 'SATHAVAHANA ISPAT LTD.' },
  { symbol: 'PRICOL.BO', name: 'PRICOL LTD.' },
  { symbol: 'UNRYLMA.BO', name: 'UNIROYAL MARINE EXPORTS LTD.' },
  { symbol: 'KARANWO.BO', name: 'KARAN WOO-SIN LTD.' },
  { symbol: 'SHERVANI.BO', name: 'SHERVANI INDUSTRIAL SYNDICATE LTD.' },
  { symbol: 'ARHNTTO.BO', name: 'ARIHANT TOURNESOL LTD.' },
  { symbol: 'SUPERTEX.BO', name: 'SUPERTEX INDUSTRIES LTD.' },
  { symbol: 'SHETR.BO', name: 'SHETRON LTD.' },
  { symbol: 'TRABI.BO', name: 'TRANSGENE BIOTEK LTD.' },
  { symbol: 'MPL.BO', name: 'MPL Plastics Limited' },
  { symbol: 'NIKHILAD.BO', name: 'NIKHIL ADHESIVES LTD.-$' },
  { symbol: 'SPENTA.BO', name: 'SPENTA INTERNATIONAL LTD.' },
  { symbol: 'MULTIBASE.BO', name: 'MULTIBASE INDIA LTD.' },
  { symbol: 'ANDREWYU.BO', name: 'ANDREW YULE & COMPANY LTD.' },
  { symbol: 'LUDLOWJUT.BO', name: 'LUDLOW JUTE & SPECIALITIES LTD.' },
  { symbol: 'ASHRAM.BO', name: 'ASHRAM ONLINE.COM LTD.' },
  { symbol: 'ROYALCU.BO', name: 'ROYAL CUSHION VINYL PRODUCTS LTD.-$' },
  { symbol: 'NOGMIND.BO', name: 'NEOGEM INDIA LTD.' },
  { symbol: 'KSOILS.BO', name: 'K.S.OILS LTD.-$' },
  { symbol: 'HITECHPLAS.BO', name: 'HITECH PLAST LTD.-$' },
  { symbol: 'BLOOM.BO', name: 'BLOOM DEKOR LTD.-$' },
  { symbol: 'MERCATOR.BO', name: 'MERCATOR LTD.-$' },
  { symbol: 'MORGAN.BO', name: 'MORGAN VENTURES LTD.' },
  { symbol: 'AMRAPLIN.BO', name: 'AMRAPALI INDUSTRIES LTD.' },
  { symbol: 'PREMEXPLQ.BO', name: 'PREMIER EXPLOSIVES LTD.' },
  { symbol: 'MIDEASTP.BO', name: 'MID EAST PORTFOLIO MANAGEMENT LTD.' },
  { symbol: 'MOLDTEK.BO', name: 'MOLD-TEK TECHNOLOGIES LTD.' },
  { symbol: 'CRSTCHM.BO', name: 'CRESTCHEM LTD.' },
  { symbol: 'DIVYAJYQ.BO', name: 'DIVYA JYOTI INDUSTRIES LTD.' },
  { symbol: 'MEDINOV.BO', name: 'MEDINOVA DIAGNOSTIC SERVICES LTD.' },
  { symbol: 'HINDIND.BO', name: 'HIND INDUSTRIES LTD.' },
  { symbol: 'DIVSHKT.BO', name: 'DIVYASHAKTI GRANITES LTD.' },
  { symbol: 'SHREYASI.BO', name: 'SHREYAS INTERMEDIATES LTD.' },
  { symbol: 'PARTIND.BO', name: 'Parth Industries Limited' },
  { symbol: 'DUROPACK.BO', name: 'DUROPACK LTD.' },
  { symbol: 'SWARNSAR.BO', name: 'Swarnsarita Gems Ltd' },
  { symbol: 'GANESHHOUC.BO', name: 'GANESH HOUSING CORPORATION LTD.-$' },
  { symbol: 'CINDHO.BO', name: 'CINDRELLA HOTELS LTD.-$' },
  { symbol: 'RPIL.BO', name: 'RITESH PROPERTIES & INDUSTRIES LTD.' },
  { symbol: 'KKALPANAIND.BO', name: 'Kkalpana Industries (India) Ltd' },
  { symbol: 'OKPLA.BO', name: 'OK PLAY INDIA LTD.' },
  { symbol: 'KRITIIND.BO', name: 'KRITI INDUSTRIES (INDIA) LTD.-$' },
  { symbol: 'WELTI.BO', name: 'WELTERMAN INTERNATIONAL LTD.' },
  { symbol: 'ASMTEC.BO', name: 'ASM TECHNOLOGIES LTD.' },
  { symbol: 'PERFEPA.BO', name: 'PERFECTPAC LTD.' },
  { symbol: 'VISIONCINE.BO', name: 'VISION CINEMAS LTD.' },
  { symbol: 'DATASOFT.BO', name: 'DATASOFT APPLICATION SOFTWARE (INDIA) LTD.' },
  { symbol: 'INDRANIB.BO', name: 'INDRAYANI BIOTECH LTD.' },
  { symbol: 'EUROLED.BO', name: 'EURO LEDER FASHION LTD.' },
  { symbol: 'WINSOMBR.BO', name: 'WINSOME BREWERIES LTD.' },
  { symbol: 'SAENTER.BO', name: 'SOUTH ASIAN ENTERPRISES LTD.' },
  { symbol: 'SKYIND.BO', name: 'SKY INDUSTRIES LTD.-$' },
  { symbol: 'PHOENXINTL.BO', name: 'PHOENIX INTERNATIONAL LTD.' },
  { symbol: 'EDUEXEL.BO', name: 'Eduexel Infotainment Limited' },
  { symbol: 'NYLOFIL.BO', name: 'NYLOFIL INDIA LTD.' },
  { symbol: 'PRATIK.BO', name: 'PRATIK PANELS LTD.' },
  { symbol: 'RISHIROOP.BO', name: 'Rishiroop Ltd' },
  { symbol: 'PROMACT.BO', name: 'PROMACT PLASTICS LTD.' },
  { symbol: 'RUBBERPR.BO', name: 'RUBBER PRODUCTS LTD.' },
  { symbol: 'STRGRENWO.BO', name: 'STERLING GREEN WOODS LTD.' },
  { symbol: 'DOLPHMED.BO', name: 'DOLPHIN MEDICAL SERVICES LTD.' },
  { symbol: 'SKSLOGLTD.BO', name: 'SKS LOGISTICS LTD.-$' },
  { symbol: 'INTLNKP.BO', name: 'INTERLINK PETROLEUM LTD.' },
  { symbol: 'ALPINEHOU.BO', name: 'ALPINE HOUSING DEVELOPMENT CORPORATION LTD.' },
  { symbol: 'SANGHIIND.BO', name: 'SANGHI INDUSTRIES LTD.' },
  { symbol: 'JAYENGY.BO', name: 'JAY ENERGY AND S.ENERGIES LTD.' },
  { symbol: 'WWLEATH.BO', name: 'WORLDWIDE LEATHER EXPORTS LTD.' },
  { symbol: 'SFPIL.BO', name: 'Square Four Projects India Limited' },
  { symbol: 'MAXIMAA.BO', name: 'MAXIMAA SYSTEMS LTD.' },
  { symbol: 'SCANPGEOM.BO', name: 'SCANPOINT GEOMATICS LTD.' },
  { symbol: 'CHOKSILA.BO', name: 'CHOKSI LABORATORIES LTD.' },
  { symbol: 'SALGUTI.BO', name: 'SALGUTI INDUSTRIES LTD.' },
  { symbol: 'JAMEHOT.BO', name: 'JAMES HOTELS LTD.' },
  { symbol: 'LONTE.BO', name: 'LONGVIEW TEA COMPANY LTD.' },
  { symbol: 'MIDWEST.BO', name: 'MIDWEST GOLD LTD.' },
  { symbol: 'ENTRINT.BO', name: 'ENTERPRISE INTERNATIONAL LTD.' },
  { symbol: 'TECHIN.BO', name: 'TECHINDIA NIRMAN LIMITED' },
  { symbol: 'TPLPLAST.BO', name: 'TPL PLASTECH LTD.' },
  { symbol: 'PHOTOQUP.BO', name: 'PHOTOQUIP INDIA LTD.' },
  { symbol: 'BNANJEN.BO', name: 'B.NANJI ENTERPRISES LTD.' },
  { symbol: 'LIPPISYS.BO', name: 'LIPPI SYSTEMS LTD.-$' },
  { symbol: 'EXPOGAS.BO', name: 'EXPO GAS CONTAINERS LTD.' },
  { symbol: 'NATPLAS.BO', name: 'NATIONAL PLASTIC INDUSTRIES LTD.' },
  { symbol: 'MFLINDIA.BO', name: 'MFL INDIA LTD.' },
  { symbol: 'AJWAFUN.BO', name: 'AJWA FUN WORLD & RESORT LTD.' },
  { symbol: 'RAYALEMA.BO', name: 'ROYALE MANOR HOTELS & INDUSTRIES LTD.' },
  { symbol: 'CALSREF.BO', name: 'CALS REFINERIES LTD.' },
  { symbol: 'MKEL.BO', name: 'Matra Kaushal Enterprise Limited' },
  { symbol: 'HOTELRUGBY.BO', name: 'HOTEL RUGBY LTD.' },
  { symbol: 'POLOHOT.BO', name: 'POLO HOTELS LTD.' },
  { symbol: 'FENOPLAS.BO', name: 'FENOPLAST LTD.-$' },
  { symbol: 'ECOPLAST.BO', name: 'ECOPLAST LTD.-$' },
  { symbol: 'ELEMARB.BO', name: 'ELEGANT MARBLES & GRANI INDUSTRIES LTD.' },
  { symbol: 'ALCHEM.BO', name: 'ALCHEMIST LTD.' },
  { symbol: 'BITS.BO', name: 'BITS LTD.' },
  { symbol: 'ADARSHPL.BO', name: 'ADARSH PLANT PROTECT LTD.' },
  { symbol: 'GOPALA.BO', name: 'GOPALA POLYPLAST LTD.' },
  { symbol: 'NICCOPAR.BO', name: 'NICCO PARKS & RESORTS LTD.-$' },
  { symbol: 'NTCIND.BO', name: 'NTC INDUSTRIES LTD.' },
  { symbol: 'GARNET.BO', name: 'GARNET CONSTRUCTION LTD.' },
  { symbol: 'BRIGHTBR.BO', name: 'BRIGHT BROTHERS LTD.-$' },
  { symbol: 'EXCAST.BO', name: 'Excel Castronics Limited' },
  { symbol: 'CORPOCO.BO', name: 'CORPORATE COURIER AND CARGO LTD.' },
  { symbol: 'SHAWGELTIN.BO', name: 'NARMADA GELATINES LTD.' },
  { symbol: 'PGFOILQ.BO', name: 'PG FOILS LTD.' },
  { symbol: 'GRATEXI.BO', name: 'GRATEX INDUSTRIES LTD.' },
  { symbol: 'VELHO.BO', name: 'VELAN HOTELS LTD.' },
  { symbol: 'HOWARHO.BO', name: 'HOWARD HOTELS LTD.' },
  { symbol: 'VALIANT.BO', name: 'VALIANT COMMUNICATIONS LTD.-$' },
  { symbol: 'MHSGRMS.BO', name: 'MAHASAGAR TRAVELS LTD.' },
  { symbol: 'UNQTYMI.BO', name: 'UNION QUALITY PLASTICS LTD.' },
  { symbol: 'PSL.BO', name: 'PSL LTD.' },
  { symbol: 'RAGHUNAT.BO', name: 'RAGHUNATH INTERNATIONAL LTD.' },
  { symbol: 'RAJINFRA.BO', name: 'Rajeswari Infrastructure Limited' },
  { symbol: 'SPICEISL.BO', name: 'SPICE ISLANDS APPARELS LTD.-$' },
  { symbol: 'SIPL.BO', name: 'SHELTER INFRA PROJECTS LTD.-$' },
  { symbol: 'ATLANTADEV.BO', name: 'Atlanta Devcon Limited' },
  { symbol: 'ASHSI.BO', name: 'ASHIRWAD STEELS & INDUSTRIES LTD.' },
  { symbol: 'AREXMIS.BO', name: 'AREX INDUSTRIES LTD.' },
  { symbol: 'ISFL.BO', name: 'ISF LIMITED' },
  { symbol: 'RISHILASE.BO', name: 'RISHI LASER LTD.' },
  { symbol: 'JAINCO.BO', name: 'JAINCO PROJECTS (INDIA) LTD.' },
  { symbol: 'INTECCAP.BO', name: 'INTEC CAPITAL LTD.' },
  { symbol: 'RAJGASES.BO', name: 'RAJASTHAN GASES LTD.' },
  { symbol: 'FINANTECH.BO', name: 'FINANCIAL TECHNOLOGIES (INDIA) LTD.' },
  { symbol: 'INDOCRED.BO', name: 'INDO CREDIT CAPITAL LTD.' },
  { symbol: 'MKTCREAT.BO', name: 'MARKET CREATORS LTD.' },
  { symbol: 'HIMIN.BO', name: 'HIMALYA INTERNATIONAL LTD.' },
  { symbol: 'SONALAD.BO', name: 'SONAL ADHESIVES LTD.' },
  { symbol: 'CHDDLTD.BO', name: 'CHD DEVELOPERS LTD.-$' },
  { symbol: 'DION.BO', name: 'DION GLOBAL SOLUTIONS LTD.' },
  { symbol: 'HRYNSHP.BO', name: 'HARIYANA SHIP BREAKERS LTD.-$' },
  { symbol: 'MBPARIKH.BO', name: 'M.B.PARIKH FINSTOCKS LTD.' },
  { symbol: 'VAICC.BO', name: 'VAISHNO CEMENT CO.LTD.' },
  { symbol: 'TYROON.BO', name: 'TYROON TEA CO.LTD.' },
  { symbol: 'UVBOARDS.BO', name: 'UV BOARDS LTD.' },
  { symbol: 'WOODSVILA.BO', name: 'WOODSVILLA LTD.' },
  { symbol: 'GUJCRAFT.BO', name: 'GUJARAT CRAFT INDUSTRIES LTD.' },
  { symbol: 'HEERAISP.BO', name: 'HEERA ISPAT LTD.' },
  { symbol: 'DHOOTIN.BO', name: 'DHOOT INDUSTRIAL FINANCE LTD.' },
  { symbol: 'CRIMSON.BO', name: 'Crimson Metal Engineering Company Ltd' },
  { symbol: 'ASHOKRE.BO', name: 'ASHOKA REFINERIES LTD.' },
  { symbol: 'URJAGLOBA.BO', name: 'URJA GLOBAL LTD.' },
  { symbol: 'SHREEPAC.BO', name: 'SHREE PACETRONIX LTD.' },
  { symbol: 'MANGCHEFER.BO', name: 'MANGALORE CHEMICALS & FERTILIZERS LTD.' },
  { symbol: 'JUBILANT.BO', name: 'JUBILANT LIFE SCIENCES LIMITED' },
  { symbol: 'FORTUNEF.BO', name: 'FORTUNE FINANCIAL SERVICES (INDIA) LTD.' },
  { symbol: 'SAMYAKINT.BO', name: 'SAMYAK INTERNATIONAL LTD.' },
  { symbol: 'AADIIND.BO', name: 'AADI INDUSTRIES LTD.' },
  { symbol: 'SANTOSHF.BO', name: 'SANTOSH FINE-FAB LTD.' },
  { symbol: 'SREEJAYA.BO', name: 'SREE JAYALAKSHMI AUTOSPIN LTD.' },
  { symbol: 'ACKNIT.BO', name: 'ACKNIT INDUSTRIES LTD.' },
  { symbol: 'TITANSEC.BO', name: 'TITAN SECURITIES LTD.' },
  { symbol: 'RAIREKMOH.BO', name: 'RAI SAHEB REKHCHAND MOHOTA SPG.& WVG.MILLS LTD.' },
  { symbol: 'JJEXPO.BO', name: 'J.J.EXPORTERS LTD.' },
  { symbol: 'RREALTY.BO', name: 'REAL REALTY MANAGEMENT COMPANY LTD.' },
  { symbol: 'HRMNYCP.BO', name: 'HARMONY CAPITAL SERVICES LTD.' },
  { symbol: 'IVEE.BO', name: 'IVEE INJECTAA LTD.' },
  { symbol: 'YASHRAJC.BO', name: 'YASHRAJ CONTAINEURS LTD.' },
  { symbol: 'LORDSHOTL.BO', name: 'Lords Ishwar Hotels Limited' },
  { symbol: 'CNSDSEC.BO', name: 'CONSOLIDATED SECURITIES LTD.' },
  { symbol: 'SELAN.BO', name: 'SELAN EXPLORATION TECHNOLOGY LTD.-$' },
  { symbol: 'FRSHTRP.BO', name: 'FRESHTROP FRUITS LTD.' },
  { symbol: 'ZYDEN.BO', name: 'ZYDEN GENTEC LTD.' },
  { symbol: 'ACEEDU.BO', name: 'ACE EDUTREND LTD.' },
  { symbol: 'BHAGWNME.BO', name: 'BHAGWANDAS METALS LTD.' },
  { symbol: 'VANTAGE.BO', name: 'VANTAGE CORPORATE SERVICES LTD.' },
  { symbol: 'RAJPACK.BO', name: 'RAJ PACKAGING INDUSTRIES LTD.' },
  { symbol: 'ADIFINCHM.BO', name: 'ADI FINECHEM LTD.' },
  { symbol: 'NATRAJPR.BO', name: 'NATRAJ PROTEINS LTD.' },
  { symbol: 'SAMRATPH.BO', name: 'SAMRAT PHARMACHEM LTD.-$' },
  { symbol: 'NPRFIN.BO', name: 'NPR FINANCE LTD.' },
  { symbol: 'UDAICEMENT.BO', name: 'UDAIPUR CEMENT WORKS LTD.' },
  { symbol: 'AMCOIND.BO', name: 'AMCO INDIA LTD.-$' },
  { symbol: 'KREONFIN.BO', name: 'KREON FINNANCIAL SERVICES LTD.' },
  { symbol: 'GYANDEV.BO', name: 'GYAN DEVELOPERS & BUILDERS LTD.' },
  { symbol: 'KISAN.BO', name: 'KISAN MOULDINGS LTD.-$' },
  { symbol: 'KSLIND.BO', name: 'KSL AND INDUSTRIES LTD.-$' },
  { symbol: 'VIJAYTX.BO', name: 'VIJAY TEXTILES LTD.' },
  { symbol: 'GARODCH.BO', name: 'GARODIA CHEMICALS LTD.' },
  { symbol: 'KERALAYUR.BO', name: 'KERALA AYURVEDA LTD.' },
  { symbol: 'MONGIPA.BO', name: 'MOONGIPA CAPITAL FINANCE LTD.' },
  { symbol: 'MOHITPPR.BO', name: 'MOHIT PAPER MILLS LTD.' },
  { symbol: 'DAULAT.BO', name: 'DAULAT SECURITIES LTD.' },
  { symbol: 'OSCARGLO.BO', name: 'OSCAR GLOBAL LTD.' },
  { symbol: 'ODYSSEY.BO', name: 'ODYSSEY TECHNOLOGIES LTD.-$' },
  { symbol: 'SPSINT.BO', name: 'SPS INTERNATIONAL LTD.' },
  { symbol: 'RSCINT.BO', name: 'RSC INTERNATIONAL LTD.' },
  { symbol: 'SURATEX.BO', name: 'SURAT TEXTILE MILLS LTD.' },
  { symbol: 'ATHARVENT.BO', name: 'ATHARV ENTERPRISES LTD.' },
  { symbol: 'CHROMATIC.BO', name: 'CHROMATIC INDIA LTD.' },
  { symbol: 'BAGADIA.BO', name: 'BAGADIA COLOURCHEM LTD.' },
  { symbol: 'KALLAM.BO', name: 'KALLAM SPINNING MILLS LTD.-$' },
  { symbol: 'BRAWN.BO', name: 'BRAWN BIOTECH LTD.' },
  { symbol: 'FORINTL.BO', name: 'FORTUNE INTERNATIONAL LTD.' },
  { symbol: 'KINGSINFR.BO', name: 'Kings Infra Ventures Limited' },
  { symbol: 'NUTRICIRCLE.BO', name: 'Nutricircle Ltd' },
  { symbol: 'SUBSM.BO', name: 'SUBHASH SILK MILLS LTD.' },
  { symbol: 'AUROLAB.BO', name: 'AURO LABORATORIES LTD.' },
  { symbol: 'KJMCFIN.BO', name: 'KJMC FINANCIAL SERVICES LTD.' },
  { symbol: 'MANGASOF.BO', name: 'MANGALYA SOFT-TECH LTD.' },
  { symbol: 'ARYAMAN.BO', name: 'ARYAMAN FINANCIAL SERVICES LTD.' },
  { symbol: 'BRIDGESE.BO', name: 'BRIDGE SECURITIES LTD.' },
  { symbol: 'RISAINTL.BO', name: 'RISA INTERNATIONAL LTD.' },
  { symbol: 'RAJTUBE.BO', name: 'RAJASTHAN TUBE MANUFACTURING CO.LTD.' },
  { symbol: 'KAYPOWR.BO', name: 'KAY POWER AND PAPER LTD.' },
  { symbol: 'INTSTOIL.BO', name: 'INTER STATE OIL CARRIER LTD.' },
  { symbol: 'ARCUTTIP.BO', name: 'ARCUTTIPORE TEA CO.LTD.' },
  { symbol: 'GLOBALCA.BO', name: 'GLOBAL CAPITAL MARKETS LTD.' },
  { symbol: 'SAINIK.BO', name: 'SAINIK FINANCE & INDUSTRIES LTD.' },
  { symbol: 'SABOOBR.BO', name: 'SABOO BROTHERS LTD.' },
  { symbol: 'RICHUNV.BO', name: 'RICH UNIVERSE NETWORK LTD.' },
  { symbol: 'QUANTDIA.BO', name: 'QUANTUM DIGITAL VISION (INDIA) LTD.' },
  { symbol: 'SPCAPIT.BO', name: 'SP CAPITAL FINANCING LTD.' },
  { symbol: 'RAJAGRO.BO', name: 'RAJ AGRO MILLS LTD.' },
  { symbol: 'CHAMANSEQ.BO', name: 'CHAMAN LAL SETIA EXPORTS LTD.-$' },
  { symbol: 'CHANDRAP.BO', name: 'CHANDRA PRABHU INTERNATIONAL LTD.-$' },
  { symbol: 'HINDTIN.BO', name: 'HINDUSTAN TIN WORKS LTD.-$' },
  { symbol: 'GODAVARI.BO', name: 'GODAVARI DRUGS LTD.' },
  { symbol: 'UNIMININ.BO', name: 'UNIMIN INDIA LTD.' },
  { symbol: 'ERAINFRA.BO', name: 'ERA INFRA ENGINEERING LTD.-$' },
  { symbol: 'EMGEECA.BO', name: 'EMGEE CABLES & COMMUNICATIONS LTD.' },
  { symbol: 'MUKESHB.BO', name: 'MUKESH BABU FINANCIAL SERVICES LTD.' },
  { symbol: 'MAGNUML.BO', name: 'MAGNUM LTD.' },
  { symbol: 'ASIANOI.BO', name: 'ASIAN OILFIELD SERVICES LTD.' },
  { symbol: 'KBSINDIA.BO', name: 'KBS INDIA LIMITED' },
  { symbol: 'SEAGOLD.BO', name: 'SEA GOLD AQUA FARMS LTD.' },
  { symbol: 'VAMSHIRU.BO', name: 'VAMSHI RUBBER LTD.' },
  { symbol: 'NILA.BO', name: 'NILA INFRASTRUCTURES LTD.' },
  { symbol: 'PETRONENGG.BO', name: 'PETRON ENGINEERING CONSTRUCTION LTD.' },
  { symbol: 'GEEFC.BO', name: 'GEEFCEE FINANCE LTD.' },
  { symbol: 'VINYOFL.BO', name: 'VINYOFLEX LTD.' },
  { symbol: 'VALLABH.BO', name: 'VALLABH POLY-PLAST INTERNATIONAL LTD.' },
  { symbol: 'JINDCAP.BO', name: 'JINDAL CAPITAL LTD.' },
  { symbol: 'EPIC.BO', name: 'EPIC ENERGY LTD.-$' },
  { symbol: 'SUMEDHA.BO', name: 'SUMEDHA FISCAL SERVICES LTD.' },
  { symbol: 'KUWERIN.BO', name: 'KUWER INDUSTRIES LTD.' },
  { symbol: 'CHOKSI.BO', name: 'CHOKSI IMAGING LTD.' },
  { symbol: 'ASHISHPO.BO', name: 'ASHISH POLYPLAST LTD.' },
  { symbol: 'ADORFO.BO', name: 'ADOR FONTECH LTD.-$' },
  { symbol: 'SHIVAAGRO.BO', name: 'SHIVA GLOBAL AGRO INDUSTRIES LTD.' },
  { symbol: 'NOESISIND.BO', name: 'Noesis Industries Limited' },
  { symbol: 'SIDDHA.BO', name: 'SIDDHA VENTURES LTD.' },
  { symbol: 'MAHAN.BO', name: 'MAHANIVESH (INDIA) LTD.' },
  { symbol: 'KIRANSY-B.BO', name: 'KIRAN SYNTEX LTD.' },
  { symbol: 'RUNGTAIR.BO', name: 'RUNGTA IRRIGATION LTD.' },
  { symbol: 'CINERAD.BO', name: 'CINERAD COMMUNICATIONS LTD.' },
  { symbol: 'VALSONQ.BO', name: 'VALSON INDUSTRIES LTD.-$' },
  { symbol: 'SABOOSOD.BO', name: 'SABOO SODIUM CHLORO LTD.' },
  { symbol: 'GSLSEC.BO', name: 'GSL SECURITIES LTD.' },
  { symbol: 'VIKRAMTH.BO', name: 'VIKRAM THERMO (INDIA) LTD.' },
  { symbol: 'VIBROSO.BO', name: 'VIBROS ORGANICS LTD.' },
  { symbol: 'CHHATTIND.BO', name: 'CHHATTISGARH INDUSTRIES LTD.' },
  { symbol: 'MARVEL.BO', name: 'MARVEL CAPITAL & FINANCE (INDIA) LTD.' },
  { symbol: 'UPERGANGES.BO', name: 'UPPER GANGES SUGAR & INDUSTRIES LTD.' },
  { symbol: 'VIRAT.BO', name: 'VIRAT INDUSTRIES LTD.' },
  { symbol: 'SHEETAL.BO', name: 'SHEETAL DIAMONDS LTD.' },
  { symbol: 'TERAI.BO', name: 'TERAI TEA CO.LTD.' },
  { symbol: 'MANRAJH.BO', name: 'MANRAJ HOUSING FINANCE LTD.' },
  { symbol: 'MARG.BO', name: 'MARG LTD.' },
  { symbol: 'COSCO.BO', name: 'COSCO (INDIA) LTD.-$' },
  { symbol: 'SSLFINANCE.BO', name: 'ARCHANA SOFTWARE LTD.' },
  { symbol: 'EXPLICITFIN.BO', name: 'EXPLICIT FINANCE LTD.' },
  { symbol: 'LADDERUP.BO', name: 'LADDERUP FINANCE LTD.' },
  { symbol: 'GOLDENGOEN.BO', name: 'GOLDEN GOENKA FINCORP LIMITED' },
  { symbol: 'EKAMLEA.BO', name: 'EKAM LEASING & FINANCE CO.LTD.' },
  { symbol: 'SWASTIKA.BO', name: 'SWASTIKA INVESTMART LTD.' },
  { symbol: 'PRIMAPLA.BO', name: 'PRIMA PLASTICS LTD.' },
  { symbol: 'JAGSONFI.BO', name: 'JAGSONPAL FINANCE & LEASING LTD.' },
  { symbol: 'GSLNOVA.BO', name: 'GSL Nova Petrochemicals Limited' },
  { symbol: 'CARNATIN.BO', name: 'CARNATION INDUSTRIES LTD.' },
  { symbol: 'STURDY.BO', name: 'STURDY INDUSTRIES LTD.' },
  { symbol: 'GARGFUR.BO', name: 'GARG FURNACE LTD.' },
  { symbol: 'SAMPRE.BO', name: 'SAMPRE NUTRITIONS LTD.' },
  { symbol: 'AKARTOOL.BO', name: 'AKAR TOOLS LTD.' },
  { symbol: 'VIPULDYE.BO', name: 'VIPUL DYE CHEM LTD.' },
  { symbol: 'ECORECO.BO', name: 'ECO RECYCLING LTD.' },
  { symbol: 'SOFTECH.BO', name: 'SOFTECH INFINIUM SOLUTIONS LTD.' },
  { symbol: 'GOODLUC.BO', name: 'GOODLUCK STEEL TUBES LTD.' },
  { symbol: 'ZENITHHE.BO', name: 'ZENITH HEALTH CARE LTD.' },
  { symbol: 'PRESOFI.BO', name: 'PREM SOMANI FINANCIAL SERVICES LTD.' },
  { symbol: 'YORKEXP.BO', name: 'YORK EXPORTS LTD.' },
  { symbol: 'PITHP.BO', name: 'PITHAMPUR POLY PRODUCTS LTD.' },
  { symbol: 'LYKISLTD.BO', name: 'Lykis Limited' },
  { symbol: 'PRIMEPRO.BO', name: 'PRIME PROPERTY DEVELOPMENT CORPORATION LTD.-$' },
  { symbol: 'NEELKATEC.BO', name: 'NEELKANTH TECHNOLOGIES LTD.' },
  { symbol: 'RAJRAYON.BO', name: 'Raj Rayon Industries Limited-$' },
  { symbol: 'KDJHRL.BO', name: 'KDJ Holidayscapes and Resorts Limited' },
  { symbol: 'INFODRIVE.BO', name: 'INFO-DRIVE SOFTWARE LTD.' },
  { symbol: 'FLORATX.BO', name: 'FLORA TEXTILES LTD.' },
  { symbol: 'GOWRALE.BO', name: 'GOWRA LEASING & FINANCE LTD.' },
  { symbol: 'JAGANLAM.BO', name: 'JAGAN LAMPS LTD.' },
  { symbol: 'AJEL.BO', name: 'AJEL LTD.' },
  { symbol: 'ALPSINDUS.BO', name: 'ALPS INDUSTRIES LTD.' },
  { symbol: 'ANGIND.BO', name: 'ANG Industries Limited' },
  { symbol: 'ASITCFIN.BO', name: 'ASIT C.MEHTA FINANCIAL SERVICES LTD.' },
  { symbol: 'NOVAPUB.BO', name: 'Nova Publications India Ltd' },
  { symbol: 'SUPERBAK.BO', name: 'SUPER BAKERS (INDIA) LTD.' },
  { symbol: 'GEINDSYS.BO', name: 'GEI INDUSTRIAL SYSTEMS LTD.' },
  { symbol: 'LNIND.BO', name: 'LN INDUSTRIES INDIA LTD.' },
  { symbol: 'INDOASIAF.BO', name: 'INDO ASIAN FINANCE LTD.' },
  { symbol: 'DEVKI.BO', name: 'DEVKI LEASING & FINANCE LTD.' },
  { symbol: 'KLGCAP.BO', name: 'KLG CAPITAL SERVICES LTD.' },
  { symbol: 'IVRCLINFRA.BO', name: 'IVRCL LTD' },
  { symbol: 'INFRAIND.BO', name: 'INFRA INDUSTRIES LTD.' },
  { symbol: 'DYNAMICP.BO', name: 'DYNAMIC PORTFOLIO MANAGEMENT & SERVICES LTD.' },
  { symbol: 'INTERHG.BO', name: 'INTERNATIONAL HOUSING FINANCE CORPORATION LTD.' },
  { symbol: 'TRANSASIA.BO', name: 'TRANS ASIA CORPORATION LTD.' },
  { symbol: 'INLANPR.BO', name: 'INLAND PRINTERS LTD.' },
  { symbol: 'CEEJAY.BO', name: 'CEEJAY FINANCE LTD.' },
  { symbol: 'SUNCITYSY.BO', name: 'SUNCITY SYNTHETICS LTD.' },
  { symbol: 'SHGANEL.BO', name: 'SHREE GANESH ELASTOPLAST LTD.' },
  { symbol: 'ANNAINFRA.BO', name: 'ANNA INFRASTRUCTURES LTD.' },
  { symbol: 'DIAMOND.BO', name: 'DIAMOND INFOSYSTEMS LTD.' },
  { symbol: 'REGALIAA.BO', name: 'REGALIAA REALTY LTD.' },
  { symbol: 'BNRUDY.BO', name: 'BNR UDYOG LTD.' },
  { symbol: 'NETVISTAIT.BO', name: 'NETVISTA INFORMATION TECHNOLOGY LTD.' },
  { symbol: 'REFNOL.BO', name: 'REFNOL RESINS & CHEMICALS LTD.' },
  { symbol: 'SSPDL.BO', name: 'SSPDL LTD.-$' },
  { symbol: 'DAIKAFFI.BO', name: 'DAIKAFFIL CHEMICALS INDIA LTD.' },
  { symbol: 'CILSEC.BO', name: 'CIL SECURITIES LTD.' },
  { symbol: 'CLIOINFO.BO', name: 'CLIO INFOTECH LTD.' },
  { symbol: 'SHRIBCL.BO', name: 'Shri Bholanath Carpets Limited' },
  { symbol: 'SUNSHIEL.BO', name: 'SUNSHIELD CHEMICALS LTD.' },
  { symbol: 'HIPOLIN.BO', name: 'HIPOLIN LTD.' },
  { symbol: 'GDLLEAS.BO', name: 'GDL LEASING & FINANCE LTD.' },
  { symbol: 'COSBOARD.BO', name: 'COSBOARD INDUSTRIES LTD.' },
  { symbol: 'FEINDIALTD.BO', name: 'FE (India) Ltd' },
  { symbol: 'SFLINTER.BO', name: 'SFL International Ltd' },
  { symbol: 'BHILTEX.BO', name: 'BHILWARA TEX-FIN LTD.' },
  { symbol: 'CENPORT.BO', name: 'CENTURY TWENTYFIRST PORTFOLIO LTD.' },
  { symbol: 'SUCROSA.BO', name: 'SUPER CROP SAFE LTD.' },
  { symbol: 'AURUMSOFT.BO', name: 'Aurum Soft Systems Limited' },
  { symbol: 'ALKA.BO', name: 'ALKA INDIA LTD.' },
  { symbol: 'NGIND.BO', name: 'N.G.INDUSTRIES LTD.-$' },
  { symbol: 'ASIAPAK.BO', name: 'ASIA PACK LTD.' },
  { symbol: 'ACIL.BO', name: 'ACIL Cotton Industries Limited' },
  { symbol: 'SAINDUS.BO', name: 'SAI INDUSTRIES LTD.' },
  { symbol: 'ERPSOFT.BO', name: 'ERP SOFT SYSTEMS LTD.' },
  { symbol: 'JRFOODS.BO', name: 'J.R.FOODS LTD.' },
  { symbol: 'RRSECUR.BO', name: 'R.R.SECURITIES LTD.' },
  { symbol: 'INTETHR.BO', name: 'INTEGRATED THERMOPLASTICS LTD.' },
  { symbol: 'RCLFOODS.BO', name: 'RCL Foods Limited' },
  { symbol: 'RAMSONS.BO', name: 'RAMSONS PROJECTS LTD.' },
  { symbol: 'HARAFIN.BO', name: 'HARYANA FINANCIAL CORPORATION LTD.' },
  { symbol: 'STANPACK.BO', name: 'STANPACKS (INDIA) LTD.' },
  { symbol: 'SABTN.BO', name: 'SRI ADHIKARI BROTHERS TELEVISION NETWORK LTD.' },
  { symbol: 'GISL.BO', name: 'GANGOTRI IRON & STEEL COMPANY LTD.' },
  { symbol: 'RAMINFO.BO', name: 'RAMINFO LIMITED' },
  { symbol: 'SUNILAGR.BO', name: 'SUNIL AGRO FOODS LTD.-$' },
  { symbol: 'MINDVISCAP.BO', name: 'Mindvision Capital Ltd' },
  { symbol: 'DIANATEA.BO', name: 'DIANA TEA CO.LTD.' },
  { symbol: 'KYRALANDS.BO', name: 'Kyra Landscapes Limited' },
  { symbol: 'NIMBUSI.BO', name: 'NIMBUS INDUSTRIES LTD.' },
  { symbol: 'ALFAICA.BO', name: 'ALFA ICA (INDIA) LTD.' },
  { symbol: 'KATWAUD.BO', name: 'KATWA UDYOG LTD.' },
  { symbol: 'INDIAHOME.BO', name: 'INDIA HOME LOAN LTD.' },
  { symbol: 'JPTSEC.BO', name: 'JPT SECURITIES LTD.' },
  { symbol: 'ROOPAIND.BO', name: 'ROOPA INDUSTRIES LTD.' },
  { symbol: 'SARTHAKGL.BO', name: 'SARTHAK GLOBAL LTD.' },
  { symbol: 'UNIQUEO.BO', name: 'UNIQUE ORGANICS LTD.' },
  { symbol: 'SWRNASE.BO', name: 'SWARNA SECURITIES LTD.' },
  { symbol: 'VENMAX.BO', name: 'Venmax Drugs And Pharmaceuticals Ltd' },
  { symbol: 'ARISE.BO', name: 'ARIHANT\'S SECURITIES LTD.' },
  { symbol: 'VISAGAR.BO', name: 'VISAGAR FINANCIAL SERVICES LTD.' },
  { symbol: 'LIBORD.BO', name: 'LIBORD SECURITIES LTD.' },
  { symbol: 'BHATEXT.BO', name: 'BHARAT TEXTILES & PROOFING INDUSTRIES LTD.' },
  { symbol: 'REGAL.BO', name: 'REGAL ENTERTAINMENT & CONSULTANTS LTD.' },
  { symbol: 'TOBUENT.BO', name: 'TOBU ENTERPRISES LTD.' },
  { symbol: 'COMPEAU.BO', name: 'COMPETENT AUTOMOBILES CO.LTD.' },
  { symbol: 'ZDHJERK.BO', name: 'DHANVANTRI JEEVAN REKHA LTD.' },
  { symbol: 'ADVPOWER.BO', name: 'Advance PowerInfra Tech Limited' },
  { symbol: 'NEELKAN.BO', name: 'NEELKANTH ROCKMINERALS LTD.' },
  { symbol: 'VINTAGES.BO', name: 'VINTAGE SECURITIES LTD.' },
  { symbol: 'GFLFIN.BO', name: 'GFL Financials India Limited' },
  { symbol: 'OSWALOR.BO', name: 'OSWAL OVERSEAS LTD.' },
  { symbol: 'CONTILI.BO', name: 'CONTIL INDIA LTD.' },
  { symbol: 'VIJSOLX.BO', name: 'VIJAY SOLVEX LTD.' },
  { symbol: 'NIHARINF.BO', name: 'NIHAR INFO GLOBAL LTD.' },
  { symbol: 'TULIPSTA.BO', name: 'TULIP STAR HOTELS LTD.-$' },
  { symbol: 'UNITDCR.BO', name: 'UNITED CREDIT LTD.' },
  { symbol: 'OMMETALS.BO', name: 'OM METALS INFRAPROJECTS LTD.' },
  { symbol: 'SURANACORP.BO', name: 'SURANA CORPORATION LTD.' },
  { symbol: 'GOTHIPL.BO', name: 'GOTHI PLASCON (INDIA) LTD.' },
  { symbol: 'BELAGRO.BO', name: 'BELL AGROMACHINA LTD.' },
  { symbol: 'SYNERGY.BO', name: 'SYNERGY COSMETICS (EXIM) LTD.' },
  { symbol: 'CEENIK.BO', name: 'CEENIK EXPORTS (INDIA) LTD.' },
  { symbol: 'VIRTUALS.BO', name: 'VIRTUALSOFT SYSTEMS LTD.' },
  { symbol: 'ANARINDUS.BO', name: 'ANAR INDUSTRIES LTD.' },
  { symbol: 'INANI.BO', name: 'INANI MARBLES & INDUSTRIES LTD.' },
  { symbol: 'LEWATERIN.BO', name: 'Le Waterina Resorts & Hotels Limited' },
  { symbol: 'GEMSI.BO', name: 'GEMSTONE INVESTMENTS LTD.' },
  { symbol: 'SRANGMARK.BO', name: 'SHREE RANG MARK TRAVELS LTD.' },
  { symbol: 'DILIGENT.BO', name: 'DILIGENT INDUSTRIES LTD.' },
  { symbol: 'EPSOMPRO.BO', name: 'EPSOM PROPERTIES LTD.' },
  { symbol: 'ALFAVIO.BO', name: 'ALFAVISION OVERSEAS (INDIA) LTD.' },
  { symbol: 'ORGCOAT.BO', name: 'ORGANIC COATINGS LTD.' },
  { symbol: 'CATVISION.BO', name: 'Catvision Limited' },
  { symbol: 'SAUMYACAP.BO', name: 'Saumya Capital Limited' },
  { symbol: 'LADIAMO.BO', name: 'LASER DIAMONDS LTD.' },
  { symbol: 'SKPSEC.BO', name: 'SKP SECURITIES LTD.' },
  { symbol: 'PRANAVSP.BO', name: 'PRANAVADITYA SPINNING MILLS LTD.' },
  { symbol: 'SYSCHEM.BO', name: 'SYSCHEM (INDIA) LTD.' },
  { symbol: 'BLSINFOTE.BO', name: 'BLS INFOTECH LTD.' },
  { symbol: 'MEFCOMCAP.BO', name: 'MEFCOM CAPITAL MARKETS LTD.' },
  { symbol: 'HEMANG.BO', name: 'Hemang Resources Ltd' },
  { symbol: 'TAVERNIER.BO', name: 'Tavernier Resources Limi' },
  { symbol: 'FILTRON.BO', name: 'FILTRON ENGINEERS LTD.' },
  { symbol: 'GAGANPO.BO', name: 'GAGAN POLYCOT INDIA LTD.' },
  { symbol: 'DHANADACO.BO', name: 'DHANADA CORPORATION LTD.' },
  { symbol: 'GLANCE.BO', name: 'GLANCE FINANCE LTD.' },
  { symbol: 'BRANDREAL.BO', name: 'BRAND REALTY SERVICES LTD.' },
  { symbol: 'KANSAFB.BO', name: 'KANSAL FIBRES LTD.' },
  { symbol: 'KWALITYCL.BO', name: 'KWALITY CREDIT & LEASING LTD.' },
  { symbol: 'RAYLA.BO', name: 'RAYMED LABS LTD.' },
  { symbol: 'COLINZ.BO', name: 'COLINZ LABORATORIES LTD.' },
  { symbol: 'WELLNESS.BO', name: 'WELLNESS NONI LTD.' },
  { symbol: 'NALINLEA.BO', name: 'NALIN LEASE FINANCE LTD.' },
  { symbol: 'RTSPOWR.BO', name: 'RTS POWER CORPORATION LTD.' },
  { symbol: 'WESTE.BO', name: 'WESTERN INDIA SHIPYARD LTD.' },
  { symbol: 'SHYAMAINFO.BO', name: 'SHYAMA INFOSYS LTD.' },
  { symbol: 'MAYURFL.BO', name: 'MAYUR FLOORINGS LTD.' },
  { symbol: 'ANJANI.BO', name: 'ANJANI SYNTHETICS LTD.-$' },
  { symbol: 'FRONTIER.BO', name: 'Frontier Informatics Limited' },
  { symbol: 'DECOMIC.BO', name: 'DECO-MICA LTD.' },
  { symbol: 'RASIELEC.BO', name: 'RASI ELECTRODES LTD.' },
  { symbol: 'CITIPOR.BO', name: 'CITIPORT FINANCIAL SERVICES LTD.' },
  { symbol: 'MEWARPOL.BO', name: 'MEWAR POLYTEX LTD.' },
  { symbol: 'DHRUVCA.BO', name: 'DHRUVA CAPITAL SERVICES LTD.' },
  { symbol: 'LINCPENQ.BO', name: 'LINC PEN & PLASTICS LTD.-$' },
  { symbol: 'PRIMAIN.BO', name: 'PRIMA INDUSTRIES LTD.' },
  { symbol: 'RNBIND.BO', name: 'RNB INDUSTRIES LTD.' },
  { symbol: 'FARRYIND.BO', name: 'FARRY INDUSTRIES LTD.' },
  { symbol: 'INDGELA.BO', name: 'INDIA GELATINE & CHEMICALS LTD.-$' },
  { symbol: 'TRANSPEKF.BO', name: 'TRANSPEK FINANCE LTD.' },
  { symbol: 'PARAGONF.BO', name: 'PARAGON FINANCE LTD.' },
  { symbol: 'PRATIKSH.BO', name: 'PRATIKSHA CHEMICALS LTD.' },
  { symbol: 'ESHAMEDIA.BO', name: 'Esha Media Research Limited' },
  { symbol: 'YKMIND.BO', name: 'YKM INDUSTRIES LTD.' },
  { symbol: 'PROGRESV.BO', name: 'PROGRESSIVE EXTRACTIONS & EXPORTS LTD.' },
  { symbol: 'B2BSOFT.BO', name: 'B2B SOFTWARE TECHNOLOGIES LTD.' },
  { symbol: 'DAZZEL.BO', name: 'DAZZEL CONFINDIVE LTD.' },
  { symbol: 'NIKKIGL.BO', name: 'NIKKI GLOBAL FINANCE LTD.' },
  { symbol: 'RADHEDE.BO', name: 'RADHE DEVELOPERS (INDIA) LTD.' },
  { symbol: 'KINETRU.BO', name: 'KINETIC TRUST LTD.' },
  { symbol: 'ELIXIR.BO', name: 'Elixir Capital Ltd' },
  { symbol: 'TRISHAKT.BO', name: 'TRISHAKTI ELECTRONICS & INDUSTRIES LTD.' },
  { symbol: 'PANKAJPO.BO', name: 'PANKAJ POLYMERS LTD.' },
  { symbol: 'PGINDST.BO', name: 'PG INDUSTRY LTD.' },
  { symbol: 'CINDRELL.BO', name: 'CINDRELLA FINANCIAL SERVICES LTD.' },
  { symbol: 'NATPLASTI.BO', name: 'NATIONAL PLASTIC TECHNOLOGIES LTD.' },
  { symbol: 'LEADFIN.BO', name: 'LEAD FINANCIAL SERVICES LTD.' },
  { symbol: 'NATFIT.BO', name: 'National Fittings Limited' },
  { symbol: 'ARTEFACT.BO', name: 'ARTEFACT PROJECTS LTD.' },
  { symbol: 'AMITINT.BO', name: 'AMIT INTERNATIONAL LTD.' },
  { symbol: 'HIGHSTREE.BO', name: 'HIGH STREET FILATEX LTD.' },
  { symbol: 'NUTECGLOB.BO', name: 'NUTECH GLOBAL LTD.' },
  { symbol: 'DHPIND.BO', name: 'DHP INDIA LTD.' },
  { symbol: 'SRK.BO', name: 'S R K INDUSTRIES LTD.' },
  { symbol: 'INTCAPM.BO', name: 'INTEGRA CAPITAL MANAGEMENT LTD.' },
  { symbol: 'MARUTISE.BO', name: 'MARUTI SECURITIES LTD.' },
  { symbol: 'SANTASPN.BO', name: 'SANTARAM SPINNERS LTD.' },
  { symbol: 'ROSELABS.BO', name: 'ROSELABS FINANCE LTD.' },
  { symbol: 'CHARMS.BO', name: 'CHARMS INDUSTRIES LTD.' },
  { symbol: 'IDEAOPT.BO', name: 'IDEAL OPTICS LTD.' },
  { symbol: 'AUROCOK.BO', name: 'AUROMA COKE LTD.' },
  { symbol: 'IRISMEDIA.BO', name: 'IRIS MEDIAWORKS LTD.' },
  { symbol: 'MILESTONE.BO', name: 'MILESTONE GLOBAL LTD.' },
  { symbol: 'JAIHINDPRO.BO', name: 'JAIHIND PROJECTS LTD.' },
  { symbol: 'BERVINL.BO', name: 'BERVIN INVESTMENT & LEASING LTD.' },
  { symbol: 'GUJINV.BO', name: 'GUJARAT INVESTA LTD.' },
  { symbol: 'INDINFRA.BO', name: 'INDIA INFRASPACE LTD.' },
  { symbol: 'EASTRED.BO', name: 'EASTERN TREADS LTD.' },
  { symbol: 'PEETISEC.BO', name: 'PEETI SECURITIES LTD.' },
  { symbol: 'SRAMSET.BO', name: 'SHRIRAM ASSET MANAGEMENT CO.LTD.' },
  { symbol: 'GOLECHA.BO', name: 'GOLECHHA GLOBAL FINANCE LTD.' },
  { symbol: 'KARURKCP.BO', name: 'KARUR K.C.P.PACKKAGINGS LTD.' },
  { symbol: 'AQUAPIV.BO', name: 'Aqua Pumps Infra Ventures Limited' },
  { symbol: 'DOLLEX.BO', name: 'DOLLEX INDUSTRIES LTD.' },
  { symbol: 'SPARCSYS.BO', name: 'SPARC SYSTEMS LTD.' },
  { symbol: 'THEBYKE.BO', name: 'THE BYKE HOSPITALITY LTD.' },
  { symbol: 'CSURGSU.BO', name: 'CENTENIAL SURGICAL SUTURE LTD.' },
  { symbol: 'JAVNTPR.BO', name: 'JAYAVANT PRODUCTS LTD.' },
  { symbol: 'HASTIFIN.BO', name: 'HASTI FINANCE LTD.' },
  { symbol: 'UPSURGE.BO', name: 'UPSURGE INVESTMENT & FINANCE LTD.' },
  { symbol: 'ASFLORA.BO', name: 'ASIAN FLORA LTD.' },
  { symbol: 'PADAMCO.BO', name: 'PADAM COTTON YARNS LTD.' },
  { symbol: 'WOMENNET.BO', name: 'WOMEN NETWORKS LTD.' },
  { symbol: 'POLYCON.BO', name: 'POLYCON INTERNATIONAL LTD.' },
  { symbol: 'SOURCENTRL.BO', name: 'SOURCE NATURAL FOODS & HERBAL SUPPL LTD.' },
  { symbol: 'LWSKNIT.BO', name: 'LWS KNITWEAR LTD.' },
  { symbol: 'ZICOM.BO', name: 'ZICOM ELECTRONIC SECURITY SYSTEMS LTD.-$' },
  { symbol: 'ANSINDUS.BO', name: 'ANS Industries Ltd' },
  { symbol: 'ALCHCORP.BO', name: 'ALCHEMIST CORPORATION LTD.' },
  { symbol: 'GUJFOIL.BO', name: 'GUJARAT FOILS LTD.' },
  { symbol: 'TUNITEX.BO', name: 'TUNI TEXTILE MILLS LTD.' },
  { symbol: 'RADIXIND.BO', name: 'Radix Industries (India) Limited' },
  { symbol: 'KIRANPR.BO', name: 'KIRAN PRINT-PACK LTD.' },
  { symbol: 'NARPROP.BO', name: 'NARENDRA PROPERTIES LTD.' },
  { symbol: 'MEGACOR.BO', name: 'MEGA CORPORATION LTD.' },
  { symbol: 'BMBMUMG.BO', name: 'BMB MUSIC & MAGNETICS LTD.' },
  { symbol: 'ADVENT.BO', name: 'ADVENT COMPUTER SERVICES LTD.' },
  { symbol: 'SYTIXSE.BO', name: 'SYSTEMATIX SECURITIES LTD.' },
  { symbol: 'SUNGOLD.BO', name: 'SUNGOLD CAPITAL LTD.' },
  { symbol: 'SAFFRON.BO', name: 'Saffron Industries Limited' },
  { symbol: 'PWASML.BO', name: 'Prakash Woollen & Synthetic Mills Ltd' },
  { symbol: 'VARDHMAN.BO', name: 'Vardhman Concrete Limited' },
  { symbol: 'ROCKONENT.BO', name: 'Rockon Enterprises Ltd' },
  { symbol: 'SAFALSEC.BO', name: 'Safal Securities Ltd' },
  { symbol: 'NCCFIN.BO', name: 'NCC FINANCE LTD.' },
  { symbol: 'POLYLINK.BO', name: 'POLYLINK POLYMERS (INDIA) LTD.' },
  { symbol: 'MINAXI.BO', name: 'MINAXI TEXTILES LTD.' },
  { symbol: 'CONTICON.BO', name: 'CONTINENTAL CONTROLS LTD.' },
  { symbol: 'GBLINFRA.BO', name: 'Global Infratech & Finance limited' },
  { symbol: 'NOUVEAU.BO', name: 'NOUVEAU GLOBAL VENTURES LTD.' },
  { symbol: 'ARNAVCORP.BO', name: 'ARNAV CORPORATION LTD.' },
  { symbol: 'DUKEOFS.BO', name: 'DUKE OFFSHORE LTD.' },
  { symbol: 'CYBELEIND.BO', name: 'CYBELE INDUSTRIES LTD.' },
  { symbol: 'FILME.BO', name: 'FILMCITY MEDIA LTD.' },
  { symbol: 'CGVAK.BO', name: 'CG-VAK SOFTWARE & EXPORTS LTD.' },
  { symbol: 'NAVBLDR.BO', name: 'NAVKAR BUILDERS LTD.' },
  { symbol: 'OMKAR.BO', name: 'OMKAR OVERSEAS LTD.' },
  { symbol: 'SYBLY.BO', name: 'SYBLY INDUSTRIES LTD.-$' },
  { symbol: 'ESARIND.BO', name: 'ESAAR (INDIA) LTD.' },
  { symbol: 'INDERGR.BO', name: 'INDERGIRI FINANCE LTD.' },
  { symbol: 'SKRABUL.BO', name: 'SHUKRA BULLIONS LTD.' },
  { symbol: 'STEP2COR.BO', name: 'STEP TWO CORPORATION LTD.' },
  { symbol: 'MAHANIN.BO', name: 'MAHAN INDUSTRIES LTD.' },
  { symbol: 'VIKASGRAN.BO', name: 'VIKAS GRANARIES LTD.' },
  { symbol: 'ANKUSHFI.BO', name: 'ANKUSH FINSTOCK LTD.' },
  { symbol: 'DESHRAK.BO', name: 'DESH RAKSHAK AUSHDHALAYA LTD.' },
  { symbol: 'RASOYPR.BO', name: 'RASOYA PROTEINS LTD.' },
  { symbol: 'ICSA.BO', name: 'ICSA (INDIA) LTD.-$' },
  { symbol: 'ACESOFT.BO', name: 'ACE SOFTWARE EXPORTS LTD.-$' },
  { symbol: 'ECOM.BO', name: 'E.COM INFOTECH (I) LTD.' },
  { symbol: 'RISHDIGA.BO', name: 'RISHABH DIGHA STEEL & ALLIED PRODUCTS LTD.-$' },
  { symbol: 'MAINFRA.BO', name: 'MARUTI INFRASTRUCTURE LTD.' },
  { symbol: 'AVONLIFE.BO', name: 'Avon Lifesciences Ltd-$' },
  { symbol: 'TIRIN.BO', name: 'TIRUPATI INDUSTRIES (INDIA) LTD.' },
  { symbol: 'JHACC.BO', name: 'JHAVERI CREDITS & CAPITAL LTD.' },
  { symbol: 'RAGHUTOB.BO', name: 'RAGHUNATH TOBACCO CO.LTD.' },
  { symbol: 'ARIAC.BO', name: 'ARIHANT AVENUES & CREDIT LTD.' },
  { symbol: 'AMITSEC.BO', name: 'AMIT SECURITIES LTD.' },
  { symbol: 'AROMAENT.BO', name: 'AROMA ENTERPRISES (INDIA) LTD.' },
  { symbol: 'PUSHPIN.BO', name: 'PUSHPSONS INDUSTRIES LTD.' },
  { symbol: 'INDOPACIFIC.BO', name: 'Indo Pacific Projects Ltd' },
  { symbol: 'ASHUTPM.BO', name: 'ASHUTOSH PAPER MILLS LTD.' },
  { symbol: 'SANJIVIN.BO', name: 'SANJIVANI PARANTERAL LTD.' },
  { symbol: 'VASINFRA.BO', name: 'VAS INFRASTRUCTURE LTD.' },
  { symbol: 'KMFBLDR.BO', name: 'KMF BUILDERS & DEVELOPERS LTD.' },
  { symbol: 'ALKADIA.BO', name: 'ALKA DIAMOND INDUSTRIES LTD.' },
  { symbol: 'BERYLSE.BO', name: 'BERYL SECURITIES LTD.' },
  { symbol: 'RAP.BO', name: 'RAP MEDIA LTD.' },
  { symbol: 'DEVINE.BO', name: 'DEVINE IMPEX LTD.' },
  { symbol: 'BILPOWER.BO', name: 'BILPOWER LTD.' },
  { symbol: 'BAMPSL.BO', name: 'BAMPSL SECURITIES LTD.' },
  { symbol: 'ADIRASA.BO', name: 'ADI RASAYAN LTD.' },
  { symbol: 'INCON.BO', name: 'INCON ENGINEERS LTD.' },
  { symbol: 'NIMBUSFOO.BO', name: 'NIMBUS FOODS INDUSTRIES LTD.' },
  { symbol: 'GOGIACAP.BO', name: 'Gogia Capital Services Limited' },
  { symbol: 'KOFFBREAK.BO', name: 'KOFFEE BREAK PICTURES LTD.' },
  { symbol: 'GORANIN.BO', name: 'GORANI INDUSTRIES LTD.' },
  { symbol: 'KGPETRO.BO', name: 'KG PETROCHEM LTD.' },
  { symbol: 'AADHAARVEN.BO', name: 'AADHAAR VENTURES INDIA LTD.' },
  { symbol: 'KGNIND.BO', name: 'KGN INDUSTRIES LTD.' },
  { symbol: 'GIVO.BO', name: 'GIVO LTD.' },
  { symbol: 'ERABUILD.BO', name: 'Era Buildsys Limited' },
  { symbol: 'STARCOM.BO', name: 'Starcom Information Technology Ltd' },
  { symbol: 'CENTERAC.BO', name: 'CENTERAC TECHNOLOGIES LTD.' },
  { symbol: 'OROSMITHS.BO', name: 'OROSIL SMITHS INDIA LTD.-$' },
  { symbol: 'STERSPN.BO', name: 'STERLING SPINNERS LTD.' },
  { symbol: 'LINCOPH.BO', name: 'LINCOLN PHARMACEUTICALS LTD.' },
  { symbol: 'SILVOAK.BO', name: 'SILVER OAK (INDIA) LTD.' },
  { symbol: 'SICL.BO', name: 'Suvidha Infraestate Corporation Limited' },
  { symbol: 'TOKYOFIN.BO', name: 'TOKYO FINANCE LTD.' },
  { symbol: 'MAHAVIRIND.BO', name: 'Mahavir Industries Limited' },
  { symbol: 'VAXHS.BO', name: 'VAX HOUSING FINANCE CORPORATION LTD.' },
  { symbol: 'NATGENI.BO', name: 'NATIONAL GENERAL INDUSTRIES LTD.' },
  { symbol: 'THIRDFIN.BO', name: 'THIRDWAVE FINANCIAL INTERMEDIARIES LTD.' },
  { symbol: 'TRIJAL.BO', name: 'TRIJAL INDUSTRIES LTD.' },
  { symbol: 'TSL.BO', name: 'TSL INDUSTRIES LTD.' },
  { symbol: 'HITTCO.BO', name: 'HITTCO TOOLS LTD.' },
  { symbol: 'YUVRAAJHPL.BO', name: 'YUVRAAJ HYGIENE PRODUCTS LTD.' },
  { symbol: 'SHGOVTR.BO', name: 'SHREE SURGOVIND TRADELINK LTD.' },
  { symbol: 'BISIL.BO', name: 'Bisil Plast Limited' },
  { symbol: 'INANISEC.BO', name: 'INANI SECURITIES LTD.' },
  { symbol: 'ANKIN.BO', name: 'ANKA INDIA LTD.' },
  { symbol: 'INDOASIAP.BO', name: 'INDO-ASIAN PROJECTS LTD.' },
  { symbol: 'TRICOM.BO', name: 'TRICOM INDIA LTD.-$' },
  { symbol: 'VAGHANI.BO', name: 'VAGHANI TECHNO-BUILD LTD.' },
  { symbol: 'ARISINT.BO', name: 'ARIS INTERNATIONAL LTD.' },
  { symbol: 'MAYUR.BO', name: 'MAYUR LEATHER PRODUCTS LTD.' },
  { symbol: 'AMARDEE.BO', name: 'AMRADEEP INDUSTRIES LTD.' },
  { symbol: 'CATECH.BO', name: 'CAT TECHNOLOGIES LTD.' },
  { symbol: 'ADVIKLA.BO', name: 'ADVIK LABORATORIES LTD.' },
  { symbol: 'KGL.BO', name: 'KARUTURI GLOBAL LTD.' },
  { symbol: 'PRITHVISOF.BO', name: 'PRITHVI SOFTECH LTD.' },
  { symbol: 'RAINBOWF.BO', name: 'RAINBOW FOUNDATIONS LTD.' },
  { symbol: 'JAGPRO.BO', name: 'Jagran Production Limited' },
  { symbol: 'SUPRDOM.BO', name: 'SUPER DOMESTIC MACHINES LTD.' },
  { symbol: 'TRIBHSG.BO', name: 'TRIBHUVAN HOUSING LTD.' },
  { symbol: 'TRILOGIC.BO', name: 'TRILOGIC DIGITAL MEDIA LTD.' },
  { symbol: 'BLUCHIP.BO', name: 'BLUECHIP STOCKSPIN LTD.' },
  { symbol: 'TRICOMFRU.BO', name: 'Tricom Fruit Products Limited' },
  { symbol: 'VIDHIDYE.BO', name: 'VIDHI DYESTUFFS MANUFACTURING LTD.-$' },
  { symbol: 'STAMPEDE.BO', name: 'Stampede Capital Limited' },
  { symbol: 'HRBFLOR.BO', name: 'HRB FLORICULTURE LTD.' },
  { symbol: 'PANCHSHEEL.BO', name: 'PANCHSHEEL ORGANICS LTD.' },
  { symbol: 'AARYAGLOBL.BO', name: 'AARYA GLOBAL SHARES AND SECURITIES LTD.' },
  { symbol: 'PRISMFN.BO', name: 'PRISM FINANCE LTD.' },
  { symbol: 'GREENCREST.BO', name: 'Greencrest Financial Services Limited' },
  { symbol: 'SILICON.BO', name: 'SILICON VALLEY INFOTECH LTD.' },
  { symbol: 'GENNEX.BO', name: 'GENNEX LABORATORIES LTD.' },
  { symbol: 'HIRAUTO.BO', name: 'HIRA AUTOMOBILES LTD.' },
  { symbol: 'GINISILK.BO', name: 'GINI SILK MILLS LTD.-$' },
  { symbol: 'SUNTECHNO.BO', name: 'SUN TECHNO OVERSEAS LTD.' },
  { symbol: 'GKCONS.BO', name: 'G.K.CONSULTANTS LTD.' },
  { symbol: 'AMULEAS.BO', name: 'AMULYA LEASING & FINANCE LTD.' },
  { symbol: 'UNJHAFOR.BO', name: 'UNJHA FORMULATIONS LTD.' },
  { symbol: 'TOWASOK.BO', name: 'TOWA SOKKI LTD.' },
  { symbol: 'KACHCHH.BO', name: 'KACHCHH MINERALS LTD.' },
  { symbol: 'KACL.BO', name: 'Kaiser Corporation Limited' },
  { symbol: 'SAPANCHEM.BO', name: 'SAPAN CHEMICALS LTD.' },
  { symbol: 'KCLINFRA.BO', name: 'KCL Infra Projects Ltd' },
  { symbol: 'NOVAGOLD.BO', name: 'NOVAGOLD PETRO-RESOURCES LTD.' },
  { symbol: 'SESHACHAL.BO', name: 'SESHACHAL TECHNOLOGIES LTD.' },
  { symbol: 'SCANPRO.BO', name: 'SCAN PROJECTS LTD.' },
  { symbol: 'PRERINFRA.BO', name: 'PRERNA INFRABUILD LTD.' },
  { symbol: 'METALCO.BO', name: 'METAL COATINGS (INDIA) LTD.' },
  { symbol: 'SGNTE.BO', name: 'SGN TELECOMS LTD.' },
  { symbol: 'GANGAPA.BO', name: 'GANGA PAPERS INDIA LTD.' },
  { symbol: 'TIRSARJ.BO', name: 'TIRUPATI SARJAN LTD.' },
  { symbol: 'PANORAMUNI.BO', name: 'PANORAMIC UNIVERSAL LTD.' },
  { symbol: 'NUWAY.BO', name: 'NUWAY ORGANIC NATURALS INDIA LTD.' },
  { symbol: 'MUNOTHFI.BO', name: 'MUNOTH FINANCIAL SERVICES LTD.' },
  { symbol: 'RODIUM.BO', name: 'Rodium Realty Limited' },
  { symbol: 'RCCEMEN.BO', name: 'RCC CEMENTS LTD.' },
  { symbol: 'TODAYS.BO', name: 'Todays Writing Instruments Ltd' },
  { symbol: 'UNISH.BO', name: 'UNISYS SOFTWARES & HOLDING INDUSTRIES LTD.' },
  { symbol: 'NAGTECH.BO', name: 'NAGARJUNA AGRI TECH LTD.' },
  { symbol: 'IECEDU.BO', name: 'IEC EDUCATION LTD.' },
  { symbol: 'INDUSFIN.BO', name: 'INDUS FINANCE CORPORATION LTD.' },
  { symbol: 'ZENITHBIR.BO', name: 'ZENITH BIRLA (INDIA) LTD.-$' },
  { symbol: 'TRINITYLEA.BO', name: 'TRINITY LEAGUE INDIA LTD.' },
  { symbol: 'FUNWTRD.BO', name: 'FUNWORLD & TOURISM DEVELOPMENT LTD.' },
  { symbol: 'PRABHAVIN.BO', name: 'PRABHAV INDUSTRIES LTD.' },
  { symbol: 'ORVENPR.BO', name: 'ORIENTAL VENEER PRODUCTS LTD.' },
  { symbol: 'JOINDRE.BO', name: 'JOINDRE CAPITAL SERVICES LTD.' },
  { symbol: 'BHARATAGRI.BO', name: 'BHARAT AGRI FERT & REALTY LTD.' },
  { symbol: 'SIGRUN.BO', name: 'Sigrun Holdings Limited' },
  { symbol: 'UNITINT.BO', name: 'UNITECH INTERNATIONAL LTD.' },
  { symbol: 'SACHEMT.BO', name: 'SACHETA METALS LTD.' },
  { symbol: 'POPULARES.BO', name: 'POPULAR ESTATE MANAGEMENT LTD.' },
  { symbol: 'ANJANIFIN.BO', name: 'ANJANI FINANCE LTD.' },
  { symbol: 'PIONDIST.BO', name: 'PIONEER DISTILLERIES LTD.' },
  { symbol: 'GMETCOAL.BO', name: 'GUJARAT METALLIC COAL & COKE LTD.-$' },
  { symbol: 'KWALITY.BO', name: 'Kwality Limited' },
  { symbol: 'SVAINDIA.BO', name: 'SVA INDIA LTD.' },
  { symbol: 'RAHME.BO', name: 'RAHUL MERCHANDISING LTD.' },
  { symbol: 'REXNORD.BO', name: 'REXNORD ELECTRONICS & CONTROLS LTD.' },
  { symbol: 'INTECH.BO', name: 'INTEGRATED TECHNOLOGIES LTD.' },
  { symbol: 'SAWABUSI.BO', name: 'SAWACA BUSINESS MACHINES LTD.' },
  { symbol: 'SANGUI.BO', name: 'SANGUINE MEDIA LTD.' },
  { symbol: 'KARUNACAB.BO', name: 'GLOBUS CORPORATION LTD.-$' },
  { symbol: 'SWAGRUHA.BO', name: 'SWAGRUHA INFRASTRUCTURE LTD.' },
  { symbol: 'TOHELPH.BO', name: 'TOHEAL PHARMACHEM LTD.' },
  { symbol: 'GALAGEX.BO', name: 'GALAXY AGRICO EXPORTS LTD.' },
  { symbol: 'GOPAIST.BO', name: 'GOPAL IRON & STEELS CO.(GUJARAT) LTD.' },
  { symbol: 'TWINSTAR.BO', name: 'Twinstar Industries Limited' },
  { symbol: 'HINDAPL.BO', name: 'HINDUSTAN APPLIANCES LTD.' },
  { symbol: 'MUDITFN.BO', name: 'MUDIT FINLEASE LTD.' },
  { symbol: 'DHAMPURE.BO', name: 'DHAMPURE SPECIALITY SUGARS LTD.' },
  { symbol: 'SHAQUAK.BO', name: 'SHANTANU SHEOREY AQUAKULT LTD.' },
  { symbol: 'GOLCA.BO', name: 'GOLDEN CARPETS LTD.' },
  { symbol: 'INNOCORP.BO', name: 'INNOCORP LTD.' },
  { symbol: 'SARTHAKIND.BO', name: 'SARTHAK INDUSTRIES LTD.' },
  { symbol: 'SAICAPI.BO', name: 'SAI CAPITAL LTD.' },
  { symbol: 'SUPREMETEX.BO', name: 'SUPREME TEX MART LTD.' },
  { symbol: 'BECKONIN.BO', name: 'BECKONS INDUSTRIES LTD.' },
  { symbol: 'SERVOTEC.BO', name: 'SERVOTECH ENGINEERING INDUSTRIES LTD.' },
  { symbol: 'SUNITEE.BO', name: 'SUNITEE CHEMICALS LTD.' },
  { symbol: 'VERTEX.BO', name: 'VERTEX SECURITIES LTD.' },
  { symbol: 'RIBATEX.BO', name: 'RIBA TEXTILES LTD.' },
  { symbol: 'SHREMETAL.BO', name: 'SHREE METALLOYS LTD.' },
  { symbol: 'UNICRSE.BO', name: 'UNIVERSAL CREDIT & SECURITIES LTD.' },
  { symbol: 'IITLPROJ.BO', name: 'IITL PROJECTS LIMITED' },
  { symbol: 'HARIGOV.BO', name: 'HARI GOVIND INTERNATIONAL LTD.' },
  { symbol: 'TRIDETOOL.BO', name: 'TRIDENT TOOLS LTD.' },
  { symbol: 'CHLOGIST.BO', name: 'CHARTERED LOGISTICS LTD.' },
  { symbol: 'HINDALUMI.BO', name: 'HIND ALUMINIUM INDUSTRIES LTD.-$' },
  { symbol: 'SENINFO.BO', name: 'Senthil Infotek Ltd' },
  { symbol: 'SPECFOOD.BO', name: 'SPECTRUM FOODS LTD.' },
  { symbol: 'DECPO.BO', name: 'DECCAN POLYPACKS LTD.' },
  { symbol: 'AMRAAGRI.BO', name: 'AMRAWORLD AGRICO LTD.' },
  { symbol: 'ODYCORP.BO', name: 'ODYSSEY CORPORATION LTD.' },
  { symbol: 'INDCTST.BO', name: 'INDUCTO STEEL LTD.' },
  { symbol: 'SAMINDUS.BO', name: 'SAM INDUSTRIES LTD.' },
  { symbol: 'SHVFL.BO', name: 'SHREEVATSAA FINANCE & LEASING LTD.' },
  { symbol: 'POOJAENT.BO', name: 'POOJA ENTERTAINMENT AND FILMS LTD.' },
  { symbol: 'GRAVITY.BO', name: 'GRAVITY (INDIA) LTD.-$' },
  { symbol: 'WELLESLEY.BO', name: 'WELLESLEY CORPORATION LTD.' },
  { symbol: 'SENBO.BO', name: 'SENBO INDUSTRIES LTD.' },
  { symbol: 'RBGUPTA.BO', name: 'R.B.GUPTA FINANCIALS LTD.' },
  { symbol: 'SOWBHAGYA.BO', name: 'SOWBHAGYA MEDIA LTD.' },
  { symbol: 'JAINSTUDIO.BO', name: 'JAIN STUDIOS LTD.' },
  { symbol: 'SAFALHBS.BO', name: 'Safal Herbs Limited' },
  { symbol: 'UNISTRMU.BO', name: 'UNISTAR MULTIMEDIA LTD.' },
  { symbol: 'EMMSONS.BO', name: 'EMMSONS INTERNATIONAL LTD.-$' },
  { symbol: 'ZENOTECH.BO', name: 'ZENOTECH LABORATORIES LTD.' },
  { symbol: 'HINDBIO.BO', name: 'HINDUSTAN BIO SCIENCES LTD.' },
  { symbol: 'WALLFORT.BO', name: 'WALLFORT FINANCIAL SERVICES LTD.' },
  { symbol: 'ADIEXRE.BO', name: 'ADINATH EXIM RESOURCES LTD.' },
  { symbol: 'ABHICAP.BO', name: 'ABHINAV CAPITAL SERVICES LTD.' },
  { symbol: 'KILPEST.BO', name: 'KILPEST INDIA LTD.' },
  { symbol: 'SUPRBPA.BO', name: 'SUPERB PAPERS LTD.' },
  { symbol: 'INTERDIGI.BO', name: 'INTERWORLD DIGITAL LTD.-$' },
  { symbol: 'MONNETIN.BO', name: 'MONNET INDUSTRIES LTD.' },
  { symbol: 'KSERASERA.BO', name: 'KSS Limited-$' },
  { symbol: 'SHKALYN.BO', name: 'SHRI KALYAN HOLDINGS LTD.' },
  { symbol: 'VANDANA.BO', name: 'VANDANA KNITWEAR LTD.' },
  { symbol: 'SAGARPROD.BO', name: 'Sagar Productions Limited' },
  { symbol: 'MUKANDENGG.BO', name: 'MUKAND ENGINEERS LTD.' },
  { symbol: 'INDOCITY.BO', name: 'INDO-CITY INFOTECH LTD.-$' },
  { symbol: 'SBECSUG.BO', name: 'SBEC SUGAR LTD.' },
  { symbol: 'MEGFI.BO', name: 'MEGA FIN (INDIA) LTD.' },
  { symbol: 'REIAGROLTD.BO', name: 'REI AGRO LTD.' },
  { symbol: 'BRIJLEAS.BO', name: 'BRIJLAXMI LEASING & FINANCE LTD.' },
  { symbol: 'AREALTY.BO', name: 'ALCHEMIST REALTY LTD.' },
  { symbol: 'DENABANK.BO', name: 'DENA BANK' },
  { symbol: 'BSELINFRA.BO', name: 'BSEL INFRASTRUCTURE REALTY LTD.' },
  { symbol: 'RELIABVEN.BO', name: 'RELIABLE VENTURES INDIA LTD.' },
  { symbol: 'MOBILTEL.BO', name: 'MOBILE TELECOMMUNICATIONS LTD.' },
  { symbol: 'HEXAWARE.BO', name: 'HEXAWARE TECHNOLOGIES LTD.' },
  { symbol: 'IFGLREFRAC.BO', name: 'IFGL REFRACTORIES LTD.' },
  { symbol: 'GTEIT.BO', name: 'G-TECH INFO-TRAINING LTD.' },
  { symbol: 'MOHITE.BO', name: 'Mohite Industries Ltd' },
  { symbol: 'ANDHRACEMT.BO', name: 'ANDHRA CEMENTS LTD.' },
  { symbol: 'HOTLSILV.BO', name: 'H.S.INDIA LTD.' },
  { symbol: 'SUNSHINE.BO', name: 'SUN AND SHINE WORLDWIDE LTD.' },
  { symbol: 'GUJSTATFIN.BO', name: 'GUJARAT STATE FINANCIAL CORPORATION LTD.' },
  { symbol: 'MINOLTAF.BO', name: 'MINOLTA FINANCE LTD.' },
  { symbol: 'ALKASEC.BO', name: 'ALKA SECURITIES LTD.' },
  { symbol: 'OMKARPH.BO', name: 'OMKAR PHARMACHEM LTD.' },
  { symbol: 'SPHEREGSL.BO', name: 'Sphere Global Services Ltd.' },
  { symbol: 'CORPBANK.BO', name: 'CORPORATION BANK' },
  { symbol: 'GAYATRI.BO', name: 'GAYATRI SUGARS LTD.' },
  { symbol: 'SBT.BO', name: 'STATE BANK OF TRAVANCORE' },
  { symbol: 'MYSOREBANK.BO', name: 'STATE BANK OF MYSORE' },
  { symbol: 'J&AMP;KBANK.BO', name: 'JAMMU & KASHMIR BANK LTD.' },
  { symbol: 'HBSTOCK.BO', name: 'HB STOCKHOLDINGS LTD.' },
  { symbol: 'SIELFNS.BO', name: 'SIEL FINANCIAL SERVICES LTD.' },
  { symbol: 'BENGALT.BO', name: 'BENGAL TEA & FABRICS LTD.' },
  { symbol: 'POLARIS.BO', name: 'Polaris Consulting & Services Limited' },
  { symbol: 'TCIIND.BO', name: 'TCI INDUSTRIES LTD.' },
  { symbol: 'CYBERMAT.BO', name: 'CYBERMATE INFOTEK LTD.' },
  { symbol: 'LANDMARC.BO', name: 'LANDMARC LEISURE CORPORATION LTD.' },
  { symbol: 'SYNDIBANK.BO', name: 'SYNDICATE BANK' },
  { symbol: 'CASTEXTECH.BO', name: 'Castex Technologies Ltd-$' },
  { symbol: 'TCFCFINQ.BO', name: 'TCFC FINANCE LTD.' },
  { symbol: 'GEOJITBNPP.BO', name: 'GEOJIT BNP PARIBAS FINANCIAL SERVICES LTD.' },
  { symbol: 'ENTEGRA.BO', name: 'ENTEGRA LTD.' },
  { symbol: 'SOFTTECHGR.BO', name: 'SOFTWARE TECHNOLOGY GROUP INTERNATIONAL LTD.' },
  { symbol: 'TATACOFFEE.BO', name: 'TATA COFFEE LTD.' },
  { symbol: 'KJMCCORP.BO', name: 'KJMC Corporate Advisors (India) Ltd.' },
  { symbol: 'MELSTAR.BO', name: 'MELSTAR INFORMATION TECHNOLOGIES LTD.' },
  { symbol: 'AIL.BO', name: 'ALSTOM India Ltd' },
  { symbol: 'GEOMETRIC.BO', name: 'GEOMETRIC LTD.' },
  { symbol: 'SKUMAR.BO', name: 'S KUMARS.COM LTD.' },
  { symbol: 'GEMINI.BO', name: 'GEMINI COMMUNICATION LTD.' },
  { symbol: 'VAARAD.BO', name: 'Vaarad Ventures Ltd' },
  { symbol: 'CADILAHC.BO', name: 'CADILA HEALTHCARE LTD.' },
  { symbol: 'ELDERPHARM.BO', name: 'ELDER PHARMACEUTICALS LTD.' },
  { symbol: 'SHIVACEM.BO', name: 'SHIVA CEMENT LTD.' },
  { symbol: 'DANLAW.BO', name: 'DANLAW TECHNOLOGIES INDIA LTD.-$' },
  { symbol: 'BIOPAC.BO', name: 'BIOPAC INDIA CORPORATION LTD.-$' },
  { symbol: 'CURATECH.BO', name: 'CURA TECHNOLOGIES LTD.' },
  { symbol: 'HBPOR.BO', name: 'HB PORTFOLIO LTD.' },
  { symbol: 'BARONINF.BO', name: 'BARON INFOTECH LTD.' },
  { symbol: 'OMNIAX.BO', name: 'OMNI AXS SOFTWARE LTD.' },
  { symbol: 'COMMEXTECH.BO', name: 'Commex Technology Limited' },
  { symbol: 'SOFTSOL.BO', name: 'SOFTSOL INDIA LTD.' },
  { symbol: 'GATI.BO', name: 'GATI LTD.' },
  { symbol: 'BLUESTINFO.BO', name: 'BLUE STAR INFOTECH LTD.' },
  { symbol: 'SUBEX.BO', name: 'SUBEX LTD.' },
  { symbol: 'PADMALAYAT.BO', name: 'PADMALAYA TELEFILMS LTD.' },
  { symbol: 'VIRGOGLOB.BO', name: 'Virgo Global Media Limited' },
  { symbol: 'PICTUREHS.BO', name: 'PICTUREHOUSE MEDIA LTD.' },
  { symbol: 'HITKITGLO.BO', name: 'HIT KIT GLOBAL SOLUTIONS LTD.' },
  { symbol: 'NAGPI.BO', name: 'NAGPUR POWER & INDUSTRIES LTD.' },
  { symbol: 'CTIL.BO', name: 'CTIL LTD.' },
  { symbol: 'CYBERSC.BO', name: 'CYBERSCAPE MULTIMEDIA LTD.' },
  { symbol: 'KANIKAIN.BO', name: 'KANIKA INFOTECH LTD.' },
  { symbol: 'LYCOS.BO', name: 'Lycos Internet Limited' },
  { symbol: 'VIRNICHIQ.BO', name: 'VIRINCHI TECHNOLOGIES LTD.' },
  { symbol: 'WEPSOLN.BO', name: 'WEP SOLUTIONS LTD.' },
  { symbol: 'STRTECH.BO', name: 'STERLITE TECHNOLOGIES LTD.' },
  { symbol: 'TIPSINDLTD*.BO', name: 'TIPS INDUSTRIES LTD.' },
  { symbol: 'MRO-TEK.BO', name: 'MRO-TEK LTD.' },
  { symbol: 'UNIVARTS.BO', name: 'UNIVERSAL ARTS LTD.' },
  { symbol: 'FIRSTOBJ.BO', name: 'FIRSTOBJECT TECHNOLOGIES LTD.' },
  { symbol: 'BABA.BO', name: 'BABA ARTS LTD.-$' },
  { symbol: 'TYCHE.BO', name: 'TYCHE INDUSTRIES LTD.' },
  { symbol: 'VALECHAENG.BO', name: 'VALECHA ENGINEERING LTD.-$' },
  { symbol: 'OPTOCIRCUI.BO', name: 'OPTO CIRCUITS (INDIA) LTD.' },
  { symbol: 'ARMSPAPER.BO', name: 'ARMS PAPER LTD.' },
  { symbol: 'KPIT.BO', name: 'KPIT Technologies Limited' },
  { symbol: 'VIJAYABANK.BO', name: 'VIJAYA BANK' },
  { symbol: 'USGTECH.BO', name: 'USG TECH SOLUTIONS LTD.' },
  { symbol: '4THGEN.BO', name: 'FOURTH GENERATION INFORMATION SYSTEMS LTD.' },
  { symbol: '7TEC.BO', name: 'SAVEN TECHNOLOGIES LTD.' },
  { symbol: 'MEGASOFT.BO', name: 'MEGASOFT LTD.' },
  { symbol: 'TRANSCOR.BO', name: 'TRANSCORP INTERNATIONAL LTD.' },
  { symbol: 'VISESHINFO.BO', name: 'VISESH INFOTECNICS LTD.' },
  { symbol: 'IKFTECH.BO', name: 'IKF TECHNOLOGIES LTD.' },
  { symbol: 'ANDHRABANK.BO', name: 'ANDHRA BANK' },
  { symbol: 'GULCHEM.BO', name: 'GULSHAN CHEMFILL LTD.' },
  { symbol: 'SANINFRA.BO', name: 'Sanmit Infra Limited' },
  { symbol: 'GOLDINFRA.BO', name: 'GOLDSTONE INFRATECH LTD.' },
  { symbol: 'RAINBOWDQ.BO', name: 'RAINBOW DENIM LTD.' },
  { symbol: 'TSPIRITUAL.BO', name: 'T.SPIRITUAL WORLD LTD.' },
  { symbol: 'SHALIWIR.BO', name: 'SHALIMAR WIRES INDUSTRIES LTD.' },
  { symbol: 'COMPUAGE.BO', name: 'COMPUAGE INFOCOM LTD.' },
  { symbol: 'AUNDEIND.BO', name: 'Aunde India Limited-$' },
  { symbol: 'HAZOOR.BO', name: 'HAZOOR MULTI PROJECTS LTD.-$' },
  { symbol: 'MPFSL.BO', name: 'MATHER & PLATT FIRE SYSTEMS LTD.' },
  { symbol: 'ISMTLTD.BO', name: 'ISMT LTD.' },
  { symbol: 'ALBK.BO', name: 'ALLAHABAD BANK' },
  { symbol: 'SHRIRAMCIT.BO', name: 'SHRIRAM CITY UNION FINANCE LTD.' },
  { symbol: 'EXCELCROP.BO', name: 'EXCEL CROP CARE LTD.' },
  { symbol: 'FLORENCE.BO', name: 'Florence Investech Limited-$' },
  { symbol: 'PALRED.BO', name: 'Palred Technologies Limited' },
  { symbol: 'DISHMAN.BO', name: 'DISHMAN PHARMACEUTICALS & CHEMICALS LTD.' },
  { symbol: 'JPASSOCIAT.BO', name: 'JAIPRAKASH ASSOCIATES LTD.' },
  { symbol: 'LUMAXAUTO.BO', name: 'LUMAX AUTOMOTIVE SYSTEMS LTD.' },
  { symbol: 'MINDAIND.BO', name: 'MINDA INDUSTRIES LTD.-$' },
  { symbol: 'NIITTECH.BO', name: 'NIIT TECHNOLOGIES LTD.' },
  { symbol: 'BHARTISHIP.BO', name: 'BHARATI SHIPYARD LTD.' },
  { symbol: 'MAXWELL.BO', name: 'MAXWELL INDUSTRIES LTD.' },
  { symbol: 'JETAIRWAYS.BO', name: 'JET AIRWAYS (INDIA) LTD.' },
  { symbol: 'GDL.BO', name: 'GATEWAY DISTRIPARKS LTD.' },
  { symbol: 'NRINTER.BO', name: 'N.R.INTERNATIONAL LTD.' },
  { symbol: 'PONDYOXIDE.BO', name: 'PONDY OXIDES & CHEMICALS LTD.' },
  { symbol: '3IINFOTECH.BO', name: '3I INFOTECH LTD.' },
  { symbol: 'MBECL.BO', name: 'MCNALLY BHARAT ENGINEERING COMPANY LTD.' },
  { symbol: 'ALLSEC.BO', name: 'ALLSEC TECHNOLOGIES LTD.' },
  { symbol: 'SGFL.BO', name: 'SHREE GANESH FORGINGS LTD.' },
  { symbol: 'BEEYU.BO', name: 'BEEYU OVERSEAS LTD.' },
  { symbol: 'UNIPLY.BO', name: 'UNIPLY INDUSTRIES LTD.' },
  { symbol: 'PROVOGE.BO', name: 'PROVOGUE (INDIA) LTD.' },
  { symbol: 'FACORALL.BO', name: 'FACOR ALLOYS LTD.' },
  { symbol: 'FACORSTE.BO', name: 'FACOR STEELS LTD.' },
  { symbol: 'EON.BO', name: 'EON ELECTRIC LTD.' },
  { symbol: 'IDFC.BO', name: 'IDFC LIMITED' },
  { symbol: 'RAJVIR.BO', name: 'RAJVIR INDUSTRIES LTD.' },
  { symbol: 'SBTL.BO', name: 'SOUTHERN ONLINE BIO TECHNOLOGIES LTD.' },
  { symbol: 'PBAINFRA.BO', name: 'PBA INFRASTRUCTURE LTD.' },
  { symbol: 'BRFL.BO', name: 'BOMBAY RAYON FASHIONS LTD.' },
  { symbol: 'STOREONE.BO', name: 'Store One Retail India Limited' },
  { symbol: 'ABGSHIP.BO', name: 'ABG SHIPYARD LTD.' },
  { symbol: 'PVR.BO', name: 'PVR LTD.' },
  { symbol: 'RAMSARUP.BO', name: 'RAMSARUP INDUSTRIES LTD.' },
  { symbol: 'RMCL.BO', name: 'RADHA MADHAV CORPORATION LTD.' },
  { symbol: 'PUNJLLOYD.BO', name: 'PUNJ LLOYD LTD.' },
  { symbol: 'BARTRONICS.BO', name: 'BARTRONICS INDIA LTD.' },
  { symbol: 'EDUCOMP.BO', name: 'EDUCOMP SOLUTIONS LTD.' },
  { symbol: 'SREESAKHTI.BO', name: 'SREE SAKTHI PAPER MILLS LTD.' },
  { symbol: 'GSPL.BO', name: 'GUJARAT STATE PETRONET LTD.' },
  { symbol: 'INOXLEISUR.BO', name: 'INOX LEISURE LTD.' },
  { symbol: 'SUNILHITEC.BO', name: 'SUNIL HITECH ENGINEERS LTD.' },
  { symbol: 'GITANJALI.BO', name: 'GITANJALI GEMS LTD.' },
  { symbol: 'PRATIBHA.BO', name: 'PRATIBHA INDUSTRIES LTD.' },
  { symbol: 'M&AMP;MFIN.BO', name: 'MAHINDRA & MAHINDRA FINANCIAL SERVICES LTD.' },
  { symbol: 'VISASTEEL.BO', name: 'VISA STEEL LTD.' },
  { symbol: 'MONNETPRO.BO', name: 'MONNET PROJECT DEVELOPERS LTD.' },
  { symbol: 'ADHUNIK.BO', name: 'ADHUNIK METALIKS LTD.' },
  { symbol: 'ROHITFERRO.BO', name: 'ROHIT FERRO-TECH LTD.' },
  { symbol: 'RSYSTEMINT.BO', name: 'R Systems International Limited' },
  { symbol: 'TANTIACONS.BO', name: 'TANTIA CONSTRUCTIONS LTD.' },
  { symbol: 'KIL.BO', name: 'KAMDHENU ISPAT LTD.' },
  { symbol: 'GTNTEX.BO', name: 'GTN TEXTILES LTD.' },
  { symbol: 'INDICAP.BO', name: 'Inditrade Capital Limited' },
  { symbol: 'UNITY.BO', name: 'UNITY INFRAPROJECTS LTD.' },
  { symbol: 'EASUNREYRL.BO', name: 'EASUN REYROLLE LTD.' },
  { symbol: 'GMRINFRA.BO', name: 'GMR INFRASTRUCTURE LTD.' },
  { symbol: 'MAHINDCIE.BO', name: 'Mahindra CIE Automotive Limited' },
  { symbol: 'ATLANTA.BO', name: 'ATLANTA LTD.' },
  { symbol: 'DEEPIND.BO', name: 'DEEP INDUSTRIES LTD.' },
  { symbol: 'HOVS.BO', name: 'HOV SERVICES LTD.' },
  { symbol: 'USHERAGRO.BO', name: 'USHER AGRO LTD.' },
  { symbol: 'RICHAIND.BO', name: 'RICHA INDUSTRIES LTD.' },
  { symbol: 'HANUNG.BO', name: 'HANUNG TOYS & TEXTILES LTD.' },
  { symbol: 'AFL.BO', name: 'ACCEL FRONTLINE LTD.' },
  { symbol: 'LITL.BO', name: 'LANCO INFRATECH LTD.' },
  { symbol: 'DAAWAT.BO', name: 'LT FOODS LTD.' },
  { symbol: 'GTOFFSHORE.BO', name: 'GOL OFFSHORE LTD.' },
  { symbol: 'ESSDEE.BO', name: 'ESS DEE ALUMINIUM LTD.' },
  { symbol: 'XLENERGY.BO', name: 'XL ENERGY LTD.' },
  { symbol: 'CAIRN.BO', name: 'CAIRN INDIA LTD.' },
  { symbol: 'SITICABLE.BO', name: 'SITI CABLE NETWORK LTD.' },
  { symbol: 'TV18BRDCST.BO', name: 'TV18 BROADCAST LTD.' },
  { symbol: 'POCHIRAJU.BO', name: 'POCHIRAJU INDUSTRIES LTD.' },
  { symbol: 'AICHAMP.BO', name: 'AI CHAMPDANY INDUSTRIES LTD.' },
  { symbol: 'CANDC.BO', name: 'C & C CONSTRUCTIONS LTD.' },
  { symbol: 'BROADCAST.BO', name: 'BROADCAST INITIATIVES LTD.' },
  { symbol: 'MINDTREE.BO', name: 'MINDTREE LTD.' },
  { symbol: 'ELAND.BO', name: 'E-Land Apparel Limited' },
  { symbol: 'EUROCERA.BO', name: 'EURO CERAMICS LTD.' },
  { symbol: 'JAGJANANI.BO', name: 'JAGJANANI TEXTILES LTD.' },
  { symbol: 'LEHAR.BO', name: 'LAWRESHWAR POLYMERS LTD.' },
  { symbol: 'ABHISHEK.BO', name: 'ABHISHEK CORPORATION LTD.' },
  { symbol: 'IBREALEST.BO', name: 'INDIABULLS REAL ESTATE LTD.' },
  { symbol: 'CAMLINFIN.BO', name: 'CAMLIN FINE SCIENCES LTD.' },
  { symbol: 'SANCIA.BO', name: 'Sancia Global Infraprojects Limited' },
  { symbol: 'ORBITCORP.BO', name: 'ORBIT CORPORATION LTD.' },
  { symbol: 'ADVANTA.BO', name: 'Advanta Limited' },
  { symbol: 'MIC.BO', name: 'MIC ELECTRONICS LTD.' },
  { symbol: 'MCDHOLDING.BO', name: 'MCDOWELL HOLDINGS LTD.' },
  { symbol: 'NITINFIRE.BO', name: 'NITIN FIRE PROTECTION INDUSTRIES LTD.' },
  { symbol: 'HARYNACAP.BO', name: 'HARYANA CAPFIN LTD.' },
  { symbol: 'MEGH.BO', name: 'MEGHMANI ORGANICS LTD.' },
  { symbol: 'QUINTEGRA.BO', name: 'QUINTEGRA SOLUTIONS LTD.' },
  { symbol: 'CELESTIAL.BO', name: 'Celestial Biolabs Limited' },
  { symbol: 'SURYACHAKRA.BO', name: 'SURYACHAKRA POWER CORPORATION LTD.' },
  { symbol: 'EVERONN.BO', name: 'Everonn Education ltd' },
  { symbol: 'SIMPLEX.BO', name: 'SIMPLEX PROJECTS LTD.' },
  { symbol: 'SSLEL.BO', name: 'SIR SHADI LAL ENTERPRISES LTD.' },
  { symbol: 'OMNITECH.BO', name: 'OMNITECH INFOSOLUTIONS LTD.' },
  { symbol: 'ZYLOG.BO', name: 'ZYLOG SYSTEMS LTD.' },
  { symbol: 'SELMCL.BO', name: 'SEL MANUFACTURING COMPANY LTD.' },
  { symbol: 'SUJANATWR.BO', name: 'SUJANA TOWERS LTD.' },
  { symbol: 'VTMLTD.BO', name: 'VTM LTD.' },
  { symbol: 'SEINV.BO', name: 'S.E.INVESTMENTS LTD.' },
  { symbol: 'SAAMYABIO.BO', name: 'SAAMYA BIOTECH (INDIA) LTD.' },
  { symbol: 'IL&AMP;FSENGG.BO', name: 'IL&FS ENGINEERING AND CONSTRUCTION COMPANY LTD.' },
  { symbol: 'SHARONBIO.BO', name: 'SHARON BIO-MEDICINE LTD.-$' },
  { symbol: 'ANILLTD.BO', name: 'ANIL LTD.' },
  { symbol: 'PARLESOFT.BO', name: 'PARLE SOFTWARE LTD.' },
  { symbol: 'NET4.BO', name: 'NET 4 INDIA LTD.' },
  { symbol: 'CSIL.BO', name: 'CIRCUIT SYSTEMS (INDIA) LTD.' },
  { symbol: 'ARCOTECH.BO', name: 'ARCOTECH LTD.' },
  { symbol: 'RATHIBAR.BO', name: 'RATHI BARS LTD.' },
  { symbol: 'ACIASIA.BO', name: 'ALLIED COMPUTERS INTERNATIONAL (ASIA) LTD.' },
  { symbol: 'EDL.BO', name: 'EMPEE DISTILLERIES LTD.' },
  { symbol: 'RJL.BO', name: 'RENAISSANCE JEWELLERY LTD.' },
  { symbol: 'TRIL.BO', name: 'TRANSFORMERS AND RECTIFIERS (INDIA) LTD.' },
  { symbol: 'BURNPUR.BO', name: 'BURNPUR CEMENT LTD.' },
  { symbol: 'PORWAL.BO', name: 'PORWAL AUTO COMPONENTS LTD.' },
  { symbol: 'CAPF.BO', name: 'CAPITAL FIRST LTD.' },
  { symbol: 'SHRIRAMEPC.BO', name: 'SHRIRAM EPC LTD.' },
  { symbol: 'TULSI.BO', name: 'TULSI EXTRUSIONS LTD.' },
  { symbol: 'GOKAKTEX.BO', name: 'GOKAK TEXTILES LTD.' },
  { symbol: 'GAMMNINFRA.BO', name: 'GAMMON INFRASTRUCTURE PROJECTS LTD.' },
  { symbol: 'IBVENTURES.BO', name: 'Indiabulls Ventures Limited' },
  { symbol: 'SITASHREE.BO', name: 'SITA SHREE FOOD PRODUCTS LTD.' },
  { symbol: 'TWL.BO', name: 'TITAGARH WAGONS LTD.' },
  { symbol: 'SANKHYAIN.BO', name: 'SANKHYA INFOTECH LTD.' },
  { symbol: 'AISHWARYA.BO', name: 'AISHWARYA TELECOM LTD.' },
  { symbol: 'PIRPHYTO.BO', name: 'Piramal Phytocare Limited' },
  { symbol: 'KTKSENSEX.BO', name: 'KOTAK MAHINDRA MUTUAL FUND' },
  { symbol: 'RBL.BO', name: 'RANE BRAKE LINING LTD.' },
  { symbol: 'RANEENGINE.BO', name: 'RANE ENGINE VALVE LTD.' },
  { symbol: 'BAFNAPHARM.BO', name: 'BAFNA PHARMACEUTICALS LTD.' },
  { symbol: 'METKORE.BO', name: 'METKORE ALLOYS & INDUSTRIES LTD.' },
  { symbol: 'MVL.BO', name: 'MVL LTD.' },
  { symbol: 'CHLLTD.BO', name: 'CHL LTD.' },
  { symbol: 'SEZAL.BO', name: 'Sezal Glass Limited' },
  { symbol: 'FIRSTWIN.BO', name: 'FIRST WINNER INDUSTRIES LTD.' },
  { symbol: 'KSK.BO', name: 'KSK ENERGY VENTURES LTD.' },
  { symbol: 'SOMICONV.BO', name: 'SOMI CONVEYOR BELTINGS LTD.' },
  { symbol: 'BIRLACOT.BO', name: 'BIRLA COTSYN (INDIA) LTD.' },
  { symbol: 'LGBFORGE.BO', name: 'LGB FORGE LTD.' },
  { symbol: 'OISL.BO', name: 'OCL IRON AND STEEL LTD.' },
  { symbol: 'NUTEK.BO', name: 'NU TEK INDIA LTD.' },
  { symbol: 'RMMIL.BO', name: 'RESURGERE MINES & MINERALS INDIA LTD.' },
  { symbol: 'SIMPLXMIL.BO', name: 'SIMPLEX MILLS COMPANY LTD.' },
  { symbol: 'SIMPLXPAP.BO', name: 'SIMPLEX PAPERS LTD.' },
  { symbol: 'WABCOINDIA.BO', name: 'WABCO INDIA LTD.' },
  { symbol: 'SELANBBPH.BO', name: 'SELANBBPH' },
  { symbol: 'GISOLUTION.BO', name: 'GI ENGINEERING SOLUTIONS LTD.' },
  { symbol: 'VEDAVAAG.BO', name: 'VEDAVAAG SYSTEMS LTD.' },
  { symbol: 'REISIXTEN.BO', name: 'REI SIX TEN RETAIL LTD.' },
  { symbol: 'ARROWTEX.BO', name: 'ARROW TEXTILES LTD.' },
  { symbol: 'MANJEERA.BO', name: 'MANJEERA CONSTRUCTIONS LTD.' },
  { symbol: 'RDEVCAB.BO', name: 'RISHABHDEV TECHNOCABLE LTD.' },
  { symbol: 'EXCEL.BO', name: 'Excel Realty N Infra Ltd' },
  { symbol: 'RAJOIL.BO', name: 'RAJ OIL MILLS LTD.' },
  { symbol: 'PEIL.BO', name: 'PREMIER ENERGY AND INFRASTRUCTURE LTD.' },
  { symbol: 'SURYAAMBA.BO', name: 'SURYAAMBA SPINNING MILLS LTD.' },
  { symbol: 'JINDCOT.BO', name: 'JINDAL COTEX LTD.' },
  { symbol: 'PIPAVAVDOC.BO', name: 'PIPAVAV DEFENCE AND OFFSHORE ENG LTD.' },
  { symbol: 'EUROMULTI.BO', name: 'EURO MULTIVISION LTD.' },
  { symbol: 'SPSL.BO', name: 'SHREE PRECOATED STEELS LTD.' },
  { symbol: 'SQSBFSI.BO', name: 'SQS India BFSI Limited' },
  { symbol: 'COX&AMP;KINGS.BO', name: 'Cox & Kings Limited' },
  { symbol: 'ESSARSEC.BO', name: 'Essar Securities Ltd' },
  { symbol: 'INFINITE.BO', name: 'Infinite Computer Solutions (India) Ltd' },
  { symbol: 'SYNCOM.BO', name: 'Syncom Healthcare Ltd' },
  { symbol: 'ARSSINFRA.BO', name: 'ARSS INFRASTRUCTURE PROJECTS LTD.' },
  { symbol: 'TIPSBBPH.BO', name: 'TIPSBBPH' },
  { symbol: 'COROENGG.BO', name: 'COROMANDEL ENGINEERING COMPANY LTD.' },
  { symbol: 'TAMBOLI.BO', name: 'TAMBOLI CAPITAL LTD.' },
  { symbol: 'UNITEDBNK.BO', name: 'UNITED BANK OF INDIA' },
  { symbol: 'DQE.BO', name: 'DQ ENTERTAINMENT (INTERNATIONAL) LTD.' },
  { symbol: 'IL&AMP;FSTRANS.BO', name: 'IL&FS Transportation Networks Ltd' },
  { symbol: 'PRADIP.BO', name: 'Pradip Overseas Ltd' },
  { symbol: 'SGJHL.BO', name: 'SHREE GANESH JEWELLERY HOUSE (I) LTD.' },
  { symbol: 'GOENKA.BO', name: 'GOENKA DIAMOND & JEWELS LTD.' },
  { symbol: 'TALWALKARS.BO', name: 'Talwalkars Better Value Fitness Ltd' },
  { symbol: 'NITESHEST.BO', name: 'Nitesh Estates Ltd' },
  { symbol: 'MANDHANA.BO', name: 'Mandhana Industries Ltd' },
  { symbol: 'JPINFRATEC.BO', name: 'Jaypee Infratech Ltd' },
  { symbol: 'PARABDRUGS.BO', name: 'Parabolic Drugs Ltd' },
  { symbol: 'GKB.BO', name: 'GKB OPHTHALMICS LTD.' },
  { symbol: 'FRONTSEC.BO', name: 'FRONTLINE SECURITIES LTD.' },
  { symbol: 'TECHNOFAB.BO', name: 'TECHNOFAB ENGINEERING LIMITED' },
  { symbol: 'EMAMIINFRA.BO', name: 'EMAMI INFRASTRUCTURE LTD.' },
  { symbol: 'SHRIASTER.BO', name: 'Shri Aster Silicates Limited' },
  { symbol: 'SKSMICRO.BO', name: 'SKS MICROFINANCE LTD.' },
  { symbol: 'BAJAJCORP.BO', name: 'BAJAJ CORP LTD.' },
  { symbol: 'INDOSOLAR.BO', name: 'INDOSOLAR LTD.' },
  { symbol: 'TIRUPATIINK.BO', name: 'TIRUPATI INKS LTD.' },
  { symbol: 'MICROSEC.BO', name: 'MICROSEC FINANCIAL SERVICES LTD.' },
  { symbol: 'CAREERP.BO', name: 'CAREER POINT LTD.' },
  { symbol: 'EROSMEDIA.BO', name: 'EROS INTERNATIONAL MEDIA LTD.' },
  { symbol: 'ESL.BO', name: 'ELECTROSTEEL STEELS LTD.' },
  { symbol: 'GALLISPAT.BO', name: 'GALLANTT ISPAT LTD.' },
  { symbol: 'SEATV.BO', name: 'SEA TV NETWORK LTD.' },
  { symbol: 'CEBBCO.BO', name: 'COMMERCIAL ENGINEERS & BODY BUILDERS CO. LTD.' },
  { symbol: 'GAL.BO', name: 'GYSCOAL ALLOYS LTD.' },
  { symbol: 'BSLIMITED.BO', name: 'BS LTD.' },
  { symbol: 'TECHNO.BO', name: 'TECHNO ELECTRIC AND ENGINEERING CO. LTD.' },
  { symbol: 'RDBRIL.BO', name: 'RDB REALTY & INFRASTRUCTURE LTD.' },
  { symbol: 'CLARIS.BO', name: 'CLARIS LIFESCIENCES LTD.' },
  { symbol: 'KTIL.BO', name: 'KESAR TERMINALS & INFRASTRUCTURE LTD.' },
  { symbol: 'SPYL.BO', name: 'SHEKHAWATI POLY-YARN LTD.' },
  { symbol: 'SRSREAL.BO', name: 'SRS REAL INFRASTRUCTURE LTD.' },
  { symbol: 'DALMIABHA.BO', name: 'DALMIA BHARAT LTD.' },
  { symbol: 'INOVSYNTH.BO', name: 'INNOVASSYNTH INVESTMENTS LTD.' },
  { symbol: 'OMKARCHEM.BO', name: 'OMKAR SPECIALITY CHEMICALS LTD.' },
  { symbol: 'JUBLINDS.BO', name: 'JUBILANT INDUSTRIES LTD.' },
  { symbol: 'BILENERGY.BO', name: 'BIL ENERGY SYSTEMS LTD.' },
  { symbol: 'ACROPETAL.BO', name: 'ACROPETAL TECHNOLOGIES LTD.' },
  { symbol: 'SUDAR.BO', name: 'SUDAR INDUSTRIES LTD.' },
  { symbol: 'SHILPI.BO', name: 'SHILPI CABLE TECHNOLOGIES LTD.' },
  { symbol: 'TCIDEVELOP.BO', name: 'TCI DEVELOPERS LTD.' },
  { symbol: 'PARAPRINT.BO', name: 'PARAMOUNT PRINTPACKAGING LTD.' },
  { symbol: 'FCEL.BO', name: 'Future Consumer Enterprise Limited' },
  { symbol: 'SERVALL.BO', name: 'SERVALAKSHMI PAPER LTD.' },
  { symbol: 'CNOVAPETRO.BO', name: 'CIL NOVA PETROCHEMICALS LTD.' },
  { symbol: 'SANGHVIFOR.BO', name: 'SANGHVI FORGING AND ENGINEERING LTD.' },
  { symbol: 'VMS.BO', name: 'VMS INDUSTRIES LTD.' },
  { symbol: 'WEIZFOREX.BO', name: 'WEIZMANN FOREX LTD.' },
  { symbol: 'ENKEIWHEL.BO', name: 'ENKEI WHEELS (INDIA) LTD.' },
  { symbol: 'KRIINFRA.BO', name: 'Kridhan Infra Limited' },
  { symbol: 'BGLOBAL.BO', name: 'BHARATIYA GLOBAL INFOMEDIA LTD.' },
  { symbol: 'L&AMP;TFH.BO', name: 'L&T FINANCE HOLDINGS LTD.' },
  { symbol: 'IBWSL.BO', name: 'INDIABULLS WHOLESALE SERVICES LTD.' },
  { symbol: 'HEALTHTECH.BO', name: 'HEALTHFORE TECHNOLOGIES LTD.' },
  { symbol: 'SRSLTD.BO', name: 'SRS LTD.' },
  { symbol: 'LESHAIND.BO', name: 'LESHA INDUSTRIES LTD.' },
  { symbol: 'PRAKASHCON.BO', name: 'PRAKASH CONSTROWELL LTD.' },
  { symbol: 'RDBRL.BO', name: 'RDB RASAYANS LTD.' },
  { symbol: 'UJAAS.BO', name: 'Ujaas Energy Limited' },
  { symbol: 'KGNENT.BO', name: 'KGN ENTERPRISES LTD.' },
  { symbol: 'RELBANK.BO', name: 'R* Shares Banking Exchange Traded Fund' },
  { symbol: 'FERVENTSYN.BO', name: 'FERVENT SYNERGIES LTD.' },
  { symbol: 'PMCFIN.BO', name: 'PMC Fincorp Limited' },
  { symbol: 'FUTSOL.BO', name: 'FUTURISTIC SOLUTIONS LTD.' },
  { symbol: 'AIML.BO', name: 'ALLIANCE INTEGRATED METALIKS LTD.' },
  { symbol: 'ORIENTREF.BO', name: 'ORIENT REFRACTORIES LTD.' },
  { symbol: 'BCBFL.BO', name: 'BCB FINANCE LTD.' },
  { symbol: 'NAGAROIL.BO', name: 'NAGARJUNA OIL REFINERY LTD.' },
  { symbol: 'OLPCL.BO', name: 'OLYMPIC CARDS LTD.' },
  { symbol: 'MAXHEIGHTS.BO', name: 'MAXHEIGHTS INFRASTRUCTURE LTD.' },
  { symbol: 'LOOKS.BO', name: 'Looks Health Services Limited' },
  { symbol: 'BGPL.BO', name: 'Bio Green Papers Ltd' },
  { symbol: 'MASL.BO', name: 'MAX ALERT SYSTEMS LTD.' },
  { symbol: 'ASEEMG.BO', name: 'ASEEM GLOBAL LTD.' },
  { symbol: 'VKSPL.BO', name: 'VKS PROJECTS LTD.' },
  { symbol: 'RTNINFRA.BO', name: 'RattanIndia Infrastructure Limited' },
  { symbol: 'SEPOWER.BO', name: 'S.E. POWER LTD.' },
  { symbol: 'JTLINFRA.BO', name: 'JTL INFRA LTD.' },
  { symbol: 'AMTL.BO', name: 'ADVANCE METERING TECHNOLOGY LTD.' },
  { symbol: 'SAL.BO', name: 'SANGAM ADVISORS LTD.' },
  { symbol: 'JUPITERIN.BO', name: 'JUPITER INFOMEDIA LTD.' },
  { symbol: 'VINAYAKPOL.BO', name: 'VINAYAK POLYCON INTERNATIONAL LTD.' },
  { symbol: 'JOINTECAED.BO', name: 'JOINTECA EDUCATION SOLUTIONS LTD.' },
  { symbol: 'DYNATECH.BO', name: 'DYNACONS TECHNOLOGIES LTD.' },
  { symbol: 'PROZONINTU.BO', name: 'Prozone Intu Properties Limited' },
  { symbol: 'LAKSHVILAS.BO', name: 'LAKSHMI VILAS BANK LTD.' },
  { symbol: 'COMCL.BO', name: 'COMFORT COMMOTRADE LTD.' },
  { symbol: 'ANSHUS.BO', name: 'ANSHUS CLOTHING LTD.' },
  { symbol: 'RCRL.BO', name: 'RCL RETAIL LTD.' },
  { symbol: 'BITL.BO', name: 'BRONZE INFRA-TECH LTD.' },
  { symbol: 'INTELLADV.BO', name: 'INTELLIVATE CAPITAL ADVISORS LTD.' },
  { symbol: 'ICVLSTEELS.BO', name: 'ICVL STEELS LTD.' },
  { symbol: 'RMCHEM.BO', name: 'Ram Minerals and Chemicals Ltd' },
  { symbol: 'VIRTUALG.BO', name: 'VIRTUAL GLOBAL EDUCATION LTD.' },
  { symbol: 'TRIOMERC.BO', name: 'TRIO MERCANTILE & TRADING LTD.' },
  { symbol: 'TARAJEWELS.BO', name: 'TARA JEWELS LTD.' },
  { symbol: 'FOCUSIRL.BO', name: 'FOCUS INDUSTRIAL RESOURCES LTD.' },
  { symbol: 'CIGNITI.BO', name: 'CIGNITI TECHNOLOGIES LTD.' },
  { symbol: 'PANKAJPOLY.BO', name: 'Pankaj Polypack Ltd' },
  { symbol: 'INFRATEL.BO', name: 'BHARTI INFRATEL LTD.' },
  { symbol: 'EFPL.BO', name: 'ECO FRIENDLY FOOD PROCESSING PARK LTD.' },
  { symbol: 'COVIDH.BO', name: 'COVIDH TECHNOLOGIES LIMITED' },
  { symbol: 'EBFL.BO', name: 'ESTEEM BIO ORGANIC FOOD PROCESSING  LTD.' },
  { symbol: 'KFL.BO', name: 'KAVITA FABRICS LTD.' },
  { symbol: 'SRDL.BO', name: 'SUNSTAR REALTY DEVELOPMENT LTD.' },
  { symbol: 'CNEL.BO', name: 'CHANNEL NINE ENTERTAINMENT LTD.' },
  { symbol: 'PEARLAGRI.BO', name: 'PEARL AGRICULTURE LTD.' },
  { symbol: 'HPCBL.BO', name: 'HPC BIOSCIENCES LTD.' },
  { symbol: 'COMFINCAP.BO', name: 'COMFORT FINCAP LTD.' },
  { symbol: 'SBISENSEX.BO', name: 'SBI Mutual Fund - SBI Sensex ETF' },
  { symbol: 'BMAL.BO', name: 'BOTHRA METALS & ALLOYS LTD.' },
  { symbol: 'LAKHOTIA.BO', name: 'LAKHOTIA POLYESTERS (INDIA) LTD.' },
  { symbol: 'GCMSECU.BO', name: 'GCM Securities Ltd' },
  { symbol: 'SRL.BO', name: 'SAMRUDDHI REALTY LTD.' },
  { symbol: 'AIFL.BO', name: 'ASHAPURA INTIMATES FASHION LTD.' },
  { symbol: 'DELTALTD.BO', name: 'DELTA LEASING & FINANCE LTD.' },
  { symbol: 'IDFCEOS1RD.BO', name: 'IDFC Equity Opportunity- Series 1- Regular Plan- Dividend' },
  { symbol: 'IDFCEOS1DD.BO', name: 'IDFC Equity Opportunity- Series 1- Direct Plan- Dividend' },
  { symbol: 'PRIMECAPM.BO', name: 'PRIME CAPITAL MARKET LTD.' },
  { symbol: 'KIFS.BO', name: 'KIFS FINANCIAL SERVICES LTD.' },
  { symbol: 'SHARDA.BO', name: 'Sharda Motor Industries Ltd' },
  { symbol: 'BINNYMILLS.BO', name: 'BINNY MILLS LTD.' },
  { symbol: 'SVGLOBAL.BO', name: 'S V GLOBAL MILL LTD.' },
  { symbol: 'OTML.BO', name: 'ONESOURCE TECHMEDIA LTD.' },
  { symbol: 'OONE.BO', name: 'OBJECTONE INFORMATION SYSTEMS LTD.' },
  { symbol: 'PAWANSUT.BO', name: 'PAWANSUT HOLDINGS LTD.' },
  { symbol: 'IFINSEC.BO', name: 'INDIA FINSEC LTD.' },
  { symbol: 'BRAHMINFRA.BO', name: 'BRAHMAPUTRA INFRASTRUCTURE LTD.' },
  { symbol: 'EDSL.BO', name: 'EDYNAMICS SOLUTIONS LTD.' },
  { symbol: 'QUEST.BO', name: 'Quest Softech (India) Ltd' },
  { symbol: 'KHOOBSURAT.BO', name: 'Khoobsurat Ltd' },
  { symbol: 'PFRL.BO', name: 'Pantaloons Fashion & Retail Ltd' },
  { symbol: 'IBULHSGFIN.BO', name: 'Indiabulls Housing Finance Ltd' },
  { symbol: 'MMLF.BO', name: 'Money Masters Leasing & Finance Ltd' },
  { symbol: 'ALSL.BO', name: 'Alacrity Securities Ltd' },
  { symbol: 'GCMCOMM.BO', name: 'GCM Commodity & Derivatives Ltd' },
  { symbol: 'INTEGRA.BO', name: 'Integra Garments And Textiles Ltd' },
  { symbol: 'SILINFRA.BO', name: 'Silverpoint Infratech Ltd' },
  { symbol: 'VKJINFRA.BO', name: 'VKJ Infradevelopers Ltd' },
  { symbol: 'KUSHAL.BO', name: 'Kushal Tradelink Ltd' },
  { symbol: 'RJBIOTECH.BO', name: 'R J Bio-Tech Ltd' },
  { symbol: 'ATWL.BO', name: 'Ace Tours Worldwide Ltd' },
  { symbol: 'JK AGRI.BO', name: 'JK Agri Genetics Ltd' },
  { symbol: 'TRIMURTHI.BO', name: 'Trimurthi Drugs & Pharmaceuticals Ltd' },
  { symbol: 'SKFL.BO', name: 'Satkar Finlease Ltd' },
  { symbol: 'NEWEVER.BO', name: 'Newever Trade Wings Ltd' },
  { symbol: 'PVVINFRA.BO', name: 'PVV Infra Ltd' },
  { symbol: 'SFCL.BO', name: 'Star Ferro and Cement Ltd' },
  { symbol: 'SUBHTEX.BO', name: 'Subh Tex (India) Ltd' },
  { symbol: 'VCU.BO', name: 'VCU Data Management Ltd' },
  { symbol: 'VAKPOWINF.BO', name: 'Vakharia Power Infrastructure Ltd' },
  { symbol: 'SRGSFL.BO', name: 'S R G Securities Finance Ltd' },
  { symbol: 'ACFSL.BO', name: 'Amrapali Capital and Finance Services Ltd' },
  { symbol: 'STELLAR.BO', name: 'Stellar Capital Services Ltd' },
  { symbol: 'FIVEX.BO', name: 'Five X Finance & Investment Ltd' },
  { symbol: 'SRSFIN.BO', name: 'SRS Finance Ltd' },
  { symbol: 'BOSTONTEK.BO', name: 'Boston Teknowsys (India) Ltd' },
  { symbol: 'INTELSOFT.BO', name: 'Integra Telecommunication & Software Ltd' },
  { symbol: 'M100.BO', name: 'Motilal Oswal Mutual Fund - Motilal Oswal MOSt Shares Midcap 100 ETF- Growth option' },
  { symbol: 'BPCAP.BO', name: 'B. P. Capital Ltd' },
  { symbol: 'CPL.BO', name: 'Captain Polyplast Ltd' },
  { symbol: 'RCLEDPLADD.BO', name: 'Reliance Mutual Fund - Reliance Close Ended Equity Fund- Series A - Direct Plan Dvdnd Pyot Optin' },
  { symbol: 'RCLEDPLADG.BO', name: 'Reliance Mutual Fund- Reliance Close Ended Equity Fund- Series A - Direct Plan  Growth Option' },
  { symbol: 'RCLENDPLAD.BO', name: 'Reliance Mutual Fund- Reliance Close Ended Equity Fund- Series A - Dividend Payout Option' },
  { symbol: 'RCLENDPLAG.BO', name: 'Reliance Mutual Fund- Reliance Close Ended Equity Fund- Series A - Growth Option' },
  { symbol: 'INIFTY.BO', name: 'ICICI Prudential Mutual Fund - ICICI Prudential Nifty ETF' },
  { symbol: 'ICNX100.BO', name: 'ICICI Prudential Mutual Fund - ICICI Prudential CNX 100 ETF' },
  { symbol: 'SATYA.BO', name: 'Satya Miners & Transporters Ltd' },
  { symbol: 'ARNOLD.BO', name: 'Arnold Holdings Ltd' },
  { symbol: 'MODEX.BO', name: 'Modex International Securities Ltd' },
  { symbol: 'TENTIMETAL.BO', name: 'Tentiwala Metal Products Ltd' },
  { symbol: 'RCLEDPLBDD.BO', name: 'Reliance Mutual Fund - Reliance Close Ended Equity Fund- Series B - Direct Plan Dividend Payo O' },
  { symbol: 'RCLEDPLBDG.BO', name: 'Reliance Mutual Fund- Reliance Close Ended Equity Fund- Series B - Direct Plan Growth Option' },
  { symbol: 'RCLENDPLBD.BO', name: 'Reliance Mutual Fund- Reliance Close Ended Equity Fund- Series B - Dividend Payout Option' },
  { symbol: 'RCLENDPLBG.BO', name: 'Reliance Mutual Fund- Reliance Close Ended Equity Fund- Series B - Growth Option' },
  { symbol: 'SUNLOC.BO', name: 'Sunil Healthcare Ltd' },
  { symbol: 'RCIIND.BO', name: 'RCI Industries & Technologies Ltd' },
  { symbol: 'CHEMTECH.BO', name: 'Chemtech Industrial Valves Ltd' },
  { symbol: 'GAILBBPH.BO', name: 'GAILBBPH' },
  { symbol: 'TAAZAINT.BO', name: 'Taaza International Ltd' },
  { symbol: 'IDFCEOS2RD.BO', name: 'IDFC Mutual Fund- IDFC Equity Opportunity- Series 2 - Regular Plan- Dividend' },
  { symbol: 'IDFCEOS2DD.BO', name: 'IDFC Mutual Fund - IDFC Equity Opportunity- Series 2 - Direct Plan- Dividend' },
  { symbol: 'RELCNX100.BO', name: 'Reliance Mutual Fund - R Shares CNX 100 Fund' },
  { symbol: 'RELNIFTY.BO', name: 'Reliance Mutual Fund - R Shares Nifty ETF' },
  { symbol: 'AGRIMONY.BO', name: 'Agrimony Commodities Ltd' },
  { symbol: 'DENISCHEM.BO', name: 'Denis Chem Lab Ltd' },
  { symbol: 'POLYMAC.BO', name: 'Polymac Thermoformers Ltd' },
  { symbol: 'UNISHIRE.BO', name: 'Unishire Urban Infra Ltd' },
  { symbol: 'BSLFEFS1RG.BO', name: 'Birla Sun Life Mutual Fund- Birla Sun Life Focused Equity Fund - Series 1 - Regular Plan - Growth' },
  { symbol: 'BSLFEFS1RN.BO', name: 'Birla Sun Life Mutual Fund- Birla Sun Life Focused Equity Fund-Series 1-Regular Plan-Divind Payot' },
  { symbol: 'BSLFEFS1DG.BO', name: 'Birla Sun Life Mutual Fund - Birla Sun Life Focused Equity Fund - Series 1 - Direct Plan - Growth' },
  { symbol: 'BSLFEFS1DN.BO', name: 'Birla Sun Life Mutual Fund - Birla Sun Life Focused Equity Fund-Series 1-Direct Plan-Dividnd Payot' },
  { symbol: 'IPRU2262.BO', name: 'ICICI Prudential Mutual Fund - ICICI Prudential Equity Savings Fund Series 1-Reglr Plan Cumulative' },
  { symbol: 'IPRU2263.BO', name: 'ICICI Prudential Mutual Fund - ICICI Prudential Equity Savings Fund Series 1-Regular Plan Dividend' },
  { symbol: 'IPRU8462.BO', name: 'ICICI Prudential Mutual Fund - ICICI Prudential Equity Savings Fund Series 1-Direct Plan Cumulative' },
  { symbol: 'IPRU8463.BO', name: 'ICICI Prudential Mutual Fund - ICICI Prudential Equity Savings Fund Series 1 - Direct Plan Dividend' },
  { symbol: 'SIVI.BO', name: 'Siddhi Vinayak Shipping Corporation Ltd' },
  { symbol: 'ETT.BO', name: 'ETT Ltd' },
  { symbol: 'KOTAKNIFTY.BO', name: 'Kotak Mahindra Mutual Fund - Kotak Nifty ETF' },
  { symbol: 'BCP.BO', name: 'B.C. Power Controls Ltd' },
  { symbol: 'KCSL.BO', name: 'Karnimata Cold Storage Ltd' },
  { symbol: 'ANISHAIMPEX.BO', name: 'Anisha Impex Ltd' },
  { symbol: 'MANGIND.BO', name: 'Mangalam Industrial Finance Ltd' },
  { symbol: 'MITL.BO', name: 'Mahadushi International Trade Ltd' },
  { symbol: 'PHOENIXTN.BO', name: 'Phoenix Township Ltd' },
  { symbol: 'RAUNAQEPC.BO', name: 'Raunaq EPC International Ltd' },
  { symbol: 'HRGESSRG2.BO', name: 'HDFC Mutual Fund - HDFC Rajiv Gandhi Equity Savings Scheme - Series 2 - Regular Plan - G O' },
  { symbol: 'HRGESSRD2.BO', name: 'HDFC Mutual Fund - HDFC Rajiv Gandhi Equity Savings Scheme - Series 2-Regular Plan- D P O' },
  { symbol: 'HRGESSDG2.BO', name: 'HDFC Mutual Fund - HDFC Rajiv Gandhi Equity Savings Scheme - Series 2- Direct Plan- Growth O' },
  { symbol: 'HRGESSDD2.BO', name: 'HDFC Mutual Fund - HDFC Rajiv Gandhi Equity Savings Scheme - Series 2- Direct Plan- Dvdd P O' },
  { symbol: 'IPRU2296.BO', name: 'ICICI Prudential Value Fund Series 3(Regular Dividend Option)' },
  { symbol: 'IPRU8496.BO', name: 'ICICI Prudential Value Fund Series 3(Direct Dividend Option)' },
  { symbol: 'SKP.BO', name: 'Shri Krishna Prasadam Ltd' },
  { symbol: 'LICNFR2GP.BO', name: 'LIC NOMURA MF RGESS Fund Series -2 -Regular Plan -Growth Option' },
  { symbol: 'LICNFR2DP.BO', name: 'LIC NOMURA MF RGESS Fund Series -2 -Regular Plan- Dividend Payout Option' },
  { symbol: 'LICNFR2G1.BO', name: 'LIC NOMURA MF RGESS Fund Series- 2 -Direct Plan- Growth Option' },
  { symbol: 'LICNFR2D1.BO', name: 'LIC NOMURA MF RGESS Fund Series- 2 -Direct Plan -Dividend Payout Option' },
  { symbol: 'BSLFEFS2RG.BO', name: 'Birla Sun Life Mutual Fund- Birla Sun Life Focused Equity Fund - Series 2 - Regular Plan - Growth' },
  { symbol: 'BSLFEFS2RN.BO', name: 'Birla Sun Life Mutual Fund- Birla Sun Life Focused Equity Fund - Series 2 - Regular Plan - D P' },
  { symbol: 'BSLFEFS2DG.BO', name: 'Birla Sun Life Mutual Fund-  Birla Sun Life Focused Equity Fund - Series 2 - Direct Plan - Growth' },
  { symbol: 'BSLFEFS2DN.BO', name: 'Birla Sun Life Mutual Fund-  Birla Sun Life Focused Equity Fund - Series 2 - Direct Plan-Dividend P' },
  { symbol: 'OBIL.BO', name: 'Oceanaa Biotek Industries Ltd' },
  { symbol: 'CPSEETF.BO', name: 'Goldman Sachs Mutual Fund- CPSE ETF-Growth Option' },
  { symbol: 'HARIAAPL.BO', name: 'Haria Apparels Ltd' },
  { symbol: 'JOONKTOLL.BO', name: 'Joonktollee Tea & Industries Ltd' },
  { symbol: 'WOMENSNEXT.BO', name: 'Women\'s Next Loungeries Ltd' },
  { symbol: 'SHARPINV.BO', name: 'Sharp Investments Ltd' },
  { symbol: 'RESPONSINF.BO', name: 'Response Informatics Ltd' },
  { symbol: 'IPRU2365.BO', name: 'IPRU2365' },
  { symbol: 'IPRU2366.BO', name: 'IPRU2366' },
  { symbol: 'IPRU8565.BO', name: 'IPRU8565' },
  { symbol: 'IPRU8566.BO', name: 'IPRU8566' },
  { symbol: 'GCMCAPI.BO', name: 'GCM Capital Advisors Ltd' },
  { symbol: 'ADHUNIKIND.BO', name: 'Adhunik Industries Ltd' },
  { symbol: 'WESTLEIRES.BO', name: 'West Leisure Resorts Ltd' },
  { symbol: 'METSL.BO', name: 'Maestros Electronics & Telecommunications Systems Ltd' },
  { symbol: 'SPS.BO', name: 'SPS Finquest Ltd' },
  { symbol: 'RCLEDIIADD.BO', name: 'Reliance Close Ended Equity Fund II- Series A - Direct Plan Dividend Payout Option' },
  { symbol: 'RCLEDIIADG.BO', name: 'Reliance Close Ended Equity Fund II- Series A - Direct Plan Growth Option' },
  { symbol: 'RCLENDIIAD.BO', name: 'Reliance Close Ended Equity Fund II- Series A - Dividend Payout Option' },
  { symbol: 'RCLENDIIAG.BO', name: 'Reliance Close Ended Equity Fund II- Series A - Growth Option' },
  { symbol: 'JACKSON.BO', name: 'Jackson Investments Ltd' },
  { symbol: 'CCFCL.BO', name: 'Classic Global Finance & Capital Ltd' },
  { symbol: 'DHANUKACOM.BO', name: 'Dhanuka Commercial Ltd' },
  { symbol: 'WORTH.BO', name: 'Worth Investment & Trading Co Ltd' },
  { symbol: 'TPROJECT.BO', name: 'Thirani Projects Ltd' },
  { symbol: 'AMARSEC.BO', name: 'Amarnath Securities Ltd' },
  { symbol: 'CTL.BO', name: 'Capital Trade Links Ltd' },
  { symbol: 'TARINI.BO', name: 'Tarini International Ltd' },
  { symbol: 'IPRU2401.BO', name: 'ICICI Prudential Growth Fund Series 1 (Regular Dividend Payout)' },
  { symbol: 'IPRU8601.BO', name: 'ICICI Prudential Growth Fund Series 1 (Direct Dividend Option)' },
  { symbol: 'CROWNTOURS.BO', name: 'Crown Tours Ltd' },
  { symbol: 'JTAPARIA.BO', name: 'J. Taparia Projects Ltd' },
  { symbol: 'RLFL.BO', name: 'Ramchandra Leasing & Finance Ltd' },
  { symbol: 'BRPL.BO', name: 'Bansal Roofing Products Ltd' },
  { symbol: 'OASIS.BO', name: 'Oasis Tradelink Ltd' },
  { symbol: 'ADARSH.BO', name: 'Adarsh Mercantile Ltd' },
  { symbol: 'JAMESWARREN.BO', name: 'James Warren Tea Ltd' },
  { symbol: 'SHUBHRA.BO', name: 'Shubhra Leasing Finance And Investment Company Ltd' },
  { symbol: 'POTENTIAL.BO', name: 'Potential Investments & Finance Ltd' },
  { symbol: 'FRUTION.BO', name: 'Fruition Venture Ltd' },
  { symbol: 'SURYAMARK.BO', name: 'Surya Marketing Ltd' },
  { symbol: 'BHANDERI.BO', name: 'Bhanderi Infracon Ltd' },
  { symbol: 'IPRU2428.BO', name: 'ICICI Prudential Growth Fund Series 2 (Regular Plan - Dividend Payout Option)' },
  { symbol: 'IPRU8628.BO', name: 'ICICI Prudential Growth Fund Series 2 (Direct Plan - Dividend Payout Option)' },
  { symbol: 'CAREWELL.BO', name: 'Carewell Industries Ltd' },
  { symbol: 'RCAPBULADD.BO', name: 'Reliance Mutual Fund - Reliance Capital Builder Fund- Series A - Direct Plan Dividend Payout Optio' },
  { symbol: 'RCAPBULADG.BO', name: 'Reliance Mutual Fund- Reliance Capital Builder Fund- Series A - Direct Plan   Growth Option' },
  { symbol: 'RCAPBUILAD.BO', name: 'Reliance Mutual Fund- Reliance Capital Builder Fund- Series A - Dividend Payout Option' },
  { symbol: 'RCAPBUILAG.BO', name: 'Reliance Mutual Fund- Reliance Capital Builder Fund- Series A - Growth Option' },
  { symbol: 'GLOSTER.BO', name: 'Gloster Ltd' },
  { symbol: 'QUANTBUILD.BO', name: 'Quantum Build-Tech Ltd' },
  { symbol: 'TTIENT.BO', name: 'TTI Enterprise Ltd' },
  { symbol: 'VISHAL.BO', name: 'Vishal Fabrics Ltd' },
  { symbol: 'OJASASSET.BO', name: 'Ojas Asset Reconstruction Company Ltd' },
  { symbol: 'GAJANANSEC.BO', name: 'Gajanan Securities Services Ltd' },
  { symbol: 'UNISON.BO', name: 'Unison Metals Ltd' },
  { symbol: 'RTFL.BO', name: 'Real  Touch Finance Ltd' },
  { symbol: 'RCAPBULBDD.BO', name: 'Reliance Mutual Fund - Reliance Capital Builder Fund- Series B - Direct Dvdnd Plan- Dvdnd Pyot O' },
  { symbol: 'RCAPBULBDG.BO', name: 'Reliance Mutual Fund- Reliance Capital Builder Fund- Series B - Direct Growth Plan- Growth Option' },
  { symbol: 'RCAPBUILBD.BO', name: 'Reliance Mutual Fund- Reliance Capital Builder Fund- Series B - Dividend Plan-Dividend Pyut Otin' },
  { symbol: 'RCAPBUILBG.BO', name: 'Reliance Mutual Fund- Reliance Capital Builder Fund- Series B - Growth Plan- Growth Option' },
  { symbol: 'VRL.BO', name: 'Vasundhara Rasayans Ltd' },
  { symbol: 'PARNAMI.BO', name: 'Parnami Credits Ltd' },
  { symbol: 'PURSHOTTAM.BO', name: 'Purshottam Investofin Ltd' },
  { symbol: 'IPRU2487.BO', name: 'ICICI Prudential Value Fund Series 5 (Regular Cumulative Option)' },
  { symbol: 'IPRU2488.BO', name: 'ICICI Prudential Value Fund Series 5 (Regular Dividend Option)' },
  { symbol: 'IPRU8687.BO', name: 'ICICI Prudential Value Fund Series 5 (Direct Cumulative Option)' },
  { symbol: 'IPRU8688.BO', name: 'ICICI Prudential Value Fund Series 5 (Direct Dividend Option)' },
  { symbol: 'HCLTD.BO', name: 'Hind Commerce Ltd' },
  { symbol: 'SIROHIA.BO', name: 'Sirohia & Sons Ltd' },
  { symbol: 'NAYSAA.BO', name: 'Naysaa Securities Ltd' },
  { symbol: 'CITYONLINE.BO', name: 'City Online Services Ltd' },
  { symbol: 'RELSENSEX.BO', name: 'Reliance Mutual Fund - R  Shares Sensex ETF' },
  { symbol: 'ENCASH.BO', name: 'Encash Entertainment Ltd' },
  { symbol: 'ULTRACAB.BO', name: 'Ultracab (India) Ltd' },
  { symbol: 'RCCL.BO', name: 'Rajasthan Cylinders & Containers Ltd' },
  { symbol: 'ETIL.BO', name: 'Econo Trade (India) Ltd' },
  { symbol: 'SEOFIGR.BO', name: 'SBI Equity Opportunities Fund - Series I- Regular Plan -Growth' },
  { symbol: 'SEOFIDR.BO', name: 'SBI Equity Opportunities Fund - Series I-Regular Plan- Dividend Payout' },
  { symbol: 'SEOFIGD.BO', name: 'SBI Equity Opportunities Fund - Series I-Direct Plan -Growth' },
  { symbol: 'SEOFIDD.BO', name: 'SBI Equity Opportunities Fund - Series I-Direct Plan - Dividend Payout' },
  { symbol: 'ATISHAY.BO', name: 'Atishay Infotech Ltd' },
  { symbol: 'SUCHITRA.BO', name: 'Suchitra Finance & Trading Company Ltd' },
  { symbol: 'DHABRIYA.BO', name: 'Dhabriya Polywood Ltd' },
  { symbol: 'ARYACAPM.BO', name: 'Aryaman Capital Markets Ltd' },
  { symbol: 'RCAPBULCDD.BO', name: 'Reliance Capital Builder Fund- Series C - Direct Dividend Plan- Dividend Payout Option' },
  { symbol: 'RCAPBULCDG.BO', name: 'Reliance Capital Builder Fund- Series C - Direct Growth Plan- Growth Option' },
  { symbol: 'RCAPBUILCD.BO', name: 'Reliance Mutual Fund- Reliance Capital Builder Fund- Series C - Dividend Plan-Dividend P O' },
  { symbol: 'RCAPBUILCG.BO', name: 'Reliance Capital Builder Fund- Series C - Growth Plan- Growth Option' },
  { symbol: 'IPRU2511.BO', name: 'ICICI Prudential Growth Fund Series 3 (Regular Plan - Dividend Payout Option)' },
  { symbol: 'IPRU8711.BO', name: 'ICICI Prudential Growth Fund Series 3 (Direct Plan - Dividend Payout Option)' },
  { symbol: 'PDSMFL.BO', name: 'PDS Multinational Fashions Ltd' },
  { symbol: 'POWERHOUSE.BO', name: 'Powerhouse Fitness And Realty Ltd' },
  { symbol: 'VGCL.BO', name: 'Vibrant Global Capital Ltd' },
  { symbol: 'STARLIT.BO', name: 'Starlit Power Systems Ltd' },
  { symbol: 'ADCC.BO', name: 'ADCC Infocad Ltd' },
  { symbol: 'RUBYTEL.BO', name: 'Ruby Traders & Exporters Ltd' },
  { symbol: 'BSLFEFS3RG.BO', name: 'Birla Sun Life Focused Equity Fund - Series 3- Regular Plan - Growth' },
  { symbol: 'BSLFEFS3RN.BO', name: 'Birla Sun Life Focused Equity Fund - Series 3 - Regular Plan - Dividend Payout' },
  { symbol: 'BSLFEFS3DG.BO', name: 'Birla Sun Life Focused Equity Fund - Series 3 - Direct Plan - Growth' },
  { symbol: 'BSLFEFS3DN.BO', name: 'Birla Sun Life Focused Equity Fund - Series 3 - Direct Plan - Dividend Payout' },
  { symbol: 'IPRU2530.BO', name: 'ICICI Prudential Growth Fund Series 4 (Regular Plan - Dividend Payout Option)' },
  { symbol: 'IPRU8730.BO', name: 'ICICI Prudential Growth Fund Series 4 (Direct Plan - Dividend Payout Option)' },
  { symbol: 'JSHL.BO', name: 'JLA Infraville Shoppers Ltd' },
  { symbol: 'CRANEINFRA.BO', name: 'Crane Infrastructure Ltd' },
  { symbol: 'PINCON.BO', name: 'Pincon Spirit Ltd' },
  { symbol: 'M3GLOBAL.BO', name: 'M3 Global Finance Ltd' },
  { symbol: 'ASIACAP.BO', name: 'Asia Capital Ltd' },
  { symbol: 'AKASHDEEP.BO', name: 'Akashdeep Metal Industries Ltd' },
  { symbol: 'CIL.BO', name: 'Citizen Infoline Ltd' },
  { symbol: 'GILADAFINS.BO', name: 'Gilada Finance & Investments Ltd' },
  { symbol: 'BUDGE BUDGE.BO', name: 'Budge Budge Company Ltd' },
  { symbol: 'TEJINFOWAY.BO', name: 'Tej Infoways Ltd' },
  { symbol: 'JETINFRA.BO', name: 'Jet Infraventure Ltd' },
  { symbol: 'D3YRCEERG.BO', name: 'DSP BlackRock 3 Years Close Ended Equity Fund-Regular- Growth' },
  { symbol: 'D3YRCEERDP.BO', name: 'DSP BlackRock 3 Years Close Ended Equity Fund-Regular- Dividend Payout' },
  { symbol: 'D3YRCEEDG.BO', name: 'DSP BlackRock 3 Years Close Ended Equity Fund- Direct Plan - Growth' },
  { symbol: 'D3YRCEEDDP.BO', name: 'DSP BlackRock 3 Years Close Ended Equity Fund-Direct Plan - Dividend Payout' },
  { symbol: 'AANCHALISP.BO', name: 'Aanchal Ispat Ltd' },
  { symbol: 'CAPPIPES.BO', name: 'Captain Pipes Ltd' },
  { symbol: 'SEOFIIGR.BO', name: 'SBI Equity Opportunities Fund - Series II-Regular Plan -Growth' },
  { symbol: 'SEOFIIDR.BO', name: 'SBI Equity Opportunities Fund - Series II-Regular Plan- Dividend Payout' },
  { symbol: 'SEOFIIGD.BO', name: 'SBI Equity Opportunities Fund - Series II-Direct Plan -Growth' },
  { symbol: 'SEOFIIDD.BO', name: 'SBI Equity Opportunities Fund - Series II-Direct Plan - Dividend Payout' },
  { symbol: 'ANUBHAV.BO', name: 'Anubhav Infrastructure Ltd' },
  { symbol: 'JSTL.BO', name: 'Jeevan Scientific Technology Ltd' },
  { symbol: 'ICL.BO', name: 'Indo Cotspin Ltd' },
  { symbol: 'BSLFEFS4RG.BO', name: 'Birla Sun Life Focused Equity Fund - Series 4- Regular Plan - Growth' },
  { symbol: 'BSLFEFS4RN.BO', name: 'Birla Sun Life Focused Equity Fund - Series 4 - Regular Plan - Dividend Payout' },
  { symbol: 'BSLFEFS4DG.BO', name: 'Birla Sun Life Focused Equity Fund - Series 4 - Direct Plan - Growth' },
  { symbol: 'BSLFEFS4DN.BO', name: 'Birla Sun Life Focused Equity Fund - Series 4 - Direct Plan - Dividend Payout' },
  { symbol: 'RCBFIIADD.BO', name: 'Reliance Capital Builder Fund II - Series A - Direct Plan Dividend Payout Option' },
  { symbol: 'RCBFIIADG.BO', name: 'Reliance Capital Builder Fund II - Series A - Direct Plan Growth Option' },
  { symbol: 'RCBFIIAD.BO', name: 'Reliance Capital Builder Fund II - Series A -  Dividend Payout Option' },
  { symbol: 'RCBFIIAG.BO', name: 'Reliance Capital Builder Fund II - Series A -  Growth Option' },
  { symbol: 'SCC.BO', name: 'Scintilla Commercial & Credit Ltd' },
  { symbol: 'CAMSONBIO.BO', name: 'Camson Bio Technologies Ltd' },
  { symbol: 'AMSONS.BO', name: 'Amsons Apparels Ltd' },
  { symbol: 'MYMONEY.BO', name: 'My Money Securities Ltd' },
  { symbol: 'AMARNATH.BO', name: 'Sri Amarnath Finance Ltd' },
  { symbol: 'CSL.BO', name: 'Continental Securities Ltd' },
  { symbol: 'WINYCOMM.BO', name: 'Winy Commercial & Fiscal Services Ltd' },
  { symbol: 'NEXUSCOMMO.BO', name: 'Nexus Commodities And Technologies Ltd' },
  { symbol: 'SELLWIN.BO', name: 'Sellwin Traders Ltd' },
  { symbol: 'STSERV.BO', name: 'S T Services Ltd' },
  { symbol: 'GALADAFIN.BO', name: 'Galada Finance Ltd' },
  { symbol: 'EMERALD.BO', name: 'Emerald Leasing Finance & Investment Company Ltd' },
  { symbol: 'MKEXIM.BO', name: 'M.K. Exim (India) Ltd' },
  { symbol: 'SIPROJECTS.BO', name: 'South India Projects Ltd' },
  { symbol: 'IPRU2586.BO', name: 'ICICI Prudential Growth Fund Series 6 (Regular Plan - Dividend Payout)' },
  { symbol: 'IPRU8788.BO', name: 'ICICI Prudential Growth Fund Series 6 (Direct Plan - Dividend Payout)' },
  { symbol: 'OCTAL.BO', name: 'Octal Credit Capital Ltd' },
  { symbol: 'MIHIKA.BO', name: 'Mihika Industries Ltd' },
  { symbol: 'SHRINIWAS.BO', name: 'Shri Niwas Leasing And Finance Ltd' },
  { symbol: 'DHUNTEAIND.BO', name: 'Dhunseri Tea & Industries Ltd' },
  { symbol: 'SKIL.BO', name: 'Skyline Ventures India Ltd' },
  { symbol: 'SPACEAGE.BO', name: 'Spaceage Products Ltd' },
  { symbol: 'RAFL.BO', name: 'Raghuvansh Agrofarms Ltd' },
  { symbol: 'CSSTECH.BO', name: 'CSS Technergy Ltd' },
  { symbol: 'SOFCOM.BO', name: 'Sofcom Systems Ltd' },
  { symbol: 'IPRU2591.BO', name: 'ICICI Prudential Growth Fund Series 7 (Regular Plan - Dividend Payout)' },
  { symbol: 'IPRU8793.BO', name: 'ICICI Prudential Growth Fund Series 7 (Direct Plan - Dividend Payout)' },
  { symbol: 'NAPL.BO', name: 'Naturite Agro Products Ltd' },
  { symbol: 'KARNAVATI.BO', name: 'Karnavati Finance Ltd' },
  { symbol: 'ABHIFIN.BO', name: 'Abhishek Finlease Ltd' },
  { symbol: 'IPRU2598.BO', name: 'ICICI Prudential Growth Fund Series 8 (Regular Plan - Dividend Payout)' },
  { symbol: 'IPRU8800.BO', name: 'ICICI Prudential Growth Fund Series 8 (Direct Plan - Dividend Payout)' },
  { symbol: 'RCBFIIBDD.BO', name: 'Reliance Capital Builder Fund II - Series B - Direct Plan Dividend Payout Option' },
  { symbol: 'RCBFIIBDG.BO', name: 'Reliance Capital Builder Fund II - Series B - Direct Plan Growth Option' },
  { symbol: 'RCBFIIBD.BO', name: 'Reliance Capital Builder Fund II - Series B -  Dividend Payout Option' },
  { symbol: 'RCBFIIBG.BO', name: 'Reliance Capital Builder Fund II - Series B -  Growth Option' },
  { symbol: 'ALFL.BO', name: 'Abhinav Leasing & Finance Ltd' },
  { symbol: 'LICNFR3G.BO', name: 'LIC NOMURA MF RGESS Fund Series- 3 -Regular Plan- Growth Option' },
  { symbol: 'LICNFR3D.BO', name: 'LIC NOMURA MF RGESS Fund Series -3- Regular Plan -Dividend Payout Option' },
  { symbol: 'LICNFR3G1.BO', name: 'LIC NOMURA MF RGESS Fund Series -3 -Direct Plan -Growth Option' },
  { symbol: 'LICNFR3D1.BO', name: 'LIC NOMURA MF RGESS Fund Series- 3 -Direct Plan -Dividend Payout Option' },
  { symbol: 'PACT.BO', name: 'Pact Industries Ltd' },
  { symbol: 'MERCURYLAB.BO', name: 'Mercury Laboratories Ltd' },
  { symbol: 'CONCORD.BO', name: 'Concord Drugs Ltd' },
  { symbol: 'SHREESEC.BO', name: 'Shree Securities Ltd' },
  { symbol: 'GRNLAMIND.BO', name: 'Greenlam Industries Ltd' },
  { symbol: 'HFEFDG.BO', name: 'HDFC Focused Equity Fund Plan A - Direct Option- Growth Option' },
  { symbol: 'HFEFDD.BO', name: 'HDFC Focused Equity Fund Plan A - Direct Option- Dividend Option' },
  { symbol: 'HFEFRG.BO', name: 'HDFC Focused Equity Fund Plan A-Regular Option- Growth Option' },
  { symbol: 'HFEFRD.BO', name: 'HDFC Focused Equity Fund Plan A-Regular Option - Dividend Option' },
  { symbol: 'TALBROSENG.BO', name: 'Talbros Engineering Ltd' },
  { symbol: 'SAPL.BO', name: 'SAR Auto Products Ltd' },
  { symbol: 'PUROHITCON.BO', name: 'Purohit Construction Ltd' },
  { symbol: 'JAYATMA.BO', name: 'Jayatma Spinners Ltd' },
  { symbol: 'GBL.BO', name: 'Gujarat Bitumen Ltd' },
  { symbol: 'CHENFERRO.BO', name: 'Chennai Ferrous Industries Ltd' },
  { symbol: 'MEGRISOFT.BO', name: 'Megri Soft Ltd' },
  { symbol: 'GITARENEW.BO', name: 'Gita Renewable Energy Ltd' },
  { symbol: 'KALPACOMME.BO', name: 'Kalpa Commercial Ltd' },
  { symbol: 'NEIL.BO', name: 'Neil Industries Ltd' },
  { symbol: 'ASHFL.BO', name: 'Akme Star Housing Finance Ltd' },
  { symbol: 'SSPNFIN.BO', name: 'SSPN Finance Ltd' },
  { symbol: 'SETFBSE100.BO', name: 'SBI Mutual Fund - SBI - ETF BSE 100' },
  { symbol: 'FRASER.BO', name: 'Fraser and Company Ltd' },
  { symbol: 'TTIL.BO', name: 'Tirupati Tyres Ltd' },
  { symbol: 'MAHABIR.BO', name: 'Mahabir Metallex Ltd' },
  { symbol: 'MANAKINDLTD.BO', name: 'Manaksia Industries Ltd' },
  { symbol: 'MANAKSTELTD.BO', name: 'Manaksia Steels Ltd' },
  { symbol: 'MNKALCOLTD.BO', name: 'Manaksia Aluminium Company Ltd' },
  { symbol: 'MNKCMILTD.BO', name: 'Manaksia Coated Metals & Industries Ltd' },
  { symbol: 'BSLFEFS5RG.BO', name: 'Birla Sun Life Focused Equity Fund - Series 5- Regular Plan - Growth' },
  { symbol: 'BSLFEFS5RN.BO', name: 'Birla Sun Life Focused Equity Fund - Series 5 - Regular Plan - Dividend Payout' },
  { symbol: 'BSLFEFS5DG.BO', name: 'Birla Sun Life Focused Equity Fund - Series 5 - Direct Plan - Growth' },
  { symbol: 'BSLFEFS5DN.BO', name: 'Birla Sun Life Focused Equity Fund - Series 5 - Direct Plan - Dividend Payout' },
  { symbol: 'ADLABS.BO', name: 'Adlabs Entertainment Ltd' },
  { symbol: 'RCBFIICDD.BO', name: 'Reliance Capital Builder Fund II - Series C - Direct Plan Dividend Payout Option' },
  { symbol: 'RCBFIICDG.BO', name: 'Reliance Capital Builder Fund II - Series C - Direct Plan Growth Option' },
  { symbol: 'RCBFIICD.BO', name: 'Reliance Capital Builder Fund II - Series C -  Dividend Payout Option' },
  { symbol: 'RCBFIICG.BO', name: 'Reliance Capital Builder Fund II - Series C -  Growth Option' },
  { symbol: 'IPRU2619.BO', name: 'ICICI Prudential India Recovery Fund - Series 1 (Regular Plan - Dividend Payout Option)' },
  { symbol: 'IPRU8821.BO', name: 'ICICI Prudential India Recovery Fund - Series 1 (Direct Plan - Dividend Payout Option)' },
  { symbol: 'RAJPUTANA.BO', name: 'Rajputana Investment and Finance Ltd' },
  { symbol: 'CITL.BO', name: 'Consecutive Investment & Trading Company Ltd' },
  { symbol: 'IPRU8841.BO', name: 'ICICI Prudential Value Fund Series 6- Direct Plan Cumulative Option' },
  { symbol: 'IPRU8842.BO', name: 'ICICI Prudential Value Fund Series 6- Direct Plan Dividend Option' },
  { symbol: 'IPRU2639.BO', name: 'ICICI Prudential Value Fund Series 6- Regular Plan Cumulative Option' },
  { symbol: 'IPRU2640.BO', name: 'ICICI Prudential Value Fund Series 6- Regular Plan Dividend Option' },
  { symbol: 'AANANDALAK.BO', name: 'Aananda Lakshmi Spinning Mills Ltd' },
  { symbol: 'YOGYA.BO', name: 'Yogya Enterprises Ltd' },
  { symbol: 'FILTRA.BO', name: 'Filtra Consultants and Engineers Ltd' },
  { symbol: 'ATHCON.BO', name: 'Athena Constructions Ltd' },
  { symbol: 'IPRU2626.BO', name: 'ICICI Prudential India Recovery Fund - Series 2 (Regular Plan - Dividend Payout Option)' },
  { symbol: 'IPRU8828.BO', name: 'ICICI Prudential India Recovery Fund - Series 2 (Direct Plan - Dividend Payout Option)' },
  { symbol: 'NORTHLINK.BO', name: 'Northlink Fiscal and Capital Services Ltd' },
  { symbol: 'SHESHAINDS.BO', name: 'Sheshadri Industries Ltd' },
  { symbol: 'SAB.BO', name: 'SAB Industries Ltd' },
  { symbol: 'HINDSECR.BO', name: 'Hind Securities & Credits Ltd' },
  { symbol: 'ALAN SCOTT.BO', name: 'Alan Scott Industries Ltd' },
  { symbol: 'OPCHAINS.BO', name: 'O. P. Chains Ltd' },
  { symbol: 'SUJALA.BO', name: 'Sujala Trading & Holdings Ltd' },
  { symbol: 'JAINMARMO.BO', name: 'Jain Marmo Industries Ltd' },
  { symbol: 'BFFL.BO', name: 'Bangalore Fort Farms Ltd' },
  { symbol: 'PALCO.BO', name: 'Palco Metals Ltd' },
  { symbol: 'BODHTREE.BO', name: 'Bodhtree Consulting Ltd' },
  { symbol: 'VBIND.BO', name: 'V B Industries Ltd' },
  { symbol: 'SARVOTTAM.BO', name: 'Sarvottam Finvest Ltd' },
  { symbol: 'HARI.BO', name: 'Haricharan Projects Ltd' },
  { symbol: 'CLLIMITED.BO', name: 'Crescent Leasing Ltd' },
  { symbol: 'VEGETABLE.BO', name: 'Vegetable Products Ltd' },
  { symbol: 'KTKKIGFG.BO', name: 'Kotak Mahindra Mutual Fund - Kotak India Growth Fund Series I Non Direct Plan- Growth option' },
  { symbol: 'HFEFBRG.BO', name: 'HDFC Focused Equity Fund Plan B - Regular Option- Growth' },
  { symbol: 'KTKKIGFD.BO', name: 'Kotak Mahindra Mutual Fund - Kotak India Growth Fund Series I Non Direct Plan- Dividend option' },
  { symbol: 'KTKKIGFGD.BO', name: 'Kotak Mahindra Mutual Fund - Kotak India Growth Fund Series I Direct Plan- Growth option' },
  { symbol: 'KTKKIGFDD.BO', name: 'Kotak Mahindra Mutual Fund - Kotak India Growth Fund Series I Direct Plan- Dividend Option' },
  { symbol: 'HFEFBRD.BO', name: 'HDFC Focused Equity Fund Plan B - Regular Option- Dividend Payout' },
  { symbol: 'HFEFBDG.BO', name: 'HDFC Focused Equity Fund Plan A - Direct Option - Growth' },
  { symbol: 'HFEFBDD.BO', name: 'HDFC Focused Equity Fund Plan A- Direct Option - Dividend Payout' },
  { symbol: 'NIFTYEES.BO', name: 'Edelweiss Mutual Fund - Edelweiss Exchange Traded Scheme - Nifty ( Nifty EES )' },
  { symbol: 'ICSL.BO', name: 'Integrated Capital Services Ltd' },
  { symbol: 'VLL.BO', name: 'Virat Leasing Ltd' },
  { symbol: 'SPISYS.BO', name: 'Spisys Ltd' },
  { symbol: 'FUNNY.BO', name: 'Funny Software Ltd' },
  { symbol: 'HELPAGE.BO', name: 'Helpage Finlease Ltd' },
  { symbol: 'INDRAIND.BO', name: 'Indra Industries Ltd' },
  { symbol: 'HAWAENG.BO', name: 'Hawa Engineers Ltd' },
  { symbol: 'IPRU2670.BO', name: 'ICICI Prudential  Value Fund Series 7' },
  { symbol: 'IPRU8872.BO', name: 'ICICI Prudential Value Fund Series 7' },
  { symbol: 'ADHBHUTIN.BO', name: 'Adhbhut Infrastructure Ltd' },
  { symbol: 'DFL.BO', name: 'Decillion Finance Ltd' },
  { symbol: 'POEL.BO', name: 'POCL Enterprises Ltd' },
  { symbol: 'AEL.BO', name: 'Amba Enterprises Ltd' },
  { symbol: 'DEVHARI.BO', name: 'Devhari Exports (India) Ltd' },
  { symbol: 'CAPFIN.BO', name: 'Capfin India Ltd' },
  { symbol: 'MOONGIPASEC.BO', name: 'Moongipa Securities Ltd' },
  { symbol: 'NOBPOL.BO', name: 'Noble Polymers Ltd' },
  { symbol: 'RCBFIIIAX.BO', name: 'Reliance Capital Builder Fund III Series A - Direct Plan Dividend Payout Option' },
  { symbol: 'RCBFIIIAZ.BO', name: 'Reliance Capital Builder Fund III Series A- Direct Plan Growth Option' },
  { symbol: 'RCBFIIIAD.BO', name: 'Reliance Capital Builder Fund III Series A- Dividend Plan Dividend Payout' },
  { symbol: 'RCBFIIIAG.BO', name: 'Reliance Capital Builder Fund III Series A- Growth Plan - Growth' },
  { symbol: 'GVBL.BO', name: 'Genomic Valley Biotech Ltd' },
  { symbol: 'MANPASAND.BO', name: 'Manpasand Beverages Ltd' },
  { symbol: 'JUNCTION.BO', name: 'Junction Fabrics and Apparels Ltd' },
  { symbol: 'SRESTHA.BO', name: 'Srestha Finvest Ltd' },
  { symbol: 'SAUMYA.BO', name: 'Saumya Consultants Ltd' },
  { symbol: 'MUL.BO', name: 'Mauria Udyog Ltd' },
  { symbol: 'MISHKA.BO', name: 'Mishka Exim Ltd' },
  { symbol: 'VMV.BO', name: 'VMV Holidays Ltd' },
  { symbol: 'AMBITION.BO', name: 'Ambition Mica Ltd' },
  { symbol: 'SIICL.BO', name: 'Shreenath Industrial Investment Company Ltd' },
  { symbol: 'JIYAECO.BO', name: 'Jiya Eco-Products Ltd' },
  { symbol: 'MDINDUCTO.BO', name: 'M.D. Inducto Cast Ltd' },
  { symbol: 'LOYAL.BO', name: 'Loyal Equipments Ltd' },
  { symbol: 'GPCL.BO', name: 'Gala Print City Ltd' },
  { symbol: 'MRSS.BO', name: 'Majestic Research Services and Solutions Ltd' },
  { symbol: 'CHEMIESYNT.BO', name: 'Chemiesynth (Vapi) Ltd' },
  { symbol: 'GPL.BO', name: 'Grandeur Products Ltd' },
  { symbol: 'JEL.BO', name: 'Jyotirgamya Enterprises Ltd' },
  { symbol: 'NIRVIKARA.BO', name: 'Nirvikara Paper Mills Ltd' },
  { symbol: 'SCFL.BO', name: 'Shyam Century Ferrous Ltd' },
  { symbol: 'SURYAINDIA.BO', name: 'Surya India Ltd' },
  { symbol: 'ADANITRANS.BO', name: 'Adani Transmission Ltd' },
  { symbol: 'STARDELTA.BO', name: 'Star Delta Transformers Ltd' },
  { symbol: 'AMRAFIN.BO', name: 'Amrapali Fincap Ltd' },
  { symbol: 'CONCRETE.BO', name: 'Concrete Credit Ltd' },
  { symbol: 'PCPROD.BO', name: 'PC Products India Ltd' },
  { symbol: 'PECOS.BO', name: 'Pecos Hotels and Pubs Ltd' },
  { symbol: 'BLFL.BO', name: 'Boston Leasing and Finance Ltd' },
  { symbol: 'MSL.BO', name: 'Mangalam Seeds Ltd' },
  { symbol: 'ALSTONE.BO', name: 'Alstone Textiles (India) Ltd' },
  { symbol: 'SYMBIOX.BO', name: 'Symbiox Investment & Trading Company Ltd' },
  { symbol: 'IPRU8895.BO', name: 'ICICI PRUDENTIAL VALUE FUND SERIES 8 - DIRECT PLAN DIVIDEND OPTION' },
  { symbol: 'IPRU2693.BO', name: 'ICICI PRUDENTIAL VALUE FUND SERIES 8 - REGULAR PLAN DIVIDEND OPTION' },
  { symbol: 'ORTINLAABS.BO', name: 'Ortin Laboratories Ltd' },
  { symbol: 'AVI.BO', name: 'AVI Polymers Ltd' },
  { symbol: 'MJCO.BO', name: 'Majesco Ltd' },
  { symbol: 'BINDALAGRO.BO', name: 'Oswal Greentech Ltd' },
  { symbol: 'OZONEWORLD.BO', name: 'Ozone World Ltd' },
  { symbol: 'AKSPINTEX.BO', name: 'A.K. Spintex Ltd' },
  { symbol: 'ARVINFRA.BO', name: 'Arvind Infrastructure Ltd' },
  { symbol: 'MINDAFIN.BO', name: 'Minda Finance Ltd' },
  { symbol: 'KARTAVYA.BO', name: 'Kartavya Udyog Viniyog Ltd' },
  { symbol: 'OYEEEE.BO', name: 'Oyeeee Media Ltd' },
  { symbol: 'NFIL.BO', name: 'Nishtha Finance and Investment (India) Ltd' },
  { symbol: 'UTISENSETF.BO', name: 'UTI- SENSEX ETF' },
  { symbol: 'UTINIFTETF.BO', name: 'UTI NIFTY ETF' },
  { symbol: 'UNIAUTO.BO', name: 'Universal Autofoundry Ltd' },
  { symbol: 'IPRU8917.BO', name: 'ICICI PRUDENTIAL INDIA RECOVERY FUND SERIES 3 - DIRECT PLAN - CUMULATIVE OPTION' },
  { symbol: 'IPRU8910.BO', name: 'ICICI PRUDENTIAL INDIA RECOVERY FUND SERIES 3 - DIRECT PLAN - DIVIDEND PAYOUT' },
  { symbol: 'IPRU2715.BO', name: 'ICICI PRUDENTIAL INDIA RECOVERY FUND SERIES 3 - REGULAR PLAN - CUMULATIVE' },
  { symbol: 'IPRU2708.BO', name: 'ICICI PRUDENTIAL INDIA RECOVERY FUND SERIES 3 - REGULAR PLAN - DIVIDEND OPTION' },
  { symbol: 'PENPEBS.BO', name: 'Pennar Engineered Building Systems Ltd' },
  { symbol: 'CHPLIND.BO', name: 'CHPL Industries Ltd' },
  { symbol: 'GUJGAS.BO', name: 'Gujarat Gas Ltd' },
  { symbol: 'HKT.BO', name: 'H.K. Trade International Ltd' },
  { symbol: 'PHL.BO', name: 'Pneumatic Holdings Ltd' },
  { symbol: 'PRABHAT.BO', name: 'Prabhat Dairy Ltd' },
  { symbol: 'PBFL.BO', name: 'P. B. Films Ltd' },
  { symbol: 'SWARAJAUTO.BO', name: 'Swaraj Automotives Ltd' },
  { symbol: 'POLYSPIN.BO', name: 'Polyspin Exports Ltd' },
  { symbol: 'SHIVKRUPA.BO', name: 'Shivkrupa Machineries and Engineering Services Ltd' },
  { symbol: 'SKC.BO', name: 'Sri Krishna Constructions (India) Ltd' },
  { symbol: 'SML.BO', name: 'Soni Medicare Ltd' },
  { symbol: 'MINFY.BO', name: 'Mahaveer Infoway Ltd' },
  { symbol: 'KRISHNACAP.BO', name: 'Krishna Capital and Securities Ltd' },
  { symbol: 'ACME.BO', name: 'Acme Resources Ltd' },
  { symbol: 'SANGFROID.BO', name: 'Sang Froid Labs (India) Ltd' },
  { symbol: 'KCL.BO', name: 'Kabra Commercial Ltd' },
  { symbol: 'IPRU8923.BO', name: 'ICICI Prudential Mutual Fund- Direct Plan Cumulative Option' },
  { symbol: 'IPRU8924.BO', name: 'ICICI Prudential Mutual Fund- Direct Plan Dividend Option' },
  { symbol: 'IPRU2721.BO', name: 'ICICI Prudential Mutual Fund- Regular Plan Cumulative Option' },
  { symbol: 'IPRU2722.BO', name: 'ICICI Prudential Mutual Fund- Regular Plan Dividend Option' },
  { symbol: 'VISHALBL.BO', name: 'Vishal Bearings Ltd' },
  { symbol: 'PJL.BO', name: 'Patdiam Jewellery Ltd' },
  { symbol: 'VAL.BO', name: 'Vaksons Automobiles Ltd' },
  { symbol: 'CBCSL.BO', name: 'Cawasji Behramji Catering Services Ltd' },
  { symbol: 'DITCO.BO', name: 'Decorous Investment & Trading Co Ltd' },
  { symbol: 'SWAGTAM.BO', name: 'Swagtam Trading & Services Ltd' },
  { symbol: 'WELPLACE.BO', name: 'Welplace Portfolio and Financial Consultancy Services Ltd' },
  { symbol: 'KUBERJI.BO', name: 'Kuber Udyog Ltd' },
  { symbol: 'NEERAJ.BO', name: 'Neeraj Paper Marketing Ltd' },
  { symbol: 'SIENERGY.BO', name: 'Sinner Energy India Ltd' },
  { symbol: 'BSLFEFS6RG.BO', name: 'BIRLA SUN LIFE FOCUSED EQUITY FUND- SERIES 6- REGULAR PLAN- GROWTH' },
  { symbol: 'BSLFEFS6RN.BO', name: 'BIRLA SUN LIFE FOCUSED EQUITY FUND- SERIES 6- REGULAR PLAN- DIVIDEND PAYOUT' },
  { symbol: 'BSLFEFS6DG.BO', name: 'BIRLA SUN LIFE FOCUSED EQUITY FUND- SERIES 6- DIRECT PLAN- GROWTH' },
  { symbol: 'BSLFEFS6DN.BO', name: 'BIRLA SUN LIFE FOCUSED EQUITY FUND- SERIES 6- DIRECT PLAN- DIVIDEND PAYOUT' },
  { symbol: 'TEJNAKSH.BO', name: 'Tejnaksh Healthcare Ltd' },
  { symbol: 'INDOGLOBAL.BO', name: 'Indo-Global Enterprises Ltd' },
  { symbol: 'BRIPORT.BO', name: 'Brilliant Portfolios Ltd' },
  { symbol: 'RFSL.BO', name: 'Richfield Financial Services Ltd' },
  { symbol: 'IDFCBANK.BO', name: 'IDFC Bank Ltd' },
  { symbol: 'KRISHFAB.BO', name: 'Krishana Fabrics Ltd' },
  { symbol: 'IGC.BO', name: 'IGC Foils Ltd' },
  { symbol: 'DASL.BO', name: 'Deepti Alloy Steel Ltd' },
  { symbol: 'IPRU8937.BO', name: 'ICICI PRUDENTIAL BUSINESS CYCLE FUND SERIES 2 DIRECT PLAN CUMULATIVE OPTION' },
  { symbol: 'IPRU8938.BO', name: 'ICICI PRUDENTIAL BUSINESS CYCLE FUND SERIES 2 DIRECT PLAN DIVIDEND OPTION' },
  { symbol: 'IPRU2735.BO', name: 'ICICI PRUDENTIAL BUSINESS CYCLE FUND SERIES 2 REGULAR PLAN CUMULATIVE OPTION' },
  { symbol: 'IPRU2736.BO', name: 'ICICI PRUDENTIAL BUSINESS CYCLE FUND SERIES 2 REGULAR PLAN DIVIDEND OPTION' },
  { symbol: 'TFSL.BO', name: 'Typhoon Financial Services Ltd' },
  { symbol: 'APUNKA.BO', name: 'Apunka Invest Commercial Ltd' },
  { symbol: 'SHREEGANES.BO', name: 'Shree Ganesh Biotech (India) Ltd' },
  { symbol: 'GTV.BO', name: 'GTV Engineering Ltd' },
  { symbol: 'LICNETFN50.BO', name: 'LIC NOMURA MF EXCHANGE TRADED FUND- NIFTY 50- GROWTH PLAN- GROWTH' },
  { symbol: 'PARIKSHA.BO', name: 'Pariksha Fin- Invest- Lease Ltd' },
  { symbol: 'GEETANJ.BO', name: 'Geetanjali Credit and Capital Ltd' },
  { symbol: 'LICNETFSEN.BO', name: 'LIC NOMURA MF EXCHANGE TRADED FUND - SENSEX' },
  { symbol: 'TFLL.BO', name: 'Tirupati Finlease Ltd' },
  { symbol: 'GOLDENPROP.BO', name: 'Golden Properties & Traders Ltd' },
  { symbol: 'ADHARSHILA.BO', name: 'Adharshila Capital Services Ltd' },
  { symbol: 'SMARTFIN.BO', name: 'Smart Finsec Ltd' },
  { symbol: 'RAJKOTINV.BO', name: 'Rajkot Investment Trust Ltd' },
  { symbol: 'ADCON.BO', name: 'Adcon Capital Services Ltd' },
  { symbol: 'GMLM.BO', name: 'Gaurav Mercantiles Ltd' },
  { symbol: 'HDFCNIFETF.BO', name: 'HDFC Mutual Fund' },
  { symbol: 'SXETF.BO', name: 'HDFC Sensex ETF - Open Ended Traded Fund' },
  { symbol: 'UDAYJEW.BO', name: 'Uday Jewellery Industries Ltd' },
  { symbol: 'MAYUKH.BO', name: 'Mayukh Dealtrade Ltd' },
  { symbol: 'SHAILJA.BO', name: 'Shailja Commercial Trade Frenzy Ltd' },
  { symbol: 'NAVIGANT.BO', name: 'Navigant Corporate Advisors Ltd' },
  { symbol: 'GROVY.BO', name: 'Grovy India Ltd' },
  { symbol: 'NAVKETAN.BO', name: 'Navketan Merchants Ltd' },
  { symbol: 'SCTL.BO', name: 'Suncare Traders Ltd' },
  { symbol: 'ISENSEX.BO', name: 'ICICI Prudential SPIcE Fund' },
  { symbol: 'TATAMTRDVR.BO', name: 'Tata Motors  Ltd - DVR' },
  { symbol: 'FRLDVR.BO', name: 'FUTURE RETAIL LTD.' },
  { symbol: 'GUJNREDVR.BO', name: 'Gujarat NRE Coke  Ltd' },
  { symbol: 'STAN.BO', name: 'STANDARD CHARTERED PLC' },
  { symbol: 'TIDEWATER.BO', name: 'TIDE WATER OIL (INDIA) LTD.' },
  { symbol: 'MOVINGPI.BO', name: 'MOVING PICTURE COMPANY (INDIA) LTD.' },
  { symbol: 'HISARMET.BO', name: 'HISAR METAL INDUSTRIES LTD.-$' },
  { symbol: 'NICCO.BO', name: 'NICCO CORPORATION LTD.' },
  { symbol: 'TIRFOAM.BO', name: 'TIRUPATI FOAM LTD.-$' },
  { symbol: 'VISUINTL.BO', name: 'VISU INTERNATIONAL LTD.-$' },
  { symbol: 'KAVVERITEL.BO', name: 'KAVVERI TELECOM PRODUCTS LTD.-$' },
  { symbol: 'HARITASEAT.BO', name: 'HARITA SEATING SYSTEMS LTD.-$' },
  { symbol: 'SMRUTHI.BO', name: 'SMRUTHI ORGANICS LTD.-$' },
  { symbol: 'SALONACOT.BO', name: 'SALONA COTSPIN LTD.' },
  { symbol: 'GREENFIRE.BO', name: 'GREEN FIRE AGRI COMMODITIES LTD.' },
  { symbol: 'BRUSHMAN.BO', name: 'BRUSHMAN (INDIA) LTD.-$' },
  { symbol: 'DUNCANSLTD.BO', name: 'DUNCANS INDUSTRIES LTD.' },
  { symbol: 'EASTERNGAS.BO', name: 'EASTERN GASES LTD.' },
  { symbol: 'BRAHMANAN.BO', name: 'BRAHMANAND HIMGHAR LTD.' },
  { symbol: 'LOHIASEC.BO', name: 'LOHIA SECURITIES LTD.' },
  { symbol: 'MAVENSBIO.BO', name: 'MAVENS BIOTECH LTD.' },
  { symbol: 'KANCOENT.BO', name: 'KANCO ENTERPRISES LTD.' },
  { symbol: 'ELLENBARR.BO', name: 'ELLENBARRIE INDUSTRIAL GASES LTD.' },
  { symbol: 'ADINATHBI.BO', name: 'ADINATH BIO-LABS LTD.' },
  { symbol: 'SWAGRO.BO', name: 'Swarnajyothi Agrotech & Power Limited-$' },
  { symbol: 'TRINETHRA.BO', name: 'TRINETHRA INFRA VENTURES LTD.-$' },
  { symbol: 'FARMAXIND.BO', name: 'Farmax India Limited-$' },
  { symbol: 'NIFTYBEES.BO', name: 'Goldman Sachs Nifty Exchange Traded Scheme' },
  { symbol: 'JUNIORBEES.BO', name: 'Goldman Sachs Nifty Junior Exchange Traded Scheme' },
  { symbol: 'BANKBEES.BO', name: 'Goldman Sachs Banking Index Exchange Traded Scheme' },
  { symbol: 'KOTAKPSUBK.BO', name: 'KOTAK MAHINDRA MUTUAL FUND - KOTAK PSU BANK ETF' },
  { symbol: 'PSUBNKBEES.BO', name: 'Goldman Sachs PSU Bank Exchange Traded Scheme' },
  { symbol: 'SHARIABEES.BO', name: 'Goldman Sachs S&P CNX Nifty Shariah Index Exchange Traded Scheme' },
  { symbol: 'QNIFTY.BO', name: 'QUANTUM MUTUAL FUND - QUANTUM INDEX FUND ETF' },
  { symbol: 'VAISHNAVI.BO', name: 'Vaishnavi Gold Limited-$' },
  { symbol: 'M50.BO', name: 'MOTILAL OSWAL MUTUAL FUND' },
  { symbol: '7SEAS.BO', name: '7Seas Technologies Ltd-$' },
  { symbol: 'PROVEST.BO', name: 'PROVESTMENT SERVICES LTD.' },
  { symbol: 'ASHIKACR.BO', name: 'ASHIKA CREDIT CAPITAL LTD.' },
  { symbol: 'RUNEECHA.BO', name: 'RUNEECHA TEXTILES LTD.' },
  { symbol: 'SHREETULSI.BO', name: 'SHREE TULSI ONLINE.COM LTD.' },
  { symbol: 'KANCOTEA.BO', name: 'KANCO TEA & INDUSTRIES LTD.' },
  { symbol: 'JAYMAHESH.BO', name: 'JAY MAHESH INFRAVENTURES LTD.' },
  { symbol: 'GENERAAGRI.BO', name: 'GENERA AGRI CORP LTD.' },
  { symbol: 'JDL.BO', name: 'Jaisukh Dealers Ltd' },
  { symbol: 'GSL.BO', name: 'Gracious Software Ltd' },
  { symbol: 'KKIL.BO', name: 'Kanak Krishi Implements Ltd' },
  { symbol: 'AUTUMN.BO', name: 'Autumn Builders Ltd' },
  { symbol: 'LEAP.BO', name: 'Learning Edge Academy of Professionals Ltd' },
  { symbol: 'DEKSON.BO', name: 'Dekson Castings Ltd' },
  { symbol: 'PCPL.BO', name: 'Premier Chennai Properties Ltd' },
  { symbol: 'SUPERNOVA.BO', name: 'Supernova Advertising Ltd' },
  { symbol: 'KDTWL.BO', name: 'K D Trend Wear Ltd' },
  { symbol: 'RICHWAY.BO', name: 'Richway International Trade Ltd' },
  { symbol: 'GOKULSOL.BO', name: 'Gokul Solutions Ltd' },
  { symbol: 'CITYON.BO', name: 'Cityon Systems (India) Ltd' },
  { symbol: 'HASJUICE.BO', name: 'Has Lifestyle Ltd' },
  { symbol: 'SANASATECH.BO', name: 'Sanasa Tech Feb Ltd' },
  { symbol: 'WEBSL.BO', name: 'Web Element Solutions Ltd' },
  { symbol: 'PSAL.BO', name: 'Parnav Sports Academy Ltd' },
  { symbol: 'ADHIRAJ.BO', name: 'Adhiraj Distributors Ltd' },
  { symbol: 'JIGYASA.BO', name: 'Jigyasa Infrastructure Ltd' },
  { symbol: 'PRITIKAST.BO', name: 'Pritika Autocast Ltd' },
  { symbol: 'LEGACY.BO', name: 'Legacy Mercantile Ltd' },
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
    if (count < 5000) {
      console.log(`Stock collection for default watchlist has ${count} stocks. Seeding full stock list...`);
      await Stock.deleteMany({ watchlist: 'default' });
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
const stockPrices = {};

/* ── WebSocket Setup & Simulator Loop ──────────────────────── */
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

wss.on('connection', (ws) => {
  console.log('Client connected to WebSocket stream');
    
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

    // Check matches for Alerts
    await checkAlertsForSymbol(symbol, data.price);
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

        // Persist in-app notification for this user
        try {
          if (alert.userId) {
            const condLabel = alert.condition.replace(/_/g, ' ');
            await new Notification({
              userId: alert.userId,
              title: `Alert Triggered: ${alert.symbol.split('.')[0]}`,
              message: `${alert.symbol} ${condLabel} ₹${alert.value} — triggered at ₹${currentPrice.toFixed(2)}`,
              type: 'alert',
              metadata: {
                alertId: alert._id,
                symbol: alert.symbol,
                condition: alert.condition,
                targetValue: alert.value,
                triggeredPrice: currentPrice
              }
            }).save();
          }
        } catch (notifErr) {
          console.error('Failed to persist alert notification:', notifErr);
        }

        // Dispatch webhook if delivery is webhook
        if (alert.delivery === 'webhook') {
          try {
            const webhookUrl = process.env.ALERT_WEBHOOK_URL;
            if (webhookUrl) {
              fetch(webhookUrl, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                  event: 'alert_triggered',
                  symbol: alert.symbol,
                  condition: alert.condition,
                  value: alert.value,
                  currentPrice,
                  triggeredAt: alert.triggeredAt
                })
              }).catch(e => console.error('Webhook dispatch failed:', e));
            }
          } catch (whErr) {
            console.error('Webhook dispatch error:', whErr);
          }
        }

        // Log email delivery placeholder (implement SMTP transport when credentials are configured)
        if (alert.delivery === 'email') {
          console.log(`[AlertEmail] Would send email for ${alert.symbol} ${alert.condition} ₹${alert.value} to userId=${alert.userId}`);
        }

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



/* ── API Routes ────────────────────────────────────────────── */



/**
 * @openapi
 * /api/watchlists:
 *   get:
 *     summary: Retrieve user watchlists
 *     description: Fetches a list of all watchlists owned by the authenticated user, or pre-seeded default watchlists if guest/unauthenticated.
 *     responses:
 *       200:
 *         description: A list of watchlists.
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               items:
 *                 $ref: '#/components/schemas/Watchlist'
 */
// GET /api/watchlists - Fetch all watchlists (Secure User-Bound & Guest fallback)
app.get('/api/watchlists', parseUserMiddleware, async (req, res) => {
  try {
    const filter = {};
    if (req.user) {
      filter.userId = req.user._id;
    } else {
      filter.userId = null; // Guests get standard predefined lists
    }

    let lists = await Watchlist.find(filter).sort({ isDefault: -1, createdAt: 1 });
    
    // Auto-create default watchlist for logged-in user if empty
    if (lists.length === 0 && req.user) {
      const def = new Watchlist({ userId: req.user._id, name: 'default', isDefault: true });
      await def.save();
      lists = [def];
    }
    
    res.json(lists);
  } catch (_err) {
    res.status(500).json({ error: 'Failed to retrieve watchlists' });
  }
});

// POST /api/watchlists - Create a custom watchlist (Secure User-Bound)
app.post('/api/watchlists', authMiddleware, async (req, res) => {
  try {
    const { name } = req.body;
    if (!name || !name.trim()) {
      return res.status(400).json({ error: 'Watchlist name is required' });
    }
    const cleanName = name.trim();
    const escapedName = cleanName.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const existing = await Watchlist.findOne({ 
      name: { $regex: new RegExp(`^${escapedName}$`, 'i') },
      userId: req.user._id 
    });
    if (existing) {
      return res.status(400).json({ error: 'A watchlist with this name already exists' });
    }

    const wl = new Watchlist({ userId: req.user._id, name: cleanName, isDefault: false });
    await wl.save();
    res.status(201).json(wl);
  } catch (err) {
    res.status(500).json({ error: 'Failed to create watchlist', details: err.message });
  }
});

// DELETE /api/watchlists/:name - Delete a custom watchlist (Secure User-Bound)
app.delete('/api/watchlists/:name', authMiddleware, async (req, res) => {
  try {
    const nameParam = req.params.name.trim();
    const wl = await Watchlist.findOne({ name: nameParam, userId: req.user._id });
    if (!wl) return res.status(404).json({ error: 'Watchlist not found' });
    
    if (wl.isDefault || wl.name.toLowerCase() === 'default') {
      return res.status(400).json({ error: 'The default watchlist cannot be deleted' });
    }

    // Delete all associated stocks for this user
    await Stock.deleteMany({ watchlist: wl.name, userId: req.user._id });
    await Watchlist.findByIdAndDelete(wl._id);
    
    res.json({ message: `Watchlist '${wl.name}' successfully deleted` });
  } catch (err) {
    res.status(500).json({ error: 'Failed to delete watchlist', details: err.message });
  }
});

// PUT /api/watchlists/:name - Rename a watchlist (Secure User-Bound)
app.put('/api/watchlists/:name', authMiddleware, async (req, res) => {
  try {
    const nameParam = req.params.name.trim();
    const { name: newName } = req.body;
    if (!newName || !newName.trim()) {
      return res.status(400).json({ error: 'New watchlist name is required' });
    }
    const cleanNew = newName.trim();

    const wl = await Watchlist.findOne({ name: nameParam, userId: req.user._id });
    if (!wl) return res.status(404).json({ error: 'Watchlist not found' });

    if (wl.isDefault || wl.name.toLowerCase() === 'default') {
      return res.status(400).json({ error: 'The default watchlist cannot be renamed' });
    }

    // Check for duplicates (case-insensitive)
    const escapedNew = cleanNew.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    const dup = await Watchlist.findOne({ 
      name: { $regex: new RegExp(`^${escapedNew}$`, 'i') },
      userId: req.user._id 
    });
    if (dup && dup._id.toString() !== wl._id.toString()) {
      return res.status(400).json({ error: 'A watchlist with this name already exists' });
    }

    const oldName = wl.name;
    wl.name = cleanNew;
    await wl.save();

    // Cascade rename to all stocks in this watchlist for this user
    await Stock.updateMany({ watchlist: oldName, userId: req.user._id }, { $set: { watchlist: cleanNew } });

    res.json(wl);
  } catch (err) {
    res.status(500).json({ error: 'Failed to rename watchlist', details: err.message });
  }
});

// GET /api/stocks/search - Server-side paginated stock search
app.get('/api/stocks/search', async (req, res) => {
  try {
    const { q, watchlist, page = '1', limit = '50' } = req.query;
    const wlName = (watchlist || 'default').trim();
    const pageNum = Math.max(1, parseInt(page, 10) || 1);
    const limitNum = Math.min(100, Math.max(1, parseInt(limit, 10) || 50));
    const skip = (pageNum - 1) * limitNum;

    let filter = { watchlist: wlName };
    if (q && q.trim()) {
      const escaped = q.trim().replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
      filter.$or = [
        { symbol: { $regex: escaped, $options: 'i' } },
        { name: { $regex: escaped, $options: 'i' } }
      ];
    }

    const [stocks, total] = await Promise.all([
      Stock.find(filter).sort({ updatedAt: -1 }).skip(skip).limit(limitNum).lean(),
      Stock.countDocuments(filter)
    ]);

    res.json({
      stocks,
      total,
      page: pageNum,
      totalPages: Math.ceil(total / limitNum)
    });
  } catch (err) {
    res.status(500).json({ error: 'Search failed', details: err.message });
  }
});





// GET /api/stocks - Fetch all stocks for a specific watchlist (Secure & Guest support)
app.get('/api/stocks', parseUserMiddleware, async (req, res) => {
  try {
    const watchlistName = req.query.watchlist || 'default';
    const filter = { watchlist: watchlistName };
    if (req.user) {
      filter.userId = req.user._id;
    } else {
      filter.userId = null; // Guests get standard seeds
    }
    const stocks = await Stock.find(filter).sort({ updatedAt: -1 });
    res.json(stocks);
  } catch (_error) {
    res.status(500).json({ error: 'Failed to retrieve stocks from database' });
  }
});

// POST /api/stocks - Add a new stock to a specific watchlist (Secure User-Bound)
app.post('/api/stocks', authMiddleware, async (req, res) => {
  try {
    let { symbol, name, isFavourite, isfavoute, watchlist } = req.body;
    if (!symbol) return res.status(400).json({ error: 'Stock symbol is required' });
    if (!name) return res.status(400).json({ error: 'Stock name is required' });

    const formattedSymbol = symbol.trim().toUpperCase();
    const formattedName = name.trim();
    const wlName = (watchlist || 'default').trim();
    const favStatus = isFavourite !== undefined ? isFavourite : (isfavoute !== undefined ? isfavoute : false);

    await registerSymbolInSimulator(formattedSymbol);

    let existingStock = await Stock.findOne({ 
      symbol: formattedSymbol, 
      watchlist: wlName, 
      userId: req.user._id 
    });
    if (existingStock) {
      existingStock.name = formattedName;
      existingStock.isFavourite = favStatus;
      await existingStock.save();
      return res.status(200).json(existingStock);
    }

    const newStock = new Stock({
      userId: req.user._id,
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

function scheduleSimulatorSync() { /* no-op — symbols registered on-demand */ }

// PATCH /api/stocks/:symbol - Update favourite or tags (Secure User-Bound)
app.patch('/api/stocks/:symbol', authMiddleware, async (req, res) => {
  try {
    const symbolParam = req.params.symbol.trim().toUpperCase();
    const wlName = (req.body.watchlist || req.query.watchlist || 'default').trim();
    const { isFavourite, isfavoute, tags } = req.body;

    const stock = await Stock.findOne({ symbol: symbolParam, watchlist: wlName, userId: req.user._id });
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

// DELETE /api/stocks/:symbol - Delete stock and its drawings (Secure User-Bound)
app.delete('/api/stocks/:symbol', authMiddleware, async (req, res) => {
  try {
    const symbolParam = req.params.symbol.trim().toUpperCase();
    const wlName = (req.query.watchlist || req.body.watchlist || 'default').trim();
    
    const result = await Stock.findOneAndDelete({ symbol: symbolParam, watchlist: wlName, userId: req.user._id });
    if (!result) return res.status(404).json({ error: `Stock not found` });

    // Clean up drawings for this stock for this user only
    await Drawing.deleteMany({ symbol: symbolParam, userId: req.user._id });
    res.json({ message: `Stock ${symbolParam} deleted successfully`, deletedStock: result });
  } catch (_error) {
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
  } catch (_err) {
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
  } catch (_err) {
    res.status(500).json({ error: 'Failed to update custom tag' });
  }
});

/* ── Watchlist Enhancements: Bulk Operations & Analytics ── */

// POST /api/watchlists/bulk-add - Add multiple stocks to a watchlist
app.post('/api/watchlists/bulk-add', authMiddleware, async (req, res) => {
  try {
    const { watchlist, stocks } = req.body;
    if (!watchlist || !Array.isArray(stocks)) {
      return res.status(400).json({ error: 'Watchlist and stocks list are required' });
    }
    const wlName = watchlist.trim();
    const added = [];

    for (const s of stocks) {
      if (!s.symbol || !s.name) continue;
      const formattedSymbol = s.symbol.trim().toUpperCase();
      const formattedName = s.name.trim();
      
      await registerSymbolInSimulator(formattedSymbol);

      // Duplicate prevention
      const exists = await Stock.findOne({
        symbol: formattedSymbol,
        watchlist: wlName,
        userId: req.user._id
      });
      if (exists) continue;

      const newStock = new Stock({
        userId: req.user._id,
        symbol: formattedSymbol,
        name: formattedName,
        watchlist: wlName
      });
      await newStock.save();
      added.push(newStock);
    }
    res.status(201).json({ message: `Successfully imported ${added.length} stocks`, added });
  } catch (err) {
    res.status(500).json({ error: 'Bulk import failed', details: err.message });
  }
});

// POST /api/watchlists/bulk-remove - Remove multiple stocks from a watchlist
app.post('/api/watchlists/bulk-remove', authMiddleware, async (req, res) => {
  try {
    const { watchlist, symbols } = req.body;
    if (!watchlist || !Array.isArray(symbols)) {
      return res.status(400).json({ error: 'Watchlist and symbols list are required' });
    }
    const wlName = watchlist.trim();
    const result = await Stock.deleteMany({
      watchlist: wlName,
      symbol: { $in: symbols.map(s => s.trim().toUpperCase()) },
      userId: req.user._id
    });
    res.json({ message: `Bulk removed ${result.deletedCount} stocks successfully` });
  } catch (err) {
    res.status(500).json({ error: 'Bulk deletion failed', details: err.message });
  }
});

// POST /api/watchlists/:name/clone - Clone a watchlist with all its stocks
app.post('/api/watchlists/:name/clone', authMiddleware, async (req, res) => {
  try {
    const srcName = req.params.name.trim();
    const { targetName } = req.body;
    if (!targetName || !targetName.trim()) {
      return res.status(400).json({ error: 'Target watchlist name is required' });
    }
    const cleanTarget = targetName.trim();

    // Check target exists
    const exists = await Watchlist.findOne({ name: cleanTarget, userId: req.user._id });
    if (exists) return res.status(400).json({ error: 'A watchlist with target name already exists' });

    // Create target watchlist
    const newWl = new Watchlist({ userId: req.user._id, name: cleanTarget });
    await newWl.save();

    // Copy stocks
    const srcStocks = await Stock.find({ watchlist: srcName, userId: req.user._id });
    const copies = srcStocks.map(s => ({
      userId: req.user._id,
      symbol: s.symbol,
      name: s.name,
      watchlist: cleanTarget,
      isFavourite: s.isFavourite,
      tags: s.tags
    }));
    if (copies.length > 0) {
      await Stock.insertMany(copies);
    }
    res.status(201).json({ watchlist: newWl, clonedCount: copies.length });
  } catch (err) {
    res.status(500).json({ error: 'Cloning watchlist failed', details: err.message });
  }
});

// GET /api/watchlists/:name/analytics - Calculate performance returns & leaders
app.get('/api/watchlists/:name/analytics', authMiddleware, async (req, res) => {
  try {
    const wlName = req.params.name.trim();
    const stocks = await Stock.find({ watchlist: wlName, userId: req.user._id });
    if (stocks.length === 0) {
      return res.json({ dailyReturn: 0, topGainer: null, topLoser: null, stocksCount: 0 });
    }

    // Load live stock quotes to calculate weights/returns
    const symbolsList = stocks.map(s => s.symbol);
    const { yahooFinance, YAHOO_MODULE_OPTS } = require('./lib/yahoo-finance');
    
    let quotes = [];
    try {
      quotes = await yahooFinance.quote(symbolsList, {}, YAHOO_MODULE_OPTS);
    } catch {
      // Fallback
    }
    if (!Array.isArray(quotes)) quotes = quotes ? [quotes] : [];

    let totalChangePercent = 0;
    let topGainer = null;
    let topLoser = null;

    stocks.forEach(s => {
      const q = quotes.find(q => q.symbol.toUpperCase() === s.symbol.toUpperCase());
      const changePct = q?.regularMarketChangePercent ?? 0;
      totalChangePercent += changePct;

      if (!topGainer || changePct > topGainer.changePercent) {
        topGainer = { symbol: s.symbol, name: s.name, changePercent: changePct, price: q?.regularMarketPrice ?? 0 };
      }
      if (!topLoser || changePct < topLoser.changePercent) {
        topLoser = { symbol: s.symbol, name: s.name, changePercent: changePct, price: q?.regularMarketPrice ?? 0 };
      }
    });

    res.json({
      dailyReturn: totalChangePercent / stocks.length,
      topGainer,
      topLoser,
      stocksCount: stocks.length
    });
  } catch (_err) {
    res.status(500).json({ error: 'Watchlist analytics processing failed' });
  }
});





/* ── Drawings API Routes (Secure User-Bound) ────────────────── */
app.get('/api/drawings', parseUserMiddleware, async (req, res) => {
  try {
    const symbolParam = (req.query.symbol || '').trim().toUpperCase();
    const chartMode = (req.query.chartMode || 'price').trim();
    if (!symbolParam) {
      return res.status(400).json({ error: 'Symbol is required' });
    }

    // Filter by active user if logged in, or fall back to guest drawings (userId: null)
    const filter = { symbol: symbolParam, chartMode };
    if (req.user) {
      filter.userId = req.user._id;
    } else {
      filter.userId = null;
    }

    const drawings = await Drawing.find(filter);
    res.json(drawings);
  } catch (error) {
    res.status(500).json({ error: 'Failed to retrieve drawings', details: error.message });
  }
});

// Secure sync endpoints for saving
const handleDrawingsSync = async (req, res) => {
  try {
    const symbolParam = (req.body.symbol || '').trim().toUpperCase();
    const chartMode = (req.body.chartMode || 'price').trim();
    const drawingsList = req.body.drawings || [];

    if (!symbolParam) {
      return res.status(400).json({ error: 'Symbol is required' });
    }

    // Overwrite previous drawings for THIS USER ONLY (or guest if not authenticated)
    const userId = req.user ? req.user._id : null;
    await Drawing.deleteMany({ symbol: symbolParam, chartMode, userId });

    if (drawingsList.length > 0) {
      const drawingsToSave = drawingsList.map((d) => ({
        userId,
        symbol: symbolParam,
        chartMode,
        type: d.type,
        points: d.points || [],
        price: d.price,
        time: d.time,
        color: d.color,
        config: d.config
      }));
      await Drawing.insertMany(drawingsToSave);
    }

    res.json({ message: 'Drawings synced successfully', count: drawingsList.length });
  } catch (error) {
    res.status(500).json({ error: 'Failed to sync drawings', details: error.message });
  }
};

app.post('/api/drawings', parseUserMiddleware, handleDrawingsSync);
app.post('/api/drawings/sync', parseUserMiddleware, handleDrawingsSync);



/* ── Alert System API Endpoints (Secure User-Bound) ────────── */
app.get('/api/alerts', authMiddleware, async (req, res) => {
  try {
    const list = await Alert.find({ userId: req.user._id }).sort({ createdAt: -1 });
    res.json(list);
  } catch (_err) {
    res.status(500).json({ error: 'Failed to load alerts' });
  }
});

app.post('/api/alerts', authMiddleware, async (req, res) => {
  try {
    const { symbol, condition, value, delivery } = req.body;
    if (!symbol || !condition || value === undefined) {
      return res.status(400).json({ error: 'Symbol, condition, and price level are required' });
    }
    const newAlert = new Alert({
      userId: req.user._id,
      symbol: symbol.trim().toUpperCase(),
      condition,
      value: parseFloat(value),
      delivery: delivery || 'in_app'
    });
    await newAlert.save();
    res.status(201).json(newAlert);
  } catch (_err) {
    res.status(500).json({ error: 'Failed to create alert' });
  }
});

app.delete('/api/alerts/:id', authMiddleware, async (req, res) => {
  try {
    const { id } = req.params;
    const deleted = await Alert.findOneAndDelete({ _id: id, userId: req.user._id });
    if (!deleted) {
      return res.status(404).json({ error: 'Alert rule not found or unauthorized' });
    }
    res.json({ message: 'Alert deleted successfully' });
  } catch (_err) {
    res.status(500).json({ error: 'Failed to delete alert' });
  }
});

/* ── Workspace Templates API Endpoints ─────────────────────── */

app.get('/api/workspace/layouts', async (req, res) => {
  try {
    const layouts = await WorkspaceLayout.find({}).sort({ updatedAt: -1 });
    res.json(layouts);
  } catch (_err) {
    res.status(500).json({ error: 'Failed to load workspace layouts' });
  }
});

app.get('/api/workspace/layouts/:name', async (req, res) => {
  try {
    const { name } = req.params;
    const item = await WorkspaceLayout.findOne({ name });
    if (!item) return res.status(404).json({ error: 'Layout not found' });
    res.json(item);
  } catch (_err) {
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
  } catch (_err) {
    res.status(500).json({ error: 'Failed to save workspace layout' });
  }
});

app.delete('/api/workspace/layouts/:name', async (req, res) => {
  try {
    const { name } = req.params;
    await WorkspaceLayout.findOneAndDelete({ name });
    res.json({ message: 'Workspace layout deleted successfully' });
  } catch (_err) {
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
  } catch (_err) {
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
      Math.max(1, parseInt(req.query.limit || '2000', 10))
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
    // Respond immediately — sync runs in the background to avoid holding the HTTP connection open
    // (a full sync can take 30–60+ minutes on cold start)
    res.status(202).json({ message: 'Screener sync started in background', force });
    runScreenerSync({ force }).catch((err) => {
      console.error('[ScreenerSync] Background sync failed:', err.message);
    });
  } catch (err) {
    res.status(500).json({ error: err.message || 'Screener sync failed' });
  }
});



/* ── Holdings & Live Portfolio Tracker (Secure User-Bound) ─── */
app.get('/api/holdings', authMiddleware, async (req, res) => {
  try {
    const list = await Holding.find({ userId: req.user._id }).lean();
    
    // Enrich with live price from Yahoo Finance
    const { yahooFinance, YAHOO_MODULE_OPTS } = require('./lib/yahoo-finance');
    const enriched = await Promise.all(
      list.map(async (h) => {
        let currentPrice = h.buyPrice;
        try {
          const q = await yahooFinance.quote(h.symbol, {}, YAHOO_MODULE_OPTS);
          if (q?.regularMarketPrice) {
            currentPrice = q.regularMarketPrice;
          }
        } catch { /* use buyPrice fallback */ }
        return {
          ...h,
          currentPrice,
        };
      })
    );
    res.json(enriched);
  } catch (_err) {
    res.status(500).json({ error: 'Failed to fetch holdings list' });
  }
});

app.post('/api/holdings', authMiddleware, async (req, res) => {
  try {
    const { symbol, name, buyPrice, quantity, purchaseDate, watchlist, transactionType, brokerageFees, standardTaxes } = req.body;
    if (!symbol || !name || !buyPrice || !quantity) {
      return res.status(400).json({ error: 'Required attributes: symbol, name, buyPrice, quantity' });
    }
    const item = new Holding({
      userId: req.user._id,
      symbol: symbol.trim().toUpperCase(),
      name: name.trim(),
      buyPrice: parseFloat(buyPrice),
      quantity: parseInt(quantity, 10),
      purchaseDate: purchaseDate ? new Date(purchaseDate) : undefined,
      watchlist: watchlist || 'default',
      transactionType: transactionType || 'buy',
      brokerageFees: parseFloat(brokerageFees || 0),
      standardTaxes: parseFloat(standardTaxes || 0)
    });
    await item.save();
    res.status(201).json(item);
  } catch (_err) {
    res.status(500).json({ error: 'Failed to record stock holding transaction' });
  }
});

app.delete('/api/holdings/:id', authMiddleware, async (req, res) => {
  try {
    const { id } = req.params;
    const deleted = await Holding.findOneAndDelete({ _id: id, userId: req.user._id });
    if (!deleted) {
      return res.status(404).json({ error: 'Holding transaction not found or unauthorized' });
    }
    res.json({ message: 'Holding transaction deleted successfully' });
  } catch (_err) {
    res.status(500).json({ error: 'Failed to delete holding transaction' });
  }
});

// POST /api/holdings/import-csv - Parse and import Zerodha trades in bulk
app.post('/api/holdings/import-csv', authMiddleware, async (req, res) => {
  try {
    const { csvData, watchlist } = req.body;
    if (!csvData) return res.status(400).json({ error: 'CSV text data is required' });
    const targetWl = watchlist || 'default';
    
    const lines = csvData.split('\n');
    let importedCount = 0;

    for (const line of lines.slice(1)) {
      const parts = line.split(',');
      if (parts.length < 4) continue;
      
      const symbol = parts[0]?.trim().toUpperCase();
      const buyPrice = parseFloat(parts[1]);
      const quantity = parseInt(parts[2], 10);
      const name = parts[3]?.trim() || symbol;

      if (!symbol || isNaN(buyPrice) || isNaN(quantity)) continue;

      const item = new Holding({
        userId: req.user._id,
        symbol,
        name,
        buyPrice,
        quantity,
        watchlist: targetWl,
        transactionType: 'buy'
      });
      await item.save();
      importedCount++;
    }

    res.json({ message: `Successfully imported ${importedCount} portfolio trades from CSV.` });
  } catch (_err) {
    res.status(500).json({ error: 'CSV file import transaction failed' });
  }
});

/* ── JWT Authentication & User Management Endpoints ────────── */
const User = require('./models/User');
const UserPreference = require('./models/UserPreference');
const { signJWT, verifyJWT } = require('./lib/jwt');
const JWT_SECRET = process.env.JWT_SECRET || 'vision-wealth-default-secret-key-321';

// Authentication verification middleware
async function authMiddleware(req, res, next) {
  try {
    const authHeader = req.headers.authorization;
    if (!authHeader || !authHeader.startsWith('Bearer ')) {
      return res.status(401).json({ error: 'Authentication token required' });
    }
    const token = authHeader.split(' ')[1];
    const payload = verifyJWT(token, JWT_SECRET);
    if (!payload || !payload.id) {
      return res.status(401).json({ error: 'Authentication token has expired or is invalid' });
    }
    const user = await User.findById(payload.id);
    if (!user) {
      return res.status(401).json({ error: 'User account no longer exists' });
    }
    req.user = user;
    next();
  } catch (err) {
    res.status(500).json({ error: 'Authentication processing failure', details: err.message });
  }
}

// Client guest parsing middleware (Optional auth)
async function parseUserMiddleware(req, res, next) {
  try {
    const authHeader = req.headers.authorization;
    if (authHeader && authHeader.startsWith('Bearer ')) {
      const token = authHeader.split(' ')[1];
      const payload = verifyJWT(token, JWT_SECRET);
      if (payload && payload.id) {
        const user = await User.findById(payload.id);
        if (user) {
          req.user = user;
        }
      }
    }
  } catch (_e) {}
  next();
}

// POST /api/auth/register - Register a new user
app.post('/api/auth/register', async (req, res) => {
  try {
    const { fullName, email, password } = req.body;
    if (!fullName || !email || !password) {
      return res.status(400).json({ error: 'All attributes (fullName, email, password) are required' });
    }

    const trimmedEmail = email.trim().toLowerCase();
    const existing = await User.findOne({ email: trimmedEmail });
    if (existing) {
      return res.status(400).json({ error: 'An account with this email already exists' });
    }

    // Hash the password with pbkdf2
    const { salt, hash } = User.hashPassword(password);
    
    // Generate verification token (simple hex string)
    const verificationToken = crypto.randomBytes(32).toString('hex');

    const newUser = new User({
      fullName: fullName.trim(),
      email: trimmedEmail,
      passwordHash: hash,
      salt,
      verificationToken,
      emailVerified: false,
    });

    await newUser.save();

    // Create default preference profile
    const prefs = new UserPreference({
      userId: newUser._id,
      theme: 'dark',
      timezone: 'Asia/Kolkata',
    });
    await prefs.save();

    // Sign active session JWT
    const token = signJWT({ id: newUser._id, email: newUser.email }, JWT_SECRET);

    res.status(201).json({
      message: 'Account registered successfully. Verification token generated.',
      token,
      user: {
        id: newUser._id,
        fullName: newUser.fullName,
        email: newUser.email,
        emailVerified: newUser.emailVerified,
        role: newUser.role,
      },
    });
  } catch (err) {
    res.status(500).json({ error: 'Registration failed', details: err.message });
  }
});

// POST /api/auth/login - User login with brute force lockout & session tracking
app.post('/api/auth/login', async (req, res) => {
  try {
    const { email, password } = req.body;
    if (!email || !password) {
      return res.status(400).json({ error: 'Email and password are required' });
    }

    const trimmedEmail = email.trim().toLowerCase();
    const user = await User.findOne({ email: trimmedEmail });
    
    if (!user) {
      return res.status(401).json({ error: 'Invalid email or password' });
    }

    // Check account lockout
    if (user.lockUntil && user.lockUntil > Date.now()) {
      const remainingMinutes = Math.ceil((user.lockUntil - Date.now()) / 60000);
      return res.status(403).json({ 
        error: `Account is temporarily locked out due to consecutive failed attempts. Try again in ${remainingMinutes} minute(s).` 
      });
    }

    // Verify password
    if (!user.validatePassword(password)) {
      user.loginAttempts = (user.loginAttempts || 0) + 1;
      if (user.loginAttempts >= 5) {
        user.lockUntil = Date.now() + 15 * 60 * 1000; // 15 mins lock
      }
      await user.save();
      return res.status(401).json({ error: 'Invalid email or password' });
    }

    // Reset attempts on successful authentication
    user.loginAttempts = 0;
    user.lockUntil = undefined;
    user.lastLogin = new Date();

    // Track user active device sessions
    const deviceInfo = req.headers['user-agent'] || 'Unknown Device';
    const ipAddress = req.headers['x-forwarded-for'] || req.socket.remoteAddress || '127.0.0.1';

    // Sign session JWT
    const token = signJWT({ id: user._id, email: user.email }, JWT_SECRET);

    // Limit sessions list to 5 concurrent devices
    let sessions = user.activeSessions || [];
    sessions = sessions.filter(s => s.token !== token); // unique token tracking
    sessions.push({ token, deviceInfo, ipAddress, lastActive: new Date() });
    if (sessions.length > 5) {
      sessions.shift();
    }
    user.activeSessions = sessions;
    await user.save();

    // Fetch user preferences
    let prefs = await UserPreference.findOne({ userId: user._id });
    if (!prefs) {
      prefs = new UserPreference({ userId: user._id });
      await prefs.save();
    }

    res.json({
      token,
      user: {
        id: user._id,
        fullName: user.fullName,
        email: user.email,
        emailVerified: user.emailVerified,
        role: user.role,
      },
      preferences: {
        theme: prefs.theme,
        timezone: prefs.timezone,
      },
    });
  } catch (err) {
    res.status(500).json({ error: 'Login failed', details: err.message });
  }
});

// GET /api/auth/me - Get current logged in user details
app.get('/api/auth/me', authMiddleware, async (req, res) => {
  try {
    const prefs = await UserPreference.findOne({ userId: req.user._id });
    res.json({
      user: {
        id: req.user._id,
        fullName: req.user.fullName,
        email: req.user.email,
        emailVerified: req.user.emailVerified,
        role: req.user.role,
        createdAt: req.user.createdAt,
      },
      preferences: prefs || { theme: 'dark', timezone: 'Asia/Kolkata' },
    });
  } catch (_err) {
    res.status(500).json({ error: 'Failed to retrieve profile details' });
  }
});

// POST /api/auth/forgot-password - Trigger reset email flow
app.post('/api/auth/forgot-password', async (req, res) => {
  try {
    const { email } = req.body;
    if (!email) return res.status(400).json({ error: 'Email is required' });

    const user = await User.findOne({ email: email.trim().toLowerCase() });
    if (!user) {
      // Avoid disclosing user existence (standard security practice)
      return res.json({ message: 'If this account exists, a reset link will be sent' });
    }

    const resetToken = crypto.randomBytes(32).toString('hex');
    user.resetPasswordToken = resetToken;
    user.resetPasswordExpires = Date.now() + 3600000; // 1 hour expiration
    await user.save();

    res.json({
      message: 'Reset link generated successfully',
      resetToken, // Return token directly for validation in sandbox without actual SMTP setup
    });
  } catch (_err) {
    res.status(500).json({ error: 'Failed to generate reset request' });
  }
});

// POST /api/auth/reset-password - Complete password reset
app.post('/api/auth/reset-password', async (req, res) => {
  try {
    const { resetToken, password } = req.body;
    if (!resetToken || !password) {
      return res.status(400).json({ error: 'Reset token and new password are required' });
    }

    const user = await User.findOne({
      resetPasswordToken: resetToken,
      resetPasswordExpires: { $gt: Date.now() },
    });

    if (!user) {
      return res.status(400).json({ error: 'Reset token is invalid or has expired' });
    }

    const { salt, hash } = User.hashPassword(password);
    user.passwordHash = hash;
    user.salt = salt;
    user.resetPasswordToken = undefined;
    user.resetPasswordExpires = undefined;
    await user.save();

    res.json({ message: 'Password reset successfully' });
  } catch (_err) {
    res.status(500).json({ error: 'Failed to reset password' });
  }
});

// POST /api/auth/verify-email - Complete verification token matching
app.post('/api/auth/verify-email', async (req, res) => {
  try {
    const { token } = req.body;
    if (!token) return res.status(400).json({ error: 'Verification token is required' });

    const user = await User.findOne({ verificationToken: token });
    if (!user) {
      return res.status(400).json({ error: 'Invalid verification token' });
    }

    user.emailVerified = true;
    user.verificationToken = undefined;
    await user.save();

    res.json({ message: 'Email address verified successfully' });
  } catch (_err) {
    res.status(500).json({ error: 'Email verification failed' });
  }
});

// PUT /api/auth/preferences - Update user preferences
app.put('/api/auth/preferences', authMiddleware, async (req, res) => {
  try {
    const { theme, timezone } = req.body;
    let prefs = await UserPreference.findOne({ userId: req.user._id });
    if (!prefs) {
      prefs = new UserPreference({ userId: req.user._id });
    }
    if (theme) prefs.theme = theme;
    if (timezone) prefs.timezone = timezone;
    await prefs.save();
    res.json({ message: 'Preferences updated successfully', preferences: prefs });
  } catch (_err) {
    res.status(500).json({ error: 'Failed to update preferences' });
  }
});

// PUT /api/auth/profile - Update profile details with password validation rules
app.put('/api/auth/profile', authMiddleware, async (req, res) => {
  try {
    const { fullName, password } = req.body;
    if (fullName) req.user.fullName = fullName.trim();
    if (password) {
      if (password.length < 8) {
        return res.status(400).json({ error: 'Password must be at least 8 characters long and include numbers/symbols.' });
      }
      const { salt, hash } = User.hashPassword(password);
      req.user.passwordHash = hash;
      req.user.salt = salt;
    }
    await req.user.save();
    res.json({ message: 'Profile updated successfully' });
  } catch (_err) {
    res.status(500).json({ error: 'Failed to update profile' });
  }
});

// GET /api/auth/sessions - Fetch active user device sessions
app.get('/api/auth/sessions', authMiddleware, async (req, res) => {
  try {
    const active = (req.user.activeSessions || []).map(s => ({
      deviceInfo: s.deviceInfo,
      ipAddress: s.ipAddress,
      lastActive: s.lastActive,
      isCurrent: req.headers.authorization?.split(' ')[1] === s.token
    }));
    res.json(active);
  } catch (_err) {
    res.status(500).json({ error: 'Failed to retrieve active sessions' });
  }
});

// POST /api/auth/logout-all - Logout from all other devices
app.post('/api/auth/logout-all', authMiddleware, async (req, res) => {
  try {
    const currentToken = req.headers.authorization?.split(' ')[1];
    req.user.activeSessions = (req.user.activeSessions || []).filter(s => s.token === currentToken);
    await req.user.save();
    res.json({ message: 'Logged out from all other active device sessions successfully.' });
  } catch (_err) {
    res.status(500).json({ error: 'Failed to perform session revocation' });
  }
});

// DELETE /api/auth/account - Delete own user account permanently
app.delete('/api/auth/account', authMiddleware, async (req, res) => {
  try {
    const userId = req.user._id;
    // Multi-partition cascaded deletion of all records associated with this userId
    const Stock = require('./models/Stock');
    const Watchlist = require('./models/Watchlist');
    const Drawing = require('./models/Drawing');
    const Alert = require('./models/Alert');
    const Holding = require('./models/Holding');

    await Promise.all([
      Stock.deleteMany({ userId }),
      Watchlist.deleteMany({ userId }),
      Drawing.deleteMany({ userId }),
      Alert.deleteMany({ userId }),
      Holding.deleteMany({ userId }),
      UserPreference.deleteOne({ userId }),
      User.deleteOne({ _id: userId })
    ]);

    res.json({ message: 'User account and all related portfolio configurations deleted permanently.' });
  } catch (_err) {
    res.status(500).json({ error: 'Account deletion transaction failed' });
  }
});

/* ── Notification Center API Endpoints (Secure User-Bound) ── */

// GET /api/notifications - Fetch all notifications for user
app.get('/api/notifications', authMiddleware, async (req, res) => {
  try {
    const limit = Math.min(100, Math.max(1, parseInt(req.query.limit || '50', 10)));
    const list = await Notification.find({ userId: req.user._id })
      .sort({ createdAt: -1 })
      .limit(limit)
      .lean();
    const unreadCount = await Notification.countDocuments({ userId: req.user._id, isRead: false });
    res.json({ notifications: list, unreadCount });
  } catch (_err) {
    res.status(500).json({ error: 'Failed to load notifications' });
  }
});

// PUT /api/notifications/:id/read - Mark a single notification as read
app.put('/api/notifications/:id/read', authMiddleware, async (req, res) => {
  try {
    const { id } = req.params;
    const notif = await Notification.findOneAndUpdate(
      { _id: id, userId: req.user._id },
      { isRead: true },
      { new: true }
    );
    if (!notif) return res.status(404).json({ error: 'Notification not found' });
    res.json(notif);
  } catch (_err) {
    res.status(500).json({ error: 'Failed to mark notification as read' });
  }
});

// PUT /api/notifications/read-all - Mark all notifications as read
app.put('/api/notifications/read-all', authMiddleware, async (req, res) => {
  try {
    await Notification.updateMany(
      { userId: req.user._id, isRead: false },
      { isRead: true }
    );
    res.json({ message: 'All notifications marked as read' });
  } catch (_err) {
    res.status(500).json({ error: 'Failed to mark all notifications as read' });
  }
});

// DELETE /api/notifications/:id - Delete a notification
app.delete('/api/notifications/:id', authMiddleware, async (req, res) => {
  try {
    const { id } = req.params;
    const deleted = await Notification.findOneAndDelete({ _id: id, userId: req.user._id });
    if (!deleted) return res.status(404).json({ error: 'Notification not found or unauthorized' });
    res.json({ message: 'Notification deleted successfully' });
  } catch (_err) {
    res.status(500).json({ error: 'Failed to delete notification' });
  }
});

/**
 * @openapi
 * /api/corporate-actions:
 *   get:
 *     summary: Get corporate actions feed
 *     description: Retrieve dynamic corporate actions (dividends, stock splits, bonus shares, and buybacks) based on the user's holdings and watchlists.
 *     responses:
 *       200:
 *         description: List of corporate actions
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               items:
 *                 $ref: '#/components/schemas/CorporateAction'
 */
// GET /api/corporate-actions - Retrieve dynamic corporate actions based on watchlist / holdings symbols
app.get('/api/corporate-actions', parseUserMiddleware, async (req, res) => {
  try {
    let symbols = [];
    if (req.user) {
      // Find all custom stock symbols from the user's watchlists and holdings
      const [watchlistStocks, holdings] = await Promise.all([
        Stock.find({ userId: req.user._id }).distinct('symbol'),
        Holding.find({ userId: req.user._id }).distinct('symbol')
      ]);
      symbols = Array.from(new Set([...watchlistStocks, ...holdings]));
    }
    
    // Add fallback/seed symbols if user has no stocks to keep feed populated
    if (symbols.length === 0) {
      symbols = ['20MICRONS.NS', 'RELIANCE.NS', 'INFY.NS', 'TCS.NS', 'HDFCBANK.NS'];
    }

    const corporateActions = [];
    symbols.forEach((sym, idx) => {
      const cleanSym = sym.trim().toUpperCase();
      const baseName = cleanSym.split('.')[0];
      const charSum = cleanSym.split('').reduce((sum, char) => sum + char.charCodeAt(0), 0);
      
      // Determine action type deterministically from the symbol name characters
      const types = ['dividend', 'split', 'bonus', 'buyback'];
      const actionType = types[charSum % 4];
      
      let ratioOrAmount = '₹2.50 per share';
      let description = `Corporate action event finalized by ${baseName} board.`;
      
      if (actionType === 'dividend') {
        const dividendAmount = (charSum % 45) + 1.5;
        ratioOrAmount = `₹${dividendAmount.toFixed(2)} per share`;
        description = `Final dividend payout recommended by the board of ${baseName} subject to shareholder AGM approval.`;
      } else if (actionType === 'split') {
        ratioOrAmount = '1:2 Stock Split';
        description = `Sub-division of equity shares of ${baseName} from face value of ₹2 to face value of ₹1 each to increase market liquidity.`;
      } else if (actionType === 'bonus') {
        ratioOrAmount = '1:1 Bonus Issue';
        description = `1 equity share will be issued free of cost by ${baseName} for every 1 fully paid equity share held as of the record date.`;
      } else if (actionType === 'buyback') {
        ratioOrAmount = 'Tender Offer Buyback';
        description = `${baseName} share buyback completed successfully via tender offer at premium price index.`;
      }

      // Generate realistic dynamic dates based on the symbol
      const exDateObj = new Date();
      exDateObj.setDate(exDateObj.getDate() + (charSum % 45) - 15);
      const exDateStr = exDateObj.toISOString().slice(0, 10);

      const recDateObj = new Date(exDateObj);
      recDateObj.setDate(recDateObj.getDate() + 2);
      const recDateStr = recDateObj.toISOString().slice(0, 10);

      const status = exDateObj > new Date() ? 'Upcoming' : 'Completed';

      corporateActions.push({
        id: `act-dyn-${idx}`,
        symbol: cleanSym,
        companyName: `${baseName} Limited`,
        type: actionType,
        ratioOrAmount,
        exDate: exDateStr,
        recordDate: recDateStr,
        status,
        description
      });
    });

    res.json(corporateActions);
  } catch (err) {
    res.status(500).json({ error: 'Failed to retrieve corporate actions list', details: err.message });
  }
});

// Error handling middleware
app.use((err, req, res, _next) => {
  console.error('Unhandled server error:', err);
  res.status(500).json({ error: 'Internal server error occurred' });
});

async function boot() {
  server.listen(PORT, () => {
    console.log(`Vision backend server is running on http://localhost:${PORT}`);
  });

  try {
    await initDatabase();
  } catch (err) {
    console.error('Database init failed:', err.message);
    console.warn('API running without MongoDB — screener endpoints will be empty until sync succeeds.');
  }
}

boot();
