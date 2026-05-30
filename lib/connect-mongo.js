const dns = require('node:dns');
const fs = require('fs');
const mongoose = require('mongoose');
const { spawnSync } = require('child_process');
const path = require('path');

// if (typeof dns.setDefaultResultOrder === 'function') {
//   dns.setDefaultResultOrder('ipv4first');
// }

const DEFAULT_OPTS = {
  maxPoolSize: 50,
  minPoolSize: 2,
  socketTimeoutMS: 45000,
  serverSelectionTimeoutMS: 20000,
  family: 4,
};

const LOCAL_URI =
  process.env.MONGODB_URI_LOCAL || 'mongodb://127.0.0.1:27017/vision_terminal';

let embeddedMongod = null;

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function tryConnect(uri, label) {
  if (!uri) return false;
  try {
    if (mongoose.connection.readyState !== 0) {
      await mongoose.disconnect();
    }
    await mongoose.connect(uri, DEFAULT_OPTS);
    console.log(`MongoDB connected (${label})`);
    return true;
  } catch (err) {
    console.warn(`MongoDB ${label} failed: ${err.message}`);
    try {
      if (mongoose.connection.readyState !== 0) await mongoose.disconnect();
    } catch {
      /* ignore */
    }
    return false;
  }
}

function dockerAvailable() {
  const r = spawnSync('docker', ['info'], { stdio: 'ignore', timeout: 15000 });
  return r.status === 0;
}

function startDockerMongo() {
  const composeDir = path.join(__dirname, '..');
  console.log('[Mongo] Starting local MongoDB via Docker…');
  const up = spawnSync('docker', ['compose', 'up', '-d', 'mongo'], {
    cwd: composeDir,
    stdio: 'inherit',
    timeout: 120000,
  });
  return up.status === 0;
}

async function waitForLocalMongo(maxAttempts = 30) {
  for (let i = 0; i < maxAttempts; i++) {
    if (await tryConnect(LOCAL_URI, 'local')) return true;
    await sleep(1000);
  }
  return false;
}

async function startEmbeddedMongo() {
  const { MongoMemoryServer } = require('mongodb-memory-server');
  const dbPath = path.join(__dirname, '..', '.mongo-data');
  fs.mkdirSync(dbPath, { recursive: true });
  embeddedMongod = await MongoMemoryServer.create({
    instance: { dbPath },
  });
  const uri = embeddedMongod.getUri('vision_terminal');
  return tryConnect(uri, 'embedded (local .mongo-data)');
}

/**
 * Connect to MongoDB for screener + API.
 * Order: MONGODB_URI → local/Docker → embedded (dev only, same process).
 */
async function connectMongo() {
  const atlasUri = process.env.MONGODB_URI;
  const directUri = process.env.MONGODB_URI_DIRECT;

  if (await tryConnect(atlasUri, 'Atlas (MONGODB_URI)')) return mongoose.connection;
  if (directUri && (await tryConnect(directUri, 'Atlas direct (MONGODB_URI_DIRECT)'))) {
    return mongoose.connection;
  }

  if (await tryConnect(LOCAL_URI, 'local')) return mongoose.connection;

  if (dockerAvailable() && startDockerMongo()) {
    if (await waitForLocalMongo()) return mongoose.connection;
  }

  const allowEmbedded =
    process.env.SCREENER_EMBEDDED_MONGO === 'true' ||
    (!process.env.MONGODB_URI && process.env.NODE_ENV !== 'production') ||
    (process.env.NODE_ENV !== 'production' && process.env.SCREENER_EMBEDDED_MONGO !== 'false');

  if (allowEmbedded) {
    try {
      if (await startEmbeddedMongo()) {
        console.warn(
          '[Mongo] Using local embedded DB at backend/.mongo-data — set MONGODB_URI for production/Atlas.'
        );
        return mongoose.connection;
      }
    } catch (err) {
      console.warn('[Mongo] Embedded fallback unavailable:', err.message);
    }
  }

  throw new Error(
    'Could not connect to MongoDB. Set MONGODB_URI in backend/.env, run "npm run db:up", or install MongoDB locally.'
  );
}

async function disconnectMongo() {
  if (mongoose.connection.readyState !== 0) {
    await mongoose.disconnect();
  }
  if (embeddedMongod) {
    await embeddedMongod.stop();
    embeddedMongod = null;
  }
}

module.exports = { connectMongo, disconnectMongo, LOCAL_URI };
