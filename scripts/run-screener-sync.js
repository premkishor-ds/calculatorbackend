/**
 * Sync all NSE+BSE stocks into MongoDB (today's snapshot).
 * Usage: npm run screener:sync:force
 */
require('dotenv').config({ path: require('path').join(__dirname, '..', '.env') });

const { connectMongo, disconnectMongo } = require('../lib/connect-mongo');
const { runScreenerSync } = require('../services/screener-sync');

const force = process.argv.includes('--force');

(async () => {
  try {
    await connectMongo();
    const result = await runScreenerSync({ force });
    console.log(JSON.stringify(result, null, 2));
    process.exit(result.skipped ? 0 : 0);
  } catch (err) {
    console.error('[screener:sync] Failed:', err.message || err);
    process.exit(1);
  } finally {
    await disconnectMongo().catch(() => {});
  }
})();
