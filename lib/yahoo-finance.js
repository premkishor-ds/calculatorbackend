const YahooFinance = require('yahoo-finance2').default;

const logger = {
  info: (...args) => console.log(...args),
  warn: (...args) => {
    const msg = String(args[0] ?? '');
    if (msg.includes('[yahoo-finance2] Unsupported environment')) return;
    console.warn(...args);
  },
  error: (...args) => console.error(...args),
  dir: (...args) => console.dir(...args),
  debug: () => undefined,
};

const yahooFinance = new YahooFinance({
  suppressNotices: ['yahooSurvey', 'ripHistorical'],
  logger,
});

/** Skip strict schema validation — many BSE tickers return partial quotes */
const YAHOO_OPTS = { validateResult: false };

module.exports = { yahooFinance, YAHOO_OPTS };
