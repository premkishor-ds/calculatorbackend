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

/** yahoo-finance2 v3: validateResult belongs in moduleOptions (3rd arg), not quote options (2nd). */
const YAHOO_MODULE_OPTS = { validateResult: false };

/** @deprecated Use YAHOO_MODULE_OPTS — kept for imports that expect YAHOO_OPTS */
const YAHOO_OPTS = YAHOO_MODULE_OPTS;

const QUOTE_ARRAY_OPTS = { return: 'array' };

function isYahooRateLimitError(err) {
  const msg = String(err?.message || err || '');
  return msg.includes('429') || msg.includes('Too Many Requests') || msg.includes('crumb');
}

module.exports = {
  yahooFinance,
  YAHOO_OPTS,
  YAHOO_MODULE_OPTS,
  QUOTE_ARRAY_OPTS,
  isYahooRateLimitError,
};
