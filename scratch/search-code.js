const fs = require('fs');
const path = require('path');

const file = path.join(__dirname, '../server.js');
const text = fs.readFileSync(file, 'utf8');

const query = 'yahooFinance';
let idx = 0;
let count = 0;

while ((idx = text.indexOf(query, idx)) !== -1) {
  const lineStart = text.lastIndexOf('\n', idx);
  const lineEnd = text.indexOf('\n', idx);
  const line = text.substring(lineStart + 1, lineEnd).trim();
  const lineNum = text.substring(0, idx).split('\n').length;
  console.log(`Line ${lineNum}: ${line}`);
  idx += query.length;
  count++;
  if (count > 20) break;
}
