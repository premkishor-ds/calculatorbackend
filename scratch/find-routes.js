const fs = require('fs');
const path = require('path');

const serverFile = path.join(__dirname, '../server.js');
const content = fs.readFileSync(serverFile, 'utf8');

const regex = /app\.(get|post|put|delete|patch)\(\s*['"]([^'"]+)['"]/g;
let match;
const routes = [];

while ((match = regex.exec(content)) !== null) {
  routes.push(`${match[1].toUpperCase()} ${match[2]}`);
}

console.log('--- FOUND ROUTES ---');
console.log(routes.join('\n'));
console.log('--------------------');
