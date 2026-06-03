const { execSync } = require('child_process');
const fs = require('fs');
const path = require('path');

function runLintAndFix(dir) {
  console.log(`Running ESLint on ${dir}...`);
  let output;
  try {
    output = execSync('npx eslint . --format=json', { cwd: dir, maxBuffer: 10 * 1024 * 1024 }).toString();
  } catch (error) {
    if (error.stdout) {
      output = error.stdout.toString();
    } else {
      console.error(`Failed to run eslint:`, error);
      return;
    }
  }

  let results;
  try {
    results = JSON.parse(output);
  } catch (e) {
    console.error('Failed to parse ESLint JSON output:', e);
    return;
  }

  // Group errors by file
  for (const fileResult of results) {
    const filePath = fileResult.filePath;
    if (!fs.existsSync(filePath)) continue;

    const messages = fileResult.messages.filter(m => 
      m.ruleId === 'no-unused-vars' || 
      m.ruleId === '@typescript-eslint/no-unused-vars' ||
      m.ruleId === 'unused-imports/no-unused-vars'
    );

    if (messages.length === 0) continue;

    console.log(`Fixing ${messages.length} unused vars in ${path.relative(dir, filePath)}...`);
    let content = fs.readFileSync(filePath, 'utf8');
    let lines = content.split('\r\n');
    let isCRLF = true;
    if (lines.length === 1) {
      lines = content.split('\n');
      isCRLF = false;
    }

    // Sort messages in descending order of line and column to avoid indexing shifts when replacing
    messages.sort((a, b) => {
      if (b.line !== a.line) return b.line - a.line;
      return b.column - a.column;
    });

    const fixedPositions = new Set();

    for (const msg of messages) {
      const lineIdx = msg.line - 1;
      const colIdx = msg.column - 1;
      const key = `${lineIdx}:${colIdx}`;
      if (fixedPositions.has(key)) continue;

      const line = lines[lineIdx];
      if (!line) continue;

      // Extract the variable name from the message
      // e.g. "'testSize' is assigned a value but never used"
      // or "'i' is defined but never used"
      const match = msg.message.match(/'([^']+)'/);
      if (!match) continue;
      const varName = match[1];

      // Verify that the variable name is at the specified column in the line
      // E.g., column is 1-indexed. Let's see if varName exists at or near colIdx
      let foundIndex = line.indexOf(varName, colIdx - 2 >= 0 ? colIdx - 2 : 0);
      if (foundIndex === -1) {
        // Fallback search
        foundIndex = line.indexOf(varName);
      }

      if (foundIndex !== -1) {
        // Verify we are not modifying an already prefixed variable
        const isAlreadyPrefixed = foundIndex > 0 && line[foundIndex - 1] === '_';
        if (!isAlreadyPrefixed) {
          // Replace varName with _varName
          const before = line.slice(0, foundIndex);
          const after = line.slice(foundIndex + varName.length);
          lines[lineIdx] = before + '_' + varName + after;
          fixedPositions.add(key);
        }
      }
    }

    fs.writeFileSync(filePath, lines.join(isCRLF ? '\r\n' : '\n'), 'utf8');
  }
}

// Run cleanup for frontend and backend
const projectRoot = path.resolve(__dirname, '../..');
runLintAndFix(path.join(projectRoot, 'frontend'));
runLintAndFix(path.join(projectRoot, 'backend'));

console.log('Unused variables cleanup complete!');
