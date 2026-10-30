import { importSqlite } from '../dist/import-sqlite.js';
import { readConfig } from '../dist/config.js';

const args = process.argv.slice(2), pathIndex = args.indexOf('--sqlite-path');
const sourcePath = pathIndex >= 0 ? args[pathIndex + 1] : process.env.CONNECTIVITY_SQLITE_IMPORT_PATH;
if (!sourcePath || sourcePath.startsWith('--')) {
  console.error('Usage: node server/scripts/import-sqlite.mjs --sqlite-path /absolute/source.sqlite3 [--verify-only]');
  process.exitCode = 2;
} else {
  try { console.log(JSON.stringify(await importSqlite(readConfig(), sourcePath, args.includes('--verify-only')), null, 2)); }
  catch (error) { console.error(error instanceof Error ? error.message : String(error)); process.exitCode = 1; }
}
