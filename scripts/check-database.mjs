// Read-only guard: never seed the primary PostgreSQL target over an unimported
// legacy SQLite dataset. Explicit isolated QA schemas are independent fixtures.
import {existsSync} from 'node:fs';
import {createRequire} from 'node:module';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {readConfig} from '../server/dist/config.js';
const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const legacy=path.join(root,'.data/connectivity.sqlite3');
const config=readConfig();
if(config.databaseSchema==='public'&&existsSync(legacy)) {
  const require=createRequire(import.meta.url);
  const {Client}=require('../server/node_modules/pg');
  const client=new Client({connectionString:config.databaseUrl});
  try {
    await client.connect();
    const relation=await client.query('SELECT to_regclass($1) AS relation',['public.metadata']);
    const ready=relation.rows[0].relation&&(await client.query("SELECT 1 FROM public.metadata WHERE key='seed_version'")).rowCount;
    if(!ready)throw new Error('Existing SQLite data must be imported before the first PostgreSQL app launch. Run: node server/scripts/import-sqlite.mjs --sqlite-path .data/connectivity.sqlite3 . No data was changed.');
  } finally {await client.end();}
}
