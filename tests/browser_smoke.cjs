/* Real browser interactions with an isolated server and temporary database. */
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const {spawn} = require('node:child_process');

let playwright;
try { playwright = require(process.env.PLAYWRIGHT_MODULE || 'playwright'); }
catch {
  console.error('Optional browser checks require Playwright. Install playwright@1.62.1 in a separate directory and set PLAYWRIGHT_MODULE to its node_modules/playwright path.');
  process.exit(1);
}

const root = path.resolve(__dirname, '..');
const selectedTransport = process.env.BROWSER_TRANSPORT || 'local';
assert(['local','kafka'].includes(selectedTransport),'BROWSER_TRANSPORT must be local or kafka');
const output = path.join(root, 'artifacts', selectedTransport === 'kafka' ? 'browser-kafka' : 'browser');
const port = Number(process.env.BROWSER_TEST_PORT || 8766);
const base = `http://127.0.0.1:${port}`;
const pgSchema = `qa_browser_${Date.now()}_${Math.random().toString(16).slice(2,10)}`;
const privateEnvironment = path.join(root,'.data/postgres/app.env');
const databaseUrl = process.env.DATABASE_URL || process.env.CONNECTIVITY_DATABASE_URL || (fs.existsSync(privateEnvironment) ? fs.readFileSync(privateEnvironment,'utf8').match(/^export DATABASE_URL='([^']+)'$/m)?.[1] : null);
const report = {started_at: new Date().toISOString(), base_url: base, transport:selectedTransport, database_isolation:'temporary PostgreSQL schema', database_isolated: true, checks: [], errors: [], failed_responses: [], viewport_checks: [], screenshots: []};
let serverOutput = '';
let browser;
let server;
let currentPage;
let inspector;
let schemaCreated=false;

function record(message) { report.checks.push(message); console.log(`PASS ${message}`); }
async function api(route) {
  const response = await fetch(base + route);
  assert(response.ok, `${route}: ${response.status}`);
  return response.json();
}
async function mutation(page, route, action, method='POST') {
  const [response] = await Promise.all([
    page.waitForResponse(response => new URL(response.url()).pathname === route && response.request().method() === method),
    action(),
  ]);
  assert(response.ok(), `${route}: ${response.status()} ${await response.text()}`);
  return response.json();
}
async function navigate(page, name) {
  const escaped = name.replace(/[.*+?^${}()|[\]\\]/g,'\\$&');
  await page.getByRole('navigation', {name:'Main navigation'}).getByRole('button', {name:new RegExp(`^${escaped}(?:\\s*\\d+)?$`)}).click();
}
async function screenshot(page, filename) {
  const dialogOpen = await page.getByRole('dialog').count() > 0;
  await page.screenshot({path:path.join(output,filename), fullPage:!dialogOpen, animations:'disabled'});
  report.screenshots.push(filename);
}
function watch(page) {
  page.on('pageerror', error => report.errors.push({kind:'pageerror', message:error.message}));
  page.on('console', message => { if (message.type() === 'error') report.errors.push({kind:'console', message:message.text(), location:message.location()}); });
  page.on('response', response => { if (response.status() >= 400) report.failed_responses.push({url:response.url(),status:response.status()}); });
}
async function noRootOverflow(page) {
  const configuredWidth = page.viewportSize().width;
  const dimensions = await page.evaluate(() => ({heading:document.querySelector('h1')?.textContent,layoutWidth:window.innerWidth, visualWidth:window.visualViewport.width, content:document.documentElement.scrollWidth}));
  report.viewport_checks.push({configuredWidth,...dimensions});
  assert(dimensions.content <= configuredWidth + 1, `Document overflow at configured ${configuredWidth}px: ${JSON.stringify(dimensions)}`);
  assert(dimensions.layoutWidth <= configuredWidth + 1, `Layout viewport expanded at configured ${configuredWidth}px: ${JSON.stringify(dimensions)}`);
}

(async () => {
  fs.mkdirSync(output, {recursive:true});
  assert(fs.existsSync(path.join(root, 'frontend/dist/index.html')), 'Run ./scripts/start.sh or npm run build in frontend first.');
  assert(fs.existsSync(path.join(root, 'server/dist/index.js')), 'Build the active Node/Express server first.');
  report.runtimes = {node:process.version, backend:'Node.js / TypeScript / Express', playwright:require(path.join(path.dirname(require.resolve(process.env.PLAYWRIGHT_MODULE || 'playwright')),'package.json')).version};
  let portOccupied = false;
  try { await fetch(base+'/api/health'); portOccupied = true; } catch {}
  assert(!portOccupied, `Port ${port} is already in use. Set BROWSER_TEST_PORT to a free port; refusing to touch an existing server.`);
  assert(databaseUrl,'Start task-owned PostgreSQL or set DATABASE_URL (credentials are never printed).');
  const {Client}=require(path.join(root,'server/node_modules/pg'));
  inspector=new Client({connectionString:databaseUrl});
  await inspector.connect();
  await inspector.query(`CREATE SCHEMA "${pgSchema}"`);
  schemaCreated=true;
  const pgInfo=await inspector.query("SELECT current_setting('server_version') AS version,current_database() AS database,current_user AS role,(SELECT rolsuper FROM pg_roles WHERE rolname=current_user) AS superuser");
  report.postgresql={...pgInfo.rows[0],schema:pgSchema};
  server = spawn(process.execPath, [path.join(root,'server/dist/index.js')], {
    cwd:root, env:{...process.env, DATABASE_URL:databaseUrl,CONNECTIVITY_DB_SCHEMA:pgSchema, CONNECTIVITY_TRANSPORT:selectedTransport,
      HOST:'127.0.0.1',PORT:String(port),CONNECTIVITY_KAFKA_TOPIC:process.env.BROWSER_KAFKA_TOPIC || `connectivity-browser-${Date.now()}`}, stdio:['ignore','pipe','pipe'],
  });
  server.stdout.on('data', chunk => {serverOutput += chunk;});
  server.stderr.on('data', chunk => {serverOutput += chunk;});
  for (let attempt=0; attempt<80; attempt++) {
    if (server.exitCode !== null) throw new Error(`Isolated API could not start: ${serverOutput}`);
    try { if ((await fetch(base+'/api/health')).ok) break; } catch {}
    if (attempt === 79) throw new Error(`Isolated API startup timed out: ${serverOutput}`);
    await new Promise(resolve => setTimeout(resolve,125));
  }
  const options = {headless:true};
  if (process.env.BROWSER_EXECUTABLE_PATH) options.executablePath = process.env.BROWSER_EXECUTABLE_PATH;
  else if (fs.existsSync('/Applications/Google Chrome.app/Contents/MacOS/Google Chrome')) options.channel = 'chrome';
  browser = await playwright.chromium.launch(options);
  report.runtimes.browser = browser.version();
  const context = await browser.newContext({viewport:{width:1440,height:1000}, acceptDownloads:true, reducedMotion:'reduce'});
  const page = await context.newPage();
  currentPage = page;
  watch(page);
  await page.goto(base);
  await page.getByRole('heading', {name:'Network overview', exact:true}).waitFor();
  await page.getByRole('button', {name:'Run clean batch', exact:true}).waitFor();
  const health = await api('/api/health');
  assert.equal(health.transport,selectedTransport);
  assert.equal(health.broker,selectedTransport === 'kafka' ? 'ok' : 'not_used');
  assert.equal(health.runtime,'node');
  assert.equal(health.framework,'express');
  assert.equal(health.database_engine,'postgresql');
  await page.getByText(selectedTransport === 'kafka' ? 'Kafka connected' : 'Local queue active',{exact:true}).first().waitFor();
  report.health = health;
  await noRootOverflow(page);
  await screenshot(page,'desktop-overview.png');
  await page.getByRole('button', {name:'7 days',exact:true}).click();
  await page.getByRole('img', {name:/over the last 7 days/}).waitFor();
  record(`Seeded desktop overview loads, ${selectedTransport} transport label matches live health, and chart range changes`);

  await navigate(page,'Schools');
  await page.getByLabel('Filter by country',{exact:true}).selectOption('Nepal');
  await page.waitForFunction(()=>document.querySelectorAll('tbody tr').length===4);
  assert.equal(await page.locator('tbody tr').count(),4);
  await page.getByRole('textbox',{name:'Search schools',exact:true}).fill('not-a-school');
  await page.getByRole('heading',{name:'No records found',exact:true}).waitFor();
  await page.getByRole('button',{name:'Clear search',exact:true}).click();
  assert.equal(await page.locator('tbody tr').count(),4);
  record('School country/search filters and empty result state work');

  await page.getByRole('button',{name:'Add measurement',exact:true}).click();
  const form = page.getByRole('dialog',{name:'Submit a simulated reading',exact:true});
  await form.getByLabel('Service date',{exact:true}).fill('2020-05-10');
  await form.getByLabel('Source',{exact:true}).fill('browser-smoke');
  await form.getByLabel('External event ID',{exact:true}).fill('browser-form-001');
  const submitted = await mutation(page,'/api/measurements',()=>form.getByRole('button',{name:'Add to queue',exact:true}).click());
  await form.waitFor({state:'hidden'});
  await page.getByRole('heading',{name:'Measurements',exact:true}).waitFor();
  await page.getByRole('textbox',{name:'Search measurements',exact:true}).fill('browser-form-001');
  await page.locator('tbody tr').filter({hasText:'Pending'}).waitFor();
  assert.equal(await page.locator('tbody tr').count(),1);
  assert.equal((await api(`/api/measurements/${submitted.measurement.id}`)).status,'pending');
  await navigate(page,'Event log');
  await mutation(page,'/api/worker/run',()=>page.getByRole('button',{name:'Process queue',exact:true}).click());
  await page.getByRole('button',{name:'Process queue',exact:true}).waitFor({state:'visible'});
  const processed = await api(`/api/measurements/${submitted.measurement.id}`);
  assert.equal(processed.status,'eligible');
  assert.equal(processed.credit_issued,true);
  await navigate(page,'Measurements');
  await page.getByRole('textbox',{name:'Search measurements',exact:true}).fill('browser-form-001');
  await page.locator('tbody tr').filter({hasText:'Issued'}).waitFor();
  await page.locator('tbody tr').getByRole('button',{name:processed.school_name,exact:true}).click();
  const details = page.getByRole('dialog',{name:'Measurement details',exact:true});
  await details.getByText('Demo credit issued',{exact:true}).waitFor();
  assert.equal(await details.locator('.check-pass').count(),5);
  await screenshot(page,'desktop-measurement-checks.png');
  await details.getByRole('button',{name:'Close details',exact:true}).click();
  record('Actual measurement form creates queued work, worker issues credit, detail exposes five passing checks');

  await navigate(page,'Overview');
  const creditCountBefore = (await api('/api/credits')).length;
  const failed = await mutation(page,'/api/demo/scenarios/worker_failure',()=>page.getByRole('button',{name:'Simulate worker failure',exact:true}).click());
  await page.getByRole('heading',{name:'Event log',exact:true}).waitFor();
  const eventRow = page.locator('tbody tr').filter({hasText:failed.event_ids[0]});
  await eventRow.getByRole('button',{name:'Retry',exact:true}).waitFor();
  assert.equal((await api('/api/credits')).length,creditCountBefore);
  await screenshot(page,'desktop-worker-failure.png');
  await mutation(page,`/api/events/${failed.event_ids[0]}/retry`,()=>eventRow.getByRole('button',{name:'Retry',exact:true}).click());
  await eventRow.getByText('Processed',{exact:true}).waitFor();
  assert.equal((await api('/api/credits')).length,creditCountBefore+1);
  assert.equal(await eventRow.getByRole('button',{name:'Retry',exact:true}).count(),0);
  record('Worker failure leaves zero new credit; UI Retry completes exactly one issuance');

  await navigate(page,'Policy');
  await page.getByRole('spinbutton',{name:'Minimum download',exact:true}).fill('60');
  await page.getByRole('spinbutton',{name:'Credits per qualifying period',exact:true}).fill('17');
  const updated = await mutation(page,'/api/rules',()=>page.getByRole('button',{name:'Save new policy version',exact:true}).click(),'PUT');
  await page.getByText(`Version ${updated.version}`,{exact:true}).waitFor();
  assert.deepEqual(await api(`/api/measurements/${submitted.measurement.id}`),processed);
  await navigate(page,'Overview');
  const batch = await mutation(page,'/api/demo/scenarios/clean_batch',()=>page.getByRole('button',{name:'Run clean batch',exact:true}).click());
  assert.equal(batch.credits_created,3);
  for (const id of batch.measurement_ids) assert.equal((await api(`/api/measurements/${id}`)).rule_version,updated.version);
  record('Policy edit saves a new version, preserves an old decision, and clean scenario uses the new policy');

  await navigate(page,'Credit ledger');
  await page.getByLabel('Filter by country',{exact:true}).selectOption('Nepal');
  await page.locator('tbody tr').first().locator('td').nth(1).getByText('Nepal',{exact:true}).waitFor();
  const rowCount = await page.locator('tbody tr').count();
  const [download] = await Promise.all([page.waitForEvent('download'),page.getByRole('button',{name:'Export CSV',exact:true}).click()]);
  const csvPath = path.join(output,download.suggestedFilename());
  await download.saveAs(csvPath);
  const lines = fs.readFileSync(csvPath,'utf8').trim().split(/\r?\n/);
  assert.equal(lines.length,rowCount+1);
  assert(lines[0].includes('demo_credits'));
  assert(lines.slice(1).every(line=>line.includes('"Nepal"')));
  record('Ledger country filter and downloaded CSV agree on filtered records');

  await page.getByRole('button',{name:'Reset demo data',exact:true}).click();
  const resetDialog = page.getByRole('dialog',{name:'Restore the demo?',exact:true});
  await resetDialog.getByRole('button',{name:'Keep current data',exact:true}).click();
  assert.equal((await api('/api/rules')).version,updated.version);
  await page.getByRole('button',{name:'Reset demo data',exact:true}).click();
  await mutation(page,'/api/demo/reset',()=>resetDialog.getByRole('button',{name:'Reset demo data',exact:true}).click());
  await page.getByRole('heading',{name:'Network overview',exact:true}).waitFor();
  assert.equal((await api('/api/rules')).version,1);
  record('Reset can be cancelled and confirmed; confirmation restores original policy and seed');

  const mobileContext = await browser.newContext({viewport:{width:390,height:844},isMobile:true,hasTouch:true,deviceScaleFactor:1,reducedMotion:'reduce'});
  const mobile = await mobileContext.newPage();
  currentPage = mobile;
  watch(mobile);
  await mobile.goto(base);
  await mobile.getByRole('heading',{name:'Network overview',exact:true}).waitFor();
  await mobile.getByRole('button',{name:'Run clean batch',exact:true}).waitFor();
  await noRootOverflow(mobile);
  await screenshot(mobile,'mobile-overview.png');
  await mobile.getByRole('button',{name:'Open navigation',exact:true}).click();
  await navigate(mobile,'Schools');
  await mobile.getByRole('heading',{name:'Schools',exact:true}).waitFor();
  await noRootOverflow(mobile);
  assert(await mobile.locator('.table-scroll').evaluate(element=>{element.scrollLeft=element.scrollWidth;const moved=element.scrollLeft>0;element.scrollLeft=0;return moved;}),'Wide school table must scroll inside its own container');
  await screenshot(mobile,'mobile-schools.png');
  await mobile.getByRole('button',{name:'Add measurement',exact:true}).click();
  await mobile.getByRole('dialog',{name:'Submit a simulated reading',exact:true}).waitFor();
  await noRootOverflow(mobile);
  const dialogBounds = await mobile.getByRole('dialog',{name:'Submit a simulated reading',exact:true}).evaluate(element=>{const rect=element.getBoundingClientRect();return {x:rect.x,y:rect.y,right:rect.right,bottom:rect.bottom,width:rect.width,height:rect.height};});
  report.mobile_dialog_bounds = dialogBounds;
  assert(dialogBounds.x >= -1 && dialogBounds.right <= 391 && dialogBounds.y >= -1 && dialogBounds.bottom <= 845,`Mobile dialog is clipped: ${JSON.stringify(dialogBounds)}`);
  await screenshot(mobile,'mobile-form.png');
  record('390px mobile overview, navigation, schools and measurement dialog fit the viewport');
  assert.deepEqual(report.errors,[],`Browser errors: ${JSON.stringify(report.errors)}`);
  record('No browser page errors or console errors on exercised flows');
})().catch(async error => {
  report.failure = error.stack || String(error);
  console.error(report.failure);
  if (currentPage) {
    try { await screenshot(currentPage,'failure.png'); } catch {}
  }
  process.exitCode = 1;
}).finally(async () => {
  report.finished_at = new Date().toISOString();
  if (browser) await browser.close();
  if (server) {server.kill('SIGTERM'); await new Promise(resolve=>{if(server.exitCode!==null)resolve();else server.once('exit',resolve);});}
  fs.mkdirSync(output,{recursive:true});
  fs.writeFileSync(path.join(output,'report.json'),JSON.stringify(report,null,2)+'\n');
  if(inspector) {
    if(schemaCreated)await inspector.query(`DROP SCHEMA "${pgSchema}" CASCADE`);
    await inspector.end();
  }
  console.log(`Browser evidence: ${output}`);
});
