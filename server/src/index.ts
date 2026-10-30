import { createApp } from './app.js';
import { readConfig } from './config.js';

const config = readConfig(), { app, runtime } = await createApp(config);
const host = process.env.HOST ?? '127.0.0.1';
const server = app.listen(config.port, host, () => {
  const address = server.address();
  const port = address && typeof address === 'object' ? address.port : config.port;
  console.log(`Connectivity Credits Express API: http://${host}:${port} (${config.transport} transport)`);
});
let stopping = false;
const stop = () => {
  if (stopping) return;
  stopping = true;
  server.close(async () => { await runtime.close(); process.exit(0); });
  setTimeout(() => process.exit(1), 5000).unref();
};
process.on('SIGINT', stop); process.on('SIGTERM', stop);
