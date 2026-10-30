import express, { type ErrorRequestHandler } from 'express';
import { existsSync } from 'node:fs';
import { resolve, extname } from 'node:path';
import { z } from 'zod';
import { ROOT, readConfig, type Config } from './config.js';
import { DomainError } from './common.js';
import { brokerHealth } from './kafka.js';
import { measurementInput, ruleInput, workerInput } from './models.js';
import { Runtime } from './runtime.js';
import * as read from './read-models.js';
export async function createApp(config: Config = readConfig()) {
    const runtime = await Runtime.create(config), app = express();
    app.disable('x-powered-by');
    app.use(express.json({ limit: '256kb', strict: true }));
    app.get('/api/health', async (_req, res) => {
        await runtime.store.get('SELECT 1');
        res.json({ status: 'ok', mode: 'demo', database: 'ok', database_engine: 'postgresql', runtime: 'node', framework: 'express', ...await brokerHealth(config) });
    });
    app.get('/api/overview', async (_req, res) => { res.json(await read.overview(runtime.store)); });
    app.get('/api/schools', async (_req, res) => { res.json(await read.listSchools(runtime.store)); });
    app.get('/api/providers', async (_req, res) => { res.json(await runtime.store.all('SELECT * FROM providers ORDER BY country,name')); });
    app.get('/api/measurements', async (_req, res) => { res.json(await read.listMeasurements(runtime.store)); });
    app.get('/api/measurements/:id', async (req, res) => { res.json(await read.getMeasurement(runtime.store, req.params.id)); });
    app.post('/api/measurements', async (req, res) => {
        const result = await runtime.engine.ingest(measurementInput.parse(req.body));
        res.status(result.duplicate ? 200 : 201).json(result);
    });
    app.get('/api/credits', async (_req, res) => { res.json(await read.listCredits(runtime.store)); });
    app.get('/api/events', async (_req, res) => { res.json(await read.listEvents(runtime.store)); });
    app.post('/api/worker/run', async (req, res) => {
        const values = workerInput.parse(req.body ?? {});
        res.json(await runtime.runWorker(values.limit, values.simulate_failure));
    });
    app.post('/api/events/:id/retry', async (req, res) => { res.json(await runtime.retry(req.params.id)); });
    app.get('/api/rules', async (_req, res) => { res.json(await read.currentRule(runtime.store)); });
    app.put('/api/rules', async (req, res) => { res.json(await runtime.engine.updateRule(ruleInput.parse(req.body))); });
    app.post('/api/demo/scenarios/:scenario', async (req, res) => { res.json(await runtime.scenario(req.params.scenario)); });
    app.post('/api/demo/reset', async (_req, res) => { res.json(await runtime.reset()); });
    app.use('/api', (_req, res) => { res.status(404).json({ detail: 'API route not found' }); });
    const dist = resolve(ROOT, 'frontend/dist');
    app.use(express.static(dist, { index: false, fallthrough: true }));
    app.use((req, res) => {
        const index = resolve(dist, 'index.html');
        if (req.method === 'GET' && !extname(req.path) && existsSync(index)) {
            res.sendFile(index);
            return;
        }
        res.status(404).json({ detail: req.method === 'GET' ? 'File not found' : 'Route not found' });
    });
    const errors: ErrorRequestHandler = (error, _req, res, _next) => {
        if (error instanceof DomainError) {
            res.status(error.statusCode).json({ detail: error.message });
            return;
        }
        if (error instanceof z.ZodError) {
            res.status(422).json({ detail: error.issues.map(issue => ({ type: issue.code, loc: ['body', ...issue.path], msg: issue.message })) });
            return;
        }
        if (error?.type === 'entity.parse.failed') {
            res.status(422).json({ detail: 'Request body must contain valid JSON' });
            return;
        }
        if (error?.type === 'entity.too.large') {
            res.status(413).json({ detail: 'Request body is too large (maximum 256 KB)' });
            return;
        }
        if (error instanceof URIError) {
            res.status(400).json({ detail: 'Invalid URL encoding' });
            return;
        }
        console.error('Request failed:', error instanceof Error ? error.message : String(error));
        res.status(500).json({ detail: 'An unexpected server error occurred' });
    };
    app.use(errors);
    return { app, runtime };
}
