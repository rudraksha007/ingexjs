import { CheerioCrawler, PlaywrightCrawler, RequestQueue, type CheerioCrawlingContext, type PlaywrightCrawlingContext } from "crawlee";
import { DefaultConfig, IngestorStatus, type ContentHandlerParams, type Data, type IngestorConfig, type IngestorEvents, type IngestorHook, type IngestorHookCallback, type IngestorProcess, type IngestorState, type StorageConnector } from "./types";
import { randomUUID } from 'crypto';
import { logger } from "./logger";
import { IngexHttpClient } from "./http-client";
import path from "node:path";

export class Ingestor {
    private config: IngestorConfig;
    private storage: StorageConnector;
    private workers: Worker[] = [];
    private state: IngestorState = {
        startUrls: [],
        maxDiscoveries: 100,
        skipped: 0,
        status: IngestorStatus.idle,
        stage: "crawl",
        visitedUrls: new Set<string>(),
        dataBuffer: []
    }

    private stages: IngestorProcess[] = [];

    private playwright: PlaywrightCrawler | undefined;
    private cheerio: CheerioCrawler | undefined;
    private elevatedQueue: RequestQueue | undefined;
    private requestQueue: RequestQueue | undefined;
    private hooks: { [E in IngestorEvents]: IngestorHook<E>[]; } = {
        start: [],
        end: [],
        stopped: [],
        error: [],
        jobCompletion: [],
        jobError: [],
        jobQueueCompletion: []
    };

    constructor(storage: StorageConnector, config?: Partial<IngestorConfig>) {
        this.storage = storage;
        this.config = { ...DefaultConfig, ...config };
    }

    on<ev extends IngestorEvents>(event: ev, hook: IngestorHookCallback[ev]) {
        const id = randomUUID();
        this.hooks[event].push({ id, event: event, callback: hook });
        return id;
    }

    off(event: IngestorEvents, id: string) {
        const hook = this.hooks[event].find(hook => hook.id === id);
        if (hook) {
            this.hooks[event].splice(this.hooks[event].indexOf(hook as any), 1);
        }
        return this;
    }

    getStatus() {
        return this.state.status;
    }

    getDataBuffer<T extends Data = Data>() {
        return this.state.dataBuffer as T[];
    }

    private emit<E extends IngestorEvents>(event: E, data: Parameters<IngestorHookCallback[E]>[0]) {
        (this.hooks[event] as IngestorHook<E>[]).forEach(hook => {
            hook.callback(data as any);
        });
    }

    setState(state: Partial<Omit<IngestorState, 'skipped' | 'status' | 'stage'>>) {
        this.state = { ...this.state, ...state };
    }

    async start(stage: string = "crawl", applyOn?: string) {
        if (stage.toLocaleLowerCase() === 'crawl') {
            await this.runScraper();
            return;
        }

        if (!applyOn) {
            throw new Error(`Stage '${stage}' requires an 'applyOn' parameter to know which data to process.`);
        }

        const proc = this.stages.find(p => p.stage === stage);
        if (!proc) {
            throw new Error(`Stage ${stage} not found!`);
        }
        this.state.stage = proc.stage;
        this.state.status = IngestorStatus.scraping;
        this.emit('start', {
            startUrls: this.state.startUrls, setUrls: (urls) => {
                this.state.startUrls = urls;
            }, status: this.state.status
        });

        const workerUrl = new URL('./worker.ts', import.meta.url).href;

        this.workers = [];
        for (let i = 0; i < proc.workerCount; i++) {
            this.workers.push(new Worker(workerUrl));
        }

        // Initialize workers
        await Promise.all(this.workers.map(worker => new Promise<void>((resolve, reject) => {
            worker.onmessage = (e: MessageEvent) => {
                const msg = e.data;
                if (msg.type === 'RESPONSE' && msg.id === proc.jobModule) {
                    if (msg.error) reject(new Error(msg.error));
                    else resolve();
                }
            };
            worker.postMessage({ type: 'INIT', data: { moduleName: proc.jobModule } });
        })));

        const PAGE_SIZE = 50;
        let processedCount = 0;
        let errorCount = 0;
        async () => {
            while (this.state.status === IngestorStatus.paused || this.state.status === IngestorStatus.scraping) {
                if (this.state.status === IngestorStatus.paused) {
                    await new Promise(r => setTimeout(r, 1000));
                    continue;
                }

                const batch = await this.storage.getBatch(applyOn, PAGE_SIZE);
                if (!batch || batch.length === 0) {
                    logger.info(`No more data to process for stage ${stage} from ${applyOn}.`);
                    break;
                }

                const ok: any[] = [];
                let nextItemIdx = 0;
                let completedCount = 0;

                await new Promise<void>((resolve) => {
                    if (batch.length === 0) return resolve();

                    const assignNext = (worker: Worker) => {
                        if (nextItemIdx >= batch.length || this.state.status !== IngestorStatus.scraping || errorCount >= 20) return;
                        const item = batch[nextItemIdx++];
                        const jobId = randomUUID();
                        worker.postMessage({ type: 'JOB', id: jobId, data: item });
                    };

                    for (const worker of this.workers) {
                        worker.onmessage = (event: MessageEvent) => {
                            const msg = event.data;
                            if (msg.type === 'RESPONSE') {
                                if (msg.error) {
                                    logger.error({ err: msg.error }, "Worker error processing item");
                                    this.emit('jobError', { error: msg.error });
                                    errorCount++;
                                } else {
                                    this.emit('jobCompletion', msg.data);
                                    ok.push(msg.data);
                                }
                            }

                            completedCount++;
                            if (completedCount >= batch.length || this.state.status !== IngestorStatus.scraping || errorCount >= 20) {
                                resolve();
                            } else {
                                assignNext(worker);
                            }
                        };

                        worker.onerror = (err) => {
                            logger.error(err, "Worker thread crashed");
                            this.emit('jobError', { error: err });
                            errorCount++;
                            completedCount++;
                            if (completedCount >= batch.length || this.state.status !== IngestorStatus.scraping || errorCount >= 20) {
                                resolve();
                            } else {
                                assignNext(worker);
                            }
                        };

                        assignNext(worker);
                    }
                });

                if (errorCount >= 20) {
                    logger.error(`Too many errors. Stopping stage ${stage}.`);
                    this.state.status = IngestorStatus.error;
                    this.emit('error', { error: new Error(`Too many errors in stage ${stage}`), stack: "", state: this.state.status });
                    break;
                }

                if (ok.length > 0) {
                    try {
                        await this.storage.insertMany(stage, ok);
                    } catch (dbErr: any) {
                        logger.error({ err: dbErr }, `Error saving processed data batch for stage ${stage}`);
                    }
                }

                try {
                    const ids = batch.map((b: any) => b._id ? b._id.toString() : b.url);
                    await this.storage.deleteMany(applyOn, ids);
                } catch (dbErr) {
                    logger.error({ err: dbErr }, `Error deleting processed batch from ${applyOn}`);
                }

                processedCount += batch.length;
                this.emit('jobQueueCompletion', { batchSize: batch.length, stage: stage });
            }
        }

        for (const worker of this.workers) {
            worker.terminate();
        }
        this.workers = [];

        if (this.state.status === IngestorStatus.scraping) {
            this.state.status = IngestorStatus.done;
            this.emit('end', { status: this.state.status });
        }
    }

    register(stage: string, jobModule: string, workerCount: number = 1) {
        if (this.state.status !== IngestorStatus.idle && this.state.status !== IngestorStatus.paused) {
            throw new Error("Cannot add stage to a running or stopped ingestor");
        }
        if (stage === 'crawl') throw new Error("Cannot override crawl stage!");

        let resolvedPath = jobModule;
        if (!path.isAbsolute(jobModule) && !jobModule.startsWith('file://')) {
            resolvedPath = path.resolve(process.cwd(), jobModule);
        }

        this.stages.push({ stage, workerCount, jobModule: resolvedPath });
        return this;
    }

    async stop() {
        await this.stopScrapper(false);
    }

    async pause() {
        await this.stopScrapper(true);
    }

    private async runScraper() {
        const setUrls = (newUrls: string[]) => { this.state.startUrls = newUrls; }
        const isResumed = this.state.status === IngestorStatus.paused;
        this.state.status = IngestorStatus.scraping;
        this.emit('start', { startUrls: this.state.startUrls, setUrls, status: this.state.status });
        // If requestQueue is not there, create one, if its there but system is starting fresh (not from pause) then recreate it
        if (!this.requestQueue) {
            this.requestQueue = await RequestQueue.open("default");
        } else if (!isResumed) {
            logger.warn("Default request queue already exists, but system is starting fresh, dropping queue...");
            await this.requestQueue.drop();
            this.requestQueue = await RequestQueue.open("default");
        }
        await this.requestQueue.addRequests(this.state.startUrls);
        if (!this.elevatedQueue) {
            this.elevatedQueue = await RequestQueue.open("elevated");
        } else if (!isResumed) {
            logger.warn("Elevated request queue already exists, but system is starting fresh, dropping queue...");
            await this.elevatedQueue.drop();
            this.elevatedQueue = await RequestQueue.open("elevated");
        }
        if (this.cheerio) await this.cheerio.teardown();
        this.cheerio = new CheerioCrawler({
            requestQueue: this.requestQueue,
            ignoreSslErrors: true,
            httpClient: IngexHttpClient,
            keepAlive: true,
            requestHandler: this.cheerioHandler,
            maxRequestRetries: 0,
            failedRequestHandler: async ({ request }) => {
                logger.warn(`CheerioCrawler failed for ${request.url}, falling back to PlaywrightCrawler...`);
                await this.elevatedQueue!.addRequest({ url: request.url });
            }
        });
        if (this.playwright) await this.playwright.teardown();
        this.playwright = new PlaywrightCrawler({ keepAlive: true, requestQueue: this.elevatedQueue, requestHandler: this.playwrightHandler });

    }

    private async stopScrapper(pause: boolean = false) {
        if (this.state.dataBuffer.length > 0) {
            try {
                await this.storage.insertMany("crawl", this.state.dataBuffer);
            } catch (err: any) {
                logger.error({ err }, `Error flushing remaining raw data batch to DB.`);
            }
            this.state.dataBuffer = [];
        }
        this.state.status = pause ? IngestorStatus.paused : IngestorStatus.killed;
        this.emit('stopped', { status: this.state.status });
        if (!pause) {
            try {
                await this.requestQueue!.drop();
                await this.elevatedQueue!.drop();
            } catch (e) {
                logger.error(e, "Error dropping storages");
            }
        }
    }

    private async cheerioHandler({ request, body, $ }: CheerioCrawlingContext) {
        if (this.state.status < 2) return;
        if (!body) {
            this.elevatedQueue!.addRequest({ url: request.url });
            return;
        }

        const urls = $('a')
            .map((_, a) => ({ url: $(a).attr('href') || '' }))
            .get()
            .filter((item) => {
                if (!item.url) return false;
                try {
                    const u = new URL(item.url, request.url);
                    return u.protocol === 'http:' || u.protocol === 'https:';
                } catch {
                    return false;
                }
            })
            .map(item => (new URL(item.url, request.url).href));
        await this.contentHandler({
            data: {
                url: request.url,
                title: $('title').text() || "",
                content: body.toString(),
                isCheerio: true,
                nestedUrls: urls,
            },
            caller: "Cheerio"
        });
    }

    private playwrightHandler = async ({ request, page }: PlaywrightCrawlingContext) => {
        if (this.state.status < 2) return;
        const html = await page.content();
        if (!html) return;
        const text = await page.textContent("body");
        if (!text) return;

        const urls = await page.$$eval('a', (elements, baseUrl) => {
            return elements
                .map(a => (a as HTMLAnchorElement).href)
                .filter(href => {
                    if (!href) return false;
                    try {
                        const u = new URL(href, baseUrl);
                        return u.protocol === 'http:' || u.protocol === 'https:';
                    } catch {
                        return false;
                    }
                })
                .map(href => (new URL(href, baseUrl).href));
        }, request.url);

        await this.contentHandler({
            data: {
                url: request.url,
                title: await page.title() || "",
                content: html,
                isCheerio: false,
                nestedUrls: urls,
            },
            caller: "Playwright"
        });
    }


    private async contentHandler({ data, caller }: ContentHandlerParams) {
        if (this.state.maxDiscoveries !== -1 && this.state.visitedUrls.size >= this.state.maxDiscoveries) {
            this.stop();
            return;
        }

        const pending = this.requestQueue!.getPendingCount() + this.elevatedQueue!.getPendingCount();
        const needed = this.state.maxDiscoveries - this.state.visitedUrls.size;
        const isStarving = pending < needed;
        if (this.state.visitedUrls.has(data.url) && !isStarving) {
            logger.info(`[${caller}] Skipping save (already exists): ${data.url}`);
            // TODO emit event
            return;
        }

        const scrapedData: Data = {
            url: data.url,
            title: data.title,
            content: data.content,
            stage: "raw",
            scrapedAt: new Date(),
        };

        if (!scrapedData.content || scrapedData.content.trim() === "") {
            if (data.isCheerio) this.elevatedQueue?.addRequest({ url: data.url });
            else {
                logger.info(`[${caller}] Skipping save (empty content): ${data.url}`);
                // TODO emit event
            }
            return;
        }

        if (!this.state.visitedUrls.has(data.url)) {
            this.state.dataBuffer.push(scrapedData);
            this.state.visitedUrls.add(data.url);
            if (this.state.dataBuffer.length >= this.config.flushGap) {
                try {
                    this.storage.insertMany("raw", this.state.dataBuffer);
                } catch (err: any) {
                    if (err.code !== 11000) {
                        logger.error({ err }, `[${caller}] Error saving raw data batch to DB.`);
                        // TODO emit event
                    }
                }
                this.state.dataBuffer = [];
            }
        }

        this.requestQueue!.addRequests(data.nestedUrls);
        this.state.visitedUrls.add(data.url);
        // TODO emit event

        if (this.state.maxDiscoveries !== -1 && this.state.visitedUrls.size >= this.state.maxDiscoveries) {
            logger.info("Max discoveries reached. Stopping crawler.");
            this.stop();
        }
    }
}