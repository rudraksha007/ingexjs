import { CheerioCrawler, PlaywrightCrawler, RequestQueue, type CheerioCrawlingContext, type PlaywrightCrawlingContext } from "crawlee";
import { DefaultConfig, IngestorStatus, type ContentHandlerParams, type Data, type IngestorConfig, type IngestorEvents, type IngestorHook, type IngestorHookCallback, type IngestorProcess, type IngestorState, type StorageConnector } from "./types";
import { randomUUID } from 'crypto';
import { logger } from "./logger";
import { IngexHttpClient } from "./http-client";
import path from "path";

export class Ingestor {
    private static instance: Ingestor | undefined;
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
    private runPromise: Promise<void> | undefined;
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

    /***
     * @param storage StorageConnector Specify the storage connector to be used for storing the data
     * @param config IngestorConfig Other configuration options
     */
    constructor(storage: StorageConnector, config?: Partial<IngestorConfig>) {
        this.storage = storage;
        this.config = { ...DefaultConfig, ...config };
        Ingestor.instance = this;
    }

    /**
     * Used to get the singleton instance of the ingestor
     * #### Ingestor must be initialised before hand
     * @returns Ingestor
     */
    public static getInstance() {
        return Ingestor.instance;
    }


    /**
     * Registers a callback hook for a specific lifecycle event.
     * @param event The event to listen for (e.g., 'start', 'end', 'error').
     * @param hook The callback function to execute when the event occurs.
     * @returns A unique string ID representing the registered hook.
     */
    on<ev extends IngestorEvents>(event: ev, hook: IngestorHookCallback[ev]) {
        const id = randomUUID();
        this.hooks[event].push({ id, event: event, callback: hook });
        return id;
    }

    /**
     * Unregisters a previously registered callback hook.
     * @param event The event the hook was registered for.
     * @param id The unique string ID returned by the `on` method.
     * @returns The Ingestor instance for chaining.
     */
    off(event: IngestorEvents, id: string) {
        const hook = this.hooks[event].find(hook => hook.id === id);
        if (hook) {
            this.hooks[event].splice(this.hooks[event].indexOf(hook as any), 1);
        }
        return this;
    }

    /**
     * Gets the current status of the Ingestor (e.g., scraping, filtering, done).
     * @returns The current IngestorStatus.
     */
    getStatus() {
        return this.state.status;
    }

    /**
     * Returns the dataBuffer that has accumulated till now in the current running batch
     * @returns The dataBuffer of type T[] where T extends Data
     */
    getDataBuffer<T extends Data = Data>() {
        return this.state.dataBuffer as T[];
    }

    private emit<E extends IngestorEvents>(event: E, data: Parameters<IngestorHookCallback[E]>[0]) {
        (this.hooks[event] as IngestorHook<E>[]).forEach(hook => {
            hook.callback(data as any);
        });
    }

    /**
     * Updates the internal state of the Ingestor
     * @param state Current state
     *   - startUrls: The URLs to start scraping from
     *   - maxDiscoveries: The maximum number of items to scrape
     *   - visitedUrls: The URLs that have been visited
     *   - dataBuffer: The data that has been accumulated
     */
    setState(state: Partial<Omit<IngestorState, 'skipped' | 'status' | 'stage'>>) {
        this.state = { ...this.state, ...state };
    }

    /**
     * Gets the current state of the Ingestor
     * @returns The current IngestorState
     */
    getState() {
        const resp = {
            visitedUrls: this.state.visitedUrls,
            maxDiscoveries: this.state.maxDiscoveries,
            startUrls: this.state.startUrls,
            stage: this.state.stage
        }

        return resp;
    }

    /**
     * Starts the Ingestor process.
     * @param stage The stage to start from (default: 'crawl').
     * @param applyOn The name of the dataset to apply the stage to (required for stages other than 'crawl').
     * @returns A Promise that resolves when the Ingestor process completes.
     */
    async start(stage: string = "crawl", applyOn?: string) {
        await this.storage.init()
        if (stage.toLocaleLowerCase() === 'crawl') {
            this.state.stage = "crawl";
            this.state.status = IngestorStatus.scraping;
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
        this.state.status = IngestorStatus.filtering;
        this.state.visitedUrls.clear();
        const totalItems = await this.storage.count(applyOn);
        if (totalItems !== undefined) {
            this.state.maxDiscoveries = totalItems;
        }
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
        await (async () => {
            while (this.state.status === IngestorStatus.paused || this.state.status === IngestorStatus.scraping || this.state.status === IngestorStatus.filtering) {
                if (this.state.status === IngestorStatus.paused) {
                    await new Promise(r => setTimeout(r, 1000));
                    continue;
                }

                const batch = await this.storage.getBatch(applyOn, PAGE_SIZE);
                logger.info(`Batch retrieved: ${batch.length}`);
                if (!batch || batch.length === 0) {
                    logger.info(`No more data to process for stage ${stage} from ${applyOn}.`);
                    break;
                }

                const ok: any[] = [];
                const rejectedItems: any[] = [];
                let nextItemIdx = 0;
                let completedCount = 0;

                await new Promise<void>((resolve) => {
                    if (batch.length === 0) return resolve();

                    const assignNext = (worker: Worker) => {
                        if (nextItemIdx >= batch.length || this.state.status !== IngestorStatus.filtering || errorCount >= 20) return;
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
                                    if (msg.data.rejected) {
                                        logger.info(`Item rejected by filter: ${msg.data.reason || "Unknown"}`);
                                        msg.data.stage = 'rejected';
                                        rejectedItems.push(msg.data);
                                    } else {
                                        ok.push(msg.data);
                                    }
                                    this.state.visitedUrls.add(msg.data.url);
                                    this.emit('jobCompletion', msg.data);
                                }
                            }

                            completedCount++;
                            if (completedCount >= batch.length || this.state.status !== IngestorStatus.filtering || errorCount >= 20) {
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
                            if (completedCount >= batch.length || this.state.status !== IngestorStatus.filtering || errorCount >= 20) {
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
                if (rejectedItems.length > 0) {
                    try {
                        await this.storage.insertMany('rejected', rejectedItems);
                    } catch (dbErr: any) {
                        logger.error({ err: dbErr }, `Error saving rejected data batch for stage ${stage}`);
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
        })()

        for (const worker of this.workers) {
            worker.terminate();
        }
        this.workers = [];

        if (this.state.status === IngestorStatus.filtering) {
            this.state.status = IngestorStatus.done;
            this.emit('end', { status: this.state.status });
        }
    }

    /**
     * Registers a new processing stage for the ingestor pipeline.
     * @param stage The name of the stage (e.g., 'transform', 'enrich').
     * @param jobModule The path to the worker module that handles jobs for this stage.
     * @param workerCount The number of worker threads to spawn for this stage (default: 1).
     * @returns The Ingestor instance for chaining.
     */
    register(stage: string, jobModule: string, workerCount: number = 1) {
        if (this.state.status !== IngestorStatus.idle && this.state.status !== IngestorStatus.paused) {
            throw new Error("Cannot add stage to a running or stopped ingestor");
        }
        if (stage === 'crawl') throw new Error("Cannot override crawl stage!");
        if (stage === 'rejected') throw new Error("Cannot register a 'rejected' stage!");

        let resolvedPath = jobModule;
        if (!path.isAbsolute(jobModule) && !jobModule.startsWith('file://')) {
            resolvedPath = path.resolve(process.cwd(), jobModule);
        }

        this.stages.push({ stage, workerCount, jobModule: resolvedPath });
        return this;
    }

    /**
     * Stops the ingestor process.
     * @param reason Optional reason for stopping, which will be logged.
     * @param force If true, forcefully kills the ingestor. Otherwise, stops gracefully.
     */
    async stop(reason?: string, force: boolean = false) {
        if (reason) logger.info(reason);
        if (this.state.status === IngestorStatus.stopping || this.state.status === IngestorStatus.killed || this.state.status === IngestorStatus.done) return;
        this.state.status = IngestorStatus.stopping;
        await this.stopScrapper(false);
        this.state.status = force ? IngestorStatus.killed : IngestorStatus.done;
    }

    /**
     * Pauses the current ingestion process, allowing it to be resumed later.
     */
    async pause() {
        if (this.state.status === IngestorStatus.paused || this.state.status === IngestorStatus.stopping || this.state.status === IngestorStatus.killed || this.state.status === IngestorStatus.done) return;
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

        this.runPromise = Promise.all([
            this.cheerio.run(),
            this.playwright.run()
        ]).then(async () => {
            if (this.state.status === IngestorStatus.scraping) {
                this.state.status = IngestorStatus.done;
                this.emit('end', { status: this.state.status });
                await this.requestQueue?.drop();
                await this.elevatedQueue?.drop();
                this.requestQueue = undefined;
                this.elevatedQueue = undefined;
            }
        }).catch((err) => {
            this.state.status = IngestorStatus.error;
            this.emit('error', { error: err, stack: "", state: this.state.status });
        });
    }

    private async stopScrapper(pause: boolean = false) {
        if (this.state.dataBuffer.length > 0) {
            try {
                await this.storage.insertMany("raw", this.state.dataBuffer);
            } catch (err: any) {
                logger.error({ err }, `Error flushing remaining raw data batch to DB.`);
            }
            this.state.dataBuffer = [];
        }
        this.state.status = pause ? IngestorStatus.paused : IngestorStatus.killed;
        this.emit('stopped', { status: this.state.status });
        if (!pause) {
            try {
                if (this.cheerio) {
                    await this.cheerio.teardown();
                }
                if (this.playwright) {
                    await this.playwright.teardown();
                }
                if (this.runPromise) {
                    await this.runPromise;
                }
                this.cheerio = undefined;
                this.playwright = undefined;
                // this.runPromise = undefined;
            } catch (e) {
                logger.error(e, "Error dropping storages");
            }
        }
    }

    private async cheerioHandler({ request, body, $ }: CheerioCrawlingContext) {
        if (Ingestor.getInstance()!.getStatus() !== IngestorStatus.scraping) return;
        if (!body) {
            await this.elevatedQueue!.addRequest({ url: request.url });
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
        await Ingestor.getInstance()!.contentHandler({
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
        if (Ingestor.getInstance()!.getStatus() !== IngestorStatus.scraping) return;
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

        await Ingestor.getInstance()!.contentHandler({
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
            if (this.getStatus() === IngestorStatus.scraping
                || this.getStatus() === IngestorStatus.filtering) await this.stop();
            return;
        }

        const pending = this.requestQueue!.getPendingCount() + this.elevatedQueue!.getPendingCount();
        const needed = this.state.maxDiscoveries - this.state.visitedUrls.size;
        const isStarving = pending < needed;
        if (this.state.visitedUrls.has(data.url) && !isStarving) {
            logger.info(`[${caller}] Skipping save (already exists): ${data.url}`);
            this.emit('jobCompletion', { url: data.url, status: "skipped_already_exists" });
            return;
        }

        const scrapedData: Data = {
            url: data.url,
            title: data.title,
            content: data.content,
            stage: "raw",
            scrapedAt: new Date(),
        };

        if (!scrapedData.content || scrapedData.content.trim() === "" || !scrapedData.title || scrapedData.title.trim() === "") {
            if (data.isCheerio) await this.elevatedQueue?.addRequest({ url: data.url });
            else {
                logger.info(`[${caller}] Skipping save (empty content or title): ${data.url}`);
                this.emit('jobCompletion', { url: data.url, status: "skipped_empty" });
            }
            return;
        }

        if (!this.state.visitedUrls.has(data.url)) {
            this.state.dataBuffer.push(scrapedData);
            this.state.visitedUrls.add(data.url);
            if (this.state.dataBuffer.length >= this.config.flushGap) {
                try {
                    await this.storage.insertMany("raw", this.state.dataBuffer);
                } catch (err: any) {
                    if (err.code !== 11000) {
                        logger.error({ err }, `[${caller}] Error saving raw data batch to DB.`);
                        this.emit('jobError', { error: err });
                    }
                }
                this.state.dataBuffer = [];
            }
        }

        this.state.visitedUrls.add(data.url);
        this.emit('jobCompletion', scrapedData);
        await this.requestQueue!.addRequests(data.nestedUrls);
        if (this.state.maxDiscoveries !== -1 && this.state.visitedUrls.size >= this.state.maxDiscoveries) {
            this.stop("Max discoveries reached. Stopping crawler.");
        }
    }
}