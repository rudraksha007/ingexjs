import { logger } from "./logger";

/**
 * Configuration options for the Ingestor.
 */
export interface IngestorConfig {
    flushGap: number;
}

export type IngestorEvents = keyof IngestorHookCallback;

/**
 * Collection of hook callbacks for Ingestor lifecycle events.
 */
export type IngestorHookCallback = {
    'start': (data: { startUrls: string[], setUrls: (urls: string[]) => void, status: IngestorStatus }) => Promise<void> | void;
    'end': (data: { status: IngestorStatus }) => Promise<void> | void;
    'stopped': (data: { status: IngestorStatus }) => Promise<void> | void;
    'error': (data: { error: unknown, stack: string, state: IngestorStatus }) => Promise<void> | void;
    'jobError': (data: { error: unknown }) => Promise<void> | void;
    'jobCompletion': (data?: any) => Promise<void> | void;
    'jobQueueCompletion': (data?: any) => Promise<void> | void;
}

/**
 * Parameters passed to content handlers during scraping.
 */
export type ContentHandlerParams = {
    data: {
        url: string, title: string, content: string, isCheerio: boolean, nestedUrls: string[]
    },
    caller: "Cheerio" | "Playwright"
}

/**
 * Represents a registered hook for an Ingestor lifecycle event.
 */
export interface IngestorHook<T extends IngestorEvents = IngestorEvents> {
    id: string;
    event: T;
    callback: IngestorHookCallback[T];
}

/**
 * Represents a background processing stage in the Ingestor.
 */
export interface IngestorProcess {
    stage: string;
    workerCount: number;
    jobModule: string;
}
/**
 * Enumeration of possible Ingestor statuses.
 */
export enum IngestorStatus {
    idle = 0,
    paused = 1,
    stopping = 2,
    scraping = 3,
    filtering = 4,
    done = 5,
    error = 6,
    killed = 7
}
/**
 * The internal state of the Ingestor engine.
 */
export type IngestorState<T extends Data = Data> = {
    startUrls: string[],
    status: IngestorStatus,
    stage: string;
    maxDiscoveries: number,
    visitedUrls: Set<string>
    skipped: number,
    dataBuffer: T[]
}
/**
 * Default configuration values for the Ingestor.
 */
export const DefaultConfig: IngestorConfig = {
    flushGap: 100
};

/**
 * Message payload for initializing a worker.
 * @param type The type of the message
 * @param data Data to be sent to the worker
 * - moduleName The name of the module to be initialized
 */
export interface WorkerInitMessage {
    type: "INIT";
    data: {
        moduleName: string;
    }
}

/**
 * Message payload for assigning a job to a worker.
 * @param type The type of the message
 * @param id The id of the job
 * @param data Data to be sent to the worker
 */
export interface WorkerJobMessage<T = any> {
    type: "JOB";
    id: string;
    data: T;
}

/**
 * Message payload for a response from a worker.
 * @param type The type of the message
 * @param id The id of the job
 * @param data Data to be sent to the worker, if any
 * @param error Error message, if  any
 */
export interface WorkerResponseMessage<T = any> {
    type: "RESPONSE";
    id: string;
    data?: T;
    error?: any;
}

export type WorkerMessage = WorkerInitMessage | WorkerJobMessage | WorkerResponseMessage;

/**
 * Standardized data structure for scraped and processed content.
 * @param data Store for any additional data to be stored by the user
 */
export interface Data<T = unknown> {
    url: string;
    title: string;
    content: string;
    data?: T,
    stage: string;
    scrapedAt: Date;
}

/**
 * Interface defining the methods required for a storage connector.
 * Allows custom storage backends (e.g., MongoDB, PostgreSQL) to be used with the Ingestor.
 */
export interface StorageConnector<T extends Data = Data> {
    isInit: boolean;
    init: () => Promise<void> | void;
    insert: (stage: string, data: T) => Promise<string> | string;
    insertMany: (stage: string, data: T[]) => Promise<string[]> | string[];
    delete: (stage: string, id: string) => Promise<void> | void;
    deleteMany: (stage: string, ids: string[]) => Promise<void> | void;
    shift: (fromStage: string, toStage: string, id: string) => Promise<void> | void;
    shiftMany: (fromStage: string, toStage: string, ids: string[]) => Promise<void> | void;
    update: (stage: string, id: string, data: T) => Promise<void> | void;
    updateMany: (stage: string, ids: string[], data: T[]) => Promise<void> | void;
    count: (stage: string) => Promise<number> | number;
    getBatch: (stage: string, limit: number, skip?: number) => Promise<T[]> | T[];
}