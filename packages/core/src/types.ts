import { logger } from "./logger";

export interface IngestorConfig {
    flushGap: number;
}

export type IngestorEvents = keyof IngestorHookCallback;

export type IngestorHookCallback = {
    'start': (data: { startUrls: string[], setUrls: (urls: string[]) => void, status: IngestorStatus }) => Promise<void> | void;
    'end': (data: { status: IngestorStatus }) => Promise<void> | void;
    'stopped': (data: { status: IngestorStatus }) => Promise<void> | void;
    'error': (data: { error: unknown, stack: string, state: IngestorStatus }) => Promise<void> | void;
    'jobError': (data: { error: unknown }) => Promise<void> | void;
    'jobCompletion': (data?: any) => Promise<void> | void;
    'jobQueueCompletion': (data?: any) => Promise<void> | void;
}

export type ContentHandlerParams = {
    data: {
        url: string, title: string, content: string, isCheerio: boolean, nestedUrls: string[]
    },
    caller: "Cheerio" | "Playwright"
}

export interface IngestorHook<T extends IngestorEvents = IngestorEvents> {
    id: string;
    event: T;
    callback: IngestorHookCallback[T];
}

export interface IngestorProcess {
    stage: string;
    workerCount: number;
    jobModule: string;
}
export enum IngestorStatus {
    idle = 0,
    paused = 1,
    scraping = 2,
    filtering = 3,
    done = 4,
    error = 5,
    killed = 6
}
export type IngestorState<T extends Data = Data> = {
    startUrls: string[],
    status: IngestorStatus,
    stage: string;
    maxDiscoveries: number,
    visitedUrls: Set<string>
    skipped: number,
    dataBuffer: T[]
}
export const DefaultConfig: IngestorConfig = {
    flushGap: 100
};

export interface WorkerInitMessage {
    type: "INIT";
    data: {
        moduleName: string;
    }
}

export interface WorkerJobMessage<T = any> {
    type: "JOB";
    id: string;
    data: T;
}

export interface WorkerResponseMessage<T = any> {
    type: "RESPONSE";
    id: string;
    data?: T;
    error?: any;
}

export type WorkerMessage = WorkerInitMessage | WorkerJobMessage | WorkerResponseMessage;

export interface Data<T = unknown> {
    url: string;
    title: string;
    content: string;
    data?: T,
    stage: string;
    scrapedAt: Date;
}

export interface StorageConnector<T extends Data = Data> {
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