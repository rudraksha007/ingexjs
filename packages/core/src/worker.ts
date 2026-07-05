import type { WorkerMessage, WorkerResponseMessage } from "./types";

/**
 * Global self reference for the web worker context.
 */
declare var self: any;

/**
 * Stores the currently loaded module's default export function to handle jobs.
 */
let jobHandler: ((data: any) => any | Promise<any>) | undefined = undefined;

/**
 * Main message event listener for the worker.
 * Handles INIT messages to load modules and JOB messages to execute tasks.
 */
self.onmessage = async (event: any) => {
    const msg = event.data as WorkerMessage;

    if (msg.type === "INIT") {
        try {
            const moduleName = msg.data.moduleName;
            // Dynamically load the module and get the default export
            const mod = await import(moduleName);
            if (typeof mod.default !== "function") {
                throw new Error(`Module ${moduleName} does not export a default function.`);
            }
            jobHandler = mod.default;
            self.postMessage({
                type: "RESPONSE",
                id: msg.data.moduleName, // Just use moduleName as id for INIT response
                data: "ok"
            } as WorkerResponseMessage);
        } catch (error: any) {
            console.error(`Worker failed to initialize module:`, error);
            self.postMessage({
                type: "RESPONSE",
                id: msg.data.moduleName,
                error: error instanceof Error ? error.message : String(error)
            } as WorkerResponseMessage);
        }
    } else if (msg.type === "JOB") {
        if (!jobHandler) {
            self.postMessage({
                type: "RESPONSE",
                id: msg.id,
                error: "Worker is not initialized yet."
            } as WorkerResponseMessage);
            return;
        }
        
        try {
            // Execute the default function passing the message data
            const result = await jobHandler(msg.data);
            self.postMessage({
                type: "RESPONSE",
                id: msg.id,
                data: result
            } as WorkerResponseMessage);
        } catch (error: any) {
            self.postMessage({
                type: "RESPONSE",
                id: msg.id,
                error: error instanceof Error ? error.message : String(error)
            } as WorkerResponseMessage);
        }
    }
};