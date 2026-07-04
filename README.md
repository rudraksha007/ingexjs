# Ingex

Ingex is a high-performance data ingestion and processing pipeline built on top of `crawlee`. It allows you to build complex multi-stage scraping and data processing workflows using a built-in coordinator, web workers, and extensible storage connectors.

## Features

- **Robust Scraping:** Powered by `crawlee`, seamlessly combining `CheerioCrawler` (fast) and `PlaywrightCrawler` (fallback/dynamic content).
- **Multi-Stage Processing Pipeline:** Define custom processing stages using multi-threaded Web Workers.
- **Dynamic Storage Connectors:** Abstracted storage mechanisms that make saving and moving data between processing stages seamless.
- **Fault-Tolerant Coordinators:** In-built batch processing and error tracking to keep your pipelines stable.

## Installation

Ingex is structured as a monorepo workspace. The core logic lives in `@ingex/core`.

To install the core engine:

```bash
bun add @ingex/core
```

### Installing Connectors

To save your data and manage state, you will need a Storage Connector. Ingex comes with an official Mongoose connector:

```bash
bun add @ingex/mongoose mongoose
```

## Quick Start

You start by initializing your storage connector and passing it to the `Ingestor`.

```typescript
import { Ingestor } from "@ingex/core";
import { getMongooseConnector } from "@ingex/mongoose";

// 1. Initialize your storage connector
const storage = getMongooseConnector("mongodb://localhost:27017/ingex");

// 2. Pass it to the Ingestor
const ingestor = new Ingestor(storage, {
    flushGap: 100, // Batch save frequency
});

// 3. Register your custom data processing stages
ingestor.register("filter", "./workers/filter-worker.ts", 5);

// 4. Start Crawling
await ingestor.start("crawl");

// 5. Start a processing stage (takes data from the 'crawl' stage and applies 'filter')
await ingestor.start("filter", "crawl");
```

## Packages Documentation

For detailed API specifications, hooks, and advanced usage, refer to the individual package documentation:

- [@ingex/core](./packages/core/README.md)
- [@ingex/mongoose](./packages/mongoose/README.md)
