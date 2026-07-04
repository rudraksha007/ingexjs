# @ingex/mongoose

The official MongoDB storage connector for Ingex. It seamlessly integrates Mongoose into the Ingex pipeline, managing database records, batch operations, and stage transitions.

## Installation

```bash
bun add @ingex/mongoose mongoose
```

## Setup

```typescript
import { getMongooseConnector } from "@ingex/mongoose";
import { Ingestor } from "@ingex/core";

const connector = getMongooseConnector("mongodb://localhost:27017/my_database");

const ingestor = new Ingestor(connector);
```

## How It Works

The connector manages a single Mongoose model (`DataModel`) with a highly flexible schema. 

### The `stage` field
Instead of creating different collections for different processing stages, this connector uses a `stage` string field on the document. 
- When the crawler runs, documents are saved with `stage: 'crawl'`.
- When the coordinator processes a batch for a new stage (e.g., `'filter'`), the processed items are inserted with `stage: 'filter'`, and the old items are removed from the `'crawl'` stage.

### The `data` field (Extra Properties)
The connector's schema is structured around a core set of fields: `url`, `title`, `content`, and `scrapedAt`.

If your workers return objects containing extra properties, the Mongoose connector automatically extracts those extra properties and nests them securely inside the `data` field (of type `Mixed`). This keeps the schema clean while allowing infinite flexibility.

## API Specification

The `getMongooseConnector` function returns an implementation of the `StorageConnector` interface expected by `@ingex/core`:

- `init()`: Connects to MongoDB.
- `insert(stage, item)`: Inserts a single item under the specified stage.
- `insertMany(stage, items)`: Batch inserts items under the specified stage.
- `getBatch(stage, limit, skip)`: Fetches a batch of items belonging to a stage for processing.
- `count(stage)`: Returns the total number of items in a stage.
- `delete(stage, id)` / `deleteMany(stage, ids)`: Removes items from a stage.
- `shift(fromStage, toStage, id)` / `shiftMany(fromStage, toStage, ids)`: Efficiently updates the `stage` field on documents without moving them.
- `update(stage, id, item)` / `updateMany(stage, ids, items)`: Updates specific documents, smartly updating standard fields and merging custom properties into the `data` field.

## Advanced Usage

You can also access the raw `DataModel` if you need to perform manual queries outside of the Ingex pipeline:

```typescript
import { DataModel } from "@ingex/mongoose";

// Find all items that successfully passed the 'summarize' stage
const summaries = await DataModel.find({ stage: 'summarize' }).lean();
```
