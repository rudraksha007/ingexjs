import { type Data, type StorageConnector } from "@ingexjs/core";
import { Schema, connect, model, type ConnectOptions } from "mongoose";
import { logger } from "../core/src/logger";



const DataSchema = new Schema<Data>({
    url: { type: String, required: true },
    title: { type: String, required: true },
    content: { type: String, required: true },
    data: { type: Schema.Types.Mixed, required: false },
    stage: { type: String, required: true, default: "raw" },
    scrapedAt: { type: Date, required: true, default: Date.now },
});

DataSchema.index({ url: 1 }, { unique: true });

export const DataModel = model<Data>("CrawledData", DataSchema);

export function getMongooseConnector(url: string, opts?: ConnectOptions): StorageConnector<any> {

    const extractDoc = (stage: string, item: any) => {
        const { url, title, content, scrapedAt, data, _id, __v, stage: _, ...rest } = item;
        const extra = data !== undefined ? data : rest;
        return {
            url, title, content, scrapedAt, stage, data: extra
        };
    };

    const extractUpdateDoc = (item: any) => {
        const { url, title, content, scrapedAt, data, _id, __v, stage: _, ...rest } = item;
        const updateDoc: any = {};
        if (url !== undefined) updateDoc.url = url;
        if (title !== undefined) updateDoc.title = title;
        if (content !== undefined) updateDoc.content = content;
        if (scrapedAt !== undefined) updateDoc.scrapedAt = scrapedAt;

        if (data !== undefined) {
            updateDoc.data = data;
        } else if (Object.keys(rest).length > 0) {
            updateDoc.data = rest;
        }
        return updateDoc;
    };

    const connector = {
        isInit: false,
        async init() {
            if (this.isInit) return;
            await connect(url, opts);
            logger.info("Connected to MongoDB");
            this.isInit = true;
        },
        insert: async (stage: string, item: any) => {
            try {
                const doc = new DataModel(extractDoc(stage, item));
                await doc.save();
                return doc._id.toString();
            } catch (err: any) {
                if (err.code === 11000 || err.message?.includes("duplicate key")) return "";
                throw err;
            }
        },
        insertMany: async (stage: string, items: any[]) => {
            if (items.length === 0) return [];
            try {
                const ops = items.map(item => ({
                    updateOne: {
                        filter: { url: item.url },
                        update: { $set: extractDoc(stage, item) },
                        upsert: true
                    }
                }));
                await DataModel.bulkWrite(ops, { ordered: false });
                return items.map((item: any) => item.url);
            } catch (err: any) {
                if (err.code === 11000 || err.message?.includes("11000") || err.message?.includes("duplicate key")) {
                    return [];
                }
                throw err;
            }
        },
        delete: async (stage: string, id: string) => {
            await DataModel.findOneAndDelete({ _id: id, stage });
        },
        deleteMany: async (stage: string, ids: string[]) => {
            await DataModel.deleteMany({ _id: { $in: ids }, stage });
        },
        shift: async (fromStage: string, toStage: string, id: string) => {
            await DataModel.findOneAndUpdate({ _id: id, stage: fromStage }, { stage: toStage });
        },
        shiftMany: async (fromStage: string, toStage: string, ids: string[]) => {
            await DataModel.updateMany({ _id: { $in: ids }, stage: fromStage }, { stage: toStage });
        },
        update: async (stage: string, id: string, item: any) => {
            await DataModel.findOneAndUpdate({ _id: id, stage }, { $set: extractUpdateDoc(item) });
        },
        updateMany: async (stage: string, ids: string[], items: any[]) => {
            await Promise.all(ids.map((id, i) =>
                DataModel.findOneAndUpdate({ _id: id, stage }, { $set: extractUpdateDoc(items[i]) })
            ));
        },
        count: async (stage: string) => {
            return await DataModel.countDocuments({ stage });
        },
        getBatch: async (stage: string, limit: number, skip: number = 0) => {
            return await DataModel.find({ stage }).skip(skip).limit(limit).lean();
        }
    };
    return connector;
}