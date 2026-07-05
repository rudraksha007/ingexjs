import { getMongooseConnector, DataModel } from "./packages/mongoose/index.ts";
import mongoose from "mongoose";

async function main() {
    const connector = getMongooseConnector("mongodb://localhost:27017/test-ingex");
    await connector.init();
    console.log("Connected");
    
    // Clear the collection first
    await DataModel.deleteMany({});
    
    const id = await connector.insert("raw", {
        url: "http://example.com/test",
        title: "Test",
        content: "Test Content"
    });
    
    console.log("Inserted ID:", id);
    
    const count = await connector.count("raw");
    console.log("Count in raw stage:", count);
    
    await mongoose.disconnect();
}

main().catch(console.error);
