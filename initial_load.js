process.chdir(__dirname);
load("./common.js");
load("./shard.js");

//----------------------------------------------------------------------

if (clusterIsSharded()) {
    sh.stopBalancer()
}

const batchSize = 50_000;

for (const useCustomID of customIDModes) {
    for (const docSize of docSizes) {
        const collName = `${useCustomID ? "customID" : "sequentialID"}_${docSize}`;
        const docsCount = Math.floor(collectionSize / docSize);
        console.log("collectionSize / docSize", collectionSize, docSize, collectionSize / docSize);

        console.log(`Creating collection: ${collName} (approx docs count: ${docsCount})`);

        if (db.getCollectionNames().includes(collName)) {
            console.log(`Collection “${collName}” already exists.`);
        } else {
            try {
                db.createCollection(collName);
            } catch(e) {
                if (e.code !== 48) {
                    throw `Failed to create collection “${collName}”: ${e}`;
                }

                console.log(`Collection “${collName}” already existed.`);
            }

            /*
            db[collName].createIndexes([
                {a: 1},
                {visitCount: 1},
                {updatedAt: 1},
                {randVal: 1},
                {flag: 1},
            ]);
            */

            if (clusterIsSharded()) {
                const maxDocID = useCustomID ? 1 : (1 + getShardNames().length) * 1_000_000_000

                sh.enableSharding(db.getName());

                const ns = `${db.getName()}.${collName}`;

                const shardKey = {_id: 1};

                if (!useCustomID) {
                    db[collName].createIndex({_id: "hashed"});
                    shardKey._id = "hashed";
                }

                console.log(`Sharding collection ${collName} …`)
                sh.shardCollection(ns, shardKey)

                if (useCustomID) {
                console.log(`Pre-splitting ${collName} …`)
                splitCollection(ns, "_id", 0, maxDocID);
                } else {
                    const chunks = db.getSiblingDB('config').chunks.find(
                        {ns: ns},
                        {shard: 1, min: 1, max: 1},
                    ).toArray();

                    const shardsWithChunks = new Set(
                        chunks.map(c => c.shard),
                    );

                    if (shardsWithChunks.size === getShardNames().length) {
                        console.log(`Forgoing split of ${collName}; already split.`);
                    } else {
                        throw new Error(`Shards: ${JSON.stringify(getShardNames())}; chunks: ${JSON.stringify(chunks)}`)
                    }
                }
            }
        }
    }
}

const LOG_INTERVAL_MS = 60_000; // Log every 60 seconds
let lastLogTime = 0;

function bytesToGiB(bytes) {
    return (bytes / (1024 ** 3)).toFixed(2);
}

while (true) {
    for (const useCustomID of customIDModes) {
        for (const docSize of docSizes) {
            const bigStr = "x".repeat(docSize);

            const collName = `${useCustomID ? "customID" : "sequentialID"}_${docSize}`;

            console.log(`Inserting into ${collName} …`);

            const docs = Array.from(
                { length: 100_000 },
                _ => ({
                    _id: useCustomID ? Math.random() : null,
                    str: bigStr,
                    num: Math.random(),
                    a: 1,
                }),
            );

            db[collName].insertMany(
                docs,
                {
                    ordered: false,
                    //writeConcern: { w: "majority", j: true },
                    writeConcern: { w: 0 },
                },
            );
        }

        // Log sizes of all collections
        if (Date.now() - lastLogTime >= LOG_INTERVAL_MS) {
            let totalSize = 0;
            for (const customID of customIDModes) {
                for (const size of docSizes) {
                    const name = `${customID ? "customID" : "sequentialID"}_${size}`;
                    const stats = db[name].stats();
                    const sizeGiB = bytesToGiB(stats.size);
                    console.log(`[${name}] Size: ${sizeGiB} GiB`);
                    totalSize += stats.size;
                }
            }
            console.log(`[TOTAL] Size: ${bytesToGiB(totalSize)} GiB`);
            lastLogTime = Date.now();
        }
    }
}
