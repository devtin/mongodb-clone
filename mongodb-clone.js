#!/usr/bin/env node

const { MongoClient } = require('mongodb');
const { streamFlowAsync } = require('stream-flow-async');
const { SingleBar } = require('cli-progress');
const fs = require('fs');
const path = require('path');

/**
 *
 * @param {db} dbConfig
 * @returns {Promise<*>}
 */
const connect = async ({ database, credentials }) => {
    const connection = await MongoClient.connect(credentials, { useNewUrlParser: true, useUnifiedTopology: true });
    const db = connection.db(database);

    db.connection = connection;
    db.close = () => connection.close();

    return db;
};

const configFile = path.resolve(process.cwd(), process.argv[2] || 'mongodb-clone.config.js');

if (!fs.existsSync(configFile)) {
    console.log(`create a file name ${path.basename(configFile)} with the following content:\n`)
    console.log(`module.exports = {
  src: {
    credentials: 'mongodb://user:password@src-host:27017',
    database: 'my_database',
    collection: 'my_collection',
    query: {} // query to retrieve the documents
  },
  dst: {
    credentials: 'mongodb://user:password@dst-host:27017',
    database: 'my_database',
    collection: \`my_database_backup_\${new Date().toLocaleDateString("en-US").replace(/\\//g, '_')}\`
  },
  flow: 1000 // amount of concurrent documents processed at once; avoids back-pressure
}`);
    process.exit(1);
}

const { src, dst } = require(configFile);

/**
 * @typedef {Object} db
 * @property {String} credentials - mongoDb connection uri
 * @property {String} database - database name
 * @property {String} collection - mongoDb collection name
 */

/**
 *
 * @param {db} src
 * @param {db} dst
 * @param {Object} [options]
 * @param {Number} [options.flow=1000] - amount of entries to process simultaneously
 * @returns {Promise<{ processed: number, succeed: number, errored: number }>}
 */
async function clone (src, dst, { flow = 1000 } = {}) {
    const progressBar = new SingleBar({
        format: 'cloning [{bar}] {percentage}% | ETA: {eta}s | {value}/{total} | Succeed: {succeed} | Errors: {errored}'
    });

    let processed = 0;
    let succeed = 0;
    let errored = 0;

    const srcConn = await connect(src);
    const dstConn = await connect(dst);

    const srcCollection = srcConn.collection(src.collection);
    const dstCollection = dstConn.collection(dst.collection);

    const amountOfDocs = await srcCollection.countDocuments(src.query);
    progressBar.start(amountOfDocs, 0, { succeed, errored });

    const collectionStream = srcCollection.find(src.query).stream();

    await streamFlowAsync({
        stream: collectionStream,
        flow,
        async handler(contact) {
            try {
                await dstCollection.insertOne(contact);
                succeed++;
            } catch (error) {
                errored++;
            }

            processed++;
            progressBar.update(processed, { succeed, errored });
        },
    });

    srcConn.close();
    dstConn.close();

    progressBar.stop();

    return { processed, succeed, errored }
}

clone(src, dst)
    .then(({ processed, succeed, errored }) => console.log(`${processed} entries processed; ${succeed} succeed and ${errored} errors.`));

