const {MongoClient} = require('mongodb');
const {setWith, get} = require('lodash');
const {v4: uuid4} = require('uuid');

const {isThenable} = require('@raychee/utils');


module.exports = {

    type: 'mongodb',

    key(options) {
        if (options.db || options.collection) return;
        return makeFullOptions(options);
    },

    async create(options, {pluginLoader}) {
        options = makeFullOptions(options);
        const {db, collection, ...restOptions} = options;
        if (db || collection) {
            const plugin = await pluginLoader.get({type: 'mongodb', ...restOptions});
            return plugin.instance._use(this, db, collection);
        } else {
            return new MongoDB(this, options, pluginLoader);
        }
    },

    unload(mongodb, job) {
        const jobId = getJobId(job);
        const bulk = mongodb._state.bulk;
        if (bulk) delete bulk[jobId];
        const operate = mongodb._state.operate;
        if (operate) delete operate[jobId];
    },

    async destroy(mongodb) {
        if (!mongodb.options.db && !mongodb.options.collection) {
            await mongodb._close();
        }
    }

};


function makeFullOptions(
    {
        host, port, user, password, db, collection,
        connectionOptions = {},
        queryOptions = {},
        aggregationOptions = {},
        mapReduceOptions = {},
        bulkOptions = {},
        operateOptions = {},
        otherOptions = {},
    }
) {
    return {
        host, port, user, password, db, collection,
        connectionOptions: {
            useNewUrlParser: true, useUnifiedTopology: true,
            ...connectionOptions,
        },
        queryOptions: {
            batchSize: 1000,
            ...queryOptions,
        },
        aggregationOptions: {
            allowDiskUse: true,
            ...aggregationOptions,
        },
        mapReduceOptions,
        bulkOptions: {
            batchSize: 1000,
            concurrency: 1,
            ...bulkOptions,
        },
        operateOptions: {
            concurrency: 1,
            ...operateOptions,
        },
        otherOptions: {
            debug: false, showProgressEvery: undefined,
            ...otherOptions,
        }
    };
}

function getJobId(logger) {
    return get(logger, ['config', '_id'], '');
}

class MongoDB {

    constructor(logger, options, pluginLoader, _state = {}) {
        this.logger = logger;
        this.options = options;
        this.pluginLoader = pluginLoader;
        this._state = _state;
    }
    
    use(logger, db, collection) {
        return this.pluginLoader.bind(this._use(logger, db, collection), logger);
    }

    _use(logger, db, collection) {
        if (!db && !collection) return this;
        const options = this._makeNewNamespaceOptions(logger, db, collection);
        return new MongoDB(this.logger, options, this.pluginLoader, this._state);
    }
    
    async _loadNamespace(logger, db, collection) {
        const options = this._makeNewNamespaceOptions(logger, db, collection);
        return this.pluginLoader.get({type: 'mongodb', ...options}, logger);
    }
    
    _makeNewNamespaceOptions(logger, db, collection) {
        logger = logger || this.logger;
        let options = this.options;
        if (db && collection) {
            options = {...this.options, db, collection};
        } else if (db) {
            if (this.options.db) {
                options = {...this.options, collection: db};
            } else {
                options = {...this.options, db};
            }
        } else if (collection) {
            logger.crash('plugin_mongodb_error', 'cannot call this.use() without db');
        } else {
            logger.crash('plugin_mongodb_error', 'cannot call this.use() without db and collection');
        }
        return options;
    }
    
    get dbName() {
        if (!this.options.db) {
            this.logger.crash('plugin_mongodb_error', 'this._db is undefined');
        }
        return this.options.db;
    }
    
    get collectionName() {
        if (!this.options.collection) {
            this.logger.crash('plugin_mongodb_error', 'this._collection is undefined');
        }
        return this.options.collection;
    }

    get namespace() {
        const {db, collection} = this.options;
        if (!db || !collection) {
            this.logger.crash('plugin_mongodb_error', 'this._db or this._collection is undefined');
        }
        return `${db}.${collection}`;
    }
    
    get namespaceObject() {
        const {db, collection} = this.options;
        if (!db || !collection) {
            this.logger.crash('plugin_mongodb_error', 'this._db or this._collection is undefined');
        }
        return {db, coll: collection};
    }
    
    aggregate(logger, pipeline, options) {
        logger = logger || this.logger;
        options = {...this.options.aggregationOptions, ...options};
        const {debug, ...opts} = options;
        if (debug || this.options.otherOptions.debug) {
            const {db, collection} = this.options;
            logger.debug('mongodb.use(', db, ', ', collection, ').aggregate(', pipeline, ', ', opts, ');');
        }
        return this._handleCursor(
            logger, this._getCollection(logger).then(coll => coll.aggregate(pipeline, opts))
        );
    }

    mapReduce(logger, map, reduce, options) {
        logger = logger || this.logger;
        options = {...this.options.mapReduceOptions, ...options};
        const {debug, ...opts} = options;
        if (debug || this.options.otherOptions.debug) {
            const {db, collection} = this.options;
            logger.debug('mongodb.use(', db, ', ', collection, ').mapReduce(', map, ', ', reduce, ', ', opts, ');');
        }
        return this._handlePromise(
            logger, this._getCollection(logger).then(coll => coll.mapReduce(map, reduce, opts))
        );
    }

    find(logger, query, options) {
        logger = logger || this.logger;
        options = {...this.options.queryOptions, ...options};
        const {debug, ...opts} = options;
        if (debug || this.options.otherOptions.debug) {
            const {db, collection} = this.options;
            logger.debug('mongodb.use(', db, ', ', collection, ').find(', query, ', ', opts, ');');
        }
        return this._handleCursor(
            logger, this._getCollection(logger).then(coll => coll.find(query, opts))
        );
    }

    findOne(logger, query, options) {
        logger = logger || this.logger;
        options = {...this.options.queryOptions, ...options};
        const {debug, ...opts} = options;
        if (debug || this.options.otherOptions.debug) {
            const {db, collection} = this.options;
            logger.debug('mongodb.use(', db, ', ', collection, ').findOne(', query, ', ', opts, ');');
        }
        return this._handlePromise(
            logger, this._getCollection(logger).then(coll => coll.findOne(query, opts))
        );
    }

    countDocuments(logger, query, options) {
        logger = logger || this.logger;
        options = {...this.options.queryOptions, ...options};
        const {debug, ...opts} = options;
        if (debug || this.options.otherOptions.debug) {
            const {db, collection} = this.options;
            logger.debug('mongodb.use(', db, ', ', collection, ').countDocuments(', query, ', ', opts, ');');
        }
        return this._handlePromise(
            logger, this._getCollection(logger).then(coll => coll.countDocuments(query, opts))
        );
    }

    updateMany(logger, filter, update, options = {}) {
        logger = logger || this.logger;
        const {debug, ...opts} = options;
        if (debug || this.options.otherOptions.debug) {
            const {db, collection} = this.options;
            logger.debug('mongodb.use(', db, ', ', collection, ').updateMany(', filter, ', ', update, ', ', opts, ');');
        }
        return this._handlePromise(
            logger, this._getCollection(logger).then(coll => coll.updateMany(filter, update, opts))
        );
    }

    updateOne(logger, filter, update, options = {}) {
        logger = logger || this.logger;
        const {debug, ...opts} = options;
        if (debug || this.options.otherOptions.debug) {
            const {db, collection} = this.options;
            logger.debug('mongodb.use(', db, ', ', collection, ').updateOne(', filter, ', ', update, ', ', opts, ');');
        }
        return this._handlePromise(
            logger, this._getCollection(logger).then(coll => coll.updateOne(filter, update, opts))
        );
    }

    deleteMany(logger, filter, options = {}) {
        logger = logger || this.logger;
        const {debug, ...opts} = options;
        if (debug || this.options.otherOptions.debug) {
            const {db, collection} = this.options;
            logger.debug('mongodb.use(', db, ', ', collection, ').deleteMany(', filter, ', ', opts, ');');
        }
        return this._handlePromise(
            logger, this._getCollection(logger).then(coll => coll.deleteMany(filter, opts))
        );
    }
    
    async operate(logger, fn, options = {}) {
        logger = logger || this.logger;
        const {db, collection} = this.options;
        let key;
        if (db && collection) {
            key = ['operate', getJobId(logger), 'collection'];
        } else if (db) {
            key = ['operate', getJobId(logger), 'db'];
        } else {
            key = ['operate', getJobId(logger), 'client'];
        }
        let state = get(this._state, key);
        if (!state) {
            state = {running: {}, errors: []};
            setWith(this._state, key, state, Object);
        }
        options = {...this.options.operateOptions, ...options};
        const {concurrency, debug} = options;
        const {running, errors} = state;
        const opId = uuid4();
        while (Object.keys(running).length >= concurrency) {
            if (debug || this.options.otherOptions.debug) {
                logger.debug('Wait for concurrency before operation: id = ', opId, '.');
            }
            await Promise.race(Object.values(running));
        }
        if (errors.length > 0) {
            const [error] = errors;
            throw error;
        }
        if (debug || this.options.otherOptions.debug) {
            logger.debug(
                'Operate: id = ', opId, ', concurrency = ', 
                Object.keys(running).length + 1, '/', concurrency, '.'
            );
        }
        running[opId] = fn()
            .catch(e => errors.push(e))
            .finally(() => {
                delete running[opId];
                if (debug || this.options.otherOptions.debug) {
                    logger.debug(
                        'Complete operation: id = ', opId, 
                        ', concurrency = ', Object.keys(running).length, '/', concurrency, '.'
                    );
                }
            });
    }
    
    async operateFlush(logger) {
        logger = logger || this.logger;
        const {db, collection} = this.options;
        let key;
        if (db && collection) {
            key = ['operate', getJobId(logger), 'collection'];
        } else if (db) {
            key = ['operate', getJobId(logger), 'db'];
        } else {
            key = ['operate', getJobId(logger), 'client'];
        }
        let state = get(this._state, key);
        if (!state) return;
        const running = Object.values(state.running);
        if (running.length > 0) {
            await Promise.all(running);
        }
        if (state.errors.length > 0) {
            const [error] = state.errors;
            state.errors = [];
            throw error;
        }
    }

    insertMany(logger, docs, options = {}) {
        logger = logger || this.logger;
        const {debug, ...opts} = options;
        if (debug || this.options.otherOptions.debug) {
            const {db, collection} = this.options;
            logger.debug('mongodb.use(', db, ', ', collection, ').insertMany(', docs, ', ', opts, ');');
        }
        return this._handlePromise(
            logger, this._getCollection(logger).then(coll => coll.insertMany(docs, opts))
        );
    }

    bulkWrite(logger, operations, options = {}) {
        logger = logger || this.logger;
        const {debug, ...opts} = options;
        if (debug || this.options.otherOptions.debug) {
            const {db, collection} = this.options;
            logger.debug('mongodb.use(', db, ', ', collection, ').bulkWrite(', operations, ', ', opts, ');');
        }
        return this._handlePromise(
            logger, this._getCollection(logger).then(coll => coll.bulkWrite(operations, opts))
        );
    }

    async bulkOperate(logger, operations, options = {}) {
        logger = logger || this.logger;
        const {db, collection} = this.options;
        if (!db || !collection) {
            logger.crash('plugin_mongodb_error', 'this._db or this._collection is undefined');
        }
        if (!operations) {
            logger.crash('plugin_mongodb_error', 'operation must be provided!');
        }
        if (!Array.isArray(operations)) operations = [operations];
        if (operations.length <= 0) return;
        options = {...this.options.bulkOptions, ...options};
        let context = get(this._state, ['bulk', getJobId(logger), db, collection]);
        if (!context) {
            context = {operations: [], running: {}, errors: [], result: {}};
            setWith(this._state, ['bulk', getJobId(logger), db, collection], context, Object);
        }
        while (context.committing) {
            await context.committing;
        }
        while (context.operations.length >= options.batchSize) {
            if (!context.committing) {
                context.committing = this._bulkCommit(logger, context, options).finally(() => delete context.committing);
            }
            await context.committing;
        }
        context.operations.push(...operations);
    }

    async _bulkCommit(logger, context, {debug, concurrency, ...opts}) {
        logger = logger || this.logger;
        const opId = uuid4();
        const {operations, running, errors, result} = context;
        if (operations.length <= 0) return;
        while (Object.keys(running).length >= concurrency) {
            if (debug || this.options.otherOptions.debug) {
                logger.debug(
                    'Wait for concurrency before bulkWrite: id = ', opId,
                    ', size = ', operations.length, '.'
                );
            }
            await Promise.race(Object.values(running));
        }
        if (errors.length > 0) {
            const [error] = errors;
            throw error;
        }
        if (debug || this.options.otherOptions.debug) {
            logger.debug(
                'BulkWrite: id = ', opId, ', size = ', operations.length,
                ', concurrency = ', Object.keys(running).length + 1, '/', concurrency, '.'
            );
        }
        running[opId] = this.bulkWrite(logger, operations, {debug, ...opts})
            .then(r => {
                for (const [p, v] of Object.entries(r)) {
                    if (typeof v === 'number') {
                        result[p] = (result[p] || 0) + v; 
                    }
                }
            })
            .catch(e => errors.push(e))
            .finally(() => {
                delete running[opId];
                if (debug || this.options.otherOptions.debug) {
                    logger.debug(
                        'Complete a bulkWrite: id = ', opId, ', size = ', operations.length,
                        ', concurrency = ', Object.keys(running).length, '/', concurrency, '.'
                    );
                }
            });
        context.operations = [];
    }

    async bulkFlush(logger, options = {}) {
        logger = logger || this.logger;
        const {db, collection} = this.options;
        if (!db || !collection) {
            logger.crash('plugin_mongodb_error', 'this._db or this._collection is undefined');
        }
        const context = get(this._state, ['bulk', getJobId(logger), db, collection]);
        if (!context) return {};
        if (context.operations.length > 0) {
            if (!context.committing) {
                context.committing = this._bulkCommit(logger, context, options).finally(() => delete context.committing);
            }
            await context.committing;
        }
        const running = Object.values(context.running);
        if (running.length > 0) {
            await Promise.all(running);
        }
        if (context.errors.length > 0) {
            const [error] = context.errors;
            context.errors = [];
            context.result = {};
            throw error;
        }
        const {result} = context;
        context.result = {};
        return result;
    }
    
    collections(logger, options = {}) {
        logger = logger || this.logger;
        const {debug, ...opts} = options;
        if (debug || this.options.otherOptions.debug) {
            const {db} = this.options;
            logger.debug('mongodb.use(', db, ').collections(', opts, ');');
        }
        return this._handlePromise(
            logger, this._getDb(logger).then(db => db.collections(opts))
        );
    }
    
    drop(logger, options = {}) {
        logger = logger || this.logger;
        const {debug, ...opts} = options;
        if (debug || this.options.otherOptions.debug) {
            const {db, collection} = this.options;
            logger.debug('mongodb.use(', db, ', ', collection, ').drop(', opts, ');');
        }
        return this._handlePromise(
            logger, this._getCollection(logger).then(coll => coll.drop(opts).catch(e => {
                if (e.message.match(/ns not found/)) {
                    return null;
                } else {
                    throw e;
                }
            }))
        );
    }

    createIndexes(logger, indexSpecs, options = {}) {
        logger = logger || this.logger;
        const {debug, ...opts} = options;
        if (debug || this.options.otherOptions.debug) {
            const {db, collection} = this.options;
            logger.debug('mongodb.use(', db, ', ', collection, ').createIndexes(', indexSpecs, ', ', opts, ');');
        }
        return this._handlePromise(
            logger, this._getCollection(logger).then(coll => coll.createIndexes(indexSpecs, opts))
        );
    }

    watch(logger, pipeline, options) {
        logger = logger || this.logger;
        const {debug, ...opts} = options;
        if (debug || this.options.otherOptions.debug) {
            const {db, collection} = this.options;
            logger.debug('mongodb.use(', db, ', ', collection, ').watch(', pipeline, ', ', opts, ');');
        }
        return this._handleCursor(
            logger, this._getCollection(logger).then(coll => coll.watch(pipeline, opts))
        );
    }
    
    async _getClient() {
        const {host, port, user, password, connectionOptions} = this.options;
        if (!this._state.client) {
            const auth = user && password ? `${encodeURIComponent(user)}:${encodeURIComponent(password)}@` : '';
            const uri = `mongodb://${auth}${host}${port ? `:${port}` : ''}`;
            const client = MongoClient(uri, connectionOptions);
            await client.connect();
            this._state.client = client;
        }
        return this._state.client;
    }
    
    async _getDb(logger) {
        logger = logger || this.logger;
        const {db} = this.options;
        if (!db) {
            logger.crash('plugin_mongodb_error', 'this._db is undefined');
        }
        const client = await this._getClient();
        return client.db(db);
    }

    async _getCollection(logger) {
        logger = logger || this.logger;
        const {db, collection} = this.options;
        if (!db || !collection) {
            logger.crash('plugin_mongodb_error', 'this.options.db or this.options.collection is undefined');
        }
        let coll = get(this._state, ['pool', db, collection]);
        if (!coll) {
            const db_ = await this._getDb(logger);
            coll = db_.collection(collection);
            setWith(this._state, ['pool', db, collection], coll, Object);
        }
        return coll;
    }

    async _close() {
        if (this._state.client) {
            await this._state.client.close();
            this._state = {};
        }
    }

    _handlePromise(logger, promise) {
        logger = logger || this.logger;
        return promise.catch(error => {
            switch (error.name) {
                case 'MongoNetworkError':
                case 'MongoTimeoutError':
                case 'MongoWriteConcernError':
                    logger.fail(error.name, error);
                    break;
                default:
                    throw error;
            }
        });
    }

    _handleCursor(logger, cursor) {
        logger = logger || this.logger;
        const {queryOptions, otherOptions} = this.options;
        let ops = [], target = cursor;
        return new Proxy({}, {
            get: (_, p, receiver) => {
                switch (p) {
                    case Symbol.asyncIterator:
                        return async function* () {
                            let count = 0;
                            while (await receiver.hasNext()) {
                                count++;
                                if (count % otherOptions.showProgressEvery === 0) {
                                    process.stdout.write('.');
                                }
                                yield await receiver.next();
                            }
                            if (otherOptions.showProgressEvery) {
                                process.stdout.write('✓\n');
                            }
                        };
                    case 'batch':
                        return async function* (batchSize) {
                            batchSize = batchSize || queryOptions.batchSize;
                            let batch = [];
                            let count = 0;
                            while (await receiver.hasNext()) {
                                count++;
                                if (count % otherOptions.showProgressEvery === 0) {
                                    process.stdout.write('.');
                                }
                                batch.push(await receiver.next());
                                if (batch.length >= batchSize) {
                                    yield batch;
                                    batch = [];
                                }
                            }
                            if (batch.length > 0) {
                                yield batch;
                            }
                            if (otherOptions.showProgressEvery) {
                                process.stdout.write('✓\n');
                            }
                        };
                    case 'then':
                        return (onFulfilled, onRejected) => {
                            return receiver.next().then(onFulfilled, onRejected);
                        };
                    case 'close':
                    case 'count':
                    case 'explain':
                    case 'forEach':
                    case 'hasNext':
                    case 'next':
                    case 'toArray':
                        return (...args) => {
                            return this._handlePromise(logger, (async () => {
                                if (isThenable(target)) {
                                    target = await target;
                                }
                                for (const {method, args} of ops) {
                                    target = target[method](...args);
                                }
                                ops = [];
                                return await target[p](...args);
                            })());
                        };
                    case 'addCursorFlag':
                    case 'addQueryModifier':
                    case 'batchSize':
                    case 'collation':
                    case 'comment':
                    case 'filter':
                    case 'hint':
                    case 'limit':
                    case 'map':
                    case 'max':
                    case 'maxAwaitTimeMS':
                    case 'maxTimeMS':
                    case 'min':
                    case 'project':
                    case 'setReadPreference':
                    case 'showRecordId':
                    case 'skip':
                    case 'sort':
                        return (...args) => {
                            ops.push({method: p, args});
                            return receiver;
                        };
                    default:
                        logger.crash('plugin_mongodb_error', 'method "', p, '" on cursor is not supported in mongodb plugin');
                }
            }
        });
    }

}
