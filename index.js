const {MongoClient} = require('mongodb');
const {setWith, get} = require('lodash');
const {v4: uuid4} = require('uuid');

const {isThenable, limit} = require('@raychee/utils');


module.exports = {

    type: 'mongodb',

    key(options) {
        return makeFullOptions(options);
    },

    async create(options, {pluginLoader}) {
        options = makeFullOptions(options);
        const {db, collection, ...restOptions} = options;
        if (db || collection) {
            const plugin = await pluginLoader.get({type: 'mongodb', ...restOptions});
            return plugin.use(db, collection);
        } else {
            return new MongoDB(this, options, pluginLoader);
        }
    },
    
    unload(mongodb, job) {
        const bulk = mongodb._state.bulk;
        if (bulk) delete bulk[getJobId(job)];
    },

    async destroy(mongodb) {
        await mongodb._close();
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
        otherOptions: {
            debug: false, showProgressEvery: undefined,
            ...otherOptions,
        }
    };
}

function getJobId(logger) {
    return get(logger, ['config', 'id'], '');
}

class MongoDB {

    constructor(logger, options, pluginLoader, _state = {}) {
        this.logger = logger;
        this.options = options;
        this.pluginLoader = pluginLoader;
        this._state = _state;
        this.bulkOperate = limit(MongoDB.prototype.bulkOperate.bind(this), 1);
    }

    use(logger, db, collection) {
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
            logger.crash('internal', 'cannot call this.use(collection) without db');
        } else {
            return this;
        }
        return this.pluginLoader.create(new MongoDB(this.logger, options, this.pluginLoader, this._state), logger);
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
            logger, this._connect(logger).then(coll => coll.aggregate(pipeline, opts))
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
            logger, this._connect(logger).then(coll => coll.mapReduce(map, reduce, opts))
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
            logger, this._connect(logger).then(coll => coll.find(query, opts))
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
            logger, this._connect(logger).then(coll => coll.findOne(query, opts))
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
            logger, this._connect(logger).then(coll => coll.countDocuments(query, opts))
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
            logger, this._connect(logger).then(coll => coll.updateMany(filter, update, opts))
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
            logger, this._connect(logger).then(coll => coll.deleteMany(filter, opts))
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
            logger, this._connect(logger).then(coll => coll.bulkWrite(operations, opts))
        );
    }

    async bulkOperate(logger, operation, options = {}) {
        logger = logger || this.logger;
        const {db, collection} = this.options;
        if (!db || !collection) {
            logger.crash('internal', 'this._db or this._collection is undefined');
        }
        if (operation.length <= 0) return;
        options = {...this.options.bulkOptions, ...options};
        let bulk = get(this._state, ['bulk', getJobId(logger), db, collection]);
        if (!bulk) {
            bulk = {operations: [], running: {}, errors: []};
            setWith(this._state, ['bulk', getJobId(logger), db, collection], bulk, Object);
        }
        bulk.operations.push(operation);
        if (bulk.operations.length > options.batchSize) {
            if (bulk.flushing) {
                await bulk.flushing;
            }
            await this._bulkCommit(logger, bulk, options);
        }
    }
    
    async _bulkCommit(logger, bulk, {debug, concurrency, ...opts}) {
        logger = logger || this.logger;
        const opId = uuid4();
        const {operations, running, errors} = bulk;
        if (errors.length > 0) {
            throw errors[0];
        }
        while (Object.keys(running).length >= concurrency) {
            if (debug || this.options.otherOptions.debug) {
                logger.debug(
                    'Wait for concurrency before bulkWrite: id = ', opId, 
                    ', size = ', operations.length, '.'
                );
            }
            await Promise.race(Object.values(running));
            if (errors.length > 0) {
                throw errors[0];
            }
        }
        if (debug || this.options.otherOptions.debug) {
            logger.debug(
                'BulkWrite: id = ', opId, ', size = ', operations.length,
                ', concurrency = ', Object.keys(running).length + 1, '/', concurrency, '.'
            );
        }
        running[opId] = this.bulkWrite(logger, operations, {debug, ...opts})
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
        bulk.operations = [];
    }
    
    async bulkFlush(logger, options = {}) {
        logger = logger || this.logger;
        const {db, collection} = this.options;
        if (!db || !collection) {
            logger.crash('internal', 'this._db or this._collection is undefined');
        }
        const bulk = get(this._state, ['bulk', getJobId(logger), db, collection]);
        if (!bulk) return;
        if (!bulk.flushing) {
            options = {...this.options.bulkOptions, ...options};
            bulk.flushing = this._bulkFlush(logger, bulk, options).finally(() => bulk.flushing = undefined);
        }
        await bulk.flushing;
    }
    
    async _bulkFlush(logger, bulk, options) {
        logger = logger || this.logger;
        if (bulk.operations.length > 0) {
            await this._bulkCommit(logger, bulk, options);
        }
        const running = Object.values(bulk.running);
        if (running.length > 0) {
            await Promise.all(running);
        }
        if (bulk.errors.length > 0) {
            const [error] = bulk.errors;
            bulk.errors = [];
            throw error;
        }
    }

    drop(logger, options = {}) {
        logger = logger || this.logger;
        const {debug, ...opts} = options;
        if (debug || this.options.otherOptions.debug) {
            const {db, collection} = this.options;
            logger.debug('mongodb.use(', db, ', ', collection, ').drop(', opts, ');');
        }
        return this._handlePromise(
            logger, this._connect(logger).then(coll => coll.drop(opts).catch(e => {
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
            logger, this._connect(logger).then(coll => coll.createIndexes(indexSpecs, opts))
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
            logger, this._connect(logger).then(coll => coll.watch(pipeline, opts))
        );
    }

    async _connect(logger) {
        logger = logger || this.logger;
        const {host, port, user, password, db, collection, connectionOptions} = this.options;
        if (!db || !collection) {
            logger.crash('internal', 'this._db or this._collection is undefined');
        }
        if (!this._state.client) {
            const auth = user && password ? `${encodeURIComponent(user)}:${encodeURIComponent(password)}@` : '';
            const uri = `mongodb://${auth}${host}${port ? `:${port}` : ''}`;
            const client = MongoClient(uri, connectionOptions);
            await client.connect();
            this._state.client = client;
        }
        let coll = get(this._state, ['pool', db, collection]);
        if (!coll) {
            coll = this._state.client.db(db).collection(collection);
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
        const options = this.options;
        let ops = [], target = cursor;
        return new Proxy({}, {
            get: (_, p, receiver) => {
                switch (p) {
                    case Symbol.asyncIterator:
                        return async function* () {
                            let count = 0;
                            while (await receiver.hasNext()) {
                                count++;
                                if (count % options.otherOptions.showProgressEvery === 0) {
                                    process.stdout.write('.');
                                }
                                yield await receiver.next();
                            }
                            if (options.otherOptions.showProgressEvery) {
                                process.stdout.write('✓\n');
                            }
                        };
                    case 'batch':
                        return async function* (batchSize) {
                            batchSize = batchSize || options.queryOptions.batchSize;
                            let batch = [];
                            let count = 0;
                            while (await receiver.hasNext()) {
                                count++;
                                if (count % options.otherOptions.showProgressEvery === 0) {
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
                            if (options.otherOptions.showProgressEvery) {
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
                        logger.crash('internal', 'method "', p, '" on cursor is not supported in mongodb plugin');
                }
            }
        });
    }

}
