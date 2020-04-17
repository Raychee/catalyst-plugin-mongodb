const {MongoClient} = require('mongodb');
const {setWith, get} = require('lodash');

const {isThenable} = require('@raychee/utils');


module.exports = {

    type: 'mongodb',

    key(
        {
            host, port, user, password, db, collection,
            connectionOptions = {},
            queryOptions = {},
            aggregationOptions = {},
            mapReduceOptions = {},
            otherOptions = {debug: false, showProgressEvery: undefined},
        }
    ) {
        return {host, port, user, password, db, collection, connectionOptions, queryOptions, aggregationOptions, mapReduceOptions, otherOptions};
    },

    async create(
        {
            host, port, user, password, db, collection,
            connectionOptions = {},
            queryOptions = {},
            aggregationOptions = {},
            mapReduceOptions = {},
            otherOptions = {debug: false, showProgressEvery: undefined},
        },
        {pluginLoader}
    ) {
        const options = {host, port, user, password, connectionOptions, queryOptions, aggregationOptions, mapReduceOptions, otherOptions};
        if (db || collection) {
            const plugin = await pluginLoader.get({type: 'mongodb', ...options});
            return plugin.use(db, collection);
        } else {
            return new MongoDB(this, options, pluginLoader);
        }
    },

    async destroy(mongodb) {
        await mongodb._close();
    }

};


class MongoDB {

    constructor(logger, options, pluginLoader, _state = {}) {
        this.logger = logger;
        this.options = options;
        this.pluginLoader = pluginLoader;

        this._state = _state;
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
        const ops = [];
        return new Proxy(cursor, {
            get: (target, p, receiver) => {
                switch (p) {
                    case Symbol.asyncIterator:
                        let options = this.options;
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
                    case 'then':
                        return (onFulfilled, onRejected) => {
                            return receiver.next().then(onFulfilled, onRejected);
                        };
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
                                return await target[p](...args);
                            })());
                        };
                    default:
                        return (...args) => {
                            ops.push({method: p, args});
                            return receiver;
                        };
                }
            }
        });
    }

}