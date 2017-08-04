; (function (global, factory) {
    if (typeof exports === 'object' && typeof module !== 'undefined') {
        var VeloxScriptLoader = require("velox-scriptloader") ;
        module.exports = factory(VeloxScriptLoader) ;
    } else if (typeof define === 'function' && define.amd) {
        define(['VeloxScriptLoader'], factory);
    } else {
        global.VeloxDatabaseClient.registerExtension(factory(global.VeloxScriptLoader));
    }
}(this, (function (VeloxScriptLoader) {
    'use strict';


    /**
     * The storage backend
     */
    var storage = null;

    var LOCAL_CHANGE_KEY = "velox_offline_changes";
    var LOCAL_SCHEMA_KEY = "velox_offline_schema";

    function saveOfflineChange(changes) {
        var localChanges = getOfflineChange();
        localChanges.push({
            date: new Date(),
            changes: changes
        });
        localStorage.setItem(LOCAL_CHANGE_KEY, JSON.stringify(localChanges));
    }

    function getOfflineChange() {
        var localChanges = localStorage.getItem(LOCAL_CHANGE_KEY);
        if (localChanges) {
            localChanges = JSON.parse(localChanges);
        } else {
            localChanges = [];
        }
        return localChanges;
    }

    function removeOfflineChange(index) {
        var localChanges = getOfflineChange();
        localChanges.splice(index, 1);
        localStorage.setItem(LOCAL_CHANGE_KEY, JSON.stringify(localChanges));
    }

    /**
     * Offline sync extension definition
     */
    var extension = {};
    extension.name = "offlinesync";

    extension.extendsObj = {};
    extension.extendsProto = {};

    /**
     * init local storage
     * 
     * @private
     */
    function prepare(callback) {
        if (!storage) {
            console.debug("No storage engined defined. Using default LokiJS storage. If you want to specify you own storage engine, use VeloxDatabaseClient.setOfflineStorageEngine");
            storage = new VeloxDbOfflineLoki();
        }
        storage.prepare(function (err) {
            if (err) { return callback(err); }

            var schema = localStorage.getItem(LOCAL_SCHEMA_KEY);
            if (schema) {
                schema = JSON.parse(schema);
                storage.schema = schema;
                callback();
            } else {
                //no local schema, get from server
                this.constructor.prototype.getSchema.bind(this)(function (err, schema) {
                    if (err) { return callback(err); }
                    storage.schema = schema;
                    localStorage.setItem(LOCAL_SCHEMA_KEY, JSON.stringify(schema));
                }.bind(this));
            }
        }.bind(this));

    }

    //TODO check schema to have foreign key and check consistence, if the FK is wrong sync will fail afterward
    extension.extendsObj.insert = function (table, record, callback) {
        prepare.bind(this)(function (err) {
            if (err) { return callback(err); }
            saveOfflineChange([{ action: "insert", table: table, record: record }]);
            storage.insert(table, record, callback);
        }.bind(this));
    };

    //TODO check schema to have foreign key and check consistence, if the FK is wrong sync will fail afterward
    extension.extendsObj.update = function (table, record, callback) {
        prepare.bind(this)(function (err) {
            if (err) { return callback(err); }
            saveOfflineChange([{ action: "update", table: table, record: record }]);
            storage.update(table, record, callback);
        }.bind(this));
    };

    //TODO check schema to have foreign key and check consistence, if the FK is wrong sync will fail afterward
    extension.extendsObj.remove = function (table, record, callback) {
        prepare.bind(this)(function (err) {
            if (err) { return callback(err); }
            saveOfflineChange([{ action: "remove", table: table, record: record }]);
            storage.remove(table, record, callback);
        }.bind(this));
    };

    extension.extendsObj.transactionalChanges = function (changeSet, callback) {
        prepare.bind(this)(function (err) {
            if (err) { return callback(err); }
            saveOfflineChange(changeSet);
            storage.transactionalChanges(changeSet, callback);
        }.bind(this));
    };

    extension.extendsObj.getByPk = function (table, pkOrRecord, callback) {
        prepare.bind(this)(function (err) {
            if (err) { return callback(err); }
            storage.getByPk(table, pkOrRecord, callback);
        }.bind(this));
    };

    extension.extendsObj.search = function (table, search, orderBy, offset, limit, callback) {
        prepare.bind(this)(function (err) {
            if (err) { return callback(err); }
            storage.search(table, search, orderBy, offset, limit, callback);
        }.bind(this));
    };

    extension.extendsObj.searchFirst = function (table, search, orderBy, callback) {
        prepare.bind(this)(function (err) {
            if (err) { return callback(err); }
            storage.searchFirst(table, search, orderBy, callback);
        }.bind(this));
    };


    var calculateTimeLapse = function(lapse, tries, callback){
        //TODO check cross timezone

        tries++ ;
        if(tries>10){
            //security, the connection is to instable to find the lapse with server
            return callback("Connection too instable to sync with server") ;
        }
        var start = new Date(new Date().getTime()+lapse);
        
        this._ajax(this.options.serverUrl + "syncGetTime", "POST", start, function (err, lapseServer) {
            if(err){ return callback(err);}

            if(Math.abs(lapseServer) < 500){
                //accept a 500ms difference, the purpose is to distinguish who from 2 offline users did modif the first
                //it is acceptable to mistake by a second
                return callback(lapse) ;
            }

            calculateTimeLapse.bind(this)(lapse+lapseServer, tries, callback) ;
        }) ;
    } ;

    var uploadChanges = function (callback) {
        var localChanges = getOfflineChange();
        if (localChanges.length > 0) {
            //local change to set to server
            calculateTimeLapse.bind(this)(0, 0, function(err, lapse){
                if(err){ return callback(err) ;}
                localChanges[0].timeLapse = lapse ;
                this._ajax(this.options.serverUrl + "sync", "POST", localChanges[0], function (err) {
                    if (err) {
                        return callback(err);
                    }
                    removeOfflineChange(0);
                    //go to next sync
                    uploadChanges.bind(this)(callback);
                }.bind(this));
            }.bind(this)) ;
            
        } else {
            callback();
        }
    };

    var syncing = false;
    /**
     * Sync data with distant server.
     * 
     * Start by upload all local data, then download new data from server
     * 
     * @param {string[]} [tables] list of tables to sync. default : all tables
     * @param {function(Error, object)} callback called on finish, give stats about what has been sync
     */
    extension.extendsProto.sync = function (tables, callback) {
        if (typeof (tables) === "function") {
            callback = tables;
            tables = null;
        }

        if (syncing) {
            //already syncing, try later
            setTimeout(function () {
                this.sync(tables, callback);
            }.bind(this), 200);
            return;
        }

        syncing = true;

        uploadChanges.bind(this)(function (err) {
            if (err) {
                syncing = false;
                return callback(err);
            }
            //nothing to send to server anymore, sync new data from server

            //first check if schema changed
            syncSchema.bind(this)(function (err) {
                if (err) { return callback(err); }

                //then check tables
                var search = {};
                if (tables) {
                    search.table_name = tables;
                }
                storage.search("velox_modif_table_version", search, function (err, localTablesVersions) {
                    if (err) {
                        syncing = false;
                        return callback(err);
                    }

                    this.constructor.prototype.search.bind(this)("velox_modif_table_version", search, function (err, distantTablesVersions) {
                        if (err) {
                            syncing = false;
                            return callback(err);
                        }

                        var localVersions = {};

                        var tableToSync = localTablesVersions.filter(function (localTable) {
                            localVersions[localTable.table_name] = localTable.version_table;
                            var hasNewData = false;
                            distantTablesVersions.some(function (distantTable) {
                                if (distantTable.table_name === localTable.table_name) {
                                    if (distantTable.version_table > localTable.version_table) {
                                        hasNewData = true;
                                    }
                                    return true;
                                }
                            });
                            return hasNewData;
                        }).map(function(t){ return t.table_name; });

                        tableToSync.push("velox_modif_table_version");

                        syncTables.bind(this)(tableToSync, localVersions, function (err) {
                            if (err) {
                                syncing = false;
                                return callback(err);
                            }
                            syncing = false;
                            callback();
                        }.bind(this));
                    }.bind(this));
                }.bind(this));
            }.bind(this));
        }.bind(this));
    };

    function syncSchema(callback) {
        this.searchFirst("velox_db_version", {}, function (err, localVersion) {
            if (err) { return callback(err); }
            this.constructor.prototype.searchFirst.bind(this)("velox_db_version", {}, function (err, distantVersion) {
                if (err) { return callback(err); }
                if (!localVersion || localVersion.version < distantVersion.version) {
                    this.constructor.prototype.getSchema.bind(this)(function (err, schema) {
                        if (err) { return callback(err); }
                        storage.schema = schema;
                        localStorage.setItem(LOCAL_SCHEMA_KEY, JSON.stringify(schema));
                        callback();
                    }.bind(this));
                } else {
                    //schema did not changed
                    callback();
                }
            }.bind(this));
        }.bind(this));


    }

    function syncTables(tablesToSync, localVersions, callback) {
        var table = tablesToSync.shift();//take first table to sync

        //search new data for this table
        this.constructor.prototype.search.bind(this)(table, { velox_version_table: { ope: ">", value: localVersions[table] } }, function (err, newRecords) {
            if (err) { return callback(err); }



            //search deleted records
            this.constructor.prototype.search.bind(this)("velox_delete_track", { table_name: table, table_version: { ope: ">", value: localVersions[table] } }, function (err, deletedRecords) {
                if (err) { return callback(err); }

                //create change set
                var changeSet = newRecords.map(function (r) {
                    return { table: table, record: r, action: "auto" };
                });

                deletedRecords.map(function (r) {
                    changeSet.push({ table: table, record: r, action: "remove" });
                });

                //apply in local storage
                storage.transactionalChanges(changeSet, function (err) {
                    if (err) { return callback(err); }
                    if (tablesToSync.length > 0) {
                        //more tables to sync, go ahead
                        syncTables.call(this, tablesToSync, localVersions, callback);
                    } else {
                        //finished
                        callback();
                    }
                }.bind(this));
            }.bind(this));
        }.bind(this));
    }

    extension.extendsGlobal = {};

    /**
     * Set the offline storage engine
     * 
     * @param {object} storageEngine the storage engine to use
     */
    extension.extendsGlobal.setOfflineStorageEngine = function (storageEngine) {
        storage = storageEngine;
    };


    var LOKIJS_VERSION = "1.5.0";

    var LOKIJS_LIB = [
        {
            name: "lokijs",
            type: "js",
            version: LOKIJS_VERSION,
            cdn: "https://cdnjs.cloudflare.com/ajax/libs/lokijs/$VERSION/lokijs.min.js",
            bowerPath: "lokijs/lokijs.min.js"
        },
        {
            name: "lokijs-indexed-adapter",
            type: "js",
            version: LOKIJS_VERSION,
            cdn: "https://cdnjs.cloudflare.com/ajax/libs/lokijs/$VERSION/loki-indexed-adapter.min.js",
            bowerPath: "lokijs/loki-indexed-adapter.min.js"
        }
    ];


    /**
     * @typedef VeloxDbOfflineLokiOptions
     * @type {object}
     * @property {string} [prefix] prefix for storage name
     * @property {object} [lokijs] the lokijs class. If not given, it will be loaded from CDN. Expected version : 1.5.0
     * @property {object} [lokiadapter] the lokijs persistence adapter object. If not given, it will be loaded from CDN. Expected version : 1.5.0
     */

    /**
     * The Velox database client
     * 
     * @constructor
     * 
     * @param {VeloxDbOfflineLokiOptions} options database client options
     */
    function VeloxDbOfflineLoki(options) {
        if (!options) {
            options = {};
        }
        this.options = options;
        this.lokijs = options.lokijs;
        this.lokiadapter = options.lokiadapter;
        this.loki = null;
    }

    VeloxDbOfflineLoki.prototype.prepare = function (callback) {
        this.importLibIfNeeded(function (err) {
            if (err) { return callback(err); }
            if (!this.loki) {
                var dbname = (this.options.prefix || "") + "velox-offline";
                if (!this.lokiadapter) {
                    this.lokiadapter = new window.LokiIndexedAdapter(dbname);
                }
                this.loki = new this.lokijs(dbname, {
                    autoload: true,
                    autoloadCallback: function () {
                        callback();
                    }.bind(this),
                    autosave: true,
                    autosaveInterval: 10000,
                    adapter: this.lokiadapter
                });
            } else {
                callback();
            }
        }.bind(this));
    };

    VeloxDbOfflineLoki.prototype.importLibIfNeeded = function (callback) {
        if (!this.lokijs) {
            //no lokijs object exists, load from CDN
            console.debug("No lokijs object given, we will load from CDN. If you don't want this, include lokijs " + LOKIJS_VERSION +
                " in your import scripts or give i18next object to VeloxWebView.i18n.configure function");

            if (!VeloxScriptLoader) {
               return console.error("To have automatic script loading, you need to import VeloxScriptLoader");
            }

            VeloxScriptLoader.load(LOKIJS_LIB, function (err) {
                if (err) { return callback(err); }
                this.lokijs = window.loki;
                callback();
            }.bind(this));
        } else {
            callback();
        }
    };

    VeloxDbOfflineLoki.prototype.getCollection = function (table) {
        var coll = this.loki.getCollection(table);
        if (coll === null) {
            var options = {
                unique: this.schema[table].pk
            };
            options.indices = [this.schema[table].pk];
            coll = this.loki.addCollection(table, options);
        }
        return coll;
    };

    VeloxDbOfflineLoki.prototype.insert = function (table, record, callback) {
        try {
            record.velox_version_record = 0;
            record.velox_version_date = new Date();
            this.getCollection(table).insert(record);
            callback(null, this._sanatizeRecord(record));
        } catch (err) {
            callback(err);
        }
    };

    VeloxDbOfflineLoki.prototype.update = function (table, record, callback) {
        //it is faster to remove object and them insert them again
        this.remove(table, record, function (err) {
            if (err) { return callback(err); }
            record.velox_version_record = (record.velox_version_record || 0) + 1;
            record.velox_version_date = new Date();
            this.insert(table, record, callback);
        }.bind(this));
    };

    VeloxDbOfflineLoki.prototype.remove = function (table, pkOrRecord, callback) {
        try {
            this.getCollection(table).findAndRemove(this._pkSearch(table, pkOrRecord));
            callback();
        } catch (err) {
            callback(err);
        }
    };

    VeloxDbOfflineLoki.prototype.transactionalChanges = function (changeSet, callback) {
        this._doChanges(changeSet.slice(), [], callback);
    };

    VeloxDbOfflineLoki.prototype._doChanges = function (changeSet, results, callback) {
        var change = changeSet.shift();
        var next = function () {
            if (changeSet.length === 0) {
                callback(null, results);
            } else {
                this._doChanges(changeSet, results, callback);
            }
        }.bind(this);
        if (change.action === "insert") {
            this.insert(change.table, change.record, function (err, insertedRecord) {
                if (err) { return callback(err); }
                results.push({ action: "insert", table: change.table, record: insertedRecord });
                next();
            }.bind(this));
        } else if (change.action === "update") {
            this.update(change.table, change.record, function (err, updatedRecord) {
                if (err) { return callback(err); }
                results.push({ action: "update", table: change.table, record: updatedRecord });
                next();
            }.bind(this));
        } else if (change.action === "remove") {
            this.remove(change.table, change.record, function (err) {
                if (err) { return callback(err); }
                results.push({ action: "remove", table: change.table, record: change.record });
                next();
            }.bind(this));
        } else {
            this.getByPk(change.table, change.record, function (err, foundRecord) {
                if (err) { return callback(err); }
                if (foundRecord) {
                    this.update(change.table, change.record, function (err, updatedRecord) {
                        if (err) { return callback(err); }
                        results.push({ action: "update", table: change.table, record: updatedRecord });
                        next();
                    }.bind(this));
                } else {
                    this.insert(change.table, change.record, function (err, insertedRecord) {
                        if (err) { return callback(err); }
                        results.push({ action: "insert", table: change.table, record: insertedRecord });
                        next();
                    }.bind(this));
                }
            }.bind(this));
        }
    };

    VeloxDbOfflineLoki.prototype.getByPk = function (table, pkOrRecord, callback) {
        try {
            var record = this.getCollection(table).findOne(this._pkSearch(table, pkOrRecord));
            if (record) {
                callback(null, this._sanatizeRecord(record));
            } else {
                callback(null, null);
            }
        } catch (err) {
            callback(err);
        }
    };

    VeloxDbOfflineLoki.prototype._sanatizeRecord = function (record) {
        record = JSON.parse(JSON.stringify(record));
        if (Array.isArray(record)) {
            record.forEach(function (r) {
                delete r.$loki;
                delete r.meta;
            });
        } else {
            delete record.$loki;
            delete record.meta;
        }
        return record;
    };



    VeloxDbOfflineLoki.prototype.search = function (table, search, orderBy, offset, limit, callback) {
        if (typeof (orderBy) === "function") {
            callback = orderBy;
            orderBy = null;
            offset = 0;
            limit = null;
        } else if (typeof (offset) === "function") {
            callback = offset;
            offset = 0;
            limit = null;
        } else if (typeof (limit) === "function") {
            callback = limit;
            limit = null;
        }

        try {
            var records = [];
            if (!offset && !limit && !orderBy) {
                records = this.getCollection(table).find(this._translateSearch(search));
            } else {
                var chain = this.getCollection(table).chain().find(this._translateSearch(search));
                if (orderBy) {
                    if (typeof (sortColumn) === "string") {
                        chain = chain.simplesort(orderBy);
                    } else {
                        if (!Array.isArray(orderBy)) {
                            orderBy = [orderBy];
                        }
                        var sortArgs = [];
                        orderBy.forEach(function (s) {
                            if (typeof (s) === "string") {
                                sortArgs.push(s);
                            } else {
                                sortArgs.push([s.col, s.direction === "desc"]);
                            }
                        });
                        chain = chain.compoundsort(sortArgs);
                    }
                }
                if (limit) {
                    chain = chain.limit(limit);
                }
                if (offset) {
                    chain = chain.offset(offset);
                }
                records = chain.data();
            }
            callback(null, this._sanatizeRecord(records));
        } catch (err) {
            callback(err);
        }
    };


    VeloxDbOfflineLoki.prototype.searchFirst = function (table, search, orderBy, callback) {
        if (typeof (orderBy) === "function") {
            callback = orderBy;
            orderBy = null;
        }
        this.search(table, search, orderBy, 0, 1, function (err, results) {
            if (err) { return callback(err); }
            if (results.length === 0) {
                callback(null, null);
            } else {
                callback(null, this._sanatizeRecord(results[0]));
            }
        }.bind(this));

    };

    VeloxDbOfflineLoki.prototype.multisearch = function (reads, callback) {
        var arrayReads = [];
        Object.keys(reads).forEach(function (k) {
            var r = JSON.parse(JSON.stringify(reads[k]));
            r.name = k;
            arrayReads.push(r);

        });
        this._doASearch(arrayReads, {}, callback);
    };

    VeloxDbOfflineLoki.prototype._doASearch = function (reads, results, callback) {
        var r = reads.shift();
        var next = function () {
            if (reads.length === 0) {
                callback(null, results);
            } else {
                this._doASearch(reads, results, callback);
            }
        }.bind(this);
        if (r.pk) {
            this.getByPk(r.table, r.pk, function (err, result) {
                if (err) { return callback(err); }
                results[r.name] = result;
                next();
            }.bind(this));
        } else if (r.search) {
            this.search(r.table, r.search, r.orderBy, r.offset, r.limit, function (err, records) {
                if (err) { return callback(err); }
                results[r.name] = records;
                next();
            }.bind(this));
        } else if (r.searchFirst) {
            this.searchFirst(r.table, r.search, r.orderBy, function (err, record) {
                if (err) { return callback(err); }
                results[r.name] = record;
                next();
            }.bind(this));
        } else {
            callback("Unkown search action for " + JSON.stringify(r));
        }
    };


    VeloxDbOfflineLoki.prototype._pkSearch = function (table, pkOrRecord) {
        var pk = this.schema[table].pk;
        if (!pk) {
            throw "Can't find pk for table " + table;
        }
        var search = {};
        if (pk.length === 1 && typeof (pkOrRecord) !== "object") {
            search[pk[0]] = pkOrRecord;
        } else {
            pk.forEach(function (k) {
                search[k] = pkOrRecord[k];
            });
        }
        return this._translateSearch(search);
    };

    VeloxDbOfflineLoki.prototype._translateSearch = function (search) {
        var lokiSearch = [];

        Object.keys(search).forEach(function (k) {
            var val = search[k];

            if (val && val.operator === "between" && Array.isArray(val.value)) {
                var between1 = {};
                between1[k] = { $gte: val.value[0] };
                var between2 = {};
                between2[k] = { $lte: val.value[1] };
                lokiSearch.push(between1);
                lokiSearch.push(between2);
            } else {
                var translatedVal = val;
                if (val && typeof (val) === "object" && val.ope) {
                    var translatedOperator = val.ope;

                    switch (val.ope.toLowerCase()) {
                        case "=":
                            translatedOperator = "$eq";
                            break;
                        case ">":
                            translatedOperator = "$gt";
                            break;
                        case ">=":
                            translatedOperator = "$gte";
                            break;
                        case "<":
                            translatedOperator = "$lt";
                            break;
                        case "<=":
                            translatedOperator = "$lte";
                            break;
                        case "<>":
                            translatedOperator = "$ne";
                            break;
                        case "in":
                            translatedOperator = "$in";
                            break;
                        case "between":
                            translatedOperator = "$between";
                            break;
                        case "not in":
                            translatedOperator = "$nin";
                            break;
                    }
                    translatedVal = {};
                    translatedVal[translatedOperator] = val.value;
                } else if (Array.isArray(val)) {
                    translatedVal = { $in: val };
                } else if (val && typeof (val) === "object" && val.constructor === RegExp) {
                    translatedVal = { $regex: val };
                } else if (val && typeof (val) === "string" && val.indexOf("%") !== -1) {
                    translatedVal = { $regex: new RegExp(val.replace(/%/g, "*")) };
                }
                var translateSearch = {};
                translateSearch[k] = translatedVal;
                lokiSearch.push(translateSearch);
            }

        });

        if (lokiSearch.length === 1) {
            return lokiSearch[0];
        } else {
            return { $and: lokiSearch };
        }
    };

    return extension;

})));