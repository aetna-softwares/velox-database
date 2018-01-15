; (function (global, factory) {
    if (typeof exports === 'object' && typeof module !== 'undefined') {
        var VeloxScriptLoader = require("velox-scriptloader") ;
        module.exports = factory(VeloxScriptLoader) ;
    } else if (typeof define === 'function' && define.amd) {
        define(['VeloxScriptLoader'], factory);
    } else {
        global.VeloxDatabaseClient.registerExtension(factory(global.veloxScriptLoader));
    }
}(this, (function (VeloxScriptLoader) {
    'use strict';

    /**
     * Tables settings
     */
    var tableSettings = null;

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
    extension.extendsGlobal = {};

    /**
     * Set the offline storage engine
     * 
     * @param {object} storageEngine the storage engine to use
     */
    extension.extendsGlobal.setOfflineStorageEngine = function (storageEngine) {
        storage = storageEngine;
    };

    /**
     * Sync strategy "always sync" : do sync before and after each operation
     */
    extension.extendsGlobal.SYNC_STRATEGY_ALWAYS = {
        before: function(callback){
            this.sync(callback) ;
        },
        after: function(callback){
            this.sync(callback) ;
        }
    } ;
    
    /**
     * Sync strategy manual : no automatic sync.
     * You must manage sync in your code
     */
    extension.extendsGlobal.SYNC_STRATEGY_MANUAL = {
        before: function(callback){ callback() ;},
        after: function(callback){ callback() ;}
    } ;
    
    /**
     * Do a sync on first operation and each 20sec
     */
    extension.extendsGlobal.SYNC_STRATEGY_AUTO = {
        before: function(callback){ 
            if(!this.lastSyncDate || new Date().getTime() - this.lastSyncDate.getTime() > 20000){
                //if not yet sync or sync more than 20s ago, sync again
                if(this.syncAutoTimeoutId){
                    //if there is a planned sync, cancel it
                    clearTimeout(this.syncAutoTimeoutId) ;
                    this.syncAutoTimeoutId = null;
                }
                this.sync(callback) ;
            }else{
                callback() ;
            }
        },
        after: function(callback){
            if(this.syncAutoTimeoutId){
                //if there is a planned sync, cancel it
                clearTimeout(this.syncAutoTimeoutId) ;
            }
            //schedule a sync in 20s (if no sync has been made in the midtime)
            this.syncAutoTimeoutId = setTimeout(function(){
                this.sync() ;
            }.bind(this), 20000) ;
            callback() ;
        }
    } ;

    var syncStrategy = extension.extendsGlobal.SYNC_STRATEGY_AUTO ;


    /**
     * @typedef VeloxDbOfflineSyncStrategy
     * @type {object}
     * @property {function} before function receiving a callback that will be call before all db operation
     * @property {function} after function receiving a callback that will be call after all db operation
     */

    /**
     * Set the sync strategy (default is the SYNC_STRATEGY_AUTO)
     * 
     * You can use VeloxDatabaseClient.SYNC_STRATEGY_ALWAYS, VeloxDatabaseClient.SYNC_STRATEGY_MANUAL, VeloxDatabaseClient.SYNC_STRATEGY_AUTO
     * 
     * or you can create your own strategy
     * 
     * @param {VeloxDbOfflineSyncStrategy} syncStrategyP the strategy to use
     */
    extension.extendsGlobal.setSyncStrategy = function (syncStrategyP) {
        syncStrategy = syncStrategyP;
    };

     /**
     * @typedef VeloxDbOfflineTableSettings
     * @type {object}
     * @property {string} name table name
     * @property {boolean} offline maintain offline version of this table
     */

    /**
     * Set the table settings
     * 
     * @param {VeloxDbOfflineTableSettings[]} settings the table settings
     */
    extension.extendsGlobal.setOfflineTableSettings = function (settings) {
        tableSettings = settings;
    };


    var prepareDone = false;
    /**
     * init local storage
     * 
     * @private
     */
    function prepare(callback) {
        if(prepareDone){
            return callback() ;
        }

        var getSchema = function(callback){
            var schema = localStorage.getItem(LOCAL_SCHEMA_KEY);
            if (schema) {
                schema = JSON.parse(schema);
                callback(null, schema);
            } else {
                //no local schema, get from server
                this.constructor.prototype.getSchema.bind(this)(function (err, schema) {
                    if (err) { return callback(err); }
                    localStorage.setItem(LOCAL_SCHEMA_KEY, JSON.stringify(schema));
                    callback(null, schema) ;
                }.bind(this));
            }
        }.bind(this) ;
        
        getSchema(function(err, schema){
            if (err) { return callback(err); }

            if (!storage) {
                console.debug("No storage engined defined. Using default LokiJS storage. If you want to specify you own storage engine, use VeloxDatabaseClient.setOfflineStorageEngine");
                storage = new VeloxDbOfflineIndDb({schema : schema});
            }
            storage.prepare(function(err){
                if(err){
                    return callback(err) ;
                }
                prepareDone = true ;
                callback() ;
            });
        }) ;


    }

    function isOffline(tableName){
        if(tableSettings){
            var isOffline = false;
            tableSettings.some(function(table){
                if(table.name === tableName){
                    isOffline = table.offline ;
                    return true ;
                }
            }) ;
            return isOffline ;
        }else{
            return true ;
        }
    }

    function doOperation(instance, action, args, callbackDo, callbackDone){
        if(action !== "multiread"){
            if(!isOffline(args[0])){
                return instance.constructor.prototype[action].apply(instance, args) ;
            }
        }


        prepare.bind(instance)(function (err) {
            if (err) { return callbackDone(err); }
            syncStrategy.before.bind(instance)(function(err){
                if (err) { return callbackDone(err); }
                callbackDo(function(err){
                    if (err) { return callbackDone(err); }
                    var results = Array.prototype.slice.call(arguments) ;
                    syncStrategy.after.bind(instance)(function(err){
                        if (err) { return callbackDone(err); }
                        callbackDone.apply(null, results) ;
                    }) ;
                }) ;
            }) ;
        }) ;
    }

    //TODO check schema to have foreign key and check consistence, if the FK is wrong sync will fail afterward
    extension.extendsObj.insert = function (table, record, callback) {
        doOperation(this, "insert" ,arguments, function(done){
            saveOfflineChange([{ action: "insert", table: table, record: record }]);
            storage.insert(table, record, done);
        }, callback) ;
    };

    //TODO check schema to have foreign key and check consistence, if the FK is wrong sync will fail afterward
    extension.extendsObj.update = function (table, record, callback) {
        doOperation(this, "update", arguments, function(done){
            saveOfflineChange([{ action: "update", table: table, record: record }]);
            storage.update(table, record, done);
        }, callback) ;
    };

    //TODO check schema to have foreign key and check consistence, if the FK is wrong sync will fail afterward
    extension.extendsObj.remove = function (table, record, callback) {
        doOperation(this, "remove", arguments,  function(done){
            saveOfflineChange([{ action: "remove", table: table, record: record }]);
            storage.remove(table, record, done);
        }, callback) ;
    };

    //TODO check schema to have foreign key and check consistence, if the FK is wrong sync will fail afterward
    extension.extendsObj.removeWhere = function (table, conditions, callback) {
        doOperation(this, "removeWhere", arguments,  function(done){
            saveOfflineChange([{ action: "removeWhere", table: table, conditions: conditions }]);
            storage.removeWhere(table, conditions, done);
        }, callback) ;
    };

    extension.extendsObj.transactionalChanges = function (changeSet, callback) {
        doOperation(this, "transactionalChanges",arguments,  function(done){
            saveOfflineChange(changeSet);
            storage.transactionalChanges(changeSet, done);
        }, callback) ;
    };
    
    extension.extendsObj.getByPk = function (table, pkOrRecord, joinFetch, callback) {
        doOperation(this, "getByPk", arguments, function(done){
            storage.getByPk(table, pkOrRecord, joinFetch, done);
        }, callback) ;
    };

    extension.extendsObj.search = function (table, search, joinFetch, orderBy, offset, limit, callback) {
        doOperation(this, "search", arguments, function(done){
            storage.search(table, search, joinFetch, orderBy, offset, limit, done);
        }, callback) ;
    };

    extension.extendsObj.searchFirst = function (table, search, joinFetch, orderBy, callback) {
        doOperation(this, "searchFirst", arguments, function(done){
            storage.searchFirst(table, joinFetch, search, orderBy, done);
        }, callback) ;
    };
    
    extension.extendsObj.multiread = function(reads, callback){
        var offlineReads = [] ;
        var onlineReads = {} ;
        Object.keys(reads).forEach(function(k){
            if(!reads[k].table){
                reads[k].table = k ;
            }
            var tableName = reads[k].table;
            reads[k].name = k;

            if(!isOffline(tableName)){
                onlineReads[k] = reads[k] ;
                return  ;
            }

            offlineReads.push(reads[k]) ;
        }) ;

        
        doOperation(this, "multiread", [reads, callback], function(done){
            storage.multiread(offlineReads, function(err, results){
                if(err){ return done(err) ;}
                if(Object.keys(onlineReads).length>0){
                    this.constructor.prototype.multiread.apply(this, onlineReads, function(err, onlineResults){
                        if(err){ return done(err) ;}
                        Object.keys(onlineResults).forEach(function(k){
                            results[k] = onlineResults[k] ;
                        }) ;
                        done(null, results) ;
                    }) ;
                }else{
                    done(null, results) ;
                }
            }.bind(this)) ;
        }.bind(this), callback) ;
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
        if(!callback){
            callback = function(){} ;
        }
        prepare.bind(this)(function (err) {
            if (err) { return callback(err); }

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
                    if(!tables){
                        //no table give, add all offline tables
                        tables = Object.keys(this.schema).filter(function(tableName){
                            return isOffline(tableName) ;
                        }) ;

                        //case of view that is composed by many table, must sync if any of used tables is modified
                        tables.forEach(function(tableName){
                            var tableDef = this.schema[tableName] ;
                            if(tableDef.viewOfTables){
                                tableDef.viewOfTables.forEach(function(subTable){
                                    if(tables.indexOf(subTable.name) === -1){
                                        tables.push(subTable.name)
                                    }
                                }) ;
                            }
                        }.bind(this)) ;
                    }

                    search.table_name = tables;
                    //get the version of tables in offline storage
                    storage.search("velox_modif_table_version", search, function (err, localTablesVersions) {
                        if (err) {
                            syncing = false;
                            return callback(err);
                        }

                        //get the version of tables on server
                        this.constructor.prototype.search.bind(this)("velox_modif_table_version", search, function (err, distantTablesVersions) {
                            if (err) {
                                syncing = false;
                                return callback(err);
                            }

                            var localVersions = {};
                            localTablesVersions.forEach(function (localTable) {
                                localVersions[localTable.table_name] = localTable.version_table;
                            });

                            //add all tables with different version number
                            var tableToSync = distantTablesVersions.filter(function (distantTable) {
                                var localVersion = localVersions[distantTable.table_name] ;
                                if(!localVersion){
                                    localVersions[distantTable.table_name] = -1 ;
                                }
                                return (!localVersion || localVersion < distantTable.version_table) ;
                            }).map(function(t){ return t.table_name; });

                            //case of view that is composed by many table, must sync if any of used tables is modified
                            Object.keys(this.schema).forEach(function(tableName){
                                var tableDef = this.schema[tableName] ;
                                if(tableDef.viewOfTables){
                                    tableDef.viewOfTables.some(function(subTable){
                                        if(tableToSync.indexOf(subTable.name) !== -1){
                                            tableToSync.push(tableName) ;
                                            return true ;
                                        }
                                    }) ;
                                }
                            }.bind(this)) ;

                            //keep only the offline tables
                            tableToSync = tableToSync.filter(function(tableName){
                                return isOffline(tableName) ;
                            }) ;

                            syncTables.bind(this)(tableToSync, localVersions, function (err) {
                                if (err) {
                                    syncing = false;
                                    return callback(err);
                                }

                                //update table versions
                                var updateVersionChanges = [] ;
                                distantTablesVersions.forEach(function(version){
                                    updateVersionChanges.push({table: "velox_modif_table_version", record : version}) ;
                                }) ;
                                storage.transactionalChanges(updateVersionChanges, function (err) {
                                    if (err) { 
                                        syncing = false;
                                        return callback(err); 
                                    }
                                    
                                    syncing = false;
                                    this.lastSyncDate = new Date() ;
                                    callback();
                                }.bind(this));

                            }.bind(this));
                        }.bind(this));
                    }.bind(this));
                }.bind(this));
            }.bind(this));
        }.bind(this));
    };

    function syncSchema(callback) {
        storage.searchFirst("velox_db_version", {}, function (err, localVersion) {
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
        if (tablesToSync.length === 0) {
            return callback() ;
        }
        var table = tablesToSync.shift();//take first table to sync

        var search = { velox_version_table: { ope: ">", value: localVersions[table] } } ;

        var tableDef = this.schema[table] ;
        if(tableDef.viewOfTables){
            var searches = [] ;
            tableDef.viewOfTables.forEach(function(subTable){
                var s = {} ;
                if(subTable.versionColumn){
                    s[subTable.versionColumn] = { ope: ">", value: localVersions[subTable.name] } ;
                }else{
                    s[subTable.name+"_velox_version_table"] = { ope: ">", value: localVersions[subTable.name] } ;
                }
                searches.push(s) ;
            }) ;
            search = { $or : searches } ; 
        }

        //search new data for this table
        this.constructor.prototype.search.bind(this)(table, search, function (err, newRecords) {
            if (err) { return callback(err); }

            //search deleted records
            this.constructor.prototype.search.bind(this)("velox_delete_track", { table_name: table, table_version: { ope: ">", value: localVersions[table] } }, function (err, deletedRecords) {
                if (err) { return callback(err); }

                //create change set
                var changeSet = newRecords.map(function (r) {
                    return { table: table, record: r, action: "auto" };
                });

                deletedRecords.map(function (r) {
                    var record = {} ;
                    var splittedPk = r.table_uid.split("$_$") ;
                    this.schema[table].pk.forEach(function(pk, i){
                        record[pk] = splittedPk[i] ;
                    }) ;

                    changeSet.push({ table: table, record: record, action: "remove" });
                }.bind(this));

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

    

    var LOKIJS_VERSION = "1.5.0";

    var LOKIJS_LIB = [
        {
            name: "lokijs",
            type: "js",
            version: LOKIJS_VERSION,
            cdn: "https://cdnjs.cloudflare.com/ajax/libs/lokijs/$VERSION/lokijs.min.js",
            bowerPath: "lokijs/build/lokijs.min.js"
        },
        {
            name: "lokijs-indexed-adapter",
            type: "js",
            version: LOKIJS_VERSION,
            cdn: "https://cdnjs.cloudflare.com/ajax/libs/lokijs/$VERSION/loki-indexed-adapter.min.js",
            bowerPath: "lokijs/build/loki-indexed-adapter.min.js"
        }
    ];


    /**
     * @typedef VeloxDbOfflineIndDbOptions
     * @type {object}
     * @property {string} [dbName] indexedDB database name
     * @property {object} schema the database schema
     */

    /**
     * Offline db implementation based on IndexedDB
     */
    function VeloxDbOfflineIndDb(options){
        if (!options) {
            options = {};
        }
        this.options = options;
        this.schema = options.schema;
    }

    var indexedDB = window.indexedDB || window.mozIndexedDB || window.webkitIndexedDB || window.msIndexedDB || window.shimIndexedDB;

    VeloxDbOfflineIndDb.prototype.prepare = function (callback) {
        var request = indexedDB.open(this.options.dbName || "velox_sync_db");
        request.onerror = function() {
            callback(request.errorCode);
        };

        var updateDb = function(event){
            var db = event.target.result;
    
            Object.keys(this.schema).forEach(function(table){
                if ((isOffline(table) || table === "velox_modif_table_version" || table === "velox_db_version") && !db.objectStoreNames.contains(table)) {
                    var options = {} ;
                    if(this.schema[table].pk && this.schema[table].pk.length>0){
                        options.keyPath =this.schema[table].pk ;
                    }
                    db.createObjectStore(table, options);
                }
            }.bind(this));
        }.bind(this) ;

        request.onupgradeneeded = updateDb;
        request.onsuccess = function(event) {
            this.db = event.target.result;

            if(this.db.version === 1){
                //created from scratch, set the version from schema version (set 1000 gap between version because we may do intermediate versions to create indexes)
                this.db.close() ;
                var request = indexedDB.open(this.db.name, this.schema.__version.version * 1000);
                request.onupgradeneeded = updateDb;
                request.onsuccess = function(event){
                    this.db = event.target.result;
                    callback() ;
                }.bind(this) ;
            }else if (this.db.version < this.schema.__version.version * 1000 ){
                //schema changed, we must update it
                this.db.close() ;
                var request = indexedDB.open(this.db.name, this.schema.__version.version * 1000);
                request.onupgradeneeded = updateDb;
                request.onsuccess = function(event){
                    this.db = event.target.result;
                    callback() ;
                }.bind(this) ;
            }else{
                //already up to date
                callback() ;
            }

        }.bind(this);
    };

    VeloxDbOfflineIndDb.prototype.tx = function (tables, mode, doTx, callback) {
        var results = null;
        var tx = new VeloxDbOfflineIndDbTransaction(this, tables, mode, function(err){
            if(err){
                return callback(err) ;
            }
            callback(null, results) ;
        }) ;
       
        doTx(tx, function(err){
            if(err){
                tx.abort() ;
                return ;
            }
            results = arguments[1] ;
            callback(null, results) ;
        }) ;

    };


    VeloxDbOfflineIndDb.prototype.insert = function (table, record, callback) {
        this.tx([table], "readwrite", function(tx, done){
            tx.insert(table, record, done) ;
        }, callback) ;
    };

    VeloxDbOfflineIndDb.prototype.update = function (table, record, callback) {
        this.tx([table], "readwrite", function(tx, done){
            tx.update(table, record, done) ;
        }, callback) ;
    };

    VeloxDbOfflineIndDb.prototype.remove = function (table, pkOrRecord, callback) {
        this.tx([table], "readwrite", function(tx, done){
            tx.remove(table, pkOrRecord, done) ;
        }, callback) ;
    };
    
    VeloxDbOfflineIndDb.prototype.removeWhere = function (table, conditions, callback) {
        this.tx([table], "readwrite", function(tx, done){
            tx.removeWhere(table, conditions, done) ;
        }, callback) ;
    };

    VeloxDbOfflineIndDb.prototype.transactionalChanges = function (changeSet, callback) {
        if(changeSet.length === 0){
            return callback(null, []) ;
        }
        this.tx(changeSet.map(function(c){ return c.table; }), "readwrite", function(tx, done){
            this._doChanges(tx, changeSet.slice(), [], done);
        }.bind(this), callback) ;
        
    };

    VeloxDbOfflineIndDb.prototype._doChanges = function (tx, changeSet, results, callback) {
        if (changeSet.length === 0) {
            callback(null, results);
            return;
        } 
        var change = changeSet.shift();
        if (change.action === "insert") {
            tx.insert(change.table, change.record, function (err, insertedRecord) {
                if (err) { return callback(err); }
                results.push({ action: "insert", table: change.table, record: insertedRecord });
                this._doChanges(tx, changeSet, results, callback);
            }.bind(this));
        } else if (change.action === "update") {
            tx.update(change.table, change.record, function (err, updatedRecord) {
                if (err) { return callback(err); }
                results.push({ action: "update", table: change.table, record: updatedRecord });
                this._doChanges(tx, changeSet, results, callback);
            }.bind(this));
        } else if (change.action === "remove") {
            tx.remove(change.table, change.record, function (err) {
                if (err) { return callback(err); }
                results.push({ action: "remove", table: change.table, record: change.record });
                this._doChanges(tx, changeSet, results, callback);
            }.bind(this));
        } else {
            tx.getByPk(change.table, change.record, function (err, foundRecord) {
                if (err) { return callback(err); }
                if (foundRecord) {
                    tx.update(change.table, change.record, function (err, updatedRecord) {
                        if (err) { return callback(err); }
                        results.push({ action: "update", table: change.table, record: updatedRecord });
                        this._doChanges(tx, changeSet, results, callback);
                    }.bind(this));
                } else {
                    tx.insert(change.table, change.record, function (err, insertedRecord) {
                        if (err) { return callback(err); }
                        results.push({ action: "insert", table: change.table, record: insertedRecord });
                        this._doChanges(tx, changeSet, results, callback);
                    }.bind(this));
                }
            }.bind(this));
        }
    };

    VeloxDbOfflineIndDb.prototype.getByPk = function (table, pkOrRecord, joinFetch, callback) {
        this.tx([table], "readonly", function(tx, done){
            tx.getByPk(table, pkOrRecord, joinFetch, done) ;
        }, callback) ;
    };

    VeloxDbOfflineIndDb.prototype.search = function (table, search, joinFetch, orderBy, offset, limit, callback) {
        if(typeof(joinFetch) === "function"){
            callback = joinFetch;
            joinFetch = null;
            orderBy = null;
            offset = 0;
            limit = null ;
        } 
        if(typeof(joinFetch) === "string"){
            callback = limit;
            limit = offset;
            offset = orderBy;
            orderBy = joinFetch;
            joinFetch = null ;
        } 
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

        this.tx([table], "readonly", function(tx, done){
            tx.search(table, search, joinFetch, orderBy, offset, limit, done) ;
        }, callback) ;
    };


    VeloxDbOfflineIndDb.prototype.searchFirst = function (table, search, joinFetch, orderBy, callback) {
        if(typeof(joinFetch) === "function"){
            callback = joinFetch;
            joinFetch = null;
            orderBy = null;
        }
        if(typeof(joinFetch) === "string"){
            callback = orderBy;
            orderBy = joinFetch;
            joinFetch = null;
        }
        if (typeof (orderBy) === "function") {
            callback = orderBy;
            orderBy = null;
        }
        this.search(table, search, orderBy, 0, 1, function (err, results) {
            if (err) { return callback(err); }
            if (results.length === 0) {
                callback(null, null);
            } else {
                callback(null, results[0]);
            }
        }.bind(this));

    };

    VeloxDbOfflineIndDb.prototype.multiread = function (reads, callback) {
        this.tx(reads.map(function(r){return r.table;}), "readonly", function(tx, done){
            var results = {} ;
            var readPromises = [] ;
            reads.forEach(function(read){
                if(read.getByPk){
                    readPromises.push(new Promise(function(resolve, reject){
                        tx.getByPk(read.table, read.getByPk, read.joinFetch, function(err, res){
                            if(err){ return reject(err) ;}
                            results[read.name] = res ;
                            resolve() ;
                        });
                    })) ;
                }else if(read.search){
                    readPromises.push(new Promise(function(resolve, reject){
                        tx.search(read.table, read.search, read.joinFetch, read.orderBy, read.offset, read.limit, function(err, res){
                            if(err){ return reject(err) ;}
                            results[read.name] = res ;
                            resolve() ;
                        });
                    })) ;
                }else if(read.searchFirst){
                    tx.push(new Promise(function(resolve, reject){
                        storage.searchFirst(read.table, read.searchFirst, read.joinFetch, read.orderBy, function(err, res){
                            if(err){ return reject(err) ;}
                            results[read.name] = res ;
                            resolve() ;
                        });
                    })) ;
                }
            }) ;
            Promise.all(readPromises).then(function(){
                done(null, results) ;
            }).catch(function(err){
                done(err) ;
            }) ;
        }, callback);
    };

   

    /**
     * Create a new transaction
     * 
     * @param {VeloxDbOfflineIndDb} db VeloxDbOfflineIndDb instance
     * @param {string} [mode] read mode (readonly, readwrite). Default: readwrite
     */
    function VeloxDbOfflineIndDbTransaction(db, tables,  mode, callbackFinished){
        this.db = db ;
        this.tables = tables;
        this.mode = mode||"readwrite";
        this.tx = db.db.transaction(tables, mode);
        this.tx.onerror = function(){
            console.log("transaction error", this.tx.error) ;
            callbackFinished(this.tx.error) ;
        }.bind(this) ;
        this.tx.onabort = function(){
            console.log("transaction abort", this.tx.error) ;
            callbackFinished(this.tx.error) ;
        }.bind(this) ;
        this.tx.oncomplete = function() {
            callbackFinished() ;
        };
    }

    VeloxDbOfflineIndDbTransaction.prototype.abort = function () {
        this.tx.abort() ;
    } ;

    VeloxDbOfflineIndDbTransaction.prototype.rollback = VeloxDbOfflineIndDbTransaction.prototype.abort ;

    VeloxDbOfflineIndDbTransaction.prototype.insert = function (table, record, callback) {
        record.velox_version_record = 0;
        record.velox_version_date = new Date();
        var request = this.tx.objectStore(table).add(record);
        request.onsuccess = function() {
            return callback(null, record);
        };
        request.onerror = function() {
            return callback(request.error);
        };
    };

    VeloxDbOfflineIndDbTransaction.prototype.update = function (table, record, callback) {
        record.velox_version_record = (record.velox_version_record || 0) + 1;
        record.velox_version_date = new Date();
        var request = this.tx.objectStore(table).put(record);
        request.onsuccess = function() {
            return callback(null, record);
        };
        request.onerror = function() {
            return callback(request.error);
        };
    };

    VeloxDbOfflineIndDbTransaction.prototype.remove = function (table, pkOrRecord, callback) {
        var request = this.tx.objectStore(table).delete(this._pkSearch(table, pkOrRecord));
        request.onsuccess = function() {
            return callback();
        };
        request.onerror = function() {
            return callback(request.error);
        };
    };


    VeloxDbOfflineIndDbTransaction.prototype.removeWhere = function (table, conditions, callback) {
        var promises = [] ;
        this.search(table, conditions, function(err, records){
            if(err){
                return callback(err) ;
            }
            records.forEach(function(r){
                promises.push(new Promise(function(resolve, reject){
                    this.remove(table, r, function(err){
                        if(err){ return reject(err) ;}
                        resolve() ;
                    }) ;
                }.bind(this))) ;
            }.bind(this)) ;
        }.bind(this)) ;

        Promise.all(promises).then(function(){
            callback() ;
        }).catch(function(err){
            callback(err) ;
        }) ;
    };

    VeloxDbOfflineIndDbTransaction.prototype.getByPk = function (table, pkOrRecord, joinFetch, callback) {
        if(typeof(joinFetch) === "function"){
            callback = joinFetch ;
            joinFetch = null;
        }

        var request = this.tx.objectStore(table).get(this._pkSearch(table, pkOrRecord));
        request.onsuccess = function() {
            var record = request.result ;
            this._doJoinFetch(table, joinFetch, record, function(err){
                if(err){ return callback(err) ;}
                callback(null, record);
            }) ;
        }.bind(this);
        request.onerror = function() {
            return callback(request.error);
        };
    };

    VeloxDbOfflineIndDbTransaction.prototype._doJoinFetch = function (table, joinFetch, record, callback) {
        if(joinFetch){
            var tablesValues = {} ;
            var searchPromises = [] ;
            joinFetch.forEach(function(join){

                var searchJoin = null ;

                var thisTable = join.thisTable || table ;
                if(join.thisTable){
                    if(!this.schema[join.thisTable]){ throw ("Unknown table "+join.thisTable) ;}
                }
                var thisField = join.thisField ;
                if(thisField){
                    if(!this.schema[thisTable].columns.some((c)=>{ return c.name === thisField ;})){ 
                        throw ("Unknown columns "+thisTable+"."+thisField) ;
                    }
                }
                var otherField = join.otherField ;
                if(otherField){
                    if(!this.schema[join.otherTable].columns.some((c)=>{ return c.name === otherField ;})){ 
                        throw ("Unknown columns "+join.otherTable+"."+otherField) ;
                    }
                }

                if(otherField && !thisField || !otherField && thisField){ throw ("You must set both otherField and thisField") ; }

                var pairs = {} ;
                if(!otherField){
                    //assuming using FK

                    //look in this table FK
                    this.schema[thisTable].fk.forEach(function(fk){
                        if(fk.targetTable === join.otherTable){
                            pairs[fk.thisColumn] = fk.targetColumn ;
                        }
                    }.bind(this));
                    
                    if(Object.keys(pairs).length === 0){
                        //look in other table FK
                        this.schema[join.otherTable].fk.forEach(function(fk){
                            if(fk.targetTable === thisTable){
                                pairs[fk.targetColumn] = fk.thisColumn ;
                            }
                        }) ;
                    }

                    if(Object.keys(pairs).length === 0){
                        throw ("No otherField/thisField given and can't find in FK") ;
                    }
                }else{
                    pairs[thisField] = otherField ;
                }

                if(thisTable === table){
                    searchJoin = {} ;
                    Object.keys(pairs).forEach(function(f){
                        searchJoin[f] = record[pairs[f]] ;
                    }) ;
                }

                var type = join.type || "2one" ;
                var limit = null;
                if(type === "2one"){
                    limit = 1 ;
                }else{
                    throw ("Unknown join type "+type+", expected 2one or 2many") ;
                }
                //by default the record is to add on the main record we fetched
                var recordHolder = record;
                if(thisTable !== table){
                    //the record is to put on a subrecord
                    recordHolder = tablesValues[thisTable] ;
                }
                if(Array.isArray(recordHolder)){
                    //the record holder has many values, we search for each of its value
                    recordHolder.forEach(function(r, i){
                        searchPromises.push(new Promise(function(resolve, reject){
                            this.search(join.otherTable, searchJoin, join.joins, 0, limit, function(err, otherRecords){
                                if(err){ return reject(err) ;}
                                recordHolder[i][join.name||join.otherTable] = limit===1?otherRecords[0]:otherRecords ;
                                resolve() ;
                            }) ;
                        }.bind(this))) ;
                    }.bind(this));
                }else{
                    //the record holder has only one value, we search for it
                    searchPromises.push(new Promise(function(resolve, reject){
                        this.search(join.otherTable, searchJoin, join.joins, 0, limit, function(err, otherRecords){
                            if(err){ return reject(err) ;}
                            recordHolder[join.name||join.otherTable] = limit===1?otherRecords[0]:otherRecords ;
                            resolve() ;
                        }) ;
                    }.bind(this))) ;
                }
            }.bind(this)) ;

            Promise.all(searchPromises).then(function(){
                callback() ;
            }).catch(function(err){
                callback(err) ;
            }) ;

        }else{
            callback() ;
        }
    } ;

    VeloxDbOfflineIndDbTransaction.prototype._checkIndexesAndOpenCursor = function (table, orderBy, callback) {
        if(!orderBy){
            var request = this.tx.objectStore(table).openCursor();
            callback(null, request) ;
        }else{
            var direction;
            var mixedDirections = false ;
            var cols = orderBy.split(",").map(function(o){
                var splitted = o.trim().split(" ") ;
                var dir = splitted.length>1 && /desc$/.test(splitted[1])?"prev":"next" ;
                if(direction && direction !== dir){
                    mixedDirections = true ;
                }
                direction = dir ;
                return splitted[0] ;
            }) ;
            if(mixedDirections){
                return callback("You can have order with different directions "+orderBy) ;
            }
            var indexName = cols.join(",") ;

            if(!this.tx.objectStore(table).indexNames.contains(indexName)){
                //missing index, must add it
                // assumes db is a previously opened connection
                var oldVersion = this.db.db.version; 
                this.db.db.close();

                // force an upgrade to a higher version
                var open = indexedDB.open(this.db.db.name, oldVersion + 1);
                open.onupgradeneeded = function() {
                    var tx = open.transaction;
                    // grab a reference to the existing object store
                    var objectStore = tx.objectStore(table);
                    // create the index
                    objectStore.createIndex(indexName, cols);
                };
                open.onsuccess = function() {
                    // store the new connection for future use
                    this.db = open.result;
                    this.tx = this.db.db.transaction(this.tables,this.mode);
                    var request = this.tx.objectStore(table).index(indexName).openCursor(null, direction);
                    callback(null, request) ;
                }.bind(this);
            }else{
                //index already exists
                var request = this.tx.objectStore(table).index(indexName).openCursor(null, direction);
                callback(null, request) ;
            }
        }
    };


    VeloxDbOfflineIndDbTransaction.prototype.search = function (table, search, joinFetch, orderBy, offset, limit, callback) {
        if(typeof(joinFetch) === "function"){
            callback = joinFetch;
            joinFetch = null;
            orderBy = null;
            offset = 0;
            limit = null ;
        } 
        if(typeof(joinFetch) === "string"){
            callback = limit;
            limit = offset;
            offset = orderBy;
            orderBy = joinFetch;
            joinFetch = null ;
        } 
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

        var records = [];
        var request = this.tx.objectStore(table).openCursor();
        var off = offset || 0 ;
        request.onerror = function() {
            return callback(request.error);
        };
        request.onsuccess = function(event) {
            var cursor = event.target.result;
            if(cursor) {
                // cursor.value contains the current record being iterated through
                // this is where you'd do something with the result
                var currentRecord = cursor.value ;
                if(this.testRecord(currentRecord, search)){
                    if(off > 0){
                        off-- ;
                    }else{
                        records.push(currentRecord) ;
                    }
                }
                if(limit && records.length === limit){
                    this._doJoinFetch(table, joinFetch, records, function(err){
                        if(err){ return callback(err) ; }
                        callback(null, records) ;
                    }) ;
                }
                cursor.continue();
            } else {
                // no more results
                callback(null, records) ;
            }
        }.bind(this);
    };

    VeloxDbOfflineIndDbTransaction.prototype.testRecord = function(record, search){
        return Object.keys(search).every(function (k) {
            var val = search[k];

            if(k === "$or"){
                return val.some(function(orPart){
                    return this.testRecord(record, orPart) ;
                }.bind(this)) ;
            }
            if(k === "$and"){
                return val.every(function(orPart){
                    return this.testRecord(record, orPart) ;
                }.bind(this)) ;
            }


            if (val && val.operator === "between" && Array.isArray(val.value)) {
                return record[k] && record[k] >= val.value[0] && record[k] <= val.value[1] ;
            } else {
                if (val && typeof (val) === "object" && val.ope) {
                    switch (val.ope.toLowerCase()) {
                        case "=":
                            return record[k] == val.value ;
                        case ">":
                            return record[k] > val.value ;
                        case ">=":
                            return record[k] >= val.value ;
                        case "<":
                            return record[k] < val.value ;
                        case "<=":
                            return record[k] <= val.value ;
                        case "<>":
                            return record[k] != val.value ;
                        case "in":
                            return Array.isArray(val.value) && val.value.indexOf(record[k]) !== -1 ;
                            case "not in":
                            return Array.isArray(val.value) && !val.value.indexOf(record[k]) !== -1 ;
                        }
                } else if (Array.isArray(val)) {
                    return Array.isArray(val) && val.indexOf(record[k]) !== -1 ;
                } else if (val && typeof (val) === "object" && val.constructor === RegExp) {
                    return val.test(record[k]) ;
                } else if (val && typeof (val) === "string" && val.indexOf("%") !== -1) {
                    return new RegExp(val.replace(/%/g, "*")).test(record[k]) ;
                }
            }
            return false;
        });
    } ;

    VeloxDbOfflineIndDbTransaction.prototype._pkSearch = function (table, pkOrRecord) {
        var pk = this.db.schema[table].pk;
        if (!pk) {
            throw "Can't find pk for table " + table;
        }
        var search = [];
        if (pk.length === 1 && typeof (pkOrRecord) !== "object") {
            search = pkOrRecord;
        } else {
            pk.forEach(function (k) {
                search.push(pkOrRecord[k]);
            });
        }
        return search;
    };



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
            return callback(null, this._sanatizeRecord(record));
        } catch (err) {
            return callback(err);
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
            return callback();
        } catch (err) {
            return callback(err);
        }
    };

    VeloxDbOfflineLoki.prototype.transactionalChanges = function (changeSet, callback) {
        this._doChanges(changeSet.slice(), [], callback);
    };

    VeloxDbOfflineLoki.prototype._doChanges = function (changeSet, results, callback) {
        if (changeSet.length === 0) {
            return callback(null, results);
        } 
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



    VeloxDbOfflineLoki.prototype._doJoinFetch = function (table, joinFetch, record) {
        if(joinFetch){
            var tablesValues = {} ;
            joinFetch.some(function(join){

                var searchJoin = null ;

                var thisTable = join.thisTable || table ;
                if(join.thisTable){
                    if(!this.schema[join.thisTable]){ throw ("Unknown table "+join.thisTable) ;}
                }
                var thisField = join.thisField ;
                if(thisField){
                    if(!this.schema[thisTable].columns.some((c)=>{ return c.name === thisField ;})){ 
                        throw ("Unknown columns "+thisTable+"."+thisField) ;
                    }
                }
                var otherField = join.otherField ;
                if(otherField){
                    if(!this.schema[join.otherTable].columns.some((c)=>{ return c.name === otherField ;})){ 
                        throw ("Unknown columns "+join.otherTable+"."+otherField) ;
                    }
                }

                if(otherField && !thisField || !otherField && thisField){ throw ("You must set both otherField and thisField") ; }

                var pairs = {} ;
                if(!otherField){
                    //assuming using FK

                    //look in this table FK
                    this.schema[thisTable].fk.forEach(function(fk){
                        if(fk.targetTable === join.otherTable){
                            pairs[fk.thisColumn] = fk.targetColumn ;
                        }
                    }.bind(this));
                    
                    if(Object.keys(pairs).length === 0){
                        //look in other table FK
                        this.schema[join.otherTable].fk.forEach(function(fk){
                            if(fk.targetTable === thisTable){
                                pairs[fk.targetColumn] = fk.thisColumn ;
                            }
                        }) ;
                    }

                    if(Object.keys(pairs).length === 0){
                        throw ("No otherField/thisField given and can't find in FK") ;
                    }
                }else{
                    pairs[thisField] = otherField ;
                }

                if(thisTable === table){
                    searchJoin = {} ;
                    Object.keys(pairs).forEach(function(f){
                        searchJoin[f] = record[pairs[f]] ;
                    }) ;
                }else{
                    if(!tablesValues[thisTable]){
                        throw ("Can't find "+thisTable+" in join chaining") ;
                    }
                    if(!Array.isArray(tablesValues[thisTable])){
                        searchJoin = {} ;
                        Object.keys(pairs).forEach(function(f){
                            searchJoin[f] = tablesValues[thisTable][pairs[f]] ;
                        }) ;
                    }else{
                        searchJoin = [] ;
                        tablesValues[thisTable].forEach(function(r){
                            var s = {};
                            Object.keys(pairs).forEach(function(f){
                                s[f] = r[pairs[f]] ;
                            }) ;
                            searchJoin.push(s) ;
                        }) ;
                    }
                }

                var addTableValue = function(otherTable, otherRecord){
                        if(!otherRecord){ return ; }
                        if(!tablesValues[otherTable]){
                            tablesValues[otherTable] = otherRecord ;
                        }else{
                            if(Array.isArray(tablesValues[otherTable])){
                                if(Array.isArray(otherRecord)){
                                    tablesValues[otherTable] = tablesValues[otherTable].concat(otherRecord) ;    
                                }else{
                                    tablesValues[otherTable].push(otherRecord) ;
                                }
                            }else{
                                if(Array.isArray(otherRecord)){
                                    tablesValues[otherTable] = [tablesValues[otherTable]].concat(otherRecord) ;    
                                }else{
                                    tablesValues[otherTable] = [tablesValues[otherTable], otherRecord] ;
                                }
                            }
                        }
                } ;

                var type = join.type || "2one" ;
                var searchFunc = null;
                if(type === "2one"){
                    searchFunc = "findOne" ;
                }else if(type === "2many"){
                    searchFunc = "find" ;
                }else{
                    throw ("Unknown join type "+type+", expected 2one or 2many") ;
                }
                //by default the record is to add on the main record we fetched
                var recordHolder = record;
                if(thisTable !== table){
                    //the record is to put on a subrecord
                    recordHolder = tablesValues[thisTable] ;
                }
                if(Array.isArray(recordHolder)){
                    //the record holder has many values, we search for each of its value
                    recordHolder.forEach(function(r, i){
                        var otherRecord = this.getCollection(join.otherTable)[searchFunc](this._translateSearch(searchJoin[i])) ;
                        recordHolder[i][join.name||join.otherTable] = otherRecord ;
                        addTableValue(join.otherTable, otherRecord) ;
                    }.bind(this));
                }else{
                    //the record holder has only one value, we search for it
                    var otherRecord = this.getCollection(join.otherTable)[searchFunc](this._translateSearch(searchJoin)) ;
                    recordHolder[join.name||join.otherTable] = otherRecord ;
                    addTableValue(join.otherTable, otherRecord) ;
                }
            }.join(this)) ;
        }
    } ;
    VeloxDbOfflineLoki.prototype.getByPk = function (table, pkOrRecord, joinFetch, callback) {
        if(typeof(joinFetch) === "function"){
            callback = joinFetch ;
            joinFetch = null;
        }
        var record ;
        try {
            record = this.getCollection(table).findOne(this._pkSearch(table, pkOrRecord));
            if (record) {
                record = this._sanatizeRecord(record) ;
                this._doJoinFetch(table, joinFetch, record) ;
            }
        } catch (err) {
            return callback(err);
        }
        callback(null,record);
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



    VeloxDbOfflineLoki.prototype.search = function (table, search, joinFetch, orderBy, offset, limit, callback) {
        if(typeof(joinFetch) === "function"){
            callback = joinFetch;
            joinFetch = null;
            orderBy = null;
            offset = 0;
            limit = null ;
        } 
        if(typeof(joinFetch) === "string"){
            callback = limit;
            limit = offset;
            offset = orderBy;
            orderBy = joinFetch;
            joinFetch = null ;
        } 
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

        var records = [];
        try {
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
            var records = this._sanatizeRecord(records);
            if(joinFetch){
                records.forEach(function(record){
                    this._doJoinFetch(table, joinFetch, record) ;
                }.bind(this)) ;
            }
        } catch (err) {
            return callback(err);
        }
        callback(null, records);
    };


    VeloxDbOfflineLoki.prototype.searchFirst = function (table, search, joinFetch, orderBy, callback) {
        if(typeof(joinFetch) === "function"){
            callback = joinFetch;
            joinFetch = null;
            orderBy = null;
        }
        if(typeof(joinFetch) === "string"){
            callback = orderBy;
            orderBy = joinFetch;
            joinFetch = null;
        }
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