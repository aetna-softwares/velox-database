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
     * Create an unique ID
     */
    function uuidv4() {
        if(typeof(window.crypto) !== "undefined" && crypto.getRandomValues){
            return ([1e7]+-1e3+-4e3+-8e3+-1e11).replace(/[018]/g, function(c) {
                return (c ^ crypto.getRandomValues(new Uint8Array(1))[0] & 15 >> c / 4).toString(16)
            }) ;
        }else{
            return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
                var r = Math.random() * 16 | 0, v = c == 'x' ? r : (r & 0x3 | 0x8);
                return v.toString(16);
            });
        }
    }

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

    var currentUser = null;

    function saveOfflineChange(changes) {
        var localChanges = getOfflineChange();
        localChanges.push({
            date: new Date(),
            uuid: uuidv4(),
            changes: changes
        });
        var key = LOCAL_CHANGE_KEY;
        if(currentUser){
            key = currentUser.login+"_"+LOCAL_CHANGE_KEY;
        }
        localStorage.setItem(key, JSON.stringify(localChanges));
    }

    function getOfflineChange() {
        var key = LOCAL_CHANGE_KEY;
        if(currentUser){
            key = currentUser.login+"_"+LOCAL_CHANGE_KEY;
        }
        var localChanges = localStorage.getItem(key);
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
        var key = LOCAL_CHANGE_KEY;
        if(currentUser){
            key = currentUser.login+"_"+LOCAL_CHANGE_KEY;
        }
        localStorage.setItem(key, JSON.stringify(localChanges));
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
            this.sync(function(err){
                if(err){ console.info("Sync failed, assume offline", err); }
                callback() ;
            }) ;
        },
        after: function(callback){
            this.sync(function(err){
                if(err){ console.info("Sync failed, assume offline", err); }
                callback() ;
            }) ;
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
                this.sync(function(err){
                    if(err){ console.info("Sync failed, assume offline", err); }
                    callback() ;
                }) ;
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
        if(this.client.currentUser && this.client.currentUser !== currentUser){
            //user change, force reprepare
            prepareDone = false;
            this.lastSyncDate = null;
        }
        if(prepareDone){
            return callback() ;
        }

        currentUser = this.client.currentUser ;
        this.getSchema(function(err, schema){
            if (err) { return callback(err); }

            if (!storage) {
                console.error("No storage engined defined. Use VeloxDatabaseClient.setOfflineStorageEngine to specify one ");
            }
            storage.prepare({ prefix: currentUser?currentUser.login+"_":"" , schema : schema}, function(err){
                if(err){
                    return callback(err) ;
                }
                prepareDone = true ;
                callback() ;
            });
        }) ;


    }

    

    function isOffline(tableName, action){
        if(Array.isArray(tableName)){
            //transaction changes list
            return tableName.every(function(change){
                if(change.action){
                    return isOffline(change.table, change.action) ;
                }else{
                    //action auto, need insert/getByPk/update
                    return isOffline(change.table, "insert") && isOffline(change.table, "read")  && isOffline(change.table, "update");
                }
            }) ;
        }
        if(tableSettings){
            var isOfflineTable = false;
            tableSettings.some(function(table){
                if(table.name === tableName){
                    if(Array.isArray(table.offline)){
                        isOfflineTable = action === "any" || table.offline.indexOf(action) !== -1 ;
                    }else{
                        isOfflineTable = table.offline ;
                    }
                    return true ;
                }
            }) ;
            return isOfflineTable ;
        }else{
            return true ;
        }
    }

    extension.extendsGlobal.isTableOffline = isOffline ;

    function doOperation(instance, action, args, callbackDo, callbackDone){
        if(action !== "multiread"){
            var ope = "read" ;
            if(action === "insert"){ ope = "insert" ;}
            if(action === "update"){ ope = "update" ;}
            if(!isOffline(args[0], ope)){
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

    extension.extendsObj.getSchema = function(callback){
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
    } ;

    extension.extendsObj.prepareSerializableRecords = function(table, records, callback){
        this.getSchema(function(err, schema){
            if(err){ return callback(err) ;}
            var preparedRecords = [] ;
            records.forEach(function(record){
                preparedRecords.push(this._prepareSerializableRecord(table, record, schema)) ;
            }.bind(this)) ;
            callback(null, preparedRecords) ;
        }.bind(this)) ;
    } ;

    extension.extendsObj.prepareSerializableRecord = function(table, record, callback){
        this.prepareSerializableRecords(table, [record], function(err, records){
            if(err){ return callback(err) ;}
            callback(null, records[0]) ;
        });
    } ;
    
    extension.extendsObj._prepareSerializableRecord = function(table, record, schema){
        var preparedRecord = {} ;
        schema[table].columns.forEach(function(col){
            var val = record[col.name] ;
            if(val !== undefined){
                if(val && typeof(val) === "object" && val.constructor != Date){
                    if(val.toNumber){
                        val = val.toNumber() ;
                    }else if(val.toString){
                        val = val.toString() ;
                    }else{
                        val = ""+val ;
                    }
                }
                preparedRecord[col.name] = val ;
            }
        }) ;
        return preparedRecord ;
    } ;

    //TODO check schema to have foreign key and check consistence, if the FK is wrong sync will fail afterward
    extension.extendsObj.insert = function (table, record, callback) {
        doOperation(this, "insert" ,arguments, function(done){
            this.prepareSerializableRecord(table, record, function(err, record){
                if(err){ return done(err) ;}
                record.velox_version_record = 0;
                record.velox_version_date = new Date();
                saveOfflineChange([{ action: "insert", table: table, record: record }]);
                storage.insert(table, record, done);
            }.bind(this)) ;
        }.bind(this), callback) ;
    };

    //TODO check schema to have foreign key and check consistence, if the FK is wrong sync will fail afterward
    extension.extendsObj.update = function (table, record, callback) {
        doOperation(this, "update", arguments, function(done){
            this.prepareSerializableRecord(table, record, function(err, record){
                if(err){ return done(err) ;}
                record.velox_version_record = (record.velox_version_record || 0) + 1;
                record.velox_version_date = new Date();
                saveOfflineChange([{ action: "update", table: table, record: record }]);
                storage.update(table, record, done);
            }.bind(this)) ;
        }.bind(this), callback) ;
    };

    //TODO check schema to have foreign key and check consistence, if the FK is wrong sync will fail afterward
    extension.extendsObj.remove = function (table, record, callback) {
        doOperation(this, "remove", arguments,  function(done){
            this.prepareSerializableRecord(table, record, function(err, record){
                if(err){ return done(err) ;}
                saveOfflineChange([{ action: "remove", table: table, record: record }]);
                storage.remove(table, record, done);
            }.bind(this)) ;
        }.bind(this), callback) ;
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
            this.getSchema(function(err, schema){
                if(err){ return callback(err) ;}
                changeSet.forEach(function(change){
                    change.record.velox_version_record = change.record.velox_version_record!==undefined?change.record.velox_version_record+1:0;
                    change.record.velox_version_date = new Date();
                    change.record = this._prepareSerializableRecord(change.table, change.record, schema) ;
                }.bind(this)) ;
                saveOfflineChange(changeSet);
                storage.transactionalChanges(changeSet, done);
            }.bind(this)) ;
        }.bind(this), callback) ;
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
            storage.searchFirst(table, search, joinFetch, orderBy, done);
        }, callback) ;
    };
    
    function getAllTableNames(read, names){
        if(!names){
            names = [] ;
        }
        if(read.table){
            names.push(read.table) ;
        }
        if(read.otherTable){
            names.push(read.otherTable) ;
        }
        if(read.joinFetch || read.joins){
            (read.joinFetch || read.joins).forEach(function(join){
                getAllTableNames(join, names) ;
            }) ;
        }
        return names ;
    }

    extension.extendsObj.multiread = function(reads, callback){
        var offlineReads = [] ;
        var onlineReads = {} ;
        Object.keys(reads).forEach(function(k){
            if(!reads[k].table){
                reads[k].table = k ;
            }
            reads[k].name = k;

            if(!getAllTableNames(reads[k]).every(function(t){ return isOffline(t); })){
                onlineReads[k] = reads[k] ;
                return  ;
            }

            offlineReads.push(reads[k]) ;
        }) ;

        
        doOperation(this, "multiread", [reads, callback], function(done){
            storage.multiread(offlineReads, function(err, results){
                if(err){ return done(err) ;}
                if(Object.keys(onlineReads).length>0){
                    this.constructor.prototype.multiread.bind(this)(onlineReads, function(err, onlineResults){
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
        
        this.client.ajax("syncGetTime", "POST", {date: start}, function (err, lapseServer) {
            if(err){ return callback(err);}

            if(Math.abs(lapseServer) < 500){
                //accept a 500ms difference, the purpose is to distinguish who from 2 offline users did modif the first
                //it is acceptable to mistake by a second
                return callback(null, lapse) ;
            }

            calculateTimeLapse.bind(this)(lapse+lapseServer, tries, callback) ;
        }.bind(this)) ;
    } ;

    var tableToForceRefresh = {} ;

    var uploadChanges = function (callback) {
        var localChanges = getOfflineChange();
        if (localChanges.length > 0) {
            //local change to set to server
            calculateTimeLapse.bind(this)(0, 0, function(err, lapse){
                if(err){ return callback(err) ;}
                localChanges[0].timeLapse = lapse ;
                this.client.ajax("sync", "POST", {changes: localChanges[0]}, "json", function (err, result) {
                    if (err) {
                        return callback(err);
                    }
                    if(result && result.shouldRefresh){
                        //something went wrong on server we should force a refresh on concerned tables
                        localChanges[0].changes.forEach(function(c){
                            tableToForceRefresh[c.table] = true ;
                        }) ;
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

    /**
     * Sync the schema definition
     * 
     * @param {function(Error, object)} callback called on finish, give stats about what has been sync
     */
    extension.extendsProto.syncSchema = function (callback) {
        prepare.bind(this)(function (err) {
            if (err) { return callback(err); }

            syncSchema.bind(this)(callback) ;
        }.bind(this));
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
                    if (err) { 
                        syncing = false;
                        return callback(err); 
                    }

                    //then check tables
                    var search = {};
                    if(!tables){
                        //no table give, add all offline tables
                        tables = Object.keys(this.schema).filter(function(tableName){
                            return tableName !== "__version" && isOffline(tableName) ;
                        }) ;

                        //case of view that is composed by many table, must sync if any of used tables is modified
                        tables.forEach(function(tableName){
                            var tableDef = this.schema[tableName] ;
                            if(tableDef.viewOfTables){
                                tableDef.viewOfTables.forEach(function(subTable){
                                    if(tables.indexOf(subTable.name) === -1){
                                        tables.push(subTable.name) ;
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
                            var distantVersions = {};
                            distantTablesVersions.forEach(function (distantTable) {
                                distantVersions[distantTable.table_name] = distantTable.version_table;
                            });

                            //add all tables with different version number
                            var tableToSync = tables.filter(function(table){
                                var distantVersion = distantVersions[table] ;
                                var localVersion = localVersions[table] ;
                                if(!localVersion || tableToForceRefresh[table]){
                                    localVersions[table] = -1 ;
                                }
                                return (
                                    !distantVersion || //case when distant version number is unknown, should sync for safety
                                    !localVersion ||   //case when locale version is unkown, never seen
                                    localVersion < distantVersion //case when distant version higher
                                ) ;
                            }) ;

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
                                    
                                    tableToSync.forEach(function(table){
                                        //reset the force refresh
                                        tableToForceRefresh[table] = false ;
                                    }) ;

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

    function syncTables(tablesToSync, localVersions, pCallback) {
        if (tablesToSync.length === 0) {
            return pCallback() ;
        }

        var callbackCalled = false ;
        var callback = function(err){
            if(callbackCalled){ return; }
            callbackCalled = true ;
            pCallback(err) ;
        } ;
        var syncingCount = tablesToSync.length ;
        tablesToSync.forEach(function(table){
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
                        syncingCount-- ;
                        if(syncingCount === 0){
                            //finished
                            callback();
                        }
                    }.bind(this));
                }.bind(this));
            }.bind(this));
        }.bind(this)) ;
    }

    
    return extension;

})));