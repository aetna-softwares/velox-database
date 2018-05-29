/*global define, VeloxDatabaseClient */
; (function (global, factory) {
    if (typeof exports === 'object' && typeof module !== 'undefined') {
        var VeloxScriptLoader = require("velox-scriptloader") ;
        module.exports = factory(VeloxScriptLoader) ;
    } else if (typeof define === 'function' && define.amd) {
        define(['VeloxScriptLoader'], factory);
    } else {
        global.VeloxDatabaseClient.setOfflineStorageEngine(factory(global.veloxScriptLoader));
    }
}(this, (function (VeloxScriptLoader) {
    'use strict';

/**
     * @typedef VeloxDbOfflineIndDbOptions
     * @type {object}
     * @property {string} [dbName] indexedDB database name
     * @property {object} schema the database schema
     */

    /**
     * Offline db implementation based on IndexedDB
     */
    function VeloxDbOfflineIndDb(){
    }

    var indexedDB = window.indexedDB || window.mozIndexedDB || window.webkitIndexedDB || window.msIndexedDB || window.shimIndexedDB;

    VeloxDbOfflineIndDb.prototype.prepare = function (options, callback) {
        this.options = options;
        this.schema = options.schema;
        var dbName = this.options.dbName || "velox_sync_db" ;
        if(options.prefix){
            dbName = options.prefix+dbName;
        }
        var request = indexedDB.open(dbName);
        request.onerror = function() {
            callback(request.errorCode);
        };

        var updateDb = function(event){
            var db = event.target.result;
    
            Object.keys(this.schema).forEach(function(table){
                if ((VeloxDatabaseClient.isTableOffline(table, "any") || table === "velox_modif_table_version" || table === "velox_db_version") && !db.objectStoreNames.contains(table)) {
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
        //check if all tables exists in db (some table may have been set to offline mode without schema change)
        var storeNames = this.db.objectStoreNames ;
        var missingTable = tables.some(function(table){
            return !storeNames.contains(table) ;
        }) ;
        if(missingTable){
            //missing table, must add it
            // assumes db is a previously opened connection
            var oldVersion = this.db.version; 
            this.db.close();

            // force an upgrade to a higher version
            var open = indexedDB.open(this.db.name, oldVersion + 1);
            open.onupgradeneeded = function(event) {
                var db = event.target.result;
                Object.keys(this.schema).forEach(function(table){
                    if ((VeloxDatabaseClient.isTableOffline(table, "any") || table === "velox_modif_table_version" || table === "velox_db_version") && !db.objectStoreNames.contains(table)) {
                        var options = {} ;
                        if(this.schema[table].pk && this.schema[table].pk.length>0){
                            options.keyPath =this.schema[table].pk ;
                        }
                        db.createObjectStore(table, options);
                    }
                }.bind(this));
            }.bind(this);
            open.onsuccess = function() {
                // store the new connection for future use
                this.db = open.result;
                this._opentx(tables, mode, doTx, callback) ;
            }.bind(this);
        }else{
            this._opentx(tables, mode, doTx, callback) ;
        }
    };

    VeloxDbOfflineIndDb.prototype._opentx = function (tables, mode, doTx, callback) {
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
        }) ;

    };


    VeloxDbOfflineIndDb.prototype.insert = function (table, record, callback) {
        this.tx([table], "readwrite", function(tx, done){
            tx.insert(table, record, done) ;
        }, callback) ;
    };

    VeloxDbOfflineIndDb.prototype.update = function (table, record, callback) {
        this.getByPk(table, record, null, function(err, fullRecord){
            if(err){ return callback(err) ;}
            Object.keys(record).forEach(function(k){
                fullRecord[k] = record[k] ;
            }) ;
            this.tx([table], "readwrite", function(tx, done){
                tx.update(table, fullRecord, done) ;
            }, callback) ;
        }.bind(this)) ;
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

    VeloxDbOfflineIndDb.prototype.getJoinTables = function(joinFetch){
        var tables = [] ;
        joinFetch.forEach(function(j){
            if(tables.indexOf(j.otherTable) === -1){
                tables.push(j.otherTable) ;
            }
            if(j.joins){
                tables = tables.concat(this.getJoinTables(j.joins)) ;
            }
        }.bind(this)) ;
        return tables;
    } ;

    VeloxDbOfflineIndDb.prototype.getByPk = function (table, pkOrRecord, joinFetch, callback) {
        if(typeof(joinFetch) === "function"){
            callback = joinFetch ;
            joinFetch = null;
        }
        var tables = [table] ;
        if(joinFetch){
            tables = tables.concat(this.getJoinTables(joinFetch)) ;
        }
        this.tx(tables, "readonly", function(tx, done){
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

        var tables = [table] ;
        if(joinFetch){
            tables = tables.concat(this.getJoinTables(joinFetch)) ;
        }

        this.tx(tables, "readonly", function(tx, done){
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
        this.search(table, search, joinFetch, orderBy, 0, 1, function (err, results) {
            if (err) { return callback(err); }
            if (results.length === 0) {
                callback(null, null);
            } else {
                callback(null, results[0]);
            }
        }.bind(this));

    };

    VeloxDbOfflineIndDb.prototype.multiread = function (reads, callback) {
        
        var results = {} ;
        var runningTx = 0;
        var globalError = null;

        if(reads.length === 0){
            return callback(null, results) ;
        }

        reads.forEach(function(read){
            var tables = [read.table] ;
            if(read.joinFetch){
                tables = tables.concat(this.getJoinTables(read.joinFetch)) ;
            }

            runningTx++;
            
            this.tx(tables, "readonly", function(tx, done){
                if(read.getByPk){
                    tx.getByPk(read.table, read.getByPk, read.joinFetch, function(err, res){
                        if(err){ return done(err) ;}
                        results[read.name] = res ;
                        done(null, res) ;
                    });
                }else if(read.search){
                    //console.log("start search "+read.table+" in "+tx.idTr) ;
                    tx.search(read.table, read.search, read.joinFetch, read.orderBy, read.offset, read.limit, function(err, res){
                        //console.log("finish search "+read.table+" in "+tx.idTr, res) ;
                        if(err){ return done(err) ;}
                        results[read.name] = res ;
                        done(null, res) ;
                    });
                }else if(read.searchFirst){
                    tx.search(read.table, read.searchFirst, read.joinFetch, read.orderBy, 0, 1, function(err, res){
                        if(err){ return done(err) ;}
                        results[read.name] = res.length>0?res[0]:null ;
                        done(null, res.length>0?res[0]:null) ;
                    });
                }else{
                    done("No action found in multiread "+JSON.stringify(read)) ;
                }
            }, function(err){
                runningTx--;
                if(globalError){
                    //already in error, discard
                    return;
                }
                if(err){ 
                    globalError = err ;
                    return callback(err) ;
                }
                if(runningTx === 0){
                    callback(null, results) ;
                }
            }) ;
        }.bind(this));
    };

    var idTr = 0;

    /**
     * Create a new transaction
     * 
     * @param {VeloxDbOfflineIndDb} db VeloxDbOfflineIndDb instance
     * @param {string} [mode] read mode (readonly, readwrite). Default: readwrite
     */
    function VeloxDbOfflineIndDbTransaction(db, tables,  mode, callbackFinished){
        this.idTr = idTr++;
        this.db = db ;
        this.tables = tables;
        this.mode = mode||"readwrite";
        
        this.tx = db.db.transaction(tables, mode);
        //console.log("transaction start "+this.idTr) ;
        this.tx.onerror = function(){
            //console.log("transaction error "+this.idTr, this.tx.error) ;
            callbackFinished(this.tx.error) ;
        }.bind(this) ;
        this.tx.onabort = function(){
            //console.log("transaction abort "+this.idTr, this.tx.error) ;
            callbackFinished(this.tx.error) ;
        }.bind(this) ;
        this.tx.oncomplete = function() {
            //console.log("transaction done "+this.idTr) ;
            callbackFinished() ;
        }.bind(this) ;
    }

    VeloxDbOfflineIndDbTransaction.prototype.abort = function () {
        this.tx.abort() ;
    } ;

    VeloxDbOfflineIndDbTransaction.prototype.rollback = VeloxDbOfflineIndDbTransaction.prototype.abort ;

    

    VeloxDbOfflineIndDbTransaction.prototype.insert = function (table, record, callback) {
        try{
            var request = this.tx.objectStore(table).add(record);
            request.onsuccess = function() {
                return callback(null, record);
            };
            request.onerror = function() {
                return callback(request.error);
            };
        }catch(err){
            console.log("Error while insert in table", err) ;
            callback(err) ;
        }
    };

    VeloxDbOfflineIndDbTransaction.prototype.update = function (table, record, callback) {
        try {
            var request = this.tx.objectStore(table).put(record);
            request.onsuccess = function() {
                return callback(null, record);
            };
            request.onerror = function() {
                return callback(request.error);
            };
        }catch(err){
            console.log("Error while update in table", err) ;
            callback(err) ;
        }
    };

    VeloxDbOfflineIndDbTransaction.prototype.remove = function (table, pkOrRecord, callback) {
        try{
            var request = this.tx.objectStore(table).delete(this._pkSearch(table, pkOrRecord));
            request.onsuccess = function() {
                return callback();
            };
            request.onerror = function() {
                return callback(request.error);
            };
        }catch(err){
            console.log("Error while remove in table", err) ;
            callback(err) ;
        }
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

        try{
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
        }catch(err){
            console.log("Error while get by pk in table", err) ;
            callback(err) ;
        }
    };

    VeloxDbOfflineIndDbTransaction.prototype._doJoinFetch = function (table, joinFetch, record, callback) {
        if(joinFetch){
            var tablesValues = {} ;
            var runningSearch = 0 ;
            var searchError = false;
            joinFetch.forEach(function(join){

                var thisTable = join.thisTable || table ;
                if(join.thisTable){
                    if(!this.db.schema[join.thisTable]){ throw ("Unknown table "+join.thisTable) ;}
                }
                var thisField = join.thisField ;
                if(thisField){
                    if(!this.db.schema[thisTable].columns.some((c)=>{ return c.name === thisField ;})){ 
                        throw ("Unknown columns "+thisTable+"."+thisField) ;
                    }
                }
                var otherField = join.otherField ;
                if(otherField){
                    if(!this.db.schema[join.otherTable].columns.some((c)=>{ return c.name === otherField ;})){ 
                        throw ("Unknown columns "+join.otherTable+"."+otherField) ;
                    }
                }

                if(otherField && !thisField || !otherField && thisField){ throw ("You must set both otherField and thisField") ; }

                var pairs = {} ;
                if(!otherField){
                    //assuming using FK

                    //look in this table FK
                    this.db.schema[thisTable].fk.forEach(function(fk){
                        if(fk.targetTable === join.otherTable){
                            pairs[fk.thisColumn] = fk.targetColumn ;
                        }
                    }.bind(this));
                    
                    if(Object.keys(pairs).length === 0){
                        //look in other table FK
                        this.db.schema[join.otherTable].fk.forEach(function(fk){
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

                

                var type = join.type || "2one" ;
                var limit = null;
                if(type === "2one"){
                    limit = 1 ;
                }
                //by default the record is to add on the main record we fetched
                var recordHolder = record;
                if(thisTable !== table){
                    //the record is to put on a subrecord
                    recordHolder = tablesValues[thisTable] ;
                }
                if(!Array.isArray(recordHolder)){
                    recordHolder = [recordHolder] ;
                }
                recordHolder.forEach(function(r){
                    var searchJoin = {} ;
                    Object.keys(pairs).forEach(function(f){
                        searchJoin[pairs[f]] = r[f] ;
                    }) ;
                    //console.log("START join "+table+" > "+join.otherTable+" WHERE ", searchJoin);
                    runningSearch++ ;
                    this.search(join.otherTable, searchJoin, join.joins, null, 0, limit, function(err, otherRecords){
                        runningSearch--;
                        if(searchError){
                            //already stop in error, discard
                            return;
                        }
                        if(err){ 
                            searchError = err ;
                            return callback(err) ;
                        }
                        //console.log(">>END join "+table+" > "+join.otherTable+" (name : "+join.name+") RESULTS ", otherRecords);
                        r[join.name||join.otherTable] = limit===1?otherRecords[0]:otherRecords ;
                        if(runningSearch === 0){
                            callback() ;
                        }
                    }) ;
                }.bind(this)) ;
            }.bind(this));

            if(runningSearch === 0){
                //no search to do
                callback() ;
            }

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
        try{
            var request = this.tx.objectStore(table).openCursor();
            var off = offset || 0 ;
            request.onerror = function() {
                //console.log("search error in "+this.idTr) ;
                return callback(request.error);
            }.bind(this);
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
                    //console.log("start join fetch "+this.idTr, table, records, joinFetch) ;
                    this._doJoinFetch(table, joinFetch, records, function(err){
                        if(err){ return callback(err) ; }
                        callback(null, records) ;
                        //console.log("end join fetch "+this.idTr, table, records) ;
                    }.bind(this)) ;
                }
            }.bind(this);
        }catch(err){
            console.log("Error while search in table", err) ;
            callback(err) ;
        }
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
                } else {
                    return record[k] == val ;
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
            if(!Array.isArray(pkOrRecord)){
                pkOrRecord = [pkOrRecord] ;
            }
            search = pkOrRecord;
        } else {
            pk.forEach(function (k) {
                search.push(pkOrRecord[k]);
            });
        }
        return search;
    };

    return new VeloxDbOfflineIndDb() ;
    
})));