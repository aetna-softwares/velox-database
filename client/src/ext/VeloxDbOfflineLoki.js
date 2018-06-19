/*global define */
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

    var LOKIJS_VERSION = "1.5.5";

    var LOKIJS_LIB = [
        {
            name: "lokijs",
            type: "js",
            version: LOKIJS_VERSION,
            cdn: "https://cdnjs.cloudflare.com/ajax/libs/lokijs/$VERSION/lokijs.min.js",
            bowerPath: "lokijs/build/lokijs.min.js",
            npmPath: "lokijs/build/lokijs.min.js"
        },
        {
            name: "lokijs-indexed-adapter",
            type: "js",
            version: LOKIJS_VERSION,
            cdn: "https://cdnjs.cloudflare.com/ajax/libs/lokijs/$VERSION/loki-indexed-adapter.min.js",
            bowerPath: "lokijs/build/loki-indexed-adapter.min.js",
            npmPath: "lokijs/build/loki-indexed-adapter.min.js"
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
     * The Velox database loki engine
     * 
     * @constructor
     * 
     * @param {VeloxDbOfflineLokiOptions} options database client options
     */
    function VeloxDbOfflineLoki() {
        this.loki = null;
    }

    VeloxDbOfflineLoki.prototype.prepare = function (options, callback) {
        this.options = options ;
        this.schema = options.schema;
        this.importLibIfNeeded(function (err) {
            if (err) { return callback(err); }
            if (!this.loki) {
                var dbname = (options.prefix || "") + "velox-offline";
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
            var options = {};
            if(this.schema[table].pk.length === 1){
                options.unique = this.schema[table].pk;
            }
            //options.indices = [this.schema[table].pk];
            coll = this.loki.addCollection(table, options);
        }
        return coll;
    };

    VeloxDbOfflineLoki.prototype.insert = function (table, record, callback) {
        try {
            this.getCollection(table).insert(record);
        } catch (err) {
            if(callback) {callback(err);}
            return {err: err};
        }
        var result = this._sanatizeRecord(record) ;
        if(callback) {callback(null, result);}
        return {record : result} ;
    };

    VeloxDbOfflineLoki.prototype.update = function (table, record, callback) {
        var coll = this.getCollection(table) ;
        var existingRecord = coll.findOne(this._pkSearch(table, record));
        if(!existingRecord){
            var err = "Record not exist in table "+table+" : "+JSON.stringify(record) ;
            if(callback) {callback(err);}
            return {err: err};
        }
        Object.keys(record).forEach(function(k){
            existingRecord[k] = record[k] ;
        }) ;
        try {
            existingRecord.velox_version_record++;
            existingRecord.velox_version_date = new Date();
            coll.update(existingRecord);
        } catch (err) {
            if(callback) {callback(err);}
            return {err: err};
        }
        var result = this._sanatizeRecord(existingRecord) ;
        if(callback) {callback(null, result);}
        return {record : result} ;
    };

    VeloxDbOfflineLoki.prototype.remove = function (table, pkOrRecord, callback) {
        try {
            this.getCollection(table).findAndRemove(this._pkSearch(table, pkOrRecord));
        } catch (err) {
            if(callback) {callback(err);}
            return {err: err};
        }
        if(callback) {callback();}
        return {} ;
    };

    VeloxDbOfflineLoki.prototype.removeWhere = function (table, conditions, callback) {
        try {
            this.getCollection(table).findAndRemove(this._translateSearch(conditions));
        } catch (err) {
            if(callback) {callback(err);}
            return err;
        }
        if(callback) {callback();}
        return {} ;
    };

    VeloxDbOfflineLoki.prototype.transactionalChanges = function (changeSet, callback) {
        this._doChanges(changeSet.slice(), [], callback);
    };

    VeloxDbOfflineLoki.prototype._doChanges = function (changeSet, results, callback) {
        if (changeSet.length === 0) {
            return callback(null, results);
        } 
        for(var i=0; i<changeSet.length; i++){
            var change = changeSet[i] ;
            if (change.action === "insert") {
                var result = this.insert(change.table, change.record) ;
                if (result.err) { return callback(result.err); }
                results.push({ action: "insert", table: change.table, record: result.record });
            } else if (change.action === "update") {
                var result = this.update(change.table, change.record) ;
                if (result.err) { return callback(result.err); }
                results.push({ action: "update", table: change.table, record: result.record });
            } else if (change.action === "remove") {
                var result = this.remove(change.table, change.record) ;
                if (result.err) { return callback(result.err); }
                results.push({ action: "remove", table: change.table, record: change.record });
            } else {
                var result = this.getByPk(change.table, change.record);
                if (result.err) { return callback(result.err); }
                if (result.record) {
                    result = this.update(change.table, change.record);
                    if (result.err) { return callback(result.err); }
                    results.push({ action: "update", table: change.table, record: result.record });
                } else {
                    result = this.insert(change.table, change.record);
                    if (result.err) { return callback(result.err); }
                    results.push({ action: "insert", table: change.table, record: result.record });
                }
            }
        }
        return callback(null, results);
    };



    VeloxDbOfflineLoki.prototype._doJoinFetch = function (table, joinFetch, record) {
        if(joinFetch){
            var tablesValues = {} ;
            for(var i=0; i<joinFetch.length; i++){
                var join = joinFetch[i] ;

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
                    for(var y=0; y<this.schema[thisTable].fk.length; y++){
                        var fk = this.schema[thisTable].fk[y] ;
                        if(fk.targetTable === join.otherTable){
                            pairs[fk.thisColumn] = fk.targetColumn ;
                        }
                    }
                    
                    if(Object.keys(pairs).length === 0){
                        //look in other table FK
                        for(var y=0; y<this.schema[join.otherTable].fk.length; y++){
                            var fk = this.schema[join.otherTable].fk[y];
                            if(fk.targetTable === thisTable){
                                pairs[fk.targetColumn] = fk.thisColumn ;
                            }
                        }
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
                for(var a = 0; a < recordHolder.length; a++){
                    var r = recordHolder[a] ;

                    var searchJoin = {} ;
                    Object.keys(pairs).forEach(function(f){
                        searchJoin[pairs[f]] = r[f] ;
                    }) ;
                    if(join.joinSearch){
                        Object.keys(join.joinSearch).forEach(function(f){
                            searchJoin[f] = join.joinSearch[f] ;
                        }) ;
                    }
                    //console.log("START join "+table+" > "+join.otherTable+" WHERE ", searchJoin);
                    
                    var result = this.search(join.otherTable, searchJoin, join.joins, null, 0, limit);
                    if(result.err){ 
                        throw result.err ;
                    }
                    //console.log(">>END join "+table+" > "+join.otherTable+" (name : "+join.name+") RESULTS ", otherRecords);
                    r[join.name||join.otherTable] = limit===1?result.records[0]:result.records ;
                }
            }
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
            if(callback) {callback(err);}
            return {err: err} ;
        }
        if(callback) {callback(null,record);}
        return {record: record} ;
    };

    VeloxDbOfflineLoki.prototype._sanatizeRecord = function (record) {
        record = JSON.parse(JSON.stringify(record));
        if (Array.isArray(record)) {
            for(var i=0; i<record.length; i++){
                var r = record[i];
                delete r.$loki;
                delete r.meta;
            }
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
                    if (typeof (orderBy) === "string") {
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
                for(var i=0; i<records.length; i++){
                    var record = records[i] ;
                    this._doJoinFetch(table, joinFetch, record) ;
                }
            }
        } catch (err) {
            if(callback){callback(err);}
            return {err: err} ;
        }
        if(callback){callback(null, records);}
        return {records: records} ;
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
        this.search(table, search, joinFetch, orderBy, 0, 1, function (err, results) {
            if (err) { return callback(err); }
            if (results.length === 0) {
                callback(null, null);
            } else {
                callback(null, this._sanatizeRecord(results[0]));
            }
        }.bind(this));

    };

    VeloxDbOfflineLoki.prototype.multiread = function (reads, callback) {
        this._doASearch(reads, {}, callback);
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
        if (r.getByPk) {
            this.getByPk(r.table, r.getByPk, r.joinFetch, function (err, result) {
                if (err) { return callback(err); }
                results[r.name] = result;
                next();
            }.bind(this));
        } else if (r.search) {
            this.search(r.table, r.search, r.joinFetch, r.orderBy, r.offset, r.limit, function (err, records) {
                if (err) { return callback(err); }
                results[r.name] = records;
                next();
            }.bind(this));
        } else if (r.searchFirst) {
            this.searchFirst(r.table, r.searchFirst, r.joinFetch, r.orderBy, function (err, record) {
                if (err) { return callback(err); }
                results[r.name] = record;
                next();
            }.bind(this));
        } else {
            callback<("Unkown search action for " + JSON.stringify(r));
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
                } else if (val && typeof (val) === "string" && (val.indexOf("%") !== -1 || val.indexOf("*") !== -1)) {
                    translatedVal = { $regex: new RegExp(val.replace(/%/g, "*").replace(/\*/g, ".*"), "i") };
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


    return new VeloxDbOfflineLoki();
})));