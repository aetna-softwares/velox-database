; (function (global, factory) {
        if (typeof exports === 'object' && typeof module !== 'undefined') {
        module.exports = factory() ;
    } else if (typeof define === 'function' && define.amd) {
        define([], factory);
    } else {
        global.VeloxDatabaseClient = factory() ;
        global.VeloxServiceClient.registerExtension(new global.VeloxDatabaseClient());
    }
}(this, (function () { 'use strict';


    /**
     * @typedef VeloxDatabaseClientOptions
     * @type {object}
     * @property {string} [dbEntryPoint] base db entry point (default : api)
     */

    /**
     * The Velox database client
     * 
     * @constructor
     * 
     * @param {VeloxDatabaseClientOptions} options database client options
     */
    function VeloxDatabaseClient() {
    }



    VeloxDatabaseClient.prototype.init = function(client, callback){
        this.client = client ;
        this.dbEntryPoint = client.options.dbEntryPoint || "api/" ;
         if(this.dbEntryPoint[this.dbEntryPoint.length-1] !== "/"){
            //add trailing slash
            this.dbEntryPoint = this.dbEntryPoint+"/" ;
        }

        //add extension features
        VeloxDatabaseClient.extensions.forEach(function(extension){
            if(extension.extendsObj){
                Object.keys(extension.extendsObj).forEach(function (key) {
                        this[key] = extension.extendsObj[key].bind(this);
                }.bind(this));
            }
        }.bind(this));

        this.getSchema(function(err, schema){
        //client.ajax(this.dbEntryPoint+"schema", "GET", null, "json", function(err, schema){
            if(err){ 
                if(err == "401"){
                    err = "Access to schema required login, you should set /"+this.dbEntryPoint+"schema as public resources" ; 
                }
                return callback(err) ;
            }
            this.schema = schema;

            //add db api entry
            var dbApi = {} ;
            var dbApiPath = this.dbEntryPoint.split("/").filter(function(p){ return !!p.trim() ;}) ;
            var currentParent = client;
            dbApiPath.forEach(function(p, i){
                if(i<dbApiPath.length-1){
                    if(!currentParent[p]){
                        currentParent[p] = {} ;
                    }
                }else{
                    currentParent[p] = dbApi ;
                }
            }) ;
            client.__velox_database = dbApi ;

            //add sub api entry for each table
            Object.keys(schema).forEach(function(table){
                dbApi[table] = {} ;

                dbApi[table].insert = function(record, callback){
                    this.insert(table, record, callback) ;
                }.bind(this) ;
                dbApi[table].update = function(record, callback){
                    this.update(table, record, callback) ;
                }.bind(this) ;
                dbApi[table].remove = function(pkOrRecord, callback){
                    this.remove(table, pkOrRecord, callback) ;
                }.bind(this) ;
                dbApi[table].getPk = function(record, callback){
                    this.getPk(table, record, callback) ;
                }.bind(this) ;
                dbApi[table].getByPk = function(pkOrRecord, joinFetch, callback){
                    this.getByPk(table, pkOrRecord, joinFetch, callback) ;
                }.bind(this) ;
                dbApi[table].search = function(search, joinFetch, orderBy, offset, limit, callback){
                    this.search(table, search, joinFetch, orderBy, offset, limit, callback) ;
                }.bind(this) ;
                dbApi[table].searchFirst = function(search, joinFetch, orderBy, callback){
                    this.searchFirst(table, search,joinFetch, orderBy, callback) ;
                }.bind(this) ;
            }.bind(this)) ;

            //add global features
            dbApi.transactionalChanges = function(changeSet, callback){
                this.transactionalChanges(changeSet, callback) ;
            }.bind(this) ;
            dbApi.multiread = function(reads, callback){
                this.multiread(reads, callback) ;
            }.bind(this) ;
            dbApi.getSchema = dbApi.schema = function( callback){
                this.getSchema( callback) ;
            }.bind(this) ;
            dbApi.sync = function(tables, callback){
                this.sync(tables, callback) ;
            }.bind(this) ;
            dbApi.syncSchema = function(callback){
                this.syncSchema(callback) ;
            }.bind(this) ;


            callback() ;
        }.bind(this)) ;
    } ;


    /**
     * get database schema if not yet retrieved
     * 
     * @private
     * @param {function(Error)} callback called on finished
     */
    VeloxDatabaseClient.prototype._checkSchema = function(callback){
        if(this.schema){
            return callback() ;
        }
        //don't know schema yet, get it
        this.client.ajax(this.dbEntryPoint+"schema", "GET", null, "json", function(err, schema){
            if(err){ return callback(err) ;}
            this.schema = schema ;
            callback() ;
        }.bind(this)) ;
    };

    /**
     * Create the URL primary key for a record of a table
     * 
     * @private
     * @param {string} table the table of the record
     * @param {object} record the record containing the primary key or the primary key
     */
    VeloxDatabaseClient.prototype._createPk = function(table, record){
        if(!this.schema[table]){ throw "Unkown table "+table; }
        if(record === null || record === undefined){ throw "No proper PK provided for "+table; }
        if(typeof(record) === "object"){
            var pk = [] ;
            this.schema[table].pk.forEach(function(k){
                pk.push(encodeURIComponent(record[k])) ;
            }) ;
            return pk.join("/") ;
        }else{
            if(this.schema[table].pk.length>1){
                throw "Wrong pk format for table "+table+", expected : "+this.schema[table].pk.this.schema[table].pkjoin(", ") ;
            }
            return record;
        }
    } ;

    VeloxDatabaseClient.prototype.getPk = function(table, record, callback){
        this._checkSchema(function(err){
            if(err){ return callback(err); }
            var pkDef = this.schema[table].pk ;
            if(pkDef.length === 1){
                if(typeof(record) !== "object"){
                    //assume it is already the pk
                    return callback(null, record) ;    
                }else{
                    return callback(null, record[pkDef[0]]) ;
                }
            }else{
                if(typeof(record) !== "object"){
                    throw "The PK is composed of many columns, an object with each column is expected. Received : "+record ;
                }
                var pk = {};
                pkDef.forEach(function(p){
                    pk[p] = record[p] ;
                }) ;
                return callback(null, pk) ;
            }
        }.bind(this)) ;
    } ;


    /**
     * Get the schema of the database
     * 
     * @param {function(Error, object)} callback called with the schema of database
     */
    VeloxDatabaseClient.prototype.getSchema = function(callback){
        this._checkSchema(function(err){
            if(err){ return callback(err); }
            callback(null, JSON.parse(JSON.stringify(this.schema))) ;
        }.bind(this)) ;
    };

    /**
     * Insert a record in database
     * 
     * @param {string} table the table in which do the insert
     * @param {object} record the record to insert
     * @param {function(Error, object)} callback called with the record inserted in database
     */
    VeloxDatabaseClient.prototype.insert = function(table, record, callback){
        this._checkSchema(function(err){
            if(err){ return callback(err); }
            this.client.ajax(this.dbEntryPoint+table, "POST", record, "json", callback) ;
        }.bind(this)) ;
    };

    /**
     * Update a record in database
     * 
     * @param {string} table the table in which do the udpate
     * @param {object} record the record to update
     * @param {function(Error, object)} callback called with the record updated in database
     */
    VeloxDatabaseClient.prototype.update = function(table, record, callback){
        this._checkSchema(function(err){
            if(err){ return callback(err); }
            this.client.ajax(this.dbEntryPoint+table+"/"+this._createPk(table,record), 
                "PUT", record, "json", callback) ;    
        }.bind(this)) ;
    };

    /**
     * Delete a record in database
     * 
     * @param {string} table the table in which do the udpate
     * @param {object} pkOrRecord the record to delete or its primary key
     * @param {function(Error, object)} callback called when finished
     */
    VeloxDatabaseClient.prototype.remove = function(table, pkOrRecord, callback){
        this._checkSchema(function(err){
            if(err){ return callback(err); }
            this.client.ajax(this.dbEntryPoint+table+"/"+this._createPk(table, pkOrRecord), 
                "DELETE", null, "json", callback) ;    
        }.bind(this)) ;
    };

    /**
     * Do a set of change in a transaction
     * 
     * The change set format is :
     * [
     *      action: "insert" | "update" | "auto" ("auto" if not given)
     *      table : table name
     *      record: {record to sync}
     * ]
     * 
     * your record can contain the special syntax ${table.field} it will be replaced by the field value from last insert/update on this table in the transaction
     * it is useful if you have some kind of auto id used as foreign key
     * 
     * @example
     * [
     *      { table : "foo", record: {key1: "val1", key2: "val2"}, action: "insert"},
     *      { table : "bar", record: {foo_id: "${foo.id}", key3: "val3"}}
     * ]
     * 
     * 
     * @param {object} changeSet the changes to do in this transaction 
     * @param {function(Error)} callback called on finish give back the operation done with inserted/updated records
     */
    VeloxDatabaseClient.prototype.transactionalChanges = function(changeSet, callback){
        this._checkSchema(function(err){
            if(err){ return callback(err); }
            this.client.ajax(this.dbEntryPoint+"transactionalChanges", "POST", changeSet, "json", callback) ;    
        }.bind(this)) ;
    };

    /**
     * Get a record in the table by its pk
     * 
     * @example
     * //get by simple pk
     * client.getByPk("foo", "id", (err, fooRecord)=>{...})
     * 
     * //get with composed pk
     * client.getByPk("bar", {k1: "valKey1", k2: "valKey2"}, (err, barRecord)=>{...})
     * 
     * //already have the record containing pk value, just give it...
     * client.getByPk("bar", barRecordAlreadyHaving, (err, barRecordFromDb)=>{...})
     * 
     * @param {string} table the table name
     * @param {any|object} pkOrRecord the pk value. can be an object containing each value for composed keys
     * @param {function(Error,object)} callback called with result. give null if not found
     */
    VeloxDatabaseClient.prototype.getByPk = function(table, pkOrRecord, joinFetch,  callback){
        if(typeof(joinFetch) === "function"){
            callback = joinFetch;
            joinFetch = null;
        }
        this._checkSchema(function(err){
            if(err){ return callback(err); }
            this.client.ajax(this.dbEntryPoint+table+"/"+this._createPk(table, pkOrRecord),
                  "GET",joinFetch?{joinFetch: joinFetch}:null, "json", callback) ;    
        }.bind(this)) ;
    };

    /**
     * Do simple search in table
     * 
     * The search object can contains : 
     * simple equals condition as {foo: "bar"}
     * in condition as {foo: ["val1", "val2"]}
     * ilike condition as {foo: "bar%"} (activated by presence of %)
     * is null condition as {foo : null}
     * more complex conditions must specify operand explicitely :
     * {foo: {ope : ">", value : 1}}
     * {foo: {ope : "<", value : 10}}
     * {foo: {ope : "between", value : [from, to]}}
     * {foo: {ope : "not in", value : ["", ""]}}
     * 
     * @param {string} table table name
     * @param {object} search search object
     * @param {string} [orderBy] order by clause
     * @param {number} [offset] offset, default is 0
     * @param {number} [limit] limit, default is no limit
     * @param {function(Error, Array)} callback called on finished. give back the found records
     */
    VeloxDatabaseClient.prototype.search = function(table, search, joinFetch, orderBy, offset, limit, callback){
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
        if(typeof(orderBy) === "function"){
            callback = orderBy;
            orderBy = null;
            offset = 0;
            limit = null ;
        } else if(typeof(offset) === "function"){
            callback = offset;
            offset = 0;
            limit = null ;
        } else if(typeof(limit) === "function"){
            callback = limit;
            limit = null ;
        }

        this._checkSchema(function(err){
            if(err){ return callback(err); }
            this.client.ajax(this.dbEntryPoint+table, "GET", { 
                search : {
                    conditions: search,
                    joinFetch: joinFetch,
                    orderBy : orderBy,
                    offset: offset,
                    limit: limit
                }
            }, "json", callback) ;    
        }.bind(this)) ;
        
    };

    /**
     * Do simple search in table and return first found record
     * 
     * The search object can contains : 
     * simple equals condition as {foo: "bar"}
     * in condition as {foo: ["val1", "val2"]}
     * ilike condition as {foo: "bar%"} (activated by presence of %)
     * is null condition as {foo : null}
     * more complex conditions must specify operand explicitely :
     * {foo: {ope : ">", value : 1}}
     * {foo: {ope : "<", value : 10}}
     * {foo: {ope : "between", value : [from, to]}}
     * {foo: {ope : "not in", value : ["", ""]}}
     * 
     * @param {string} table table name
     * @param {object} search search object
     * @param {string} [orderBy] order by clause
     * @param {function(Error, Array)} callback called on finished. give back the first found records
     */
    VeloxDatabaseClient.prototype.searchFirst = function(table, search,joinFetch, orderBy, callback){
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
        if(typeof(orderBy) === "function"){
            callback = orderBy;
            orderBy = null;
        }
        this._checkSchema(function(err){
            if(err){ return callback(err); }
            this.client.ajax(this.dbEntryPoint+table, "GET", {
                searchFirst:  {
                    conditions: search,
                    orderBy : orderBy,
                    joinFetch: joinFetch
                }
            }, "json", callback) ;    
        }.bind(this)) ;
        
    };

    /**
     * Do many reads in one time
     * 
     * @example
     * //reads format 
     * {
     *      name1 : { pk : recordOk },
     *      name2 : {search: {...}, orderBy : "", offset: 0, limit: 10}
     *      name3 : {searchFirst: {...}, orderBy : ""}
     * }
     * 
     * //returns will be
     * {
     *      name1 : { record },
     *      name2 : [ records ],
     *      name3 : { record }
     * }
     * 
     * @param {object} reads object of search read to do
     * @param {function(Error, object)} callback called with results of searches
     */
    VeloxDatabaseClient.prototype.multiread = function(reads, callback){
        this._checkSchema(function(err){
            if(err){ return callback(err); }
            this.client.ajax(this.dbEntryPoint+"multiread", "POST", {
                reads: reads
            }, "json",callback) ;    
        }.bind(this)) ;
        
    };



    /**
     * contains extensions
     */
    VeloxDatabaseClient.extensions = [];

    /**
     * Register extensions
     * 
     * extension object should have : 
     *  name : the name of the extension
     *  extendsObj : object containing function to add to VeloxDatabaseClient instance
     *  extendsProto : object containing function to add to VeloxDatabaseClient prototype
     *  extendsGlobal : object containing function to add to VeloxDatabaseClient global object
     * 
     * @param {object} extension - The extension to register
     */
    VeloxDatabaseClient.registerExtension = function (extension) {
            VeloxDatabaseClient.extensions.push(extension);

            if (extension.extendsProto) {
                Object.keys(extension.extendsProto).forEach(function (key) {
                        VeloxDatabaseClient.prototype[key] = extension.extendsProto[key];
                });
            }
            if (extension.extendsGlobal) {
                Object.keys(extension.extendsGlobal).forEach(function (key) {
                        VeloxDatabaseClient[key] = extension.extendsGlobal[key];
                });
            }
    };


    return VeloxDatabaseClient;
})));