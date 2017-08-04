; (function (global, factory) {
        typeof exports === 'object' && typeof module !== 'undefined' ? module.exports = factory() :
        typeof define === 'function' && define.amd ? define(factory) :
        global.VeloxDatabaseClient = factory() ;
}(this, (function () { 'use strict';


    /**
     * @typedef VeloxDatabaseClientOptions
     * @type {object}
     * @property {string} serverUrl Server end point URL
     * @property {function} xhrPrepare function that receive the XHR object to customize it if needed
     */

    /**
     * The Velox database client
     * 
     * @constructor
     * 
     * @param {VeloxDatabaseClientOptions} options database client options
     */
    function VeloxDatabaseClient(options) {
        if(!options || typeof(options) !== "object"){
            throw "VeloxDatabaseClient missing options" ;
        }
        this.options = JSON.parse(JSON.stringify(options))  ;
        if(!this.options.serverUrl){
            throw "VeloxDatabaseClient missing option serverUrl" ;
        }

        if(this.options.serverUrl[this.options.serverUrl.length-1] !== "/"){
            //add trailing slash
            this.options.serverUrl+"/" ;
        }

        var self = this ;
        VeloxDatabaseClient.extensions.forEach(function(extension){
            if(extension.extendsObj){
                Object.keys(extension.extendsObj).forEach(function (key) {
                        self[key] = extension.extendsObj[key];
                });
            }
        })
    }

    /**
     * Perform ajax call
     * 
     * @private
     * @param {string} url the url to call
     * @param {string} method the HTTP method
     * @param {object} data the parameters to send
     * @param {function(Error, *)} callback called with error or result
     */
    VeloxDatabaseClient.prototype._ajax = function (url, method, data, callback) {
        var xhr = new XMLHttpRequest();
        if(method === "GET" && data){
            var querystring = [] ;
            Object.keys(data).forEach(function(k){
                querystring.push(k+"="+encodeURIComponent(JSON.stringify(data[k]))) ;
            }) ;
            url = url+"?"+querystring.join("&") ;
        }
        
        xhr.open(method, url);
        xhr.setRequestHeader("Content-type", "application/json");

        xhr.onreadystatechange = (function () {
            
            if (xhr.readyState === 4){
                var responseResult = xhr.responseText ;
                if(responseResult){
                    try{
                        responseResult = JSON.parse(responseResult) ;
                    }catch(e){}
                }
                if(xhr.status >= 200 && xhr.status < 300) {
                    callback(null, responseResult);
                } else {
                    callback(responseResult||xhr.status);
                }
            } 
        }).bind(this);
        if(this.options.xhrPrepare){
            this.options.xhrPrepare(xhr) ;
        }
        if(method === "POST" || method === "PUT"){
            xhr.setRequestHeader("Content-type", "application/json");
            xhr.send(JSON.stringify(data));
        } else {
            xhr.send();
        }
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
        this._ajax(this.options.serverUrl+"schema", "GET", null, function(err, schema){
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

    /**
     * Get the schema of the database
     * 
     * @param {function(Error, object)} callback called with the schema of database
     */
    VeloxDatabaseClient.prototype.getSchema = function(callback){
        this._checkSchema(function(err){
            if(err){ return callback(err); }
            callback(null, this.schema) ;
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
            this._ajax(this.options.serverUrl+table, "POST", record, callback) ;
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
            this._ajax(this.options.serverUrl+table+"/"+this._createPk(table,record), 
                "PUT", record, callback) ;    
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
            this._ajax(this.options.serverUrl+table+"/"+this._createPk(table, pkOrRecord), 
                "DELETE", null, callback) ;    
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
            this._ajax(this.options.serverUrl+"transactionalChanges", "POST", changeSet, callback) ;    
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
    VeloxDatabaseClient.prototype.getByPk = function(table, pkOrRecord, callback){
        this._checkSchema(function(err){
            if(err){ return callback(err); }
            this._ajax(this.options.serverUrl+table+"/"+this._createPk(table, pkOrRecord),
                 "GET", null, callback) ;    
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
    VeloxDatabaseClient.prototype.search = function(table, search, orderBy, offset, limit, callback){
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
            this._ajax(this.options.serverUrl+table, "GET", { 
                search : {
                    conditions: search,
                    orderBy : orderBy,
                    offset: offset,
                    limit: limit
                }
            }, callback) ;    
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
    VeloxDatabaseClient.prototype.searchFirst = function(table, search, orderBy, callback){
        if(typeof(orderBy) === "function"){
            callback = orderBy;
            orderBy = null;
        }
        this._checkSchema(function(err){
            if(err){ return callback(err); }
            this._ajax(this.options.serverUrl+table, "GET", {
                searchFirst:  {
                    conditions: search,
                    orderBy : orderBy
                }
            }, callback) ;    
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
            this._ajax(this.options.serverUrl+"multiread", "POST", {
                reads
            }, callback) ;    
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
              L          VeloxDatabaseClient.prototype[key] = extension.extendsProto[key];
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