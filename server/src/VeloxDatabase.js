const VeloxDbPgBackend = require("./backends/pg/VeloxDbPgBackend");
const VeloxSqlUpdater = require("./VeloxSqlUpdater") ;
const VeloxLogger = require("velox-commons/VeloxLogger") ;
const AsyncJob = require("velox-commons/AsyncJob") ;
const events = require("events") ;
/**
 * VeloxDatabase helps you to manage your database
 */
class VeloxDatabase {

    /**
     * @typedef InterfaceLogger
     * @type {object}
     * @property {function(string)} debug log debug
     * @property {function(string)} info log info
     * @property {function(string)} warn log warn
     * @property {function(string)} error log error
     */

    /**
     * @typedef VeloxDatabaseOptions
     * @type {object}
     * @property {string} user database user
     * @property {string} host database host
     * @property {string} database database name
     * @property {string} password database password
     * @property {'pg'} backend database backend
     * @property {string} migrationFolder  migration scripts folder
     * @property {object} schema database schema information (will extends information from database schema)
     * @property {InterfaceLogger} [logger=console] logger (use console if not given)
     */

    /**
     * 
     * Create a VeloxDatabase
     * 
     * @param {VeloxDatabaseOptions} options options
     */
    constructor(options){
        this.options = options ;

        for( let k of ["user", "host", "port", "database", "password", "backend"]){
            if(options[k] === undefined) { throw "VeloxDatabase : missing option "+k ; } 
        }

        var logger = options.logger;

        this.logger = new VeloxLogger("VeloxDatabase", logger) ;

        if(!logger){
            this.logger.warn("No logger provided, using console.") ;
        }

        this.backend = new VeloxDbPgBackend({
            user: options.user,
            host: options.host,
            port: options.port,
            database: options.database,
            password: options.password,
            schema : options.schema,
            logger: logger,
            customClientInit: []
        });

        for(let extension of VeloxDatabase.extensions){
            if(extension.extendsBackends && extension.extendsBackends[options.backend]){                
                Object.keys(extension.extendsBackends[options.backend]).forEach((key)=> {
                    this.backend[key] = extension.extendsBackends[options.backend][key];
                });
            }
        }

        //register express middleware action from extensions
        this.expressExtensions = {} ;
        for(let extension of VeloxDatabase.extensions){
            if(extension.extendsExpress){
                Object.keys(extension.extendsExpress).forEach((key)=> {
                    this.expressExtensions[key] = extension.extendsExpress[key];
                });
            }
        }

        this.expressExtensionsProto = {} ;
        for(let extension of VeloxDatabase.extensions){
            if(extension.extendsExpressProto){
                Object.keys(extension.extendsExpressProto).forEach((key)=> {
                    this.expressExtensionsProto[key] = extension.extendsExpressProto[key];
                });
            }
        }
        this.expressExtensionsConfigure = [] ;
        for(let extension of VeloxDatabase.extensions){
            if(extension.extendsExpressConfigure){
                extension.extendsExpressConfigure.forEach((c)=>{
                    this.expressExtensionsConfigure.push(c) ;
                });
            }
        }

        for(let extension of VeloxDatabase.extensions){
            if(extension.init){
                extension.init(this) ;
            }
        }

        this._addClientCustomInit() ;
    }

    /**
     * Will apply needed change to the schema
     * 
     * @param {function(err)} callback - Called when update is done
     */
    updateSchema(callback){
        this.logger.info("Start update database schema") ;
        this.backend.createIfNotExist((err)=>{
            if(err){ return callback(err); }
            
            this.transaction((client, done)=>{
                if(err){ return done(err); }

                this._createDbVersionTable(client, (err)=>{
                    if(err){ return done(err); }

                    this._getAndApplyChanges(client, (err)=>{
                        if(err){ return done(err); }

                        done() ;
                    }) ;
                }) ;
            }, callback) ;
        }) ;
    }

    /**
     * add interceptor from extensions on the client
     * 
     * @param {VeloxDatabaseClient} client database client
     */
    _addClientCustomInit(){
        this.backend.customClientInit.push(function(client){
            client.multiread = function(reads, done){
                let job = new AsyncJob(AsyncJob.PARALLEL) ;
                let results = {} ;
                for(let k of Object.keys(reads)){
                    let r = reads[k] ;
                    job.push((cb)=>{
                        if(r.pk){
                            client.getByPk(r.table || k, r.pk, r.joinFetch, (err, record)=>{
                                if(err){ return cb(err); }
                                results[k] = record ;
                                cb() ;
                            }) ;
                        }else if(r.search){
                            client.search(r.table || k, r.search, r.joinFetch, r.orderBy, r.offset, r.limit, (err, records)=>{
                                if(err){ return cb(err); }
                                results[k] = records ;
                                cb() ;
                            }) ;
                        }else if(r.searchFirst){
                            client.searchFirst(r.table || k, r.searchFirst, r.joinFetch, r.orderBy, (err, record)=>{
                                if(err){ return cb(err); }
                                results[k] = record ;
                                cb() ;
                            }) ;
                        }else{
                            cb("Unknown operation for "+JSON.stringify(r)) ;
                        }
                    }) ;
                }
                job.async((err)=>{
                    if(err){ return done(err) ;}
                    done(null, results) ;
                }) ;
            } ;
        }) ;
        this.backend.customClientInit.push(function(client){
            client.multiread = function(reads, done){
                let job = new AsyncJob(AsyncJob.PARALLEL) ;
                let results = {} ;
                for(let k of Object.keys(reads)){
                    let r = reads[k] ;
                    job.push((cb)=>{
                        if(r.pk){
                            client.getByPk(r.table || k, r.pk, r.joinFetch, (err, record)=>{
                                if(err){ return cb(err); }
                                results[k] = record ;
                                cb() ;
                            }) ;
                        }else if(r.search){
                            client.search(r.table || k, r.search, r.joinFetch, r.orderBy, r.offset, r.limit, (err, records)=>{
                                if(err){ return cb(err); }
                                results[k] = records ;
                                cb() ;
                            }) ;
                        }else if(r.searchFirst){
                            client.searchFirst(r.table || k, r.searchFirst, r.joinFetch, r.orderBy, (err, record)=>{
                                if(err){ return cb(err); }
                                results[k] = record ;
                                cb() ;
                            }) ;
                        }else{
                            cb("Unknown operation for "+JSON.stringify(r)) ;
                        }
                    }) ;
                }
                job.async((err)=>{
                    if(err){ return done(err) ;}
                    done(null, results) ;
                }) ;
            } ;
        }) ;
        this.backend.customClientInit.push(function(client){
            if(!client.unsafe){
                client.unsafe = function(unsafeFun, callback){
                    if(!callback){ callback = function(){} ;}
                    let tx = this ;
                    unsafeFun(tx, function(err){
                        if(err){ callback(err) ;}
                        callback.apply(null, arguments) ;
                    }.bind(this)) ;
                } ;
            }

            client.changes = function(changeSet, done){
                let tx = this ;
                let results = [] ;
                let recordCache = {};
                let updatePlaceholder = (record)=>{
                    if(typeof(record) === "object"){
                        for(let k of Object.keys(record)){
                            if(record[k] && typeof(record[k]) === "string" && record[k].indexOf("${") === 0){
                                //this record contains ${table.field} that must be replaced by the real value of last inserted record of this table                        
                                let [othertable, otherfield] = record[k].replace("${", "").replace("}", "").split(".") ;
                                if(recordCache[othertable]){
                                    record[k] = recordCache[othertable][otherfield] ;
                                }
                            }
                        }
                    }
                } ;
                let job = new AsyncJob(AsyncJob.SERIES) ;
                
                for(let change of changeSet){
                    let record = change.record ;
                    
                    let table = change.table ;
                    let action = change.action ;
                    if(action === "insert"){
                        job.push((cb)=>{
                            updatePlaceholder(record) ;
                            tx.insert(table, record, (err, insertedRecord)=>{
                                if(err){ return cb(err); }
                                results.push({
                                    action: "insert",
                                    table : table,
                                    record: insertedRecord
                                }) ;
                                recordCache[table] = insertedRecord ;
                                cb() ;
                            }) ;
                        });
                    }
                    if(action === "update"){
                        job.push((cb)=>{
                            updatePlaceholder(record) ;
                            tx.update(table, record, (err, updatedRecord)=>{
                                if(err){ return cb(err); }
                                results.push({
                                    action: "update",
                                    table : table,
                                    record: updatedRecord
                                }) ;
                                recordCache[table] = updatedRecord ;
                                cb() ;
                            }) ;
                        });
                    }
                    if(action === "remove"){
                        job.push((cb)=>{
                            updatePlaceholder(record) ;
                            tx.remove(table, record, (err)=>{
                                if(err){ return cb(err); }
                                results.push({
                                    action: "remove",
                                    table : table
                                }) ;
                                cb() ;
                            }) ;
                        });
                    }
                    if(action === "removeWhere"){
                        job.push((cb)=>{
                            updatePlaceholder(record) ;
                            tx.removeWhere(table, record, (err)=>{
                                if(err){ return cb(err); }
                                results.push({
                                    action: "removeWhere",
                                    table : table
                                }) ;
                                cb() ;
                            }) ;
                        });
                    }
                    if(action === "updateWhere"){
                        job.push((cb)=>{
                            updatePlaceholder(record) ;
                            tx.updateWhere(table, record.values, record.conditions, (err)=>{
                                if(err){ return cb(err); }
                                results.push({
                                    action: "updateWhere",
                                    table : table,
                                }) ;
                                cb() ;
                            }) ;
                        });
                    }
                    if(!action || action === "auto"){
                        job.push((cb)=>{
                            updatePlaceholder(record) ;
                            tx.getPrimaryKey(table, (err, primaryKey)=>{
                                if(err) { return cb(err) ;}
                                let hasPkValue = true ;
                                if(Object.keys(record).length < primaryKey.length){
                                    hasPkValue = false;
                                }
                                for(let k of primaryKey){
                                    if(Object.keys(record).indexOf(k) === -1){
                                        hasPkValue = false ;
                                        break;
                                    }
                                }
                                if(hasPkValue){
                                    //has PK value
                                    tx.unsafe((txUnsafe, done)=>{
                                        txUnsafe.getByPk(table, record, done) ;
                                    }, (err, recordDb)=>{
                                        if(err) { return cb(err) ;}
                                        if(recordDb){
                                            //already exists, update
                                            tx.update(table, record, (err, updatedRecord)=>{
                                                if(err){ return cb(err); }
                                                results.push({
                                                    action: "update",
                                                    table : table,
                                                    record: updatedRecord
                                                }) ;
                                                recordCache[table] = updatedRecord ;
                                                cb() ;
                                            });
                                        }else{
                                            //not exists yet, insert
                                            tx.insert(table, record, (err, insertedRecord)=>{
                                                if(err){ return cb(err); }
                                                results.push({
                                                    action: "insert",
                                                    table : table,
                                                    record: insertedRecord
                                                }) ;
                                                recordCache[table] = insertedRecord ;
                                                cb() ;
                                            }) ;
                                        }
                                    }) ;
                                }else{
                                    //no pk in the record, insert
                                    tx.insert(table, record, (err, insertedRecord)=>{
                                        if(err){ return cb(err); }
                                        results.push({
                                            action: "insert",
                                            table : table,
                                            record: insertedRecord
                                        }) ;
                                        recordCache[table] = insertedRecord ;
                                        cb() ;
                                    });
                                }
                            }) ;
                        });
                    }
                }
                job.async((err)=>{
                    if(err){ return done(err) ;}
                    done(null, results) ;
                }) ;
            } ;
        }) ;
        this.backend.customClientInit.push(function(client){
            let interceptorsByActions = {} ;
            for(let extension of VeloxDatabase.extensions){
                if(extension.extendsClient){                
                    Object.keys(extension.extendsClient).forEach((key)=> {
                        client[key] = extension.extendsClient[key];
                    });
                }
                if(extension.interceptClientQueries){
                    for(let interception of extension.interceptClientQueries){
                        if(!interceptorsByActions[interception.name]){
                            interceptorsByActions[interception.name] = [] ;
                        }
                        interceptorsByActions[interception.name].push(interception) ;
                    }
                }
            }
            var callOneInterceptor = (interceptor, args, callback)=>{
                if(!interceptor){ return callback() ;}
                if(interceptor.length === args.length){
                    try{
                        interceptor.apply(client, args) ;
                    }catch(err){
                        return callback(err) ;
                    }
                    callback() ;
                }else{
                    interceptor.apply(client, args.concat([callback])) ;
                }
            } ;
            for(let actionName of Object.keys(interceptorsByActions)){
                let interceptors = interceptorsByActions[actionName] ;
                let originalFunction = client[actionName] ;
                client[actionName] = function(){
                    let args = Array.prototype.slice.call(arguments) ;
                    let tableName = args[0] ;
                    let realCallback = args.pop();
                    if((actionName === "query" || actionName === "queryFirst") && args.length === 1){
                        //called without params, add them
                        args.push([]) ;
                    }
                    let jobBefore = new AsyncJob(AsyncJob.SERIES) ;
                    for(let int of interceptors.filter(function(int){return !int.table || (int.table === tableName && int.before) ;})){
                        jobBefore.push((cb)=>{
                            var argsCall = [args[0], args[1]] ;
                            if((actionName === "search" || actionName === "searchFirst" || actionName === "getByPk")){
                                //read interceptor, give the joinFetch if any
                                if(Array.isArray(args[2])){
                                    argsCall.push(args[2]) ;
                                }else{
                                    argsCall.push(null) ;
                                }
                            }
                            callOneInterceptor(int.before, argsCall, cb) ;
                        });
                    }
                    jobBefore.async((err)=>{
                        if(err){ return realCallback(err) ; }
                        originalFunction.apply(this, args.concat([function(err){
                            if(err){
                                return realCallback(err) ;
                            }
                            args = Array.prototype.slice.call(arguments) ;
                            args = [tableName].concat(args.splice(1)) ;
                            let jobAfter = new AsyncJob(AsyncJob.SERIES) ;
                            for(let int of interceptors.filter(function(int){return !int.table  || (int.table === tableName && int.after) ;})){
                                jobAfter.push((cb)=>{
                                    callOneInterceptor(int.after, args, cb) ;
                                });
                            }
                            jobAfter.async((err)=>{
                                if(err){ return realCallback(err); }
                                realCallback.apply(null, [null, args[1]]) ;
                            }) ;
                        }.bind(this)])) ;
                    }) ;
                }.bind(client) ;
            }
        });
    }

    /**
     * Do actions in database
     * 
     * Note : you should use this when you have only read action to do. If you need insert/update, use the transaction
     * 
     * @example
     * db.inDatabase((client, done){
     *    //run a first query    
     *    client.query(sql, [...], (err, result1) => {
     *        if(err){ return done(err); }
     * 
     *        //return a second query
     *        client.query(sql, [...], (err, result2) => {
     *           if(err){ return done(err); }
     * 
     *           //finished !
     *           done(null, result1, result2) ;
     *        }) ;
     *    }) ;
     * }, (err, result1, result2) {
     *      if(err) { return console.log("error in database "+err); }
     *      
     *      //done
     *      console.log("my results : "+result1+", "+result2) ;
     * }) ;
     * 
     * @param {function(VeloxDatabaseClient, function)} callbackDoInDb function that do the needed job in database
     * @param {function(Error)} callbackDone function called when database actions are done
     */
    inDatabase(callbackDoInDb, callbackDone){
        this.backend.open((err, client)=>{
            if(err){ return callbackDone(err); }
            try {
                callbackDoInDb(client, function(err){
                    client.close() ;
                    if(err){ return callbackDone(err); }
                    callbackDone.apply(null, arguments) ;
                }) ;
            } catch (error) {
                client.close() ;
                return callbackDone(error);
            }
        }) ;
    }

    getByPk(table, pk, joinFetch, callback){
        if(typeof(joinFetch) === "function"){
            callback = joinFetch;
            joinFetch = null;
        }
        this.inDatabase((client, done)=>{
            client.getByPk(table, pk, joinFetch, done) ;
        }, callback) ;
    }

    search(table, search, joinFetch,orderBy, offset, limit, callback){
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
        this.inDatabase((client, done)=>{
            client.search(table, search, joinFetch,orderBy, offset, limit, done) ;
        }, callback) ;
    }

    /**
     * Get the schema of the database. Result format is : 
     * {
     *      table1 : {
     *          columns : [
     *              {name : "", type: "", size: 123}
     *          ],
     *          pk: ["field1", field2]
     *      },
     *      table2 : {...s}
     * }
     * 
     * Note : result is cached so in the case you modify the table while application is running you should restart to see the modifications
     * 
     * @param {function(Error,object)} callback 
     */
    getSchema(callback){
        this.backend.open((err, client)=>{
            if(err){ return callback(err); }
            try {
                client.getSchema((err, schema)=>{
                    client.close() ;
                    if(err){ return callback(err); }
                    callback(null, schema) ;
                }) ;
            } catch (error) {
                client.close() ;
                return callback(error);
            }
        }) ;
    }

    /**
     * Alias of inDatabase
     * 
     * @see #transaction
     */
    inDb(callbackDoInDb, callbackDone){ 
        this.inDatabase(callbackDoInDb, callbackDone) ;
    }

    /**
     * Do some actions in a database inside an unique transaction
     * 
     * @example
     *          db.transaction("Insert profile and user",
     *          function txActions(tx, done){
     *              tx.query("...", [], (err, result) => {
     *                   if(err){ return done(err); } //error handling
     *
     *                   //profile inserted, insert user
     *                   tx.query("...", [], (err) => {
     *                      if(err){ return done(err); } //error handling
     *                      //finish succesfully
     *                      done(null, "a result");
     *                  });
     *              });
     *          },
     *          function txDone(err, result){
     *              if(err){
     *              	return logger.error("Error !!", err) ;
     *              }
     *              logger.info("Success !!")
     *          });
     *
     * @param {function({VeloxDbPgClient}, {function(err, result)})} callbackDoTransaction - function that do the content of the transaction receive tx should call done() on finish
     * @param {function(err)} [callbackDone] - called when the transaction is finished
     * @param {number} [timeout] - if this timeout (seconds) is expired, the transaction is automatically rollbacked.
     *          If not set, default value is 30s. If set to 0, there is no timeout (not recomended)
     *
     */
    transaction(callbackDoTransaction, callbackDone, timeout){ 
        if(!callbackDone){ callbackDone = function(){} ;}
        var eventTx = new events.EventEmitter();

        this.backend.open((err, client)=>{
            if(err){ return callbackDone(err) ;}
            client.on = eventTx.on.bind(eventTx) ;
            client.transaction((tx, done)=>{
                callbackDoTransaction(client, done) ;
            }, function(err){ //explicit use of function instead of arrow to have argument variable
                eventTx.emit("close", {db: this}) ;
                client.close() ;
                if(err){ 
                    return callbackDone(err) ;
                }
                callbackDone.apply(null, arguments) ;
            }, timeout) ;
        }) ;
    }

    /**
     * Alias of transaction
     * 
     * @see #transaction
     */
    tx(callbackDoTransaction, callbackDone, timeout){ 
        this.transaction(callbackDoTransaction, callbackDone, timeout) ;
    }

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
    multiread(reads, callback){
        this.inDatabase((client, done)=>{
            client.multiread(reads, done) ;
        }, callback) ;
    }



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
     * @param {function(Error)} callback called on finish
     */
    transactionalChanges(changeSet, callback){
        
        this.transaction((tx, done)=>{
            tx.changes(changeSet, done) ;
        }, (err, results)=>{
            if(err) { return callback(err) ;}
            callback(null, results) ;
        }) ;
    }

    /**
     * Create the database version table
     * 
     * @private
     * @param {VeloxDbClient} client - database client connection
     * @param {function(err)} callback - called when finished 
     */
    _createDbVersionTable(client, callback){
        client.dbVersionTableExists((err, exists)=>{
            if(err){ return callback(err); }
            if(exists){
                return callback() ;
            }
            this.logger.info("Create version table") ;
            return client.createDbVersionTable(callback) ;
        }) ;
    }

    /**
     * Get the schema update to do, run them and update database version
     * 
     * @private
     * @param {VeloxDbClient} client - database client connection
     * @param {function(err)} callback - called when finished 
     */
    _getAndApplyChanges(client, callback){
        let updater = new VeloxSqlUpdater() ;
        updater.loadChanges(this.options.migrationFolder, (err)=>{
            if(err){ return callback(err); }

            client.getCurrentVersion((err, version)=>{
                if(err){ return callback(err); }

                let changes = [];

                let lastVersion = updater.getLastVersion() ;
                
                for(let extension of VeloxDatabase.extensions){
                    if(extension.prependSchemaChanges){
                        let extensionChanges = extension.prependSchemaChanges(this.options.backend, version, lastVersion) ;
                        for(let c of extensionChanges){
                            changes.push(c) ;
                        }
                    }
                }
                
                changes = changes.concat(updater.getChanges(version)) ;
                
                

                for(let extension of VeloxDatabase.extensions){
                    if(extension.addSchemaChanges){
                        let extensionChanges = extension.addSchemaChanges(this.options.backend, version, lastVersion) ;
                        for(let c of extensionChanges){
                            changes.push(c) ;
                        }
                    }
                }

                if(changes.length>0){
                    this.logger.info("Update from "+version+" to "+lastVersion+" - "+changes.length+" changes to apply") ;

                    client.runQueriesAndUpdateVersion(changes, lastVersion, callback) ;
                }else{
                    this.logger.info("No update to do") ;
                    callback() ;
                }
            }) ;
        }) ;
    }
}


/**
 * contains extensions
 */
VeloxDatabase.extensions = [];

/**
 * @typedef VeloxDatabaseExtension
 * @type {object}
 * @property {string} name name of the extension
 * @property {VeloxDatabaseExtension[]} [dependencies] dependencies on other extensions
 * @property {function} [addSchemaChanges] add schema change on schema update
 * @property {object} [extendsProto] object containing function to add to VeloxWebView prototype
 * @property {object} [extendsGlobal] object containing function to add to VeloxWebView global object
 * @property {object} [extendsBackends]  object that extend backend clients
 */

/**
 * Register extensions
 * 
 * @param {VeloxDatabaseExtension} extension - The extension to register
 */
VeloxDatabase.registerExtension = function (extension) {
    if(!extension.name) {
        throw "Extension should have a name";
    }

    if(VeloxDatabase.extensions.some((ext)=>{
        return ext.name === extension.name ;
    })){
        console.log("Extension "+extension.name+" is already registered, ignore") ;
        return;
    }

    if(extension.dependencies){
        for(let d of extension.dependencies){
            VeloxDatabase.registerExtension(d) ;
        }
    }

    VeloxDatabase.extensions.push(extension);

    if (extension.extendsProto) {
        Object.keys(extension.extendsProto).forEach(function (key) {
                VeloxDatabase.prototype[key] = extension.extendsProto[key];
        });
    }
    if (extension.extendsGlobal) {
        Object.keys(extension.extendsGlobal).forEach(function (key) {
                VeloxDatabase[key] = extension.extendsGlobal[key];
        });
    }
};


/**
 * This class wrap a database connection with some helping function.
 * 
 * It should be implemented for each backend
 */
class VeloxDatabaseClient {

    /**
     * Check if the db version table exists
     * 
     * Note : this function is for internal schema update usage. It may be changed or
     * be remove anytime, don't rely on it
     * 
     * @param {function(err, exists)} callback - Called when check is done
     */
    dbVersionTableExists(callback) { callback("not implemented"); }

    /**
     * Create the db version table and initialize it with version 0
     * 
     * Note : this function is for internal schema update usage. It may be changed or
     * be remove anytime, don't rely on it
     * 
     * @param {function(err)} callback - called when finished
     */
    createDbVersionTable(callback) { callback("not implemented"); }

    /**
     * Get database version number
     * 
     * @param {function(err, version)} callback - called when finished with the version number
     */
    getCurrentVersion(callback) { callback("not implemented"); }

    /**
     * Execute a query and give the result back
     * 
     * @param {string} sql - SQL to execute
     * @param {Array} [params] - Params
     * @param {function(err, results)} callback - called when finished
     */
    query(sql, params, callback){ callback("not implemented"); }

    /**
     * Execute a query and give the first result back
     * 
     * Note : the query is not modified, you should add the LIMIT clause yourself !
     * 
     * @param {string} sql - SQL to execute
     * @param {Array} [params] - Params
     * @param {function(err, results)} callback - called when finished
     */
    queryFirst(sql, params, callback){ callback("not implemented"); }

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
     * @param {any|object} pk the pk value. can be an object containing each value for composed keys
     * @param {function(Error,object)} callback called with result. give null if not found
     */
    getByPk(table, pk, joinFetch, callback){ callback("not implemented"); }


    /**
     * Insert a record in the table. Give back the inserted record (with potential generated values)
     * 
     * @param {string} table the table name
     * @param {object} record the object to insert
     * @param {function(Error, object)} callback called when insert is done. give back the inserted result (with potential generated values)
     */
    insert(table, record, callback){ callback("not implemented"); }

    /**
     * Update a record in the table. Give back the updated record (with potential generated values)
     * 
     * @param {string} table the table name
     * @param {object} record the object to insert
     * @param {function(Error, object)} callback called when insert is done. give back the updated result (with potential generated values)
     */
    update(table, record, callback){ callback("not implemented"); }

    /**
     * Helpers to do simple search in table
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
     * @param {VeloxDatabaseJoinFetch} [joinFetch] join fetch from other sub tables
     * @param {string} [orderBy] order by clause
     * @param {number} [offset] offset, default is 0
     * @param {number} [limit] limit, default is no limit
     * @param {function(Error, Array)} callback called on finished. give back the found records
     */
    search(table, search, joinFetch, orderBy, offset, limit, callback){ callback("not implemented"); }

    /**
     * Helpers to do simple search in table and return first found record
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
     * @param {VeloxDatabaseJoinFetch} [joinFetch] join fetch from other sub tables
     * @param {string} [orderBy] order by clause
     * @param {function(Error, Array)} callback called on finished. give back the first found records
     */
    searchFirst(table, search, joinFetch, orderBy, callback){ callback("not implemented"); }

    /**
     * Get the columns of a table. Give back an array of columns definition
     * 
     * Note : result is cached so in the case you modify the table while application is running you should restart to see the modifications
     * 
     * @param {string} table the table name
     * @param {function(Error, Array)} callback called when found primary key, return array of column definitions
     */
    getColumnsDefinition(table, callback){ callback("not implemented"); }

    /**
     * Get the primary key of a table. Give back an array of column composing the primary key
     * 
     * Note : result is cached so in the case you modify the table while application is running you should restart to see the modifications
     * 
     * @param {string} table the table name
     * @param {function(Error, Array)} callback called when found primary key, return array of column names composing primary key
     */
    getPrimaryKey(table, callback){ callback("not implemented"); }

    /**
     * Execute the schema changes and update the version number
     * 
     * Note : this function is for internal schema update usage. It may be changed or
     * be remove anytime, don't rely on it
     * 
     * @param {VeloxSqlChange[]} changes - Array of changes
     * @param {number} newVersion - The new database version
     * @param {function(err)} callback - called when finish
     */
    runQueriesAndUpdateVersion(changes, newVersion, callback){ callback("not implemented"); }


     /**
     * Do some actions in a database inside an unique transaction
     * 
     * @example
     *          db.transaction("Insert profile and user",
     *          function txActions(tx, done){
     *              tx.query("...", [], (err, result) => {
     *                   if(err){ return done(err); } //error handling
     *
     *                   //profile inserted, insert user
     *                   tx.query("...", [], (err) => {
     *                      if(err){ return done(err); } //error handling
     *                      //finish succesfully
     *                      done(null, "a result");
     *                  });
     *              });
     *          },
     *          function txDone(err, result){
     *              if(err){
     *              	return logger.error("Error !!", err) ;
     *              }
     *              logger.info("Success !!")
     *          });
     *
     * @param {function({VeloxDbPgClient}, {function(err, result)})} callbackDoTransaction - function that do the content of the transaction receive tx should call done() on finish
     * @param {function(err)} [callbackDone] - called when the transaction is finished
     * @param {number} [timeout] - if this timeout (seconds) is expired, the transaction is automatically rollbacked.
     *          If not set, default value is 30s. If set to 0, there is no timeout (not recomended)
     *
     */
    transaction(callbackDoTransaction, callbackDone, timeout){ callbackDone("not implemented"); }


    /**
     * Delete a record in the table by its pk
     * 
     * @example
     * //delete by simple pk
     * client.remove("foo", "id", (err)=>{...})
     * 
     * //delete with composed pk
     * client.remove("bar", {k1: "valKey1", k2: "valKey2"}, (err)=>{...})
     * 
     * //already have the record containing pk value, just give it...
     * client.remove("bar", barRecordAlreadyHaving, (err)=>{...})
     * 
     * @param {string} table the table name
     * @param {any|object} pk the pk value. can be an object containing each value for composed keys
     * @param {function(Error)} callback called when done
     */
    remove(table, pk, callback){ callback("not implemented"); }

    /**
     * Close the database connection
     */
    close() { throw "not implemented" ; }
}


module.exports = VeloxDatabase ;