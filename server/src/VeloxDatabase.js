const VeloxDbPgBackend = require("./backends/pg/VeloxDbPgBackend");
const VeloxSqlUpdater = require("./VeloxSqlUpdater") ;
const VeloxLogger = require("../../../helpers/VeloxLogger") ;
const AsyncJob = require("../../../helpers/AsyncJob") ;
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

        for( let k of ["user", "host", "port", "database", "password", "backend", "migrationFolder"]){
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
            logger: logger
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
            
            this.backend.open((err, client)=>{
                if(err){ return callback(err); }

                this._createDbVersionTable(client, (err)=>{
                    if(err){ return callback(err); }

                    this._getAndApplyChanges(client, (err)=>{
                        if(err){ return callback(err); }

                        callback() ;
                    }) ;
                }) ;
            }) ;
        }) ;
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
        this.backend.open((err, client)=>{
            if(err){ return callbackDone(err) ;}
            client.transaction(callbackDoTransaction, (err)=>{
                client.close() ;
                if(err){ 
                    return callbackDone(err) ;
                }
                callbackDone() ;
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
            let job = new AsyncJob(AsyncJob.PARALLEL) ;
            let results = {} ;
            for(let k of Object.keys(reads)){
                let r = reads[k] ;
                job.push((cb)=>{
                    if(r.pk){
                        client.getByPk(r.table, r.pk, (err, record)=>{
                            if(err){ return cb(err); }
                            results[k] = record ;
                            cb() ;
                        }) ;
                    }else if(r.search){
                        client.search(r.table, r.search, r.orderBy. r.offset, r.limit, (err, records)=>{
                            if(err){ return cb(err); }
                            results[k] = records ;
                            cb() ;
                        }) ;
                    }else if(r.searchFirst){
                        client.searchFirst(r.table, r.searchFirst, r.orderBy, (err, record)=>{
                            if(err){ return cb(err); }
                            results[k] = record ;
                            cb() ;
                        }) ;
                    }
                }) ;
            }
            job.async((err)=>{
                if(err){ return done(err) ;}
                done(null, results) ;
            }) ;
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
        let results = [] ;
        let recordCache = {};
        let updatePlaceholder = (record)=>{
            for(let k of Object.keys(record)){
                if(record[k] && typeof(record[k]) === "string" && record[k].indexOf("${") === 0){
                    //this record contains ${table.field} that must be replaced by the real value of last inserted record of this table                        
                    let [othertable, otherfield] = record[k].replace("${", "").replace("}", "").split(".") ;
                    if(recordCache[othertable]){
                        record[k] = recordCache[othertable][otherfield] ;
                    }
                }
            }
        } ;
        this.transaction((tx, done)=>{
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
                                tx.getByPk(table, record, (err, recordDb)=>{
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
            job.async(done) ;
        }, (err)=>{
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

                let changes = updater.getChanges(version) ;

                if(changes.length>0){
                    let lastVersion = updater.getLastVersion() ;
                    this.logger.info("Update from "+version+" to "+lastVersion+" - "+changes.length+" changes to apply") ;

                    for(let extension of VeloxDatabase.extensions){
                        if(extension.addSchemaChanges){
                            let extensionChanges = extension.addSchemaChanges(this.options.backend, version, lastVersion) ;
                            for(let c of extensionChanges){
                                changes.push(c) ;
                            }
                        }
                    }

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
    getByPk(table, pk, callback){ callback("not implemented"); }


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
     * @param {string} [orderBy] order by clause
     * @param {number} [offset] offset, default is 0
     * @param {number} [limit] limit, default is no limit
     * @param {function(Error, Array)} callback called on finished. give back the found records
     */
    search(table, search, orderBy, offset, limit, callback){ callback("not implemented"); }

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
     * @param {string} [orderBy] order by clause
     * @param {function(Error, Array)} callback called on finished. give back the first found records
     */
    searchFirst(table, search, orderBy, callback){ callback("not implemented"); }

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