const { Pool, Client } = require('pg');
const AsyncJob = require("../../../../../helpers/AsyncJob") ;
const VeloxLogger = require("../../../../../helpers/VeloxLogger") ;

const DB_VERSION_TABLE = "velox_db_version" ;

class VeloxDbPgClient {

    /**
     * Create the client connection
     * 
     * @param {object} connection The connection client from the pool
     * @param {function} closeCb the callback to give back the client to the pool
     * @param {VeloxLogger} logger logger
     */
    constructor(connection, closeCb, logger){
        this.connection = connection;
        this.closeCb = closeCb ;
        this.logger = logger ;

        this._cachePk = {} ;
        this._cacheColumns = {} ;
    }

    /**
     * Check if the db version table exists
     * @param {function(err, exists)} callback - Called when check is done
     */
    dbVersionTableExists(callback) {
          this.connection.query(`SELECT EXISTS (
                    SELECT 1 
                    FROM   pg_tables
                    WHERE  schemaname = 'public'
                    AND    tablename = $1
                    ) as exist`, [DB_VERSION_TABLE], (err, res) => {
                if(err){ return callback(err); }
                callback(null, res.rows[0].exist === true) ;
          });
    }

    /**
     * Create the db version table and initialize it with version 0
     * 
     * @param {function(err)} callback - called when finished
     */
    createDbVersionTable(callback) {
          this.connection.query(`CREATE TABLE ${DB_VERSION_TABLE} (
                    version bigint,
                    last_update timestamp without time zone
                    ) `, [], (err) => {
                        if(err){ return callback(err); }
                        this.connection.query(`INSERT INTO ${DB_VERSION_TABLE} (version, last_update) 
                            VALUES ($1, now())`, [0], callback) ;
                    });
    }

    /**
     * Get database version number
     * 
     * @param {function(err, version)} callback - called when finished with the version number
     */
    getCurrentVersion(callback) {
        this.connection.query(`SELECT version FROM ${DB_VERSION_TABLE} LIMIT 1 ;`, [], (err, results) => {
            if(err){ return callback(err); }

            if(results.rows.length === 0){
                //nothing in the table, should not happen, assume 0
                this.connection.query(`INSERT INTO ${DB_VERSION_TABLE} (version, last_update) 
                            VALUES ($1, now())`, [0], (err)=>{
                    if(err){ return callback(err); }
                    callback(null, 0) ;
                }) ;
            } else {
                callback(null, results.rows[0].version) ;
            }
        });
    }

    /**
     * Execute a query and give the result back
     * 
     * @param {string} sql - SQL to execute
     * @param {Array} [params] - Params
     * @param {function(err, results)} callback - called when finished
     */
    query(sql, params, callback){
        
        if(!callback && typeof(params) === "function"){
            callback = params;
            params = [];
        }
        this.logger.debug("Run SQL "+sql+", params "+JSON.stringify(params)) ;
        this.connection.query(sql, params, callback) ;
    }

    /**
     * Execute a query and give the first result back
     * 
     * Note : the query is not modified, you should add the LIMIT clause yourself !
     * 
     * @param {string} sql - SQL to execute
     * @param {Array} [params] - Params
     * @param {function(err, results)} callback - called when finished
     */
    queryFirst(sql, params, callback){
        if(!callback && typeof(params) === "function"){
            callback = params;
            params = [];
        }
        this.query(sql, params, (err, results)=>{
            if(err){ return callback(err); }
            if(results.rows.length === 0){
                return callback(null, null) ;
            }
            return callback(null, results.rows[0]) ;
        }) ;
    }

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
    getByPk(table, pk, callback){
        this.getPrimaryKey(table, (err, pkColumns)=>{
            if(err){ return callback(err); }

            if(pkColumns.length === 0){
                return callback("Error searching in table "+table+", no primary column for this table") ;
            }

            //check given pk is consistent with table pk
            if(typeof(pk) === "object"){
                //the given pk has the form {col1: "", col2: ""}
                if(Object.keys(pk).length < pkColumns.length){
                    return callback("Error searching in table "+table+", the given PK has "+Object.keys(pk).length+" properties but PK has "+pkColumns.length) ;
                }
                for(let k of pkColumns){
                    if(Object.keys(pk).indexOf(k) === -1){
                        return callback("Error searching in table "+table+", the given PK miss "+k+" property") ;
                    }
                }
            }else{
                //the given pk is a simple value, assuming simple PK form
                if(pkColumns.length > 1){
                    return callback("Error searching in table "+table+", the primary key should be composed of "+pkColumns.join(", "));
                }
                let formatedPk = {} ;
                formatedPk[pkColumns[0]] = pk ;
                pk = formatedPk ;
            }

            let where = [] ;
            let params = [] ;
            for(let k of pkColumns){
                params.push(pk[k]) ;
                where.push(k+" = $"+params.length) ;
            }

            let sql = `SELECT * FROM ${table} WHERE ${where.join(" AND ")}` ;

            this.queryFirst(sql, params, callback) ;
        }) ;
    }

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
    remove(table, pk, callback){
        this.getPrimaryKey(table, (err, pkColumns)=>{
            if(err){ return callback(err); }

            if(pkColumns.length === 0){
                return callback("Error deleting in table "+table+", no primary column for this table") ;
            }

            //check given pk is consistent with table pk
            if(typeof(pk) === "object"){
                //the given pk has the form {col1: "", col2: ""}
                if(Object.keys(pk).length < pkColumns.length){
                    return callback("Error deleting in table "+table+", the given PK has "+Object.keys(pk).length+" properties but PK has "+pkColumns.length) ;
                }
                for(let k of pkColumns){
                    if(Object.keys(pk).indexOf(k) === -1){
                        return callback("Error deleting in table "+table+", the given PK miss "+k+" property") ;
                    }
                }
            }else{
                //the given pk is a simple value, assuming simple PK form
                if(pkColumns.length > 1){
                    return callback("Error deleting in table "+table+", the primary key should be composed of "+pkColumns.join(", "));
                }
                let formatedPk = {} ;
                formatedPk[pkColumns[0]] = pk ;
                pk = formatedPk ;
            }

            let where = [] ;
            let params = [] ;
            for(let k of pkColumns){
                params.push(pk[k]) ;
                where.push(k+" = $"+params.length) ;
            }

            let sql = `DELETE FROM ${table} WHERE ${where.join(" AND ")}` ;

            this.query(sql, params, callback) ;
        }) ;
    }


    /**
     * Insert a record in the table. Give back the inserted record (with potential generated values)
     * 
     * @param {string} table the table name
     * @param {object} record the object to insert
     * @param {function(Error, object)} callback called when insert is done. give back the inserted result (with potential generated values)
     */
    insert(table, record, callback){
        if(!record) { return callback("Try to insert null record in table "+table) ; }
        this.getColumnsDefinition(table, (err, columns)=>{
            if(err){ return callback(err); }

            let cols = [];
            let values = [];
            let params = [] ;
            for(let c of columns){
                if(record[c.column_name] !== undefined){
                    cols.push(c.column_name) ;
                    params.push(record[c.column_name]) ;
                    values.push("$"+params.length) ;
                }
            }

            if(cols.length === 0){
                return callback("Can't found any column to insert in "+table+" from record "+JSON.stringify(record)) ;
            }

            let sql = `INSERT INTO ${table}(${cols.join(",")}) VALUES (${values.join(",")}) RETURNING *` ;

            this.queryFirst(sql, params, callback) ;
        }) ;
    }

    /**
     * Update a record in the table. Give back the updated record (with potential generated values)
     * 
     * @param {string} table the table name
     * @param {object} record the object to insert
     * @param {function(Error, object)} callback called when insert is done. give back the updated result (with potential generated values)
     */
    update(table, record, callback){
        if(!record) { return callback("Try to update null record in table "+table) ; }
        this.getColumnsDefinition(table, (err, columns)=>{
            if(err){ return callback(err); }
            this.getPrimaryKey(table, (err, pkColumns)=>{
                if(err){ return callback(err); }

                //check PK
                if(Object.keys(record).length < pkColumns.length){
                    return callback("Error updating in table "+table+", the given record miss primary keys, expected : "+pkColumns.join(",")) ;
                }
                for(let k of pkColumns){
                    if(Object.keys(record).indexOf(k) === -1){
                        return callback("Error updating in table "+table+", the given record miss primary key "+k+" property") ;
                    }
                }

                let sets = [];
                let params = [] ;
                for(let c of columns){
                    if(record[c.column_name] !== undefined && pkColumns.indexOf(c.column_name) === -1){
                        params.push(record[c.column_name]) ;
                        sets.push(c.column_name+" = $"+params.length) ;
                    }
                }
                let where = [] ;
                for(let k of pkColumns){
                    params.push(record[k]) ;
                    where.push(k+" = $"+params.length) ;
                }

                if(sets.length === 0){
                    return callback("Can't found any column to update in "+table+" from record "+JSON.stringify(record)) ;
                }

                let sql = `UPDATE ${table} SET ${sets.join(",")} WHERE ${where.join(" AND ")} RETURNING *` ;

                this.queryFirst(sql, params, callback) ;
            }) ;
        }) ;
    }

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
    search(table, search, orderBy, offset, limit, callback){
        this._prepareSearchQuery(table, search, orderBy, offset, limit, (err, sql, params)=>{
            if(err){ return callback(err); }
            this.query(sql, params, (err, result)=>{
                if(err){ return callback(err); }
                callback(null, result.rows) ;
            }) ;
        }) ;
    }

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
    searchFirst(table, search, orderBy, callback){
        if(typeof(orderBy) === "function"){
            callback = orderBy;
            orderBy = null;
        }
        this.search(table, search, orderBy, 0, 1, (err, results)=>{
            if(err){ return callback(err); }
            if(results.length === 0){
                callback(null, null) ;
            }else{
                callback(null, results[0]) ;
            }
        }) ;
    }


    /**
     * Prepare the search SQL
     * 
     * @param {string} table table name
     * @param {object} search search object
     * @param {string} [orderBy] order by clause
     * @param {number} [offset] offset
     * @param {number} [limit] limit
     * @param {function(Error, Array)} callback called on finished. give back the created sql and params
     */
    _prepareSearchQuery(table, search, orderBy, offset, limit, callback){
        if(!search) { return callback("Try to search with null search in table "+table) ; }

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

        this.getColumnsDefinition(table, (err, columns)=>{
            if(err){ return callback(err); }

            let where = [];
            let params = [] ;
            for(let c of columns){
                if(search[c.column_name] !== undefined){
                    let value = search[c.column_name] ;
                    let ope = "=" ;
                    if(typeof(value) === "object" && !Array.isArray(value)){
                        ope = value.ope ;
                        value = value.value ;
                    }else{
                        if(Array.isArray(value)){
                            ope = "IN" ;
                        }else if(value.indexOf("%") !== -1){
                            ope = "ILIKE" ;
                        }                        
                    }

                    if(ope.toUpperCase() === "IN" || ope.toUpperCase() === "NOT IN"){
                        if(!Array.isArray(value) || value.length === 0){
                            return callback("Search in table "+table+" failed. Search operand IN provided with no value. Expected an array with at least one value") ;
                        }
                        let wVals = [] ;
                        for(let v of value){
                            params.push(v) ;
                            wVals.push("$"+params.length) ;
                        }
                        where.push(c.column_name+" "+ope+" ("+wVals.join(",")+")") ;
                    } else if (ope.toUpperCase() === "BETWEEN"){
                        if(!Array.isArray(value) || value.length !== 2){
                            return callback("Search in table "+table+" failed. Search operand BETWEEN provided with wrong value. Expected an array with 2 values") ;
                        }
                        params.push(value[0]) ;
                        params.push(value[1]) ;
                        where.push(c.column_name+" BETWEEN $"+(params.length-1)+" AND $"+params.length) ;
                    } else {
                        //simple value ope
                        if(ope === "=" && value === null){
                            where.push(c.column_name+" IS NULL") ;
                        }else{
                            params.push(value) ;
                            where.push(c.column_name+" "+ope+" $"+params.length) ;
                        }
                    }
                }
            }

            let sql = `SELECT * FROM ${table}`;
            if(where.length > 0){
                sql += ` WHERE ${where.join("AND")}` ;
            }
             
            if(orderBy){
                let colNames = columns.map((c)=>{ return c.column_name ;})
                if(orderBy.split(",").every((ob)=>{
                    //check we only receive a valid column name and asc/desc
                    let col = ob.replace("DESC", "").replace("desc", "")
                    .replace("ASC", "").replace("asc", "").trim() ;
                    return colNames.indexOf(col) !== -1 ;
                }) ){
                    sql += ` ORDER BY ${orderBy}` ;
                }else{
                    return callback("Invalid order by clause "+orderBy) ;
                }
            }
            if(limit) {
                limit = parseInt(limit, 10) ;
                if(!isNaN(limit)){
                    sql += ` LIMIT ${limit}` ;
                }else{
                    return callback("Invalid limit clause "+limit) ;
                }
            }
            if(offset) {
                offset = parseInt(offset, 10) ;
                if(!isNaN(offset)){
                    sql += ` OFFSET ${offset}` ;
                }else{
                   return callback("Invalid offset clause "+offset) ;
                }
            }
            callback(null, sql, params) ;
        });
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
        if(this.schema){
            return callback(null, this.schema) ;
        }
        this.query(`
                SELECT t.table_name, column_name, udt_name, character_maximum_length, numeric_precision, datetime_precision
                    FROM information_schema.columns t
                JOIN information_schema.tables t1 on t.table_name = t1.table_name
                    WHERE t.table_schema='public'
                    AND t1.table_type = 'BASE TABLE'
                    order by t.table_name, ordinal_position
        `, [], (err, results)=>{
            if(err){ return callback(err); }

            let schema = {} ;
            for(let r of results.rows){
                
                let table = schema[r.table_name] ;

                if(!table){
                    table = {
                        columns: [],
                        pk: []
                    } ;
                    schema[r.table_name] = table;
                }
                
                delete r.table_name ;
                table.columns.push({
                    name: r.column_name,
                    type: r.udt_name,
                    size : r.character_maximum_length || r.numeric_precision || r.datetime_precision
                }) ;
            }

            this.query( `
                    select kc.column_name , t.table_name
                    from  
                        information_schema.table_constraints tc
                        JOIN information_schema.key_column_usage kc ON kc.table_name = tc.table_name and kc.table_schema = tc.table_schema
                        and kc.constraint_name = tc.constraint_name
                        JOIN information_schema.tables t on tc.table_name = t.table_name
                    where 
                        tc.constraint_type = 'PRIMARY KEY' 
                    order by t.table_name, ordinal_position
            `, [], (err, results)=>{
                    if(err){ return callback(err); }
                    for(let r of results.rows){
                        let table = schema[r.table_name] ;
                        if(table){
                            table.pk.push(r.column_name) ;
                        }
                    }

                    this.schema = schema ;
                    callback(null, schema) ;
            }) ;
        }) ;
    }

    /**
     * Get the columns of a table. Give back an array of columns definition
     * 
     * Note : result is cached so in the case you modify the table while application is running you should restart to see the modifications
     * 
     * @param {string} table the table name
     * @param {function(Error, Array)} callback called when found primary key, return array of column definitions
     */
    getColumnsDefinition(table, callback){
        if(this._cacheColumns[table]){
            return callback(null, this._cacheColumns[table]) ;
        }
        this.query(`SELECT column_name, udt_name, character_maximum_length, numeric_precision, datetime_precision
                    FROM information_schema.columns t
                JOIN information_schema.tables t1 on t.table_name = t1.table_name
                    WHERE t.table_schema='public'
                    AND t1.table_type = 'BASE TABLE' AND t.table_name = $1
                    order by t.table_name, ordinal_position
                    `, [table], (err, result)=>{
            if(err){ return callback(err); }

            this._cacheColumns[table] = result.rows ;
            callback(null, this._cacheColumns[table]) ;
        });
    }

    /**
     * Get the primary key of a table. Give back an array of column composing the primary key
     * 
     * Note : result is cached so in the case you modify the table while application is running you should restart to see the modifications
     * 
     * @param {string} table the table name
     * @param {function(Error, Array)} callback called when found primary key, return array of column names composing primary key
     */
    getPrimaryKey(table, callback){
        if(this._cachePk[table]){
            return callback(null, this._cachePk[table]) ;
        }
        this.query(`select kc.column_name 
                    from  
                        information_schema.table_constraints tc
                        JOIN information_schema.key_column_usage kc ON kc.table_name = tc.table_name and kc.table_schema = tc.table_schema
                        and kc.constraint_name = tc.constraint_name
                        JOIN information_schema.tables t on tc.table_name = t.table_name
                    where 
                        tc.constraint_type = 'PRIMARY KEY' AND tc.table_name = $1
                    `, [table], (err, result)=>{
            if(err){ return callback(err); }

            this._cachePk[table] = result.rows.map((r)=>{
                return r.column_name ;
            }) ;
            callback(null, this._cachePk[table]) ;
        });
    }

    /**
     * Execute the schema changes and update the version number
     * @param {VeloxSqlChange[]} changes - Array of changes
     * @param {number} newVersion - The new database version
     * @param {function(err)} callback - called when finish
     */
    runQueriesAndUpdateVersion(changes, newVersion, callback){
        this.transaction((tx, done)=>{
            let job = new AsyncJob(AsyncJob.SERIES) ;
            for(let change of changes){
                if(change.run){
                    //this change is a function that must be executed
                    job.push((cb)=>{
                        
                        change.run(tx, cb) ;
                    }) ;
                } else {
                    //this change is a SQL query to run
                    job.push((cb)=>{
                        tx.query(change.sql, change.params, cb) ;
                    }) ;
                }
            }
            job.push((cb)=>{
                tx.query(`UPDATE ${DB_VERSION_TABLE} SET version = $1, last_update = now()`, [newVersion], cb) ;
            }) ;
            job.async(done) ;
        }, callback) ;
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
     * @param {number} timeout - if this timeout (seconds) is expired, the transaction is automatically rollbacked.
     *          If not set, default value is 30s. If set to 0, there is no timeout (not recomended)
     *
     */
    transaction(callbackDoTransaction, callbackDone, timeout){
        if(!callbackDone){ callbackDone = function(){} ;}
        var finished = false;
        if(timeout === undefined){ timeout = 30; }
			
        var tx = new VeloxDbPgClient(this.connection, function(){}, this.logger) ;
        tx.transaction = function(){ throw "You should not start a transaction in a transaction !"; }
            
		this.connection.query("BEGIN", (err) => {
            if(err){
                finished = true ;
                return callbackDone(err);
            }
            
            var timeoutId = null;
            if(timeout > 0){
                timeoutId = setTimeout(function(){
                    if(!finished){
                        //if the transaction is not closed, do rollback
                        this.connection.query("ROLLBACK", (err)=>{
                            finished = true;
                            if(err) {
                                return callbackDone("Transaction timeout after "+timeout+" seconds. Rollback failed : "+err);
                            }
                            callbackDone("Transaction timeout after "+timeout+" seconds. Rollback done");
                        });  
                    }
                }, timeout*1000);
            }
	
            try{
                callbackDoTransaction(tx, (err, data)=>{
                        if(finished){
                            //Finish work for this transaction after being already commited or rollbacked. Ignore commit
                            //Maybe done has been called twice
                            return;
                        }
                        if(err){
                            if(!finished){
                                //if the transaction is not closed, do rollback
                                this.connection.query("ROLLBACK", (errRollback)=>{
                                    if(timeoutId){ clearTimeout(timeoutId) ;}
                                    finished = true;
                                    if(errRollback) {
                                        return callbackDone("Transaction fail with error "+err+" and rollback failed with error "+errRollback);
                                    }
                                    callbackDone(err);
                                });
                            }else{
                                //the transaction is already closed, do nothing
                                callbackDone(err) ;
                            }
                        } else {
                            this.connection.query("COMMIT",(errCommit)=>{
                                if(timeoutId){ clearTimeout(timeoutId) ;}
                                finished = true;
                                if(errCommit) {
                                    return callbackDone("Transaction fail when commit "+errCommit);
                                }
                                callbackDone(null, data);
                            });
                        }
                    }) ;
            }catch(e){
                if(!finished){
                    //if the transaction is not closed, do rollback
                    this.connection.query("ROLLBACK",(errRollback)=>{
                        if(timeoutId){ clearTimeout(timeoutId) ;}
                        finished = true;
                        if(errRollback) {
                            return callbackDone("Transaction fail with error "+e+" and rollback failed with error "+errRollback);
                        }
                        callbackDone(e);
                    });
                }else{
                    //already closed
                    callbackDone(e);	
                }
            }
		}) ;
    };

    /**
     * Close the database connection
     */
    close() {
        this.closeCb() ;
    }
}

/**
 * VeloxDatabase PostgreSQL backend
 */
class VeloxDbPgBackend {

   /**
     * @typedef VeloxDbPgBackendOptions
     * @type {object}
     * @property {string} user database user
     * @property {string} host database host
     * @property {string} database database name
     * @property {string} password database password
     * @property {VeloxLogger} logger logger
     */

    /**
     * Create a VeloxDbPgBackend
     * 
     * @param {VeloxDbPgBackendOptions} options 
     */
    constructor(options){
        this.options = options ;

        for( let k of ["user", "host", "port", "database", "password"]){
            if(options[k] === undefined) { throw "VeloxDbPgBackend : missing option "+k ; } 
        }

        this.pool = new Pool({
            user: options.user,
            host: options.host,
            database: options.database,
            password: options.password,
            port: options.port || 3211
        }) ;

        this.logger = new VeloxLogger("VeloxDbPgBackend", options.logger) ;

    }

    /**
     * Get a database connection from the pool
     * 
     * @param {function(Error, VeloxDbPgClient)} callback - Callback with VeloxDbPgClient instance
     */
    open(callback){
        this.pool.connect((err, client, done) => {
            if(err){ return callback(err); }

            let dbClient = new VeloxDbPgClient(client, done, this.logger) ;
            callback(null, dbClient) ;
        });
    }

    /**
     * Create the database if not exists
     * 
     * @param {function(err)} callback 
     */
    createIfNotExist(callback){
        const client = new Client(this.options) ;
        client.connect((err) => {
            if(err){ 
                //likely db does not exists
                this.logger.info("Database does not exists, try to create");
                let optionsTemplate1 = JSON.parse(JSON.stringify(this.options)) ;
                optionsTemplate1.database = "template1" ;
                const clientTemplate = new Client(optionsTemplate1) ;
                clientTemplate.connect((err)=>{
                    if(err) {
                        //can't connect to template1 to create database
                        this.logger.error("Can't connect to template1 to create database");
                        return callback(err) ;
                    }
                    clientTemplate.query("CREATE DATABASE "+this.options.database, [], (err)=>{
                        clientTemplate.end() ;
                        if(err){
                            //CREATE query failed
                            this.logger.error("Create database failed");
                            return callback(err) ;
                        }
                        callback(); //CREATE ok
                    }) ;
                }) ;
            }else{
                //connection OK
                this.logger.debug("Database connection OK");
                client.end() ;
                callback() ;
            }
        }) ;
    }


}

module.exports = VeloxDbPgBackend ;