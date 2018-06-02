const { Pool, Client } = require('pg');
const AsyncJob = require("velox-commons/AsyncJob") ;
const VeloxLogger = require("velox-commons/VeloxLogger") ;

const DB_VERSION_TABLE = "velox_db_version" ;

function extendsSchema(schemaBase, schemaExtends){
    Object.keys(schemaExtends).forEach(function(table){
        if(schemaBase[table]){
            if(schemaExtends[table].columns){
                schemaExtends[table].columns.forEach(function(col){
                    var found = schemaBase[table].columns.some(function(colBase){
                        if(colBase.name === col.name){
                            Object.keys(col).forEach(function(k){
                                colBase[k] = col[k] ;
                            }) ;
                            return true ;
                        }
                    }) ;
                    if(!found){
                        schemaBase[table].columns.push(col) ;
                    }
                }) ;
            }
            Object.keys(schemaExtends[table]).forEach(function(k){
                if(!schemaBase[table][k] || (Array.isArray(schemaBase[table][k]) && schemaBase[table][k].length === 0 )) {
                    schemaBase[table][k] = schemaExtends[table][k] ;
                }
            }) ;
            
        }
    }) ;
}

class VeloxDbPgClient {

    /**
     * Create the client connection
     * 
     * @param {object} connection The connection client from the pool
     * @param {function} closeCb the callback to give back the client to the pool
     * @param {VeloxLogger} logger logger
     */
    constructor(connection, closeCb, logger, cache, schema, customInit){
        this.connection = connection;
        this.closeCb = closeCb ;
        this.logger = logger ;

        if(!cache._cachePk){
            cache._cachePk = {} ;
        }
        if(!cache._cacheColumns){
            cache._cacheColumns = {} ;
        }
        this.cache = cache ;
        this.schema = schema ;
        this.customInit = customInit ;
        for(let custo of customInit){
            custo(this);
        }
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
        this._query(sql, params, callback) ;
    }

    /**
     * Execute a query and give the result back
     * 
     * @param {string} sql - SQL to execute
     * @param {Array} [params] - Params
     * @param {function(err, results)} callback - called when finished
     */
    _query(sql, params, callback){
        
        if(!callback && typeof(params) === "function"){
            callback = params;
            params = [];
        }
        this.logger.debug("Run SQL "+sql+", params "+JSON.stringify(params)) ;
        let lowerSql = sql.toLowerCase() ;
        if(lowerSql.indexOf("create ") != -1 || lowerSql.indexOf("alter ") != -1 ){
            delete this.cache.schema ;
        }
        this.connection.query(sql, params, (err, results)=>{
            if(err){
                this.logger.error("Error while running query "+sql+", params "+JSON.stringify(params)+" : "+JSON.stringify(err)) ;
                return callback(err) ;
            }
            callback(null, results) ;
        }) ;
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
        this._queryFirst(sql, params, callback) ;
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
    _queryFirst(sql, params, callback){
        if(!callback && typeof(params) === "function"){
            callback = params;
            params = [];
        }
        this._query(sql, params, (err, results)=>{
            if(err){ return callback(err); }
            if(results.rows.length === 0){
                return callback(null, null) ;
            }
            return callback(null, results.rows[0]) ;
        }) ;
    }

    /**
     * If you want to get a subquery instead of table, implement getTable_your_table_name function
     * 
     * @example
     * this.extendsClient = {
     *    getTable_foo = function(){ return "(select * from foo where restricted = false)" ;}
     * }
     * 
     * @param {string} table 
     */
    getTable(table){
        if(this["getTable_"+table]){
            return this["getTable_"+table]() ;
        }
        return table;
    }

    _createFromWithJoin(table, joinFetch, params, schema){
        let from = [`${this.getTable(table)} t`] ;
        let select = ["t.*"] ;
        let aliases = {} ;
        aliases.main = "t" ;
        if(joinFetch){
            for(let join of joinFetch){
                this._addFromJoin(join, schema, select, from, aliases, params, table) ;
            }
        }
        return {
            from: from,
            select: select,
            aliases: aliases
        } ;
    }

    _addFromJoin(join, schema, select, from, aliases, params, baseTable, parentAliasId){
        if(!parentAliasId){ parentAliasId = "main" ;}
        let j = "";
        j += " LEFT JOIN" ;
        
        if(!schema[join.otherTable]){ throw ("Unknown table "+join.otherTable) ;}

        let alias = "t"+from.length ;
        var aliasId = parentAliasId+"_"+(join.name||join.otherTable) ;
        aliases[aliasId] = alias ;
        j += ` ${this.getTable(join.otherTable)} ${alias} ` ;

        let otherField = join.otherField ;
        if(otherField){
            if(!schema[join.otherTable].columns.some((c)=>{ return c.name === otherField ;})){ 
                throw ("Unknown columns "+join.otherTable+"."+otherField) ;
            }
        }
        
        let thisTable = join.thisTable||baseTable;
        if(join.thisTable){
            if(!schema[join.thisTable]){ throw ("Unknown table "+join.thisTable) ;}
        }
        let thisField = join.thisField;
        if(thisField){
            if(!schema[thisTable].columns.some((c)=>{ return c.name === thisField ;})){ 
                throw ("Unknown columns "+thisTable+"."+thisField) ;
            }
        }

        if(otherField && !thisField || !otherField && thisField){ throw ("You must set both otherField and thisField") ; }

        if(otherField && thisField){
            j += " ON "+aliases[aliasId]+".\""+otherField+"\" = "+aliases[parentAliasId]+".\""+thisField+"\"" ;
        }else{
            if(!otherField){
                //assuming using FK

                let pairs = {} ;

                //look in this table FK
                for(let fk of schema[thisTable].fk){
                    if(fk.targetTable === join.otherTable){
                        pairs[aliases[parentAliasId]+".\""+fk.thisColumn+"\""] = aliases[aliasId]+".\""+fk.targetColumn+"\"" ;
                    }
                }

                if(Object.keys(pairs).length === 0){
                    //look in other table FK
                    for(let fk of schema[join.otherTable].fk){
                        if(fk.targetTable === thisTable){
                            pairs[aliases[aliasId]+".\""+fk.thisColumn+"\""] = aliases[parentAliasId]+".\""+fk.targetColumn+"\"" ;
                        }
                    }
                }

                if(Object.keys(pairs).length === 0){
                    throw ("No otherField/thisField given and can't find in FK in join "+JSON.stringify(join)) ;
                }

                for(let left of Object.keys(pairs)){
                    j += " ON "+left+" = "+pairs[left] ;
                }
            }
        }

        if(join.joinSearch){
            var {where, params} = this._prepareWhereCondition(schema[join.otherTable].columns, join.joinSearch, join.otherTable, params, alias) ;
            j += " AND "+where.join(" AND ") ;
        }

        for(let col of schema[join.otherTable].columns){
            select.push(alias+".\""+col.name+"\" AS \""+alias+"_"+col.name+"\"") ;
        }

        from.push(j) ;

        if(join.joins){
            for(let subJoin of join.joins){
                this._addFromJoin(subJoin, schema, select, from, aliases, params, join.otherTable, aliasId) ;
            }
        }
    }

    

    constructResults(schema, table, aliases, rows, joinFetch, aliasId){
        //aggregates records by pk

        let recordsByPk = [];
        let pkIndexes = {} ;
        for(let r of rows){
            let pkValue = "";
            let a = aliases[aliasId||"main"] ;
            if(a === "t"){ 
                //main table, no column prefix
                a = "" ;
            }else{
                a = a+"_" ;
            }

            if(schema[table].pk.length === 0){
                throw "Missing pk on table "+table ;
            }

            for(let pk of schema[table].pk){
                pkValue += r[a+pk] || "";
            }
            if(pkValue){
                //on some JOIN cases, the record can be present because of the JOIN but all values are null
                //don't add a record in this case
                let index = pkIndexes[pkValue] ;
                if(index === undefined){
                    index = recordsByPk.length ;
                    pkIndexes[pkValue] = index;
                    var record = {} ;
                    for(let col of schema[table].columns){
                        record[col.name] = r[a+col.name] ;
                    }
                    recordsByPk.push({
                        record: record,
                        rows : []
                    }) ;
                }
                recordsByPk[index].rows.push(r) ;
            }
        }

        //do a first loop to initialize the thisTable property because be dig inside sub join, it will provoque ambiguity
        if(joinFetch){
            for(let join of joinFetch){
                if(!join.thisTable){ join.thisTable = table; }
            }
            for(let join of joinFetch){
                let thisTable = join.thisTable;
                let otherTable = join.otherTable;
                let joinType = join.type || "2one" ;
                var joinAliasId = (aliasId||"main")+"_"+(join.name||join.otherTable) ;
                if(thisTable === table){
                    for(let rec of recordsByPk){
                        let otherRecords = this.constructResults(schema, otherTable, aliases, rec.rows, join.joins, joinAliasId) ;
                        if(joinType === "2many"){
                            rec.record[join.name||join.otherTable] = otherRecords ;
                        }else{
                            rec.record[join.name||join.otherTable] = otherRecords[0]||null ;
                        }
                    }
                }
            }
        }

        return recordsByPk.map(function(rec){ return rec.record ;}) ;
    }

    /**
     * @typedef VeloxDatabaseJoinFetch
     * @type {object}
     * @property {string} otherTable other table name
     * @property {string} [otherField] field in other table to make the join (if not given, will try to rely on the foreing keys)
     * @property {string} [thisTable] the starting table for the join (default : the current table)
     * @property {string} [thisField] field in starting table to make the join (if not given, will try to rely on the foreing keys)
     * @property {string} [type] type of join fetching : 2one for a single result, 2many for many result (default : 2one)
     * @property {string} [name] name of the join, will be the name of the property holding the result (default : otherTable name)
     */

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
     * @param {VeloxDatabaseJoinFetch} [joinFetch] join fetch from other sub tables
     * @param {function(Error,object)} callback called with result. give null if not found
     */
    getByPk(table, pk, joinFetch, callback){
        if(typeof(joinFetch) === "function"){
            callback = joinFetch;
            joinFetch = null;
        }

        if(!pk) {
            return callback("Error searching in table "+table+", empty primary key given") ;
        }

        this.getSchema((err, schema)=>{
            if(err){ return callback(err); }

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

                let params = [] ;
                let selectFrom = null;
                try{
                    selectFrom = this._createFromWithJoin(table, joinFetch, params, schema) ;
                }catch(e){
                    return callback(e) ;
                }
                let select = selectFrom.select ;
                let from = selectFrom.from ;
                let aliases = selectFrom.aliases;
                let where = [] ;
                
                
                for(let k of pkColumns){
                    params.push(pk[k]) ;
                    where.push("t."+k+" = $"+params.length) ;
                }

                var orderByItems = [] ;

                if(joinFetch){
                    for(let join of joinFetch){
                        this._addOrderByJoin(join, schema, orderByItems, aliases) ;
                    }
                }

                if(orderByItems.length > 0){
                    //there is order by clause in join fetch, force add the pk in order by
                    let pkNames = schema[table].pk.map((p)=>{ return "t."+p ;}).join(", ") ; 
                    if(!pkNames){ callback("No PK defined for table "+table) ;}
                    orderByItems = [pkNames].concat(orderByItems) ;
                }
                
                let sql = `SELECT ${select.join(", ")} FROM ${from.join(" ")} WHERE ${where.join(" AND ")}` ;
                
                if(orderByItems.length > 0){
                    sql += ` ORDER BY ${orderByItems.join(",")}` ;
                }

                this._query(sql, params, (err, results)=>{
                    if(err){ return callback(err) ;}
                    if(results.rows.length === 0){ return callback(null, null); }
                    if(!joinFetch){ return callback(null, results.rows[0]); }

                    //need some aggregate from joins
                    try{
                        var records = this.constructResults(schema, table, aliases, results.rows, joinFetch) ;

                        var record = records[0]||null ;
                    } catch (err) {
                        return callback(err) ;
                    }
                    
                    callback(null, record) ;

                }) ;
            }) ;
        }) ;
    }

    _addOrderByJoin(join, schema, orderByItems, aliases, parentAliasId){
        if(!parentAliasId){ parentAliasId = "main" ;}
        
        var aliasId = parentAliasId+"_"+(join.name||join.otherTable) ;

        if(join.orderBy){
            if(!this._checkOrderByClause(join.orderBy, schema[join.otherTable].columns)){
                throw "Invalid order by clause "+join.orderBy ;
            }
            orderByItems.push(join.orderBy.split(",").map((orderItem)=>{
                return aliases[aliasId]+"."+orderItem.trim() ;
            }).join(",")) ;
        }

        if(join.joins){
            for(let subJoin of join.joins){
                this._addOrderByJoin(subJoin, schema, orderByItems, aliases, aliasId) ;
            }
        }
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

            this._query(sql, params, callback) ;
        }) ;
    }
    
    /**
     * Delete a record in the table following conditions
     * 
     * @example
     * //delete by simple column
     * client.removeWhere("foo", {"bar": 2}, (err)=>{...})
     * //delete by condition
     * client.removeWhere("foo", {"bar": {ope : ">", value : 1}}, (err)=>{...})
     * 
     * @param {string} table the table name
     * @param {object} condition the search condition
     * @param {function(Error)} callback called when done
     */
    removeWhere(table, conditions, callback){

        this.getSchema((err, schema)=>{
            if(err){ return callback(err); }

            if(!schema[table]){
                return callback("Unkown table "+table) ;
            }

            let columns = schema[table].columns ;
            try {
                var {where, params} = this._prepareWhereCondition(columns, conditions, table) ;
            }catch(e){
                callback(e) ;
            }
            
            let sql = `DELETE FROM ${table} t WHERE ${where.join(" AND ")}` ;

            this._query(sql, params, callback) ;
        }) ;
    }


    /**
     * Insert a record in the table. Give back the inserted record (with potential generated values)
     * 
     * @param {string} table the table name
     * @param {object} records the object to insert or an array of object to insert
     * @param {function(Error, object)} callback called when insert is done. give back the inserted result (with potential generated values)
     */
    insert(table, records, callback){
        if(!records) { return callback("Try to insert null record in table "+table) ; }
        this.getSchema((err, schema)=>{
            if(err){ return callback(err); }

            let cols = [];
            let values = [];
            let params = [] ;
            if(!Array.isArray(records)){
                records = [records] ;
            }
            for(let c of schema[table].columns){
                for(let r of records){
                    if(r[c.name] !== undefined){
                        cols.push(c.name) ;
                        break;
                    }
                }
            }
            if(cols.length === 0){
                return callback("Can't found any column to insert in "+table+" from record "+JSON.stringify(records)) ;
            }

            var sequences = {} ;
            if(schema[table].sequenceCols){
                for(let sc of schema[table].sequenceCols){
                    if(cols.indexOf(sc.col) === -1){
                        cols.push(sc.col) ;
                    }
                    sequences[sc.col] = sc.sequence;
                }
            }

            for(let record of records){
                var valuesCols = [] ;
                for(let c of cols){
                    if(sequences[c] && record[c] === undefined){
                        //this column is a sequence and is not given
                        valuesCols.push("nextval('"+sequences[c]+"')") ;
                    }else{
                        params.push(record[c]) ;
                        valuesCols.push("$"+params.length) ;
                    }
                }
                values.push(`(${valuesCols.join(",")})`);
            }


            let sql = `INSERT INTO ${table}(${cols.map((c)=>{ return '"'+c+'"' ;}).join(",")}) VALUES ${values.join(",")} RETURNING *` ;

            this._queryFirst(sql, params, callback) ;
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
                        sets.push("\""+c.column_name+"\" = $"+params.length) ;
                    }
                }
                let where = [] ;
                for(let k of pkColumns){
                    params.push(record[k]) ;
                    where.push("\""+k+"\" = $"+params.length) ;
                }

                if(sets.length === 0){
                    //nothing to update, select the record and return it
                    return this.getByPk(table, record, callback) ;
                }

                let sql = `UPDATE ${table} SET ${sets.join(",")} WHERE ${where.join(" AND ")} RETURNING *` ;

                this._queryFirst(sql, params, callback) ;
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
     * @param {VeloxDatabaseJoinFetch} [joinFetch] join fetch from other sub tables
     * @param {string} [orderBy] order by clause
     * @param {number} [offset] offset, default is 0
     * @param {number} [limit] limit, default is no limit
     * @param {function(Error, Array)} callback called on finished. give back the found records
     */
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
        
        this._prepareSearchQuery(table, search, joinFetch, orderBy, offset, limit, (err, sql, params, aliases, joinFetch, schema)=>{
            if(err){ return callback(err); }
            this._query(sql, params, (err, result)=>{
                if(err){ return callback(err); }

                if(!joinFetch){
                    return callback(null, result.rows) ;
                }
                try{
                    let records = this.constructResults(schema, table,  aliases, result.rows, joinFetch) ;
                    callback(null, records) ;
                }catch(err){
                    return callback(err) ;
                }

                
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
     * @param {VeloxDatabaseJoinFetch} [joinFetch] join fetch from other sub tables
     * @param {string} [orderBy] order by clause
     * @param {function(Error, Array)} callback called on finished. give back the first found records
     */
    searchFirst(table, search, joinFetch, orderBy, callback){
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
        this.search(table, search, joinFetch, orderBy, 0, 1, (err, results)=>{
            if(err){ return callback(err); }
            if(results.length === 0){
                callback(null, null) ;
            }else{
                callback(null, results[0]) ;
            }
        }) ;
    }

    _prepareWhereCondition(columns, search, table, params, alias){
        let where = [];
        if(!params){
            params = [] ;
        }
        if(!alias){
            alias = "t" ;
        }
        for(let c of columns){
            if(search[c.name] !== undefined){
                let value = search[c.name] ;
                let ope = "=" ;
                if(typeof(value) === "object" && !Array.isArray(value)){
                    ope = value.ope ;
                    value = value.value ;
                    if(!ope){
                        throw ("Search with special condition wrong syntax. Expected {ope: ..., value: ...}. received "+JSON.stringify(search)) ;
                    }
                }else{
                    if(Array.isArray(value)){
                        ope = "IN" ;
                    }else if(typeof(value) === "string" && value.indexOf("%") !== -1){
                        ope = "ILIKE" ;
                    }                        
                }

                if(ope.toUpperCase() === "IN" || ope.toUpperCase() === "NOT IN"){
                    if(!Array.isArray(value) || value.length === 0){
                        throw ("Search in table "+table+" failed. Search operand IN provided with no value. Expected an array with at least one value") ;
                    }
                    let wVals = [] ;
                    for(let v of value){
                        params.push(v) ;
                        wVals.push("$"+params.length) ;
                    }
                    where.push(alias+".\""+c.name+"\" "+ope+" ("+wVals.join(",")+")") ;
                } else if (ope.toUpperCase() === "BETWEEN"){
                    if(!Array.isArray(value) || value.length !== 2){
                        throw ("Search in table "+table+" failed. Search operand BETWEEN provided with wrong value. Expected an array with 2 values") ;
                    }
                    params.push(value[0]) ;
                    params.push(value[1]) ;
                    where.push(alias+".\""+c.name+"\" BETWEEN $"+(params.length-1)+" AND $"+params.length) ;
                } else {
                    //simple value ope
                    if(ope === "=" && value === null){
                        where.push(alias+".\""+c.name+"\" IS NULL") ;
                    }else{
                        params.push(value) ;
                        where.push(alias+".\""+c.name+"\" "+ope+" $"+params.length) ;
                    }
                }
            }
        }
        if(search.$or){
            if(!Array.isArray(search.$or)){
                throw "$or must be an array of sub predicate" ;
            }
            var subWheres = [] ;
            for(let orPart of search.$or){
                var subConditions = this._prepareWhereCondition(columns, orPart, table, params, alias) ;
                subWheres.push(subConditions.where.join(" AND ")) ;
            }
            where.push("("+subWheres.map((w)=>{ return "("+w+")" ;}).join(" OR ")+")") ;
        }
        if(search.$and){
            if(!Array.isArray(search.$and)){
                throw "$and must be an array of sub predicate" ;
            }
            var subWheres = [] ;
            for(let andPart of search.$and){
                var subConditions = this._prepareWhereCondition(columns, andPart, table, params, alias) ;
                subWheres.push(subConditions.where.join(" AND ")) ;
            }
            where.push("("+subWheres.map((w)=>{ return "("+w+")" ;}).join(" AND ")+")") ;
        }
        return {where: where, params: params};
    }

    _checkOrderByClause(orderBy, columns){
        let colNames = columns.map((c)=>{ return c.name ;}) ;
        var orderByIsRealColumn = orderBy.split(",").every((ob)=>{
            //check we only receive a valid column name and asc/desc
            let col = ob.replace("DESC", "").replace("desc", "")
            .replace("ASC", "").replace("asc", "").trim() ;
            return colNames.indexOf(col) !== -1 ;
        })  ;
        return orderByIsRealColumn ;
    }

    /**
     * Prepare the search SQL
     * 
     * @param {string} table table name
     * @param {object} search search object
     * @param {VeloxDatabaseJoinFetch} [joinFetch] join fetch from other sub tables
     * @param {string} [orderBy] order by clause
     * @param {number} [offset] offset
     * @param {number} [limit] limit
     * @param {function(Error, Array)} callback called on finished. give back the created sql and params
     */
    _prepareSearchQuery(table, search, joinFetch, orderBy, offset, limit, callback){
        if(!search) { return callback("Try to search with null search in table "+table) ; }

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

        this.getSchema((err, schema)=>{
            if(err){ return callback(err); }

            if(!schema[table]){
                return callback("Unkown table "+table) ;
            }

            let columns = schema[table].columns ;
            let params = [] ;
            let selectFrom = null;
            try{
                selectFrom = this._createFromWithJoin(table, joinFetch, params, schema) ;
            }catch(e){
                return callback(e) ;
            }
            let select = selectFrom.select ;
            let from = selectFrom.from ;
            let aliases = selectFrom.aliases;
            try {
                var {where, _} = this._prepareWhereCondition(columns, search, table, params) ;
            }catch(e){
                callback(e) ;
            }

            var orderByItems = [] ;
            if(orderBy){
                if(!this._checkOrderByClause(orderBy, columns)){
                    return callback("Invalid order by clause "+orderBy) ;
                }
                orderByItems.push(orderBy) ;
            }


            if(joinFetch){
                for(let join of joinFetch){
                    this._addOrderByJoin(join, schema, orderByItems, aliases) ;
                }
            }

            if(orderByItems.length > 0){
                //there is order by clause in join fetch, force add the pk in order by
                let pkNames = schema[table].pk.map((p)=>{ return "t."+p ;}).join(", ") ; 
                if(!pkNames){ callback("No PK defined for table "+table) ;}
                orderByItems = orderByItems.concat([pkNames]) ;
            }


            if(limit || offset){
                if(joinFetch){
                    //we must do some windowing
                    let pkNames = schema[table].pk.map((p)=>{ return "t."+p ;}).join(", ") ; 
                    if(!pkNames){ callback("No PK defined for table "+table) ;}
                    select.push(`DENSE_RANK() OVER (ORDER BY ${pkNames}) AS velox_window_rownum`) ;
                }else{
                    //classical offset/limit
                    if(limit) {
                        limit = parseInt(limit, 10) ;
                        if(isNaN(limit)){
                            return callback("Invalid limit clause "+limit) ;
                        }
                    }
                    if(offset) {
                        offset = parseInt(offset, 10) ;
                        if(isNaN(offset)){
                            return callback("Invalid offset clause "+offset) ;
                        }
                    }
                }
            }

            let sql = `SELECT ${select.join(", ")} FROM ${from.join(" ")}`;
            if(where.length > 0){
                sql += ` WHERE ${where.join(" AND ")}` ;
            }
            if(orderByItems.length > 0){
                sql += ` ORDER BY ${orderByItems.join(",")}` ;
            }
            if(!joinFetch && (limit || offset)){
                //normal offset
                if(limit) {
                    sql += ` LIMIT ${limit}` ;
                }
                if(offset) {
                    sql += ` OFFSET ${offset}` ;
                }
            }else if(joinFetch && (limit || offset)){
                //must do windowing
                let windowWhere = [] ;
                if(limit){
                    windowWhere.push(`sub.velox_window_rownum <= ${limit}`) ;
                } 
                if(offset){
                    windowWhere.push(`sub.velox_window_rownum >= ${offset}`) ;
                }
                sql = `SELECT * FROM (${sql}) sub WHERE ${windowWhere.join(" AND ")}` ;
            }
            
            callback(null, sql, params, aliases, joinFetch, schema) ;
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
        if(this.cache.schema){
            return callback(null, this.cache.schema) ;
        }
        this._query(`
                SELECT t.table_name, column_name, udt_name, character_maximum_length, numeric_precision, datetime_precision
                    FROM information_schema.columns t
                JOIN information_schema.tables t1 on t.table_name = t1.table_name
                    WHERE t.table_schema='public'
                    AND t1.table_type IN ('BASE TABLE', 'VIEW')
                    order by t.table_name, ordinal_position
        `, [], (err, results)=>{
            if(err){ return callback(err); }

            let schema = {} ;
            for(let r of results.rows){
                
                let table = schema[r.table_name] ;

                if(!table){
                    table = {
                        columns: [],
                        pk: [],
                        fk: []
                    } ;
                    schema[r.table_name] = table;
                }
                
                delete r.table_name ;
                table.columns.push({
                    name: r.column_name,
                    type: this._sanitizeType(r.udt_name),
                    size : r.character_maximum_length || r.numeric_precision || r.datetime_precision
                }) ;
            }

            this._query( `
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

                    this._query( `
                            select kc.column_name , tc.table_name, ccu.table_name AS foreign_table_name,
                                ccu.column_name AS foreign_column_name 
                            from  
                                information_schema.table_constraints tc
                                JOIN information_schema.key_column_usage kc ON kc.table_name = tc.table_name and kc.table_schema = tc.table_schema
                                and kc.constraint_name = tc.constraint_name
                                JOIN information_schema.constraint_column_usage AS ccu
                                ON ccu.constraint_name = tc.constraint_name
                            where 
                                tc.constraint_type = 'FOREIGN KEY' 
                            order by tc.table_name, ordinal_position
                    `, [], (err, results)=>{
                            if(err){ return callback(err); }
                            for(let r of results.rows){
                                let table = schema[r.table_name] ;
                                if(table){
                                    table.fk.push({
                                        targetTable: r.foreign_table_name,
                                        thisColumn: r.column_name,
                                        targetColumn: r.foreign_column_name,
                                    }) ;
                                }
                            }

                            extendsSchema(schema, this.schema) ;

                            for(let tableName of Object.keys(schema)){
                                if(schema[tableName].pk.length === 0){
                                    schema[tableName].pk = schema[tableName].columns.map((c)=>{return c.name ;}) ;
                                }
                            }


                            if(schema.velox_db_version){
                                this._query("select * from velox_db_version", [], (err, results)=>{
                                    if(err){ return callback(err); }
                                    schema.__version = results.rows.length>0?results.rows[0]:{version: 0};

                                    if(schema.__version.version == 0){
                                        //we don't manage schema change, so we just compute a fake version number from columns and table count
                                        //this assume that you always add more table and columns to the database !
                                        this._query(`
                                        select t.table_count + c.col_count as version, NULL as last_update from 
                                            (select count(*) col_count from information_schema.columns) c,
                                            (select count(*) table_count from information_schema.tables) t
                                        `, [], (err, results) => {
                                            if(err){ return callback(err); }
                                            schema.__version = results.rows.length>0?results.rows[0]:{version: 0};

                                            this.cache.schema = schema ;
                                            callback(null, schema) ;
                                        }) ;
                                    }else{
                                        this.cache.schema = schema ;
            
                                        callback(null, schema) ;
                                    }

                                }) ;
                            }else{
                                //we don't manage schema change, so we just compute a fake version number from columns and table count
                                //this assume that you always add more table and columns to the database !
                                this._query(`
                                select t.table_count + c.col_count as version, NULL as last_update from 
                                    (select count(*) col_count from information_schema.columns) c,
                                    (select count(*) table_count from information_schema.tables) t
                                `, [], (err, results) => {
                                    if(err){ return callback(err); }
                                    schema.__version = results.rows.length>0?results.rows[0]:{version: 0};

                                    this.cache.schema = schema ;
                                    callback(null, schema) ;
                                }) ;
                            }

                    }) ;
            }) ;
        }) ;
    }

    _sanitizeType(type){
        if(type === "int4"){
            return "int" ;
        }
        return type;
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
        if(this.cache._cacheColumns[table]){
            return callback(null, this.cache._cacheColumns[table]) ;
        }
        this._query(`SELECT column_name, udt_name, character_maximum_length, numeric_precision, datetime_precision
                    FROM information_schema.columns t
                JOIN information_schema.tables t1 on t.table_name = t1.table_name
                    WHERE t.table_schema='public'
                    AND t1.table_type = 'BASE TABLE' AND t.table_name = $1
                    order by t.table_name, ordinal_position
                    `, [table], (err, result)=>{
            if(err){ return callback(err); }

            this.cache._cacheColumns[table] = result.rows ;
            callback(null, this.cache._cacheColumns[table]) ;
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
        if(this.cache._cachePk[table]){
            return callback(null, this.cache._cachePk[table]) ;
        }
        this._query(`select kc.column_name 
                    from  
                        information_schema.table_constraints tc
                        JOIN information_schema.key_column_usage kc ON kc.table_name = tc.table_name and kc.table_schema = tc.table_schema
                        and kc.constraint_name = tc.constraint_name
                        JOIN information_schema.tables t on tc.table_name = t.table_name
                    where 
                        tc.constraint_type = 'PRIMARY KEY' AND tc.table_name = $1
                    `, [table], (err, result)=>{
            if(err){ return callback(err); }

            this.cache._cachePk[table] = result.rows.map((r)=>{
                return r.column_name ;
            }) ;
            callback(null, this.cache._cachePk[table]) ;
        });
    }

    /**
     * Execute the schema changes and update the version number
     * @param {VeloxSqlChange[]} changes - Array of changes
     * @param {number} newVersion - The new database version
     * @param {function(err)} callback - called when finish
     */
    runQueriesAndUpdateVersion(changes, newVersion, callback){
        let job = new AsyncJob(AsyncJob.SERIES) ;
        for(let change of changes){
            if(change.run){
                //this change is a function that must be executed
                job.push((cb)=>{
                    
                    change.run(this, cb) ;
                }) ;
            } else {
                //this change is a SQL query to run
                job.push((cb)=>{
                    this._query(change.sql, change.params, cb) ;
                }) ;
            }
        }
        job.push((cb)=>{
            this._query(`UPDATE ${DB_VERSION_TABLE} SET version = $1, last_update = now()`, [newVersion], cb) ;
        }) ;
        job.async(callback) ;
    }

    clone(){
        return new VeloxDbPgClient(this.connection, function(){}, this.logger, this.cache, this.schema, this.customInit) ;
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
			
        var tx = this.clone() ;
        tx.transaction = function(){ throw "You should not start a transaction in a transaction !"; } ;
            
		this.connection.query("BEGIN", (err) => {
            if(err){
                finished = true ;
                return callbackDone(err);
            }
            
            var timeoutId = null;
            if(timeout > 0){
                timeoutId = setTimeout(()=>{
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
        this.cache = {} ;
        this.schema = options.schema || {} ;
        this.customClientInit = options.customClientInit || [] ;
    }

    /**
     * Get a database connection from the pool
     * 
     * @param {function(Error, VeloxDbPgClient)} callback - Callback with VeloxDbPgClient instance
     */
    open(callback){
        this.pool.connect((err, client, done) => {
            if(err){ return callback(err); }

            let dbClient = new VeloxDbPgClient(client, done, this.logger, this.cache, this.schema, this.customClientInit) ;
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