function getJoinTables(tables, joins){
    if(joins){
        joins.forEach(function(join){
            if(tables.indexOf(join.otherTable) === -1){
                tables.push(join.otherTable) ;
            }
            if(join.joins){
                getJoinTables(tables, join.joins);
            }
        });
    }
}

/**
 * This extension handle encrypt of columns
 * 
 */
class VeloxCryto{

    /**
     * @typedef VeloxCryptoColumnOptions
     * @type {object}
     * @property {string} name Column name
     * @property {string} [table] Table name (for example, if the column belong to a view, put the source table name)
     */
    
     /**
     * @typedef VeloxCryptoTableOptions
     * @type {object}
     * @property {string} name Table name
     * @property {VeloxCryptoColumnOptions[]} columns Translated columns
     */

     /**
     * @typedef VeloxCryptoOptions
     * @type {object}
     * 
     * @example
     * {
     *   tables : [
     *      {name : "foo", columns : [
     *          {name : "bar"}
     *      ]}
     *   ]
     * }
     * 
     * @property {VeloxCryptoTableOptions[]} tables the translated tables
     * @property {string} key the encryption key
     */

    /**
     * Create the VeloxI18n extension
     * 
     * @param {VeloxCryptoOptions} [options] options 
     */
    constructor(options){
        this.name = "VeloxCrypto";

        if(!options){
            options = {} ;
        }
        this.options = options ;
        this.key = options.key ;

        var self = this;
        
        this.extendsClient = {} ;
        this.interceptClientQueries = [];

        let beforeSearchHook = function(table, search, joinFetch, callback){
            let client = this;
            //let callback = arguments[arguments.length-1] ;
            self.beforeSearchHook(client, table, joinFetch, callback) ;
        } ;

        this.cryptedTables = {} ;
        for(let table of options.tables){
            this.cryptedTables[table.name] = true ;

            let cryptedColumns = {};
            for(let c of table.columns){
                cryptedColumns[c.name] = true ;
            }

            //For each crypted column, we add the crypting function on the fly when insert/update
            this.extendsClient["getColumnWrite_"+table.name] = function(table, column, paramNumber){
                if(cryptedColumns[column]){
                    return "PGP_SYM_ENCRYPT($"+paramNumber+","+this.key+")" ;
                }else{
                    return "$"+paramNumber ;
                }
            } ;

            //register the hook to decrypt on the fly
            this.interceptClientQueries.push({name : "getByPk", table: table.name, before : beforeSearchHook });
            this.interceptClientQueries.push({name : "searchFirst", table: table.name, before : beforeSearchHook });
            this.interceptClientQueries.push({name : "search", table: table.name, before : beforeSearchHook });
        }
    }

    /**
     * For each table concerned by a read (ie the table and the join tables), change the table columns to add
     * the decrypt on the fly
     */
    beforeSearchHook(client, tableP, joinFetch, callback){
        var tables = [] ;
        tables.push(tableP) ;
        getJoinTables(tables, joinFetch) ;
        if(tables.every((table)=>{ return !this.cryptedTables[table] || !!client["initCrypto"+table] ; })){ return callback() ; }
        let lang = client.getLang() ;
        if(lang === "base"){ return callback() ;}
        client.getSchema((err, schema)=>{
            if(err){ return callback(err) ;}
            for(let table of tables){
                if(!this.cryptedTables[table] || client["initCrypto"+table]){ continue ;}
                client["initCrypto"+table] = true ;
                let tableSql = client.getTable(table) ;
                let cryptedColumns = [] ;
                this.options.tables.some(function(t){
                    if(t.name === table){
                        cryptedColumns = t.columns.map(function(c){ return c.name ;});
                        return true;
                    }
                }) ;
                let notCryptedColumns = schema[table].columns.filter(function(c){
                    return cryptedColumns.indexOf(c.name) === -1;
                }).map(function(c){ return c.name ;}) ;
    
                let from = tableSql+" m " ;
                let columns = notCryptedColumns ;
                for(let col of cryptedColumns){
                    columns.push("PGP_SYM_DECRYPT(\""+col+"\"::bytea, '"+this.key+"') as \""+col+'"') ;
                }
                let cols = columns.join(",") ;
    
                let sql = `(SELECT ${cols} FROM ${from})` ;
                client["getTable_"+table] = function(){
                    return sql ;
                } ;
            }
            callback() ;
        }) ;
    }




    /**
     * Add needed schema changes on schema updates
     * 
     * @param {string} backend 
     */
    prependSchemaChanges(backend){
        if(["pg"].indexOf(backend) === -1){
            throw "Backend "+backend+" not handled by this extension" ;
        }

        let changes = [] ;

        changes.push({
            sql: "CREATE EXTENSION pgcrypto"
        }) ;

        for(let table of this.options.tables){
            for(let col of table.columns){
                changes.push({
                    run: (tx, cb)=>{
                        tx._query(`SELECT data_type FROM information_schema.columns WHERE table_name = $1
                          AND column_name = 2`,[table.name, col.name], (err, result)=>{
                            if(err){ return cb(err); }
                            if(result.rows.length === 0){
                                return cb("Column "+col.name+" does not exists in table "+table.name) ;
                            }
                            if(result.rows[0].data_type === "text"){
                                return cb(); //already transformed
                            }
                            tx._query(`ALTER TABLE ${table.name} ALTER COLUMN "${col.name}" TYPE TEXT`, (err) => {
                                if(err){ return cb(err); }
                                tx._query(`UPDATE ${table.name} SET "${col.name}" = PGP_SYM_ENCRYPT("${col.name}",${this.key})`, (err) => {
                                    if(err){ return cb(err); }
                                    cb() ;
                                }) ;
                            }) ;
                        }) ;
                    }
                }) ;
            }
        }
        
        return changes;
    }

   
}

module.exports = VeloxCryto;