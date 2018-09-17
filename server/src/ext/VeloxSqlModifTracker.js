const AsyncJob = require("velox-commons/AsyncJob") ;

/**
 * This extension create a table modification tracker.
 * 
 * It maintain the following information : 
 * 
 * In the table velox_modif_table_version, it keep track of the table version (sequential number that is incremented each time a insert or update is done in the table)
 * and last table modification date
 * 
 * In the table velox_modif_track, it keep track of the modified field in each tracked table with : 
 *  - the record version of this modification
 *  - the table version of this modification
 *  - the date of modification
 *  - the user of modification
 *  - the id of record modifier
 *  - the modified column
 *  - the value before modification
 *  - the value after modification
 * 
 * In each tracked table, the following column are added : 
 *  - velox_version_record : the record version (start to 0 at first insert)
 *  - velox_version_table : the table version at the moment of last record modification
 *  - velox_version_date : the date of last modification
 *  - velox_version_user : the user who did the last modification
 * 
 * Everything is managed automatically by database trigger so even manual database modification are tracked
 * 
 * When you do an update query, you must set the velox_version_user with the username that did the modification (if you don't give it, it will works but the user information will be empty)
 * You can also force the velox_version_date with a date
 * 
 * @example
 * //To use this extension, just register it on VeloxDatabase
 * const VeloxDatabase = require("");
 * const VeloxSqlModifTracker = require("");
 * 
 * VeloxDatabase.registerExtension(new VeloxSqlModifTracker()) ;
 * 
 */
class VeloxSqlModifTracker{

    /**
     * @typedef VeloxSqlModifTrackerOption
     * @type {object}
     * @property {function|Array|object} [tablesToTrack] the table to track configuration. If not given all tables are tracked.
     *  it can be :
     *   - a function that take the table name as argument and return true/false
     *   - an array of table to track
     *   - an object {include: []} where include is array of tables to track
     *   - an object {exclude: []} where exclude is array of tables we should not track
     */

    /**
     * Create the VeloxSqlModifTracker extension
     * 
     * @example
     * 
     * //track all tables
     * new VeloxSqlModifTracker();
     * 
     * //track all tables which name contains "trackme"
     * new VeloxSqlModifTracker({ tablesToTrack : (table)=>{ return table.indexOf("trackme") !== -1 ; } });
     * 
     * //track tables table1 and table2
     * new VeloxSqlModifTracker({ tablesToTrack : ["table1", "table2"] });
     * new VeloxSqlModifTracker({ tablesToTrack : { include : ["table1", "table2"] } });
     * 
     * //track all table but table1
     * new VeloxSqlModifTracker({ tablesToTrack : { exclude : ["table1"] } });
     * 
     * @param {VeloxSqlModifTrackerOption} [options] options 
     */
    constructor(options){
        this.name = "VeloxSqlModifTracker";
        this._tablesToTrack = ()=>{ return true; } ;
        if(options && options.tablesToTrack){
            if(typeof(options.tablesToTrack) === "function"){
                this._tablesToTrack = options.tablesToTrack ;
            }else if(Array.isArray(options.tablesToTrack)){
                this._tablesToTrack = (t)=>{return options.tablesToTrack.indexOf(t) !== -1 ;} ;
            }else if(options.tablesToTrack.include && Array.isArray(options.tablesToTrack.include)){
                this._tablesToTrack = (t)=>{return options.tablesToTrack.include.indexOf(t) !== -1 ;} ;
            }else if(options.tablesToTrack.exclude && Array.isArray(options.tablesToTrack.exclude)){
                this._tablesToTrack = (t)=>{return options.tablesToTrack.exclude.indexOf(t) === -1 ;} ;
            }else{
                throw "incorrect tablesToTrack option. If should be a function receiving table name and return true to track, "+
                "or an array of tables to track or an object {include: []} containing tables to track"+
                "or an object {exclude: []} containing tables not to track" ;
            }
        }

        this.interceptClientQueries = [
            {name : "insert", before: function(table, record, callback){
                var db = this;
                var currentUserId = db.context?db.context.req.user.uid:null ;
                record.velox_version_user = currentUserId ;
                callback();
            } },
            {name : "update", before: function(table, record, callback){
                var db = this;
                var currentUserId = db.context?db.context.req.user.uid:null ;
                record.velox_version_user = currentUserId ;
                callback();
            } },
        ] ;
    }

    tablesToTrack(table){
        if(table.indexOf("velox_") === 0){
            if(["velox_user_profile", "velox_user_realm", "velox_user", "velox_link_user_profile", "velox_binary", "velox_map"].indexOf(table) === -1){
                return false;
            }
        }
        return this._tablesToTrack(table) ;
    }

    /**
     * Add needed schema changes on schema updates
     * 
     * @param {string} backend 
     */
    addSchemaChanges(backend){
        if(["pg"].indexOf(backend) === -1){
            throw "Backend "+backend+" not handled by this extension" ;
        }

        let changes = [] ;

        changes.push({
            sql: this.getCreateTableVersion(backend)
        }) ;
        changes.push({
            sql: this.getCreateTableModifTrack(backend)
        }) ;

        // var tablesToUpdate = [] ;

        changes.push({
            run: (tx, cb)=>{
                this.addColumnToTables(backend, tx, "velox_version_record", this.getTypeBigInt(backend), cb);
                // this.getTablesMissingColumn(backend, "velox_version_record", function(err, result){
                //     if(err){ return cb(err) ; }
                //     tablesToUpdate = result.rows.map((r)=>{ return r.table_name ;}) ;
                // }) ;
            }
        }) ;
        changes.push({
            run: (tx, cb)=>{
                this.addColumnToTables(backend, tx, "velox_version_table", this.getTypeBigInt(backend), cb);
            }
        }) ;
        changes.push({
            run: (tx, cb)=>{
                this.addColumnToTables(backend, tx, "velox_version_date", this.getTypeTimestamp(backend), cb);
            }
        }) ;
        changes.push({
            run: (tx, cb)=>{
                this.addColumnToTables(backend, tx, "velox_version_user", this.getTypeVarchar(backend, 128), cb);
            }
        }) ;

        changes.push({
            run: (tx, cb)=>{
                this.createTriggerForTables(backend, tx, this.createTriggerBeforeUpdate.bind(this), cb);
            }
        }) ;

        changes.push({
            run: (tx, cb)=>{
                this.createTriggerForTables(backend, tx, this.createTriggerBeforeInsert.bind(this), cb);
            }
        }) ;

        // changes.push({
        //     run: (tx, cb)=>{
        //         let alertJob = new AsyncJob(AsyncJob.SERIES) ;
        //         for(let t of tablesToUpdate){
        //             alertJob.push((cb)=>{
        //                 tx._query(`UPDATE ${t} SET velox_version_date = now()`, cb) ;
        //             }) ;
        //         }
        //         alertJob.async(cb) ;
        //     }
        // }) ;

        return changes;
    }

    /**
     * Add needed column to tracked tables if they not having it yet
     * @param {string} backend 
     * @param {object} tx 
     * @param {string} columnName 
     * @param {string} columnDef 
     * @param {function(err)} callback 
     */
    addColumnToTables(backend, tx, columnName, columnDef, callback){
         tx._query(this.getTablesMissingColumn(backend, columnName), (err, result)=>{
            if(err){ return callback(err); }
             
            let alertJob = new AsyncJob(AsyncJob.SERIES) ;
            for(let r of result.rows){
                if(this.tablesToTrack(r.table_name)) {
                    alertJob.push((cb)=>{
                        tx._query(this.getAlterAddColumn(backend, r.table_name, columnName, columnDef), cb) ;
                    }) ;
                }
            }
            alertJob.async(callback) ;
         }) ;
    }

    /**
     * Create trigger on tracked tables
     * 
     * @param {string} backend 
     * @param {object} tx 
     * @param {function} triggerCreateFunc the function that do the trigger creation for a table
     * @param {function(Error)} callback 
     */
    createTriggerForTables(backend, tx, triggerCreateFunc, callback){
         tx._query(this.getAllTables(backend), (err, result)=>{
            if(err){ return callback(err); }
             
            let alertJob = new AsyncJob(AsyncJob.SERIES) ;
            for(let r of result.rows){
                if(this.tablesToTrack(r.table_name)) {
                    alertJob.push((cb)=>{
                        triggerCreateFunc(backend, tx, r.table_name, cb) ;
                    }) ;
                }
            }
            alertJob.async(callback) ;
         }) ;
    }

    /**
     * Get the BIGINT type for this backend
     * @param {string} backend 
     */
    getTypeBigInt(backend){
        if(backend === "pg"){
            return "bigint not null default 0";
        }
        throw "not implemented for backend "+backend ;
    }

    /**
     * Get the VARCHAR type for this backend
     * @param {string} backend 
     */
    getTypeVarchar(backend, size){
        if(backend === "pg"){
            return "varchar("+size+")";
        }
        throw "not implemented for backend "+backend ;
    }

    /**
     * Get the TIMESTAMP WITHOUT TIME ZONE type for this backend
     * @param {string} backend 
     */
    getTypeTimestamp(backend){
        if(backend === "pg"){
            return "timestamp without time zone default now()";
        }
        throw "not implemented for backend "+backend ;
    }

    /**
     * Create the table velox_modif_table_version if not exists
     * @param {string} backend 
     */
    getCreateTableVersion(backend){
        if(backend === "pg"){
            return `
            CREATE TABLE IF NOT EXISTS velox_modif_table_version (
                table_name VARCHAR(128) PRIMARY KEY,
                version_table bigint,
                force_refresh bigint,
                version_date timestamp without time zone
            )
            ` ;
        }
        throw "not implemented for backend "+backend ;
    }

    /**
     * Create the table velox_modif_track if not exists
     * @param {string} backend 
     */
    getCreateTableModifTrack(backend){
        if(backend === "pg"){
            return `
            CREATE TABLE IF NOT EXISTS velox_modif_track (
                version_record bigint,
                version_table bigint,
                version_date timestamp without time zone,
                version_user varchar(128),
                table_name varchar(128),
                table_uid varchar(255),
                column_name varchar(128),
                column_before TEXT,
                column_after TEXT,
                realm_code varchar(30),
                user_uid varchar(40),
                PRIMARY KEY (table_name, table_uid, version_table, version_record, version_date, column_name)
            )
            ` ;
        }
        throw "not implemented for backend "+backend ;
    }

    /**
     * Create a sequence if it does not exists
     * 
     * @param {string} backend 
     * @param {object} tx 
     * @param {string} name name of the sequence
     * @param {function(Error)} callback 
     */
    createSequenceIfNotExists(backend, tx, name, callback){
        if(backend === "pg"){
            tx._query(`SELECT c.relname FROM pg_class c WHERE c.relkind = 'S' and relname=$1`,[name], (err, result)=>{
                if(err){ return callback(err); }
                if(result.rows.length > 0){
                    return callback() ;//already exists
                }
                //create
                tx._query(`CREATE SEQUENCE ${name} START 1`, callback) ;
            }) ;
        } else {
            callback("not implemented for backend "+backend) ;
        }
    }

    /**
     * Create the trigger on before update on all tracked tables
     * 
     * @param {string} backend 
     * @param {object} tx 
     * @param {string} table table name
     * @param {function(Error)} callback 
     */
    createTriggerBeforeUpdate(backend, tx, table, callback){
        if(backend === "pg"){
            tx._query(`DROP TRIGGER IF EXISTS trig_velox_modiftrack_${table}_onupdate ON ${table}`, (err)=>{
                if(err){ return callback(err); }

                this.createSequenceIfNotExists(backend, tx, `velox_modiftrack_table_version_${table}`, (err)=>{
                    if(err){ return callback(err); }

                    tx._query("select column_name from information_schema.columns where table_name=$1", [table], (err, result)=>{
                        if(err){ return callback(err); }

                        let columns = result.rows.map((r)=>{return r.column_name;}).filter((c)=>{
                            return c.indexOf("velox_") !== 0 ;
                        }) ;

                        tx._query(`select kc.column_name 
                            from  
                                information_schema.table_constraints tc
                                JOIN information_schema.key_column_usage kc ON kc.table_name = tc.table_name and kc.table_schema = tc.table_schema
                                and kc.constraint_name = tc.constraint_name
                                JOIN information_schema.tables t on tc.table_name = t.table_name
                            where 
                                tc.constraint_type = 'PRIMARY KEY' AND tc.table_name = $1
                            `, [table], (err, result)=>{
                            if(err){ return callback(err); }

                            if(result.rows.length === 0){
                                //no primary key, assume the primary key is composed of all columns
                                result.rows = columns ;
                                //return callback("Table "+table+" doesn't have any primary key, can't use modification track") ;
                            }

                            let pkInOld = result.rows.map(function(pk){
                                return "OLD."+pk.column_name ;
                            }).join(" || '$_$' || ") ;

                            let trig = `CREATE OR REPLACE FUNCTION func_velox_modiftrack_${table}_onupdate() RETURNS trigger AS 
                            $$
                                DECLARE table_version BIGINT;
                                DECLARE found_version BIGINT;
                                BEGIN 

                                -- always increment record version 
                                IF OLD.velox_version_record IS NULL THEN
                                    NEW.velox_version_record = 1;
                                ELSE
                                    NEW.velox_version_record = OLD.velox_version_record + 1;
                                END IF ;

                                -- increment global table version
                                SELECT nextval('velox_modiftrack_table_version_${table}') INTO table_version ;

                                -- update information in global version_table
                                SELECT version_table INTO found_version FROM velox_modif_table_version WHERE table_name = '${table}';
                                IF NOT FOUND THEN
                                    INSERT INTO velox_modif_table_version(table_name, version_table, version_date) VALUES 
                                    ('${table}', table_version, now()) ;
                                ELSE
                                    UPDATE velox_modif_table_version SET version_table=table_version, version_date=now() WHERE table_name='${table}' ;
                                END IF;

                                -- keep global table version on record
                                NEW.velox_version_table = table_version ;

                                IF OLD.velox_version_date = NEW.velox_version_date THEN
                                    -- the version date has not been manually modified, set it to now
                                    NEW.velox_version_date = now() ;
                                END IF ;
                                ` ;
                            for(let c of columns){
                                trig += `
                                -- save all modifications in tracking table
                                IF OLD."${c}"::text <> NEW."${c}"::text THEN
                                    INSERT INTO velox_modif_track (version_record, version_table, version_date, version_user, table_name, table_uid, column_name, column_before, column_after)
                                    VALUES (NEW.velox_version_record, table_version, NEW.velox_version_date, NEW.velox_version_user, '${table}', ${pkInOld}, '${c}', OLD."${c}", NEW."${c}") ;
                                END IF ;
                                ` ;
                            }
                            trig += `
                            RETURN NEW;
                            END; 
                            $$ 
                            LANGUAGE 'plpgsql'` ;
                            
                            tx._query(trig, (err)=>{
                                if(err){ return callback(err); }
                                tx._query(`CREATE TRIGGER trig_velox_modiftrack_${table}_onupdate BEFORE UPDATE ON ${table} 
                                FOR EACH ROW EXECUTE PROCEDURE func_velox_modiftrack_${table}_onupdate()`, (err)=>{
                                    if(err){ return callback(err); }
                                    callback() ;
                                }) ;
                            }) ;
                        }) ;
                    }) ;
                }) ;
            }) ;
        }else{
            throw callback("not implemented for backend "+backend) ;
        }
    }

    /**
     * Create the trigger on before insert on tracked table
     * @param {string} backend 
     * @param {object} tx 
     * @param {string} table table name
     * @param {function(Error)} callback 
     */
    createTriggerBeforeInsert(backend, tx, table, callback){
        if(backend === "pg"){
            tx._query(`DROP TRIGGER IF EXISTS trig_velox_modiftrack_${table}_oninsert ON ${table}`, (err)=>{
                if(err){ return callback(err); }
                let trig = `CREATE OR REPLACE FUNCTION func_velox_modiftrack_${table}_oninsert() RETURNS trigger AS 
                $$
                    DECLARE table_version BIGINT;
                    DECLARE found_version BIGINT;
                    BEGIN 

                    -- always increment record version
                    NEW.velox_version_record = 0;

                    -- increment global table version
                    SELECT nextval('velox_modiftrack_table_version_${table}') INTO table_version ;

                    -- update information in global version_table
                    SELECT version_table INTO found_version FROM velox_modif_table_version WHERE table_name = '${table}';
                    IF NOT FOUND THEN
                        INSERT INTO velox_modif_table_version(table_name, version_table, version_date) VALUES 
                        ('${table}', table_version, now()) ;
                    ELSE
                        UPDATE velox_modif_table_version SET version_table=table_version, version_date=now() WHERE table_name='${table}' ;
                    END IF;

                    -- keep global table version on record
                    NEW.velox_version_table = table_version ;

                    IF NEW.velox_version_date IS NULL THEN
                        -- the version date has not been manually modified, set it to now
                        NEW.velox_version_date = now() ;
                    END IF ;
                    
                    RETURN NEW;
                END; 
                $$ 
                LANGUAGE 'plpgsql'` ;
                
                tx._query(trig, (err)=>{
                    if(err){ return callback(err); }
                    tx._query(`CREATE TRIGGER trig_velox_modiftrack_${table}_oninsert BEFORE INSERT ON ${table} 
                    FOR EACH ROW EXECUTE PROCEDURE func_velox_modiftrack_${table}_oninsert()`, (err)=>{
                        if(err){ return callback(err); }
                        callback() ;
                    }) ;
                }) ;
            }) ;
        }else{
            throw callback("not implemented for backend "+backend) ;
        }
    }

    /**
     * Get the list of table that does not have this column yet
     * 
     * @param {string} backend 
     * @param {string} columnName 
     */
    getTablesMissingColumn(backend, columnName){
        if(backend === "pg"){
            return `
                SELECT table_name FROM information_schema.tables WHERE table_name NOT IN
                (
                    SELECT distinct t.table_name
                    FROM information_schema.columns t
                JOIN information_schema.tables t1 on t.table_name = t1.table_name
                    WHERE t.table_schema='public'
                    AND column_name = '${columnName}'
                    AND t1.table_type = 'BASE TABLE'
                ) AND table_type = 'BASE TABLE' AND table_schema='public'
            ` ;
        }
        throw "not implemented for backend "+backend ;
    }

    /**
     * Get all tables
     * 
     * @param {string} backend 
     */
    getAllTables(backend){
        if(backend === "pg"){
            return `
                SELECT table_name FROM information_schema.tables 
                WHERE table_type = 'BASE TABLE' 
                AND table_schema='public'
            ` ;
        }
        throw "not implemented for backend "+backend ;
    }

    /**
     * Get alter table syntax for this backend
     * 
     * @param {string} backend 
     * @param {string} table 
     * @param {string} columnName 
     * @param {string} columnDef 
     */
    getAlterAddColumn(backend, table, columnName, columnDef){
        if(backend === "pg"){
            return `
                ALTER TABLE ${table} ADD COLUMN ${columnName} ${columnDef}
            ` ;
        }
        throw "not implemented for backend "+backend ;
    }

   
}

module.exports = VeloxSqlModifTracker;