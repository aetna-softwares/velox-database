const AsyncJob = require("../../../../helpers/AsyncJob") ;

/**
 * This extension create a table delete tracker.
 * 
 * It maintain the following information : 
 * 
 * In the table velox_modif_table_version, it keep track of the table version (sequential number that is incremented each time a insert or update is done in the table)
 * and last table modification date
 * 
 * In the table velox_delete_track, it keep track of the deleted records with : 
 *  - the table version when the delete happens
 *  - the date of delete
 *  - the id of deleted record
 * 
 * Everything is managed automatically by database trigger so even manual database delete are tracked
 * 
 * Note : this is do not do a backup of deleted record ! it only track that a record has been deleted. 
 * The primary target was to have the deletion information for sync process
 * 
 * @example
 * //To use this extension, just register it on VeloxDatabase
 * const VeloxDatabase = require("");
 * const VeloxSqlDeleteTracker = require("");
 * 
 * VeloxDatabase.registerExtension(new VeloxSqlDeleteTracker()) ;
 * 
 */
class VeloxSqlDeleteTracker{

    /**
     * @typedef VeloxSqlDeleteTrackerOption
     * @type {object}
     * @property {function|Array|object} [tablesToTrack] the table to track configuration. If not given all tables are tracked.
     *  it can be :
     *   - a function that take the table name as argument and return true/false
     *   - an array of table to track
     *   - an object {include: []} where include is array of tables to track
     *   - an object {exclude: []} where exclude is array of tables we should not track
     */

    /**
     * Create the VeloxSqlDeleteTracker extension
     * 
     * @example
     * 
     * //track all tables
     * new VeloxSqlDeleteTracker();
     * 
     * //track all tables which name contains "trackme"
     * new VeloxSqlDeleteTracker({ tablesToTrack : (table)=>{ return table.indexOf("trackme") !== -1 ; } });
     * 
     * //track tables table1 and table2
     * new VeloxSqlDeleteTracker({ tablesToTrack : ["table1", "table2"] });
     * new VeloxSqlDeleteTracker({ tablesToTrack : { include : ["table1", "table2"] } });
     * 
     * //track all table but table1
     * new VeloxSqlDeleteTracker({ tablesToTrack : { exclude : ["table1"] } });
     * 
     * @param {VeloxSqlDeleteTrackerOption} [options] options 
     */
    constructor(options){
        this.name = "VeloxSqlDeleteTracker";
        this.tablesToTrack = ()=>{ return true; } ;
        if(options && options.tablesToTrack){
            if(typeof(options.tablesToTrack) === "function"){
                this.tablesToTrack = options.tablesToTrack ;
            }else if(Array.isArray(options.tablesToTrack)){
                this.tablesToTrack = (t)=>{return options.tablesToTrack.indexOf(t) !== -1 ;} ;
            }else if(options.tablesToTrack.include && Array.isArray(options.tablesToTrack.include)){
                this.tablesToTrack = (t)=>{return options.tablesToTrack.include.indexOf(t) !== -1 ;} ;
            }else if(options.tablesToTrack.exclude && Array.isArray(options.tablesToTrack.exclude)){
                this.tablesToTrack = (t)=>{return options.tablesToTrack.exclude.indexOf(t) === -1 ;} ;
            }else{
                throw "incorrect tablesToTrack option. If should be a function receiving table name and return true to track, "+
                "or an array of tables to track or an object {include: []} containing tables to track"+
                "or an object {exclude: []} containing tables not to track" ;
            }
        }
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
            sql: this.getCreateTableDeleteTrack(backend)
        }) ;
        
        changes.push({
            run: (tx, cb)=>{
                this.createTriggerForTables(backend, tx, this.createTriggerBeforeDelete.bind(this), cb);
            }
        }) ;

        return changes;
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
         tx.query(this.getAllTables(backend), (err, result)=>{
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
     * Create the table velox_modif_table_version if not exists
     * @param {string} backend 
     */
    getCreateTableVersion(backend){
        if(backend === "pg"){
            return `
            CREATE TABLE IF NOT EXISTS velox_modif_table_version (
                table_name VARCHAR(128),
                version_table bigint,
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
    getCreateTableDeleteTrack(backend){
        if(backend === "pg"){
            return `
            CREATE TABLE IF NOT EXISTS velox_delete_track (
                version_table bigint,
                delete_date timestamp without time zone,
                table_name varchar(128),
                table_uid varchar(128)
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
            tx.query(`SELECT c.relname FROM pg_class c WHERE c.relkind = 'S' and relname=$1`,[name], (err, result)=>{
                if(err){ return callback(err); }
                if(result.rows.length > 0){
                    return callback() ;//already exists
                }
                //create
                tx.query(`CREATE SEQUENCE ${name} START 1`, callback) ;
            }) ;
        } else {
            callback("not implemented for backend "+backend) ;
        }
    }

    /**
     * Create the trigger on before delete on all tracked tables
     * 
     * @param {string} backend 
     * @param {object} tx 
     * @param {string} table table name
     * @param {function(Error)} callback 
     */
    createTriggerBeforeDelete(backend, tx, table, callback){
        if(backend === "pg"){
            tx.query(`DROP TRIGGER IF EXISTS trig_velox_modiftrack_${table}_ondelete ON ${table}`, (err)=>{
                if(err){ return callback(err); }

                this.createSequenceIfNotExists(backend, tx, `velox_modiftrack_table_version_${table}`, (err)=>{
                    if(err){ return callback(err); }

                    

                    tx.query(`select kc.column_name 
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
                            return callback("Table "+table+" doesn't have any primary key, can't use modification track") ;
                        }

                        if(result.rows.length > 1){
                            return callback("Table "+table+" have many primary keys, can't use modification track") ;
                        }

                        let pkName = result.rows[0].column_name ;

                        let trig = `CREATE OR REPLACE FUNCTION func_velox_modiftrack_${table}_ondelete() RETURNS trigger AS 
                        $$
                            DECLARE table_version BIGINT;
                            DECLARE found_version BIGINT;
                            BEGIN 

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

                            INSERT INTO velox_delete_track (version_table, delete_date, table_name, table_uid) VALUES 
                                (table_version, now(), '${table}', OLD."${pkName}") ;

                            
                            RETURN OLD;
                        END; 
                        $$ 
                        LANGUAGE 'plpgsql'` ;
                        
                        tx.query(trig, (err)=>{
                            if(err){ return callback(err); }
                            tx.query(`CREATE TRIGGER trig_velox_modiftrack_${table}_ondelete BEFORE DELETE ON ${table} 
                            FOR EACH ROW EXECUTE PROCEDURE func_velox_modiftrack_${table}_ondelete()`, (err)=>{
                                if(err){ return callback(err); }
                                callback() ;
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
     * Get all tables
     * 
     * @param {string} backend 
     */
    getAllTables(backend){
        if(backend === "pg"){
            return `
                SELECT table_name FROM information_schema.tables 
                WHERE table_name not like 'velox_%' 
                AND table_type = 'BASE TABLE' 
                AND table_schema='public'
            ` ;
        }
        throw "not implemented for backend "+backend ;
    }

    
}

module.exports = VeloxSqlDeleteTracker;