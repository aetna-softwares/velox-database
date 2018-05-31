const AsyncJob = require("velox-commons/AsyncJob") ;
const VeloxSqlModifTracker = require("./VeloxSqlModifTracker");
const VeloxSqlDeleteTracker = require("./VeloxSqlDeleteTracker");
/**
 * Add changeset sync for database
 * 
 * The purpose is to support offline asynchroneous synchronizations with following scenario
 *  - user goes offline
 *  - user continue to do insert/update locally on his device, they are stored in changesets
 *  - when user come back online, he send his changeset to sync with distant database
 * 
 * The synchronization is done as following : 
 *  - if the record does not exists yet : insert it
 *  - if the record in database has lower version that the one of user, update it 
 *  - if the record in database has same or higher version, check the modified fields 
 *     - if the modified fields has not been modified
 *     - if the modified fields has been modified before this modification (case of an other user did modification after but sync before), apply this modification
 *     - if the modified fields has been modified after this modification, don't apply the modification on the field but keep track of the value in the modif_track table
 * 
 * This extension will automatically add VeloxSqlModifTracker and VeloxSqlDeleteTracker extensions
 * 
 */
class VeloxSqlSync{

    /**
     * @typedef VeloxSqlSyncOption
     * @type {object}
     * @property {function|Array|object} [tablesToTrack] the table to track configuration. If not given all tables are tracked.
     * @property {string} [syncGetTimeEndPoint] the endpoint to sync time (default /syncGetTime)
     * @property {string} [syncEndPoint] the endpoint to sync (default /sync)
     *  it can be :
     *   - a function that take the table name as argument and return true/false
     *   - an array of table to track
     *   - an object {include: []} where include is array of tables to track
     *   - an object {exclude: []} where exclude is array of tables we should not track
     */

    /**
     * Create the VeloxSqlSync extension
     * 
     * @example
     * 
     * @param {VeloxSqlSyncOption} [options] options 
     */
    constructor(options){
        this.name = "VeloxSqlSync";
        this.dependencies = [
            new VeloxSqlModifTracker(options),
            new VeloxSqlDeleteTracker(options)
        ] ;
        var self = this ;
        this.extendsProto = {
            syncChangeSet : function(changeSet, callback){
                //this is the VeloxDatabase object
                self.applyChangeSet(this.backend, changeSet, callback) ;
            }
        } ;
        this.extendsExpressProto = {
            getSyncMiddleware: function(){
                return (req, res)=>{
                    let changes = req.body.changes;
                    self.applyChangeSet(this.db, changes, (err, result)=>{
                        if (err) {
                            this.db.logger.error("sync failed : "+ err, changes);
                            return res.status(500).json(err);
                        }
                        res.json(result) ;
                    }) ;
                } ;
            },
            getSyncGetTimeMiddleware: function(){
                return (req, res)=>{
                    let date = req.body.date;
                    var clientDate = new Date(date) ;
                    var serverDate = new Date() ;
                    res.end(""+(serverDate.getTime() - clientDate.getTime())) ;
                } ;
            }
        } ;

        this.extendsExpressConfigure = [
            function(app){
                //this is the VeloxDatabaseExpress object
                app.post(options.syncGetTimeEndPoint||"/syncGetTime", this.getSyncGetTimeMiddleware());
                app.post(options.syncEndPoint||"/sync", this.getSyncMiddleware());
            }
        ] ;
    }



    /**
     * Apply a changeset in database. The change set is done in a single transaction
     * 
     * The change set format is :
     * {
     *  date : date of the modification
     *  timeGap : the gap between time of client that create the modification and the server
     *  changes : [
     *      table : table name
     *      record: {record to sync}
     *  ]
     * }
     * 
     * @param {object} changeSet the changeset to sync in database
     * @param {function(Error)} callback called on finished
     */
    applyChangeSet(db, changeSet, callback){

        var records = [] ;
        let job = new AsyncJob(AsyncJob.SERIES) ;
        let changeDateTimestampMilli = new Date(changeSet.date).getTime() ;
        let localTimeGap = changeSet.timeLapse ;
        changeDateTimestampMilli += localTimeGap ;
        db.inDatabase((client, done)=>{
            for(let change of changeSet.changes){
                job.push((cb)=>{
                    if(change.action === "remove"){
                        records.push(change);
                        cb();
                    }else if(change.action === "insert"){
                        records.push(change);
                        cb();
                    }else{
                        client.getByPk(change.table, change.record, (err, recordDb)=>{
                            if(err){ return cb(err); }
                            if(!recordDb){
                                //record does not exists yet, insert it
                                change.action = "insert" ;
                                records.push(change);
                                cb();
                            }else{
                                //record exist in database
                                if(recordDb.velox_version_record < change.record.velox_version_record){
                                    //record in database is older, update
                                    change.action = "update" ;
                                    records.push(change);
                                }else{
                                    //record in database is more recent, compare which column changed
        
                                    let changedColumns = Object.keys(change.record).filter((col)=>{
                                        return col.indexOf("velox_") !== 0 &&
                                            change.record[col] != recordDb[col]; //don't do !== on purpose because 1 shoud equals "1"
                                    }) ;
        
                                    if(changedColumns.length === 0){
                                        //no modifications to do, no need to go further
                                        return cb() ;
                                    }
        
                                    client.getPrimaryKey(change.table, (err, pkNames)=>{
                                        if(err){ return cb(err); }
                                        client.search("velox_modif_track", {
                                            table_name: change.table, 
                                            table_uid: pkNames[0], 
                                            version_record: {ope: ">", value: (change.record.velox_version_record || 0)-1}
                                        }, "version_record", (err, modifications)=>{
                                            if(err){ return cb(err); }
                                            
        
                                            for(let modif of modifications){
                                                let index = changedColumns.indexOf(modif.column_name);
                                                if(index !== -1){
                                                    //conflicting column
                                                    
                                                    let modifDateMilli = new Date(modif.version_date).getTime() ;
        
                                                    if(modifDateMilli <= changeDateTimestampMilli){
                                                        //the modif date is older that our new modification
                                                        //this can happen if 2 offline synchronize but the newest user synchronize after the oldest
                                                    }else{
                                                        //the modif date is newer, we won't change in the table but we must modify the modif track
                                                        // from oldval -> dbVal to oldval -> myVal -> dbVal
                                                        var oldestVal = modif.column_before;
                                                        var midWayVal = "" + change.record[modif.column_name] ;  
                                                        
                                                        //modifying existing modif by setting our change value as old value
                                                        modif.column_before = midWayVal ;
                                                        records.push({table : "velox_modif_track", action: "update", record: modif});
                                                        records.push({table : "velox_modif_track", action: "insert", record: {
                                                            version_date : new Date(changeDateTimestampMilli),
                                                            column_before : oldestVal,
                                                            column_after : midWayVal,
                                                            version_user : change.record.velox_version_user,
                                                            version_table : recordDb.version_table
                                                        }});
        
                                                        //remove from changed column
                                                        changedColumns.splice(index, 1) ;
                                                        //remove column from record
                                                        delete change.record[modif.column_name] ;
                                                    }
                                                }
                                            }
        
                                                
                                            if(changedColumns.length === 0){
                                                //no modifications left to do
                                                return cb() ;
                                            } else {
                                                // still some modification to do, apply them
                                                change.action = "update" ;
                                                records.push(change) ;
                                            }
                                        }) ;
                                    }) ;
                                }
                            }
                        }) ;
                    }
                });
            }
            job.async(done) ;

        }, function(err){
            if(err){ return callback(err) ;}
            db.transactionalChanges(records, callback) ;
        }) ;
    }

}

module.exports = VeloxSqlSync ;