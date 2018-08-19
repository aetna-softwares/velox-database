const uuid = require("uuid") ;
const fs = require('fs-extra');
const path = require('path');
const multiparty = require('multiparty');
const crypto = require('crypto');

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
class VeloxBinarySync{

    /**
     * @typedef VeloxBinarySyncOption
     * @type {object}
     * @property {string} [appName] The application name (use in error mail)
     * @property {string} [emailAlert] Send alert by email on sync error. Possible values are : none (default if no adress given), immediate, hourly (default if adress given), daily
     * @property {string} [emailAddressFrom] Email address to send alerts
     * @property {string} [emailAddressTo] Email address to send alerts
     * @property {Array} [maskedColumns] Masked columns that should not be tracked in sync (typically passwords that we don't wan't to track)
     * @property {string} [syncEndPoint] the endpoint to sync (default /binarySync)
     * @property {string} pathStorage The path storage to put binary files
     * @property {string} [pathPattern] The pattern to construct path of binary file (default : {table}/{date}/{table_uid}_{uid}_{datehour_sync}_{sync_uid}_{ext})
     */

    /**
     * Create the VeloxSqlSync extension
     * 
     * @example
     * 
     * @param {VeloxBinarySyncOption} [options] options 
     */
    constructor(options){
        this.name = "VeloxBinarySync";

        if(!options){
            throw "You must give options to VeloxBinarySync instance" ;
        }
        if(!options.pathStorage){
            throw "You must give option pathStorage to VeloxBinarySync instance" ;
        }

        this.pathStorage = options.pathStorage ;
        this.pathStorageTemp = path.join(this.pathStorage, "temp") ;
        this.pathPattern = options.pathPattern || "{table}/{date}/{table_uid}_{uid}_{datehour_sync}_{sync_uid}_{ext}" ;

        this.appName = options.appName ;
        
        this.emailAddressFrom = options.emailAddressFrom ;
        this.emailAddressTo = options.emailAddressTo ;

        if(options.emailAddressTo){
            this.emailAlert = options.emailAlert||"hourly" ;
        }else{
            this.emailAlert = options.emailAlert||"none" ;
        }

        if(this.emailAlert !== "none" && !this.emailAddressFrom){
            throw "You must give a sender address email" ;
        }
        if(this.emailAlert !== "none" && !this.emailAddressTo){
            throw "You must give a destination address email" ;
        }

        this.dependencies = [] ;
        var self = this ;
        this.extendsProto = {
            binarySync : function(binaryRecord, file, checksum, action, context, callback){
                //this is the VeloxDatabase object
                self.binarySync(this, binaryRecord, file, checksum, action, context, callback) ;
            }
        } ;
        this.extendsExpressProto = {
            getBinarySyncMiddleware: function(){
                return (req, res)=>{
                    res.connection.setTimeout(0); //remove the 120s default timeout because client can take a long time to upload file
                    var form = new multiparty.Form();
                    form.parse(req, (err, fields, files) => {
                        if (err) {
                            this.db.logger.error("error parse request", err);
                            return res.status(500).json(err);
                        }

                        let action = null;
                        if(!fields.action || !fields.action[0]) {
                            this.db.logger.error("missing action");
                            return res.status(500).end("Missing action");
                        }
                        action = fields.action[0] ;
                        
                        let checksum = null;
                        if(!fields.checksum || !fields.checksum[0]) {
                            this.db.logger.error("missing checksum");
                            return res.status(500).end("Missing checksum");
                        }
                        checksum = fields.checksum[0] ;
                        
                        if(!fields.binaryRecord || !fields.binaryRecord[0]) {
                            this.db.logger.error("missing binaryRecord");
                            return res.status(500).end("Missing binaryRecord");
                        }
                        
                        let binaryRecord = null;
                        try{
                            binaryRecord = JSON.parse(fields.binaryRecord[0]) ;
                        }catch(err){
                            this.db.logger.error("error parse record", err);
                            return res.status(500).json(err);
                        }

                        if(action.indexOf("upload") === 0 && (!files.contents || !files.contents[0])) {
                            this.db.logger.error("missing contents");
                            return res.status(500).end("Missing contents");
                        }
                        let pathUpload = null;
                        if(files.contents && files.contents[0]){
                            pathUpload = files.contents[0].path;
                        } 

                        let context = {} ;
                        this._setContext(context, req) ;
                        this.db.binarySync(binaryRecord, pathUpload, checksum, action, context, (err, savedRecord)=>{
                            if(err){
                                this.db.logger.error("error when save binary", err);
                                return res.status(500).json(err) ;
                            }
                            if(pathUpload){
                                fs.unlink(pathUpload, (err)=>{
                                    if(err){ this.db.logger.warning("error when delete upload file", err); }
                                }) ;
                            }
                            res.json(savedRecord) ;
                        }) ;
                    }) ;
                } ;
            }
        } ;

        this.extendsExpressConfigure = [
            function(app){
                //this is the VeloxDatabaseExpress object
                app.post(options.syncEndPoint||"/binarySync", this.getBinarySyncMiddleware());
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
    binarySync(db, binaryRecord, file, checksum, action, context, callback){
        var syncUid = uuid.v4() ;
        if(!binaryRecord.creation_datetime){
            binaryRecord.creation_datetime = new Date() ;
        }
        db.transaction((tx, done)=>{
            tx.context = context.context ;
            tx.insert("velox_bin_sync_log", {
                uid       : syncUid,
                sync_date : new Date(),
                status    : "todo",
                binary_uid: binaryRecord.uid,
                checksum  : checksum,
                action    : action
            }, (err)=>{
                if(err){ return done(err) ;}
                return done(null, true) ;
            }) ;
        }, (err, shouldApplyChange) => {
            if(err){ return callback(err) ;}

            if(!shouldApplyChange){ return callback() ;}
            db.transaction((tx, doneTx)=>{
                tx.context = context.context ;
                let done = (error)=>{
                    if(error){
                        var strError = "" ;
                        if(error.stack){
                            strError = error.stack ;
                        }else{
                            strError = JSON.stringify(error) ;
                        }
                        tx.update("velox_bin_sync_log", {uid: syncUid, status: 'error', error_msg: strError}, (err)=>{
                            if(err){ return doneTx(err) ;}
                            if(this.emailAlert !== "none"){
                                var email = {
                                    uid: uuid.v4(),
                                    from_addr: this.emailAddressFrom,
                                    to_addr: this.emailAddressTo,
                                    subject: "["+this.appName+"] Binary sync error report",
                                    text: "The following errors happens :\n\n",
                                    html: "The following errors happens :<br /><br />",
                                };
                                if(this.emailAlert === "immediate"){
                                    email.schedule_type = "now";
                                    email.status = "tosend";
                                }else if(this.emailAlert === "hourly"){
                                    email.schedule_type = "later";
                                    email.status = "tosend";
                                    var now = new Date() ;
                                    email.schedule_date = new Date(now.getFullYear(), now.getMonth(), now.getDate(), now.getHours()+1, 0, 0, 0);
                                }else if(this.emailAlert === "daily"){
                                    email.schedule_type = "later";
                                    email.status = "tosend";
                                    var now = new Date() ;
                                    email.schedule_date = new Date(now.getFullYear(), now.getMonth(), now.getDate()+1, 0, 0, 0, 0);
                                }
                                tx.searchFirst("velox_mail", {status: "tosend", subject: email.subject}, (err, foundEmail)=>{
                                    if(err){ return doneTx(err) ;}
                                    if(foundEmail){
                                        email = foundEmail ;
                                    }
            
                                    email.text += "----------------------------------\n" ;
                                    email.text += "Date : "+new Date()+"\n";
                                    email.text += "Error : "+JSON.stringify(error)+"\n";
                                    email.text += "----------------------------------\n" ;
                                    email.html = email.text.replace(/\n/g, "<br />") ;
            
                                    if(foundEmail){
                                        tx.update("velox_mail", email, doneTx) ;
                                    }else{
                                        tx.insert("velox_mail", email, doneTx) ;
                                    }
                                }) ;
                            }else{
                                //no email alert
                                doneTx() ;
                            }
                        }) ;
                    }else{
                        tx.update("velox_bin_sync_log", {uid: syncUid, status: 'done'}, doneTx) ;
                    }
                } ;
                if(file){
                    //reveived expected checksum, save the file in sync storage
                    let finalPath = this.createTargetPath(binaryRecord, syncUid) ;
                    fs.copy(file, path.join(this.pathStorage, finalPath), {overwrite: true}, (err)=>{
                        if(err){ 
                            //move failed, rollback transaction
                            return done(err);
                        } 
                        this.checksum(file, (err, computeChecksum) => {
                            if(err){ return done(err) ;}
                            if(computeChecksum !== checksum){
                                return done("Wrong received checksum "+ computeChecksum+" expected "+checksum) ;
                            }
                            if(action.indexOf("upload") === 0){
                                tx.saveBinary(binaryRecord, file, (err, binaryRecord)=>{
                                    if(err){ return done(err) ;}
                                    done(null, binaryRecord) ;
                                }) ;
                            }else{
                                done(null, binaryRecord) ;
                            }
                        }) ;
                    }) ;
                }else{
                    done(null, binaryRecord) ;
                }
            }, callback) ;
        });
    }


    /**
     * Create the target path from the configured pattern 
     * 
     * @param {object} binaryRecord the binary record
     * @param {string} syncUid the binary record
     */
    createTargetPath(binaryRecord, syncUid){
        var targetPath = this.pathPattern ;
        //{table}/{date}/{table_uid}_{uid}_{datehour_sync}_{sync_uid}_{ext}
        targetPath = targetPath.replace(new RegExp("{table}", "g"), binaryRecord.table_name || "no_table") ;
        targetPath = targetPath.replace(new RegExp("{table_uid}", "g"), binaryRecord.table_uid || "no_uid") ;
        targetPath = targetPath.replace(new RegExp("{uid}", "g"), binaryRecord.uid) ;
        targetPath = targetPath.replace(new RegExp("{ext}", "g"), binaryRecord.filename?path.extname(binaryRecord.filename):"") ;
        targetPath = targetPath.replace(new RegExp("{date}", "g"), binaryRecord.creation_datetime.toISOString().substring(0,10)) ;
        targetPath = targetPath.replace(new RegExp("{time}", "g"), binaryRecord.creation_datetime.toISOString().substring(11,19).replace(/:/g, "_")) ;
        var dateHour = new Date() ;
        dateHour = dateHour.toISOString().substring(0,10)+"_"+dateHour.toISOString().substring(11,19).replace(/:/g, "_") ;
        targetPath = targetPath.replace(new RegExp("{datehour_sync}", "g"), dateHour) ;
        targetPath = targetPath.replace(new RegExp("{sync_uid}", "g"), syncUid) ;
        return targetPath ;
    }

    /**
     * Compute the checksum of the file
     * 
     * @param {string} filePath path of the file
     * @param {function} callback called with the checksum
     */
    checksum(filePath, callback){
        var digest = crypto.createHash("sha256");
        var stream = fs.createReadStream(filePath);
        stream.on("data", function(d) {digest.update(d);});
        stream.on("error", function (error) {return callback(error);});
        stream.on("end", function() {
            var hex = digest.digest("hex");
            return callback(null, hex);
        });
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
            sql: this.getCreateTableSyncLog(backend)
        }) ;
        
        
        return changes;
    }

    /**
     * Create the table velox_sync_log if not exists
     * @param {string} backend 
     */
    getCreateTableSyncLog(backend){
        let lines = [
            "uid VARCHAR(40) PRIMARY KEY",
            "binary_uid VARCHAR(40)",
            "checksum VARCHAR(128)",
            "sync_date timestamp without time zone",
            "data JSONB",
            "status VARCHAR(20)",
            "action VARCHAR(30)",
            "error_msg TEXT"
        ] ;
        if(backend === "pg"){
            return `
            CREATE TABLE IF NOT EXISTS velox_bin_sync_log (
                ${lines.join(",")}
            )
            ` ;
        }
        throw "not implemented for backend "+backend ;
    }
}

module.exports = VeloxBinarySync ;