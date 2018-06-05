
const uuid = require("uuid") ;

/**
 * This extension save crash reports in database
 * 
 * It create the following tables : 
 *  - velox_crash_report : crash report storage
 * 
 * Express configuration :
 * If you are using express automatic configuration, it will register 1 end point saveCrashReport
 * 
 * You can also handle express configuration yourself, in this case, this extension give you 2 helpers on the VeloxDatabaseExpress object :
 * getSaveCrashReport : give you a middleware to handle request to save binary file
 * 
 * @example
 * //To use this extension, just register it on VeloxDatabase
 * const VeloxDatabase = require("");
 * const VeloxBinaryStorage = require("");
 * 
 * VeloxDatabase.registerExtension(new VeloxCrashReport({})) ;
 * 
 */
class VeloxCrashReport{

    /**
     * @typedef VeloxCrashReportOptions
     * @type {object}
     * @property {string} [emailAlert] Send alert by email. Possible values are : none (default), immediate, hourly, daily
     * @property {string} [emailAddressFrom] Email address to send alerts
     * @property {string} [emailAddressTo] Email address to send alerts
     * @property {string} [saveEndPoint] The Express end point for crash saving (default : /saveCrashReport)
     */

    /**
     * Create the e extension
     * 
     * @param {VeloxCrashReportOptions} [options] options 
     */
    constructor(options){
        this.name = "e";

        if(!options){
            options = {} ;
        }

        this.emailAlert = options.emailAlert||"none" ;
        this.emailAddressFrom = options.emailAddressFrom ;
        this.emailAddressTo = options.emailAddressTo ;
        if(this.emailAlert !== "none" && !this.emailAddressFrom){
            throw "You must give a sender address email" ;
        }
        if(this.emailAlert !== "none" && !this.emailAddressTo){
            throw "You must give a destination address email" ;
        }

        var self = this ;
        this.extendsProto = {
            saveCrashReport : function(record, callback){
                //this is the VeloxDatabase object
                self.saveCrashReport(this, record, callback) ;
            },
        } ;

        this.extendsExpressProto = {
            getSaveCrashReport: function(){
                //this is the VeloxDatabaseExpress object
                return (req, res)=>{
                    let crashReport = req.body.report ;

                    this.db.saveCrashReport(crashReport, (err)=>{
                        if(err){
                            this.db.logger.error("error when save binary", err);
                            return res.status(500).json(err) ;
                        }
                        res.end("OK") ;
                    }) ;
                } ;
            }
        } ;

        this.extendsExpressConfigure = [
            function(app){
                //this is the VeloxDatabaseExpress object
                app.post(options.saveEndPoint || "/saveCrashReport", this.getSaveCrashReport());
            }
        ] ;
    }

    /**
     * @typedef VeloxCrashReportRecord
     * @type {object}
     * @property {string} [uid] The report UID
     * @property {string} [app_name] Application name
     * @property {string} [app_version] Application version
     * @property {string} [user_uid] The user uid
     * @property {string} [user_agent] The user agent
     * @property {string} [url] The url
     * @property {string} [date] The date and time
     * @property {string} [error] The full error
     */

    /**
     * Save a crash record
     * 
     * @param {VeloxDatabase} db the db access
     * @param {VeloxCrashReportRecord} record the binary record
     * @param {function} callback called with saved record if succeed
     */
    saveCrashReport(db, record, callback) {
        db.transaction((client, done)=>{
            record.uid = uuid.v4() ;
            record.user_uid = db.context?db.context.req.user.uid:null ;
            client.insert("velox_crash_report", record, (err)=>{
                if(err){ return done(err) ;}
                if(this.emailAlert !== "none"){
                    var email = {
                        uid: record.uid,
                        from_addr: this.emailAddressFrom,
                        to_addr: this.emailAddressTo,
                        subject: "["+record.app_name+" "+record.app_version+"] Crash report",
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
                    client.searchFirst("velox_mail", {status: "tosend", subject: email.subject}, (err, foundEmail)=>{
                        if(err){ return done(err) ;}
                        if(foundEmail){
                            email = foundEmail ;
                        }

                        email.text += "----------------------------------\n" ;
                        email.text += "User : "+record.user_uid+"\n";
                        email.text += "User agent : "+record.user_agent+"\n";
                        email.text += "URL : "+record.url+"\n";
                        email.text += "Date : "+record.date+"\n";
                        email.text += "Error : "+record.error+"\n";
                        email.text += "----------------------------------\n" ;
                        email.html = email.text.replace(/\n/g, "<br />") ;

                        if(foundEmail){
                            client.update("velox_mail", email, done) ;
                        }else{
                            client.insert("velox_mail", email, done) ;
                        }
                    }) ;
                }else{
                    //no email alert
                    done() ;
                }
            }) ;
        }, callback) ;
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
            sql: this.getCreateTableCrashReport(backend)
        }) ;
        
        return changes;
    }


    /**
     * Create the table velox_crash_report if not exists
     * @param {string} backend 
     */
    getCreateTableCrashReport(backend){
        if(backend === "pg"){
            return `
            CREATE TABLE IF NOT EXISTS velox_crash_report (
                uid VARCHAR(40) PRIMARY KEY,
                app_name VARCHAR(128),
                app_version VARCHAR(50),
                user_uid VARCHAR(40),
                user_agent VARCHAR(512),
                url VARCHAR(512),
                date timestamp without time zone,
                error TEXT
            )
            ` ;
        }
        throw "not implemented for backend "+backend ;
    }

    
}

module.exports = VeloxCrashReport;