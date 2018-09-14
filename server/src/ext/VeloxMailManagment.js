const nodemailer = require('nodemailer');
const AsyncJob = require("velox-commons/AsyncJob") ;
const schedule = require('node-schedule');
const fs = require("fs") ;

/**
 * This extension handle mail sending and archive
 * 
 * It create the following tables : 
 *  - velox_mail : mails
 *  - velox_mail_smtp_server : SMTP server
 * 
 * When add record in velox_mail, it will send mail automatically
 * 
 * @example
 * //To use this extension, just register it on VeloxDatabase
 * const VeloxDatabase = require("");
 * const VeloxMailManagment = require("");
 * 
 * VeloxDatabase.registerExtension(new VeloxMailManagment()) ;
 * 
 */
class VeloxMailManagment{

    /**
     * @typedef VeloxMailManagmentOption
     * @type {object}
     * @property {string} [cronSchedule] mail cron sending (default : * * * * * which is every minute)
     * @property {object} [defaultMailServer] default mail server configuration
     */

    /**
     * Create the VeloxMailManagment extension
     * 
     * @param {VeloxMailManagmentOption} [options] options 
     */
    constructor(options){
        this.name = "VeloxMailManagment";

        if(!options){
            options = {} ;
        }
        this.options = options ;

        this.transporters = {} ;
        this.transportersDefinition = {} ;

        var self = this;
        
        this.interceptClientQueries = [
            {name : "insert", table: "velox_mail", after: function(table, mail, callback){
                self.afterMailInsert(this, mail, callback) ;
            } },
        ] ;
    }

    /**
     * Called after insert mail record. Send the mail if the schedule_type is "now"
     * 
     * @private
     * @param {VeloxDatabaseClient} tx the database transaction client
     * @param {object} mail the mail record to send
     * @param {function} callback called on finished
     */
    afterMailInsert(tx, mail, callback){
        if(mail.schedule_type === "now" && mail.status === "tosend"){
            tx.on("close", (ctx)=>{
                this.cronJob(ctx.db) ;
            }) ;
        }
        callback() ;
    }

    /**
     * Send one mail.
     * 
     * Will use the SMTP server specified on the server_uid property of the mail or 
     * assume that there is only one SMTP server and will take first record found in the table
     * 
     * Note : if no SMTP server is configured, the sending will fail
     * 
     * @param {VeloxDatabaseClient} tx the database transaction client
     * @param {object} mail the mail record to send
     * @param {function} callback called on finished
     */
    sendAMail(tx, mail, callback){
        var searchServer = {} ;
        if(mail.server_uid){
            searchServer["uid"] = mail.server_uid ;
        }
        
        tx.searchFirst("velox_mail_smtp_server", searchServer, (err, smtpServer)=>{
            if(err){ return callback(err) ;}
            if(!smtpServer){
                return callback("No SMTP server") ;
            }

            let transporter = this.transporters[smtpServer.uid] ;
            if(transporter){
                let savedDef = this.transportersDefinition[smtpServer.uid] ;
                if(JSON.stringify(savedDef) !== JSON.stringify(smtpServer)){
                    //configuration changed
                    transporter = null;
                }
            }
            if(!transporter){
                let params = {
                    host: smtpServer.host,
                    port: smtpServer.port,
                    secure: smtpServer.port===465                   
                } ;
                if(smtpServer.username){
                    params.auth = {
                        user: smtpServer.username,
                        pass: smtpServer.pass
                    } ;
                }
                transporter = nodemailer.createTransport(params);
                this.transporters[smtpServer.uid] = transporter ;
                this.transportersDefinition[smtpServer.uid] = smtpServer ;
            }

            let mailOptions = {
                from: mail.from_addr, // sender address
                to: mail.to_addr, // list of receivers
                subject: mail.subject, // Subject line
                text: mail.text, // plain text body
                html: mail.html // html body
            };

            if(smtpServer.username){
                //put in reply-to to avoid spam detection
                mailOptions.replyTo = mailOptions.from;
                mailOptions.from = smtpServer.username;
            }

            var jobAttachs = new AsyncJob(AsyncJob.PARALLEL) ;
            if(mail.attachs.length>0){
                mailOptions.attachments = [] ;
                mail.attachs.forEach(function(attach){
                    jobAttachs.push(function(cb){
                        if(attach.binary_uid){
                            tx.getBinaryStream(attach.binary_uid, (err, stream)=>{
                                if(err){ return cb(err) ;}
                                mailOptions.attachments.push({
                                    filename : attach.name,
                                    content: stream
                                }) ;
                                cb() ;
                            });
                        }else{
                            mailOptions.attachments.push({
                                filename : attach.name,
                                content: attach.path
                            }) ;
                            cb() ;
                        }
                    });
                }) ;
            }

            jobAttachs.async((err)=>{
                if(err){ return callback(err) ;}

                // send mail with defined transport object
                transporter.sendMail(mailOptions, (error, info) => {
                    if (error) {
                        tx.update("velox_mail", {uid: mail.uid, 
                            error: JSON.stringify(error),
                            status: "error"
                        }, function(err){
                            if(err){
                                return callback(err) ;
                            }
                            callback() ;
                        }) ;
                    } else {
                        tx.update("velox_mail", {uid: mail.uid, 
                            message_id: info.messageId, 
                            accepted: info.accepted?info.accepted.join(","):"",
                            rejected: info.rejected?info.rejected.join(","):"",
                            pending: info.pending?info.pending.join(","):"",
                            smtp_response: info.response,
                            status: "sent"
                        }, (err)=>{
                            if(err){ return callback(err) ;}
                            var jobAttachsDelete = new AsyncJob(AsyncJob.PARALLEL) ;
                            if(mail.attachs.length>0){
                                mailOptions.attachments = [] ;
                                mail.attachs.forEach(function(attach){
                                    if(attach.delete_after){
                                        jobAttachsDelete.push(function(cb){
                                            if(attach.binary_uid){
                                                tx.remove("velox_binary", attach.binary_uid, cb) ;
                                            }else{
                                                fs.unlink(attach.path, cb) ;
                                            }
                                        });
                                    }
                                }) ;
                                jobAttachsDelete.async(callback) ;
                            } else {
                                callback() ;
                            }
                        }) ;
                    }
                });
            }) ;
        
        }) ;
    }

    /**
     * Run the cron job. Send all mails having status "tosend"
     * 
     * @param {VeloxDatabase} db the db access
     */
    cronJob(db) {
        if(this.cronRunning){
            //already running
            return;
        }
        this.cronRunning = true ;
        try {
            db.transaction((client, done)=>{
                client.search("velox_mail", {status: "tosend"}, [
                    {otherTable: "velox_mail_attach", type: "2many", name: "attachs"}
                ], (err, mails)=>{
                    if(err){ return done(err) ;}
                    let job = new AsyncJob(AsyncJob.SERIES) ;
                    for(let mail of mails){
                        if(!mail.schedule_date || mail.schedule_date < new Date()){
                            job.push((cb)=>{
                                this.sendAMail(client, mail, cb) ;
                            }) ;
                        }
                    }
                    job.async(done) ;
                }) ;
            }, ()=>{
                this.cronRunning = false ;
            }) ;
        } catch (error) {
            this.cronRunning = false ;
        }
    }

    /**
     * Called on database init.
     * 
     * Will schedule the cron job
     * 
     * @param {VeloxDatabase} db the db access
     */
    init(db){
        schedule.scheduleJob(this.options.cronSchedule || "* * * * *", ()=>{
            this.cronJob(db) ;
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
            sql: this.getCreateTableSmtpServer(backend)
        }) ;
        changes.push({
            sql: this.getCreateTableMail(backend)
        }) ;
        changes.push({
            sql: this.getCreateTableMailAttach(backend)
        }) ;

        if(this.options.defaultMailServer){
            changes.push({
                run: (tx, cb)=>{
                    tx.searchFirst("velox_mail_smtp_server", {}, (err, existingProfile)=>{
                        if(err){ return cb(err); }
                        if(existingProfile){
                            return cb() ;
                        }
                        if(!this.options.defaultMailServer.uid){
                            this.options.defaultMailServer.uid = this.options.defaultMailServer.host ;
                        }
                        tx.insert("velox_mail_smtp_server", this.options.defaultMailServer, cb) ;
                    }) ;
                }
            }) ;
        }
        
        return changes;
    }

    /**
     * Create the table velox_mail if not exists
     * @param {string} backend 
     */
    getCreateTableMail(backend){
        let lines = [
            "uid VARCHAR(40) PRIMARY KEY",
            "from_addr VARCHAR(128)",
            "to_addr VARCHAR(128)",
            "subject VARCHAR(128)",
            "text TEXT",
            "html TEXT",
            "status VARCHAR(20)",
            "schedule_type VARCHAR(20)",
            "schedule_date timestamp without time zone",
            "server_uid VARCHAR(50) REFERENCES velox_mail_smtp_server(uid)",
            "message_id VARCHAR(128)",
            "accepted VARCHAR(128)",
            "rejected VARCHAR(128)",
            "pending VARCHAR(128)",
            "smtp_response VARCHAR(128)",
            "realm_code VARCHAR(40)",
            "user_uid VARCHAR(40)"
        ] ;
        if(backend === "pg"){
            return `
            CREATE TABLE IF NOT EXISTS velox_mail (
                ${lines.join(",")}
            )
            ` ;
        }
        throw "not implemented for backend "+backend ;
    }

     /**
     * Create the table velox_mail if not exists
     * @param {string} backend 
     */
    getCreateTableMailAttach(backend){
        let lines = [
            "uid VARCHAR(40) PRIMARY KEY",
            "mail_uid VARCHAR(40) REFERENCES velox_mail(uid)",
            "name VARCHAR(128)",
            "path VARCHAR(256)",
            "binary_uid VARCHAR(40)",// REFERENCES velox_binary(uid)
            "delete_after BOOLEAN"
        ] ;
        if(backend === "pg"){
            return `
            CREATE TABLE IF NOT EXISTS velox_mail_attach (
                ${lines.join(",")}
            )
            ` ;
        }
        throw "not implemented for backend "+backend ;
    }

    /**
     * Create the table velox_mail_smtp_server if not exists
     * @param {string} backend 
     */
    getCreateTableSmtpServer(backend){
        if(backend === "pg"){
            return `
            CREATE TABLE IF NOT EXISTS velox_mail_smtp_server (
                uid VARCHAR(40) PRIMARY KEY,
                host VARCHAR(128),
                port INT,
                username VARCHAR(75),
                pass VARCHAR(75)
            )
            ` ;
        }
        throw "not implemented for backend "+backend ;
    }



}

module.exports = VeloxMailManagment;