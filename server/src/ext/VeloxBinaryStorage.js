
const uuid = require("uuid") ;
const fs = require('fs-extra');
const path = require('path');
const multiparty = require('multiparty');
const url = require('url');
const crypto = require('crypto');

/**
 * This extension handle binary storage in database
 * 
 * It create the following tables : 
 *  - velox_binary : binary storage reference
 * 
 * This extension provide a functions on the VeloxDatabase object : 
 * saveBinary : ([table], [pk], pathOrStream, meta, callback) to save binary data
 * getBinary : ([table], [pk], [id], callback) to get a binary content
 * getBinaryStream : ([table], [pk], [id], callback) to get a binary stream
 * 
 * Express configuration :
 * If you are using express automatic configuration, it will register 2 end points saveBinary and readBinary (you can change names in options)
 * 
 * You can also handle express configuration yourself, in this case, this extension give you 2 helpers on the VeloxDatabaseExpress object :
 * getSaveBinaryMiddleware : give you a middleware to handle request to save binary file
 * getReadBinaryMiddleware : give you a middleware to handle request to read binary file
 * 
 * @example
 * //To use this extension, just register it on VeloxDatabase
 * const VeloxDatabase = require("");
 * const VeloxBinaryStorage = require("");
 * 
 * VeloxDatabase.registerExtension(new VeloxBinaryStorage({pathStorage: ...})) ;
 * 
 */
class VeloxBinaryStorage{

    /**
     * @typedef VeloxBinaryStorageOptions
     * @type {object}
     * @property {string} pathStorage The path storage to put binary files
     * @property {boolean} [useChecksum] Save checksum for file (default: true)
     * @property {string} [checksumAlgo] Checksum algorithm to use (default : md5)
     * @property {string} [pathPattern] The pattern to construct path of binary file (default : {table}/{date}/{table_uid}_{uid}{ext})
     * @property {string} [saveEndPoint] The Express end point for binary saving (default : /saveBinary)
     * @property {string} [readEndPoint] The Express end point for binary reading (default : /readBinary)
     */

    /**
     * Create the VeloxBinaryStorage extension
     * 
     * @param {VeloxBinaryStorageOptions} [options] options 
     */
    constructor(options){
        this.name = "VeloxBinaryStorage";

        if(!options){
            throw "You must give options to VeloxBinaryStorage instance" ;
        }
        if(!options.pathStorage){
            throw "You must give option pathStorage to VeloxBinaryStorage instance" ;
        }

        this.pathStorage = options.pathStorage ;
        this.pathStorageTemp = path.join(this.pathStorage, "temp") ;
        this.pathPattern = options.pathPattern || "{table}/{date}/{table_uid}_{uid}{ext}" ;
        this.useChecksum = options.useChecksum===undefined?true:options.useChecksum ;
        this.checksumAlgo = options.checksumAlgo || "md5" ;

        var self = this ;
        this.extendsProto = {
            saveBinary : function(record, pathOrStream, callback){
                //this is the VeloxDatabase object
                self.saveBinary(this, record, pathOrStream, callback) ;
            },
            getBinary : function(uid, callback){
                //this is the VeloxDatabase object
                self.getBinary(this, uid, callback) ;
            },
            getBinaryStream : function(uid, callback){
                //this is the VeloxDatabase object
                self.getBinaryStream(this, uid, callback) ;
            }
        } ;

        this.extendsExpressProto = {
            getSaveBinaryMiddleware: function(){
                //this is the VeloxDatabaseExpress object
                return (req, res)=>{
                    res.connection.setTimeout(0); //remove the 120s default timeout because client can take a long time to upload file
                    var form = new multiparty.Form();
                    form.parse(req, (err, fields, files) => {
                        if (err) {
                            this.db.logger.error("error parse request", err);
                            return res.status(500).json(err);
                        }

                        if(!fields.record || !fields.record[0]) {
                            this.db.logger.error("missing record");
                            return res.status(500).end("Missing record");
                        }
                        
                        let record = null;
                        try{
                            record = JSON.parse(fields.record[0]) ;
                        }catch(err){
                            this.db.logger.error("error parse record", err);
                            return res.status(500).json(err);
                        }

                        if(!files.contents || !files.contents[0]) {
                            this.db.logger.error("missing contents");
                            return res.status(500).end("Missing contents");
                        }
                        let pathUpload = files.contents[0].path;

                        this.db.saveBinary(record, pathUpload, (err, savedRecord)=>{
                            if(err){
                                this.db.logger.error("error when save binary", err);
                                return res.status(500).json(err) ;
                            }
                            fs.unlink(pathUpload, (err)=>{
                                if(err){ this.db.logger.warning("error when delete upload file", err); }
                            }) ;
                            res.json(savedRecord) ;
                        }) ;
                    }) ;
                } ;
            },
            getReadBinaryMiddleware: function(){
                return (req, res)=>{
                    let uid = req.params.uid;
                    let disposition = req.params.action==="download"?"attachment":"inline";                    
                    this.db.getBinary(uid, (err, buffer, record)=>{
                        if (err) {
                            this.db.logger.error("get binary failed : ", err, uid);
                            return res.status(500).json(err);
                        }

                        if(req.query.downloadToken){
                            res.cookie(req.query.downloadToken, "here is your download cookie",  { httpOnly: false , secure : false}) ;
                        }

                        let filename = req.params.filename || record.filename ;

                        res.setHeader('Content-disposition', disposition+'; filename=' + filename.replace(/[^a-zA-Z.\-_0-9]/g, "_"));
                        res.setHeader('Content-type', record.mime_type);
            
                        res.end(buffer);
                    }) ;
                } ;
            }
        } ;

        this.extendsExpressConfigure = [
            function(app){
                //this is the VeloxDatabaseExpress object
                app.post(options.saveEndPoint || "/saveBinary", this.getSaveBinaryMiddleware());
                app.get((options.readEndPoint || "/readBinary")+"/:action/:uid/:filename?", this.getReadBinaryMiddleware());
            }
        ] ;
    }

    /**
     * @typedef VeloxBinaryStorageRecord
     * @type {object}
     * @property {string} [uid] The unique id of file
     * @property {string} [table_name] The table name to which this file is linked
     * @property {string|object} [table_uid] The primary key of the table to which this file is linked
     * @property {string} [mime_type] The mime type of the file
     * @property {string} [description] A description of the file
     * @property {string} [filename] The filename
     */

    /**
     * Save a binary file
     * 
     * @param {VeloxDatabase} db the db access
     * @param {VeloxBinaryStorageRecord} record the binary record
     * @param {string|ReadStream} pathOrStream the path to save or the stram to save
     * @param {function} callback called with saved record if succeed
     */
    saveBinary(db, record, pathOrStream, callback) {
        fs.mkdirs(this.pathStorageTemp, (err)=>{
            if(err){ return callback(err); }

            var tempUid = uuid.v4() ;
            var tempFile = path.join(this.pathStorage, tempUid) ;

            var readStream = pathOrStream ;
            if(typeof(pathOrStream) === "string"){
                //not a stream, create a stream
                readStream = fs.createReadStream(pathOrStream);
            }

            let writeStream = fs.createWriteStream(tempFile) ;
            
            var errored = false ;
            let onError = (err)=> {
                // ensure callback is called only once:
                if (!errored) {
                    errored = true ;
                    return callback(err) ;
                }
            } ;

            readStream.on('error', onError);
            writeStream.on('error', onError);
        
            
            writeStream.on('open',  () => {
                readStream.pipe(writeStream) ;
            }) ;

            
            
        
            writeStream.once('close', ()=> {
                //copy in temp file succeed

                fs.stat(tempFile, (err, stats)=>{
                    if(err){ return callback(err) ;}
                    this.checksum(tempFile, (err, checksum)=>{
                        if(err){ return callback(err) ;}

                        var binaryRecord = {} ;
                        Object.keys(record).forEach((k)=>{
                            binaryRecord[k] = record[k] ;
                        }) ;
                        binaryRecord.checksum = checksum ;
                        binaryRecord.size = stats.size ;
                        binaryRecord.modification_datetime = new Date() ;
                        let mustCreate = false ;
                        if(!binaryRecord.uid){
                            binaryRecord.uid = uuid.v4() ;
                            mustCreate = true ;
                            binaryRecord.creation_datetime = binaryRecord.modification_datetime ;
                        }

                        
                        
                        
                        let moveFailed = false;
                        db.transaction((client, done)=>{
                            var afterSaveDone = (err, savedRecord)=>{ //when the db save is done, move the file
                                if(err){ return done(err); } //save db failed, abort
                                fs.move(tempFile, path.join(this.pathStorage, binaryRecord.path), {overwrite: true}, function(err){
                                    if(err){ 
                                        //move failed, rollback transaction
                                        moveFailed = true ;
                                        return done(err);
                                    } 
                                    
                                    done(null, savedRecord) ;
                                }) ;
                            } ;

                            if(mustCreate){
                                binaryRecord.path = this.createTargetPath(binaryRecord) ;
                                client.insert("velox_binary", binaryRecord, afterSaveDone) ;
                            }else{
                                client.getByPk("velox_binary", binaryRecord.uid, (err, existingRecord)=>{
                                    if(err){ return done(err); }
                                    if(!existingRecord){
                                        binaryRecord.creation_datetime = binaryRecord.modification_datetime ;
                                        binaryRecord.path = this.createTargetPath(binaryRecord) ;
                                        client.insert("velox_binary", binaryRecord, afterSaveDone) ;
                                    }else{
                                        if(!binaryRecord.creation_datetime){
                                            binaryRecord.creation_datetime = binaryRecord.modification_datetime ;
                                        }
                                        binaryRecord.path = this.createTargetPath(binaryRecord) ;
                                        client.update("velox_binary", binaryRecord, afterSaveDone) ;
                                    }
                                }) ;
                            }
                        }, (err, savedRecord)=>{
                            if(err){
                                if(moveFailed){
                                    //an error happens when trying to move, don't delete the temp file that is OK and should be used to retrieve information 
                                    //and log the problem
                                    db.logger.error("Move from "+tempFile+" to "+binaryRecord.path+" failed. The temp file "+tempFile+" is kept for analyze", err) ;
                                    callback(err) ;
                                }else{
                                    //error before move, remove the temp file
                                    fs.unlink(tempFile, (errDelete)=>{
                                        if(errDelete){
                                            db.logger.error("Can't remove "+tempFile, errDelete) ;
                                        }
                                        callback(err) ;
                                    }) ;
                                }
                            }else{
                                fs.unlink(tempFile, (errDelete)=>{
                                    if(errDelete){
                                        db.logger.error("Can't remove "+tempFile, errDelete) ;
                                    }
                                    callback(null, savedRecord) ;
                                }) ;
                            }
                        });
                    }) ;
                }) ;
                
            }) ;
        }) ;
    }

    /**
     * Create the target path from the configured pattern 
     * 
     * @param {object} binaryRecord the binary record
     */
    createTargetPath(binaryRecord){
        var targetPath = this.pathPattern ;
        targetPath = targetPath.replace(new RegExp("{table}", "g"), binaryRecord.table_name || "no_table") ;
        targetPath = targetPath.replace(new RegExp("{table_uid}", "g"), binaryRecord.table_uid || "no_uid") ;
        targetPath = targetPath.replace(new RegExp("{uid}", "g"), binaryRecord.uid) ;
        targetPath = targetPath.replace(new RegExp("{ext}", "g"), binaryRecord.filename?path.extname(binaryRecord.filename):"") ;
        targetPath = targetPath.replace(new RegExp("{date}", "g"), binaryRecord.creation_datetime.toISOString().substring(0,10)) ;
        targetPath = targetPath.replace(new RegExp("{time}", "g"), binaryRecord.creation_datetime.toISOString().substring(11,19).replace(/:/g, "_")) ;
        return targetPath ;
    }

    /**
     * Compute the checksum of the file
     * 
     * @param {string} filePath path of the file
     * @param {function} callback called with the checksum
     */
    checksum(filePath, callback){
        if(!this.useChecksum){ return callback(); }
        var digest = crypto.createHash(this.checksumAlgo);
        var stream = fs.createReadStream(filePath);
        stream.on("data", function(d) {digest.update(d);});
        stream.on("error", function (error) {return callback(error);});
        stream.on("end", function() {
            var hex = digest.digest("hex");
            return callback(null, hex);
        });
    }

    /**
     * Get the file content (buffer) and the meta data 
     * 
     * @param {VeloxDatabase} db the database access
     * @param {string} uid the binary record uid
     * @param {function} callback callback, receive the file content and the record meta
     */
    getBinary(db, uid, callback){
        db.inDatabase((client, done)=>{
            client.getByPk("velox_binary", uid, (err, record)=>{
                if(err){ return done(err); }
                if(!record) { return done("No binary data with id "+uid+" found") ;}
                let filePath = path.join(this.pathStorage, record.path) ;
                fs.readFile(filePath, (err, contents)=>{
                    if(err){ return done(err);}
                    done(null, contents, record) ;
                }) ;
            }) ;
        }, callback) ;
    }

    /**
     * Get a file read stream and the meta data 
     * 
     * @param {VeloxDatabase} db the database access
     * @param {string} uid the binary record uid
     * @param {function} callback callback, receive the file content and the record meta
     */
    getBinaryStream(db, uid, callback){
        db.inDatabase((client, done)=>{
            client.getByPk("velox_binary", uid, (err, record)=>{
                if(err){ return done(err); }
                if(!record) { return done("No binary data with id "+uid+" found") ;}
                let filePath = path.join(this.pathStorage, record.path) ;
                let stream = fs.createReadStream(filePath) ;
                done(null, stream, record) ;
            }) ;
        }, callback) ;
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
            sql: this.getCreateTableBinary(backend)
        }) ;
        
        return changes;
    }

    /**
     * Create the table velox_binary if not exists
     * @param {string} backend 
     */
    getCreateTableBinary(backend){
        if(backend === "pg"){
            return `
            CREATE TABLE IF NOT EXISTS velox_binary (
                uid VARCHAR(40) PRIMARY KEY,
                table_name VARCHAR(128),
                table_uid VARCHAR(128),
                checksum VARCHAR(128),
                size BIGINT,
                creation_datetime timestamp without time zone,
                modification_datetime timestamp without time zone,
                mime_type VARCHAR(75),
                description  VARCHAR(128),
                filename VARCHAR(128),
                path VARCHAR(256)
            )
            ` ;
        }
        throw "not implemented for backend "+backend ;
    }

    
}

module.exports = VeloxBinaryStorage;