/**
 * To be use in electron
 * 
 * you must import the module in native files and register it :
 */

const fs = require('fs');
const path = require('path');
const electron = require('electron');
const shell = electron.shell;
const app = electron.app;
const crypto = require('crypto');
const chokidar = require('chokidar');

var storagePath = null ;
var pathPattern = "{table}/{date}/{table_uid}_{uid}/{filename}";

var offlineDesktop = {} ;

function mkdirs(p, callback){
    fs.mkdir(p, function(err){
        if(err){
            if(err.code === 'EEXIST'){ return callback() ;}
            if(err.code === 'ENOENT'){ return mkdirs(path.dirname(p), function(err){
                    if(err){ return callback(err) ;}
                    mkdirs(p, callback) ;
                }) ;
            }
            return callback(err) ;
        }
        callback();
    }) ;
}

offlineDesktop.prepare = function (options, callback) {
    if(options.pathPattern){
        pathPattern = options.pathPattern ;
    }
    if (!storagePath) {
        storagePath = path.join(app.getPath("userData"), "binary_storage", (options.prefix || "default")) ;
        mkdirs(storagePath, function(err) {
            if (err) {callback(err); }
            callback() ;
        });
    }else{
        callback() ;
    }
};

function getBuffer(bufferOrFile, callback){
    var isBuffer = bufferOrFile instanceof Buffer;
    if(isBuffer){
        return callback(null, bufferOrFile) ;
    }
    if(typeof(bufferOrFile) === "string"){
        return callback(null, new Uint8Array(bufferOrFile)) ;
        //return callback(null, Buffer.from(bufferOrFile)) ;
    }
    console.log("buf ????", typeof(bufferOrFile), bufferOrFile.constructor.name, bufferOrFile) ;
    console.trace();
    fs.readFile(bufferOrFile.path, function(err, buffer){
        if(err){ return callback(err) ;}
        callback(null, buffer) ;
    }) ;
}

 /**
 * Create the target path from the configured pattern 
 * 
 * @param {object} binaryRecord the binary record
 */
function createTargetPath(binaryRecord){
    var targetPath = pathPattern ;
    targetPath = targetPath.replace(new RegExp("{table}", "g"), binaryRecord.table_name || "no_table") ;
    targetPath = targetPath.replace(new RegExp("{table_uid}", "g"), binaryRecord.table_uid || "no_uid") ;
    targetPath = targetPath.replace(new RegExp("{uid}", "g"), binaryRecord.uid) ;
    targetPath = targetPath.replace(new RegExp("{filename}", "g"), binaryRecord.filename) ;
    targetPath = targetPath.replace(new RegExp("{ext}", "g"), binaryRecord.filename?path.extname(binaryRecord.filename):"") ;
    var creationDatetimeIso = binaryRecord.creation_datetime ;
    if(typeof(creationDatetimeIso) !== "string"){
        creationDatetimeIso = binaryRecord.creation_datetime.toISOString() ;
    }
    targetPath = targetPath.replace(new RegExp("{date}", "g"), creationDatetimeIso.substring(0,10)) ;
    targetPath = targetPath.replace(new RegExp("{time}", "g"), creationDatetimeIso.substring(11,19).replace(/:/g, "_")) ;
    return targetPath ;
}

function createRecordPath(filepath){
    return filepath + ".json" ;
}

offlineDesktop.saveBinary = function(bufferOrFile, binaryRecord, callback){
    getBuffer(bufferOrFile, function(err, buffer){
        if(err){ return callback(err) ;}
        var filepath = path.join(storagePath, createTargetPath(binaryRecord)) ;
        console.log("create dir "+path.dirname(filepath)) ;
        mkdirs(path.dirname(filepath), function(err){
            if(err){ return callback(err) ;}
            console.log("write file ",filepath) ;
            fs.writeFile(filepath, buffer, function(err){
                if(err){ return callback(err) ;}
                console.log("written file ",filepath) ;
                fs.writeFile(createRecordPath(filepath), JSON.stringify(binaryRecord, null, 2), {encoding: "utf8"}, function(err){
                    if(err){ return callback(err) ;}
                    callback() ;
                }) ;
            }) ;
        });
    }) ;
} ;

function checksum(buffer, callback){
    var digest = crypto.createHash("sha256");
    digest.update(buffer) ;
    callback(null,digest.digest('hex')) ;
}

offlineDesktop.getLocalInfos = function(binaryRecord, callback){
    var filepath = path.join(storagePath, createTargetPath(binaryRecord)) ;
    var recordpath = createRecordPath(filepath) ;
    fs.readFile(recordpath, {encoding: "utf8"}, function(err, recordStr){
        if(err){
            if(err.code === 'ENOENT'){ 
                return callback(null, null, null) ;
            }
            return callback(err) ;
        }
        var lastSyncRecord = null;
        try {
            lastSyncRecord = JSON.parse(recordStr) ;
        } catch(err){
            return callback(err) ;
        }
        fs.readFile(filepath, function(err, buffer){
            if(err){ return callback(err) ;}
            checksum(buffer, function(err, hash){
                if(err){ return callback(err) ;}
                var file = new Uint8Array(buffer) ;
                callback(null, {file: file, checksum: hash}, lastSyncRecord) ;
            });
        }) ;
    }) ;
} ;

offlineDesktop.getFileBuffer = function(binaryRecord, callback){
    var filepath = path.join(storagePath, createTargetPath(binaryRecord)) ;
    fs.readFile(filepath, function(err, buffer){
        if(err){ return callback(err) ;}
        callback(null, buffer) ;
    }.bind(this));
} ;



offlineDesktop.openFile = function(binaryRecord, filename, callback){
    var filepath = path.join(storagePath, createTargetPath(binaryRecord)) ;
    shell.openItem(filepath);
    callback() ;
} ;

offlineDesktop.markAsUploaded = function(binaryRecord, callback){
    var filepath = path.join(storagePath, createTargetPath(binaryRecord)) ;
    var recordpath = createRecordPath(filepath) ;
    fs.writeFile(recordpath, JSON.stringify(binaryRecord, null, 2), {encoding: "utf8"}, function(err){
        if(err){ return callback(err) ;}
        callback() ;
    }) ;
} ;

offlineDesktop.watchFile = function(binaryRecord, callbackChanged){
    var filepath = path.join(storagePath, createTargetPath(binaryRecord)) ;
    var watcher = chokidar.watch(filepath, {});
    watcher.on("change", function(filePath, stats){
        callbackChanged() ;
    }) ;
} ;

module.exports = offlineDesktop ;