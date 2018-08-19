/*global define */
; (function (global, factory) {
    if (typeof exports === 'object' && typeof module !== 'undefined') {
        var VeloxScriptLoader = require("velox-scriptloader") ;
        module.exports = factory(VeloxScriptLoader) ;
    } else if (typeof define === 'function' && define.amd) {
        define(['VeloxScriptLoader'], factory);
    } else {
        global.VeloxServiceClient.setOfflineBinaryStorageEngine(factory(global.veloxScriptLoader));
    }
}(this, (function (VeloxScriptLoader) {
    'use strict';

    var LOCALFORAGE_VERSION = "1.7.2";

    var LOCALFORAGE_LIB = [
        {
            name: "localforage",
            type: "js",
            version: LOCALFORAGE_VERSION,
            cdn: "https://cdnjs.cloudflare.com/ajax/libs/localforage/$VERSION/localforage.nopromises.min.js",
            bowerPath: "localforage/dist/localforage.nopromises.min.js",
            npmPath: "localforage/dist/localforage.nopromises.min.js"
        }
    ];

    /**
     * @typedef VeloxDbOfflineLokiOptions
     * @type {object}
     * @property {string} [prefix] prefix for storage name
     */

    /**
     * The Velox database loki engine
     * 
     * @constructor
     * 
     * @param {VeloxDbOfflineLokiOptions} options database client options
     */
    function VeloxBinaryOfflineForage() {
        this.path = null;
    }

    VeloxBinaryOfflineForage.prototype.prepare = function (options, callback) {
        this.options = options ;
        if (!this.path) {
            this.path = (options.prefix || "") + "velox-binary-offline";
        }
        this.importLibIfNeeded(function (err) {
            if (err) { return callback(err); }
            this.localforage.config({
                name: (options.prefix || "") + "velox-binary-offline"
            });
            callback() ;
        }.bind(this));
    };

    VeloxBinaryOfflineForage.prototype.importLibIfNeeded = function (callback) {
        if (!this.localforage) {
            if (!VeloxScriptLoader) {
               return console.error("To have automatic script loading, you need to import VeloxScriptLoader");
            }

            VeloxScriptLoader.load(LOCALFORAGE_LIB, function (err) {
                if (err) { return callback(err); }
                this.localforage = window.localforage;
                callback();
            }.bind(this));
        } else {
            callback();
        }
    };

    function checksum(buffer, callback){
        crypto.subtle.digest("SHA-256", buffer).then(function(buf){
            var hash = Array.prototype.map.call(new Uint8Array(buf), x=>(('00'+x.toString(16)).slice(-2))).join('');
            callback(null, hash) ;
        }).catch(function(err){ callback(err) ;});
    }

    function getBuffer(blobOrFile, callback){
        var hasArrayBuffer = typeof ArrayBuffer === 'function';
        var isArrayBuffer = hasArrayBuffer && (blobOrFile instanceof ArrayBuffer || toString.call(blobOrFile) === '[object ArrayBuffer]');
        if(isArrayBuffer){
            return callback(null, blobOrFile) ;
        }

        var reader = new FileReader();

        reader.onerror = function (e) {
            callback(e) ;
        };

        reader.onload = function () {
            var data = reader.result;
            callback(null, data) ;
        };

        reader.readAsArrayBuffer(blobOrFile);
    }

    function fileKey(binaryRecord){
        return "bin-file-"+binaryRecord.uid ;
    }
    function recordKey(binaryRecord){
        return "bin-record-"+binaryRecord.uid ;
    }

    VeloxBinaryOfflineForage.prototype.saveBinary = function(blobOrFile, binaryRecord, callback){
        getBuffer(blobOrFile, function(err, buffer){
            if(err){ return callback(err) ;}
            this.localforage.setItem(fileKey(binaryRecord), buffer, function(err){
                if(err){ return callback(err) ;}
                this.localforage.setItem(recordKey(binaryRecord), binaryRecord, function(err){
                    if(err){ return callback(err) ;}
                    callback() ;
                }.bind(this));
            }.bind(this));
        }.bind(this)) ;
    } ;

    VeloxBinaryOfflineForage.prototype.getLocalInfos = function(binaryRecord, callback){
        this.localforage.getItem(recordKey(binaryRecord), function(err, lastSyncRecord){
            if(!lastSyncRecord){
                return callback(null, null, null) ;
            }
            if(err){ return callback(err) ;}
            this.localforage.getItem(fileKey(binaryRecord), function(err, buffer){
                if(err){ return callback(err) ;}
                checksum(buffer, function(err, hash){
                    if(err){ return callback(err) ;}
                    var file = buffer ;
                    callback(null, {file: file, checksum: hash}, lastSyncRecord) ;
                }.bind(this));
            }.bind(this));
        }.bind(this));
    } ;
    
    VeloxBinaryOfflineForage.prototype.getFileBuffer = function(binaryRecord, callback){
        this.localforage.getItem(fileKey(binaryRecord), function(err, buffer){
            if(err){ return callback(err) ;}
            callback(null, buffer) ;
        }.bind(this));
    } ;
    VeloxBinaryOfflineForage.prototype.openFile = function(binaryRecord, filename, callback){
        this.localforage.getItem(recordKey(binaryRecord), function(err, binaryRecord){
            if(!binaryRecord){
                return callback("file not found") ;
            }
            if(err){ return callback(err) ;}
            this.localforage.getItem(fileKey(binaryRecord), function(err, buffer){
                if(err){ return callback(err) ;}
                var blob = new Blob( [ buffer ], { type: binaryRecord.mime_type } );
                var link = document.createElement('a');
                link.href = window.URL.createObjectURL(blob);
                link.download = filename || binaryRecord.filename;
                link.click();
                callback(null) ;
            }.bind(this));
        }.bind(this));
    } ;

    VeloxBinaryOfflineForage.prototype.markAsUploaded = function(binaryRecord, callback){
        this.localforage.setItem(recordKey(binaryRecord), binaryRecord, function(err){
            if(err){ return callback(err) ;}
            callback() ;
        }.bind(this));
    } ;

    return new VeloxBinaryOfflineForage();
})));