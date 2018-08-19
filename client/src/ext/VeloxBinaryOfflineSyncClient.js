/*global define */
; (function (global, factory) {
    if (typeof exports === 'object' && typeof module !== 'undefined') {
        var VeloxScriptLoader = require("velox-scriptloader");
        module.exports = factory(VeloxScriptLoader);
    } else if (typeof define === 'function' && define.amd) {
        define(['VeloxScriptLoader'], factory);
    } else {
        global.VeloxServiceClient.registerExtension(factory(global.veloxScriptLoader));
    }
}(this, (function () {
    'use strict';

    /**
     * Create an unique ID
     */
    function uuidv4() {
        if(typeof(window.crypto) !== "undefined" && crypto.getRandomValues){
            return ([1e7]+-1e3+-4e3+-8e3+-1e11).replace(/[018]/g, function(c) {
                return (c ^ crypto.getRandomValues(new Uint8Array(1))[0] & 15 >> c / 4).toString(16) ;
            }) ;
        }else{
            return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
                var r = Math.random() * 16 | 0, v = c == 'x' ? r : (r & 0x3 | 0x8);
                return v.toString(16);
            });
        }
    }

    /**
     * Tables settings
     */
    var tableSettings = null;

    var conflictResolver = function (fileInfos, serverBinaryRecord, callback) {
        return callback(null, "upload-conflictlocal");
    };

    /**
     * The storage backend
     */
    var storage = null;

    var currentUser = null;

    /**
     * Offline sync extension definition
     */
    var extension = {};
    extension.name = "offlinebinarysync";

    extension.extendsObj = {};
    extension.extendsProto = {};
    extension.extendsGlobal = {};


    extension.init = function (client, callback) {
        //add save api entry
        var syncEndPoint = client.options.syncEndPoint || "binarySync";
        var ajaxSync = client._createEndPointFunction(syncEndPoint, "POST", "multipart", ["contents", "binaryRecord", "checksum", "action"]);
        var syncFun = function (contents, binaryRecord, checksum, action, callback, callbackProgress) {
            var xhrUpload = ajaxSync.bind(client)(contents, JSON.stringify(binaryRecord), checksum, action, callback);
            if (callbackProgress) {
                xhrUpload.addEventListener(callbackProgress);
            }
        };

        client._registerEndPointFunction(syncEndPoint, function (binaryRecord, file, checksum, action, callback, timeout) {
            syncFun(file, binaryRecord, checksum, action, function (err, binaryRecord) {
                if (err) {
                    return callback(err);
                }
                if (action.indexOf("download") === 0) {
                    client.downloadBinary(binaryRecord.uid, "download", function (err, buffer) {
                        callback(err, buffer, binaryRecord);
                    });
                } else {
                    callback(err, null, binaryRecord);
                }
            });
        });

        client.syncBinary = syncBinary.bind(client);
        client.saveBinary = saveBinary.bind(client);
        if (!client.readBinary) {
            client.readBinary = {};
        }
        client.readBinary.download = download.bind(client);
        client.readBinary.url = url.bind(client);

        callback();
    };

    /**
     * Set the offline storage engine
     * 
     * @param {object} storageEngine the storage engine to use
     */
    extension.extendsGlobal.setOfflineBinaryStorageEngine = function (storageEngine) {
        storage = storageEngine;
    };


    var syncAuto = true;


    /**
     * Set if the sync should be automatic or not
     * 
     * @param {boolean} syncAuto 
     */
    extension.extendsGlobal.setSyncBinaryAuto = function (syncAutoP) {
        syncAuto = syncAutoP;
    };

    /**
    * @typedef VeloxDbOfflineTableSettings
    * @type {object}
    * @property {string} name table name
    * @property {boolean|function} prefetch does record should be prefetch boolean or function returning true/false depending on record
    */

    /**
     * Set the table settings
     * 
     * @param {VeloxDbOfflineTableSettings[]} settings the table settings
     */
    extension.extendsGlobal.setOfflineTableBinarySettings = function (settings) {
        tableSettings = settings;
    };

    /**
     * Set the conflict resolver.
     * 
     * The default resolver give the priority to local without asking
     * 
     * function(fileInfos, serverBinaryRecord, callback){
     *   return callback(null, "upload-conflictlocal") ;
     * } ;
     * 
     * @param {function} conflictResolverP the conflict resolver
     */
    extension.extendsGlobal.setOfflineBinaryConflictResolver = function (conflictResolverP) {
        conflictResolver = conflictResolverP;
    };


    var prepareDone = false;
    /**
     * init local storage
     * 
     * @private
     */
    function prepare(callback) {
        if (this.currentUser && this.currentUser !== currentUser) {
            //user change, force reprepare
            prepareDone = false;
            this.lastSyncDate = null;
        }
        if (prepareDone) {
            return callback();
        }

        currentUser = this.currentUser;

        if (!storage) {
            console.error("No storage engined defined. Use VeloxDatabaseClient.setOfflineStorageEngine to specify one ");
        }
        storage.prepare({ prefix: currentUser ? currentUser.login + "_" : "" }, function (err) {
            if (err) {
                return callback(err);
            }
            prepareDone = true;
            callback();
        });
    }

    /**
     * Get the settings to apply for this binary record
     * @param {object} binaryRecord the binary record
     */
    function getBinarySettings(binaryRecord) {
        var table = binaryRecord.table_name;
        var settings = tableSettings && tableSettings[table]?tableSettings[table] : { cached: true, prefetch: true };

        if (!settings.conflictResolver) {
            settings.conflictResolver = conflictResolver;
        }

        var cachedFun = settings.cached;
        if (typeof (cachedFun) !== "function") {
            cachedFun = function (binaryRecord) { return settings.cached; };
        }
        var prefetchFun = settings.prefetch;
        if (typeof (prefetchFun) !== "function") {
            prefetchFun = function (binaryRecord) { return settings.prefetch; };
        }
        return {
            cached: cachedFun(binaryRecord),
            prefetch: prefetchFun(binaryRecord)
        };
    }

    /**
     * Override the download function. will write the file then sync with server
     */
    function saveBinary(file, binaryRecord, callback) {
        var settings = getBinarySettings(binaryRecord);
        if (!settings.cached) {
            //no cache, use standard function
            return this.constructor.prototype.saveBinary.bind(this)(file, binaryRecord, callback);
        }

        if(!binaryRecord.uid){
            binaryRecord.uid = uuidv4() ;
        }
        if(!binaryRecord.creation_datetime){
            binaryRecord.creation_datetime = new Date() ;
        }

        this.__velox_database.transactionalChanges([{
            table: "velox_binary", record: binaryRecord
        }], function (err) {
            if (err) { return callback(err); }
            prepare.bind(this)(function (err) {
                if (err) { return callback(err); }
                console.log(file, file.constructor.name) ;
                storage.saveBinary(file, binaryRecord, function (err) {
                    if (err) { return callback(err); }
                    if (syncAuto) {
                        this.syncBinary(binaryRecord, callback);
                    }
                }.bind(this));
            }.bind(this));
        }.bind(this));
    };

    /**
     * Override the download function. will sync before open the file
     */
    function url(recordOrUid, filename, callback) {
        this.__velox_database.getByPk("velox_binary", recordOrUid, function (err, binaryRecord) {
            if (err) { return callback(err); }
            if (!binaryRecord) { return callback("Binary record " + JSON.stringify(recordOrUid) + " does not exists"); }
            var settings = getBinarySettings(binaryRecord);
            if (!settings.cached) {
                //no cache, use standard function
                return this.constructor.prototype.readBinary.url.bind(this)(recordOrUid, filename, callback);
            }
            prepare.bind(this)(function (err) {
                if (err) { return callback(err); }
                if (syncAuto) {
                    this.syncBinary(binaryRecord, function (err) {
                        if (err) { return callback(err); }
                        storage.getFileBuffer(binaryRecord, function (err, buffer) {
                            if (err) { return callback(err); }
                            var blob = new Blob( [ buffer ], { type: binaryRecord.mime_type } );
                            var urlCreator = window.URL || window.webkitURL;
                            var url = urlCreator.createObjectURL( blob );
                            callback(null, url);
                        }.bind(this));
                    }.bind(this));
                } else {
                    storage.getUrl(binaryRecord, filename, function (err, url) {
                        if (err) { return callback(err); }
                        callback(null, url);
                    }.bind(this));
                }
            }.bind(this));
        }.bind(this));
    };

    /**
     * Override the download function. will sync before open the file
     */
    function download(recordOrUid, filename, callback) {
        if(typeof(filename) === "function"){
            callback = filename;
            filename = null;
        }
        this.__velox_database.getByPk("velox_binary", recordOrUid, function (err, binaryRecord) {
            if (err) { return callback(err); }
            if (!binaryRecord) { return callback("Binary record " + JSON.stringify(recordOrUid) + " does not exists"); }
            var settings = getBinarySettings(binaryRecord);
            if (!settings.cached) {
                //no cache, use standard function
                return this.constructor.prototype.readBinary.url.bind(this)(recordOrUid, filename, callback);
            }
            prepare.bind(this)(function (err) {
                if (err) { return callback(err); }

                if (syncAuto) {
                    this.syncBinary(binaryRecord, function (err) {
                        if (err) { return callback(err); }
                        storage.openFile(binaryRecord, filename, function (err, url) {
                            if (err) { return callback(err); }
                            watchFile.bind(this)(binaryRecord) ;
                            callback(null, url);
                        }.bind(this));
                    }.bind(this));
                } else {
                    if (err) { return callback(err); }
                    storage.openFile(binaryRecord, filename, function (err, url) {
                        if (err) { return callback(err); }
                        callback(null, url);
                    }.bind(this));
                }
            }.bind(this));
        }.bind(this));
    };

    /**
     * Sync the records with the server
     * 
     * @param {object|Array} [binaryRecords] The records to sync. If not given, all prefetchable records are sync
     * @param {function} callback 
     */
    function syncBinary(binaryRecords, callback) {
        if (typeof (binaryRecords) === "function") {
            callback = binaryRecords;
            binaryRecords = null;
        }
        var getRecords = function (cb) {
            if (binaryRecords) {
                if (Array.isArray(binaryRecords)) {
                    return cb(null, binaryRecords.slice());
                }
                return cb(null, [binaryRecords]);
            }
            var tablesWithoutPrefetch = [];
            Object.keys(tableSettings).forEach(function (tableName) {
                if (!tableSettings[tableName].prefetch) {
                    tablesWithoutPrefetch.push(tableName);
                }
            });
            var searchBinary = {};
            if (tablesWithoutPrefetch.length > 0) {
                searchBinary = { table_name: { ope: "not in", value: tablesWithoutPrefetch } };
            }
            this.search("velox_binary", searchBinary, function (err, binaries) {
                if (err) { return callback(err); }
                var binaryRecords = binaries.filter(function (b) {
                    return getBinarySettings(b).cached;
                });
                return cb(null, binaryRecords);
            }.bind(this));
        }.bind(this);

        prepare.bind(this)(function (err) {
            if (err) { return callback(err); }
            getRecords(function (err, binaryRecords) {
                if (err) { return callback(err); }

                dosync.bind(this)(binaryRecords, callback);
            }.bind(this));
        }.bind(this));
    };

    /**
     * Upload the file to the server
     * 
     * @param {object} binaryRecord the binary record to upload
     * @param {object} currentInfos the current file informations
     * @param {string} action the detailed upload action to perform
     * @param {function} callback 
     */
    function doUpload(binaryRecord, currentInfos, action, callback) {
        this.binarySync(binaryRecord, currentInfos.file, currentInfos.checksum, action, function (err, blob, binaryRecord) {
            if (err) { return callback(err); }
            if (action.indexOf("upload") === 0) {
                storage.markAsUploaded(binaryRecord, function (err) {
                    if (err) { return callback(err); }
                    callback();
                }.bind(this));
            } else {
                //it may be an upload for trace only (then action is not starting by upload and we don't mark it as uploaded)
                callback();
            }
        }.bind(this));
    }

    /**
     * Download the file from the server
     * 
     * @param {object} binaryRecord the binary record to download from server
     * @param {string} action the action details
     * @param {function} callback 
     */
    function doDownload(binaryRecord, action, callback) {
        this.binarySync(binaryRecord, null, null, action, function (err, blob, binaryRecord) {
            if (err) { return callback(err); }
            storage.saveBinary(blob, binaryRecord, function (err) {
                if (err) { return callback(err); }
                callback();
            }.bind(this));
        }.bind(this));
    }


    var syncingRecords = {} ;

    /**
     * Sync all this records with server
     * 
     * @param {Array} binaryRecords list of record to sync
     * @param {function} callback 
     */
    function dosync(binaryRecords, callback) {
        if(binaryRecords.length > 0 && binaryRecords.every(function(binaryRecord){
            return syncingRecords[binaryRecord.uid] ;
        })){
            //all records are in sync process, wait 1sec and retry
            return setTimeout(function(){
                dosync.bind(this)(binaryRecords, callback) ;
            }.bind(this), 1000) ;
        }

        var binaryRecord = binaryRecords.shift();
        if (!binaryRecord) { return callback(); }

        if(syncingRecords[binaryRecord.uid]){
            //this record is already in sync, send it back to the end of the list
            binaryRecords.push(binaryRecord) ;
            //go to next
            return dosync.bind(this)(binaryRecords, callback) ;
        }

        syncingRecords[binaryRecord.uid] = true ;
        var next = function (err) {
            delete syncingRecords[binaryRecord.uid] ;
            if (err) { return callback(err); }
            dosync.bind(this)(binaryRecords, callback);
        };
        this.__velox_database.getByPk("velox_binary", binaryRecord, function (err, binaryRecord) {
            if (err) { return callback(err); }
            if (!binaryRecord) {
                return callback();//no server record, skip that. If the record has been deleted, the cleaning process should take care of it
            }
            storage.getLocalInfos(binaryRecord, function (err, currentInfos, lastSyncRecord) {
                if (err) { return callback(err); }
                if(currentInfos && currentInfos.file){
                    currentInfos.file = new Blob([currentInfos.file], {type: binaryRecord.mime_type} ) ;
                }
                var localChecksum = currentInfos ? currentInfos.checksum : null;
                var lastSyncChecksum = lastSyncRecord ? lastSyncRecord.checksum : null;
                var serverChecksum = binaryRecord.checksum;
                if (localChecksum && serverChecksum && localChecksum === serverChecksum) {
                    //all checksum are the same, nothing to do
                    next() ;
                } else if (!localChecksum && serverChecksum) {
                    //no local file and a server file download it
                    doDownload.bind(this)(binaryRecord, "download-nolocal", next);
                } else if (localChecksum && !serverChecksum) {
                    //local file and no server file upload it
                    doUpload.bind(this)(binaryRecord, currentInfos, "upload-noserver", next);
                } else if (!localChecksum && !serverChecksum) {
                    //nobody knows this file, it is very unlikely to happens and should be ignored
                } else if (serverChecksum === lastSyncChecksum && localChecksum !== lastSyncChecksum) {
                    //the server is the same than last sync and local is modified since then, upload
                    doUpload.bind(this)(binaryRecord, currentInfos, "upload-localmodified", next);
                } else if (localChecksum === lastSyncChecksum && serverChecksum !== lastSyncChecksum) {
                    //the local is the same than last sync and server is modified since then, download
                    doDownload.bind(this)(binaryRecord, "download-servermodified", next);
                } else if (localChecksum !== lastSyncChecksum && serverChecksum !== lastSyncChecksum) {
                    //the local and the server has been modified since last sync, conflict
                    var settings = getBinarySettings(binaryRecord);
                    settings.conflictResolver(currentInfos, binaryRecord, function (err, action) {
                        if (err) { callback(err); }
                        var uploadOrDownload = action.split("-")[0];
                        if (uploadOrDownload === "upload") {
                            //user choose to upload his file, just upload it
                            doUpload.bind(this)(binaryRecord, currentInfos, action, next);
                        } else {
                            //user choose to download server file, upload user file for trace then download the server one
                            doUpload.bind(this)(binaryRecord, currentInfos, action, function (err) {
                                if (err) { callback(err); }
                                doDownload.bind(this)(binaryRecord, action, next);
                            }.bind(this));
                        }
                    }.bind(this));
                }
            }.bind(this));
        }.bind(this));
    }


    var watchingFiles = {} ;
    var watchTimers = {} ;
    function watchFile(binaryRecord){
        if(storage.watchFile && !watchingFiles[binaryRecord.uid]){
            watchingFiles[binaryRecord.uid] = true ;
            storage.watchFile(binaryRecord, function changed(){
                if(watchTimers[binaryRecord.uid]){
                    clearTimeout(watchTimers[binaryRecord.uid]) ;
                    delete watchTimers[binaryRecord.uid] ;
                }
                watchTimers[binaryRecord.uid] = setTimeout(function(){
                    console.log("Modification detected on "+binaryRecord.filename+" "+binaryRecord.uid+" start sync it") ;
                    dosync.bind(this)([binaryRecord], function(err){
                        if(err){ return console.log("sync failed", err) ;}
                        console.log("Sync done for "+binaryRecord.filename+" "+binaryRecord.uid) ;
                    }) ;
                }.bind(this), 5000) ;
            }.bind(this)) ;
        }
    }



    return extension;


})));