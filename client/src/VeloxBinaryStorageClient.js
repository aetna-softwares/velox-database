/*global define*/
; (function (global, factory) {
        if (typeof exports === 'object' && typeof module !== 'undefined') {
        module.exports = factory() ;
    } else if (typeof define === 'function' && define.amd) {
        define([], factory);
    } else {
        global.VeloxBinaryStorageClient = factory() ;
        global.VeloxServiceClient.registerExtension(new global.VeloxBinaryStorageClient());
    }
}(this, (function () { 'use strict';

    /**
     * @typedef VeloxUserManagmentClientOptions
     * @type {object}
     * @property {string} [saveEndPoint] the save entry point (default saveBinary)
     * @property {string} [readEndPoint] the read entry point (default readBinary)
     */

    /**
     * The Velox user managment client
     * 
     * @constructor
     */
    function VeloxBinaryStorageClient() {

    }

    VeloxBinaryStorageClient.prototype.init = function(client, callback){
        //add save api entry
        var saveEndPoint = client.options.saveEndPoint || "saveBinary" ;
        var ajaxSave = client._createEndPointFunction(saveEndPoint , "POST", "multipart", [ "contents", "record" ]) ;
        var saveFun = function(contents, record, callback, callbackProgress){
            var xhrUpload = ajaxSave.bind(client)(contents, JSON.stringify(record), callback) ;
            if(callbackProgress){
                xhrUpload.addEventListener(callbackProgress) ;
            }
        } ;
        client._registerEndPointFunction(saveEndPoint, saveFun) ;
        

        var downloadEndPoint = client.options.downloadEndPoint || "downloadBinary" ;
        var ajaxDownload = client._createEndPointFunction(downloadEndPoint , "GET", "ajax", null, "arraybuffer",  [ "uid", "action" ]) ;
        var downloadFun = function(uid, action, callback, callbackProgress){
            var xhrUpload = ajaxDownload.bind(client)(uid, action, callback) ;
            if(callbackProgress){
                xhrUpload.addEventListener(callbackProgress) ;
            }
        } ;
        client._registerEndPointFunction(downloadEndPoint, downloadFun) ;
        
        
        //add read api entry
        var readEndPoint = client.options.readEndPoint || "readBinary" ;
        client._registerEndPointFunction(readEndPoint+"/download", function(recordOrUid, filename, callback, timeout){
            if(typeof(filename) === "function"){
                timeout = callback ;
                callback = filename;
                filename = null;
            }
            var uid = recordOrUid;
            if(typeof(recordOrUid) === "object"){
                uid = recordOrUid.uid ;
                if(!filename){
                    filename = recordOrUid.filename ;
                }
            }
            var downloadToken = (uid+"_"+Date.now()) ;
            var url = readEndPoint+"/download/"+uid+(filename?"/"+filename:"")+"?downloadToken="+downloadToken ;
            if(callback){
                var start = Date.now() ;
                var timer = setInterval(function(){
                    var finished = document.cookie.split(";").some(function(cook){
                        var cookAndValue = cook.trim().split("=") ;
                        if(cookAndValue[0] === downloadToken){
                            //server has set a cookie, the download finished
                            clearInterval(timer) ;
                            callback() ;
                            return true ;
                        }
                    }) ;
                    if(!finished && timeout){
                        if(Date.now() - start > timeout*1000){
                            clearInterval(timer) ;
                            callback("timout "+timeout+" reached") ;
                        }
                    }
                }, 100) ;
            }
            document.location.href = url ;
        }) ;

        client._registerEndPointFunction(readEndPoint+"/url", function(recordOrUid, filename, callback){
            var uid = recordOrUid;
            if(typeof(recordOrUid) === "object"){
                uid = recordOrUid.uid ;
                if(!filename){
                    filename = recordOrUid.filename ;
                }
            }
            callback(null, client.options.serverUrl+readEndPoint+"/inline/"+uid+(filename?"/"+filename:"")) ;
        }) ;

        callback() ;
    } ;


    return VeloxBinaryStorageClient;
})));