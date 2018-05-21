; (function (global, factory) {
        if (typeof exports === 'object' && typeof module !== 'undefined') {
        module.exports = factory() ;
    } else if (typeof define === 'function' && define.amd) {
        define([], factory);
    } else {
        global.VeloxCrashReportClient = factory() ;
        global.VeloxServiceClient.registerExtension(new global.VeloxCrashReportClient());
    }
}(this, (function () { 'use strict';

    /**
     * @typedef VeloxCrashReportClientOptions
     * @type {object}
     * @property {string} [saveEndPoint] the save entry point (default saveCrashReport)
     */

    /**
     * The Velox crash report client
     * 
     * @constructor
     */
    function VeloxCrashReportClient() {

    }

    VeloxCrashReportClient.prototype.init = function(client, callback){
        //add save api entry
        var saveEndPoint = client.options.saveEndPoint || "saveCrashReport" ;
        var ajaxSave = client._createEndPointFunction(saveEndPoint , "POST", [ "report"]) ;
        var saveFun = function(report, callback, callbackProgress){
            var xhrUpload = ajaxSave.bind(client)(report, callback) ;
            if(callbackProgress){
                xhrUpload.addEventListener(callbackProgress) ;
            }
        } ;
        client._registerEndPointFunction(saveEndPoint, saveFun) ;
        
        callback() ;
    } ;


    return VeloxCrashReportClient;
})));