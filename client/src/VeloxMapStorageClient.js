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
     * @typedef VeloxMapStorageClientOptions
     * @type {object}
     * @property {string} [saveMapEndPoint] the save entry point (default saveMap)
     * @property {string} [getMapEndPoint] the get entry point (default getMap)
     * @property {string} [getMapsEndPoint] the get entry point (default getMaps)
     */

    /**
     * The Velox user managment client
     * 
     * @constructor
     */
    function VeloxMapStorageClient() {

    }

    VeloxMapStorageClient.prototype.init = function(client, callback){
        //add save api entry
        var saveMapEndPoint = client.options.saveMapEndPoint || "saveMap" ;
        var ajaxSave = client._createEndPointFunction(saveMapEndPoint , "POST", [ "code", "key", "value" ]) ;
        var saveFun = function(code, key, value, callback){
           ajaxSave.bind(client)(code, key, value, callback) ;
        } ;
        client._registerEndPointFunction(saveMapEndPoint, saveFun) ;
        
        var getMapEndPoint = client.options.saveMapEndPoint || "getMap" ;
        var ajaxGetMap = client._createEndPointFunction(getMapEndPoint , "GET", [ "code", "key" ]) ;
        var getMapFun = function(code, key, callback){
            ajaxGetMap.bind(client)(code, key, callback) ;
        } ;
        client._registerEndPointFunction(getMapEndPoint, getMapFun) ;
        
        var getMapsEndPoint = client.options.saveMapsEndPoint || "getMaps" ;
        var ajaxGetMaps = client._createEndPointFunction(getMapsEndPoint , "GET", [ "code" ]) ;
        var getMapsFun = function(code, callback){
            ajaxGetMaps.bind(client)(code, callback) ;
        } ;
        client._registerEndPointFunction(getMapsEndPoint, getMapsFun) ;
        
        callback() ;
    } ;


    return VeloxMapStorageClient;
})));