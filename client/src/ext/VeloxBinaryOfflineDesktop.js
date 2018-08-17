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
    function VeloxBinaryOfflineDesktop() {
        this.path = null;
    }

    VeloxBinaryOfflineDesktop.prototype.prepare = function (options, callback) {
        this.options = options ;
        if (!this.path) {
            this.path = (options.prefix || "") + "velox-binary-offline";
        }
        callback() ;
    };

    VeloxBinaryOfflineDesktop.prototype.storage.saveBinary = function(blobOrFile, binaryRecord, callback){

    } ;

    VeloxBinaryOfflineDesktop.prototype.storage.getLocalInfos = function(binaryRecord, callback){
        callback(null, currentInfos, lastSyncRecord) ;
    } ;
    
    VeloxBinaryOfflineDesktop.prototype.storage.getUrl = function(binaryRecord, filename, callback){
        callback(null, url) ;
    } ;
    VeloxBinaryOfflineDesktop.prototype.storage.openFile = function(binaryRecord, filename, callback){
    } ;
    VeloxBinaryOfflineDesktop.prototype.storage.markAsUploaded = function(binaryRecord, callback){
    } ;

    return new VeloxBinaryOfflineDesktop();
})));