; (function (global, factory) {
        if (typeof exports === 'object' && typeof module !== 'undefined') {
        module.exports = factory() ;
    } else if (typeof define === 'function' && define.amd) {
        define([], factory);
    } else {
        global.VeloxUserManagmentClient = factory() ;
        global.VeloxServiceClient.registerExtension(new global.VeloxUserManagmentClient());
    }
}(this, (function () { 'use strict';


    /**
     * @typedef VeloxUserManagmentClientOptions
     * @type {object}
     * @property {string} [authEndPoint] the auth entry point (default auth)
     * @property {string} [logoutEndPoint] the auth entry point (default logout)
     */

    /**
     * The Velox user managment client
     * 
     * @constructor
     */
    function VeloxUserManagmentClient() {
        
    }

    VeloxUserManagmentClient.prototype.init = function(client, callback){
        this.client = client ;

        client.addEndPoints([
            {endpoint: client.options.authEndPoint || "auth" , method: "POST", args: [ "username", "password" ]},
            {endpoint: client.options.logoutEndPoint || "logout", method: "POST"}
        ]) ;

        callback() ;
    } ;


    return VeloxUserManagmentClient;
})));