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

    var localStorageUserKey = "velox_current_user";

    /**
     * @typedef VeloxUserManagmentClientOptions
     * @type {object}
     * @property {string} [authEndPoint] the auth entry point (default auth)
     * @property {string} [logoutEndPoint] the auth entry point (default logout)
     * @property {string} [localStorageUserKey] the local storage key to store current user (default velox_current_user)
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

        var userKey = client.options.localStorageUserKey || localStorageUserKey ;
        var savedUser = localStorage.getItem(userKey);
        if(savedUser){
            client.currentUser = JSON.parse(savedUser) ;
        }

        //add auth api entry
        var authEndPoint = client.options.authEndPoint || "auth" ;
        var ajaxAuth = client._createEndPointFunction(authEndPoint , "POST", [ "username", "password" ]) ;
        var authFun = function(username, password, callback){
            ajaxAuth.bind(client)(username, password, function(err, user){
                if(err){
                    this.currentUser = null;
                    localStorage.removeItem(userKey) ;
                    return callback(err) ;
                }
                this.currentUser = user;
                localStorage.setItem(userKey, JSON.stringify(user)) ;
                callback(null, user) ;
            }.bind(client)) ;
        } ;
        client._registerEndPointFunction(authEndPoint, authFun) ;
        

         //add auth api entry
        var logoutEndPoint = client.options.logoutEndPoint || "logout" ;
        var ajaxLogout = client._createEndPointFunction(logoutEndPoint , "POST") ;
        var logoutFun = function(callback){
            ajaxLogout.bind(client)(function(err){
                localStorage.removeItem(userKey) ;
                this.currentUser = null;
                if(err){
                    return callback(err) ;
                }
                callback() ;
            }.bind(client)) ;
        } ;
        client._registerEndPointFunction(logoutEndPoint, logoutFun) ;

        callback() ;
    } ;


    return VeloxUserManagmentClient;
})));