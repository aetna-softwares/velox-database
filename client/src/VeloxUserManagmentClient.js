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
     * @property {string} [authEndPoint] the auth entry point (default: auth)
     * @property {string} [activateEndPoint] the activate entry point (default: activateUser)
     * @property {string} [googleAuthEndPoint] the auth entry point (default: auth/google)
     * @property {string} [refreshEndPoint] the refresh user entry point (default: refreshUser)
     * @property {string} [createEndPoint] the refresh user entry point (default: createUser)
     * @property {string} [logoutEndPoint] the auth entry point (default: logout)
     * @property {string} [localStorageUserKey] the local storage key to store current user (default: velox_current_user)
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
        var authEndPoint = client.options.authEndPoint || "auth/user" ;
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
        var activateEndPoint = client.options.activateEndPoint || "activateUser" ;
        var ajaxActivate = client._createEndPointFunction(activateEndPoint , "POST", [ "activationToken", "password" ]) ;
        var activateFun = function(token, password, callback){
            ajaxActivate.bind(client)(token, password, function(err, user){
                if(err){
                    return callback(err) ;
                }
                callback(null, user) ;
            }.bind(client)) ;
        } ;
        client._registerEndPointFunction(activateEndPoint, activateFun) ;
        
        //add auth api entry
        var googleAuthEndPoint = client.options.googleAuthEndPoint || "auth/google" ;
        var ajaxGoogleAuth = client._createEndPointFunction(googleAuthEndPoint , "POST", [ "id_token" ]) ;
        var authGoogleFun = function(token, callback){
            ajaxGoogleAuth.bind(client)(token, function(err, user){
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
        client._registerEndPointFunction(googleAuthEndPoint, authGoogleFun) ;
        
        //add refresh user api entry
        var refreshEndPoint = client.options.refreshEndPoint || "refreshUser" ;
        var ajaxRefresh = client._createEndPointFunction(refreshEndPoint , "GET", [ ]) ;
        var refreshFun = function(callback){
            ajaxRefresh.bind(client)(function(err, user){
                if(err){
                    return callback(err) ;
                }
                this.currentUser = user;
                localStorage.setItem(userKey, JSON.stringify(user)) ;
                callback(null, user) ;
            }.bind(client)) ;
        } ;
        client._registerEndPointFunction(refreshEndPoint, refreshFun) ;
        
        //add create user api entry
        var createEndPoint = client.options.createEndPoint || "createUser" ;
        var ajaxCreate = client._createEndPointFunction(createEndPoint , "POST", "json", ["user"]) ;
        var createFun = function(user, callback){
            ajaxCreate.bind(client)(user, function(err, user){
                if(err){
                    return callback(err) ;
                }
                callback(null, user) ;
            }.bind(client)) ;
        } ;
        client._registerEndPointFunction(createEndPoint, createFun) ;
        

         //add logout api entry
        var logoutEndPoint = client.options.logoutEndPoint || "logout" ;
        var ajaxLogout = client._createEndPointFunction(logoutEndPoint , "POST") ;
        var logoutFun = function(callback){
            ajaxLogout.bind(client)(function(err){
                localStorage.removeItem(userKey) ;
                this.currentUser = null;
                if(window.gapi && window.gapi.auth2.getAuthInstance()){
                    //logged with google, sign out from google auth instance too
                    window.gapi.auth2.getAuthInstance().signOut().then(function () {
                        if(err){
                            return callback(err) ;
                        }
                        callback() ;
                    });
                }else{
                    if(err){
                        return callback(err) ;
                    }
                    callback() ;
                }
            }.bind(client)) ;
        } ;
        client._registerEndPointFunction(logoutEndPoint, logoutFun) ;

        callback() ;
    } ;


    return VeloxUserManagmentClient;
})));