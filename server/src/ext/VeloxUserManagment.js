const bcrypt = require("bcrypt") ;
const uuid = require("uuid") ;
const LocalStrategy = require('passport-local').Strategy ;
var GoogleTokenStrategy = require('./google/PassportGoogleTokenStrategy');

const FAKE_PASSWORD = "*********" ;

/**
 * This extension handle user management in database
 * 
 * It create the following tables : 
 *  - velox_user : users
 *  - velox_user_profile : user profiles
 *  - velox_user_realm : user realm
 *  - velox_link_user_realm : link between users and realm
 *  - velox_user_session : user session
 * 
 * 
 * 
 * This extension provide a functions on the VeloxDatabase object : 
 * authenticate : (login, password, [realm], callback) to authenticate an user 
 * 
 * The use of a realm is optional, if not provided, all users are considered to belong to the same domain
 * The use of profile is also optional, it can work as a simple authentication system
 * 
 * It also automatically hash password on user insertion and replace it by a fake placeholder when read user
 * 
 * On first database creation, an admin user is created. If you don't want it, use the dontCreateAdmin option.
 * If it is deleted, it will be automatically recreated, disable it instead of delete it (or set dontCreateAdmin option)
 * 
 * Express configuration :
 * If you are using express automatic configuration, it will register express-session and passport authentication system
 * 
 * You can also handle express configuration yourself, in this case, this extension give you 2 helpers on the VeloxDatabaseExpress object :
 * configurePassport : configure passport to use velox user tables
 * getSessionMiddleware : get an express session middleware to handle session in velox session table
 * getSessionCheckMiddleware : get an express middleware to check session user right on each request
 * 
 * @example
 * //To use this extension, just register it on VeloxDatabase
 * const VeloxDatabase = require("");
 * const VeloxUserManagment = require("");
 * 
 * VeloxDatabase.registerExtension(new VeloxUserManagment()) ;
 * 
 */
class VeloxUserManagment{

    /**
     * @typedef VeloxUserManagmentGoogleOptions
     * @type {object}
     * @property {string} clientID Google client ID
     * @property {string} [authEndPoint] the login authentication end point (default : "/auth/google")
     */

     /**
     * @typedef VeloxUserManagmentRestriction
     * @type {object}
     * @property {string} name the table name
     * @property {string} [realmCol] the name of the column containing realm code. It this option is set, the queries will be filtered on record linked to the realm to which the user has right
     * @property {int} [insertMinProfileLevel] the minimum profile level required to do insert action on this table (note that highest level is the lowest number, ie 0 is the top admin level)
     * @property {int} [updateMinProfileLevel] the minimum profile level required to do update action on this table (note that highest level is the lowest number, ie 0 is the top admin level)
     * @property {int} [removeMinProfileLevel] the minimum profile level required to do remove action on this table (note that highest level is the lowest number, ie 0 is the top admin level)
     * @property {int} [readMinProfileLevel] the minimum profile level required to do remove action on this table (note that highest level is the lowest number, ie 0 is the top admin level)
     * @property {int} [minProfileLevel] shortcut to set insertMinProfileLevel, updateMinProfileLevel, removeMinProfileLevel at once
     * @property {int} [allMinProfileLevel] shortcut to set insertMinProfileLevel, updateMinProfileLevel, removeMinProfileLevel, readMinProfileLevel at once
     */


    /**
     * @typedef VeloxUserManagmentOption
     * @type {object}
     * @property {Array} [userMeta] Meta data to add on user table : [{name: "foo", type: "varchar(128)"}, {name: "bar", type: "int"}]
     * @property {Array} [profileMeta] Meta data to add on profile table : [{name: "foo", type: "varchar(128)"}, {name: "bar", type: "int"}]
     * @property {object} [anonymousUser] The anonymous user to create on database creation. All none auth connection will be considered as anonymous
     * @property {object} [adminUser] The admin user to create on database creation
     * @property {object} [adminProfile] The admin profile to create on database creation
     * @property {object} [fixedProfiles] List of profiles to create by default in database
     * @property {object[]} [defaultRealms] The default realms to create
     * @property {boolean} [dontCreateAdmin] set to true if you don't want the admin automatic creation
     * @property {boolean} [useProfile] set to true if your user management use profile. default : false of no adminProfile and no fixedProfile, true elsewhere
     * @property {boolean} [useRealm] set to true if your user management use realms. default : false of no defaultRealms, true elsewhere
     * @property {VeloxUserManagmentRestriction[]} [restrictedTables] add database restriction check on tables
     * @property {string} [sessionSecret] the session secret salt phrase for express middleware
     * @property {string} [usernameField] the username field name in login form (default : username)
     * @property {string} [passwordField] the password field name in login form (default : password)
     * @property {string} [realmField] the realm field name in login form (default : realm)
     * @property {string} [authEndPoint] the login authentication end point (default : "/auth/user")
     * @property {string} [logoutEndPoint] the logout end point (default : "/logout")
     * @property {string} [refreshEndPoint] the refresh user end point (default : "/refreshUser")
     * @property {string} [activateEndPoint] the activation user end point (default : "/activateUser")
     * @property {string} [changePasswordEndPoint] the activation user end point (default : "/changeUserPassword")
     * @property {string} [createEndPoint] the activation user end point (default : "/createUser")
     * @property {boolean} [mustActivate] user must activate account with token (default: false)
     * @property {object} [sessionOptions] custom options for express-session
     * @property {object} [sessionCheck] option for session check
     * @property {object} [google] option for google authentication
     */

    /**
     * Create the VeloxUserManagment extension
     * 
     * @param {VeloxUserManagmentOption} [options] options 
     */
    constructor(options){
        this.name = "VeloxUserManagment";

        if(!options){
            options = {} ;
        }
        this.options = options ;
        this.userMeta = options.userMeta || [] ;
        this.profileMeta = options.profileMeta || [] ;
        this.dontCreateAdmin = options.dontCreateAdmin || false ;
        this.fixedProfiles = options.fixedProfiles || null ;
        this.adminProfile = options.adminProfile || null ;
        this.defaultRealms = options.defaultRealms || null ;
        this.adminUser = options.adminUser || {login: "admin", password: "admin", name: "Administrator", auth_type: "password"} ;
        this.anonymousUser = options.anonymousUser || {login: "anonymous", name: "Anonymous"};
        this.useProfile = options.useProfile ;
        if(this.useProfile === undefined){
            if(this.adminProfile || this.fixedProfiles){
                this.useProfile = true;
            }
        }
        this.useRealm = options.useRealm ;
        if(this.useRealm === undefined){
            if(this.defaultRealms){
                this.useRealm = true;
            }
        }
        if(this.adminProfile){
            if(typeof(this.adminProfile) === "string"){
                //just give the code, profile should be in fixedProfile list
                if(!this.fixedProfiles || !this.fixedProfiles.some(function(p){ return p.code === this.adminProfile ; }.bind(this))){
                    throw "You give a adminProfile code but this code is no where in fixedProfiles option." ;
                }
                this.adminUser.profile_code = this.adminProfile ;
            }else{
                this.adminUser.profile_code = this.adminProfile.code ;
            }
        }
        if(!this.adminUser.uid){
            this.adminUser.uid = uuid.v4() ;
        }
        if(this.anonymousUser && !this.anonymousUser.uid){
            this.anonymousUser.uid = uuid.v4() ;
        }

        if(this.adminUser.active === undefined){
            this.adminUser.active = true ;
        }
        if(this.anonymousUser.active === undefined){
            this.anonymousUser.active = true ;
        }

        var self = this;
        this.extendsClient = {
            createUser : function(user, callback){
                //this is the VeloxDatabaseClient object
                self.createUserInTransaction(this, user, callback) ;
            },
        };
        this.extendsProto = {
            authenticate : function(login, password, realm, callback){
                //this is the VeloxDatabase object
                self.authenticateUser(this, login, password, realm, callback) ;
            },
            authenticateGoogle : function(token, callback){
                //this is the VeloxDatabase object
                self.authenticateGoogleUser(this, token, callback) ;
            },
            activate : function(activationToken, password, callback){
                //this is the VeloxDatabase object
                self.activateUser(this, activationToken, password, callback) ;
            },
            changeUserPassword : function(userUid, oldPassword, newPassword, callback){
                //this is the VeloxDatabase object
                self.changePassword(this, userUid, oldPassword, newPassword, callback) ;
            },
            changeUserPasswordToken : function(tokenPassword, newPassword, callback){
                //this is the VeloxDatabase object
                self.changePasswordToken(this, tokenPassword, newPassword, callback) ;
            },
            requestPasswordToken : function(userEmail, email, callback){
                //this is the VeloxDatabase object
                self.requestPasswordToken(this, userEmail, email, callback) ;
            },
            createUser : function(user, callback){
                //this is the VeloxDatabase object
                self.createUser(this, user, callback) ;
            },
            refreshUser : function(uid, callback){
                //this is the VeloxDatabase object
                self.refreshUser(this, uid, callback) ;
            }
        } ;

        let globalOptions = options ;

        this.extendsExpressProto = {
            /**
             * configure the passport strategy to use velox users
             * 
             * @param {object} passport the passportjs object
             * @param {object} [strategyOptions] the strategy option, you can set usernameField, passwordField and realmField to use custom form field names
             */
            configurePassport : function(passport, strategyOptions){
                //this is the VeloxDatabaseExpress object
                passport.serializeUser((user, done) => {
                    done(null, user.uid);
                });

                passport.deserializeUser((uid, done) => {
                    this.db.inDatabase((client,done)=>{
                        client.getByPk("velox_user", uid, [
                            {name: "profile", otherTable: "velox_user_profile"},
                            {name: "realms", otherTable: "velox_link_user_realm", type:"2many", joins: [
                                {name: "realm", otherTable: "velox_user_realm"},
                                {name: "profile", otherTable: "velox_user_profile"},
                            ]},
                        ], done) ;
                    },done) ;
                });

                var options = {} ;
                if(strategyOptions){
                    options = JSON.parse(JSON.stringify(strategyOptions)) ;
                }
                options.passReqToCallback = true ;
                passport.use(new LocalStrategy(options, (req, username, password, done) => { 
                        if(!req.body){
                            this.db.logger.error("You should have req.body populated when call login. Did you forget to add body-parser ? (or worse did you try to login with a GET request ?)");
                            return done("Missing req.body") ;
                        }
                        this.db.authenticate(username, password, req.body[options.realmField || "realm"], done) ;
                }));

                if(options.google){

                    passport.use(new GoogleTokenStrategy({
                        clientID: options.google.clientID
                    },
                      (parsedToken, googleId, done)=> {
                        this.db.transaction((tx, done)=>{
                            tx.searchFirst("velox_user", {login: googleId, auth_type:"google"}, (err, user)=>{
                                if(err){ return done(err) ;}
                                if(user) { return done(null, user) ;}
                                var newUser = {} ;
                                newUser.uid = uuid.v4() ;
                                newUser.login = googleId ;
                                newUser.name  = parsedToken.name;
                                newUser.auth_type = "google" ;
                                newUser.email = parsedToken.email;
                                newUser.active = true;
                                newUser.disabled = false;
                                //parsedToken.locale (contains fr)
                                tx.insert("velox_user",newUser, done) ;
                            }) ;
                        }, done) ;
                      }
                    ));
                }


            },

            getSessionMiddleware: function(session, sessionOptions){
                //this is the VeloxDatabaseExpress object

                let VeloxUserManagmentSessionStore = require('./VeloxUserManagmentSessionStore')(session);
                var options = {
                    store: new VeloxUserManagmentSessionStore(this.db, this.anonymousUser),
                    rolling: true,
                    cookie: {
                        maxAge : 1000*60*60*6 //6 hours
                    },
                    saveUninitialized: true,
                    resave: true
                } ;

                if(!sessionOptions || !sessionOptions.secret){
                    throw "You should at least give the secret option" ;
                }
                Object.keys(sessionOptions).forEach(function(k){
                    options[k] = sessionOptions[k] ;
                }) ;

                return session(options);
            },
            
            /**
             * if use anonymous mode, set the current user to anonymous if no user is logged
             */
            getAnynonymousMiddleware: function(anonymousUser){
                //this is the VeloxDatabaseExpress object
                let db = this.db;
                return function(req, res, next){
                    //as we allow anonymous user, if no user is authenticated, we set the current user to anonymous user
                    if(!req.user){
                        db.getByPk("velox_user", anonymousUser.uid, [
                            {name: "profile", otherTable: "velox_user_profile"},
                            {name: "realms", otherTable: "velox_link_user_realm", type: "2many", joins : [
                                {name: "realm", otherTable: "velox_user_realm"},
                                {name: "profile", otherTable: "velox_user_profile"},
                            ]},
                        ], function(err, user){
                            if(err){ 
                                throw "Can't get anonymous user "+err ;
                            }
                            req.user = user ;
                            next() ;
                        }) ;
                    }else{
                        next() ;
                    }
                } ;
            },

            /**
             * Check session and user right on each request
             * 
             * Note : the auth and logout endpoints are always public
             * 
             * @example
             * { publicUrls: ["/"]} //everything is public
             * 
             * { publicUrls: ["/public"]} //only /public is public, other endpoint need logged user
             * 
             * { privateUrls: ["/private"]} //only /private is restricted to logged user, other endpoints are public
             * 
             * { 
             *     publicUrls: ["/public"], //"/public" is public for not logged user
             *     byProfile : {
             *      "ADMIN" : { authorizedUrls: ["/"] }, //ADMIN user has access to everything
             *      "LEVEL1" : { authorizedUrls: ["/level1", "level0"] }, //LEVEL1 user has access to "/level1" and "/level0" URL
             *      "LEVEL2" : { restrictedUrls: ["/admin"] }, //LEVEL2 user has access to everything except "/admin"
             *      "SPECIAL": { checkUrl: function(logged, url, user){...}} //apply a special rule for SPECIAL users
             *     }
             * } 
             * 
             * { checkUrl: function(logged, url, user){...}} //custom check
             * 
             * @param {object} options the session check option (see examples)
             * @param {object} globalDatabaseOptions the global database options (the one passed to VeloxDatabase object)
             */
            getSessionCheckMiddleware: function(options, globalDatabaseOptions){
                //this is the VeloxDatabaseExpress object
                if(!options){
                    options = {} ;
                }
                let checkUrl = function(){ return true ; } ;
                if(options.byProfile){
                    //accessible URL depends on profile
                    checkUrl = function(logged, url, user){
                        if(options.publicUrls){
                            //check general public URLS
                            if(options.publicUrls.some(function(publicUrl){
                                if(url.indexOf(publicUrl) === 0){
                                    return true; //it is a public URL
                                }
                                return false; 
                            })){
                                //it is a global public url
                                return true ;
                            }
                        }
                        if(!logged){ return false ; }//not logged and not a public URL

                        if(!user || !user.profile){
                            return false ;//no user profile
                        }
                        let byProfile = options.byProfile[user.profile.code] ;
                        if(!byProfile){
                            return false; //no configuration for this profile assume no right
                        }
                        if(byProfile.restrictedUrls){
                            //profile access to everything except restricted url
                            return options.byProfile.restrictedUrls.every(function(privateUrl){
                                if(url.indexOf(privateUrl) === 0){
                                    return false; //it is a private URL
                                }
                                return true; //ok 
                            });
                        }else if(byProfile.authorizedUrls){
                            //profile access to nothing except authorized url
                            return options.byProfile.authorizedUrls.some(function(publicUrl){
                                if(url.indexOf(publicUrl) === 0){
                                    return true; //it is an authorized URL
                                }
                                return false;
                            });
                        }else if(byProfile.checkUrl){
                            return byProfile.checkUrl(url, user) ;
                        }
                    } ;
                }else if(options.privateUrls){
                    //everything public except private URLS accessible to connected user
                    checkUrl = function(logged, url, user){ 
                        if(logged && //is logged
                            (
                                !globalOptions.anonymousUser //no anonymous defined
                                || user.login !== globalOptions.anonymousUser.login //anonymous defined and user is not anonymous
                            )
                        ){ return true ; } //user is connected

                        return options.privateUrls.every(function(privateUrl){
                            if(url.indexOf(privateUrl) === 0){
                                return false; //it is a private URL
                            }
                            return true; //ok 
                        });
                    } ;
                } else if(options.publicUrls){
                    //everything only accessible to connected user except public URLS
                    checkUrl = function(logged, url, user){ 
                        if(logged && //is logged
                            (
                                !globalOptions.anonymousUser //no anonymous defined
                                || user.login !== globalOptions.anonymousUser.login //anonymous defined and user is not anonymous
                            )
                        ){ return true ; } //user is connected

                        return options.publicUrls.some(function(publicUrl){
                            if(url.indexOf(publicUrl) === 0){
                                return true; //it is a public URL
                            }
                            return false; 
                        });
                    } ;
                } else if(options.checkUrl){
                    checkUrl = options.checkUrl ;
                }

                return function(req, res, next){
                    if(req.url.indexOf(globalOptions.authEndPoint || "/auth/user") === 0){
                        return next(); //always accept auth endpoint
                    }
                    if(globalOptions.google && req.url.indexOf(globalOptions.google.authEndPoint || "/auth/google") === 0){
                        return next(); //always accept google auth endpoint
                    }
                    if(req.url.indexOf(globalOptions.logoutEndPoint|| "/logout") === 0){
                        return next(); //always accept logout endpoint
                    }
                    if(req.url.indexOf(globalOptions.createUser|| "/createUser") === 0){
                        return next(); //always accept create user endpoint
                    }

                    if(!options.makeSchemaPrivate){
                        if(req.url.indexOf(globalDatabaseOptions.dbEntryPoint+"/schema") === 0){
                            return next(); //always accept schema endpoint
                        }
                    }
                    

                    if(!checkUrl(req.isAuthenticated(), req.url, req.user)){
                        return res.status(401).end();
                    } ;

                    next() ;
                } ;
            },

            /**
             * put the db object on the req object with context initialized to properly handle user right
             */
            getDbInstanceMiddleware : function(){
                return (req, res, next)=>{
                    let dbOriginal = this.db;
                    if(req.db){
                        dbOriginal = req.db ;
                    }
                    let proxyDb = {} ;
                    for(let f of Object.getOwnPropertyNames(dbOriginal).concat(Object.getOwnPropertyNames(dbOriginal.__proto__))){
                        if(typeof(dbOriginal[f]) === "function"){
                            proxyDb[f] = dbOriginal[f].bind(dbOriginal) ;
                        }
                    }
                    let _inDatabase = dbOriginal.inDatabase ;
                    proxyDb.inDatabase = (bodyFunc, callback)=>{
                        _inDatabase.bind(dbOriginal)((client, done)=>{
                            this._setContext(client, req) ;

                            bodyFunc(client, done) ;
                        }, callback) ;
                    } ;
                    let _transaction = dbOriginal.transaction ;
                    proxyDb.transaction = (bodyFunc, callback)=>{
                        _transaction.bind(dbOriginal)((client, done)=>{
                            this._setContext(client, req) ;
                            bodyFunc(client, done) ;
                        }, callback) ;
                    } ;
                    req.db = proxyDb ;
                    next() ;
                } ;
            }
        } ;

        this.extendsExpressConfigure = [
            function(app, globalDatabaseOptions){
                //this is the VeloxDatabaseExpress object

                var sessionOptions = {secret: options.sessionSecret} ;
                if(options.sessionOptions){
                    sessionOptions = JSON.parse(JSON.stringify(options.sessionOptions)) ;
                    if(!sessionOptions.secret){
                        sessionOptions.secret = options.sessionSecret ;
                    }
                }


                const passport = require('passport');
                const session = require('express-session');
                app.use(this.getSessionMiddleware(session, sessionOptions)) ;
                
                app.use(passport.initialize());
                app.use(passport.session());

                if(options.anonymousUser){
                    app.use(this.getAnynonymousMiddleware(options.anonymousUser)) ;
                }

                
                this.configurePassport(passport, {
                    usernameField : options.usernameField,
                    passwordField : options.passwordField,
                    realmField : options.realmField,
                    google: options.google
                }) ;

                app.use(this.getSessionCheckMiddleware(options.sessionCheck, globalDatabaseOptions)) ;
                
                app.use(this.getDbInstanceMiddleware()) ;

                app.post(options.authEndPoint || "/auth/user",
                    passport.authenticate('local'),
                    function(req, res) {
                        // If this function gets called, authentication was successful.
                        // `req.user` contains the authenticated user.
                        res.json(req.user);
                });
                app.post(options.logoutEndPoint || "/logout",
                    function(req, res) {
                        req.session.destroy(function(err) {
                            if(err){ return res.status(500).json(err) ;}
                            res.end() ;
                        });
                });
                app.get(options.refreshEndPoint || "/refreshUser",
                    (req, res) => {
                        if(!req.user){ 
                            return res.status(401).end("no user"); 
                        }
                        this.db.refreshUser(req.user.uid, (err, user)=>{
                            if(err){ return res.status(500).json(err); }
                            res.json(user) ;
                        }) ;
                });
                app.post(options.activateEndPoint || "/activateUser",
                    (req, res) => {
                        this.db.activate(req.body.activationToken, req.body.password, (err, user)=>{
                            if(err){ return res.status(500).json(err); }
                            res.json(user) ;
                        }) ;
                });
                app.post(options.changePasswordEndPoint || "/changeUserPassword",
                    (req, res) => {
                        this.db.changeUserPassword(req.user.uid, req.body.oldPassword, req.body.newPassword, (err, user)=>{
                            if(err){ return res.status(500).json(err); }
                            res.json(user) ;
                        }) ;
                });
                app.post(options.changePasswordTokenEndPoint || "/changeUserPasswordToken",
                    (req, res) => {
                        this.db.changeUserPasswordToken(req.body.tokenPassword, req.body.newPassword, (err, user)=>{
                            if(err){ return res.status(500).json(err); }
                            res.json(user) ;
                        }) ;
                });
                app.post(options.requestPasswordTokenEndPoint || "/requestPasswordToken",
                    (req, res) => {
                        this.db.requestPasswordToken(req.body.userEmail, JSON.parse(req.body.email), (err, user)=>{
                            if(err){ return res.status(500).json(err); }
                            res.json(user) ;
                        }) ;
                });
                app.post(options.createEndPoint || "/createUser",
                    (req, res) => {
                        this.db.createUser(req.body.user, (err, user)=>{
                            if(err){ return res.status(500).json(err); }
                            res.json(user) ;
                        }) ;
                });


                if(options.google){
                    //google authentication is activated

                    app.post(options.google.authEndPoint || '/auth/google',
                        passport.authenticate('google-id-token'),
                        function(req, res) {
                            // If this function gets called, authentication was successful.
                            // `req.user` contains the authenticated user.
                            res.json(req.user);
                    });
                }

            }
        ] ;

        this.interceptClientQueries = [
            {name : "insert", table: "velox_user", before : this.beforeInsertOrUpdate, after: this.removePassword },
            {name : "update", table: "velox_user", before : this.beforeInsertOrUpdate, after: this.removePassword },
            {name : "remove", table: "velox_user", before : this.beforeCheckUserAllowed, after: this.removePassword },
            {name : "insert", table: "velox_link_user_realm", before : this.beforeCheckUserRealmAllowed },
            {name : "update", table: "velox_link_user_realm", before : this.beforeCheckUserRealmAllowed },
            {name : "remove", table: "velox_link_user_realm", before : this.beforeCheckUserRealmAllowed },
            {name : "insert", table: "velox_user_realm", after : this.afterInsertRealm },
            {name : "getByPk", table: "velox_user", after : this.removePassword },
            {name : "searchFirst", table: "velox_user", after : this.removePassword },
            {name : "search", table: "velox_user", after : this.removePassword },
        ] ;

        this.extendsClient.getTable_velox_user = function(){
            var client = this ; //this is the db client
            if(client.context && client.context.req && client.context.req.user){
                if(self.useProfile && self.useRealm) {
                    //must restrict read of users of same realms and profile same or lower
                    return `
                        (SELECT *
                        FROM
                        (SELECT DISTINCT u.*
                        FROM velox_user u
                        JOIN velox_link_user_realm l ON u.uid = l.user_uid
                        JOIN velox_user_profile p ON p.code = l.profile_code
                        JOIN velox_link_user_realm r ON l.realm_code = r.realm_code
                        JOIN velox_user_profile pc ON pc.code = r.profile_code
                        WHERE r.user_uid = '${client.context.req.user.uid}'
                            AND p.level>=pc.level ) AS velox_user)
                    `;
                } else if(self.useRealm) {
                    //must restrict read of users of same realms
                    return `
                        (SELECT *
                        FROM
                        (SELECT DISTINCT u.*
                        FROM velox_user u
                        JOIN velox_link_user_realm l ON u.uid = l.user_uid
                        JOIN velox_link_user_realm r ON l.realm_code = r.realm_code
                        WHERE r.user_uid = '${client.context.req.user.uid}' ) AS velox_user)
                    `;
                } else {
                    return "velox_user" ;
                }
                
            }else{
                return "velox_user" ;
            }
        };

        if(this.options.restrictedTables){
            this.extendsClient.isUnsafe = function(){
                return !!this.disableRestriction ;
            } ;
            this.extendsClient.unsafe = function(unsafeFun, callback){
                if(!callback){ callback = function(){} ;}
                let unsafeClient = this.clone() ;
                unsafeClient.disableRestriction = true ;
                unsafeFun(unsafeClient, function(err){
                    if(err){ callback(err) ;}
                    callback.apply(null, arguments) ;
                }.bind(this)) ;
            } ;
            this.interceptClientQueries.push({name: "query", before: function(sql, params, callback){
                if(!this.disableRestriction){
                    return callback("You can't execute direct query in safe mode, you must use unsafe mode. client.unsafe((client, done)=>{ client.query('unsafe query here', done) ;}, (err)=>{ // continue here is safe mode} : " + sql);
                }
                callback() ;
            }}) ;
            this.interceptClientQueries.push({name: "queryFirst", before: function(sql, params, callback){
                if(!this.disableRestriction){
                    return callback("You can't execute direct query in safe mode, you must use unsafe mode. client.unsafe((client, done)=>{ client.queryFirst('unsafe query here', done) ;}, (err)=>{ // continue here is safe mode} : " + sql) ;
                }
                callback() ;
            }}) ;
            let handleHiddenColumns = function(getTableFunc, hiddenCols){
                let sql = getTableFunc.bind(this)() ;
                if(!hiddenCols || this.disableRestriction) { return sql ;}
                return `(SELECT *, ${hiddenCols.map((c)=>{ return " NULL AS "+c ;}).join(',')} FROM ${sql} subH)` ;
            } ;
            var useProfile = this.useProfile ;
            var useRealm = this.options.useRealm ;
            for(let table of this.options.restrictedTables){

                //override the gettable to give a restricted view according to read restriction
                this.extendsClient["getTable_"+table.name] = function(){
                    return handleHiddenColumns.bind(this)(function(){
                        var client = this ; //this is the db client

                        if(client.disableRestriction){ return table.name ;} //restriction disabled

                        if(!client.context || !client.context.req || !client.context.req.user ){ return table.name ;} //no user context

                        if(table.hidden){
                            //the table is always hidden, return empty table
                            return `
                                (SELECT DISTINCT t.*
                                FROM
                                ${table.name} t
                                WHERE 0 = 1)
                            `;
                        }


                        let user = client.context.req.user ;
                        let tableFrom = table.name ;

                        if(table.readCondition){
                            //restrict on arbitrary where condition
                            tableFrom = `
                                (SELECT *
                                FROM
                                ${table.name} 
                                WHERE ${table.readCondition.replace(/\$user_uid/g, "'"+client.context.req.user.uid+"'")} )
                            ` ;
                        }

                        if(table.rules){
                            var profileLevel = user.profile ? user.profile.level : null;

                            if(!useProfile){
                                //don't use profile, so consider as "super admin"
                                profileLevel = 0 ;
                            }

                            if(profileLevel !== undefined && profileLevel !== null){
                                //This user has a global profile level

                                //check if a rule grant a read access without realm restriction
                                let hasFullReadAccess = false;
                                for(let rule of table.rules){
                                    if(!useProfile){
                                        //don't use profile, so consider as "super admin"
                                        rule.profile = 0 ;
                                    }
                                    if(rule.rights.indexOf("read") !== -1 && !rule.realmRestrict && !rule.userRestrict 
                                        && (rule.profile === profileLevel || ( rule.profile.indexOf && rule.profile.indexOf(profileLevel) !== -1 ) )){
                                            hasFullReadAccess = true ; 
                                            break ;
                                    }
                                }

                                if(hasFullReadAccess){
                                    return tableFrom ; //full read access, give back normal table
                                }
                            }

                            //no full read access granted to this user, get available rules
                            let authorizedLevelsOnRealm = [] ;
                            let authorizedLevelsOnUser = [] ;
                            for(let rule of table.rules){
                                if(!useProfile){
                                    //don't use profile, so consider as "super admin"
                                    rule.profile = 0 ;
                                }
                                if(rule.rights.indexOf("read") !== -1 && rule.realmRestrict){
                                    if(Array.isArray(rule.profile)){
                                        authorizedLevelsOnRealm = authorizedLevelsOnRealm.concat(rule.profile) ;
                                    }else{
                                        authorizedLevelsOnRealm.push(rule.profile) ;
                                    }
                                }
                                if(authorizedLevelsOnRealm.length === 0){
                                    //no authorization rule on realm, look for user
                                    if(rule.rights.indexOf("read") !== -1 && rule.userRestrict){
                                        if(Array.isArray(rule.profile)){
                                            authorizedLevelsOnUser = authorizedLevelsOnUser.concat(rule.profile) ;
                                        }else{
                                            authorizedLevelsOnUser.push(rule.profile) ;
                                        }
                                    }
                                }
                            }
                            if(authorizedLevelsOnRealm.length > 0){
                                //create a sub query restricted on realm for authorized level
                                var realmColPath = table.realmCol.split(".") ;
                                var from = `FROM ${table.name}` ;
                                var currentTable = table.name ;
                                realmColPath.forEach((p, i)=>{
                                    if(i === realmColPath.length-1){
                                        from += ` JOIN velox_link_user_realm r ON ${currentTable}.${p} = r.realm_code 
                                        JOIN velox_user u ON u.uid = r.user_uid
                                        JOIN velox_user_profile p ON p.code = COALESCE(r.profile_code, u.profile_code)
                                        ` ;
                                    }else{
                                        from += ` JOIN ${p} `+createJoinOnFromFk(client.cache.schema, currentTable, p) ;
                                        currentTable = p ;
                                    }
                                }) ;
                                return `
                                    (SELECT DISTINCT ${table.name}.*
                                    ${from} 
                                    
                                    WHERE r.user_uid = '${client.context.req.user.uid}' AND p.level IN (${authorizedLevelsOnRealm.join(", ")}))
                                `;
                            } else if (authorizedLevelsOnUser.length > 0) {
                                    //create a sub query restricted on user for authorized level
                                    var userColPath = table.userCol.split(".") ;
                                    var from = `FROM ${table.name}` ;
                                    var currentTable = table.name ;
                                    userColPath.forEach((p, i)=>{
                                        if(i === userColPath.length-1){
                                            from += ` JOIN velox_user u ON ${currentTable}.${p} = u.uid
                                            JOIN velox_user_profile p ON u.profile_code = p.code
                                            ` ;
                                        }else{
                                            from += ` JOIN ${p} `+createJoinOnFromFk(client.cache.schema, currentTable, p) ;
                                            currentTable = p ;
                                        }
                                    }) ;

                                    return `(SELECT DISTINCT ${table.name}.*
                                         ${from} 
                                        WHERE u.uid = '${client.context.req.user.uid}' AND p.level IN (${authorizedLevelsOnUser.join(", ")})
                                    )`;
                            } else {
                                //no authorization on user neither, give back fake empty table
                                return ` (SELECT t.* FROM ${table.name} t WHERE 0 = 1) ` ;
                            }
                        }else{
                            //no restriction rules
                            return tableFrom ;
                        }
                    }, table.hiddenCols) ;
                };

                var createRestrictFunction = function(table, action){
                    return function(tableName, record, callback){
                        if(!this.disableRestriction && this.context && this.context.req && this.context.req.user){

                            let user = this.context.req.user ;

                            //force user col if any
                            if(tableName !== 'velox_user' && table.userCol && table.userCol.indexOf(".") === -1 && !record[table.userCol]){
                                record[table.userCol] = user.uid;
                            }

                            if(tableName !== 'velox_user' && table.realmCol && table.realmCol.indexOf(".") === -1 && !record[table.realmCol] && user.realms.length>0){
                                record[table.realmCol] = user.realms[0].realm_code;
                            }

                            if(table.rules){
                                var profileLevel = user.profile ? user.profile.level : null;
    
                                if(profileLevel !== undefined && profileLevel !== null){
                                    //This user has a global profile level
    
                                    //check if a rule grant an access without realm restriction
                                    let hasFullReadAccess = false;
                                    for(let rule of table.rules){
                                        if(rule.rights.indexOf(action) !== -1 && !rule.realmRestrict 
                                            && (rule.profile === profileLevel || ( rule.profile.indexOf && rule.profile.indexOf(profileLevel) !== -1 ) )){
                                                hasFullReadAccess = true ; 
                                                break ;
                                        }
                                    }
    
                                    if(hasFullReadAccess){
                                        return callback() ; //full access OK
                                    }
                                }
    
                                //no full access granted to this user, get available rules
                                let authorizedLevelsOnRealm = [] ;
                                for(let rule of table.rules){
                                    if(rule.rights.indexOf(action) !== -1 && rule.realmRestrict){
                                        if(Array.isArray(rule.profile)){
                                            authorizedLevelsOnRealm = authorizedLevelsOnRealm.concat(rule.profile) ;
                                        }else{
                                            authorizedLevelsOnRealm.push(rule.profile) ;
                                        }
                                    }
                                }
                                if(authorizedLevelsOnRealm.length === 0){
                                    //no authorization rule on realm, look for user
                                    let authorizedLevelsOnUser = [] ;
                                    for(let rule of table.rules){
                                        if(rule.rights.indexOf(action) !== -1 && rule.userRestrict){
                                            if(Array.isArray(rule.profile)){
                                                authorizedLevelsOnUser = authorizedLevelsOnUser.concat(rule.profile) ;
                                            }else{
                                                authorizedLevelsOnUser.push(rule.profile) ;
                                            }
                                        }
                                    }
                                        
                                    if(authorizedLevelsOnUser.length === 0){
                                        //no authorization on user neither, give back fake empty table
                                        return callback("No rule for "+action+" permitted to "+table.name) ;
                                    }else{
                                        //check if given user is correct on record
                                        var userColPath = table.userCol.split(".") ;

                                        if(userColPath.length === 1){
                                            if(record[table.userCol] !== user.uid){
                                                return callback("You're not allowed to set user "+record[table.userCol]+" in table "+table.name) ;
                                            }
                                            if(authorizedLevelsOnUser.indexOf(profileLevel) === -1){
                                                return callback("You're not allowed for action "+action+" on table "+table.name) ;
                                            }
                                        }else if(userColPath.length > 1){
                                            //search uid on related table

                                            var currentTable = userColPath[0] ;
                                            var from = `FROM ${currentTable}` ;
                                            userColPath.forEach((p, i)=>{
                                                if(i === userColPath.length-1){
                                                    from += ` JOIN velox_user u ON ${currentTable}.${p} = u.uid
                                                    JOIN velox_user_profile p ON u.profile_code = p.code
                                                        ` ;
                                                }else if(i>0){
                                                    from += ` JOIN ${p} `+createJoinOnFromFk(this.cache.schema, currentTable, p) ;
                                                    currentTable = p ;
                                                }
                                            }) ;

                                            let params = [] ;
                                            let whereCols = getJoinPairsFromFk(this.cache.schema, table.name, userColPath[0]) ;
                                            let where = Object.keys(whereCols).map((thisCol)=>{
                                                params.push(record[thisCol]) ;
                                                return userColPath[0]+"."+whereCols[thisCol] + " = $"+params.length ;
                                            }).join(" AND ") ;

                                            var sql = `(SELECT 1
                                                ${from} 
                                                WHERE u.uid = '${this.context.req.user.uid}' AND p.level IN (${authorizedLevelsOnUser.join(", ")})
                                                AND ${where}
                                            )`;
                                            this.unsafe((txUnsafe, done)=>{
                                                txUnsafe._query(sql, params, done) ;
                                            }, (err, results)=>{
                                                if(err){ return callback(err) ;}
                                                if(results.rows.length === 0){
                                                    return callback("You're not allowed for action "+action+" on table "+table.name) ;
                                                } else {
                                                    return callback(); //OK
                                                }
                                            }) ;
                                        }
                                    }
                                }else{
                                    //check if realm is authorized
                                    var realmColPath = table.realmCol.split(".") ;

                                    if(realmColPath.length > 0){
                                        //check realm realm on related table

                                        var currentTable = table.name;
                                        var from = `FROM velox_link_user_realm r JOIN velox_user u ON r.user_uid = u.uid
                                        JOIN velox_user_profile p ON COALESCE(r.profile_code, u.profile_code) = p.code` ;
                                        let where = "";
                                        let params = [] ;
                                        if(realmColPath.length>1){
                                            currentTable = realmColPath[0] ;
                                            var columnName = realmColPath[realmColPath.length-1] ;
                                            for(let i=realmColPath.length-2; i>=0; i--){
                                                var tableName = realmColPath[i] ;
                                                if(i===realmColPath.length-2){
                                                    from += ` JOIN ${tableName} ON ${tableName}.${columnName} = r.realm_code` ;
                                                }else{
                                                    var previousTable = realmColPath[i+1] ;
                                                    from += ` JOIN ${tableName} `+createJoinOnFromFk(this.cache.schema, previousTable, tableName) ;
                                                }
                                            }
                                            let whereCols = getJoinPairsFromFk(this.cache.schema, table.name, realmColPath[0]) ;
                                            where = Object.keys(whereCols).map((thisCol)=>{
                                                params.push(record[thisCol]) ;
                                                return realmColPath[0]+"."+whereCols[thisCol] + " = $"+params.length ;
                                            }).join(" AND ") ;
                                        } else {
                                            params.push(record[realmColPath[0]]) ;
                                            where = "r.realm_code = $"+params.length ;
                                        }
                                        
                                        var sql = `(SELECT 1
                                            ${from} 
                                            WHERE r.user_uid = '${this.context.req.user.uid}' AND p.level IN (${authorizedLevelsOnRealm.join(", ")})
                                            AND ${where}
                                        )`;
                                        this.unsafe((txUnsafe, done)=>{
                                            txUnsafe._query(sql, params, done) ;
                                        }, (err, results)=>{
                                            if(err){ return callback(err) ;}
                                            if(results.rows.length === 0){
                                                return callback("You're not allowed for action "+action+" on table "+table.name) ;
                                            } else {
                                                return callback(); //OK
                                            }
                                        }) ;
                                    }
                                }    
                            }else{
                                //no restriction rules
                                return callback() ;
                            }
                        } else {
                            callback() ;
                        }
                    } ;
                };
                
                this.interceptClientQueries.push(
                    {name : "insert", table: table.name, before : createRestrictFunction(table, "insert") }
                );
                this.interceptClientQueries.push(
                    {name : "removeWhere", table: table.name, before : createRestrictFunction(table, "remove") }
                );
                this.interceptClientQueries.push(
                    {name : "update", table: table.name, before : createRestrictFunction(table, "update") }
                );
                
                    
                this.interceptClientQueries.push(
                    {name : "remove", table: table.name, before : createRestrictFunction(table, "remove") }
                );

                this.interceptClientQueries.push(
                    {name : "update", table: table.name, after : function(tableName, record, callback){
                            let client = this;
                            if(!client.cache.schema.velox_modif_track){
                                return callback() ;
                            }
                            if(!this.context || !this.context.req || !this.context.req.user){
                                return callback() ;
                            }
                            let user = this.context.req.user ;

                            let userUid = null;
                            let realmCode = null;

                            if(user){
                                userUid = user.uid;
                            }

                            if(user && user.realms.length>0){
                                realmCode = user.realms[0].realm_code;
                            }

                            var tableDef = client.cache.schema[tableName] ;
                            let pkValue = tableDef.pk.map(function(pk){
                                return ""+record[pk] ;
                            }).join("$_$") ;
                            client.unsafe((client, done)=>{ 
                                client.query("UPDATE velox_modif_track SET realm_code = $1, user_uid = $2 WHERE table_name = $3 AND table_uid = $4", 
                                [realmCode, userUid, tableName, pkValue], (err)=>{
                                    if(err){ return done(err) ;}
                                    done() ;
                                }) ;
                            }, callback) ;
                            
                        } 
                    }
                );
            }
        }
    }

    

    /**
     * Check if the current user can interract with this user
     * 
     * @param {string} table table name
     * @param {object} record user record
     * @param {function} callback called on finished
     */
    beforeCheckUserAllowed(table, record, callback){
        this.getByPk(table, record, function(err, foundUser){
            if(err){ return callback(err) ;}
            if(!foundUser){
                //this user is not accessible from current user, don't allow to modify it
                return callback("You are not allowed to modify this user") ;
            }
            callback() ;//ok user found in allowed user, accept it
        }) ;
    }

    /**
     * Check if the current user has enough rights on this realm and profile
     * 
     * @param {string} table table name
     * @param {object} record user record
     * @param {function} callback called on finished
     */
    beforeCheckUserRealmAllowed(table, record, callback){
        if(this.context && this.context.req && this.context.req.user){
            //check current user is allowed on this realm
            this.searchFirst("velox_user_profile", {code : this.context.req.user.profile_code}, (err, userProfile)=>{
                if(err){ return callback(err) ;}
                if(userProfile && userProfile.full_realm_access){
                    //this user has a profile with full realm access, skip further tests
                    return callback() ;
                }

                this.search("velox_link_user_realm", {user_uid : this.context.req.user.uid}, (err, currentUserRealms)=>{
                    if(err){ return callback(err) ;}
                    let thisRealmLine = null;
                    currentUserRealms.some((r)=>{
                        if(r.realm_code === record.realm_code){
                            thisRealmLine = r;
                            return true ;
                        }
                    }) ;
                    if(!thisRealmLine){
                        return callback("You are not allowed on realm "+record.realm_code) ;
                    }
                    if(!record.profile_code){ return callback() ;}

                    //check current user has profile higher or equal to requested profile
                    this.searchFirst("velox_user_profile", {code : thisRealmLine.profile_code}, (err, currentUserProfile)=>{
                        if(err){ return callback(err) ;}
                        this.searchFirst("velox_user_profile", {code : record.profile_code}, (err, askedUserProfile)=>{
                            if(err){ return callback(err) ;}
                            if(currentUserProfile.level > askedUserProfile.level){
                                return callback("You don't have access to profile "+askedUserProfile.code) ;
                            }
                            callback() ;
                        }) ;
                    }) ;
                }) ;
            }) ;
        }else{
            callback() ;
        }
    }

    /**
     * hash password on insert/update
     * 
     * @private
     * @param {string} table table name
     * @param {object} record record to insert or update
     * @param {function(Error)} callback called on finish
     */
    beforeInsertOrUpdate(table, record, callback){
        if(record.password){
            if(record.password === FAKE_PASSWORD){
                //fake password, remove
                delete record.password ;
                return callback() ;
            }
            bcrypt.hash(record.password, 10, function(err, hash) {
                if(err){ return callback(err); }
                record.password = hash ;
                callback() ;
            });
        } else {
            callback() ;
        }
    }

    /**
     * replace password by a placeholder
     * 
     * @private
     * @param {Array} records list of found users
     */
    removePassword(table, records){
        if(!records){ return; }
        if(!Array.isArray(records)){
            records = [records] ;
        }
        records.forEach(function(record){
            record.password = FAKE_PASSWORD ;
        });
    }

    /**
     * Register the current user on realm as it has created it
     * 
     * @param {object} realm the inserted realm 
     * @param {function} callback 
     */
    afterInsertRealm(table, realm, callback){
        if(this.context && this.context.req && this.context.req.user){
            //automatically link current user to this new realm

            //search profile to use
            this._queryFirst("SELECT profile_code FROM velox_link_user_realm WHERE user_uid = $1 LIMIT 1", [this.context.req.user.uid], (err, profile)=>{
                if(err){ return callback(err) ;}
                if(!profile){
                    //no existing link, this user is probably not restricted to realm
                    return callback() ;
                }
                //don't use insert on purpose to avoid right check that will say that this user does not belong to this realm
                this._query("INSERT INTO velox_link_user_realm(user_uid, realm_code, profile_code) VALUES ($1, $2, $3)", [this.context.req.user.uid, realm.code, profile.profile_code], callback) ;
            }) ;
            
        }else{
            callback() ;
        }
    }

    authenticateGoogleUser(db, token, callback){
        if(!this.options.google || !this.options.google.clientID){
            return callback("Google CLIENT ID missing") ;
        }
        //see documentation https://developers.google.com/identity/sign-in/web/backend-auth
        // var auth = new GoogleAuth;
        // var client = new auth.OAuth2(this.options.google.clientID, '', '');
        // client.verifyIdToken(
        //     token,
        //     this.options.google.clientID,
        //     function(err, login) {
        //         if(err){ return callback(err); }

        //         var payload = login.getPayload();
        //         callback(null, payload) ;
        //         var userid = payload['sub'];
        //         // If request specified a G Suite domain:
        //         //var domain = payload['hd'];
        //     });
    }

    /**
     * Authenticate the user
     * 
     * Return the user if succeed with profile as user.profile property
     * 
     * @param {VeloxDatabase} db the db access
     * @param {string} login user login
     * @param {string} password user password (not hashed)
     * @param {string} [realm] the realm in which search the user
     * @param {function(err, user)} callback called with user if succeed
     */
    authenticateUser(db, login, password, realm, callback) {
        if(typeof(realm) === "function"){
            callback = realm ;
            realm = null;
        }
        db.inDatabase((client, done)=>{
            let sql = "SELECT *, profile_code as profile FROM velox_user WHERE login = $1 AND disabled = FALSE AND active = TRUE" ;
            let params = [login];
            if(realm){
                sql = "SELECT u.*, l.profile_code as profile FROM velox_user u JOIN velox_link_user_realm l ON u.uid = l.user_uid WHERE l.realm_code = $1 AND u.login = $2 AND u.disabled = FALSE AND u.active = TRUE" ;
                params = [realm, login] ;
            }
            client._query(sql, params, (err, results)=>{
                if(err){ return done(err); }

                if(results.rows.length === 0){
                    return done(null, false) ;
                }

                if(results.rows.length > 1){
                    //there is a problem in the configuration somewhere
                    db.logger.error("The user "+login+" exists many times in realm "+realm) ;
                    return done(null, false) ;
                }

                let user = results.rows[0] ;

                bcrypt.compare(password, user.password, (err, checkPassOk)=>{
                    if(err){ return done(err); }
                    if(!checkPassOk){
                        return done(null, false) ;
                    }

                    this.removePassword(user) ;

                    client.search("velox_link_user_realm", {user_uid : user.uid}, [
                        {name : "realm", otherTable: "velox_user_realm"},
                        {name : "profile", otherTable: "velox_user_profile"}
                    ], "realm_code", (err, realms)=>{
                        if(err){ return done(err); }
                        user.realms = realms.map((r)=>{
                            return {realm: r.realm, profile: r.profile} ;
                        }) ;

                        if(user.profile){
                            client.getByPk("velox_user_profile", user.profile, (err, profile)=>{
                                if(err){ return done(err); }
                                user.profile = profile ;
                                return done(null, user) ;
                            }) ;
                        } else {
                            return done(null, user) ;
                        }
                    }) ;

                });
                
            }) ;
        }, (err,result)=>{
            if(err){
                db.logger.warn("Authenticate user "+login+" failed ", err) ;
                return callback(err) ;
            }
            callback(null, result) ;
        }) ;
    }

    /**
     * Create a new user
     * 
     * @param {VeloxDatabase} db the db access
     * @param {object} user the user to create
     * @param {boolean} mustActivate if true the user must activate the account
     * @param {function(err, user)} callback called with created user if succeed
     */
    createUser(db, user, callback){
        db.transaction((client, done)=>{
            this.createUserInTransaction(client, user, done) ;
        }, callback) ;
    }
   
    /**
     * Create a new user
     * 
     * @param {VeloxDatabaseClient} client the db client access
     * @param {object} user the user to create
     * @param {function(err, user)} callback called with created user if succeed
     */
    createUserInTransaction(client, user, callback){
        delete user.profile_code; //remove profile code if someone is trying to inject it
        if(this.options.mustActivate){
            user.active = false ;
            user.activation_token = uuid.v4() ;
        }else{
            user.active = true ;
        }
        if(!user.uid){ user.uid = uuid.v4() ; }
        client.insert("velox_user", user, callback) ;
    }

    /**
     * Change user password
     * 
     * @param {VeloxDatabase} db the db access
     * @param {string} oldPassword the current password
     * @param {string} newPassword the new password
     * @param {function(err, boolean)} callback called with true if succeed
     */
    changePassword(db, userUid, oldPassword, newPassword, callback){
        db.transaction((client, done)=>{
            let sql = "SELECT *, profile_code as profile FROM velox_user WHERE uid = $1 AND disabled = FALSE AND active = TRUE" ;
            let params = [userUid];
            client._query(sql, params, (err, results)=>{
                if(err){ return done(err); }

                if(results.rows.length === 0){
                    return done(null, false) ;
                }
                var user = results.rows[0] ;

                bcrypt.compare(oldPassword, user.password, (err, checkPassOk)=>{
                    if(err){ return done(err); }
                    if(!checkPassOk){
                        return done(null, false) ;
                    }

                    var updateData = {password: newPassword, uid: user.uid} ;
                    client.update("velox_user", updateData, (err, user)=>{
                        if(err){ return done(err); }
                        return done(null, true) ;
                    }) ;
                });
            });
        }, callback) ;
    }
   
    /**
     * Change user password with token
     * 
     * @param {VeloxDatabase} db the db access
     * @param {string} passwordToken the password token
     * @param {string} newPassword the new password
     * @param {function(err, boolean)} callback called with true if succeed
     */
    changePasswordToken(db, passwordToken, newPassword, callback){
        db.transaction((client, done)=>{
            let sql = "SELECT *, profile_code as profile FROM velox_user WHERE password_token = $1 AND (password_token_validity IS NULL OR password_token_validity > now())" ;
            let params = [passwordToken];
            client._query(sql, params, (err, results)=>{
                if(err){ return done(err); }

                if(results.rows.length === 0){
                    return done(null, false) ;
                }
                var user = results.rows[0] ;

               
                var updateData = {password: newPassword, uid: user.uid} ;
                client.update("velox_user", updateData, (err, user)=>{
                    if(err){ return done(err); }
                    return done(null, true) ;
                }) ;
            });
        }, callback) ;
    }

    /**
     * Change user password with token
     * 
     * @param {VeloxDatabase} db the db access
     * @param {string} userEmail User email
     * @param {object} email email object
     * @param {function(err, boolean)} callback called with true if succeed
     */
    requestPasswordToken(db, userEmail, email, callback){
        db.transaction((client, done)=>{
            let sql = "SELECT *, profile_code as profile FROM velox_user WHERE email = $1" ;
            let params = [userEmail];
            client._query(sql, params, (err, results)=>{
                if(err){ return done(err); }

                if(results.rows.length === 0){
                    return done(null, false) ;
                }
                var user = results.rows[0] ;

                email.to_addr = user.email ;
               
                var updateData = {activation_token: uuid.v4(), activation_token_validity: new Date(new Date().getTime()+(2*60*60*1000)), uid: user.uid} ;
                email.text = (email.text || "").replace(new RegExp("__activation_token__", "g"), updateData.activation_token);
                email.html = (email.html || "").replace(new RegExp("__activation_token__", "g"), updateData.activation_token);

                client.insert("velox_mail", email, (err)=>{
                    if(err){ return done(err); }
                    client.update("velox_user", updateData, (err)=>{
                        if(err){ return done(err); }
                        return done(null, true) ;
                    }) ;
                }) ;
            });
        }, callback) ;
    }

    /**
     * Activate the user using the activation token
     * 
     * Optionnaly set the password
     * 
     * @param {VeloxDatabase} db the db access
     * @param {string} activationToken user password (not hashed)
     * @param {string} [password] user password (not hashed)
     * @param {function(err, user)} callback called with user if succeed
     */
    activateUser(db, activationToken, password, callback) {
        if(typeof(password) === "function"){
            callback = password ;
            password = null;
        }
        db.transaction((client, done)=>{
            let sql = "SELECT * FROM velox_user WHERE activation_token = $1" ;
            let params = [activationToken];
            client._query(sql, params, (err, results)=>{
                if(err){ return done(err); }

                if(results.rows.length === 0){
                    return done("INVALID_TOKEN") ;
                }

                if(results.rows.length > 1){
                    //there is a problem in the configuration somewhere
                    db.logger.error("The activation token "+activationToken+" exists many times ") ;
                    return done("INVALID_TOKEN") ;
                }

                let user = results.rows[0] ;

                if(user.active){
                    //already active
                    return done("ALREADY_ACTIVE") ;
                }

                var updateData = {active: true, uid: user.uid} ;
                if(password){
                    updateData.password = password ;
                }
                client.update("velox_user", updateData, (err, user)=>{
                    if(err){ return done(err); }
                    return done(null, user) ;
                }) ;
            }) ;
        }, (err,result)=>{
            if(err){
                db.logger.warn("Activate user "+activationToken+" failed ", err) ;
                return callback(err) ;
            }
            callback(null, result) ;
        }) ;
    }

    /**
     * Get a refreshed copy of user
     * 
     * @param {VeloxDatabase} db the database access
     * @param {string} uid the user id
     * @param {function} callback called with refreshed user
     */
    refreshUser(db, uid, callback){
        db.inDatabase((client, done)=>{
            client.getByPk("velox_user", uid, [
                {name: "profile", otherTable: "velox_user_profile"},
                {name: "realms", otherTable: "velox_link_user_realm", type:"2many", joins: [
                    {name: "realm", otherTable: "velox_user_realm"},
                    {name: "profile", otherTable: "velox_user_profile"},
                ]},
            ],  done);
        }, callback);
    }

    _getFullUser(client, user, callback){
        this.removePassword(user) ;
        
        client.search("velox_link_user_realm", {user_uid : user.uid}, [
            {name : "realm", otherTable: "velox_user_realm"},
            {name : "profile", otherTable: "velox_user_profile"}
        ], "realm_code", (err, realms)=>{
            if(err){ return callback(err); }
            user.realms = realms.map((r)=>{
                return {realm: r.realm, profile: r.profile} ;
            }) ;

            if(user.profile){
                client.getByPk("velox_user_profile", user.profile, (err, profile)=>{
                    if(err){ return callback(err); }
                    user.profile = profile ;
                    return callback(null, user) ;
                }) ;
            } else {
                return callback(null, user) ;
            }
        }) ;
    }


    /**
     * Add needed schema changes on schema updates
     * 
     * @param {string} backend 
     */
    prependSchemaChanges(backend){
        if(["pg"].indexOf(backend) === -1){
            throw "Backend "+backend+" not handled by this extension" ;
        }

        let changes = [] ;

        changes.push({
            sql: this.getCreateTableProfile(backend)
        }) ;
        changes.push({
            sql: this.getCreateTableRealm(backend)
        }) ;
        changes.push({
            sql: this.getCreateTableUser(backend)
        }) ;
        changes.push({
            sql: this.getCreateTableLinkUserRealm(backend)
        }) ;
        changes.push({
            sql: this.getCreateTableSession(backend)
        }) ;

        if(this.fixedProfiles){
            for(let profile of this.fixedProfiles){
                changes.push({
                    run: (tx, cb)=>{
                        tx.searchFirst("velox_user_profile", {code: profile.code}, (err, existingProfile)=>{
                            if(err){ return cb(err); }
                            if(existingProfile){
                                return tx.update("velox_user_profile", profile, cb) ;
                            }
                            tx.insert("velox_user_profile", profile, cb) ;
                        }) ;
                    }
                }) ;
            }
        }

        if(!this.dontCreateAdmin){ 
            if(this.adminProfile && typeof(this.adminProfile) === "object"){
                changes.push({
                    run: (tx, cb)=>{
                        tx.searchFirst("velox_user_profile", {code: this.adminProfile.code}, (err, adminProfile)=>{
                            if(err){ return cb(err); }
                            if(adminProfile){
                                return tx.update("velox_user_profile", this.adminProfile, cb) ;
                            }
                            tx.insert("velox_user_profile", this.adminProfile, cb) ;
                        }) ;
                    }
                }) ;
            }
            
            if(this.defaultRealms){
                for(let realm of this.defaultRealms){
                    changes.push({
                        run: (tx, cb)=>{
                            tx.searchFirst("velox_user_realm", { code: realm.code}, (err, existingRealm)=>{
                                if(err){ return cb(err); }
                                if(existingRealm){
                                    return tx.update("velox_user_realm", realm, cb) ;
                                }
                                tx.insert("velox_user_realm", realm, cb) ;
                            }) ;
                        }
                    }) ;
                }
            }

            changes.push({
                run: (tx, cb)=>{
                    tx.searchFirst("velox_user", {login: this.adminUser.login}, (err, adminUser)=>{
                        if(err){ return cb(err); }
                        if(adminUser){
                            this.adminUser.uid = adminUser.uid;
                            //update only the profile, other information may have been change by user
                            return tx.update("velox_user", {uid: adminUser.uid, profile_code: this.adminUser.profile_code}, cb) ;
                        }
                        tx.insert("velox_user", this.adminUser, cb) ;
                    }) ;
                }
            }) ;

            if(this.anonymousUser){
                changes.push({
                    run: (tx, cb)=>{
                        tx.searchFirst("velox_user", {login: this.anonymousUser.login}, (err, anonUser)=>{
                            if(err){ return cb(err); }
                            if(anonUser){
                                this.anonymousUser.uid = anonUser.uid;
                                //update only the profile, other information may have been change by user
                                return tx.update("velox_user", {uid: anonUser.uid, profile_code: this.anonymousUser.profile_code}, cb) ;
                            }
                            tx.insert("velox_user", this.anonymousUser, cb) ;
                        }) ;
                    }
                }) ;
            }

            if(this.adminUser.realms){
                for(let realm of this.adminUser.realms){
                    changes.push({
                        run: (tx, cb)=>{
                            realm.user_uid = this.adminUser.uid ;
                            tx.getByPk("velox_link_user_realm", realm, (err, existingRealm)=>{
                                if(err){ return cb(err); }
                                if(existingRealm){
                                    return cb() ;
                                }
                                tx.insert("velox_link_user_realm", realm, cb) ;
                            }) ;
                        }
                    }) ;
                }
            }
        }

        
        
        return changes;
    }

    /**
     * Create the table velox_user_profile if not exists
     * @param {string} backend 
     */
    getCreateTableProfile(backend){
        let metaLines = this.profileMeta.map((m)=>{
            return m.name+" "+m.type;
        }) ;
        let lines = [
            "code VARCHAR(30) PRIMARY KEY",
            "level INT",
            "full_realm_access BOOLEAN",
            "name VARCHAR(75)"
        ].concat(metaLines) ;
        if(backend === "pg"){
            return `
            CREATE TABLE IF NOT EXISTS velox_user_profile (
                ${lines.join(",")}
            )
            ` ;
        }
        throw "not implemented for backend "+backend ;
    }

    /**
     * Create the table velox_user_realm if not exists
     * @param {string} backend 
     */
    getCreateTableRealm(backend){
        if(backend === "pg"){
            return `
            CREATE TABLE IF NOT EXISTS velox_user_realm (
                code VARCHAR(30) PRIMARY KEY,
                name VARCHAR(75)
            )
            ` ;
        }
        throw "not implemented for backend "+backend ;
    }

    /**
     * Create the table velox_user if not exists
     * @param {string} backend 
     */
    getCreateTableUser(backend){
        let metaLines = this.userMeta.map((m)=>{
            return m.name+" "+m.type;
        }) ;
        let lines = [
            "uid VARCHAR(40) PRIMARY KEY",
            "login VARCHAR(128)",
            "auth_type VARCHAR(30)",
            "password VARCHAR(128)",
            "name VARCHAR(128)",
            "disabled BOOLEAN DEFAULT FALSE",
            "active BOOLEAN DEFAULT FALSE",
            "activation_token VARCHAR(40)",
            "password_token VARCHAR(40)",
            "password_token_validity TIMESTAMP",
            "lang VARCHAR(5)",
            "profile_code VARCHAR(30) REFERENCES velox_user_profile(code)",
        ].concat(metaLines) ;
        if(backend === "pg"){
            return `
            CREATE TABLE IF NOT EXISTS velox_user (
                ${lines.join(",")}
            )
            ` ;
        }
        throw "not implemented for backend "+backend ;
    }



    

    /**
     * Create the table velox_link_user_realm if not exists
     * @param {string} backend 
     */
    getCreateTableLinkUserRealm(backend){
        if(backend === "pg"){
            return `
            CREATE TABLE IF NOT EXISTS velox_link_user_realm (
                user_uid VARCHAR(40) REFERENCES velox_user(uid),
                realm_code VARCHAR(30) REFERENCES velox_user_realm(code),
                profile_code VARCHAR(30) REFERENCES velox_user_profile(code),
                PRIMARY KEY(user_uid, realm_code)
            )
            ` ;
        }
        throw "not implemented for backend "+backend ;
    }


    /**
     * Create the table velox_user_session if not exists
     * @param {string} backend 
     */
    getCreateTableSession(backend){
        if(backend === "pg"){
            return `
            CREATE TABLE IF NOT EXISTS velox_user_session (
                sid VARCHAR(128) PRIMARY KEY,
                contents VARCHAR(1024),
                user_uid VARCHAR(40) REFERENCES velox_user(uid),
                expire timestamp without time zone
            )
            ` ;
        }
        throw "not implemented for backend "+backend ;
    }
    
}

/**
 * Create the JOIN "ON" condition between 2 table using FK definitions
 * 
 * @param {object} schema the database schema
 * @param {string} thisTable starting table name
 * @param {string} otherTable destination table name
 */
function createJoinOnFromFk(schema, thisTable, otherTable){
    let pairs = getJoinPairsFromFk(schema, thisTable, otherTable) ;

    if(Object.keys(pairs).length === 0){
        throw "Can't create JOIN condition between "+thisTable+" and "+otherTable+", can't find suitable foreign key to do so" ;
    }
    return " ON "+Object.keys(pairs).map((left)=>{ return thisTable+"."+left+" = "+otherTable+"."+pairs[left] ;}).join(" AND ") ;
}

/**
 * Get the JOIN "ON" pairs
 * 
 * @param {object} schema the database schema
 * @param {string} thisTable starting table name
 * @param {string} otherTable destination table name
 */
function getJoinPairsFromFk(schema, thisTable, otherTable){
    let pairs = {} ;

    //look in this table FK
    for(let fk of schema[thisTable].fk){
        if(fk.targetTable === otherTable){
            pairs[fk.thisColumn] = fk.targetColumn ;
        }
    }

    if(Object.keys(pairs).length === 0){
        //look in other table FK
        for(let fk of schema[otherTable].fk){
            if(fk.targetTable === thisTable){
                pairs[fk.targetColumn] = fk.thisColumn ;
            }
        }
    }
    return pairs ;
}

module.exports = VeloxUserManagment;