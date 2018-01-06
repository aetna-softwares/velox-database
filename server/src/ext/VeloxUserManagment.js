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

        if(this.adminUser.active === undefined){
            this.adminUser.active = true ;
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
                        client.getByPk("velox_user", uid, done) ;
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
                    store: new VeloxUserManagmentSessionStore(this.db),
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
                    checkUrl = function(logged, url){ 
                        if(logged){ return true ; } //user is connected

                        return options.privateUrls.every(function(privateUrl){
                            if(url.indexOf(privateUrl) === 0){
                                return false; //it is a private URL
                            }
                            return true; //ok 
                        });
                    } ;
                } else if(options.publicUrls){
                    //everything only accessible to connected user except public URLS
                    checkUrl = function(logged, url){ 
                        if(logged){ return true ; } //user is connected

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
                        if(!req.user){ return res.status(401).end("no user"); }
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
                    return callback("You can't execute direct query in safe mode, you must use unsafe mode. client.unsafe((client, done)=>{ client.query('unsafe query here', done) ;}, (err)=>{ // continue here is safe mode}");
                }
                callback() ;
            }}) ;
            this.interceptClientQueries.push({name: "queryFirst", before: function(sql, params, callback){
                if(!this.disableRestriction){
                    return callback("You can't execute direct query in safe mode, you must use unsafe mode. client.unsafe((client, done)=>{ client.queryFirst('unsafe query here', done) ;}, (err)=>{ // continue here is safe mode}") ;
                }
                callback() ;
            }}) ;
            let handleHiddenColumns = function(getTableFunc, hiddenCols){
                let sql = getTableFunc.bind(this)() ;
                if(!hiddenCols || this.disableRestriction) { return sql ;}
                return `(SELECT *, ${hiddenCols.map((c)=>{ return " NULL AS "+c ;}).join(',')} FROM ${sql} subH)` ;
            } ;
            for(let table of this.options.restrictedTables){
                var insertMinProfileLevel = table.insertMinProfileLevel;
                var updateMinProfileLevel = table.updateMinProfileLevel;
                var removeMinProfileLevel = table.removeMinProfileLevel;
                var readMinProfileLevel = table.readMinProfileLevel;
                if(table.minProfileLevel){
                    insertMinProfileLevel = insertMinProfileLevel === undefined?table.minProfileLevel:insertMinProfileLevel;
                    updateMinProfileLevel = updateMinProfileLevel === undefined?table.minProfileLevel:updateMinProfileLevel;
                    removeMinProfileLevel = removeMinProfileLevel === undefined?table.minProfileLevel:removeMinProfileLevel;
                }
                if(table.allMinProfileLevel){
                    insertMinProfileLevel = insertMinProfileLevel === undefined?table.allMinProfileLevel:insertMinProfileLevel;
                    updateMinProfileLevel = updateMinProfileLevel === undefined?table.allMinProfileLevel:updateMinProfileLevel;
                    removeMinProfileLevel = removeMinProfileLevel === undefined?table.allMinProfileLevel:removeMinProfileLevel;
                    readMinProfileLevel = readMinProfileLevel === undefined?table.allMinProfileLevel:readMinProfileLevel;
                }


                if(table.readCondition){
                    //restrict on arbitrary where condition
                    this.extendsClient["getTable_"+table.name] = function(){
                        return handleHiddenColumns.bind(this)(function(){
                            var client = this ; //this is the db client
                            if(!client.disableRestriction && client.context && client.context.req && client.context.req.user){
                                return `
                                    (SELECT *
                                    FROM
                                    ${table.name} 
                                    WHERE ${table.readCondition.replace(/\$user_uid/g, "'"+client.context.req.user.uid+"'")} )
                                `;
                            }else{
                                return table.name ;
                            }
                        }, table.hiddenCols) ;
                    };
                }else if(table.realmCol){
                    //restrict on records linked to user realm
                    this.extendsClient["getTable_"+table.name] = function(){
                        return handleHiddenColumns.bind(this)(function(){
                            var client = this ; //this is the db client
                            if(!client.disableRestriction && client.context && client.context.req && client.context.req.user){
                                if(readMinProfileLevel){
                                    //restrict on all allowed realm with sufficient permission
                                    return `
                                        (SELECT DISTINCT t.*
                                        FROM
                                        ${table.name} t
                                        JOIN velox_link_user_realm r ON t.${table.realmCol} = r.realm_code
                                        JOIN velox_user_profile p ON r.profile_code = p.code
                                        WHERE r.user_uid = '${client.context.req.user.uid}' AND p.level <= ${readMinProfileLevel})
                                    `;
                                }else{
                                    //restrict only on all allowed realm
                                    return `
                                        (SELECT DISTINCT t.*
                                        FROM
                                        ${table.name} t
                                        JOIN velox_link_user_realm r ON t.${table.realmCol} = r.realm_code
                                        WHERE r.user_uid = '${client.context.req.user.uid}')
                                    `;
                                }
                            }else{
                                return table.name ;
                            }
                        }, table.hiddenCols) ;
                    } ;
                } else if (table.userCol){
                    //restrict on records linked to user
                    this.extendsClient["getTable_"+table.name] = function(){
                        return handleHiddenColumns.bind(this)(function(){
                            var client = this ; //this is the db client
                            if(!client.disableRestriction && client.context && client.context.req && client.context.req.user){
                                return `
                                    (SELECT DISTINCT t.*
                                    FROM
                                    ${table.name} t
                                    WHERE ${table.userCol} = '${client.context.req.user.uid}')
                                `;
                            }else{
                                return table.name ;
                            }
                        }, table.hiddenCols)  ;
                    };
                } else if (table.hidden){
                    //hide all records
                    this.extendsClient["getTable_"+table.name] = function(){
                        var client = this ; //this is the db client
                        if(!client.disableRestriction){
                            return `
                                (SELECT DISTINCT t.*
                                FROM
                                ${table.name} t
                                WHERE 0 = 1)
                            `;
                        }else{
                            return table.name ;
                        }
                    } ;
                }
                
                if(insertMinProfileLevel || updateMinProfileLevel || removeMinProfileLevel){
                    //action restriction on profile level
                    var createRestrictFunction = function(table, minLevel, action){
                        return function(tableName, record, callback){
                            if(!this.disableRestriction && this.context && this.context.req && this.context.req.user){
                                if(action === "insert"){
                                    if(!record[table.realmCol] && this.context && this.context.req && this.context.req.currentRealm){
                                        record[table.realmCol] = this.context.req.currentRealm;
                                    }
                                }

                                //check current user is allowed on this realm
                                this.search("velox_link_user_realm", {user_uid : this.context.req.user.uid},[{otherTable: "velox_user_profile", name: "profile"}], (err, currentUserRealms)=>{
                                    if(err){ return callback(err) ;}
                                    let thisRealmLines = currentUserRealms;
                                    if(table.realmCol){
                                        thisRealmLines = [];
                                        currentUserRealms.forEach((r)=>{
                                            if(r.realm_code === record[table.realmCol]){
                                                thisRealmLines.push(r) ;
                                            }
                                        }) ;
                                    }
                                    
                                    if(thisRealmLines.length === 0){
                                        return callback("You are not allowed for "+table.name+" (no realm line)") ;
                                    }
    
                                    let profileOk = thisRealmLines.some((r)=>{
                                        return r.profile.level <= minLevel ;
                                    });
                                    if(profileOk){
                                        callback();
                                    }else{
                                        callback("You are not allowed for "+table.name+" (profile not enough)") ;
                                    }
                                }) ;
                            }else{
                                callback() ;
                            }
                        } ;
                    };
                    if(insertMinProfileLevel !== undefined){
                        this.interceptClientQueries.push(
                            {name : "insert", table: table.name, before : createRestrictFunction(table, insertMinProfileLevel, "insert") }
                        );
                    }
                    if(updateMinProfileLevel !== undefined){
                        this.interceptClientQueries.push(
                            {name : "update", table: table.name, before : createRestrictFunction(table, updateMinProfileLevel, "update") }
                        );
                    }
                    if(removeMinProfileLevel !== undefined){
                        this.interceptClientQueries.push(
                            {name : "remove", table: table.name, before : createRestrictFunction(table, removeMinProfileLevel, "remove") }
                        );
                    }
                } else if(table.realmCol){
                    //no restriction on level, add restriction on realm record
                    let realmRestrict = function(tableName, record, callback){
                        if(!this.disableRestriction && this.context && this.context.req && this.context.req.user){
                            //check current user is allowed on this realm
                            this.search("velox_link_user_realm", {user_uid : this.context.req.user.uid},[{otherTable: "velox_user_profile", name: "profile"}], (err, currentUserRealms)=>{
                                if(err){ return callback(err) ;}
                                let thisRealmLines = currentUserRealms;
                                if(table.realmCol){
                                    thisRealmLines = [];
                                    currentUserRealms.forEach((r)=>{
                                        if(r.realm_code === record[table.realmCol]){
                                            thisRealmLines.push(r) ;
                                        }
                                    }) ;
                                }
                                
                                if(thisRealmLines.length === 0){
                                    return callback("You are not allowed for "+tableName+" (no realm)") ;
                                }

                                callback();
                            }) ;
                        }else{
                            callback() ;
                        }
                    } ;
                    this.interceptClientQueries.push( {name : "insert", table: table.name, before : realmRestrict } );
                    this.interceptClientQueries.push( {name : "update", table: table.name, before : realmRestrict } );
                    this.interceptClientQueries.push( {name : "remove", table: table.name, before : realmRestrict } );
                } else if(table.userCol){
                    //no restriction on profile or realm, add restriction on user records
                    let userRestrict = function(tableName, record, callback){
                        if(!this.disableRestriction && this.context && this.context.req && this.context.req.user){
                            //check current user is allowed on this record
                            if(!record[table.userCol]){
                                record[table.userCol] = this.context.req.user.uid;
                            }
                            if(this.context.req.user.uid !== record[table.userCol]){
                                return callback("You are not allowed for "+tableName+" (user not allowed for this record)") ;
                            }
                            callback();
                        }else{
                            callback() ;
                        }
                    } ;
                    this.interceptClientQueries.push( {name : "insert", table: table.name, before : userRestrict } );
                    this.interceptClientQueries.push( {name : "update", table: table.name, before : userRestrict } );
                    this.interceptClientQueries.push( {name : "remove", table: table.name, before : userRestrict } );
                }
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
    removePassword(records){
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
    afterInsertRealm(realm, callback){
        if(this.context && this.context.req && this.context.req.user){
            //automatically link current user to this new realm

            //search profile to use
            this._queryFirst("SELECT profile_code FROM velox_link_user_realm WHERE user_uid = $1 LIMIT 1", [this.context.req.user.uid], (err, profile)=>{
                if(err){ return callback(err) ;}
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
        var auth = new GoogleAuth;
        var client = new auth.OAuth2(this.options.google.clientID, '', '');
        client.verifyIdToken(
            token,
            this.options.google.clientID,
            function(err, login) {
                if(err){ return callback(err); }

                var payload = login.getPayload();
                callback(null, payload) ;
                var userid = payload['sub'];
                // If request specified a G Suite domain:
                //var domain = payload['hd'];
            });
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
            let sql = "SELECT * FROM velox_user WHERE activation_token = $1 AND active = FALSE" ;
            let params = [activationToken];
            client._query(sql, params, (err, results)=>{
                if(err){ return done(err); }

                if(results.rows.length === 0){
                    return done("Invalid token") ;
                }

                if(results.rows.length > 1){
                    //there is a problem in the configuration somewhere
                    db.logger.error("The activation token "+activationToken+" exists many times ") ;
                    return done("Invalid token") ;
                }

                let user = results.rows[0] ;
                var updateData = {activation_token: '', active: true, uid: user.uid} ;
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
            client.getByPk("velox_user", uid, (err, user)=>{
                if(err){ return done(err); }
                this._getFullUser(client, user, done) ;
            });
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

module.exports = VeloxUserManagment;