const bcrypt = require("bcrypt") ;
const uuid = require("uuid") ;
const LocalStrategy = require('passport-local').Strategy ;
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
     * @typedef VeloxUserManagmentOption
     * @type {object}
     * @property {Array} [userMeta] Meta data to add on user table : [{name: "foo", type: "varchar(128)"}, {name: "bar", type: "int"}]
     * @property {Array} [profileMeta] Meta data to add on profile table : [{name: "foo", type: "varchar(128)"}, {name: "bar", type: "int"}]
     * @property {object} [adminUser] The admin user to create on database creation
     * @property {object} [adminProfile] The admin profile to create on database creation
     * @property {boolean} [dontCreateAdmin] set to true if you don't want the admin automatic creation
     * @property {string} [sessionSecret] the session secret salt phrase for express middleware
     * @property {string} [usernameField] the username field name in login form (default : username)
     * @property {string} [passwordField] the password field name in login form (default : password)
     * @property {string} [realmField] the realm field name in login form (default : realm)
     * @property {string} [authEndPoint] the login authentication end point (default : "/auth")
     * @property {string} [logoutEndPoint] the logout end point (default : "/logout")
     * @property {object} [sessionOptions] custom options for express-session
     * @property {object} [sessionCheck] option for session check
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
        this.userMeta = options.userMeta || [] ;
        this.profileMeta = options.profileMeta || [] ;
        this.dontCreateAdmin = options.dontCreateAdmin || false ;
        this.fixedProfiles = options.fixedProfiles || null ;
        this.adminProfile = options.adminProfile || null ;
        this.adminUser = options.adminUser || {login: "admin", password: "admin", name: "Administrator", auth_type: "password"} ;

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

        var self = this;
        this.extendsProto = {
            authenticate : function(login, password, realm, callback){
                //this is the VeloxDatabase object
                self.authenticateUser(this, login, password, realm, callback) ;
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
                    if(req.url.indexOf(globalOptions.authEndPoint || "/auth") === 0){
                        return next(); //always accept auth endpoint
                    }
                    if(req.url.indexOf(globalOptions.logoutEndPoint|| "/logout") === 0){
                        return next(); //always accept logout endpoint
                    }

                    if(!options.makeSchemaPrivate){
                        if(req.url.indexOf(globalDatabaseOptions.dbEntryPoint+"/schema") === 0){
                            return next(); //always accept logout endpoint
                        }
                    }
                    

                    if(!checkUrl(req.isAuthenticated(), req.url, req.user)){
                        return res.status(401).end();
                    } ;

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
                }) ;

                app.use(this.getSessionCheckMiddleware(options.sessionCheck, globalDatabaseOptions)) ;
                

                app.post(options.authEndPoint || "/auth",
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

            }
        ] ;

        this.interceptClientQueries = [
            {name : "insert", table: "velox_user", before : this.beforeInsertOrUpdate },
            {name : "update", table: "velox_user", before : this.beforeInsertOrUpdate },
            {name : "getByPk", table: "velox_user", after : this.removePassword },
            {name : "searchFirst", table: "velox_user", after : this.removePassword },
            {name : "search", table: "velox_user", after : this.removePassword },
        ] ;
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
            let sql = "SELECT *, profile_code as profile FROM velox_user WHERE login = $1 AND disabled = FALSE" ;
            let params = [login];
            if(realm){
                sql = "SELECT u.*, l.profile_code as profile FROM velox_user u JOIN velox_link_user_realm l ON u.uid = l.user_uid WHERE l.realm_code = $1 AND u.login = $2 AND u.disabled = FALSE" ;
                params = [realm, login] ;
            }
            client.query(sql, params, (err, results)=>{
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

                    if(user.profile){
                        client.getByPk("velox_user_profile", user.profile, (err, profile)=>{
                            if(err){ return done(err); }
                            user.profile = profile ;
                            return done(null, user) ;
                        }) ;
                    } else {
                        return done(null, user) ;
                    }

                });
                
            }) ;
        }, callback) ;
    }


    /**
     * Add needed schema changes on schema updates
     * 
     * @param {string} backend 
     */
    addSchemaChanges(backend){
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

            changes.push({
                run: (tx, cb)=>{
                    tx.searchFirst("velox_user", {login: this.adminUser.login}, (err, adminUser)=>{
                        if(err){ return cb(err); }
                        if(adminUser){
                            //update only the profile, other information may have been change by user
                            return tx.update("velox_user", {uid: adminUser.uid, profile_code: this.adminUser.profile_code}, cb) ;
                        }
                        tx.insert("velox_user", this.adminUser, cb) ;
                    }) ;
                }
            }) ;
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
                profile_code VARCHAR(30) REFERENCES velox_user_profile(code)
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