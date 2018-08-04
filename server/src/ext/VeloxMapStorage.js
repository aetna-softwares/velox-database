/**
 * This extension handle basic map storage in database
 * 
 * It create the following tables : 
 *  - velox_map : map storage
 * 
 * This extension provide a functions on the VeloxDatabase object : 
 * saveMap : (key, code, value, callback) to save map entry
 * getMap : (key, code, callback) to get a map entry
 * getMaps : (key, callback) to get all map entries
 * 
 * Express configuration :
 * If you are using express automatic configuration, it will register 3 end points 
 *   saveMap, getMap, getMaps
 * 
 * You can also handle express configuration yourself, in this case, this extension give you 2 helpers on the VeloxDatabaseExpress object :
 * getSaveMapMiddleware : give you a middleware to handle request to save map
 * getGetMapMiddleware : give you a middleware to handle request to read map entry
 * getGetMapsMiddleware : give you a middleware to handle request to read map entries
 * 
 * @example
 * //To use this extension, just register it on VeloxDatabase
 * const VeloxDatabase = require("");
 * const VeloxMapStorage = require("");
 * 
 * VeloxDatabase.registerExtension(new VeloxMapStorage({pathStorage: ...})) ;
 * 
 */
class VeloxMapStorage{

    /**
     * @typedef VeloxMapStorageStorageOptions
     * @type {object}
     * @property {string} [saveMapEndPoint] The Express end point for map saving (default : /saveMap)
     * @property {string} [getMapEndPoint] The Express end point for read map entry (default : /getMap)
     * @property {string} [getMapsEndPoint] The Express end point for read map entries (default : /getMaps)
     */

    /**
     * Create the VeloxMapStorage extension
     * 
     * @param {VeloxMapStorageOptions} [options] options 
     */
    constructor(options){
        this.name = "VeloxMapStorage";

        if(!options){
            options = {} ;
        }

        var self = this ;
        this.extendsProto = {
            saveMap : function(key, code, value, callback){
                //this is the VeloxDatabase object
                self.saveMap(this, key, code, value, callback) ;
            },
            getMap : function(key, code, callback){
                //this is the VeloxDatabase object
                self.getMap(this, key, code, callback) ;
            },
            getMaps : function(key, callback){
                //this is the VeloxDatabase object
                self.getMaps(this, key, callback) ;
            },
        } ;

        this.extendsExpressProto = {
            getSaveMapMiddleware: function(){
                //this is the VeloxDatabaseExpress object
                return (req, res)=>{
                    var key = req.body.key;
                    var code = req.body.code;
                    var value = req.body.value;

                    this.db.saveBinary(key, code, value, (err, savedRecord)=>{
                        if(err){
                            this.db.logger.error("error when save map", err);
                            return res.status(500).json(err) ;
                        }
                        res.json(savedRecord.value) ;
                    }) ;
                } ;
            },
            getGetMapMiddleware: function(){
                return (req, res)=>{
                    let key = req.params.key;
                    let code = req.params.code;
                    this.db.getMap(key, code, (err, value)=>{
                        if (err) {
                            this.db.logger.error("get map failed : ", err, key, code);
                            return res.status(500).json(err);
                        }
                        res.json(value);
                    }) ;
                } ;
            },
            getGetMapsMiddleware: function(){
                return (req, res)=>{
                    let key = req.params.key;
                    this.db.getMaps(key, (err, values)=>{
                        if (err) {
                            this.db.logger.error("get maps failed : ", err, key);
                            return res.status(500).json(err);
                        }
                        res.json(values);
                    }) ;
                } ;
            }
        } ;

        this.extendsExpressConfigure = [
            function(app){
                //this is the VeloxDatabaseExpress object
                app.post(options.saveMapEndPoint || "/saveMap", this.getSaveMapMiddleware());
                app.get(options.getMapEndPoint || "/getMap", this.getGetMapMiddleware());
                app.get(options.getMapsEndPoint || "/getMaps", this.getGetMapsMiddleware());
            }
        ] ;
    }

    /**
     * @typedef VeloxMapStorageRecord
     * @type {object}
     * @property {string} code Code of map
     * @property {string} key Key in the map
     * @property {object} value Value corresponding to this key
     */

    /**
     * Save a map entry
     * 
     * @param {VeloxDatabase} db the db access
     * @property {string} code Code of map
     * @property {string} key Key in the map
     * @property {object} value Value corresponding to this key
     * @param {function} callback called with saved record if succeed
     */
    saveMap(db, code, key, value, callback) {
        db.transactionalChanges([{table: "velox_map", record:{code: code, key: key, value: value}}],callback) ;
    }
    
    /**
     * Get a map entry
     * 
     * @param {VeloxDatabase} db the db access
     * @property {string} code Code of map
     * @property {string} key Key in the map
     * @param {function} callback called with saved record if succeed
     */
    getMap(db, code, key, value, callback) {
        db.inDatabase((client, done)=> {
            client.getByPk({code: code, key: key}, done) ;
        }, callback) ;
    }
    
    /**
     * Get map entries
     * 
     * @param {VeloxDatabase} db the db access
     * @property {string} code Code of map
     * @param {function} callback called with saved record if succeed
     */
    getMaps(db, code, value, callback) {
        db.inDatabase((client, done)=> {
            client.search({code: code}, done) ;
        }, callback) ;
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
            sql: this.getCreateTableMap(backend)
        }) ;

        
        return changes;
    }

    /**
     * Create the table velox_map if not exists
     * @param {string} backend 
     */
    getCreateTableMap(backend){
        if(backend === "pg"){
            return `
            CREATE TABLE IF NOT EXISTS velox_map (
                code VARCHAR(128),
                key VARCHAR(128),
                value JSONB,
                realm_code VARCHAR(40),
                user_uid VARCHAR(40),
                PRIMARY KEY(code, key)
            )
            ` ;
        }
        throw "not implemented for backend "+backend ;
    }


    

    
}

module.exports = VeloxMapStorage;