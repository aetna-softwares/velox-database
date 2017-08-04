const fs = require("fs");
const path = require("path");
const hjson = require("hjson") ;
const AsyncJob = require("../../../helpers/AsyncJob") ;

/**
 * contains a SQL change
 * 
 * @property {number} sinceVersion - modification to do in version
 * @property {string} sql - sql query to apply
 * @property {Array} [params] - sql params
 */
class VeloxSqlChange {
    /**
     * Create an SQL change
     * 
     * @param {number} sinceVersion - modification to do in version
     * @param {string} sql - sql query to apply
     * @param {Array} [params] - sql params
     */
    constructor(sinceVersion, sql, params){
        this.sinceVersion = sinceVersion;
        this.sql = sql;
        this.params = params;
    }
}

/**
 * SQL Updater manager
 */
class VeloxSqlUpdater {
    constructor(){
        this.changes = [] ;
    }

    /**
     * Add a change to do
     * 
     * @param {number} sinceVersion - version since this query must be run
     * @param {string} sql - SQL query to run
     * @param {Array} [params] - SQL params for the query
     */
    addChange(sinceVersion, sql, params){
        this.changes.push(new VeloxSqlChange(sinceVersion, sql, params)) ;
    }

    /**
     * Get all the changes from a version to another
     * 
     * @param {number} fromVersion - the start version
     * @param {number} [toVersion] - the target version
     * @return {VeloxSqlChange[]} - the list of changes to apply
     */
    getChanges(fromVersion, toVersion){
        return this._getSortedChanges().filter((c)=>{
            return c.sinceVersion > fromVersion && 
                (!toVersion || c.sinceVersion<=toVersion) ;
        }) ;
    }

    /**
     * Get the last version of schema
     * 
     * @return {number} - last version
     */
    getLastVersion() {
        if(this.changes.length === 0){ return 0; }
        return this._getSortedChanges().reverse()[0].sinceVersion ;
    }

    /**
     * Sort the changes following version
     * 
     * @private
     * @return {VeloxSqlChange[]} - the list of changes sorted
     */
    _getSortedChanges(){
        return this.changes.sort((c1, c2)=>{
            return c1.sinceVersion - c2.sinceVersion ;
        }) ;
    }

    /**
     * Load migration files from a folder. Each file refering to a version
     * 
     * File should written in HJSON and containing :
     * {
     *  version
     *  queries: [
     *     "Simple query"
     *     { query : "query with params", params: [...] }
     *  ]
     * }
     * 
     * @param {string} folder - Path of folder containing migration files
     * @param {function(err)} callback - Called when loaded all files
     */
    loadChanges(folder, callback){
        fs.readdir(folder, (err, fileList)=>{
            if(err){ return callback(err) ;}
            let jobRead = new AsyncJob(AsyncJob.PARALLEL) ;
            let changes = [] ;
            for (let file of fileList) {
                jobRead.push((cb)=>{
                    fs.readFile(path.join(folder, file), {encoding: "utf8"}, (err, strFile) => {
                        if(err){ return cb(err); }
                        try {
                            let hjsonFile = hjson.parse(strFile) ;

                            if(!hjsonFile.version){
                                return cb("Missing version in file "+file) ;
                            }
                            if(!hjsonFile.queries){
                                return cb("Missing queries in file "+file) ;
                            }
                            if(!Array.isArray(hjsonFile.queries)){
                                return cb("Queries is not an array in file "+file) ;
                            }

                            let toVersion = hjsonFile.version ;
                            if(changes.some((c)=> { return c.sinceVersion === toVersion; })){
                                throw "Error parsing file "+file+", another file already define version "+toVersion ;
                            }
                            for(let q of hjsonFile.queries){
                                if(typeof(q) === "string"){
                                    changes.push(new VeloxSqlChange(toVersion, q, [])) ;
                                } else if (typeof(q) === "object"){
                                    if(!q.query) {
                                        return cb("Missing query key in queries entry in file "+file) ;
                                    }
                                    if(!q.params) {
                                        return cb("Missing params key in queries entry in file "+file) ;
                                    }
                                    if(!Array.isArray(q.params)) {
                                        return cb("Params is not an array in queries entry in file "+file) ;
                                    }
                                    changes.push(new VeloxSqlChange(toVersion, q.query, q.params)) ;
                                }
                            }
                            cb() ;
                        } catch(e) {
                            return cb("Can't parse file "+file+" : "+e) ;
                        }
                    }) ;
                }) ;
            }
            jobRead.async((err)=>{
                if(err){ return callback(err); }
                this.changes = changes ;
                callback();
            }) ;
        }) ;
        
    }
}

module.exports = VeloxSqlUpdater;