const url = require('url');

class VeloxDatabaseExpress {
     /**
     * @typedef VeloxDatabaseOptions
     * @type {object}
     * @property {string} [dbEntryPoint] database entry point (default : /api)
     */

    /**
     * Create the Velox Database Express middleware
     * 
     * @param {VeloxDatabase} db database instance
     * @param {VeloxDatabaseOptions} [options] configuration options 
     */
    constructor(db, options){
        this.db = db ;

        this.options = options?JSON.parse(JSON.stringify(options)):{} ;

        if(!this.options.dbEntryPoint){
            this.options.dbEntryPoint = "/api" ;
        }

        if(this.db.expressExtensionsProto){
            Object.keys(this.db.expressExtensionsProto).forEach((k)=>{
                this[k] = this.db.expressExtensionsProto[k].bind(this) ;
            }) ;
        }
    }

   

    /**
     * Configure the express app
     * 
     * It add body-parser middleware and register database api endpoint
     * to the VeloxDatabaseOptions#dbEntryPoint (default '/api')
     * 
     * other automatic configuration can came from extensions
     * 
     * @param {object} app express app object
     */
    configureExpress(app){
        const bodyParser = require('body-parser');
        app.use(bodyParser.json()) ;
        app.use(bodyParser.urlencoded({extended: true}));

        if(this.db.expressExtensionsConfigure){
            this.db.expressExtensionsConfigure.forEach((c)=>{
                c.bind(this)(app, this.options) ;
            }) ;
        }

        app.use(this.options.dbEntryPoint, this.middleware()) ;
    }

    /**
     * Get the database schema
     * @param {function(Error, object)} callback 
     */
    _getSchema(callback){
        if(this.schema){
            return callback(null, this.schema) ;
        }
        this.db.getSchema((err, schema)=>{
            if(err){ return callback(err); }
            this.schema = schema ;
            callback(null, schema) ;
        }) ;
    }

    /**
     * Format error to send to client
     * 
     * @param {*} err 
     */
    _formatErr(err){
        if(typeof(err) === "string"){ return err ;}
        if(typeof(err) === "object"){ 
            if(err instanceof Error){
                return ""+err ;
            }else{
                return JSON.stringify(err) ;
            }
        }
        return err ;
    }

    /**
     * Format error to send to print on logger
     * 
     * @param {*} err 
     */
    _formatErrLogger(err, req){
        var str = err;
        if(typeof(err) === "object"){ 
            if(err instanceof Error){
               str = ""+err ;
            }else{
                str = JSON.stringify(err) ;
            }
        }
        str += " [query : "+JSON.stringify(req.query)+
        ", params : "+JSON.stringify(req.query)+", body : "+JSON.stringify(req.body)+"]" ;
        if(err && err instanceof Error){
            str += ", stack : "+err.stack ;
        }
        return str;
    }

    /**
     * 
     * @param {VeloxDatabaseClient} client the database client instance
     * @param {HttpRequest} req the current HTTP request
     */
    _setContext(client, req){
        client.context = {
            req: req
        } ;
    }

    /**
     * Give back the express middleware 
     * 
     * @example
     * app.use("/crud", new VeloxDatabaseExpress(DB).middleware()) ;
     * 
     * 
     */
    middleware() {
        return (req, res) => {
            let data = '' ;
            
            let urlParsed = url.parse(req.url) ;
            let urlPath = urlParsed.pathname ;

            let record = req.body ;
            
            this._getSchema((err, schema)=>{
                if(err){ return res.status(500).end(this._formatErr(err)) ; }

                let urlParts = urlPath.split("/") ;
                if(urlParts.length < 2){
                    this.db.logger.error("Wrong call url, get "+urlPath) ;
                    return res.status(500).end("Wrong call url, get "+urlPath) ;
                }
                urlParts = urlParts.map((p)=>{ return unescape(p) ;}) ;

                let table = urlParts[1] ;
                

                if(this.db.expressExtensions[table]){
                    this.db.expressExtensions[table].bind(this)(record, (err, result)=>{
                        if(err){ 
                            this.db.logger.error(this._formatErrLogger(err, req)) ;
                            return res.status(500).end(this._formatErr(err)) ; 
                        }
                        res.status(200).json(result) ;
                    }) ;
                } else if(table === "transactionalChanges"){
                    this.db.transaction((tx, done)=>{
                        this._setContext(tx, req) ;
                        tx.changes(record, done) ;
                    }, (err, modifiedRecord)=>{
                        if(err){ 
                            this.db.logger.error(this._formatErrLogger(err, req)) ;
                            return res.status(500).end(this._formatErr(err)) ; 
                        }
                        res.status(200).json(modifiedRecord) ;
                    }) ;
                } else if(table === "schema"){
                    res.status(200).json(schema) ;
                } else if(table === "multiread"){
                    let reads = record.reads;
                    this.db.inDatabase((client, done)=>{
                        this._setContext(client, req) ;
                        client.multiread(reads, done) ;
                    }, (err, results)=>{
                        if(err){ 
                            this.db.logger.error(this._formatErrLogger(err, req)) ;
                            return res.status(500).end(this._formatErr(err)) ; 
                        }
                        res.status(200).json(results) ;
                    }) ;
                } else {
                    if(!schema[table]){
                        this.db.logger.error("Unknown table "+table) ;
                        return res.status(500).end("Unknown table "+table) ;
                    }

                    let pk = null ;

                    for(let i=2; i<urlParts.length; i++){
                        if(!pk){ pk = {} ; }
                        let pkName = schema[table].pk[i-2] ;
                        if(!pkName){
                            this.db.logger.error("Wrong pk definition for "+table+" : "+JSON.stringify(pk)) ;
                            return res.status(500).end("Wrong pk definition for "+table) ;
                        }
                        pk[pkName] = urlParts[i] ;
                    }

                    if(req.method === "POST"){
                        this.db.transaction((client, done)=>{
                            this._setContext(client, req) ;
                            client.insert(table, record, done) ;
                        }, (err, insertedRecord)=>{
                            if(err){ 
                                this.db.logger.error(this._formatErrLogger(err, req)) ;
                                return res.status(500).end(this._formatErr(err)) ; 
                            }
                            res.status(201).json(insertedRecord) ;
                        }) ;
                    } else if(req.method === "PUT"){
                        if(!pk){
                            this.db.logger.error("missing pk") ;
                            return res.status(500).end("missing pk") ;
                        }
                        if(Object.keys(pk).length !== schema[table].pk.length) {
                            this.db.logger.error("wrong pk, expected "+schema[table].pk.join(", ")+", received "+JSON.stringify(pk)) ;
                            return res.status(500).end("wrong pk, expected "+schema[table].pk.join(", ")) ;
                        }
                        for(let k of Object.keys(pk)){
                            record[k] = pk[k] ;
                        }
                        this.db.transaction((client, done)=>{
                            this._setContext(client, req) ;
                            client.update(table, record, done) ;
                        }, (err, updatedRecord)=>{
                            if(err){ 
                                this.db.logger.error(this._formatErrLogger(err, req)) ;
                                return res.status(500).end(this._formatErr(err)) ; 
                            }
                            res.status(200).json(updatedRecord) ;
                        }) ;
                    } else if(req.method === "DELETE"){
                        if(!pk){
                            this.db.logger.error("missing pk") ;
                            return res.status(500).end("missing pk") ;
                        }
                        if(Object.keys(pk).length !== schema[table].pk.length) {
                            this.db.logger.error("wrong pk, expected "+schema[table].pk.join(", ")+", received "+JSON.stringify(pk)) ;
                            return res.status(500).end("wrong pk, expected "+schema[table].pk.join(", ")) ;
                        }
                        this.db.transaction((client, done)=>{
                            this._setContext(client, req) ;
                            client.remove(table, pk, done) ;
                        }, (err, insertedRecord)=>{
                            if(err){ 
                                this.db.logger.error(this._formatErrLogger(err, req)) ;
                                return res.status(500).end(this._formatErr(err)) ;
                            }
                            res.status(200).json(insertedRecord);
                        }) ;
                    } else if(req.method === "GET"){
                        if(pk){
                            //get by id
                            if(Object.keys(pk).length !== schema[table].pk.length) {
                                this.db.logger.error("wrong pk, expected "+schema[table].pk.join(", ")+", received "+JSON.stringify(pk)) ;
                                return res.status(500).end("wrong pk, expected "+schema[table].pk.join(", ")) ;
                            }
                            this.db.inDatabase((client, done)=>{
                                this._setContext(client, req) ;
                                var joinFetch = null;
                                if(req.query.joinFetch){
                                    joinFetch = JSON.parse(req.query.joinFetch) ;
                                }
                                client.getByPk(table, pk, joinFetch, done) ;
                            }, (err, foundRecord)=>{
                                if(err){ 
                                    this.db.logger.error(this._formatErrLogger(err, req)) ;
                                    return res.status(500).end(this._formatErr(err)) ; 
                                }
                                res.status(200).json(foundRecord) ;
                            }) ;
                        }else if(req.query["search"]){
                            try{
                                let search = JSON.parse(req.query["search"]) ;
                                this.db.inDatabase((client, done)=>{
                                    this._setContext(client, req) ;
                                    client.search(table, search.conditions, search.joinFetch, search.orderBy, search.offset, search.limit, done) ;
                                }, (err, foundRecords)=>{
                                    if(err){ 
                                        this.db.logger.error(this._formatErrLogger(err, req)) ;
                                        return res.status(500).end(this._formatErr(err)) ; 
                                    }
                                    res.status(200).json(foundRecords) ;
                                }) ;
                            } catch (error) {
                                this.db.logger.error("invalid search format : "+req.query["search"]+" : "+error) ;
                                return res.status(500).end("invalid search format") ;
                            }
                        }else if(req.query["searchFirst"]){
                            try{
                                let search = JSON.parse(req.query["searchFirst"]) ;
                                this.db.inDatabase((client, done)=>{
                                    this._setContext(client, req) ;
                                    client.searchFirst(table, search.conditions, search.joinFetch, search.orderBy, done) ;
                                }, (err, foundRecords)=>{
                                    if(err){ 
                                        this.db.logger.error(this._formatErrLogger(err, req)) ;
                                        return res.status(500).end(this._formatErr(err)) ; 
                                    }
                                    res.status(200).json(foundRecords) ;
                                }) ;
                            } catch (error) {
                                this.db.logger.error("invalid search format : "+req.query["searchFirst"]+" : "+error) ;
                                return res.status(500).end("invalid search format") ;
                            }
                        }else{
                            this.db.logger.error("Wrong GET access") ;
                            res.status(500).end("Wrong GET access") ;    
                        }
                    } else {
                        this.db.logger.error("Unknown method "+req.method) ;
                        res.status(500).end("Unknown method "+req.method) ;
                    }   
                }
            }) ;
        } ;
    }    
}


module.exports = VeloxDatabaseExpress ;