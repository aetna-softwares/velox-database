const url = require('url');

class VeloxDatabaseExpress {
    /**
     * Create the Velox Database Express middleware
     * @param {VeloxDatabase} db 
     */
    constructor(db){
        this.db = db ;
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
            req.on('data', (chunk)=>{
                data += chunk ;
            }) ;

            var urlParsed = url.parse(req.url) ;
            let urlPath = urlParsed.pathname ;

            req.on('end', ()=>{

                let record = data ;
                try {
                    record = JSON.parse(data) ;
                }catch(e){
                    if(req.method === "POST" || req.method === "PUT"){
                        return res.status(500).end("Invalid JSON payload") ;
                    }
                }

                this._getSchema((err, schema)=>{
                    if(err){ return res.status(500).end(this._formatErr(err)) ; }

                    let urlParts = urlPath.split("/") ;
                    if(urlParts.length < 2){
                        return res.status(500).end("Wrong call url, get "+urlPath) ;
                    }

                    let table = urlParts[1] ;
                    

                    if(this.db.expressExtensions[table]){
                        this.db.expressExtensions[table].bind(this)(record, (err, result)=>{
                            if(err){ return res.status(500).end(this._formatErr(err)) ; }
                            res.status(200).json(result) ;
                        }) ;
                    } else if(table === "transactionalChanges"){
                        this.db.transactionalChanges(record, (err, modifiedRecord)=>{
                            if(err){ return res.status(500).end(this._formatErr(err)) ; }
                            res.status(200).json(modifiedRecord) ;
                        }) ;
                    } else if(table === "schema"){
                        res.status(200).json(schema) ;
                    } else if(table === "multiread"){
                        let reads = record;
                        this.db.multiread(reads, (err, results)=>{
                            if(err){ return res.status(500).end(this._formatErr(err)) ; }
                            res.status(200).json(results) ;
                        }) ;
                    } else {
                        if(!schema[table]){
                            return res.status(500).end("Unkown table "+table) ;
                        }

                        let pk = null ;

                        for(let i=2; i<urlParts.length; i++){
                            if(!pk){ pk = {} ; }
                            let pkName = schema[table].pk[i-2] ;
                            if(!pkName){
                                return res.status(500).end("Wrong pk definition for "+table) ;
                            }
                            pk[pkName] = urlParts[i] ;
                        }

                        if(req.method === "POST"){
                            this.db.inDatabase((client, done)=>{
                                client.insert(table, record, done) ;
                            }, (err, insertedRecord)=>{
                                if(err){ return res.status(500).end(this._formatErr(err)) ; }
                                res.status(201).json(insertedRecord) ;
                            }) ;
                        } else if(req.method === "PUT"){
                            if(!pk){
                                return res.status(500).end("missing pk") ;
                            }
                            if(Object.keys(pk).length !== schema[table].pk.length) {
                                return res.status(500).end("wrong pk, expected "+schema[table].pk.join(", ")) ;
                            }
                            for(let k of Object.keys(pk)){
                                record[k] = pk[k] ;
                            }
                            this.db.inDatabase((client, done)=>{
                                client.update(table, record, done) ;
                            }, (err, updatedRecord)=>{
                                if(err){ return res.status(500).end(this._formatErr(err)) ; }
                                res.status(200).json(updatedRecord) ;
                            }) ;
                        } else if(req.method === "DELETE"){
                            if(!pk){
                                return res.status(500).end("missing pk") ;
                            }
                            if(Object.keys(pk).length !== schema[table].pk.length) {
                                return res.status(500).end("wrong pk, expected "+schema[table].pk.join(", ")) ;
                            }
                            this.db.inDatabase((client, done)=>{
                                client.remove(table, pk, done) ;
                            }, (err, insertedRecord)=>{
                                if(err){ return res.status(500).end(this._formatErr(err)) ; }
                                res.status(200).json(insertedRecord);
                            }) ;
                        } else if(req.method === "GET"){
                            if(pk){
                                //get by id
                                 if(Object.keys(pk).length !== schema[table].pk.length) {
                                    return res.status(500).end("wrong pk, expected "+schema[table].pk.join(", ")) ;
                                }
                                this.db.inDatabase((client, done)=>{
                                    client.getByPk(table, pk, done) ;
                                }, (err, foundRecord)=>{
                                    if(err){ return res.status(500).end(this._formatErr(err)) ; }
                                    res.status(200).json(foundRecord) ;
                                }) ;
                            }else if(req.query["search"]){
                                try{
                                    let search = JSON.parse(req.query["search"]) ;
                                    this.db.inDatabase((client, done)=>{
                                        client.search(table, search.conditions, search.orderBy, search.offset, search.limit, done) ;
                                    }, (err, foundRecords)=>{
                                        if(err){ return res.status(500).end(this._formatErr(err)) ; }
                                        res.status(200).json(foundRecords) ;
                                    }) ;
                                } catch (error) {
                                    return res.status(500).end("invalid search format") ;
                                }
                            }else if(req.query["searchFirst"]){
                                try{
                                    let search = JSON.parse(req.query["searchFirst"]) ;
                                    this.db.inDatabase((client, done)=>{
                                        client.searchFirst(table, search.conditions, search.orderBy, done) ;
                                    }, (err, foundRecords)=>{
                                        if(err){ return res.status(500).end(this._formatErr(err)) ; }
                                        res.status(200).json(foundRecords) ;
                                    }) ;
                                } catch (error) {
                                    return res.status(500).end("invalid search format") ;
                                }
                            }else{
                                res.status(500).end("Wrong GET access") ;    
                            }
                        } else {
                            res.status(500).end("Unkown method "+req.method) ;
                        }   
                    }
                }) ;
            }) ;
        } ;
    }    
}


module.exports = VeloxDatabaseExpress ;