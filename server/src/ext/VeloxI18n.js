const uuid = require("uuid") ;

let cacheTranslations = null;

/**
 * This extension handle user management in database
 * 
 * It create the following tables : 
 *  - velox_lang : langs
 *  - velox_translation : translations
 */
class VeloxI18n{

    /**
     * @typedef VeloxI18nColumnOptions
     * @type {object}
     * @property {string} name Column name
     * @property {string} [table] Table name (for example, if the column belong to a view, put the source table name)
     */
    
     /**
     * @typedef VeloxI18nTableOptions
     * @type {object}
     * @property {string} name Table name
     * @property {VeloxI18nColumnOptions[]} columns Translated columns
     */

     /**
     * @typedef VeloxI18nOptions
     * @type {object}
     * 
     * @example
     * {
     *   tables : [
     *      {name : "foo", columns : [
     *          {name : "bar"}
     *      ]}
     *   ]
     * }
     * 
     * @property {VeloxI18nTableOptions[]} tables the translated tables
     */

    /**
     * Create the VeloxI18n extension
     * 
     * @param {VeloxI18n} [options] options 
     */
    constructor(options){
        this.name = "VeloxI18n";

        if(!options){
            options = {} ;
        }
        this.options = options ;

        var self = this;
        this.extendsClient = {
            getTranslations : function(callback){
                //this is the VeloxDatabaseClient object
                self.getTranslations(this, callback) ;
            },
            getI18nOptions : function(){
                //this is the VeloxDatabaseClient object
                return self.options;
            },
            translateRecords: function(lang, table, records, callback){
                //this is the VeloxDatabaseClient object
                self.translateRecords(this, lang, table, records, callback);
            },
            translateSave : function(lang, table, record, callback){
                self.translateSave(this, lang, table, record, callback);
            }
        };
        

        this.interceptClientQueries = [];

        this.interceptClientQueries.push({name : "insert", table: "velox_translation", before : this.beforeSaveTranslation });
        this.interceptClientQueries.push({name : "update", table: "velox_translation", before : this.beforeSaveTranslation });

        for(let table of options.tables){
            this.interceptClientQueries.push({name : "getByPk", table: table.name, after : this.translateOne });
            this.interceptClientQueries.push({name : "searchFirst", table: table.name, after : this.translateOne });
            this.interceptClientQueries.push({name : "search", table: table.name, after : this.translateMany });
            this.interceptClientQueries.push({name : "insert", table: table.name, after : this.translateSaveHook });
            this.interceptClientQueries.push({name : "update", table: table.name, after : this.translateSaveHook });
        }
        
    }

    /**
     * Clear cache
     * 
     * @private
     * @param {string} table table name
     * @param {object} record record to insert or update
     * @param {function(Error)} callback called on finish
     */
    beforeSaveTranslation(table, record, callback){
        cacheTranslations = null;
        callback() ;
    }
    
    /**
     * Get translations (handle cache)
     * 
     * @param {VeloxDatabaseClient} db database client
     * @param {function(Error)} callback called on finish
     */
    getTranslations(db, callback){
        if(cacheTranslations){
            return callback(null, cacheTranslations) ;
        }

        db.search("velox_lang", {},  [{name : "trs", otherTable: "velox_translation", type: "2many"}], "lang", function(err, langs){
            if(err){ return callback(err) ;}
            cacheTranslations = {} ;
            for(let lang of langs){
                if(!cacheTranslations[lang.lang]){
                    cacheTranslations[lang.lang] = {} ;
                }
                for(let tr of lang.trs){
                    if(!cacheTranslations[lang.lang][tr.table_name]){
                        cacheTranslations[lang.lang][tr.table_name] = {} ;
                    }
                    if(!cacheTranslations[lang.lang][tr.table_name][tr.col]){
                        cacheTranslations[lang.lang][tr.table_name][tr.col] = {} ;
                    }
                    if(!cacheTranslations[lang.lang][tr.table_name][tr.col][tr.uid]){
                        cacheTranslations[lang.lang][tr.table_name][tr.col][tr.uid] = tr.value ;
                    }
                }
            }
            callback(null, cacheTranslations) ;
        }) ;
    }
   
    /**
     * Translate records
     * 
     * @param {VeloxDatabaseClient} db database client
     * @param {function(Error)} callback called on finish
     */
    translateRecords(db, lang, table, records, callback){
        db.getTranslations((err, trs)=>{
            if(err){ return callback(err) ;}
            db.getPrimaryKey(table, (err, pkColumns)=>{
                if(err){ return callback(err); }
                
                let allLangs = Object.keys(trs) ;
                for(let record of records){
                    let uid = pkColumns.map((pk)=>{ return record[pk] ;}).join("_") ;
                    let tableDef = db.getI18nOptions().tables.find((t)=>{ return t.name === table ;}) ;
                    let velox_translations = {} ;
                    for(let l of allLangs){
                        velox_translations[l] = {} ;
                    }
                    if(tableDef){
                        for(let col of tableDef.columns){
                            var colTable = col.table||table;
                            for(let l of allLangs){
                                if(trs[l] && trs[l][colTable] && trs[l][colTable][col.name] && trs[l][colTable][col.name][uid]){
                                    velox_translations[l][col.name] = trs[l][colTable][col.name][uid];
                                }
                            }

                            if(trs[lang] && trs[lang][colTable] && trs[lang][colTable][col.name] && trs[lang][colTable][col.name][uid]){
                                record[col.name] = trs[lang][colTable][col.name][uid];
                            } else if(trs["base"] && trs["base"][colTable] && trs["base"][colTable][col.name] && trs["base"][colTable][col.name][uid]){
                                record[col.name] = trs["base"][colTable][col.name][uid];
                            }
                        }
                    }
                }
                callback(null, records) ;
            });
        });
    }
    
    /**
     * Save translations
     * 
     * @param {VeloxDatabaseClient} db database client
     * @param {string} lang lang code
     * @param {string} table table name
     * @param {object} record record saved
     * @param {function(Error)} callback called on finish
     */
    translateSave(db, lang, table, record, callback){
        db.getPrimaryKey(table, (err, pkColumns)=>{
            if(err){ return callback(err); }
            
            let uid = pkColumns.map((pk)=>{ return record[pk] ;}).join("_") ;
            db.searchFirst("velox_lang", {lang: lang}, (err, langRecord)=>{
                if(err){
                    return callback(err) ;
                }
                let changes = [] ;
                if(!langRecord){
                    changes.push({table: "velox_lang", record: {lang: lang}}) ;
                }

                let tableDef = db.getI18nOptions().tables.find((t)=>{ return t.name === table ;}) ;
                for(let colDef of tableDef.columns){
                    changes.push({table: "velox_translation", record: {lang: lang, table_name: colDef.table||table, col: colDef.name, uid: uid, value: record[colDef.name]}}) ;
                }
                db.changes(changes, callback) ;
            }) ;
        });
    }

    /**
     * Update records with translations
     * 
     * @param {table} table the records tables
     * @param {object[]} records list of records
     * @param {function} callback 
     */
    translateSaveHook(table, record, callback){
        var lang = "base" ;

        if(this.context && this.context.req){
            if(this.context.req.lang){
                lang = this.context.req.lang ;
            } else if(this.context.req.user && this.context.req.user.lang){
                lang = this.context.req.user.lang ;
            } else if (this.context.req.headers["accept-language"]){
                let [acceptedLang] = this.context.req.headers["accept-language"].split(",") ;
                if(acceptedLang){
                    lang = acceptedLang.trim() ;
                }
            }
            
            this.translateSave(lang, table, record, callback);


        }else{
            callback() ;
        }
    }

    /**
     * Update records with translations
     * 
     * @param {table} table the records tables
     * @param {object[]} records list of records
     * @param {function} callback 
     */
    translateMany(table, records, callback){
        var lang = "base" ;

        if(this.context && this.context.req){
            if(this.context.req.lang){
                lang = this.context.req.lang ;
            } else if(this.context.req.user && this.context.req.user.lang){
                lang = this.context.req.user.lang ;
            } else if (this.context.req.headers["accept-language"]){
                let [acceptedLang] = this.context.req.headers["accept-language"].split(",") ;
                if(acceptedLang){
                    lang = acceptedLang.trim() ;
                }
            }

            this.translateRecords(lang, table, records, (err)=>{
                if(err){
                    return callback(err) ;
                }
                callback(null, records) ;
            });
            
            
        }else{
            callback(null, records) ;
        }
    }

    /**
     * Update record with translations
     * 
     * @param {table} table the records tables
     * @param {object} records record
     * @param {function} callback 
     */
    translateOne(table, record, callback){
        var lang = "base" ;

        if(this.context && this.context.req){
            if(this.context.req.lang){
                lang = this.context.req.lang ;
            } else if(this.context.req.user && this.context.req.user.lang){
                lang = this.context.req.user.lang ;
            } else if (this.context.req.headers["accept-language"]){
                let [acceptedLang] = this.context.req.headers["accept-language"].split(",") ;
                if(acceptedLang){
                    lang = acceptedLang.trim() ;
                }
            }

            this.translateRecords(lang, table, [record], (err)=>{
                if(err){
                    return callback(err) ;
                }
                callback(null, record) ;
            });
            
        }else{
            callback(null, record) ;
        }
    };


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
            sql: this.getCreateTableLang(backend)
        }) ;
        changes.push({
            sql: this.getCreateTableTranslation(backend)
        }) ;
        
        return changes;
    }

    /**
     * Create the table velox_lang if not exists
     * @param {string} backend 
     */
    getCreateTableLang(backend){
        let lines = [
            "lang VARCHAR(10) PRIMARY KEY",
            "description VARCHAR(75)"
        ] ;
        if(backend === "pg"){
            return `
            CREATE TABLE IF NOT EXISTS velox_lang (
                ${lines.join(",")}
            )
            ` ;
        }
        throw "not implemented for backend "+backend ;
    }

    /**
     * Create the table velox_translation if not exists
     * @param {string} backend 
     */
    getCreateTableTranslation(backend){
        let lines = [
            "lang VARCHAR(10) REFERENCES velox_lang(lang)",
            "table_name VARCHAR(128)",
            "col VARCHAR(128)",
            "uid VARCHAR(128)",
            "value TEXT",
            "PRIMARY KEY(lang, table_name, col, uid)",
        ];
        if(backend === "pg"){
            return `
            CREATE TABLE IF NOT EXISTS velox_translation (
                ${lines.join(",")}
            )
            ` ;
        }
        throw "not implemented for backend "+backend ;
    }
}

module.exports = VeloxI18n;