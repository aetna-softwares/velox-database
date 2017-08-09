
var util = require('util');
var noop = function(){};

/**
 * One day in milliseconds.
 */

var oneDay = 86400000;


/**
 * Return the VeloxUserManagmentSessionStore extending `express`'s session Store.
 *
 * @param {object} express session
 * @return {Function}
 * @api public
 */

module.exports = function (session) {

  /**
   * Express's session Store.
   */

  var Store = session.Store;

  /**
   * Initialize VeloxUserManagmentSessionStore
   *
   * @param {VeloxDatabase} db the database instance
   */
  function VeloxUserManagmentSessionStore (db) {
    Store.call(this);
    this.db = db ;
  }

  /**
   * Inherit from `Store`.
   */
  util.inherits(VeloxUserManagmentSessionStore, Store);

  /**
   * Attempt to fetch session by the given `sid`.
   *
   * @param {String} sid
   * @param {Function} fn
   * @api public
   */
  VeloxUserManagmentSessionStore.prototype.get = function (sid, fn) {
    this.db.inDatabase((client, done)=>{
        client.getByPk("velox_user_session", sid, done) ;
    }, (err, session)=>{
        if(err){ return fn(err); }
        if(!session) { return fn() ;}
        try {
            fn(null, JSON.parse(session.contents)) ;
        }catch(err){
            return fn(err) ;
        }
    }) ;
  };

  /**
   * Commit the given `sess` object associated with the given `sid`.
   *
   * @param {String} sid
   * @param {Session} sess
   * @param {Function} fn
   * @api public
   */
  VeloxUserManagmentSessionStore.prototype.set = function (sid, sess, fn) {
     var user_uid = null;
      if(sess && sess.passport && sess.passport.user ){
          user_uid = sess.passport.user ;
      }
      var changes = [{table: "velox_user_session", record: {sid: sid, contents: JSON.stringify(sess), 
        user_uid:user_uid , expire : this.getExpireTime()}, action: "auto" }] ;
      
      this.db.transactionalChanges(changes, fn) ;

      //FIXME clear old sessions
      
    // this.db.transaction((client, done)=>{
    //     //clear old sessions
    //     client.query("DELETE FROM velox_user_session WHERE expire < $1", [new Date()], (err)=>{
    //         if(err){ return done(err); }
    //         client.insert("velox_user_session", , done) ;
    //     }) ;
    // }, fn) ;
  };

  /**
   * Destroy the session associated with the given `sid`.
   *
   * @param {String} sid
   * @api public
   */
  VeloxUserManagmentSessionStore.prototype.destroy = function (sid, fn) {
      var changes = [] ;
      if(!Array.isArray(sid)){
          sid = [sid] ;
      }
      sid.forEach(function(s){
          changes.push({table: "velox_user_session", record: s, action: "remove" }) ;
      }) ;
      this.db.transactionalChanges(changes, fn) ;
  };

  /**
   * Figure out when a session should expire
   *
   * @param {Number} [maxAge] - the maximum age of the session cookie
   * @return {Number} the unix timestamp, in seconds
   * @private
   */
  VeloxUserManagmentSessionStore.prototype.getExpireTime = function (maxAge) {
    let ttl = this.ttl;

    ttl = ttl || (typeof maxAge === 'number' ? maxAge : oneDay);
    ttl = ttl + Date.now();

    return new Date(ttl);
  };

  /**
   * Refresh the time-to-live for the session with the given `sid`.
   *
   * @param {String} sid
   * @param {Session} sess
   * @param {Function} fn
   * @api public
   */
  VeloxUserManagmentSessionStore.prototype.touch = function (sid, sess, fn) {
    const expireTime = this.getExpireTime(sess.cookie.maxAge);
    this.db.transactionalChanges({table: "velox_user_session", record: {sid: sid, expire: expireTime}}, fn) ;
  };


  /**
   * Fetch all sessions
   *
   * @param {Function} fn
   * @api public
   */
  VeloxUserManagmentSessionStore.prototype.all = function (fn) {
    this.db.inDatabase((client, done)=>{
        client.search("velox_user_session", {expire : {ope : ">", value: new Date()}}, done) ;
    }, (err, sessions)=>{
        if(err){ return fn(err); }
        var mapSessions = {} ;
        sessions.forEach(function(s){
            mapSessions[s.sid] = JSON.parse(s.contents) ;
        }) ;
    } ) ;
  };

  return VeloxUserManagmentSessionStore;
};