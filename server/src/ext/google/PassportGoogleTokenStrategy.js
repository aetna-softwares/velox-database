//inspired by https://github.com/jmreyes/passport-google-id-token but use google lib to do the verification

const Strategy = require('passport-strategy');
const util = require("util") ;
const GoogleAuth = require('google-auth-library');


/**
 * `Strategy` constructor.
 *
 * The Google authentication strategy authenticates requests by verifying the
 * signature and fields of the token.
 *
 * Applications must supply a `verify` callback which accepts the `idToken`
 * coming from the user to be authenticated, and then calls the `done` callback
 * supplying a `parsedToken` (with all its information in visible form) and the
 * `googleId`.
 *
 * Options:
 * - `clientID` your Google application's client id (or several as Array)
 * - `getGoogleCerts` optional custom function that returns the Google certificates
 *
 * Examples:
 *
 * passport.use(new GoogleTokenStrategy({
 *     clientID: '123-456-789'
 *   },
 *   function(parsedToken, googleId, done) {
 *     User.findOrCreate(..., function (err, user) {
 *       done(err, user);
 *     });
 *   }
 * ));
 *
 * @param {Object} options
 * @param {Function} verify
 * @api public
 */
function GoogleTokenStrategy(options, verify) {
    if (typeof options == 'function') {
      verify = options;
      options = {};
    }
  
    if (!verify) throw new Error('GoogleTokenStrategy requires a verify function');
  
    this._passReqToCallback = options.passReqToCallback;
  
    this._clientID = options.clientID;
  
    Strategy.call(this);
    this.name = 'google-id-token';
    this._verify = verify;
  }
  
  
  /**
   * Inherit from `Strategy`.
   */
  util.inherits(GoogleTokenStrategy, Strategy);
  
  /**
   * Authenticate request by verifying the token
   *
   * @param {Object} req
   * @api protected
   */
  GoogleTokenStrategy.prototype.authenticate = function(req, options) {
    options = options || {};
    var self = this;
  
    var idToken = (req.body && (req.body.id_token || req.body.access_token))
      || (req.query && (req.query.id_token || req.query.access_token))
      || (req.headers && (req.headers.id_token || req.headers.access_token));
  
    if (!idToken) {
      return self.fail({ message: "no ID token provided" });
    }
  
    self._verifyGoogleToken(idToken, self._clientID, function(err, payload, info) {
      if (err) return self.fail({ message: err.message });
  
      if (!payload) return self.fail(info);
  
      function verified(err, user, info) {
        if (err) return self.error(err);
        if (!user) return self.fail(info);
        self.success(user, info);
      }
  
      if (self._passReqToCallback) {
        self._verify(req, payload, payload.sub, verified);
      } else {
        self._verify(payload, payload.sub, verified);
      }
    });
  } ;
  
  /**
   * Verify signature and token fields
   *
   * @param {String} idToken
   * @param {String} clientID
   * @param {Function} done
   * @api protected
   */
  GoogleTokenStrategy.prototype._verifyGoogleToken = function(idToken, clientID, done) {
    var auth = new GoogleAuth();
    var client = new auth.OAuth2(clientID, '', '');
    client.verifyIdToken(
        idToken,
        clientID,
        // Or, if multiple clients access the backend:
        //[CLIENT_ID_1, CLIENT_ID_2, CLIENT_ID_3],
        function(err, login) {
        
          if (err) {
            done(null, false, {message: err.message});
          } else {
            done(null, login.getPayload());
          }
        });


    
  } ;
  
  /**
   * Expose `GoogleTokenStrategy`.
   */
  module.exports = GoogleTokenStrategy;