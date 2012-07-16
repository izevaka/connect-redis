
/*!
 * Connect - Redis
 * Copyright(c) 2012 TJ Holowaychuk <tj@vision-media.ca>
 * MIT Licensed
 */

/**
 * Module dependencies.
 */

var redis = require('redis')
  , debug = require('debug')('connect:redis');

/**
 * One day in seconds.
 */

var oneDay = 86400;

/**
 * Return the `RedisStore` extending `connect`'s session Store.
 *
 * @param {object} connect
 * @return {Function}
 * @api public
 */

module.exports = function(connect){

  /**
   * Connect's Store.
   */

  var Store = connect.session.Store;

  /**
   * Initialize RedisStore with the given `options`.
   *
   * @param {Object} options
   * @api public
   */

  function RedisStore(options) {
    var self = this;

    options = options || {};
    Store.call(this, options);
    this.prefix = null == options.prefix
      ? 'sess:'
      : options.prefix;

    this.pool = options.pool;

    if (!options.pool) {
      this.client = options.client || new redis.createClient(options.port || options.socket, options.host, options);
      if (options.pass) {
        this.client.auth(options.pass, function(err){
          if (err) throw err;
        });
        if (options.db) {

          self.client.select(options.db);
          self.client.on("connect", function() {
            self.client.send_anyways = true;
            self.client.select(options.db);
            self.client.send_anyways = false;
          });
        }
      }

      //Fake pool implmentation
      this.pool = {
        acquire: function(cb) {
          cb(self.client);
        },
        release: function() {/*noop*/}
      };
    }

    this.ttl =  options.ttl;

  };

  /**
   * Inherit from `Store`.
   */

  RedisStore.prototype.__proto__ = Store.prototype;

  /**
   * Attempt to fetch session by the given `sid`.
   *
   * @param {String} sid
   * @param {Function} fn
   * @api public
   */

  RedisStore.prototype.get = function(sid, fn){
    var self = this;
    sid = this.prefix + sid;
    debug('GET "%s"', sid);
    self.pool.acquire(function (client) {
      client.get(sid, function(err, data){
        self.pool.release(client);
        try {
          if (!data) return fn();
          data = data.toString();
          debug('GOT %s', data);
          fn(null, JSON.parse(data));
        } catch (err) {
          fn(err);
        }
      });
    });
  };

  /**
   * Commit the given `sess` object associated with the given `sid`.
   *
   * @param {String} sid
   * @param {Session} sess
   * @param {Function} fn
   * @api public
   */

  RedisStore.prototype.set = function(sid, sess, fn){
    sid = this.prefix + sid;
    try {
      var maxAge = sess.cookie.maxAge
        , ttl = this.ttl
        , sess = JSON.stringify(sess)
        , self = this;

      ttl = ttl || ('number' == typeof maxAge
          ? maxAge / 1000 | 0
          : oneDay);

      debug('SETEX "%s" ttl:%s %s', sid, sess);
      self.pool.acquire(function (client) {
        client.setex(sid, ttl, sess, function(err){
          self.pool.release(client);
          err || debug('SETEX complete');
          fn && fn.apply(this, arguments);
        });
      });
    } catch (err) {
      fn && fn(err);
    }
  };

  /**
   * Destroy the session associated with the given `sid`.
   *
   * @param {String} sid
   * @api public
   */

  RedisStore.prototype.destroy = function(sid, fn){
    var self = this;

    sid = this.prefix + sid;
    self.pool.acquire(function (client) {
      self.pool.release(client);
      client.del(sid, fn);
    });
  };

  return RedisStore;
};
