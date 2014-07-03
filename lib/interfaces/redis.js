var _ = require('lodash'),
    //EventEmitter = require('events').EventEmitter,
    redis = require('redis'),
    logger = global.logger;

var insts = {
  typed: {},
  named: {}
};

function rcache(options) {
  this. options = options || {};
  var that = this;

  this._prefix = _.isString(options.prefix) ? options.prefix : 'cache_';
  this.rclient = redis.createClient(options.port, options.host, options.options);

  /*this.rclient.on('ready', function() {
    that.emit('ready');
  });*/

  if(options.options.retryDelay) this.rclient.retryDelay = options.options.retryDelay;
  if(options.options.retryBackoff) this.rclient.retryBackoff = options.options.retryBackoff;

  // Keepalive - otherwise we lose our Redis connection every 5 minutes
  setInterval(function() {
    if(that.rclient.connected) {
      that.rclient.stream.write('\r\n', function() { logger.debug('Redis pubsub keepalive'); });
    }
  }, 60000);
}

// Singleton getters
rcache.getType = function(type, options) {
  var types = {'store': true, 'sub': true};
  if(!types[type]) type = 'store';

  return insts.typed[type] || (insts.typed[type] = new rcache(options));
};

rcache.getNamed = function(name, options) {
  return insts.named[name] || (insts.named[name] = new rcache(options));
};

_.extend(rcache.prototype, {
  deleteObjKeys: function(key, keys, done) {
    var cacheKey = this._prefix + key;
    this.rclient.hdel(cacheKey, keys, function(err) {
      var errMsg = 'Error encountered while trying to delete data from Redis cache: ' + err.message;
      logger.error(errMsg);

      if(_.isFunction(done)) {
        done(err ? new Error(errMsg) : null);
      }
    });
  },

  cacheObject: function(key, obj, done) {
    var saveObj = {};
    for(var k in obj) {
      saveObj[k] = JSON.stringify(obj[k]);
    }

    var that = this,
        cacheKey = this._prefix + key;

    this.rclient.hmset(cacheKey, saveObj, function(err) {
      if(err) {
        var errMsg = 'Error encountered while trying to save data to Redis cache: ' + (err.message || err);
        logger.error(errMsg);
      }

      if(_.isFunction(done)) {
        done(err ? new Error(errMsg) : null);
      }
    });

    if(this.options.keyTimeout) {
      // We check to see if the key already has a TTL an intentionally don't update the TTL if so to prevent the cache from
      // growing indefinitely. The problem is, we can't set a TTL on individual hash keys. Maybe in the future the cache
      // should use individual cache keys per entity instead of a hash so we can set TTLs on everything and do this right.
      this.rclient.ttl(cacheKey, function(err, ttl) {
        if(ttl < 0) {
          that.rclient.expire(cacheKey, that.options.keyTimeout);
        }
      });
    }
  },

  fetchObject: function(key, keys, done) {
    if(_.isFunction(keys)) {
      done = keys;
      keys = null;
    }

    var args = [key],
        f = this.rclient.hgetall;
    if(keys) {
      args.push(keys);
      f = this.rclient.hmget;
    }

    args.push(function(err, result) {
      var arrayResult = _.isArray(result);

      if(err) {
        done(new Error('Error encountered while trying to fetch data from Redis cache: ' + err.message));
      } else {
        var resObj = {};
        for(var key in result) {
          if(result[key] === null) continue;
          try {
            resObj[arrayResult ? keys[key] : key] = JSON.parse(result[key]);
          } catch(err) {
            var str = result[key],
                summary = str.length > 60 ? (str.substr(0, 30) + ' ... ' + str.substr(-30)) : str;
            logger.error('Error encountered while parsing cached data: ' + err.message + '\nData (summary): ' + summary);
          }
        }

        done(null, resObj);
      }
    });

    f.apply(this.rclient, args);
  }
});


module.exports = rcache;