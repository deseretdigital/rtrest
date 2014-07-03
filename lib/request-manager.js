var _ = require('lodash'),
    LRU = require('lru-cache'),
    urlMod = require('url'),
    modQs = require('querystring'),
    request = require('request'),
    crypto = require('crypto'),
    config = global.config,
    logger = global.logger;

//
// Cache management
//

var localCache = LRU({
  max: 2000,
  maxAge: 60 * 60 * 1000  // 1 hour
});

var redisCache,
    cacheEnabled = config.cache.enabled;

if(cacheEnabled) {
  var cacheModule = config.cache.module;
  if(cacheModule[0] != '/') {
    cacheModule = config.paths.interfaces + cacheModule;
  }

  redisCache = new (require(cacheModule))(config.cache.config);
}

function fetchCached(key, props, done) {
  var data = {};

  if(!cacheEnabled) {
    done(null, data);
    return;
  }

  // Check local cache first
  var remoteProps = [];
  _.each(props, function(prop) {
    var val = localCache.get(key + prop);
    if(val) data[prop] = val;
    else remoteProps.push(prop);
  });

  // Fill in the blanks from the remote cache, if they're there
  if(remoteProps.length) {
    redisCache.fetchObject(key, props ? remoteProps : undefined, function(err, cachedData) {
      if(!err) {
        _.each(cachedData, function(val, key) {
          data[key] = val;
          localCache.set(key + prop, val);
        });
      }

      done(err, data);
    });
  } else {
    done(null, data);
  }
}

function cacheEntities(key, entities, done) {
  if(!cacheEnabled) {
    done();
    return;
  }

  // Save to local cache
  _.each(entities, function(entity, prop) {
    localCache.set(key + prop, entity);
  });

  // Save to remote cache
  redisCache.cacheObject(key, entities, done);
}


//
// Scalability
//

var recentPubMessages = {};

function reqMan(options) {
  // Required options: 'redisSub', 'redisPub'
  this.options = _.extend({}, options);

  var that = this;

  this._activeElections = {};
  this._entityWaitCount = 0;
  this._entityWait = {};

  this.redisPub = this.options.redisPub;
  this.redisSub = this.options.redisSub;

  this.redisSub.subscribe('reqelect', 'request_data');
  //this.redisSub.psubscribe('reqelect_*');
  this.redisSub.on('message', function() { that._onMessage.apply(that, arguments); });
  //this.redisSub.on('pmessage', function() { that._onpMessage.apply(that, arguments); });
}

_.extend(reqMan.prototype, {
  entityDeleted: function(endpoint, entity, cachePrefix) {
    // TODO: At some point we might want to temporarily track recently deleted items and not return the entity IDs in fetched lists.
    //       On the other hand, that could, and maybe should, be handled be each individual API module.
    redisCache.deleteObjKeys(cachePrefix + endpoint, [entity]);

    localCache.del(cachePrefix + endpoint + entity);
  },

  _reservePubMsgId: function() {
    var msgId = (parseInt(Math.random() * 1000).toString(16) + parseInt(Math.random() * 1000).toString(16));
    recentPubMessages[msgId] = true;
    return msgId;
  },

  _waitForEntities: function(endpoint, entities, options, done) {
    var that = this;

    if(!entities || !entities.length) {
      done(null, {}, true);
      return;
    }

    if(_.isFunction(options)) {
      done = options;
      options = null;
    }

    options = _.extend({
      incremental: true,  // The `done` callback will potentially be called multiple times with data as it's received rather than
                          // waiting for all data to be fetched. A 3rd argument will be passed, set to `true` when all data to fulfill
                          // the request has been fetched.
      minTimestamp: 0,    // Any data older than this timestamp will not fulfill the request
      autoUnwait: true,   // This should almost always be `true`. Don't change unless you really know what you're doing.
      timeout: 3000      // After 30 seconds we'll let it give up and fetch itself
      // TODO: CHANGE THIS BACK to 30000
    }, options || {});


    // Add to wait list
    var waitId = (this._entityWaitCount++).toString(16),

        waitData = this._entityWait[waitId] = _.extend({
          callback: done
        }, options);

    waitData.entities = _.clone(entities);

    // If timeout expires, automatically unwait and notify callback
    if(options.timeout) {
      waitData.expireTimeout = setTimeout(function() {
        var waitData = that._entityWait[waitId];
        if(waitData) {
          that._unwaitForEntities(waitId);
          waitData.callback((new Error("Timed out waiting for entities from '" + endpoint + "'")).code = 'ETIMEDOUT');
        }
      }, options.timeout);
    }

    return waitId;
  },

  _unwaitForEntities: function(waitId, entities) {
    var waitData = this._entityWait[waitId];
    if(waitData) {
      if(!entities) {
        clearTimeout(waitData.expireTimeout);
        delete this._entityWait[waitId];
      } else {
        waitData.entities = _.difference(waitData.entities, entities);
      }
    }
  },

  _reqElect: function(endpoint, entities, done) {
    var that = this;

    if(!entities || !entities.length) {
      done(null, []);
      return;
    }

    this._activeElections[endpoint] = {
      endpoint: endpoint,
      entities: _.clone(entities),
      promise: _.Deferred(),
      done: done
    };

    this.redisPub.publish('reqelect', JSON.stringify({
      msgId: this._reservePubMsgId(),
      endpoint: endpoint,
      entities: entities,
      vote: (Math.random() * 100000) >> 0,
      trump: (Math.random() * 100000) >> 0
    }));

    // Most votes should be in within a couple milliseconds. This should give enough time for all servers to receive the update notficiation,
    // generate a vote and process all other votes to figure out who's making the request. There is no need for more delay. Worst case in a high
    // latency situation is that multiple servers repeat a request, but there's not much we can (or should?) do to prevent that.
    setTimeout(function() {
      var activeElection = that._activeElections[endpoint];
      if(activeElection) {
        // This may seem unintuitive, but `activeElections.entities` is no longer what it was. During the few milliseconds we were waiting
        // any entities won by other servers in the election process were removed. See `_onMessage()` below.
        done(null, activeElection.entities);

        delete that._activeElections[endpoint];
        activeElection.promise.resolve();
      }
    }, 3);
  },

  _onMessage: function(channel, message) {
    var that = this;
    try {
      message = JSON.parse(message);
    } catch(err) {
      logger.warn('Error encountered while trying to parse published message. Received data: '  + message);
      return;
    }

    // If we sent the message then we don't want to process it
    if(recentPubMessages[message.msgId]) {
      delete recentPubMessages[message.msgId];
      return;
    }

    if(channel == 'reqelect') {
      var activeElection = this._activeElections[message.endpoint];
      if(activeElection) {
        var lostVote = message.vote > activeElection.vote || (message.vote == activeElection.vote && message.trump > activeElection.trump);
        if(lostVote) {
          activeElection.entities = _.difference(activeElection.entities, message.entities);
        }
      }
    } else if(channel == 'request_data') {
      console.log('CALLED');
      // Sanity check
      if(!message.endpoint || !message.entities || !message.timestamp) {
        logger.warn('Received bad request data. Missing one or more required properties. Received props: ' + JSON.stringify(_.keys(message)));
        return;
      }

      var msgEntities = {};
      _.each(message.entities, function(entity) {
        localCache.set(message.endpoint + entity.id, entity);
        msgEntities[entity.id] = entity;
      });

      // If any of the fetched entities satisfy any entityWait requests then pass them along
      _.each(this._entityWait, function(waitRequest, waitId) {
        if(waitRequest.endpoint == message.endpoint && message.timestamp >= waitRequest.minTimestamp) {
          if(!waitRequest._waitEntities) waitRequest._waitEntities = _.clone(waitRequest.entities);

          // This is intentionally using `waitRequest.entities` instead of `waitRequest._waitEntities` because if `waitRequest.autoUnwait` is `false`
          // then we still want to report any entities that are in the entity wait request list. It is up to the callback to unwait. If
          // `waitRequest.incremental` is also set to `false` then there will be a performance hit if it requires multiple `request_data` notifications
          // to fulfull the entity wait request since we iterate over the full list of entities even though only `_waitEntities` is really needed.
          var reqEntities = waitRequest.entities || [],
              matchEntities = {},
              matchEntityIds = [];

          // We iterate over the request entities instead of the message entities since it's an array. Also a normal `for` is used for performance
          // since this is theoretically going to run quite a bit and we want every bit of performance we can get.
          // See performance: http://jsperf.com/iterate-object-vs-array-with-each/1
          for(var i = 0; i < reqEntities.length; i++) {
            var entityId = reqEntities[i],
                entity = msgEntities[entityId];
            if(entity) {
              matchEntityIds.push(entityId);
              matchEntities[entityId] = entity;
            }
          }

          // Track received data and what we're still waiting on
          waitRequest._receivedEntities = _.extend(waitRequest._receivedEntities || {}, matchEntities);
          waitRequest._waitEntities = _.difference(waitRequest._waitEntities, matchEntityIds);

          if(waitRequest.autoUnwait && matchEntityIds.length) {
            that._unwaitForEntities(waitId, matchEntityIds);
          }

          var complete = !waitRequest._waitEntities.length;
          if(options.incremental || complete) {
            waitRequest.callback(null, options.incremental ? matchEntities : waitRequest._receivedEntities, complete);
          }

          // If it's not waiting for anything then it's just wasting our (CPU) time!
          if(!waitRequest.entities.length) {
            that._unwaitForEntities(waitId);
          }
        }
      });
    }
  },

  // -TODO: When data is received publish with timestamp and entities by ID then call local callback.
  //       Store each entity individually wrapped with its timestamp.
  // -TODO: Fetch cached version of data and compare timestamps. Use the newest version.
  // -TODO: Optionally cache data (in both LRU cache and Redis)
  // -TODO: Pass complete: `true` as 3rd argument to callback when all entities have been fetched
  _reqResponse: function(error, response, body, retry, options) {
    var that = this;

    if(response.headers['content-type'] == 'application/json') {
      var data = {};
      try {
        data = JSON.parse(body);
      } catch(err) {
        console.warn("Error parsing response while fetching '" + options.endpoint + '/' + options.entity + "': " + err.message);
        if(retry.retries >= options.maxRetries) {
          console.info('Giving up after ' + options.maxRetries + ' retries');
          options.done(err);
        } else {
          console.info('Retrying in ' + (options.retryDelay / 1000) + ' seconds');
          retry(options.retryDelay);
        }
      }

      var newEntityTimestamp = data.timestamp || options.requestStart;

      that.redisPub.publish('request_data', JSON.stringify({
        msgId: this._reservePubMsgId(),
        endpoint: options.endpoint,
        entities: {
          timestamp: newEntityTimestamp,
          data: data.data
        },
        timestamp: newEntityTimestamp
      }));

      var entities = {};
      if(options.type == 'list') {
        entities[options.entity] = {
          data: data.data,
          timestamp: newEntityTimestamp
        }
      } else if(options.type == 'resource') {
        _.each(data.data, function(entity) {
          entities[entity.id] = {
            data: entity,
            timestamp: newEntityTimestamp
          };
        });
      }

      if(options.cacheResult) {
        var cacheKey = options.cachePrefix + options.endpoint;
        fetchCached(cacheKey, _.keys(entities), function(err, cachedEntities) {
          if(err) {
            options.done(err);
            return;
          }

          // Don't cache entities that are older than existing cached entities
          _.each(cachedEntities, function(entity, entityId) {
            if(entity.timestamp > newEntityTimestamp) {
              delete entities[entityId];
            }
          });

          cacheEntities(cacheKey, entities);
        });
      }

      options.done(null, entities);
    } else {
      options.done(new Error("Unsupported content type '" + response.headers['content-type'] + "'"));
    }
  },

  _request: function(endpoint, entities, options, done) {
    if(!entities || !entities.length) {
      done(null, {}, true);
      return;
    }

    if(_.isFunction(options)) {
      done = options;
      options = null;
    }

    options = _.extend({
      retryDelay: 5000,
      maxRetries: 5,
      cacheResult: true,
      incremental: true,
      cachePrefix: ''
    }, options || {});

    var that = this,
        reqLists = [],
        reqResources = [];

    _.each(entities, function(entity) {
      if(entity[0] == '?') reqLists.push(entity);
      else reqResources.push(entity);
    });

    function paramifyEntities(params, entities) {
      if(!_.isArray(entities)) {  // Querystring params object - list filters
        _.extend(params, entities);
      } else {  // Array of resource IDs
        params.id = entities.join(',');
      }

      return params;
    }

    var waitRequests = {},
        reqCount = 0;

    // NOTE: We do resource requests first because they are generally much faster than lists since there's no searching or ordering involved
    //       and they are much more important anyway since it's the resource updates that are most visible/noticeable to users.

    // Request resources
    if(reqResources.length) {
      requestOptions = _.extend({
        url: endpoint,
        qs: (options.paramifyEntities || paramifyEntities)({}, reqResources),
        entities: reqResources,
        type: 'resource'
      }, options);

      if(requestOptions.prepFn) requestOptions.prepFn(requestOptions);
      requestOptions.url = urlMod.format({
        protocol: (requestOptions.protocol || 'http') + ':',
        host: endpoint,
        query: requestOptions.qs
      });
      delete requestOptions.qs;

      var reqId = ++reqCount;
      waitRequests[reqId] = true;

      var requestStart = Date.now();
      global.util.request(requestOptions, function(error, response, body, retry) {
        that._reqResponse.call(that, error, response, body, retry, {
          endpoint: endpoint,
          entities: reqResources,
          done: function(error, data, complete) {
            delete waitRequests[reqId];
            if(!_.size(waitRequests))
              complete = true;

            done(error, data, complete);
          },
          requestStart: requestStart,
          type: 'resource',
          cachePrefix: options.cachePrefix,
          cacheResult: options.cacheResult
        });
      });
    }

    // Request lists
    var requestOptions = _.extend({}, options);
    _.each(reqLists, function(listFilters) {
      requestOptions.url = endpoint;
      requestOptions.qs = (options.paramifyEntities || paramifyEntities)({}, modQs.parse(listFilters.slice(1)));
      requestOptions.entities = listFilters;
      requestOptions.type = 'list';

      if(requestOptions.prepFn) requestOptions.prepFn(requestOptions);
      requestOptions.url = urlMod.format({
        protocol: (requestOptions.protocol || 'http') + ':',
        host: endpoint,
        query: requestOptions.qs
      });
      delete requestOptions.qs;

      var reqId = ++reqCount;
      waitRequests[reqId] = true;

      var requestStart = Date.now();
      global.util.request(requestOptions, function(error, response, body, retry) {
        that._reqResponse.call(that, error, response, body, retry, {
          endpoint: endpoint,
          entity: listFilters,
          done: function(error, data, complete) {
            delete waitRequests[reqId];
            if(!_.size(waitRequests))
              complete = true;

            done(error, data, complete);
          },
          requestStart: requestStart,
          type: 'list',
          cachePrefix: options.cachePrefix,
          cacheResult: options.cacheResult
        });
      });
    });
  },

  fetch: function(endpoint, entities, options, done) {
    var fetchStart = Date.now(),
        that = this;

    // We don't want interference during request election
    if(this._activeElections[endpoint]) {
      _.when(this._activeElections[endpoint].promise).done(function() {
        that.fetch(endpoint, entities, options, done);
      });
      return;
    }

    if(_.isFunction(options)) {
      done = options;
      options = null;
    }

    if(!_.isArray(entities)) {
      entities = [entities];
    }

    options = _.extend({
      skipCache: false,
      cacheResult: true,
      incremental: true,
      cachePrefix: ''
    }, options || {});


    var result = {},
        cachedLoaded;

    // Fetch cached data first
    var requestComplete = false;
    if(!options.skipCache) {
      cachedLoaded = _.Deferred();
      fetchCached(options.cachePrefix + endpoint, entities, function(err, data) {
        if(!err && data) {
          var receivedEntities = [];
          _.each(data, function(entity, entityName) {
            result[entityName] = entity;
            receivedEntities.push(entityName);
          });

          if(options.incremental && receivedEntities.length) {
            requestComplete = Boolean(_.difference(entities, receivedEntities).length);
            done(null, result, requestComplete);
          }
        } else {
          logger.warn("Error fetching data from redis cache: " + err.message);
          done("Error fetching data from redis cache: " + err.message);
        }

        cachedLoaded.resolve();
      });
    }
    if(requestComplete) return;

    // Now try to request whatever's missing
    _.when(cachedLoaded).done(function() {
      var fetchEntities = _.difference(entities, _.keys(result));

      var waitOptions = {
        minTimestamp: options.skipCache ? fetchStart : 0,
        incremental: options.incremental
      };

      // Handle remote entity fetching
      // NOTE: If caching is disabled then we definitely want to make sure we're getting data newer than the fetch request.
      // By fetching with 'skipCache' false (we're allowed to use cached data) it means we are willing to accept, perhaps, older data.
      function handleWaitReply(err, entities, complete) {
        if(err) {
          if(err == 'ETIMEDOUT' || err.code == 'ETIMEDOUT') {
            logger.warn("Timed out waiting for remote server to fetch entities. Retrying locally...", "\nAPI Endpoint: ", endpoint);
            logger.debug("Failed Entities: ", fetchEntities);

            that._request(endpoint, fetchEntities, options, processRequestResponse);
          } else {
            logger.error('Error encountered while waiting for remote server to fetch entities: ' + (err.message || err));
          }
        } else {
          _.extend(result, entities);

          // Since we may have potentially large result sets and all we want to know is that the object is not empty,
          // it should be much faster to just do the following loop. _.size and _.keys would both iterate the whole object.
          var receivedData = false;
          for(var e in entities) {
            receivedData = true;
            break;
          }

          if(options.incremental && receivedData) {
            done(null, entities, complete);
          }
        }
      }


      // Handle local entity fetching
      that._reqElect(endpoint, fetchEntities, function(error, wonEntities) {
        // Remove `wonEntities` from `fetchEntities` because after this we will no longer need to fetch them if there
        // is a timeout that requires us to locally fetch the other entities we're waiting for.
        fetchEntities = _.difference(fetchEntities, wonEntities);

        // `fetchEntities` is now only the set we are waiting on remote servers to fetch
        that._waitForEntities(endpoint, fetchEntities, waitOptions, handleWaitReply);

        // Request the entities we are responsible for now
        that._request(endpoint, wonEntities, options, processRequestResponse);
      });
    });

    function processRequestResponse(err, data, complete) {
      if(err) {
        logger.warn("Error making API request: ", err.message, "\nAPI Endpoint: ", endpoint);
      } else {
        _.extend(result, data);

        if(options.incremental) {
          done(null, data, complete);
        } else if(complete) {
          done(null, result, complete);
        }
      }
    }
  }
});


module.exports = reqMan;