var _ = require('lodash'),
    url = require('url'),
    modQs = require('querystring'),
    EventEmitter = require('events').EventEmitter,
    config = global.config,
    logger = global.logger,
    normalizeURL = global.util.normalizeURL,
    redisInterface = require(config.paths.interfaces + 'redis'),
    redisPub = redisInterface.getType('store', config.pubsub.config).rclient,
    redisSub = redisInterface.getType('sub', config.pubsub.config).rclient,
    reqman = new (require(config.paths.lib + 'request-manager'))({
      redisPub: redisPub,
      redisSub: redisSub
    });

var subCheckCount = 0;

function subscriber(apis) {
  var that = this;
  EventEmitter.call(this);

  this.subscriptions = {};
  this._activeSubChecks = {};

  this._updateData = {};
  this._fetchUpdates = _.debounce(this._fetchUpdates, 80, {maxWait: 500});


  _.each(apis, function(apiMod) {
    apiMod.inst.on('dataChange', function(endpoint, entity, changeType) {
      // On update, insert or delete we need to refresh all lists for the given endpoint.
      // On update we also refresh the entity itself (on insert no one is subscribed to it yet) - technically just if
      // anyone is subscribed and it's not a delete then we refresh it.
      // On delete we want to notify all subscribers and then auto-unsubscribe them from updates to that resource and clear it from the cache

      var entity = apiMod.inst.normalizeEntities(endpoint, [entity])[0];
      var endpointEntities = that.subscriptions[endpoint];

      // Refresh lists
      _.each(endpointEntities, function(sub, entity) {
        if(entity[0] == '?') {
          if(!that._updateData[endpoint]) {
            that._updateData[endpoint] = [];
          }
          that._updateData[endpoint].push(entity);
        }
      });

      // Refresh resource if anyone is subscribed and it's not deleted
      if(changeType != 'delete' && endpointEntities && endpointEntities[entity]) {
        if(!that._updateData[endpoint]) {
          that._updateData[endpoint] = [];
        }
        that._updateData[endpoint].push(entity);
      }

      if(changeType == 'delete' && endpointEntities && endpointEntities[entity]) {
        _.each(endpointEntities[entity].subscribers, function(subscriber) {
          var unsubscribeSubs = [];
          try {
            var subscriberSubs = subscriber.subs[endpoint][entity];
            that.unsubscribe(subscriberSubs, subscriber);
            _.each(subscriberSubs, function(sub, subEntity) {
              subscriber.notifyData(null, {changeType: 'delete', subscription: sub});
            });
          } catch(err) {
            logger.warn("Failed notifying subscriber of deletion");
          }
        });

        reqman.entityDeleted(endpoint, entity, apiMod.cachePrefix);
      }

      that._fetchUpdates();
    });
  });


  // Protocol to check for existing subscriptions accross all servers in the cluster to determine if the cache is fresh
  redisSub.on('message', function(channel, message) {
    if(channel == 'subcheckRequest') {
      // We received a subscription check request. Reply with all active subscriptions matching the request.
      try {
        message = JSON.parse(message);
      } catch(err) {
        // TODO: Log this? It should be fairly harmless, but might be good for a warning/verbose log.
        return;
      }

      var response = {checkId: message.checkId, activeSubs: {}};
      _.each(message.subs, function(entities, endpoint) {
        if(that.subscriptions[endpoint]) {
          response.activeSubs[endpoint] = _.intersection(entities, _.keys(that.subscriptions[endpoint]));
        }
      });

      redisPub.publish('subcheckReply', JSON.stringify(response));
    } else if(channel == 'subcheckReply') {
      // We received a subscription check reply. Remove any entities that are actively maintained from the set of new subscriptions.
      message = JSON.parse(message);

      var subcheck = that._activeSubChecks[message.checkId];
      if(subcheck) {
        _.each(message.activeSubs, function(entities, endpoint) {
          if(subcheck[endpoint]) {
            subcheck[endpoint] = _.difference(subcheck[endpoint], entities);
            if(!subcheck[endpoint].length) {
              delete subcheck[endpoint];
            }
          }
        });
      } else {
        logger.warn('Received a subscription check reply for a check request that no longer exists. Perhaps we need to increase the timeout to allow servers time to respond?');
      }
    }
  });
}

_.extend(subscriber.prototype, EventEmitter.prototype, {
  _notifySubscribers: function(error, entities, endpoint, subscribersToNotify) {
    var that = this,
        apiInst = global.getApiMod(endpoint).inst,
        subscriptions = this.subscriptions[endpoint];

    if(subscribersToNotify && !_.isArray(subscribersToNotify)) subscribersToNotify = [subscribersToNotify];

    if(subscriptions) {
      _.each(entities, function(entityData, entity) {
        if(subscriptions[entity]) {
          var subscribers = subscribersToNotify || subscriptions[entity].subscribers;
          for(var i = 0; i < subscribers.length; i++) {
            var subscriber = subscribers[i],
                subscriberSubs = [entity];

            if(subscriber.subs) {
              // This should be an array of the subscribers original subscriptions for this normalized entity before being normalized
              subscriberSubs = subscriber.subs[endpoint][entity] || {};
            }

            _.each(subscriberSubs, function(origSub, subStr) {
              var sendData = {};
              sendData[entity] = entityData.data;
              sendData = apiInst.filter(sendData, endpoint, origSub, subscriber);

              if(_.size(sendData)) {
                if(_.isFunction(subscriber)) {
                  subscriber(error, sendData);
                } else {
                  subscriber.notifyData(error, {changeType: 'update', subscription: origSub, data: sendData});
                }
              }
            });
          }
        }
      });
    }
  },

  _fetchUpdates: function() {
    var that = this,
        updates = this._updateData;
    this._updateData = {};

    _.each(updates, function(entities, endpoint) {
      that._getData(endpoint, entities, function(error, data) {
        that._notifySubscribers(error, data, endpoint);
      }, {useCache: false});
    });

    /*var that = this,
        updates = this._updateData;
    this._updateData = {};

    _.each(updates, function(entities, endpoint) {
      var apiMod = global.getApiMod(endpoint);
      if(apiMod.normalizeEntities) {
        entities = apiMod.normalizeEntities(endpoint, entities);
      }

      that._getData(endpoint, entities, function(error, data) {
        if(!error) {
          data = apiMod.processResult(endpoint, data);
        }
        that._notifySubscribers(error, data, endpoint);
      }, {useCache: false});
    });*/
  },

  _getData: function(endpoint, entities, callback, options) {
    options = _.extend({
      useCache: true
    }, options || {});

    if(!callback) return;
    /*if(!subscribers) return;
    if(!_.isArray(subscribers)) subscribers = [subscribers];*/

    var apiMod = global.getApiMod(endpoint),
        apiInst = apiMod.inst;

    var fetchOptions = _.extend({
      skipCache: !options.useCache,
      cacheResult: global.config.cache.enabled && apiMod.cacheEnabled,
      cachePrefix: apiMod.cachePrefix
    }, apiInst.getRequestOptions(endpoint, entities));

    // Fetch data, filter it per subscriber and send it
    reqman.fetch(endpoint, entities, fetchOptions, function processResult(err, data, complete) {
      if(err) {
        callback(err);
      } else {
        callback(null, apiInst.processResult(endpoint, data));
      }

      /*_.each(subscribers, function(subscriber) {
        var origSub = {};
        if(subscriber.subs) {
          try {
            origSub = subscriber.subs[endpoint]
          }
        }

        var subscriberData = data ? apiMod.filter(data, endpoint, subscriber) : null;
        if(_.isFunction(subscriber)) {
          subscriber(err, subscriberData);
        } else {
          subscriber.notifyData(err, subscriberData);
        }
      });*/
    });
  },

  subscribe: function(subs, subscriber) {
    var that = this,
        newSubs = {},
        addedSubs = {},
        fetchData = {},
        globalCacheEnabled = global.config.cache.enabled;

    if(!subscriber) {
      logger.warn("Subscription failed: 'subscriber' callback was empty");
      return;
    }

    // Subscribe callback to data
    _.each(subs, function(sub) {
      if(!sub.endpoint || !sub.entity) {
        var err = new Error("Invalid subscription. Missing required property 'endpoint' and/or 'entity'.");
        err.sub = sub;

        // Subscriber could be a function or a socket (or anything else that has the functions we need to call)
        if(_.isFunction(subscriber)) {
          subscriber(err);
        } else {
          subscriber.notifyData(new Error(err));
        }
        return;
      }

      var endpoint = global.util.normalizeURL(sub.endpoint, {includeProtocol: false}),
          apiMod = global.getApiMod(endpoint),
          apiInst = apiMod.inst,
          apiCacheEnabled = apiMod.cacheEnabled,

          // NOTE: We may want to do something like this in the future to convert something like /owner/1234/dogs to /dogs?owner=1234
          //       To do so we have to probably also work with entities somehow since that converts and endpoint to, perhaps, an existing
          //       endpoint, but now with an entity filter (e.g. list)
          //endpoint = _.isFunction(apiInst.normalizeEndpoint) ? apiInst.normalizeEndpoint(endpoint) : endpoint,

          entity = sub.entity,
          subEntity = entity[0] == '?'
                        ? global.util.normalizeQstr(entity, {returnAsObj: true})
                        : entity.toString(),
          newEndpoint = false,
          newSub = false;

      // It's a set of list filters
      if(_.isObject(subEntity)) {
        sub.params = subEntity;
      }

      var entity = apiInst.normalizeEntities(endpoint, [entity])[0];

      if(_.isObject(subEntity)) {
        subEntity = modQs.stringify(subEntity);
        if(subEntity) subEntity = '?' + subEntity;
      }

      // We need to know if this is a new (to us) endpoint or entity so we can manage the cache properly
      var svcSubs = that.subscriptions[endpoint];
      if(!svcSubs) {
        newEndpoint = true;
        svcSubs = that.subscriptions[endpoint] = {};
      }

      if(!svcSubs[entity]) {
        newSub = true;
        svcSubs[entity] = {subscribers: []};
      }

      // Only add the subscriber function/socket/whatever if it is not already subscribed
      if(global.util.fastIndexOf(svcSubs[entity].subscribers, subscriber) == -1) {
        svcSubs[entity].subscribers.push(subscriber);
      } else {
        // Already subscribed. Nothing to do.
        return;
      }

      // Notify the API interface - it needs to be able to appropriately subscribe to the right notification channels for realtime updates
      apiInst.onSubscribe(endpoint, entity, svcSubs[entity], newEndpoint);

      if(!addedSubs[endpoint]) {
        addedSubs[endpoint] = {};
      }
      if(!addedSubs[endpoint][entity]) {
        addedSubs[endpoint][entity] = {};
      }
      addedSubs[endpoint][entity][subEntity] = sub;

      // Prepare to add a data fetch request
      if(!fetchData[endpoint]) {
        fetchData[endpoint] = {
          allowCacheEntities: [],
          noCacheEntities: []
        };
      }

      if(globalCacheEnabled && apiCacheEnabled && newSub) {
        // We need to keep track of new subscriptions so we can see if they're just new to us or new to all servers
        if(!newSubs[endpoint]) {
          newSubs[endpoint] = [];
        }
        newSubs[endpoint].push(entity);
      } else {
        // TODO: Fetch data now with cache: true (unless caching is disabled globally or for the apiMod)
        //var fetchEntity = apiInst.entityFilter(endpoint, entity);

        fetchData[endpoint].allowCacheEntities.push(entity);
      }
    });

    // Track socket subscriptions on socket object for easy removal when the socket is disconnected
    if(!_.isFunction(subscriber)) {
      subscriber.subs = _.merge(subscriber.subs || {}, addedSubs);
    }

    // TODO: Check for existing subscriptions with other servers and then decide cache: (true|false) for each
    if(_.size(newSubs)) {
      var checkId = subCheckCount++;
      redisPub.publish('subcheckRequest', JSON.stringify({checkId: checkId, subs: newSubs}));
      this._activeSubChecks[checkId] = _.cloneDeep(newSubs);

      setTimeout(function() {
        var subcheck = that._activeSubChecks[checkId],
            cached = {},
            uncached = {};

        delete that._activeSubChecks[checkId];

        // Determine which entities are cached and which are not
        _.each(newSubs, function(entities, endpoint) {

          if(!subcheck[endpoint]) {
            //cached[endpoint] = newSubs[endpoint];

            // The entity was known by another server. The cache *should* be fresh. No point in looping through each
            // entity if the whole endpoint is cleared.
            fetchData[endpoint].allowCacheEntities.push(entity);
          } else {
            //uncached[endpoint] = [];
            _.each(newSubs[endpoint], function(entity) {
              if(subcheck[endpoint][entity]) {
                //uncached[endpoint].push(entity);

                // The entity was not a known subscription on any other servers. The cache is stale.
                fetchData[endpoint].noCacheEntities.push(entity);
              } else {
                /*if(!cached[endpoint]) {
                  cached[endpoint] = [];
                }
                cached[endpoint].push(entity);*/

                // The entity was known by another server. The cache *should* be fresh.
                fetchData[endpoint].allowCacheEntities.push(entity);
              }
            });
          }
        });

        _.each(fetchData, function(data, endpoint) {
          var notifySubscriber = function(error, data) {
            that._notifySubscribers(error, data, endpoint, subscriber);
          };

          // Fetch cached
          if(data.allowCacheEntities.length) {
            that._getData(endpoint, data.allowCacheEntities, notifySubscriber, {useCache: true});
          }

          // Fetch uncached
          if(data.noCacheEntities.length) {
            that._getData(endpoint, data.noCacheEntities, notifySubscriber, {useCache: false});
          }
        });
      }, 3);
    }
  },

  unsubscribe: function(subs, subscriber) {
    // 'subs' object looks like the following (as stored on the socket objects):
    //
    // {
    //   'example.com/api/endpoint': ['entity1', 'entity2', '?list=entity'],
    //   'example.com/api/endpoint2': [...]
    // }

    var that = this;

    _.each(subs, function(sub) {
      var endpoint = global.util.normalizeURL(sub.endpoint, {includeProtocol: false}),
          apiInst = global.getApiMod(endpoint).inst,
          entity = sub.entity,
          subEntity = entity[0] == '?'
                        ? global.util.normalizeQstr(entity)
                        : entity.toString();

      // It's a set of list filters
      if(_.isObject(subEntity)) {
        sub.params = subEntity;
      }

      var entity = apiInst.normalizeEntities(endpoint, [entity])[0];

      if(_.isObject(subEntity)) {
        subEntity = modQs.stringify(subEntity);
        if(subEntity) subEntity = '?' + subEntity;
      }

      // Remove subscription and notify API module
      var subEntities = that.subscriptions[endpoint];
      if(subEntities && subEntities[entity]) {
        var subscriberLoc = global.util.fastIndexOf(subEntities[entity].subscribers, subscriber);
        if(subscriberLoc !== -1) {
          var entitySub = subEntities[entity],
              lastEndpointSub = false;

          // Unsubscribe
          entitySub.subscribers.splice(subscriberLoc, 1);

          // Cleanup
          if(!entitySub.subscribers.length) {
            delete subEntities[entity];

            if(!_.size(subEntities)) {
              lastEndpointSub = true;
              delete that.subscriptions[endpoint];
            }
          }

          // Notify API module
          apiInst.onUnsubscribe(endpoint, entity, entitySub, lastEndpointSub);
        }
      }

      // Remove subscription from socket
      var subSubs = subscriber.subs;
      if(subSubs && subSubs[endpoint] && subSubs[endpoint][entity]) {
        delete subSubs[endpoint][entity][subEntity];

        if(!_.size(subSubs[endpoint][entity])) {
          delete subSubs[endpoint][entity];

          if(!_.size(subSubs[endpoint])) {
            delete subSubs[endpoint];
          }
        }
      }
    });
  }
});


module.exports = subscriber;