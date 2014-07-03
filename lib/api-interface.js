var _ = require('lodash'),
    EventEmitter = require('events').EventEmitter;

function api(options) {
  EventEmitter.call(this);

  // Required options: 'requestManager'
  this.options = _.merge({
    // This can contain keys of explicit config options for specific collections within an API
    typeConfig: {}
  }, this.defaultOptions, options);

  this._cache = this.options.cache;
  this._reqMan = this.options.requestManager;

  this.initialize.apply(this, arguments);
}

_.extend(api.prototype, EventEmitter.prototype, {
  initialize: function() {
  },

  authorize: function(socket, authData, authCb) {
    authCb(true);
  },

  getRequestOptions: function(endpoint, entities) {},

  normalizeEntities: function(endpoint, entities) {
  },

  processResult: function(endpoint, data) {
    return data;
  },

  filter: function(entities, endpoint, subscription, subscriber) {},

  onSubscribe: function(endpoint, entity, subscription, newEndpointSub) {},

  onUnsubscribe: function(endpoint, entity, subscription, lastEndpointSub) {},
});


// This extend function is copied from Backbone.js source
api.extend = function(protoProps, staticProps) {
  var parent = this;
  var child;

  // The constructor function for the new subclass is either defined by you
  // (the "constructor" property in your `extend` definition), or defaulted
  // by us to simply call the parent's constructor.
  if (protoProps && _.has(protoProps, 'constructor')) {
    child = protoProps.constructor;
  } else {
    child = function(){ return parent.apply(this, arguments); };
  }

  // Add static properties to the constructor function, if supplied.
  _.extend(child, parent, staticProps);

  // Set the prototype chain to inherit from `parent`, without calling
  // `parent`'s constructor function.
  var Surrogate = function(){ this.constructor = child; };
  Surrogate.prototype = parent.prototype;
  child.prototype = new Surrogate;

  // Add prototype properties (instance properties) to the subclass,
  // if supplied.
  if (protoProps) _.extend(child.prototype, protoProps);

  // Set a convenience property in case the parent's prototype is needed
  // later.
  child.__super__ = parent.prototype;

  return child;
};


module.exports = api;