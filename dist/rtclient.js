(function(root, _, io) {
  function parseQs(qs) {
    if(typeof(qs) != 'string') return qs;

    if(qs[0] == '?') qs = qs.slice(1);

    var qs = qs.split('&'),
        parsed = {};
    for(var i = 0; i < qs.length; ++i) {
      var parts = qs[i].split('=');
      if(parts.length != 2) continue;
      parsed[parts[0]] = decodeURIComponent(parts[1].replace(/\+/g, " "));
    }

    return parsed;
  }

  var events = {
    _events: null,

    emit: function(event) {
      if(!this._events || !this._events[event]) return;

      var callbacks = this._events[event],
          args = Array.prototype.slice.call(arguments, 1);

      for(var i = 0; i < callbacks.length; i++) {
        callbacks[i].apply(root, args);
      }
    },

    once: function(event, callback) {
      if(!callback) throw new Error('No callback given!');

      var that = this;
      this.on(event, function cbWrapper() {
        that.off(event, cbWrapper);
        callback.apply(this, arguments);
      });
    },

    on: function(event, callback) {
      if(!callback) throw new Error('No callback given!');
      if(!this._events) this._events = {};

      if(!this._events[event]) this._events[event] = [];

      var cbLoc = this._events[event].indexOf(callback);
      if(cbLoc == -1) {
        this._events[event].push(callback);
      }
    },

    off: function(event, callback) {
      if(!callback) throw new Error('No callback given!');
      if(!this._events || !this._events[event]) return;

      if(!callback) delete this._events[event];

      var cbLoc = this._events[event].indexOf(callback);
      if(cbLoc != -1) {
        this._events[event].splice(cbLoc, 1);
      }
    }
  };


  var subCount = 0;
  function rtClient(options) {
      this.options = _.extend({
        firstConnectionInterval: 4000,
        connectTimeout: 4000,
        maxReconnectDelay: 4000,
        maxReconnectAttempts: Infinity
      }, options);

      if(!this.options.host) this.options.host = location.hostname;
      if(this.options.host.substr(0, 4) != 'http') this.options.host = location.protocol + '//' + this.options.host;
      if(!this.options.port) this.options.port = 7334;

      this._authRequests = {};
      this._authedAPIs = {};
      this._reconnectAuths = {};
      this._subsToSend = [];
      this._unsubsToSend = [];
      this.subscriptions = {};

      this._sendSubs = _.debounce(this._sendSubs, 50, {maxWait: 500});
      this._sendUnsubs = _.debounce(this._sendUnsubs, 50, {maxWait: 500});
  }

  Object.defineProperty(rtClient.prototype, 'connected', {
    get: function connected() {
      return this.connection ? this.connection.socket.connected : false;
    }
  });
  Object.defineProperty(rtClient.prototype, 'connecting', {
    get: function connecting() {
      return this.connection ? (this.connection.socket.connecting || this.connection.socket.reconnecting) : false;
    }
  });

  _.extend(rtClient.prototype, events, {
    connect: function() {
      var that = this;

      this.state = 'disconnected';
      this.on('connection', function() { that.state = 'connected'; });
      this.on('disconnect', function() { that.state = 'disconnected'; });
      this.on('connecting', function() { that.state = 'connecting'; });

      var host = this.options.host + ':' + this.options.port;
      this.emit('connecting');

      if(this.connection) {
        try {
          // Even though calling 'disconnect' here will throw an error the following 'connect' call won't work without it if we've had a failed connection attempt already
          this.connection.socket.disconnect();
        } catch(err) {}
        this.connection.socket.connect();
        return;
      }

      // NOTE: If the host we're connecting to does not match what's in the URL then it's considered a cross-origin request and the browser will not send cookies for authorization
      var connection = this.connection = io.connect(host, {
        'reconnect': true,
        'connect timeout': this.options.connectTimeout,
        'max reconnection attempts': this.options.maxReconnectAttempts,
        'reconnection limit': this.options.maxReconnectDelay
      });

      connection.on('connect', function() {
        // NOTE: This should be called before emitting 'connection'. When that is triggered first, whatever is bound to that
        //       event will likely call 'authAPI' initially, and then this will re-auth when it's not needed. By calling this
        //       first it will only actually do any work on a reconnect.
        that._reAuthAndSubscribe();

        that._successfulConnection = true;
        that.emit('connection');
      });

      connection.on('error', function(errStr) {
        if(!connection.connected) {
          that.emit('error', errStr);

          if(!that._successfulConnection) {
            setTimeout(function() {
              that.connect();
            }, that.options.firstConnectionInterval);
          }

          if(connection.reconnecting) {
            that.emit('connecting');
          } else {
            that.emit('disconnect');
          }
        }
      });

      connection.on('disconnect', function() {
        that.emit('disconnect');
      });

      connection.on('sub-data', function(data) {
        // `data` should be an array of objects, each of which is a record with data and the subscription the data is for

        _.each(data, function(record) {
          // `record.data` should be an object with a timestamp, the actual data, and any other important metadata about the data
          // `record.data[sub.entity]` should be the actual data

          var sub = record.subscription,
              subKey = sub.endpoint + '/' + sub.entity,
              subscription = that.subscriptions[subKey],
              subscribers = subscription.callbacks;

          subscription.data = record.data[sub.entity];

          if(subscribers) {
            for(var i = 0; i < subscribers.length; i++) {
              subscribers[i](subscription.data);
            }
          }
        });
      });

      connection.on('subscription-error', function(data) {
        console.error('Error: ' + data.code + ' - ' + data.message, data.subscription);
      });

      connection.on('auth-response', function(data) {
        if(data.authorized) {
          that._authedAPIs[data.authData.apiUrl] = data;
        }
        that.emit('auth-response', data);
      });

      return this;
    },

    disconnect: function() {
      this.connection.disconnect();

      return this;
    },


    _reAuthAndSubscribe: function() {
      var that = this;

      var pendingAuths = {};
      _.each(this._reconnectAuths, function(authData, url) {
        pendingAuths[url] = true;
        that.authAPI(authData, function() {
          delete pendingAuths[url];

          if(_.size(pendingAuths) == 0) {
            that._subsToSend = [];

            var subscriptions = that.subscriptions;
            for(var subKey in subscriptions) {
              that._subsToSend.push(subscriptions[subKey].subscription);
            }
            that._sendSubs();
          }
        });
      });

      return this;
    },

    authAPI: function(authData, done) {
      var that = this;
      if(!_.isFunction(done)) done = function() {};

      this._reconnectAuths[authData.apiUrl] = authData;
      if(this._authedAPIs[authData.apiUrl]) {
        setTimeout(function() { done(that._authedAPIs[authData.apiUrl]); });
        return this;
      }

      if(this._authRequests[authData.apiUrl]) {
        return this;
      } else {
        this._authRequests[authData.apiUrl] = authData;
      }

      function doAuth() {
        function authRespCb(data) {
          if(data.authData.apiUrl == authData.apiUrl) {
            clearTimeout(authData.timeout);
            delete that._authRequests[authData.apiUrl];
            that.off('auth-response', authRespCb);
            done(data);

            that.emit('api-auth', authData.apiUrl, data.authorized);
          }
        }

        that.on('auth-response', authRespCb);
        authData.timeout = setTimeout(function() {
          delete that._authRequests[authData.apiUrl];
          that.off('auth-response', authRespCb);

          var err = new Error("Exceeded timeout trying to authorize API at '" + authData.apiUrl + "'");
          err.authData = authData;
          console.error(err.message, authData);
        }, 5000);

        that.connection.emit('auth-api', authData);
      }

      if(this.connected) {
        doAuth();
      } else {
        this.once('connection', function() {
          doAuth();
        });
      }

      return this;
    },


    _normalizeSub: function(sub) {
      var newSub = {
          endpoint: sub.endpoint.replace(/\/{2,}/g, '/'),
          entity: '',
          _subId: 0
      };

      if(_.isString(sub.entity) && sub.entity[0] == '?') {
        sub.entity = parseQs(sub.entity);
      }

      if(_.isObject(sub.entity)) {
        // List subscription
        var qsKeys = _.keys(sub.entity);
            params = [];

        // Sort
        _.each(qsKeys, function(key) {
          params.push(key + '=' + sub.entity[key]);
        });

        newSub.entity = '?' + params.join('&');
      } else {
        // Resource subscription
        newSub.entity = sub.entity.toString();
      }

      return newSub;
    },

    subscribe: function(subs, callback) {
      var that = this;

      if(!_.isArray(subs)) subs = [subs];

      var delaySubs = [];
      _.each(subs, function(sub) {
        sub = that._normalizeSub(sub);
        sub._subId = ++subCount;

        var subApiUrl = sub.endpoint.split('/')[0];
        if(!that._authedAPIs[subApiUrl]) {
          if(that._authRequests[subApiUrl]) {
            delaySubs.push({
              apiUrl: subApiUrl,
              sub: sub
            });
          } else {
            console.error("Attempted to subscribe to '" + subApiUrl + "' API before attempting authorization");
          }
          return;
        }

        var subCb = sub.callback || callback;
        if(!subCb) throw new Error('Missing subscription callback');

        var subKey = sub.endpoint + '/' + sub.entity,
            epSubs = that.subscriptions[subKey];
        if(!epSubs) {
          epSubs = that.subscriptions[subKey] = {
            data: null,
            subscription: sub,
            callbacks: []
          };

          // We only need to send the subscription if it's a new one
          that._subsToSend.push(sub);
        } else if(epSubs.data) {
          // Return data from local cache if we have it
          setTimeout(function() {
            subCb(epSubs.data);
          });
          return;
        }

        // Only add callback if it's not already subscribed
        if(epSubs.callbacks.indexOf(subCb) == -1) {
          epSubs.callbacks.push(subCb);
        }
      });

      // Subscribe to subscription requests as soon as authorization has succeeded
      if(delaySubs.length) {
        function queueSubs(apiUrl, authorized) {
          var fulfilled = [],
              stillWaiting = [];
          _.each(delaySubs, function(subReq) {
            if(authorized && subReq.apiUrl == apiUrl) {
              fulfilled.push(subReq.sub);
            } else {
              stillWaiting.push(subReq);
            }
          });

          delaySubs = stillWaiting;

          if(!stillWaiting.length) {
            that.off('api-auth', queueSubs);
          }
        }

        that.on('api-auth', queueSubs);
        setTimeout(function() {
          if(delaySubs.length) {
            that.off('api-auth', queueSubs);
            console.error("Subscriptions failed. Auth timeout exceeded.", delaySubs);
          }
        }, 10000);
      }

      that._sendSubs();

      return this;
    },

    unsubscribe: function(subs, callback) {
      var that = this;

      if(!_.isArray(subs)) subs = [subs];

      _.each(subs, function(sub) {
        sub = that._normalizeSub(sub);

        var subCb = sub.callback || callback;

        var subKey = sub.endpoint + '/' + sub.entity;
        if(!subCb) {
          // Unsubscribe all if no callback is given
          delete that.subscriptions[subKey];
          that._unsubsToSend.push(sub);
        } else {
          var epSubs = that.subscriptions[subKey];
          var cbLoc = epSubs.callbacks.indexOf(subCb);
          if(cbLoc != -1) {
            epSubs.callbacks.splice(cbLoc, 1);
          }

          if(!epSubs.callbacks.length) {
            delete that.subscriptions[subKey];
            that._unsubsToSend.push(sub);
          }
        }
      });

      that._sendUnsubs();

      return this;
    },

    _sendSubs: function() {
      if(this._subsToSend.length) {
        this.connection.emit('subscribe', this._subsToSend);
        this._subsToSend = [];
      }

      return this;
    },

    _sendUnsubs: function() {
      if(this._unsubsToSend.length) {
        this.connection.emit('subscribe', this._unsubsToSend);
        this._unsubsToSend = [];
      }

      return this;
    }
  });


  root.rtClient = rtClient;
})(this, this.lodash || this._, this.io);