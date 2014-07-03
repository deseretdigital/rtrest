var _ = require('lodash'),
    path = require('path');

_.str = require('underscore.string');
_.mixin(require('underscore.deferred'));
_.sortObj = function(sourceObj, inPlace) {
  var retObj = {};
  if(inPlace) {
    retObj = sourceObj;
    sourceObj = _.clone(sourceObj);
    for(var k in retObj) {
      delete retObj[k];
    }
  }

  var keys = _.keys(sourceObj);
  keys.sort();
  for(var i = 0; i < keys.length; i++) {
    var key = keys[i];
    retObj[key] = sourceObj[key];
  }

  return retObj;
};

var config = global.config = require(__dirname + '/config.js');

// Must be setup before any modules that try to use the logger are included
var logger = global.logger = require(__dirname + '/lib/logger.js');
logger.addConsoleLog({level: config.logLevel || 'info'});

var fs = require('fs'),
    redisInterface = require(__dirname + '/lib/interfaces/redis');

config.paths = {
  lib: __dirname + '/lib/',
  interfaces: __dirname + '/lib/interfaces/'
};

global.util = require(config.paths.lib + 'util');

// Construct Redis pub/sub interface
var redisPub = redisInterface.getType('store', config.pubsub.config),
    redisSub = redisInterface.getType('sub', config.pubsub.config),
    pubLoaded = _.Deferred(),
    subLoaded = _.Deferred();

redisPub.rclient.on('ready', function() { pubLoaded.resolve(); });
redisSub.rclient.on('ready', function() { subLoaded.resolve(); });

_.when(pubLoaded, subLoaded).done(function() {
  redisPub.rclient.publish('rtrest', 'ping', function(error, subscribers) {
    if(error) {
      logger.error('Failed trying to ping other running instances: ' + error);
      process.exit(1);
    }

    if(subscribers === 0) {
      process.stdout.write('No other active server instances found. Flushing stale cache data... ');
      redisPub.rclient.flushdb(function(error, result) {
        if(error) {
          logger.error('Error trying to flush the redis datastore: ' + error);
          process.exit(1);
        } else {
          console.log('done!');
        }
      });
    }

    redisSub.rclient.subscribe('rtrest', function(error, result) {
      if(error) {
        logger.error('Error subscribing to multi-instance communication channel: ' + error);
        process.exit(1);
      }
    });
  });
});


// Load API interfaces
var apis = [],
    apiId = 1;
_.each(config.apis, function(apiConfig, apiMatch) {
  var apiMod = apiConfig.module ? apiConfig.module : (__dirname + '/lib/api-interface.js');
  if(apiMod[0] != '/') apiMod = path.join(config._path, apiMod);

  try {
    apiMod = require(apiMod);
  } catch(err) {
    logger.error("Failed loading API module '" + apiMod + "': " + err.message);
    process.exit(1);
  }

  var apiOptions = _.merge({
    pub: redisPub,
    sub: redisSub
  }, apiConfig);

  var apiData = _.cloneDeep(apiConfig);
  delete apiConfig.module;
  apiData.id = apiId++;
  apiData.inst = new apiMod(apiOptions);
  apiData.apiMatch = new RegExp(apiMatch);

  apis.push(apiData);
});

// We look at the whole service URL (but without the specific entity) to allow using a different API module for specific controllers, etc...
var _apiMap = {};
global.getApiMod = function getApiMod(url) {
  //    serviceDomain = url.match(/(?:\w*:\/\/)?([^\/]+)/)[0] || '';

  var apiMod = _apiMap[url];
  if(!apiMod) {
    var normUrl = global.util.normalizeURL(url, {includeProtocol: false});
    _.each(apis, function(api) {
      if(api.apiMatch.test(url)) {
        _apiMap[url] = api;
        apiMod = api;
        return false;
      }
    });
  }

  return apiMod;
};

global.apiSocketAuthed = function apiSocketAuthed(apiMod, socket) {
  return socket._authedAPIs.indexOf(apiMod) != -1;
}


// Implement client API (client: 'subscribe', 'unsubscribe', server: 'subscribed', 'unsubscribed', 'error', 'notice/progress')
var ssl = config.ssl,
    useSSL = ssl.key && ssl.cert,
    httpMod = require('http' + (useSSL ? 's' : '')),
    srvArgs = [],
    srv;

if(useSSL) {
  srvArgs.push({
    key: fs.readFileSync(ssl.key),
    cert: fs.readFileSync(ssl.cert)
  });
}

srvArgs.push(function(req, res) {
  res.end('');
});
srv = httpMod.createServer.apply(httpMod, srvArgs).listen(config.port, config.host, function(err) {
  if(err) {
    logger.fatal("Error trying to bind to interface or port: " + err.message);
  } else {
    logger.info('Listening on ' + config.host + ':' + config.port);
  }
});

var io = require('socket.io').listen(srv);
io.set('log level', 1);
io.enable('browser client minification');
io.enable('browser client etag');


var subscriber = new (require(config.paths.lib + 'subscriber'))(apis);
io.sockets.on('connection', function(socket) {
  var socketSend = _.debounce(function() {
    socket.emit('sub-data', socket.sendData);
    socket.sendData = [];
  }, 80, {maxWait: 500});
  socket.sendData = [];

  socket.notifyData = function notifyData(error, data) {
    if(error) {
      // NOTE: `entity` and `code` may or may not be populated
      socket.emit('data-error', {code: error.code, message: error.message, entity: error.entity});
    } {
      //global.util.merge(socket.sendData, data);
      socket.sendData.push(data);
      socketSend();
    }
  };

  var batchSubs = [],
      batchSubscribe = _.debounce(function() {
        subscriber.subscribe(batchSubs, socket);
      }, 50, {maxWait: 3000});

  socket.on('subscribe', function(subs) {
    // Expected subscription structure:
    //    {
    //      endpoint: 'example.com/api/test',
    //      entity: ('12345' || '?key=val&key1=val1')
    //    }

    for(var i = 0; i < subs.length; i++) {
      var sub = subs[i];
      if(apiSocketAuthed(getApiMod(sub.endpoint.split('/')[0]), socket)) {
        batchSubs.push(sub);
      } else {
        socket.emit('subscription-error', {code: 'EPERM', message: "You are not authorized to access the API at '" + sub.apiUrl, subscription: sub});
      }
    }

    if(!_.isArray(subs)) {
      batchSubs.push(subs);
    } else {
      batchSubs = batchSubs.concat(subs);
    }
    batchSubscribe();
  });

  socket.on('unsubscribe', function(subs) {
    subscriber.unsubscribe(subs, socket);
  });

  socket.on('disconnect', function() {
    var unsubscribeSubs = [];

    _.each(socket.subs, function(entities, endpoint) {
      _.each(entities, function(subs, entity) {
        unsubscribeSubs = unsubscribeSubs.concat(_.values(subs));
      });
    });

    subscriber.unsubscribe(unsubscribeSubs, socket);

    _.each(socket._authedAPIs, function(apiMod) {
      apiMod.inst.endAuth(socket);
    });
    delete socket._authedAPIs;
  });

  socket._authedAPIs = [];
  socket.on('auth-api', function(authData) {
    var apiMod = getApiMod(authData.apiUrl);
    if(apiMod) {
      apiMod.inst.authorize(socket, authData, function(authorized, message) {
        if(authorized) {
          socket._authedAPIs.push(apiMod);
        }
        socket.emit('auth-response', {authorized: authorized, message: message, authData: authData});
      });
    } else {
      socket.emit('auth-response', {authorized: false, message: "No matching API for URL '" + (authData.apiUrl || '') + "'", authData: authData});
    }
  });
});