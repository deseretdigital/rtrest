var _ = require('lodash'),
    path = require('path'),
    fs = require('fs');

var cfgFile = process.argv[2];
if(!cfgFile) {
  console.error('[Error] No config file given. Please specify the location of the config file.');
  process.exit(1);
}

if(cfgFile[0] != '/') cfgFile = path.join(__dirname, cfgFile);

if(!fs.existsSync(cfgFile)) {
  console.error("[Error] Specified config file does not exist ('" + cfgFile + "')");
  process.exit(1);
}

try {
  var config = _.merge({
    _path: path.dirname(cfgFile),
    logLevel: 'info',
    host: '0.0.0.0',
    port: 7334,

    cache: {
      enabled: true,  // Global cache enable/disable
      module: 'redis',
      config: {
        keyTimeout: 12 * 60 * 60, // We will wait 12 hours after the last write to a cache key before expiring it. 0 disables timeout.
        //host: 'localhost',
        //port: 6379,
        options: {
          retryMaxDelay: 5000,
          //retryDelay: 5000,
          retryBackoff: 10
        }
      }
    },

    pubsub: {
      module: 'redis',
      config: {
        //host: 'localhost',
        //port: 6379,
        options: {
          retryMaxDelay: 5000,
          //retryDelay: 5000,
          retryBackoff: 10
        }
      }
    },

    ssl: {
      //key: '',
      //cert: ''
    },

    apis: {
      // Specify domain regex as key, object as value. Object contains (optional) file path for API interface module, and options to pass to it.
      // Domains regex's are matched in order specified.
      // Example:
      //
      // // To match any domain (not recommended) just use '.*'
      // '(.+\\.)?yourdomain\\.com/api/': {
      //   module: '/path/to/module.js',  // optional
      //   filters: '/path/to/filters'    // required if caching is enabled,
      //   cacheEnabled: true,            // optional
      //   cacheLimit: 1000,              // optional
      //
      //   typeConfig: {
      //     // http://yourdomain.com/api/dogs  <-- (assuming the default `type` regex above - alternative '[^/]+/[^/]+/?$' would map to 'api/dogs' as the type)
      //     'dogs': {
      //       'cacheEnabled': true,  (already defaults to enabled)
      //       'cacheLimit': 500
      //     }
      //   }
      // }
    }
  }, JSON.parse(fs.readFileSync(cfgFile).toString().replace(/(?:^|[\r\n])+\s*\/\/.*(?:[\r\n]|$)/g, '')));
} catch(err) {
  console.error("[Error] Failed loading config file '" + cfgFile + "': " + err.message);
  process.exit(1);
}

module.exports = config;