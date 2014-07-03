var request = require('request'),
    _ = require('lodash'),
    modUrl = require('url'),
    modQs = require('querystring');

module.exports = util = {
  // See: http://jsperf.com/js-for-loop-vs-array-indexof/159
  fastIndexOf: function(arr, search) {
    for(var i = 0; i < arr.length; i++) {
      if(arr[i] === search) {
        return i;
      }
    }

    return -1;
  },

  merge: function() {
    var args = Array.prototype.slice.apply(arguments);
    args.push(function(a, b) {
      if(a === undefined || b === undefined) return;

      // Multiples of a given property become an array, arrays are concatenated, duplicates are removed
      return _.uniq([].concat(a, b));
    });

    return _.merge.apply(_, args);
  },

  // See: http://stackoverflow.com/questions/3446170/escape-string-for-use-in-javascript-regex#answer-6969486
  regexpEscape: function(str) {
    return str.replace(/[\-\[\]\/\{\}\(\)\*\+\?\.\\\^\$\|]/g, "\\$&");
  },

  parseCookies: function(cookies) {
    cookies = cookies.toString();
    var matchPairs = new RegExp('([^=]+?)=(.*?)(?:;|$)', 'g'),
        pair = null;

    var pairs = {};
    while(pair = matchPairs.exec(cookies)) {
      var cookieName = pair[1].trim();
      if(pairs[cookieName] && !_.isArray(pairs[cookieName])) {
        pairs[cookieName] = [pairs[cookieName]];
      }

      if(_.isArray(pairs[cookieName])) {
        pairs[cookieName].push(pair[2].trim());
      } else {
        pairs[cookieName] = pair[2].trim();
      }
    }

    return pairs;
  },

  normalizeURI: function(uri) {
    uri = decodeURI(uri || '');
    if(uri[0] != '/') {
      if(!_.str.startsWith(uri, 'http')) {
        uri = 'http://' + uri;
      }
      var urlInfo = modUrl.parse(uri);
      uri = urlInfo.pathname;
    }
    uri = ('/' + uri + '/').replace(/\/+/g, '/').slice(0, -1);
    return uri;
  },

  normalizeQstr: function(qstr, options) {
    options = _.extend({
      arrayBrackets: false,
      returnAsObj: false
    }, options || {});

    var qobj = _.clone(qstr);
    if(_.isString(qobj)) {
      if(qstr[0] == '?') qstr = qstr.slice(1);
      qobj = modQs.parse(qstr);
    }

    var paramKeys = _.keys(qobj).sort(),
        newQsObj = {};
    for(var i = 0; i < paramKeys.length; i++) {
      var param = paramKeys[i],
          val = qobj[param];

      if(_.isArray(val)) {
        if(options.arrayBrackets && !_.str.endsWith(param, '[]')) {
          newQsObj[param + '[]'] = val;
        } else if(!options.arrayBrackets && _.str.endsWith(param, '[]')) {
          newQsObj[param.match(/[^\[\]]*/)[0]] = val;
        } else {
          newQsObj[param] = val;
        }
      } else {
        newQsObj[param] = val;
      }
    }

    return options.returnAsObj ? newQsObj : modQs.stringify(newQsObj);
  },

  normalizeURL: function(url, options) {
    options = _.extend({
      qsArrayBrackets: false,
      includeProtocol: true,
      includeQS: true
    }, options || {});

    url = decodeURI(url || '');
    if(!_.str.startsWith(url, 'http')) {
      url = 'http://' + url;
    }

    var urlInfo = modUrl.parse(url);
    url = modUrl.format({
      protocol: options.includeProtocol ? (urlInfo.protocol || 'http:') : null,
      host: urlInfo.host,
      pathname: util.normalizeURI(urlInfo.pathname),
      search: options.includeQS ? util.normalizeQstr(urlInfo.query) : null
    });

    if(!options.includeProtocol) {
      // Remove the '//' from the beginning of the URL
      url = url.slice(2);
    }

    return url;
  },

  readCookie: function(cookies, name) {
    var cookie = cookies.match(new RegExp(name + '=(.*?)(?:;|$)'));
    if(cookie) {
      return cookie[1];
    }
  },

  request: function(options, cb) {
    options || (options = {});
    if(_.isString(options)) {
      options = {url: options};
    }

    if(!_.str.startsWith(options.url, 'http')) {
      options.url = 'http://' + options.url;
    }

    options = _.merge({
      jar: false,
      strictSSL: true,
      followRedirect: true,
      followAllRedirects: true,
      headers: {
        accept: 'application/json'
      },
      encoding: 'utf8',
      timeout: 10000
    }, options);
    var urlInfo = modUrl.parse(options.url);
    delete options.protocol;

    _.each(options.cookies, function(cval, cname) {
      if(!options.jar) options.jar = request.jar();
      options.jar.setCookie(request.cookie(cname + '=' + cval), urlInfo.protocol + '//' + urlInfo.hostname);
    });

    // Pass an extra retry function to callback
    var retries = 0;
    function handleResponse() {
      var args = Array.prototype.slice.apply(arguments);
      args.push(function(delay, maxRetries) {
        if(retries >= maxRetries) return;

        setTimeout(function() {
          request(options, handleResponse);
        }, delay);
      });
      args[args.length - 1].retries = ++retries;
      cb.apply(this, args);
    }

    return request(options, handleResponse);
  }
};