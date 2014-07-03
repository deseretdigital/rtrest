var winston = require('winston'),
    _ = require('lodash');

if(!_.str) {
  _.str = require('underscore.string');
}

var customLevels = _.cloneDeep(winston.config.cli);
customLevels.levels.fatal = 10;
customLevels.colors.fatal = 'red';
winston.addColors(customLevels.colors);

var logger = new (winston.Logger)({levels: customLevels.levels});

var fatalFn = logger.fatal;
logger.fatal = function() {
  fatalFn.apply(this, arguments);
  process.exit(1);
}


function tsFn() {
  var d = new Date();
  return _.str.sprintf('%s-%s-%s %s:%s:%s',
    d.getFullYear(),
    _.str.pad(d.getMonth() + 1, 2, '0'),
    _.str.pad(d.getDate(), 2, '0'),
    _.str.pad(d.getHours(), 2, '0'),
    _.str.pad(d.getMinutes(), 2, '0'),
    _.str.pad(d.getSeconds(), 2, '0')
  );
}


//
//  Useful options to pass to these functions include things like 'level', 'filename' (for file transport)
//


logger.addConsoleLog = function(options) {
  logger.add(winston.transports.Console, _.extend({
    handleExceptions: false,
    colorize: true,
    timestamp: tsFn
  }, options || {}));
};

logger.addFileLog = function(options) {
  logger.add(winston.transports.File, _.extend({
    handleExceptions: false,
    colorize: false,
    json: false,
    timestamp: tsFn
  }, options || {}));
};


module.exports = logger;