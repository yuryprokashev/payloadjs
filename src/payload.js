/**
 * Created by py on 03/09/16.
 */

var PayloadService = require('./PayloadService2');
const parseProcessArgs = require('./parseProcessArgs');

var args = parseProcessArgs();

var app = new PayloadService(args[0].isProd);