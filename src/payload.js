/**
 * Created by py on 03/09/16.
 */

var PayloadService = require('./PayloadService');
var PayloadModel = require('./PayloadModel');

var Bus = require('./BusService');

var app = new PayloadService(PayloadModel, Bus);