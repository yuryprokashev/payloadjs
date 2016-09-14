/**
 * Created by py on 11/08/16.
 */

var mongoose = require('mongoose');

mongoose.connect("mongodb://localhost:27017/pfin");

var PayloadModel = mongoose.model('Payload', require('./payload.schema.js'), 'payloads');

module.exports = PayloadModel;