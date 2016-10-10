/**
 * Created by py on 11/08/16.
 */

var mongoose = require('mongoose');

const MONGO_HOST = "mongodb://localhost:27017/pfin";
// const MONGO_HOST = "mongodb://54.229.108.38:27017/pfin";

mongoose.connect(MONGO_HOST);

var PayloadModel = mongoose.model('Payload', require('./payload.schema.js'), 'payloads');

module.exports = PayloadModel;