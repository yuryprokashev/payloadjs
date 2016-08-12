/**
 * Created by py on 11/08/16.
 */

var BusService;

var KafkaAdapter = require('./KafkaAdapter');

BusService = function (adapter) {
    var self = this;
    self.adapter = adapter;
    return {
        send: function(topic, message) {
            self.adapter.send(topic, message);
        },
        subscribe: function(topic, callback) {
            self.adapter.subscribe(topic, callback);
        }
    }
}(KafkaAdapter);

module.exports = BusService;