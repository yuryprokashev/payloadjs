/**
 * Created by py on 11/08/16.
 */

var BusService;

var KafkaAdapter = require('./KafkaAdapter');

BusService = (function (adapter) {

    return {
        send: function(topic, message) {
            // console.log(`bus sending message ${message}`);
            adapter.send(topic, {message: message});
        },
        subscribe: function(topic, callback) {
            // console.log(`bus gets  and call ${topic}`);
            adapter.subscribe(topic, callback);
        }
    }
})(new KafkaAdapter());

module.exports = BusService;