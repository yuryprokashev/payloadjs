/**
 * Created by py on 09/08/16.
 */


var KafkaAdapter;

KafkaAdapter = function () {

    var self = this;

    var kafka = require('kafka-node');
    var kafkaClient = new kafka.Client('localhost:2181/', 'kafka-node-client');

    var setUpProducer = function (kafkaClient) {
        self.producer = new kafka.Producer(kafkaClient);
        self.producer.on('ready', function () {
            console.log('NodeJS Kafka Producer Ready...');
        });
    };

    var setUpConsumer = function(kafkaClient){
        self.consumer = new kafka.Consumer(kafkaClient,[]);
        self.consumer.on('error', function (error) {
            console.log('consumer error:');
            console.log(error);
            console.log('-------------');
        })
    };

    // param: String topic - topic, where to put the new message
    // param: Object message - arbitrary Message {message: <whatever>} format
    // function: sends message.<whatever> property to Kafka withing 'topic'
    // return: void
    var send = function (topic, message) {
        var payload = [];
        payload.push(
            {
                topic: topic,
                messages: JSON.stringify(message.message)
            });
        self.producer.send(payload, function (err, data) {
            if(err) {
                // console.log("error");
                // console.log(err);
            }
            if(data){
                // console.log("data");
                // console.log(data);
            }
        })
    };

    // param: String - topic
    // param: Function callback - the function to be executed after message will arrive
    // function: tell Kafka that you want to get messages for this topic
    var subscribe = function (topic, callback) {
        self.consumer.addTopics([topic], function (err, added) {
            if(err){
                // console.log("error");
                // console.log(err);
            }
            if(added){
                // console.log("added");
                // console.log(added);
            }
        });
        self.consumer.on('message', function (message) {
            callback(message);
        });
    };

    // MAIN LOOP
    setUpProducer(kafkaClient);
    setUpConsumer(kafkaClient);

    // MODIFY BELOW VERY CAREFULLY!!!
    return {
        send: send,
        subscribe: subscribe
    }
}();

module.exports = KafkaAdapter;