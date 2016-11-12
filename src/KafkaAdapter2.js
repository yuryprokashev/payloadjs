/**
 * Created by py on 03/11/2016.
 */
'use strict';

const kafka = require('kafka-node');

class KafkaAdapter2 {
    constructor(kafkaHost, clientName, partitionerType){
        this.clientName = clientName;
        let kafkaClient = new kafka.Client(`${kafkaHost}:2181/`, clientName);
        this.producer = new kafka.Producer(kafkaClient, {partitionerType: partitionerType});
        function onProducerReady() {
            console.log(`${clientName}: NodeJS Kafka Producer Ready...`);
        }
        this.producer.on('ready', onProducerReady);
        this.producer.on('error', onProducerError);
        function onProducerError(err) {
            console.log('producer error');
            console.log(err);
            console.log('-------------');
        }
        this.consumer = new kafka.Consumer(kafkaClient, []);
        function onConsumerError(error) {
            console.log('consumer error:');
            console.log(error);
            console.log('-------------');
        }
        this.consumer.on('error', onConsumerError);
    }
    send(topic, message){
        console.log(`${this.clientName}`);
        console.log(`${JSON.stringify(message)}`);
        let payload = [];
        payload.push(
            {
                topic: topic,
                messages: JSON.stringify(message)
            }
        );
        this.producer.send(payload, onProducerSent);
        function onProducerSent(err, data){
            if(err){
                console.log('producer send error');
                console.log(err);
            }
            if(data){

            }
        }
    }
    subscribe(topic, callback){
        let topics = (function(qty){
            let t = [];
            for(let i = 0; i < qty; i++){
                t.push({topic: topic, partition: i});
            }
            return t;
        })(6);
        this.consumer.addTopics(topics, onTopicsAdded);
        function onTopicsAdded(err, added){
            if(err){
                console.log('consumer adding topics failed');
                console.log(err);
            }
        }
        this.consumer.on('message', onConsumerMessage);
        function onConsumerMessage(m){
            if(m.topic === topic){
                callback(m);
            }
        }
    }
}

module.exports = KafkaAdapter2;
