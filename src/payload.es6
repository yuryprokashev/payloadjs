/**
 *Created by py on 08/12/2016
 */
"use strict";
const KAFKA_TEST = "54.154.211.165";
const KAFKA_PROD = "54.154.226.55";
const parseProcessArgs = require('./parseProcessArgs.es6');
let args = parseProcessArgs();
let kafkaHost = (function(bool){
    let result = bool ? KAFKA_PROD : KAFKA_TEST;
    console.log(result);
    return result;
})(args[0].isProd);

const dbFactory = require('./dbFactory.es6');
const kafkaBusFactory = require('./kafkaBusFactory.es6');
const kafkaServiceFactory = require('./kafkaServiceFactory.es6');
const configFactory = require('./configFactory.es6');
const payloadCtrlFactory = require('./payloadCtrlFactory.es6');
const payloadServiceFactory = require('./payloadServiceFactory.es6');


const kafkaBus = kafkaBusFactory(kafkaHost, 'Message-Service');
const kafkaService = kafkaServiceFactory(kafkaBus);

let configService, payloadCtrl, payloadService, db;

kafkaBus.producer.on('ready', ()=> {
    configService = configFactory(kafkaService);
    configService.on('ready', ()=>{
        let config = configService.get();
        db = dbFactory(config.db.dbURL);
        payloadService = payloadServiceFactory(db);
        payloadCtrl = payloadCtrlFactory(payloadService,  kafkaService);

        kafkaService.subscribe('create-message-response', payloadCtrl.reactKafkaMessage);
        kafkaService.subscribe('get-payload-request', payloadCtrl.handleKafkaMessage);
        kafkaService.subscribe('copy-payload-request', payloadCtrl.handleKafkaMessage);
        kafkaService.subscribe('clear-payload-request', payloadCtrl.handleKafkaMessage);
        kafkaService.subscribe('agg-month-data-request', payloadCtrl.handleKafkaMessage);
    });
});
