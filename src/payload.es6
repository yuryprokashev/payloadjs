/**
 *Created by py on 08/12/2016
 */
"use strict";
const SERVICE_NAME = 'payloadjs';

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
const buildMongoConStr = require('./helpers/buildConnString.es6');

let kafkaBus,
    db;

let kafkaService,
    configService,
    payloadService;

let payloadCtrl;

let dbConfig,
    dbConnectStr,
    kafkaListeners;

kafkaBus = kafkaBusFactory(kafkaHost, 'Message-Service');
kafkaService = kafkaServiceFactory(kafkaBus);


kafkaBus.producer.on('ready', ()=> {
    configService = configFactory(kafkaService);
    configService.on('ready', ()=>{
        dbConfig = configService.get(SERVICE_NAME);

        dbConnectStr = buildMongoConStr(dbConfig);
        db = dbFactory(dbConnectStr);
        payloadService = payloadServiceFactory(db);
        payloadCtrl = payloadCtrlFactory(payloadService, kafkaService);

        kafkaListeners = configService.get(SERVICE_NAME).kafkaListeners;

        kafkaService.subscribe(kafkaListeners.createMessage, payloadCtrl.reactKafkaMessage);
        kafkaService.subscribe(kafkaListeners.getPayload, payloadCtrl.handleKafkaMessage);
        kafkaService.subscribe(kafkaListeners.copyPayload, payloadCtrl.handleKafkaMessage);
        kafkaService.subscribe(kafkaListeners.clearPayload, payloadCtrl.handleKafkaMessage);
        kafkaService.subscribe(kafkaListeners.aggMonthData, payloadCtrl.handleKafkaMessage);
    });
});
