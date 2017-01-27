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
const kafkaBusFactory = require('my-kafka').kafkaBusFactory;
const kafkaServiceFactory = require('my-kafka').kafkaServiceFactory;

const configObjectFactory = require('my-config').configObjectFactory;
const configServiceFactory = require('my-config').configServiceFactory;
const configCtrlFactory = require('my-config').configCtrlFactory;

const payloadCtrlFactory = require('./payloadCtrlFactory.es6');
const payloadServiceFactory = require('./payloadServiceFactory.es6');
const buildMongoConStr = require('./helpers/buildConnString.es6');

let kafkaBus,
    db,
    configObject;

let kafkaService,
    configService,
    payloadService;

let payloadCtrl,
    configCtrl;

let dbConfig,
    dbConnectStr,
    kafkaListeners;

kafkaBus = kafkaBusFactory(kafkaHost, SERVICE_NAME);
kafkaService = kafkaServiceFactory(kafkaBus);

kafkaBus.producer.on('ready', ()=> {

    configObject = configObjectFactory(SERVICE_NAME);
    configObject.init().then(
        (config) => {
            configService = configServiceFactory(config);
            configCtrl = configCtrlFactory(configService, kafkaService);
            kafkaService.subscribe('get-config-response', configCtrl.writeConfig);
            kafkaService.send('get-config-request', configObject);
            configCtrl.on('ready', () => {
                dbConfig = configService.read(SERVICE_NAME, 'db');
                dbConnectStr = buildMongoConStr(dbConfig);
                db = dbFactory(dbConnectStr);

                payloadService = payloadCtrlFactory(db);
                payloadCtrl = payloadCtrlFactory(payloadService, kafkaService);

                kafkaListeners = configService.read(SERVICE_NAME, 'kafkaListeners');

                kafkaService.subscribe(kafkaListeners.createMessage, payloadCtrl.reactKafkaMessage);
                kafkaService.subscribe(kafkaListeners.getPayload, payloadCtrl.handleKafkaMessage);
                kafkaService.subscribe(kafkaListeners.copyPayload, payloadCtrl.handleKafkaMessage);
                kafkaService.subscribe(kafkaListeners.clearPayload, payloadCtrl.handleKafkaMessage);
                kafkaService.subscribe(kafkaListeners.aggMonthData, payloadCtrl.handleKafkaMessage);
            });
            configCtrl.on('error', (args) => {
                console.log(args);
            });
        },
        (err) => {
            console.log(`ConfigObject Promise rejected ${JSON.stringify(err.error)}`);
        }
    );
});

//
// kafkaBus.producer.on('ready', ()=> {
//     configService = configFactory(kafkaService);
//     configService.on('ready', ()=>{
//         dbConfig = configService.get(SERVICE_NAME).db;
//
//         dbConnectStr = buildMongoConStr(dbConfig);
//         db = dbFactory(dbConnectStr);
//         payloadService = payloadServiceFactory(db);
//         payloadCtrl = payloadCtrlFactory(payloadService, kafkaService);
//
//         kafkaListeners = configService.get(SERVICE_NAME).kafkaListeners;
//
//         kafkaService.subscribe(kafkaListeners.createMessage, payloadCtrl.reactKafkaMessage);
//         kafkaService.subscribe(kafkaListeners.getPayload, payloadCtrl.handleKafkaMessage);
//         kafkaService.subscribe(kafkaListeners.copyPayload, payloadCtrl.handleKafkaMessage);
//         kafkaService.subscribe(kafkaListeners.clearPayload, payloadCtrl.handleKafkaMessage);
//         kafkaService.subscribe(kafkaListeners.aggMonthData, payloadCtrl.handleKafkaMessage);
//     });
// });
