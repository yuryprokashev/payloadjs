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
    dbConnectStr;

let bootstrapComponents,
    handleError;

bootstrapComponents = () => {
    configObject = configObjectFactory(SERVICE_NAME);
    configService = configServiceFactory(configObject);
    configCtrl = configCtrlFactory(configService, kafkaService);

    configCtrl.on('ready',() => {
        dbConfig = configService.read(`${SERVICE_NAME}.db`);
        dbConnectStr = buildMongoConStr(dbConfig);
        db = dbFactory(dbConnectStr);

        payloadService = payloadCtrlFactory(db);
        console.log(`before payloadCtrlFactory call ${JSON.stringify(configService)}`);
        payloadCtrl = payloadCtrlFactory(payloadService, configService, kafkaService);

    });

    configCtrl.on('error', (args) => {
        handleError(args);
    })
};

handleError = (err) => {
    //TODO. Implement centralized error logging.
    console.log(err);
};

kafkaBus = kafkaBusFactory(kafkaHost, SERVICE_NAME);
kafkaService = kafkaServiceFactory(kafkaBus);

kafkaBus.producer.on('ready', bootstrapComponents);
