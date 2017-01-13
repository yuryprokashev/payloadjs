/**
 *Created by py on 25/11/2016
 */
'use strict';
const EventEmitter = require('events').EventEmitter;
class ConfigService extends EventEmitter {
    constructor() {
        super();
    }
}

module.exports = (kafkaService) => {

    let configService = new ConfigService();
    let config = {};
    let requestId = require('./helpers/guid.es6')();
    kafkaService.subscribe('get-config-response', (kafkaMessage) => {
        let message = JSON.parse(kafkaMessage.value);
        if(message.requestId === requestId) {
            config = message.responsePayload[0];
            configService.emit('ready');
        }
    });
    kafkaService.send('get-config-request', {requestId: requestId});

    configService.get = (option) => {
        if(!option) {
            return config;
        }
        else {
            return config[option];
        }
    };

    return configService;
    
};