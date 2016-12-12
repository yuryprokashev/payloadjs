/**
 *Created by py on 08/12/2016
 */
'use strict';
module.exports = (payloadService, kafkaService) => {
    let payloadCtrl = {};

    payloadCtrl.subscribe = (topic, callback) => {
        kafkaService.subscribe(topic, callback);
    };

    payloadCtrl.createOrUpdatePayload = (kafkaMessage) => {
        let parsedMessage = JSON.parse(kafkaMessage.value);
        let topic = kafkaMessage.topic;
        let response = {
            requestId: parsedMessage.requestId,
            responsePayload: {},
            responseErrors: []
        };
        payloadService.createOrUpdate(topic, parsedMessage).then(
            (result) => {
                response.responsePayload = result;
                kafkaService.send('payload-done', response);

            },
            (error) => {
                response.responseErrors.push(error);
                kafkaService.send('payload-done', response);
            }
        )

    };

    payloadCtrl.getPayloads = (kafkaMessage) => {
        let parsedMessage = JSON.parse(kafkaMessage.value);
        let topic = kafkaMessage.topic;

        let response = {
            requestId: parsedMessage.requestId,
            responsePayload: {},
            responseErrors: []
        };
        payloadService.findPayloads(topic, parsedMessage).then(
            (result) => {
                response.responsePayload = result;
                kafkaService.send('payload-response', response);

            },
            (error) => {
                response.responseErrors.push(error);
                kafkaService.send('payload-response', response);
            }
        )

    };

    payloadCtrl.copyPayloads = (kafkaMessage) => {
        let parsedMessage = JSON.parse(kafkaMessage.value);
        let response = {
            requestId: parsedMessage.requestId,
            responsePayload: {},
            responseErrors: []
        };
        
    };

    payloadCtrl.clearPayloads = (kafkaMessage) => {
        let parsedMessage = JSON.parse(kafkaMessage.value);
        let response = {
            requestId: parsedMessage.requestId,
            responsePayload: {},
            responseErrors: []
        };

    };

    payloadCtrl.aggregatePayloads = (kafkaMessage) => {
        let parsedMessage = JSON.parse(kafkaMessage.value);
        let response = {
            requestId: parsedMessage.requestId,
            responsePayload: {},
            responseErrors: []
        };

    };


    return payloadCtrl;
};