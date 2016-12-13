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
        // console.log(kafkaMessage);
        let context = JSON.parse(kafkaMessage.value);
        if(context === undefined) {
            kafkaService.send('get-month-data-response',
                {
                    context: {
                        response: {
                            error: 'api sent empty context in kafkaMessage.value'
                        }
                    }
                });
        }
        if(context.request === undefined) {
            kafkaService.send('get-month-data-response',
                {
                    context: {
                        response: {
                            error: 'api sent empty request in kafkaMessage.value.context'
                        }
                    }
                });
        }
        
        payloadService.aggregatePayloads(kafkaMessage.topic, context.request).then(
            (result) => {
                console.log(`result is ${result}`);
                context.response = result;
                kafkaService.send('get-month-data-response', context);
            },
            (error) => {
                // console.log(error);
                context.response = error;
                kafkaService.send('get-month-data-response', context);
            }
        )

    };


    return payloadCtrl;
};