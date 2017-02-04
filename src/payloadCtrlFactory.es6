/**
 *Created by py on 08/12/2016
 */
'use strict';
const guid = require('./helpers/guid.es6');

module.exports = (payloadService, configService, kafkaService) => {

    let payloadCtrl = {};

    let kafkaListeners,
        isSignedMessage;

    let handleKafkaMessage,
        reactKafkaMessage,
        reply,
        extractMethod,
        extractWriteDataFromResponse;

    extractWriteDataFromResponse = kafkaMessage => {
        let writeData, method;
        method = extractMethod(kafkaMessage);
        if(method === 'createOrUpdate'){
            writeData = JSON.parse(kafkaMessage.value).response;
            if(writeData === undefined || writeData === null) {
                let context;
                context = kafkaService.extractContext(kafkaMessage);
                context.response = {error: 'response in kafkaMessage is empty'};
                kafkaService.send(kafkaService.makeResponseTopic(kafkaMessage), false, context);
            }
            else {
                let payload = JSON.parse(writeData.payload);
                // console.log(`Payload \n ${JSON.stringify(payload)}`);
                return {
                    _id: payload.id || guid(),
                    type: payload.type || 0,
                    amount: payload.amount || 0,
                    dayCode: payload.dayCode,
                    monthCode: payload.monthCode,
                    description: payload.description || '',
                    labels: payload.labels,
                    occurredAt: writeData.occurredAt,
                    sourceId: writeData.sourceId,
                    campaignId: writeData.campaignId,
                    userId: writeData.userId,
                    messageId: writeData.id,
                    userToken: writeData.userToken || undefined,
                    commandId: writeData.commandId || undefined,
                    // storedAt: new Date().valueOf()
                };
            }
        }
        else {
            return undefined;
        }
    };

    extractMethod = kafkaMessage => {
        if(/(get)/.test(kafkaMessage.topic) === true) {
            return 'find';
        }
        else if(/(create)|(update)/.test(kafkaMessage.topic) === true) {
            return 'createOrUpdate';
        }
        else if(/agg/.test(kafkaMessage.topic) === true) {
            return 'aggregate';
        }
        else if(/copy/.test(kafkaMessage.topic) === true) {
            return 'copy';
        }
        else if(/clear/.test(kafkaMessage.topic) === true) {
            return 'clear';
        }
        else {
            return null;
        }
    };

    reply = (data, kafkaMessage) => {
        let context;
        context = kafkaService.extractContext(kafkaMessage);
        context.response = data;
        kafkaService.send(kafkaService.makeResponseTopic(kafkaMessage), false, context);
    };

    handleKafkaMessage = kafkaMessage => {

        let method, query, data;
        method = extractMethod(kafkaMessage);
        if(method === null) {
            console.log('shit! extractMethod does not work!')
        }
        query = kafkaService.extractQuery(kafkaMessage);
        data = kafkaService.extractWriteData(kafkaMessage);


        // console.log(`method ${method} \n ${JSON.stringify(query)}, \n data: ${JSON.stringify(data)}, \n`);

        payloadService.handle(method, query, data).then(
            ((kafkaMessage) => {
                return (data) => {
                    // console.log(`SUCCESS inside service.handle \n ${JSON.stringify(data)}`);
                    reply(data, kafkaMessage);
                }
            })(kafkaMessage),
            ((kafkaMessage) => {
                return (data) => {
                    // console.log(`ERROR inside service.handle \n ${JSON.stringify(data)}`);
                    reply(data, kafkaMessage);
                }
            })(kafkaMessage)
        );
    };

    reactKafkaMessage = kafkaMessage => {
        let method, query, data;

        method = extractMethod(kafkaMessage);
        if(method === null) {
            // console.log('shit! method extraction does not work');
        }
        data = extractWriteDataFromResponse(kafkaMessage);
        query = {_id: data._id};

        payloadService.handle(method, query, data).then(
            ((kafkaMessage) => {
                return (data) => {
                    console.log(`SUCCESS inside service.react \n ${JSON.stringify(data)}`);
                    reply(data, kafkaMessage);
                }
            })(kafkaMessage),
            ((kafkaMessage) => {
                return (data) => {
                    console.log(`ERROR inside service.react \n ${JSON.stringify(data)}`);
                    reply(data, kafkaMessage);
                }
            })(kafkaMessage)

        )
    };

    kafkaListeners = configService.read('payloadjs.kafkaListeners');
    isSignedMessage = false;
    if(kafkaListeners !== undefined) {
        kafkaService.subscribe(kafkaListeners.createMessage, isSignedMessage, reactKafkaMessage);
        kafkaService.subscribe(kafkaListeners.getPayload, isSignedMessage, handleKafkaMessage);
        kafkaService.subscribe(kafkaListeners.copyPayload, isSignedMessage, handleKafkaMessage);
        kafkaService.subscribe(kafkaListeners.clearPayload, isSignedMessage, handleKafkaMessage);
        kafkaService.subscribe(kafkaListeners.aggMonthData, isSignedMessage, handleKafkaMessage);
    }

    return payloadCtrl;
};