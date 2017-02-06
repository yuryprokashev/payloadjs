/**
 *Created by py on 08/12/2016
 */
'use strict';
const guid = require('./helpers/guid.es6');

module.exports = (payloadService, configService, kafkaService) => {

    let payloadCtrl = {};

    let kafkaListeners;

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
                return {error: 'unable to extract writeData from kafkaMessage.response'};
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
            return {error: 'unknown method'};
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
            return {error: 'unable to define method from topic'};
        }
    };

    reply = (data, kafkaMessage) => {
        let context, topic;
        context = kafkaService.extractContext(kafkaMessage);
        if(context.error === undefined) {
            context.response = data;
            topic = kafkaService.makeResponseTopic(kafkaMessage);
            kafkaService.send(topic, context);
        }
        else {
            console.error(context.error);
        }

    };

    handleKafkaMessage = kafkaMessage => {

        let method, query, data;
        method = extractMethod(kafkaMessage);
        if(method.error !== undefined) {console.log(method.error)}

        query = kafkaService.extractQuery(kafkaMessage);
        if(query.error !== undefined) {console.error(query.error)}

        data = kafkaService.extractWriteData(kafkaMessage);
        if(data.error !== undefined) {console.error(data.error)}


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
    if(kafkaListeners !== undefined) {
        kafkaService.subscribe(kafkaListeners.createMessage, reactKafkaMessage);
        kafkaService.subscribe(kafkaListeners.getPayload, handleKafkaMessage);
        kafkaService.subscribe(kafkaListeners.copyPayload, handleKafkaMessage);
        kafkaService.subscribe(kafkaListeners.clearPayload, handleKafkaMessage);
        kafkaService.subscribe(kafkaListeners.aggMonthData, handleKafkaMessage);
    }

    return payloadCtrl;
};