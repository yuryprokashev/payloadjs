/**
 *Created by py on 08/12/2016
 */
'use strict';
module.exports = (payloadService, kafkaService) => {

    const guid = require('./guid.es6');

    const extractContext = (kafkaMessage) => {
        let context;
        context = JSON.parse(kafkaMessage.value);
        if(context === undefined) {
            let newContext = {};
            newContext.response = {error: 'arrived context is empty'};
            kafkaService.send(makeResponseTopic(kafkaMessage), newContext);
        }
        return context;
    };

    const extractQuery = (kafkaMessage) => {
        let query = JSON.parse(kafkaMessage.value).request.query;
        if(query === undefined || query === null) {
            let context;
            context = extractContext(kafkaMessage);
            context.response = {error: 'query is empty'};
            kafkaService.send(makeResponseTopic(kafkaMessage), context);
        }
        else {
            return query;
        }
    };

    const extractWriteData = (kafkaMessage) => {
        let writeData, method;
        method = extractMethod(kafkaMessage);
        if(method === 'createOrUpdate' || method === 'copy'){
            writeData = JSON.parse(kafkaMessage.value).request.writeData;
            if(writeData === undefined || writeData === null) {
                let context;
                context = extractContext(kafkaMessage);
                context.response = {error: 'writeData is empty'};
                kafkaService.send(makeResponseTopic(kafkaMessage), context);
            }
            else {
                return writeData;
            }
        }
        else {
            return undefined;
        }

    };
    
    const extractWriteDataFromResponse = kafkaMessage => {
        let writeData, method;
        method = extractMethod(kafkaMessage);
        if(method === 'createOrUpdate'){
            writeData = JSON.parse(kafkaMessage.value).response;
            if(writeData === undefined || writeData === null) {
                let context;
                context = extractContext(kafkaMessage);
                context.response = {error: 'response in kafkaMessage is empty'};
                kafkaService.send(makeResponseTopic(kafkaMessage), context);
            }
            else {
                let payload = JSON.parse(writeData.payload);
                // console.log(`Payload \n ${JSON.stringify(payload)}`);
                return {
                    _id: payload.id || guid(),
                    type: 1, // TODO. It means 'Expense', but when bot send a payload, this is not expense.
                    amount: payload.amount,
                    dayCode: payload.dayCode,
                    monthCode: payload.monthCode,
                    description: payload.description || '',
                    labels: payload.labels,
                    occurredAt: writeData.occurredAt,
                    sourceId: writeData.sourceId,
                    campaignId: writeData.campaignId,
                    userId: writeData.userId,
                    messageId: writeData.id,
                    userToken: writeData.userToken,
                    commandId: writeData.commandId,
                    storedAt: new Date().valueOf()

                };
            }
        }
        else {
            return undefined;
        }
    };

    const makeResponseTopic = (kafkaMessage) => {
        if(/-request/.test(kafkaMessage.topic)){
            return kafkaMessage.topic.replace(/-request/, '-response');
        }
        else if(/-response/.test(kafkaMessage.topic)) {
            return `${kafkaMessage.topic}-processed`;
        }

    };

    const extractMethod = kafkaMessage => {
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

    const reply = (data, kafkaMessage) => {
        let context;
        context = extractContext(kafkaMessage);
        context.response = data;
        kafkaService.send(makeResponseTopic(kafkaMessage), context);
    };



    let payloadCtrl = {};

    payloadCtrl.handleKafkaMessage = kafkaMessage => {

        let method, query, data;
        method = extractMethod(kafkaMessage);
        if(method === null) {
            console.log('shit! extractMethod does not work!')
        }
        query = extractQuery(kafkaMessage);
        data = extractWriteData(kafkaMessage);

        // console.log(`method ${method} \n ${JSON.stringify(query)}, \n data: ${JSON.stringify(data)}, \n`);

        payloadService.handle(method, query, data).then(
            ((kafkaMessage) => {
                return (data) => {
                    console.log(`SUCCESS inside service.handle \n ${JSON.stringify(data)}`);
                    reply(data, kafkaMessage);
                }
            })(kafkaMessage),
            ((kafkaMessage) => {
                return (data) => {
                    console.log(`ERROR inside service.handle \n ${JSON.stringify(data)}`);
                    reply(data, kafkaMessage);
                }
            })(kafkaMessage)
        );
    };

    payloadCtrl.reactKafkaMessage = kafkaMessage => {
        let method, query, data;

        method = extractMethod(kafkaMessage);
        if(method === null) {
            console.log('shit! method extraction does not work');
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

    return payloadCtrl;
};