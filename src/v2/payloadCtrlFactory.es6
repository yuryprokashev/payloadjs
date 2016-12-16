/**
 *Created by py on 08/12/2016
 */
'use strict';
module.exports = (payloadService, kafkaService) => {

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

    const extractWriteData =(kafkaMessage) => {
        let profile = JSON.parse(kafkaMessage.value).request.writeData;
        if(profile === undefined || profile === null) {
            let context;
            context = extractContext(kafkaMessage);
            context.response = {error: 'profile is empty'};
            kafkaService.send(makeResponseTopic(kafkaMessage), context);
        }
        else {
            return profile;
        }
    };

    const makeResponseTopic = (kafkaMessage) => {
        let re = /-request/;
        return kafkaMessage.topic.replace(re, '-response');
    };


    let payloadCtrl = {};

    payloadCtrl.createOrUpdatePayload = (kafkaMessage) => {

        let context, query, data;
        context = extractContext(kafkaMessage);
        query = extractQuery(kafkaMessage);
        data = extractWriteData(kafkaMessage);


        payloadService.createOrUpdate(query, data).then(
            (result) => {
                context.response = result;
                kafkaService.send('payload-done', context);

            },
            (error) => {
                context.response = error;
                kafkaService.send('payload-done', context);
            }
        )

    };

    payloadCtrl.getPayloads = (kafkaMessage) => {

        let context, query, data;

        context = extractContext(kafkaMessage);
        query = extractQuery(kafkaMessage);
        data = undefined;

        payloadService.find(query).then(
            (result) => {
                context.response = result;
                kafkaService.send(makeResponseTopic(kafkaMessage), response);

            },
            (error) => {
                context.response = error;
                kafkaService.send(makeResponseTopic(kafkaMessage), response);
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

    payloadCtrl.getMonthData = (kafkaMessage) => {

        let context, query, data;

        context = extractContext(kafkaMessage);
        query = extractQuery(kafkaMessage);
        data = undefined;
        // console.log(query);

        payloadService.aggregate(query).then(
            (result) => {
                // console.log(`result is ${JSON.stringify(result)}`);
                context.response = result;
                kafkaService.send(makeResponseTopic(kafkaMessage), context);
            },
            (error) => {
                // console.log(error);
                context.response = error;
                kafkaService.send(makeResponseTopic(kafkaMessage), context);
            }
        )

    };


    return payloadCtrl;
};