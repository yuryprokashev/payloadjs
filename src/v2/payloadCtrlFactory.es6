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
    
    const extractQueryFromResponse = kafkaMessage => {
        let query;
        query = {_id: JSON.parse(kafkaMessage.value).response.id};
        return query;
    };

    const extractWriteData = (kafkaMessage) => {
        let writeData, method;
        method = extractMethod(kafkaMessage);
        if(method === 'createOrUpdate'){
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
                return writeData;
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
        console.log(`outside service.handle \n ${JSON.stringify(kafkaMessage)}`);

        let method, query, data;
        method = extractMethod(kafkaMessage);
        query = extractQuery(kafkaMessage);
        data = extractWriteData(kafkaMessage);

        console.log(`method ${method} \n ${JSON.stringify(query)}, \n data: ${JSON.stringify(data)}, \n`);

        payloadService.handle(method, query, data).then(
            ((kafkaMessage) => {
                return (data) => {
                    console.log(`inside service.handle \n ${JSON.stringify(kafkaMessage)}`);
                    reply(data, kafkaMessage);
                }
            })(kafkaMessage),
            ((kafkaMessage) => {
                return (data) => {
                    console.log(`inside service.handle \n ${JSON.stringify(kafkaMessage)}`);
                    reply(data, kafkaMessage);
                }
            })(kafkaMessage)
        );
    };

    payloadCtrl.reactKafkaMessage = kafkaMessage => {
        let method, query, data;

        method = extractMethod(kafkaMessage);
        if(method === null) {
            console.log('shit! method extraction doesnt work');
        }
        query = {};
        data = extractWriteDataFromResponse(kafkaMessage);

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

    // payloadCtrl.createOrUpdatePayload = (kafkaMessage) => {
    //
    //     let context, query, data;
    //     context = extractContext(kafkaMessage);
    //     query = extractQuery(kafkaMessage);
    //     data = extractWriteData(kafkaMessage);
    //
    //
    //     payloadService.createOrUpdate(query, data).then(
    //         (result) => {
    //             context.response = result;
    //             kafkaService.send(makeResponseTopic(kafkaMessage), context);
    //
    //         },
    //         (error) => {
    //             context.response = error;
    //             kafkaService.send(makeResponseTopic(kafkaMessage), context);
    //         }
    //     )
    //
    // };
    //
    // payloadCtrl.getPayloads = (kafkaMessage) => {
    //
    //     let context, query, data;
    //
    //     context = extractContext(kafkaMessage);
    //     query = extractQuery(kafkaMessage);
    //     data = undefined;
    //
    //     payloadService.find(query).then(
    //         (result) => {
    //             context.response = result;
    //             kafkaService.send(makeResponseTopic(kafkaMessage), context);
    //
    //         },
    //         (error) => {
    //             context.response = error;
    //             kafkaService.send(makeResponseTopic(kafkaMessage), context);
    //         }
    //     )
    //
    // };
    //
    // payloadCtrl.copyPayloads = (kafkaMessage) => {
    //     let parsedMessage = JSON.parse(kafkaMessage.value);
    //     let response = {
    //         requestId: parsedMessage.requestId,
    //         responsePayload: {},
    //         responseErrors: []
    //     };
    //
    // };
    //
    // payloadCtrl.clearPayloads = (kafkaMessage) => {
    //     let parsedMessage = JSON.parse(kafkaMessage.value);
    //     let response = {
    //         requestId: parsedMessage.requestId,
    //         responsePayload: {},
    //         responseErrors: []
    //     };
    //
    // };
    //
    // payloadCtrl.getMonthData = (kafkaMessage) => {
    //
    //     let context, query, data;
    //
    //     context = extractContext(kafkaMessage);
    //     query = extractQuery(kafkaMessage);
    //     data = undefined;
    //     // console.log(query);
    //
    //     payloadService.aggregate(query).then(
    //         (result) => {
    //             // console.log(`result is ${JSON.stringify(result)}`);
    //             context.response = result;
    //             kafkaService.send(makeResponseTopic(kafkaMessage), context);
    //         },
    //         (error) => {
    //             // console.log(error);
    //             context.response = error;
    //             kafkaService.send(makeResponseTopic(kafkaMessage), context);
    //         }
    //     )
    //
    // };

    return payloadCtrl;
};